"""Alpha Vantage provider adapter.

One URL — ``https://www.alphavantage.co/query`` — with the function selected by
the ``function`` query parameter and the credential by ``apikey`` (env name
``ALPHAVANTAGE_API_KEY``). The throttle bucket is the same ``alphavantage`` host
key the OHLCV loader uses.

**The gotcha this module exists for:** Alpha Vantage answers **HTTP 200** with an
in-band envelope instead of data — ``{"Information": ...}`` for a premium gate
*or* a rate limit, ``{"Note": ...}`` for a soft throttle, and
``{"Error Message": ...}`` for a bad request. A premium gate therefore looks
exactly like a successful call unless the envelope is detected, so
:meth:`AlphaVantageProvider._extract` checks all three keys and raises
:class:`~src.data_providers.errors.ProviderHTTPError` carrying the message.

One refinement, taken from the measured behaviour of
``backtest.loaders.alphavantage_loader``: Alpha Vantage can return a *usable*
payload alongside an upsell note, so the envelope only fails the call when the
expected data key is absent. That keeps failover honest for a premium gate
(there is no data key at all) without discarding real bars.

Tier note (measured against the configured key on 2026-10-08; free tier is
**25 requests/day** shared across every function). Real data: ``TIME_SERIES_DAILY``,
``RSI``, ``TOP_GAINERS_LOSERS``, ``TREASURY_YIELD``, ``FEDERAL_FUNDS_RATE``,
``CPI``, ``INCOME_STATEMENT``, ``LISTING_STATUS`` (CSV). Premium-gated on this
key, answering ``Information: ... This is a premium endpoint ...``: ``OVERVIEW``
and ``NEWS_SENTIMENT``. Those two are implemented anyway and will raise, which is
the correct failover signal rather than a fake empty result. ``news_data`` and
``news_sentiment`` both read ``NEWS_SENTIMENT`` — Alpha Vantage publishes exactly
one news function — so on this key both categories fail over with the same
``Information`` body.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Any

from src.data_providers._http import fetch_json, fetch_text
from src.data_providers.base import Provider, env_value
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_PROVIDER = "alpha_vantage"
_BASE_URL = "https://www.alphavantage.co/query"
_API_KEY_ENV = "ALPHAVANTAGE_API_KEY"
_HOST_KEY = "alphavantage"
_MIN_INTERVAL_ENV = "VIBE_TRADING_ALPHAVANTAGE_MIN_INTERVAL"
# The free tier is a *daily* quota (25 requests/day), so spacing is the only
# lever left to avoid burning it in a burst.
_DEFAULT_MIN_INTERVAL_S = 1.0

#: Env values that are present but meaningless (Alpha Vantage's own demo key).
_API_KEY_PLACEHOLDERS = frozenset({"", "your-alphavantage-api-key", "demo"})

#: In-band envelopes that arrive with HTTP 200 in place of data.
_ENVELOPE_KEYS = ("Error Message", "Note", "Information")

#: Statement param -> Alpha Vantage function, and the container it answers under.
_STATEMENT_FUNCTIONS = {
    "income": "INCOME_STATEMENT",
    "balance": "BALANCE_SHEET",
    "cash": "CASH_FLOW",
}
_STATEMENT_KEYS = {
    "income": "income_statement",
    "balance": "balance_sheet",
    "cash": "cash_flow",
}
_REPORT_KEYS = {"annual": "annualReports", "quarterly": "quarterlyReports"}

#: Macro series this adapter has actually verified against the configured key.
_MACRO_FUNCTIONS = frozenset({"FEDERAL_FUNDS_RATE", "CPI"})

#: ``market_movers`` kind -> Alpha Vantage's list key (``None`` = all three).
_MOVERS_KEYS: dict[str, str | None] = {
    "gainers": "top_gainers",
    "losers": "top_losers",
    "most_active": "most_actively_traded",
    "all": None,
}
_MOVERS_KEYS_ORDER = ("top_gainers", "top_losers", "most_actively_traded")
#: Reverse of the map above, so a row from ``kind="all"`` still says which list
#: it came off.
_MOVERS_KIND_BY_KEY = {
    key: kind for kind, key in _MOVERS_KEYS.items() if key is not None
}


def _num(value: Any) -> float | None:
    """Coerce an Alpha Vantage string-encoded number to ``float``.

    Every Alpha Vantage value is a string; ``"None"`` and ``""`` mean "not
    reported" and must not become ``0.0``.

    Args:
        value: Raw field value.

    Returns:
        The float, or ``None`` when absent/non-numeric.
    """
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _percent(value: Any) -> float | None:
    """Coerce a percent-suffixed Alpha Vantage field to ``float``.

    ``TOP_GAINERS_LOSERS`` renders ``change_percentage`` as ``"133.9844%"``, so
    the trailing sign has to be removed before the number will parse.

    Args:
        value: Raw field value.

    Returns:
        The float percentage, or ``None`` when absent/non-numeric.
    """
    if isinstance(value, str):
        value = value.strip().removesuffix("%")
    return _num(value)


def _envelope_message(payload: Any) -> str | None:
    """Return the in-band envelope message from a payload, when present.

    Args:
        payload: Decoded JSON body.

    Returns:
        The first non-empty ``Error Message``/``Note``/``Information`` string,
        else ``None``.
    """
    if not isinstance(payload, dict):
        return None
    for key in _ENVELOPE_KEYS:
        message = payload.get(key)
        if isinstance(message, str) and message.strip():
            return message.strip()
    return None


def _av_field(row: dict[str, Any], name: str) -> Any:
    """Return the bar field whose Alpha Vantage label ends with *name*.

    Alpha Vantage numbers its bar fields (``"1. open"``, ``"5. volume"``) and the
    adjusted series renumbers them (``"5. adjusted close"``, ``"6. volume"``), so
    matching on the label's tail rather than the exact key keeps both series
    parseable with one map.

    Args:
        row: One bar's field mapping.
        name: Unnumbered field name, e.g. ``"adjusted close"``.

    Returns:
        The raw value, or ``None`` when the field is absent.
    """
    for key, value in row.items():
        if str(key).split(". ", 1)[-1].strip().lower() == name:
            return value
    return None


def _av_symbol(code: str) -> str:
    """Translate a project symbol into Alpha Vantage's ticker convention.

    The project writes US equities as ``AAPL.US``; Alpha Vantage uses bare US
    tickers and exchange suffixes only for non-US listings (``.LON``,
    ``.TRT``, ...). The project's ``.US`` suffix is therefore dropped and
    anything else passes through uppercased — sending ``AAPL.US`` upstream
    returns an error envelope, not data.

    Args:
        code: Project-side symbol, e.g. ``AAPL.US`` or a bare ``MSFT``.

    Returns:
        The Alpha Vantage ticker.
    """
    upper = code.strip().upper()
    if upper.endswith(".US"):
        return upper[: -len(".US")]
    return upper


def _av_tickers(value: str) -> str:
    """Translate a comma-separated project ticker list for Alpha Vantage.

    Args:
        value: One ticker or a comma-separated list, e.g. ``AAPL.US,MSFT``.

    Returns:
        The same list with each entry passed through :func:`_av_symbol`.
    """
    parts = [part.strip() for part in str(value).split(",")]
    return ",".join(_av_symbol(part) for part in parts if part)


def _iso_from_compact(value: Any) -> str | None:
    """Render Alpha Vantage's ``YYYYMMDDTHHMMSS`` timestamp as ISO-8601.

    Args:
        value: Compact UTC timestamp.

    Returns:
        The ISO-8601 rendering, the raw input when unparseable, or ``None``.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%dT%H%M%S").isoformat()
    except ValueError:
        return value


@register_provider
class AlphaVantageProvider(Provider):
    """Key-gated Alpha Vantage REST adapter (one URL, many functions)."""

    name = _PROVIDER
    env_keys = (_API_KEY_ENV,)
    capabilities = {
        "core_stock_apis": "core_stock_apis",
        "news_data": "news_data",
        "news_sentiment": "news_sentiment",
        "fundamental_data": "fundamental_data",
        "technical_indicators": "technical_indicators",
        "market_movers": "market_movers",
        "risk_free_curve": "risk_free_curve",
        "macro_data": "macro_data",
        "exchange_symbols": "exchange_symbols",
    }

    def unavailable_reason(self) -> str:
        """Why this provider cannot be attempted; ``""`` when it can.

        Alpha Vantage ships a public ``demo`` key and the repo's own loader
        treats it (and the ``.env`` template placeholder) as "not configured";
        matching that here avoids spending a request on a canned response.
        """
        value = env_value(_API_KEY_ENV)
        if not value:
            return f"{_API_KEY_ENV} is not configured"
        if value.lower() in _API_KEY_PLACEHOLDERS:
            return f"{_API_KEY_ENV} is a placeholder value"
        return ""

    def _get_json(self, function: str, *, category: str, params: dict[str, Any]) -> Any:
        """Call one Alpha Vantage function and decode the JSON body.

        Args:
            function: Alpha Vantage function name (for the error message).
            category: Category the call belongs to.
            params: Query parameters, excluding ``apikey``.

        Returns:
            The decoded JSON body (an envelope is not yet rejected here).

        Raises:
            ProviderHTTPError: The key is missing, the transport failed, the
                status was not 2xx, or the body was not JSON.
        """
        token = env_value(_API_KEY_ENV)
        if not token:
            raise ProviderHTTPError(
                self.name, f"{_API_KEY_ENV} is not configured", category=category
            )
        query = dict(params)
        query["apikey"] = token
        return fetch_json(
            provider=self.name,
            category=category,
            url=_BASE_URL,
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=query,
        )

    def _extract(
        self, payload: Any, key: str, *, function: str, category: str
    ) -> Any:
        """Return ``payload[key]``, turning an in-band envelope into an error.

        Args:
            payload: Decoded JSON body.
            key: The data key this function is expected to answer under.
            function: Function name, for the error message.
            category: Category the call belongs to.

        Returns:
            The value under *key*.

        Raises:
            ProviderHTTPError: The key was absent — carrying the upstream's
                ``Error Message``/``Note``/``Information`` text when present, so
                a premium gate or a spent quota fails the chain instead of
                looking like an empty result.
        """
        data = payload.get(key) if isinstance(payload, dict) else None
        if data is not None:
            return data
        message = _envelope_message(payload)
        if message is not None:
            raise ProviderHTTPError(
                self.name, f"{function}: {message}"[:300], category=category
            )
        keys = list(payload)[:6] if isinstance(payload, dict) else type(payload).__name__
        raise ProviderHTTPError(
            self.name, f"{function}: response carried no {key!r} (got {keys!r})", category=category
        )

    def _get_csv(self, function: str, *, category: str, params: dict[str, Any]) -> str:
        """Call a CSV-only function (``LISTING_STATUS`` and friends).

        Args:
            function: Alpha Vantage function name.
            category: Category the call belongs to.
            params: Query parameters, excluding ``apikey``.

        Returns:
            The CSV body.

        Raises:
            ProviderHTTPError: The key is missing, the transport failed, the
                status was not 2xx, or the body was a JSON error envelope — a
                throttled CSV function answers with JSON, not CSV.
        """
        token = env_value(_API_KEY_ENV)
        if not token:
            raise ProviderHTTPError(
                self.name, f"{_API_KEY_ENV} is not configured", category=category
            )
        query = dict(params)
        query["apikey"] = token
        text = fetch_text(
            provider=self.name,
            category=category,
            url=_BASE_URL,
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=query,
        )
        # CSV functions still answer a JSON envelope when gated/throttled.
        if text.lstrip().startswith("{"):
            try:
                payload = json.loads(text)
            except ValueError:
                payload = None
            message = _envelope_message(payload)
            if message is not None:
                raise ProviderHTTPError(
                    self.name, f"{function}: {message}"[:300], category=category
                )
            raise ProviderHTTPError(
                self.name,
                f"{function}: expected CSV, got a JSON body",
                category=category,
            )
        return text

    # -- core_stock_apis ---------------------------------------------------

    def core_stock_apis(
        self,
        *,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        adjusted: bool = False,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Daily bars for *symbol*, ascending.

        Args:
            symbol: Project symbol, e.g. ``AAPL.US``; the ``.US`` suffix is
                stripped because Alpha Vantage carries US tickers bare.
            start: Inclusive ``YYYY-MM-DD`` lower bound; also widens the request
                to ``outputsize=full``.
            end: Inclusive ``YYYY-MM-DD`` upper bound.
            limit: Optional cap on returned rows (the most recent ones).
            adjusted: Use ``TIME_SERIES_DAILY_ADJUSTED`` (premium on this key)
                so rows carry ``adjusted_close``.

        Returns:
            Rows of ``{trade_date, open, high, low, close, volume}``, plus
            ``adjusted_close`` when *adjusted*.

        Raises:
            ProviderHTTPError: The upstream refused, or the free daily quota or
                a premium gate answered with an envelope.
        """
        function = "TIME_SERIES_DAILY_ADJUSTED" if adjusted else "TIME_SERIES_DAILY"
        payload = self._get_json(
            function,
            category="core_stock_apis",
            params={
                "function": function,
                "symbol": _av_symbol(symbol),
                # ``compact`` is the last 100 bars and the cheaper default; a
                # bounded start needs the full history to slice into.
                "outputsize": "full" if start else "compact",
            },
        )
        series = self._extract(
            payload, "Time Series (Daily)", function=function, category="core_stock_apis"
        )
        if not isinstance(series, dict):
            raise ProviderHTTPError(
                self.name, "time series was not an object", category="core_stock_apis"
            )
        rows: list[dict[str, Any]] = []
        # Alpha Vantage returns bars newest-first; the payload convention is
        # ascending, so the dates are walked in order.
        for trade_date in sorted(series):
            if start and trade_date < start:
                continue
            if end and trade_date > end:
                continue
            bar = series[trade_date]
            if not isinstance(bar, dict):
                continue
            row = {
                "trade_date": trade_date,
                "open": _num(_av_field(bar, "open")),
                "high": _num(_av_field(bar, "high")),
                "low": _num(_av_field(bar, "low")),
                "close": _num(_av_field(bar, "close")),
                "volume": _num(_av_field(bar, "volume")),
            }
            if adjusted:
                row["adjusted_close"] = _num(_av_field(bar, "adjusted close"))
            rows.append(row)
        if limit and limit > 0:
            rows = rows[-int(limit) :]
        return rows

    # -- news_data ---------------------------------------------------------

    def news_data(
        self,
        *,
        symbol: str | None = None,
        tickers: str | None = None,
        query: str | None = None,
        limit: int = 20,
        topics: str | None = None,
        start: str | None = None,
        end: str | None = None,
        **_,
    ) -> list[dict[str, Any]]:
        """Market news articles, most recent first (``NEWS_SENTIMENT``).

        Alpha Vantage has exactly one news function, so this and
        :meth:`news_sentiment` read the same endpoint; this one keeps the
        ``news_data`` shape and the category's article fields only. The endpoint
        is premium on the configured key (measured ``Information: ... This is a
        premium endpoint ...``), so this raises there and the chain fails over —
        the slot is implemented and honest, not a phantom.

        Args:
            symbol: Single ticker filter (preferred over *tickers*), e.g.
                ``AAPL.US``; the ``.US`` suffix is stripped upstream.
            tickers: Comma-separated ticker filter the chain may pass instead.
            query: Accepted for chain compatibility. Alpha Vantage filters news
                by ticker/topic/time and has no free-text search, so a
                query-only request is refused rather than served as an
                unfiltered market feed.
            limit: Maximum articles (upstream caps at 1000).
            topics: Comma-separated topic filter, e.g. ``technology``.
            start: ``YYYYMMDDTHHMM`` lower bound (``time_from``).
            end: ``YYYYMMDDTHHMM`` upper bound (``time_to``).

        Returns:
            Rows of ``{title, url, published, source, summary}`` plus
            ``authors``/``source_domain``/``topics``/``tickers`` extras.

        Raises:
            ProviderHTTPError: A query-only request (no text search upstream),
                the premium gate, or a spent quota.
        """
        target = tickers or symbol
        if query and not target and not topics:
            raise ProviderHTTPError(
                self.name,
                "Alpha Vantage news has no free-text search; supply symbol/tickers "
                "or topics so the request can be served",
                category="news_data",
            )
        feed = self._news_feed(
            category="news_data",
            params=self._news_params(
                tickers=target, limit=limit, topics=topics, start=start, end=end
            ),
        )
        return [self._article(item, with_sentiment=False) for item in feed]

    # -- news_sentiment ----------------------------------------------------

    def news_sentiment(
        self,
        *,
        symbol: str | None = None,
        tickers: str | None = None,
        limit: int = 50,
        topics: str | None = None,
        start: str | None = None,
        end: str | None = None,
        **_,
    ) -> list[dict[str, Any]]:
        """Market news with per-article sentiment (``NEWS_SENTIMENT``).

        Premium on the configured key (measured ``Information: ... This is a
        premium endpoint ...``), so this raises there and the chain fails over.

        Args:
            symbol: Single ticker filter (preferred over *tickers*), e.g.
                ``AAPL.US``; the ``.US`` suffix is stripped upstream.
            tickers: Comma-separated ticker filter the chain may pass instead.
            limit: Maximum articles (upstream caps at 1000).
            topics: Comma-separated topic filter, e.g. ``technology``.
            start: ``YYYYMMDDTHHMM`` lower bound (``time_from``).
            end: ``YYYYMMDDTHHMM`` upper bound (``time_to``).

        Returns:
            Rows of ``{title, url, published, sentiment, score}`` plus source,
            summary and the per-ticker sentiment breakdown.

        Raises:
            ProviderHTTPError: The premium gate or a spent quota, carrying the
                upstream ``Information`` text.
        """
        feed = self._news_feed(
            category="news_sentiment",
            params=self._news_params(
                tickers=tickers or symbol, limit=limit, topics=topics, start=start, end=end
            ),
        )
        return [self._article(item, with_sentiment=True) for item in feed]

    # -- fundamental_data --------------------------------------------------

    def fundamental_data(
        self,
        *,
        symbol: str,
        statement: str = "income",
        period: str = "annual",
        include_profile: bool = False,
        **_: Any,
    ) -> dict[str, Any]:
        """Financial statements for *symbol*, plus the company overview.

        Args:
            symbol: Project symbol, e.g. ``AAPL.US``; the ``.US`` suffix is
                stripped because Alpha Vantage carries US tickers bare.
            statement: ``income``, ``balance`` or ``cash``.
            period: ``annual`` or ``quarterly``.
            include_profile: Also fetch ``OVERVIEW``. Off by default because
                ``OVERVIEW`` is premium on this key (measured) and would fail
                the whole call; the free statements stay reachable without it.

        Returns:
            ``{"symbol", "periods", "statements": {<name>: [reports]}}``, plus
            ``profile`` when requested. ``periods`` is empty because Alpha
            Vantage stamps ``fiscalDateEnding`` on every report row.

        Raises:
            ProviderHTTPError: An unknown ``statement``/``period``, a spent
                quota, or the premium gate on ``OVERVIEW``/``INCOME_STATEMENT``.
        """
        function = _STATEMENT_FUNCTIONS.get(statement)
        if function is None:
            raise ProviderHTTPError(
                self.name,
                f"unsupported statement {statement!r}; expected one of "
                f"{sorted(_STATEMENT_FUNCTIONS)}",
                category="fundamental_data",
            )
        report_key = _REPORT_KEYS.get(period)
        if report_key is None:
            raise ProviderHTTPError(
                self.name,
                f"unsupported period {period!r}; expected one of {sorted(_REPORT_KEYS)}",
                category="fundamental_data",
            )
        payload = self._get_json(
            function,
            category="fundamental_data",
            params={"function": function, "symbol": _av_symbol(symbol)},
        )
        reports = self._extract(
            payload, report_key, function=function, category="fundamental_data"
        )
        if not isinstance(reports, list):
            raise ProviderHTTPError(
                self.name, f"{report_key} was not an array", category="fundamental_data"
            )
        out: dict[str, Any] = {
            "symbol": _av_symbol(symbol),
            "periods": [],
            "statements": {_STATEMENT_KEYS[statement]: reports},
            "statement": statement,
            "period": period,
        }
        if include_profile:
            overview = self._get_json(
                "OVERVIEW",
                category="fundamental_data",
                params={"function": "OVERVIEW", "symbol": _av_symbol(symbol)},
            )
            out["profile"] = self._extract(
                overview, "Symbol", function="OVERVIEW", category="fundamental_data"
            )
            out["overview"] = overview
        return out

    # -- technical_indicators ----------------------------------------------

    def technical_indicators(
        self,
        *,
        symbol: str,
        indicator: str = "RSI",
        interval: str = "daily",
        time_period: int = 14,
        series_type: str = "close",
        fastperiod: int | None = None,
        slowperiod: int | None = None,
        signalperiod: int | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """One technical indicator series for *symbol*, ascending.

        Args:
            symbol: Project symbol, e.g. ``AAPL.US``; the ``.US`` suffix is
                stripped because Alpha Vantage carries US tickers bare.
            indicator: Alpha Vantage function name, e.g. ``RSI``, ``SMA``,
                ``MACD`` (``MACD`` and ``VWAP`` are premium on this key).
            interval: ``daily``, ``weekly``, ``monthly`` or an intraday
                ``1min``/``5min``/... interval.
            time_period: Lookback period for single-series indicators.
            series_type: ``close``, ``open``, ``high`` or ``low``.
            fastperiod: MACD fast period (only sent when given).
            slowperiod: MACD slow period (only sent when given).
            signalperiod: MACD signal period (only sent when given).
            limit: Optional cap on returned rows (the most recent ones).

        Returns:
            Rows of ``{trade_date, value}`` plus every other field the indicator
            reports (``macd_signal``, ``real_upper_band``, ...).

        Raises:
            ProviderHTTPError: The upstream refused, or the indicator is premium
                on this key.
        """
        function = str(indicator).upper()
        params: dict[str, Any] = {
            "function": function,
            "symbol": _av_symbol(symbol),
            "interval": interval,
            "series_type": series_type,
        }
        if time_period:
            params["time_period"] = int(time_period)
        for name, value in (
            ("fastperiod", fastperiod),
            ("slowperiod", slowperiod),
            ("signalperiod", signalperiod),
        ):
            if value is not None:
                params[name] = int(value)
        payload = self._get_json(function, category="technical_indicators", params=params)
        series = self._extract(
            payload,
            f"Technical Analysis: {function}",
            function=function,
            category="technical_indicators",
        )
        if not isinstance(series, dict):
            raise ProviderHTTPError(
                self.name,
                f"indicator series for {function} was not an object",
                category="technical_indicators",
            )
        rows: list[dict[str, Any]] = []
        # Alpha Vantage returns the series newest-first.
        for trade_date in sorted(series):
            fields = series[trade_date]
            if not isinstance(fields, dict):
                continue
            value = fields.get(function)
            if value is None:
                # Multi-line indicators (MACD, BBANDS, STOCH) name their main
                # series separately; the first field is that line.
                value = next(iter(fields.values()), None)
            row: dict[str, Any] = {"trade_date": trade_date, "value": _num(value)}
            row.update(
                {
                    str(name).lower().replace(" ", "_").replace("-", "_"): _num(field)
                    for name, field in fields.items()
                }
            )
            rows.append(row)
        if limit and limit > 0:
            rows = rows[-int(limit) :]
        return rows

    # -- market_movers -----------------------------------------------------

    def market_movers(
        self, *, kind: str = "gainers", limit: int | None = None, **_: Any
    ) -> list[dict[str, Any]]:
        """Top gainers / losers / most-active US tickers (``TOP_GAINERS_LOSERS``).

        Args:
            kind: ``gainers``, ``losers``, ``most_active`` or ``all``.
            limit: Optional cap on returned rows.

        Returns:
            Rows of ``{symbol, name, price, change_percent, volume}`` plus
            ``change_amount`` and the source list under ``kind``. Alpha Vantage
            does not publish a company name, so ``name`` is ``None``.

        Raises:
            ProviderHTTPError: An unknown ``kind``, a spent quota, or a refused
                request.
        """
        keys = _MOVERS_KEYS.get(kind)
        if kind not in _MOVERS_KEYS:
            raise ProviderHTTPError(
                self.name,
                f"unsupported movers kind {kind!r}; expected one of {sorted(_MOVERS_KEYS)}",
                category="market_movers",
            )
        payload = self._get_json(
            "TOP_GAINERS_LOSERS", category="market_movers", params={"function": "TOP_GAINERS_LOSERS"}
        )
        selected = _MOVERS_KEYS_ORDER if keys is None else (keys,)
        rows: list[dict[str, Any]] = []
        for key in selected:
            entries = self._extract(
                payload, key, function="TOP_GAINERS_LOSERS", category="market_movers"
            )
            if not isinstance(entries, list):
                raise ProviderHTTPError(
                    self.name, f"{key} was not an array", category="market_movers"
                )
            rows.extend(
                {
                    "symbol": entry.get("ticker"),
                    "name": None,
                    "price": _num(entry.get("price")),
                    "change_percent": _percent(entry.get("change_percentage")),
                    "volume": _num(entry.get("volume")),
                    "change_amount": _num(entry.get("change_amount")),
                    "kind": _MOVERS_KIND_BY_KEY.get(key, kind),
                }
                for entry in entries
                if isinstance(entry, dict)
            )
        if limit and limit > 0:
            rows = rows[: int(limit)]
        return rows

    # -- risk_free_curve ---------------------------------------------------

    def risk_free_curve(
        self,
        *,
        maturity: str = "10year",
        interval: str = "monthly",
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """US Treasury constant-maturity yield for one tenor (``TREASURY_YIELD``).

        Args:
            maturity: ``3month``, ``2year``, ``5year``, ``7year``, ``10year`` or
                ``30year``.
            interval: ``daily``, ``weekly`` or ``monthly``.
            limit: Optional cap on returned rows (the most recent ones).

        Returns:
            Ascending rows of ``{date, tenor, rate}`` plus the series ``unit``.

        Raises:
            ProviderHTTPError: A spent quota or a refused request.
        """
        payload = self._get_json(
            "TREASURY_YIELD",
            category="risk_free_curve",
            params={"function": "TREASURY_YIELD", "interval": interval, "maturity": maturity},
        )
        data = self._extract(
            payload, "data", function="TREASURY_YIELD", category="risk_free_curve"
        )
        rows = self._series_rows(data, category="risk_free_curve", function="TREASURY_YIELD")
        out = [
            {"date": row.get("date"), "tenor": maturity, "rate": _num(row.get("value")), "unit": payload.get("unit")}
            for row in rows
        ]
        if limit and limit > 0:
            out = out[-int(limit) :]
        return out

    # -- macro_data --------------------------------------------------------

    def macro_data(
        self,
        *,
        series: str = "FEDERAL_FUNDS_RATE",
        interval: str | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """One macro rate/index series, ascending.

        Args:
            series: ``FEDERAL_FUNDS_RATE`` or ``CPI`` — the two this adapter has
                verified against the configured key.
            interval: Upstream interval (``monthly``/``weekly``/``daily`` for
                the funds rate, ``monthly``/``semiannual`` for CPI).
            limit: Optional cap on returned rows (the most recent ones).

        Returns:
            Ascending rows of ``{date, value}`` plus the series ``name`` and
            ``unit``.

        Raises:
            ProviderHTTPError: An unverified ``series``, a spent quota, or a
                refused request.
        """
        function = str(series).upper()
        if function not in _MACRO_FUNCTIONS:
            raise ProviderHTTPError(
                self.name,
                f"unsupported macro series {series!r}; expected one of "
                f"{sorted(_MACRO_FUNCTIONS)}",
                category="macro_data",
            )
        params: dict[str, Any] = {"function": function}
        if interval:
            params["interval"] = interval
        payload = self._get_json(function, category="macro_data", params=params)
        data = self._extract(payload, "data", function=function, category="macro_data")
        rows = self._series_rows(data, category="macro_data", function=function)
        out = [
            {
                "date": row.get("date"),
                "value": _num(row.get("value")),
                "name": payload.get("name"),
                "unit": payload.get("unit"),
            }
            for row in rows
        ]
        if limit and limit > 0:
            out = out[-int(limit) :]
        return out

    # -- exchange_symbols --------------------------------------------------

    def exchange_symbols(
        self,
        *,
        state: str | None = None,
        date: str | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Every listed/delisted symbol (``LISTING_STATUS``, always CSV).

        Args:
            state: ``active`` or ``delisted`` (upstream default is ``active``).
            date: ``YYYY-MM-DD`` snapshot date (>= 2010-01-01).
            limit: Optional cap on returned rows.

        Returns:
            Rows of ``{symbol, name, exchange, type, currency}`` plus ``ipo_date``,
            ``delisting_date`` and ``status`` extras. Alpha Vantage's listing
            CSV carries no currency, so ``currency`` is ``None``.

        Raises:
            ProviderHTTPError: A spent quota (a JSON envelope where CSV was
                expected) or a refused request.
        """
        params: dict[str, Any] = {"function": "LISTING_STATUS"}
        if state:
            params["state"] = state
        if date:
            params["date"] = date
        text = self._get_csv("LISTING_STATUS", category="exchange_symbols", params=params)
        rows: list[dict[str, Any]] = []
        for record in csv.DictReader(io.StringIO(text)):
            symbol = (record.get("symbol") or "").strip()
            if not symbol:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "name": record.get("name"),
                    "exchange": record.get("exchange"),
                    "type": record.get("assetType"),
                    "currency": None,
                    "ipo_date": record.get("ipoDate"),
                    "delisting_date": record.get("delistingDate"),
                    "status": record.get("status"),
                }
            )
            if limit and limit > 0 and len(rows) >= int(limit):
                break
        return rows

    # -- shared helpers ----------------------------------------------------

    def _news_params(
        self,
        *,
        tickers: str | None,
        limit: int,
        topics: str | None,
        start: str | None,
        end: str | None,
    ) -> dict[str, Any]:
        """Build the ``NEWS_SENTIMENT`` query shared by both news categories.

        Args:
            tickers: Ticker filter (already comma-joined), or ``None``.
            limit: Maximum articles; clamped to Alpha Vantage's 1000 cap.
            topics: Comma-separated topic filter, or ``None``.
            start: ``YYYYMMDDTHHMM`` lower bound, or ``None``.
            end: ``YYYYMMDDTHHMM`` upper bound, or ``None``.

        Returns:
            The query parameters, excluding ``apikey``.
        """
        params: dict[str, Any] = {
            "function": "NEWS_SENTIMENT",
            "limit": max(1, min(int(limit), 1000)),
            "sort": "LATEST",
        }
        if tickers:
            params["tickers"] = _av_tickers(tickers)
        if topics:
            params["topics"] = topics
        if start:
            params["time_from"] = start
        if end:
            params["time_to"] = end
        return params

    def _news_feed(self, *, category: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch and unwrap the ``NEWS_SENTIMENT`` article list.

        Args:
            category: Calling category (``news_data`` or ``news_sentiment``).
            params: Query parameters from :meth:`_news_params`.

        Returns:
            The article dicts.

        Raises:
            ProviderHTTPError: The premium gate / spent quota (via
                :meth:`_extract`), or a payload whose feed is not an array.
        """
        payload = self._get_json("NEWS_SENTIMENT", category=category, params=params)
        feed = self._extract(payload, "feed", function="NEWS_SENTIMENT", category=category)
        if not isinstance(feed, list):
            raise ProviderHTTPError(
                self.name, "news feed was not an array", category=category
            )
        return [item for item in feed if isinstance(item, dict)]

    @staticmethod
    def _article(item: dict[str, Any], *, with_sentiment: bool) -> dict[str, Any]:
        """Normalise one ``NEWS_SENTIMENT`` article.

        Args:
            item: One entry of the upstream ``feed`` array.
            with_sentiment: Keep the sentiment fields; ``news_data`` rows carry
                the article shape only, ``news_sentiment`` rows carry both.

        Returns:
            A row starting with ``{title, url, published, source, summary}``.
        """
        row: dict[str, Any] = {
            "title": item.get("title"),
            "url": item.get("url"),
            "published": _iso_from_compact(item.get("time_published")),
            "source": item.get("source"),
            "summary": item.get("summary"),
            "authors": item.get("authors"),
            "source_domain": item.get("source_domain"),
            "topics": [
                entry.get("topic")
                for entry in item.get("topics") or []
                if isinstance(entry, dict)
            ],
            "tickers": [
                entry.get("ticker")
                for entry in item.get("ticker_sentiment") or []
                if isinstance(entry, dict)
            ],
        }
        if with_sentiment:
            row["sentiment"] = item.get("overall_sentiment_label")
            row["score"] = _num(item.get("overall_sentiment_score"))
        return row

    def _series_rows(self, data: Any, *, category: str, function: str) -> list[dict[str, Any]]:
        """Normalise an Alpha Vantage ``data: [{date, value}]`` block, ascending.

        Args:
            data: The extracted ``data`` value.
            category: Category the call belongs to.
            function: Function name, for the error message.

        Returns:
            Row dicts ordered oldest-first (Alpha Vantage answers newest-first).

        Raises:
            ProviderHTTPError: The block was not a list of objects.
        """
        if not isinstance(data, list):
            raise ProviderHTTPError(
                self.name, f"{function}: data block was not an array", category=category
            )
        rows = [row for row in data if isinstance(row, dict)]
        return sorted(rows, key=lambda row: str(row.get("date") or ""))
