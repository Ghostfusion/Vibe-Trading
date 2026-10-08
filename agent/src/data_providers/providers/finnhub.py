"""Finnhub provider adapter.

Base URL ``https://finnhub.io/api/v1``; the credential rides in the ``token``
query parameter for every endpoint (env name ``FINNHUB_API_KEY``). The throttle
bucket is the same ``finnhub`` host key the OHLCV loader uses, so the two layers
share one process-wide request budget instead of double-hitting the free tier.

Tier note (measured against the configured key on 2026-10-08 with
``?token=<FINNHUB_API_KEY>``). Answering 200: ``/quote``, ``/company-news``,
``/news``, ``/stock/recommendation``, ``/stock/earnings``,
``/calendar/earnings``, ``/calendar/ipo``, ``/stock/insider-transactions``,
``/stock/metric``, ``/stock/symbol``, ``/stock/profile2``,
``/fda-advisory-committee-calendar``. Answering **403**
``{"error": "You don't have access to this resource."}``:
``/stock/candle``, ``/news-sentiment``, ``/stock/price-target``,
``/stock/upgrade-downgrade``, ``/institutional/ownership``,
``/stock/option-chain``, ``/stock/short-interest``.

Two of those 403s are structural rather than tier-related and are therefore
**not implemented at all**: Finnhub's public API has no options-chain endpoint
(``/stock/option-chain`` is not in its Swagger path list) and no short-interest
endpoint. A capability that can only ever 403 is a fake chain slot, so
``options_data``/``options_surface``/``short_interest`` are left to providers
that actually publish them. ``/stock/candle`` *is* implemented — it is a real
endpoint that the configured key cannot reach — and is expected to raise so the
chain fails over.

Symbol convention: the project writes US equities as ``AAPL.US`` (EODHD's
``SYMBOL.EXCHANGE`` form) while Finnhub carries US tickers bare, so every
symbol-taking route below translates through :func:`_finnhub_symbol` — the same
rule ``backtest.loaders.finnhub_loader`` already applies. Without it Finnhub
answers 200 with an *empty* payload for the unknown ticker ``AAPL.US``.

Because of that, **an empty result from this adapter means "nothing for that
ticker and window", not "ticker verified"**: Finnhub cannot distinguish an
unknown symbol from a real symbol with no rows — both are HTTP 200 with an empty
body (measured: ``/stock/metric`` returns ``{"metric":{}...}`` for ``AAPL.US``,
``/company-news`` returns ``[]``, ``/stock/insider-transactions`` returns
``{"data":[]}``). This adapter does not fabricate a client-side ticker-validity
check; it reports the empty payload as an empty payload. Returned rows echo the
translated bare ticker (``AAPL``), matching Finnhub's own ``symbol``/``ticker``
fields, whatever project form the caller passed in.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider, env_value
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_PROVIDER = "finnhub"
_BASE_URL = "https://finnhub.io/api/v1"
_API_KEY_ENV = "FINNHUB_API_KEY"
_HOST_KEY = "finnhub"
_MIN_INTERVAL_ENV = "VIBE_TRADING_FINNHUB_MIN_INTERVAL"
_DEFAULT_MIN_INTERVAL_S = 0.4

#: Default lookback/lookahead when a caller gives no explicit window. Both
#: endpoints involved require one, so omitting it upstream would be a 400.
_DEFAULT_WINDOW_DAYS = 30
_SECONDS_PER_DAY = 86_400

#: Resolutions that carry one bar per day-or-longer, rendered as a bare date.
_DATE_ONLY_RESOLUTIONS = frozenset({"D", "W", "M"})


def _num(value: Any) -> float | None:
    """Coerce a Finnhub numeric field to ``float``.

    Args:
        value: Raw field value (numbers, numeric strings, or ``None``).

    Returns:
        The float, or ``None`` when absent/non-numeric.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _today() -> str:
    """Return today's UTC date as ``YYYY-MM-DD``."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _shift(date_str: str, days: int) -> str:
    """Shift a ``YYYY-MM-DD`` date by *days* (negative moves backwards)."""
    try:
        base = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return date_str
    return (base + timedelta(days=days)).strftime("%Y-%m-%d")


def _epoch(value: Any) -> int | None:
    """Parse an epoch-second or ``YYYY-MM-DD`` bound into epoch seconds.

    Args:
        value: Integer/epoch string, or an ISO ``YYYY-MM-DD`` date.

    Returns:
        Epoch seconds (UTC midnight for a date), or ``None`` when unusable.
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    try:
        parsed = datetime.strptime(str(value)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return int(parsed.timestamp())


def _iso_from_epoch(value: Any, *, resolution: str = "D") -> str | None:
    """Render an epoch-second timestamp as the payload's ISO-8601 date.

    Args:
        value: Epoch seconds.
        resolution: Bar resolution; day-or-longer keeps the bare date.

    Returns:
        ``YYYY-MM-DD`` for daily-weeks-monthly bars, full ISO-8601 otherwise,
        or ``None`` when the timestamp is unusable.
    """
    if value is None:
        return None
    try:
        moment = datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return None
    if resolution.upper() in _DATE_ONLY_RESOLUTIONS:
        return moment.strftime("%Y-%m-%d")
    return moment.isoformat()


def _at(values: Any, index: int) -> Any:
    """Return ``values[index]`` from Finnhub's parallel arrays, else ``None``."""
    if isinstance(values, list) and 0 <= index < len(values):
        return values[index]
    return None


def _finnhub_symbol(code: str) -> str:
    """Translate a project symbol into Finnhub's ticker convention.

    The project writes US equities as ``AAPL.US`` while Finnhub carries US
    tickers bare; any other suffix is a global exchange code (``AC.TO``) and
    passes through. This mirrors ``backtest.loaders.finnhub_loader``, and it is
    not cosmetic: Finnhub answers HTTP 200 with an *empty* payload for the
    unknown ticker ``AAPL.US``, so skipping the translation looks like a
    successful "no data" rather than a wrong request.

    Args:
        code: Project-side symbol, e.g. ``AAPL.US`` or a bare ``MSFT``.

    Returns:
        The Finnhub ticker: ``.US`` stripped, otherwise uppercased unchanged.
    """
    upper = code.strip().upper()
    if upper.endswith(".US"):
        return upper[: -len(".US")]
    return upper


@register_provider
class FinnhubProvider(Provider):
    """Key-gated Finnhub REST adapter."""

    name = _PROVIDER
    env_keys = (_API_KEY_ENV,)
    capabilities = {
        "core_stock_apis": "core_stock_apis",
        "news_data": "news_data",
        "fundamental_data": "fundamental_data",
        "analyst_ratings": "analyst_ratings",
        "earnings_calendar": "earnings_calendar",
        "smart_money": "smart_money",
        "exchange_symbols": "exchange_symbols",
    }

    def _get(self, path: str, *, category: str, params: dict[str, Any] | None = None) -> Any:
        """Call one Finnhub endpoint with the configured token.

        Args:
            path: Route below ``/api/v1``, e.g. ``/stock/recommendation``.
            category: Category the call belongs to, for error reporting.
            params: Extra query parameters.

        Returns:
            The decoded JSON body.

        Raises:
            ProviderHTTPError: The key is missing, the transport failed, the
                upstream refused (403/429/...), the body was not JSON, or a 200
                body carried an in-band ``{"error": ...}`` envelope.
        """
        token = env_value(_API_KEY_ENV)
        if not token:
            raise ProviderHTTPError(
                self.name, f"{_API_KEY_ENV} is not configured", category=category
            )
        query = dict(params or {})
        query["token"] = token
        payload = fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}{path}",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=query,
        )
        # Finnhub also answers 200 with ``{"error": ...}`` for some bad
        # requests; that must fail the chain rather than parse as data.
        if isinstance(payload, dict) and payload.get("error"):
            raise ProviderHTTPError(
                self.name, str(payload["error"])[:200], category=category, status=200
            )
        return payload

    # -- core_stock_apis ---------------------------------------------------

    def core_stock_apis(
        self,
        *,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        resolution: str = "D",
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Candles for *symbol* (``/stock/candle``).

        Implemented knowing it answers **403** on this key: the chain must fail
        over rather than the endpoint be silently absent. When the key does have
        access, the response is parallel arrays (``t``/``o``/``h``/``l``/``c``/
        ``v``) plus a status field ``s`` that reads ``"ok"`` on a hit.

        Args:
            symbol: Project symbol, e.g. ``AAPL.US``; the ``.US`` suffix is
                stripped because Finnhub carries US tickers bare.
            start: Epoch seconds or ``YYYY-MM-DD`` lower bound.
            end: Epoch seconds or ``YYYY-MM-DD`` upper bound.
            limit: Optional cap on returned rows (the most recent ones).
            resolution: ``1``/``5``/``15``/``30``/``60``/``D``/``W``/``M``.

        Returns:
            Ascending rows of ``{trade_date, open, high, low, close, volume}``.

        Raises:
            ProviderHTTPError: On the tier's 403 (or any other refusal), and on
            a 200 body whose shape is not a candle payload.
        """
        to_seconds = _epoch(end)
        if to_seconds is None:
            to_seconds = _epoch(_today())
        from_seconds = _epoch(start)
        if from_seconds is None:
            from_seconds = (to_seconds or 0) - _DEFAULT_WINDOW_DAYS * _SECONDS_PER_DAY
        payload = self._get(
            "/stock/candle",
            category="core_stock_apis",
            params={
                "symbol": _finnhub_symbol(symbol),
                "resolution": str(resolution).upper(),
                "from": from_seconds,
                "to": to_seconds,
            },
        )
        if not isinstance(payload, dict) or "s" not in payload:
            raise ProviderHTTPError(
                self.name, "candle response carried no status field", category="core_stock_apis"
            )
        if payload.get("s") != "ok":
            # ``no_data`` means the window genuinely holds no bars.
            return []
        timestamps = payload.get("t") or []
        rows: list[dict[str, Any]] = []
        for index, epoch in enumerate(timestamps):
            values = {
                field: _num(_at(payload.get(key), index))
                for key, field in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"))
            }
            if any(values[field] is None for field in ("open", "high", "low", "close")):
                # A gap in any OHLC slot is dropped, matching the loader.
                continue
            rows.append(
                {
                    "trade_date": _iso_from_epoch(epoch, resolution=str(resolution)),
                    "open": values["open"],
                    "high": values["high"],
                    "low": values["low"],
                    "close": values["close"],
                    "volume": _num(_at(payload.get("v"), index)),
                }
            )
        if limit and limit > 0:
            rows = rows[-int(limit) :]
        return rows

    # -- news_data ---------------------------------------------------------

    def news_data(
        self,
        *,
        symbol: str | None = None,
        query: str | None = None,
        limit: int = 20,
        start: str | None = None,
        end: str | None = None,
        **_,
    ) -> list[dict[str, Any]]:
        """Company news, most recent first (``/company-news``).

        Args:
            symbol: Ticker filter, e.g. ``AAPL.US`` (``.US`` stripped upstream).
                Required — ``/company-news`` is ticker-scoped and its ``/news``
                sibling is a market feed with no symbol filter, so a query-only
                request fails over instead.
            query: Accepted for chain compatibility; Finnhub has no free-text
                news search.
            limit: Maximum rows returned (the upstream window is unbounded).
            start: ``YYYY-MM-DD`` lower bound (defaults to 30 days back).
            end: ``YYYY-MM-DD`` upper bound (defaults to today).

        Returns:
            Rows of ``{title, url, published, source, summary}`` plus ``id`` and
            ``related`` extras.

        Raises:
            ProviderHTTPError: When no ticker was supplied.
        """
        if not symbol:
            raise ProviderHTTPError(
                self.name,
                "Finnhub company news is ticker-scoped; a query-only news search is unsupported",
                category="news_data",
            )
        end_date = end or _today()
        start_date = start or _shift(end_date, -_DEFAULT_WINDOW_DAYS)
        payload = self._get(
            "/company-news",
            category="news_data",
            params={"symbol": _finnhub_symbol(symbol), "from": start_date, "to": end_date},
        )
        if not isinstance(payload, list):
            raise ProviderHTTPError(
                self.name, "company-news payload was not an array", category="news_data"
            )
        rows = [row for row in payload if isinstance(row, dict)]
        if limit and limit > 0:
            rows = rows[: int(limit)]
        return [
            {
                "title": row.get("headline"),
                "url": row.get("url"),
                "published": _iso_from_epoch(row.get("datetime"), resolution="1"),
                "source": row.get("source"),
                "summary": row.get("summary"),
                "id": row.get("id"),
                "related": row.get("related"),
            }
            for row in rows
        ]

    # -- analyst_ratings ---------------------------------------------------

    def analyst_ratings(self, *, symbol: str, limit: int | None = None, **_: Any) -> dict[str, Any]:
        """Analyst recommendation trend for *symbol* (``/stock/recommendation``).

        Args:
            symbol: Project symbol, e.g. ``AAPL.US``; the ``.US`` suffix is
                stripped because Finnhub carries US tickers bare.
            limit: Optional cap on the number of monthly periods returned.

        Returns:
            ``{"symbol", "consensus", "summary"}`` where ``consensus`` holds the
            latest period's ``strong_buy``/``buy``/``hold``/``sell``/
            ``strong_sell`` counts, plus the full period history.

        Raises:
            ProviderHTTPError: The upstream refused or returned a non-array.
        """
        payload = self._get(
            "/stock/recommendation",
            category="analyst_ratings",
            params={"symbol": _finnhub_symbol(symbol)},
        )
        if not isinstance(payload, list):
            raise ProviderHTTPError(
                self.name, "recommendation payload was not an array", category="analyst_ratings"
            )
        rows = [row for row in payload if isinstance(row, dict)]
        if limit and limit > 0:
            rows = rows[: int(limit)]
        if not rows:
            return {"symbol": _finnhub_symbol(symbol), "consensus": {}, "summary": "", "history": []}
        latest = rows[0]
        consensus = {
            "strong_buy": latest.get("strongBuy"),
            "buy": latest.get("buy"),
            "hold": latest.get("hold"),
            "sell": latest.get("sell"),
            "strong_sell": latest.get("strongSell"),
        }
        total = sum(value for value in consensus.values() if isinstance(value, (int, float)))
        return {
            "symbol": _finnhub_symbol(symbol),
            "consensus": consensus,
            "summary": f"recommendation trend for {latest.get('period') or 'latest period'}"
            f" from {total} analyst(s)",
            "period": latest.get("period"),
            "history": [
                {
                    "period": row.get("period"),
                    "strong_buy": row.get("strongBuy"),
                    "buy": row.get("buy"),
                    "hold": row.get("hold"),
                    "sell": row.get("sell"),
                    "strong_sell": row.get("strongSell"),
                }
                for row in rows
            ],
        }

    # -- earnings_calendar -------------------------------------------------

    def earnings_calendar(
        self,
        *,
        symbol: str | None = None,
        limit: int | None = None,
        start: str | None = None,
        end: str | None = None,
        **_,
    ) -> list[dict[str, Any]]:
        """Earnings dates with estimates (and actuals once reported).

        Two upstream routes, picked by request shape:

        * ``symbol`` with no window asks ``/stock/earnings`` for that ticker's
          recent quarters (the endpoint is symbol-scoped and takes no window).
        * otherwise ``/calendar/earnings`` serves the market/date window; its
          ``from``/``to`` are required, so a default 30-day forward window is
          applied when the caller gives none.

        Args:
            symbol: Ticker filter (optional), e.g. ``AAPL.US`` (``.US``
                stripped upstream).
            limit: Optional cap on returned rows.
            start: ``YYYY-MM-DD`` lower bound (calendar route only).
            end: ``YYYY-MM-DD`` upper bound (calendar route only).

        Returns:
            Rows of ``{symbol, date, eps_estimate, revenue_estimate, hour}`` plus
            actual/surprise and fiscal-period extras. When the calendar route
            carries no revenue-estimate field the key is ``None``.

        Raises:
            ProviderHTTPError: The upstream refused or returned junk.
        """
        if symbol and not start and not end:
            payload = self._get(
                "/stock/earnings",
                category="earnings_calendar",
                params={"symbol": _finnhub_symbol(symbol)},
            )
            if not isinstance(payload, list):
                raise ProviderHTTPError(
                    self.name, "earnings payload was not an array", category="earnings_calendar"
                )
            rows = [
                {
                    "symbol": row.get("symbol"),
                    "date": row.get("period"),
                    "eps_estimate": _num(row.get("estimate")),
                    "revenue_estimate": None,
                    "hour": None,
                    "eps_actual": _num(row.get("actual")),
                    "eps_surprise": _num(row.get("surprise")),
                    "eps_surprise_percent": _num(row.get("surprisePercent")),
                    "period": f"Q{row.get('quarter')}" if row.get("quarter") else None,
                    "period_year": row.get("year"),
                }
                for row in payload
                if isinstance(row, dict)
            ]
        else:
            start_date = start or _today()
            end_date = end or _shift(start_date, _DEFAULT_WINDOW_DAYS)
            params: dict[str, Any] = {"from": start_date, "to": end_date}
            if symbol:
                params["symbol"] = _finnhub_symbol(symbol)
            payload = self._get("/calendar/earnings", category="earnings_calendar", params=params)
            raw = payload.get("earningsCalendar") if isinstance(payload, dict) else payload
            if not isinstance(raw, list):
                raise ProviderHTTPError(
                    self.name,
                    "earnings calendar payload was not an array",
                    category="earnings_calendar",
                )
            rows = [
                {
                    "symbol": row.get("symbol"),
                    "date": row.get("date"),
                    "eps_estimate": _num(row.get("epsEstimate")),
                    "revenue_estimate": _num(row.get("revenueEstimate")),
                    "hour": row.get("hour") or None,
                    "eps_actual": _num(row.get("epsActual")),
                    "revenue_actual": _num(row.get("revenueActual")),
                    "period": f"Q{row.get('quarter')}" if row.get("quarter") else None,
                    "period_year": row.get("year"),
                }
                for row in raw
                if isinstance(row, dict)
            ]
        if limit and limit > 0:
            rows = rows[: int(limit)]
        return rows

    # -- fundamental_data --------------------------------------------------

    def fundamental_data(self, *, symbol: str, **_: Any) -> dict[str, Any]:
        """Basic financials and company profile for *symbol*.

        ``/stock/metric`` is a ratio snapshot and ``/stock/profile2`` a company
        record; neither is a period-by-period statement, so the documented
        ``periods``/``statements`` container stays empty and the real payloads
        are carried under ``metrics``/``profile`` rather than being forced into
        a statement shape they do not have.

        Args:
            symbol: Project symbol, e.g. ``AAPL.US``; the ``.US`` suffix is
                stripped because Finnhub carries US tickers bare.

        Returns:
            ``{"symbol", "periods", "statements", "metrics", "profile"}``.

        Raises:
            ProviderHTTPError: Either upstream call refused or returned junk.
        """
        metrics = self._get(
            "/stock/metric",
            category="fundamental_data",
            params={"symbol": _finnhub_symbol(symbol), "metric": "all"},
        )
        if not isinstance(metrics, dict):
            raise ProviderHTTPError(
                self.name, "metric payload was not an object", category="fundamental_data"
            )
        profile = self._get(
            "/stock/profile2", category="fundamental_data", params={"symbol": _finnhub_symbol(symbol)}
        )
        if not isinstance(profile, dict):
            raise ProviderHTTPError(
                self.name, "profile payload was not an object", category="fundamental_data"
            )
        return {
            "symbol": _finnhub_symbol(symbol),
            "periods": [],
            "statements": {},
            "metrics": metrics.get("metric") if isinstance(metrics.get("metric"), dict) else {},
            "metric_type": metrics.get("metricType"),
            "profile": profile,
        }

    # -- smart_money -------------------------------------------------------

    def smart_money(
        self,
        *,
        symbol: str,
        limit: int | None = None,
        start: str | None = None,
        end: str | None = None,
        **_,
    ) -> dict[str, Any]:
        """Insider transactions for *symbol* (``/stock/insider-transactions``).

        Args:
            symbol: Project symbol, e.g. ``AAPL.US``; the ``.US`` suffix is
                stripped because Finnhub carries US tickers bare.
            limit: Optional cap on returned trades (the endpoint caps at 100).
            start: ``YYYY-MM-DD`` lower bound (``from``).
            end: ``YYYY-MM-DD`` upper bound (``to``).

        Returns:
            ``{"trades": [{name, symbol, shares, change, filing_date,
            transaction_date, transaction_code, transaction_price, ...}]}``.

        Raises:
            ProviderHTTPError: The upstream refused or returned junk.
        """
        params: dict[str, Any] = {"symbol": _finnhub_symbol(symbol)}
        if start:
            params["from"] = start
        if end:
            params["to"] = end
        payload = self._get(
            "/stock/insider-transactions", category="smart_money", params=params
        )
        raw = payload.get("data") if isinstance(payload, dict) else payload
        if not isinstance(raw, list):
            raise ProviderHTTPError(
                self.name, "insider transactions payload carried no data array", category="smart_money"
            )
        trades = [
            {
                "name": row.get("name"),
                "symbol": row.get("symbol") or _finnhub_symbol(symbol),
                "shares": _num(row.get("share")),
                "change": _num(row.get("change")),
                "filing_date": row.get("filingDate"),
                "transaction_date": row.get("transactionDate"),
                "transaction_code": row.get("transactionCode"),
                "transaction_price": _num(row.get("transactionPrice")),
                "is_derivative": row.get("isDerivative"),
                "source": row.get("source"),
                "id": row.get("id"),
            }
            for row in raw
            if isinstance(row, dict)
        ]
        if limit and limit > 0:
            trades = trades[: int(limit)]
        return {"trades": trades}

    # -- exchange_symbols --------------------------------------------------

    def exchange_symbols(
        self, *, exchange: str = "US", limit: int | None = None, **_: Any
    ) -> list[dict[str, Any]]:
        """Every symbol Finnhub lists on *exchange* (``/stock/symbol``).

        The route scopes results by ``exchange`` alone: a ``symbol`` filter is
        **not honoured upstream** (it silently returns the whole exchange list),
        so this adapter never sends one and ``limit`` is what bounds the
        response. Slicing happens here, after the full list is read.

        Args:
            exchange: Finnhub exchange code, e.g. ``US``.
            limit: Optional cap on returned rows; ``None`` returns the whole
                exchange listing.

        Returns:
            Rows of ``{symbol, name, exchange, type, currency}`` plus ``mic``,
            ``display_symbol``, ``isin`` and ``figi`` extras.

        Raises:
            ProviderHTTPError: The upstream refused or returned a non-array.
        """
        code = exchange.upper()
        payload = self._get(
            "/stock/symbol", category="exchange_symbols", params={"exchange": code}
        )
        if not isinstance(payload, list):
            raise ProviderHTTPError(
                self.name, "symbol payload was not an array", category="exchange_symbols"
            )
        rows = [row for row in payload if isinstance(row, dict)]
        if limit and limit > 0:
            rows = rows[: int(limit)]
        return [
            {
                "symbol": row.get("symbol"),
                "name": row.get("description"),
                "exchange": code,
                "type": row.get("type"),
                "currency": row.get("currency"),
                "mic": row.get("mic"),
                "display_symbol": row.get("displaySymbol"),
                "isin": row.get("isin"),
                "figi": row.get("figi"),
            }
            for row in rows
        ]
