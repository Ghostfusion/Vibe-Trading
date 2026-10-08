"""yfinance (Yahoo Finance) provider adapter.

No credential: yfinance scrapes the same public JSON the finance.yahoo.com site
itself calls, so ``env_keys`` is empty and the provider is always a candidate.
Two request paths are used:

* the **yfinance library** (``Ticker.history``, ``.income_stmt``, ``.options``,
  ...), which performs Yahoo's cookie+crumb handshake internally for the
  crumb-gated ``/v10/finance/quoteSummary`` and ``/v7/finance/options``
  endpoints. The repo's ``backtest/loaders/yahoo_client.py`` implements the same
  handshake by hand; the library is used here because the adapter contract maps
  one library attribute to one category, and a missing library degrades to
  :class:`ProviderUnavailable`;
* :func:`fetch_json` for the **predefined screener**
  (``/v1/finance/screener/predefined/saved``), which is not crumb-gated and is
  verified to answer 200 — this keeps screener/movers on the shared per-host
  throttle bucket instead of the library's own session.

Symbol convention (project -> Yahoo): ``AAPL.US`` -> ``AAPL``, ``00700.HK`` ->
``0700.HK``, ``600519.SH`` -> ``600519.SS``. The mapping is
``backtest.loaders.yahoo_client.map_symbol`` so there is exactly one translation
table in the repo.

Interval semantics: ``history`` takes ``end`` as **exclusive** and supports
``period``/``interval`` (``1d``, ``1h``, ``5m``, ...); daily history is
unlimited while intraday is capped by Yahoo (1m <= 7 days, 2m-90m <= 60 days).

An empty result from a library call is treated as a failure, never as an empty
answer: a wrong symbol, a throttled session (Yahoo answers 429 by IP) and a
genuinely empty window are indistinguishable from the caller's side, so the
adapter raises :class:`ProviderHTTPError` and the chain moves on.

Measured on this machine (2026-10-08, yfinance 1.5.2 / pandas 2.3.3):
``history``, ``income_stmt``/``balance_sheet``/``cashflow``, ``recommendations``,
``analyst_price_targets``, ``calendar``/``earnings_dates``, ``options`` +
``option_chain``, ``institutional_holders``/``major_holders``,
``insider_transactions``, ``actions``/``dividends``/``splits`` and
``info['sharesShort']`` all answered. ``Ticker.news`` returned an **empty list
(0 items)** for both ``AAPL`` and ``MSFT`` from this IP, but the un-gated
``/v1/finance/search`` endpoint served headlines, so
:meth:`YfinanceProvider.news_data` falls back to it.
"""

from __future__ import annotations

import importlib
from typing import Any, Callable

import pandas as pd

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider
from src.data_providers.errors import ProviderHTTPError, ProviderUnavailable
from src.data_providers.registry import register_provider

#: Predefined screener endpoint (no crumb); ``scrIds`` selects the screen.
_SCREENER_URL = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
#: Un-gated search endpoint; its ``news`` block is the fallback news surface.
_NEWS_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"
_HOST_KEY = "yahoo"
_MIN_INTERVAL_ENV = "VIBE_TRADING_YAHOO_MIN_INTERVAL"
_DEFAULT_MIN_INTERVAL_S = 0.6

#: category direction -> Yahoo predefined screen id.
_MOVER_SCREENS: dict[str, str] = {
    "gainers": "day_gainers",
    "losers": "day_losers",
    "most_actives": "most_actives",
}

_SUPPORTED_INDICATORS = frozenset({"sma", "ema", "rsi"})

#: Intervals whose bars are one calendar session, so ``trade_date`` is a plain
#: ``YYYY-MM-DD``; anything else keeps the full timestamp.
_DAILY_INTERVALS = frozenset({"1d", "5d", "1wk", "1mo", "3mo"})


def _yf() -> Any:
    """Import yfinance lazily.

    Returns:
        The ``yfinance`` module.

    Raises:
        ProviderUnavailable: The library is not installed, so this provider can
            never serve the request.
    """
    try:
        import yfinance as yf  # noqa: PLC0415
    except ImportError as exc:
        raise ProviderUnavailable(
            "yfinance", "yfinance is not installed"
        ) from exc
    return yf


def _map_symbol(symbol: str) -> str:
    """Translate a project symbol into Yahoo's convention.

    Delegates to ``backtest.loaders.yahoo_client.map_symbol`` for the US (``.US``
    -> bare, class shares hyphenated) and Hong Kong (``00700.HK`` -> ``0700.HK``)
    rules, then adds the Shanghai suffix Yahoo spells ``.SS`` rather than the
    project's ``.SH`` (``600519.SH`` -> ``600519.SS``). Shenzhen (``.SZ``) and
    every other suffix already match Yahoo's spelling and pass through.
    """
    from backtest.loaders.yahoo_client import map_symbol  # noqa: PLC0415

    cleaned = symbol.strip()
    if cleaned.upper().endswith(".SH"):
        return f"{cleaned[: -len('.SH')]}.SS"
    return map_symbol(cleaned)


def _clean(value: Any) -> Any:
    """Coerce a pandas scalar to a JSON-friendly value (NaN/NaT -> ``None``)."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    # numpy scalars (np.float64, np.int64, np.bool_) are not JSON-serialisable;
    # .item() returns the equivalent Python native so the payload survives a
    # json.dumps without a second translation layer.
    item = getattr(value, "item", None)
    if callable(item) and not isinstance(value, (str, bytes)):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (int, float, bool, str)):
        return value
    return str(value)


def _clean_day(value: Any) -> Any:
    """Render a date-like scalar as an ISO ``YYYY-MM-DD`` (or pass it through)."""
    date_method = getattr(value, "date", None)
    if callable(date_method):
        try:
            return date_method().isoformat()
        except (ValueError, AttributeError):
            pass
    return _clean(value)


def _records(frame: Any) -> list[dict[str, Any]]:
    """Convert a DataFrame to a list of flat row dicts.

    Args:
        frame: A DataFrame (or ``None``/empty), as returned by a yfinance
            property. Version drift means the caller must not assume it exists.

    Returns:
        One dict per row, scalar values cleaned; empty for a ``None``/empty
        frame.
    """
    if frame is None or getattr(frame, "empty", True):
        return []
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        rows.append({str(column): _clean(row[column]) for column in frame.columns})
    return rows


def _normalize_news(items: Any) -> list[dict[str, Any]]:
    """Normalise Yahoo news items (either payload generation) to the schema.

    Handles the nested ``content`` shape newer yfinance versions return and the
    flat ``/v1/finance/search`` shape (``title``/``publisher``/``link``/
    ``providerPublishTime``).
    """
    rows: list[dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        content = item.get("content") if isinstance(item.get("content"), dict) else {}
        provider = (
            content.get("provider") if isinstance(content.get("provider"), dict) else {}
        )
        canonical = (
            content.get("canonicalUrl")
            if isinstance(content.get("canonicalUrl"), dict)
            else {}
        )
        published = content.get("pubDate")
        if not published and isinstance(item.get("providerPublishTime"), (int, float)):
            published = _clean(
                pd.Timestamp(item["providerPublishTime"], unit="s", tz="UTC")
            )
        rows.append(
            {
                "title": item.get("title") or content.get("title"),
                "url": item.get("link") or canonical.get("url"),
                "published": published,
                "source": item.get("publisher") or provider.get("displayName"),
                "summary": content.get("summary") or content.get("description"),
            }
        )
    return rows


def _statement_periods(frame: Any) -> list[dict[str, Any]]:
    """Transpose a statement DataFrame into one dict per period column."""
    if frame is None or getattr(frame, "empty", True):
        return []
    periods: list[dict[str, Any]] = []
    for column in frame.columns:
        record: dict[str, Any] = {"end_date": _clean_day(column)}
        for label, value in frame[column].items():
            record[str(label)] = _clean(value)
        periods.append(record)
    return periods


def _require(rows: Any, *, category: str, symbol: str, what: str) -> Any:
    """Raise when a library call produced nothing.

    An empty frame is the failure signal for yfinance: it cannot be told apart
    from a throttled session or a wrong symbol, so the chain must be allowed to
    move on instead of receiving a misleading ``[]``.

    Args:
        rows: The candidate result (list or dict).
        category: Category being served.
        symbol: The symbol requested, for the error message.
        what: Human description of the result (e.g. ``"bars"``).

    Returns:
        *rows* unchanged when non-empty.

    Raises:
        ProviderHTTPError: When *rows* is empty or ``None``.
    """
    if rows is None:
        empty = True
    elif isinstance(rows, pd.DataFrame):
        empty = rows.empty
    else:
        empty = not rows
    if empty:
        raise ProviderHTTPError(
            "yfinance",
            f"no {what} returned for {symbol!r} (wrong symbol or throttled)",
            category=category,
        )
    return rows


def _rsi(closes: pd.Series, period: int) -> pd.Series:
    """Wilder's RSI over *closes*, computed locally with pandas."""
    delta = closes.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


@register_provider
class YfinanceProvider(Provider):
    """Credential-free yfinance adapter over Yahoo Finance."""

    name = "yfinance"
    env_keys: tuple[str, ...] = ()

    def unavailable_reason(self) -> str:
        """Return why yfinance cannot be used, or ``""``.

        The library is a runtime prerequisite (mirroring moomoo's gateway
        probe), so a machine without it must be reported as unavailable
        *before* the call — the trace slot should read ``unavailable``, not
        ``failed`` at request time.
        """
        try:
            importlib.import_module("yfinance")
        except ImportError:
            return "yfinance is not installed"
        return ""

    capabilities = {
        "core_stock_apis": "core_stock_apis",
        "technical_indicators": "technical_indicators",
        "fundamental_data": "fundamental_data",
        "analyst_ratings": "analyst_ratings",
        "earnings_calendar": "earnings_calendar",
        "news_data": "news_data",
        "options_data": "options_data",
        "institution_data": "institution_data",
        "short_interest": "short_interest",
        "smart_money": "smart_money",
        "corporate_actions": "corporate_actions",
        "equity_screener": "equity_screener",
        "market_movers": "market_movers",
    }

    # -- helpers -----------------------------------------------------------

    def _ticker(self, symbol: str) -> Any:
        """Build a ``Ticker`` for *symbol* (mapped to Yahoo's convention)."""
        return _yf().Ticker(_map_symbol(symbol))

    def _call(self, category: str, symbol: str, what: str, fn: Callable[[], Any]) -> Any:
        """Run a yfinance attribute access, converting library errors.

        yfinance raises assorted exception types (its own rate-limit error, a
        ``requests`` HTTP error, ``KeyError`` on a shape change). None of them
        is a resolver-visible outcome, so they are converted here into
        :class:`ProviderHTTPError` — the failover signal — rather than escaping
        and being treated as an adapter bug.
        """
        try:
            return fn()
        except ProviderHTTPError:
            raise
        except Exception as exc:  # noqa: BLE001 - any library failure is an upstream failure
            raise ProviderHTTPError(
                self.name,
                f"{what} for {symbol!r} failed: {type(exc).__name__}: {exc}",
                category=category,
            ) from exc

    # -- core_stock_apis ---------------------------------------------------

    def core_stock_apis(
        self,
        *,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        interval: str = "1d",
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Daily (or intraday) OHLCV bars for *symbol*.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            start: Inclusive ``YYYY-MM-DD`` lower bound.
            end: **Exclusive** ``YYYY-MM-DD`` upper bound (Yahoo's semantics).
            interval: Bar size (``1d``, ``1h``, ``5m``, ...).
            limit: Keep only the most recent ``limit`` rows.

        Returns:
            Ascending rows of ``{trade_date, open, high, low, close,
            adjusted_close, volume}``.

        Raises:
            ProviderHTTPError: When Yahoo returned no bars (wrong symbol or a
                throttled session).
        """
        ticker = self._ticker(symbol)

        def _fetch() -> Any:
            if start or end:
                return ticker.history(
                    start=start, end=end, interval=interval, auto_adjust=False
                )
            return ticker.history(interval=interval, auto_adjust=False)

        frame = self._call("core_stock_apis", symbol, "history", _fetch)
        _require(
            None if frame is None else ([] if frame.empty else frame),
            category="core_stock_apis",
            symbol=symbol,
            what="bars",
        )
        daily = interval in _DAILY_INTERVALS
        rows: list[dict[str, Any]] = []
        for timestamp, bar in frame.iterrows():
            rows.append(
                {
                    "trade_date": _clean_day(timestamp) if daily else _clean(timestamp),
                    "open": _clean(bar.get("Open")),
                    "high": _clean(bar.get("High")),
                    "low": _clean(bar.get("Low")),
                    "close": _clean(bar.get("Close")),
                    "adjusted_close": _clean(bar.get("Adj Close")),
                    "volume": _clean(bar.get("Volume")),
                }
            )
        if limit is not None and limit > 0:
            rows = rows[-int(limit) :]
        return rows

    # -- technical_indicators ----------------------------------------------

    def technical_indicators(
        self,
        *,
        symbol: str,
        indicator: str = "sma",
        period: int = 20,
        start: str | None = None,
        end: str | None = None,
        interval: str = "1d",
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """SMA/EMA/RSI for *symbol*, **computed locally** with pandas.

        Yahoo serves raw OHLCV only; there is no indicator endpoint. The
        indicator is therefore computed here from
        :meth:`core_stock_apis` bars: SMA via ``rolling``, EMA via ``ewm`` and
        RSI via Wilder's smoothing (``ewm(alpha=1/period)``). Values before the
        warm-up window are ``NaN`` and are dropped.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            indicator: ``sma``, ``ema`` or ``rsi``.
            period: Look-back/window length.
            start: Inclusive ``YYYY-MM-DD`` lower bound.
            end: Exclusive ``YYYY-MM-DD`` upper bound.
            interval: Bar size used to build the series.
            limit: Keep only the most recent ``limit`` values.

        Returns:
            Ascending rows of ``{trade_date, value}`` for the indicator.

        Raises:
            ProviderHTTPError: For an unsupported indicator, no bars, or too
                few bars to fill the warm-up window.
        """
        name = indicator.strip().lower()
        if name not in _SUPPORTED_INDICATORS:
            raise ProviderHTTPError(
                self.name,
                f"unsupported technical indicator {indicator!r}",
                category="technical_indicators",
            )
        if period < 1:
            raise ProviderHTTPError(
                self.name,
                f"indicator period must be >= 1, got {period}",
                category="technical_indicators",
            )
        bars = self.core_stock_apis(
            symbol=symbol, start=start, end=end, interval=interval
        )
        closes = pd.Series(
            [bar["close"] for bar in bars],
            index=[bar["trade_date"] for bar in bars],
            dtype="float64",
        )
        if name == "sma":
            series = closes.rolling(period).mean()
        elif name == "ema":
            series = closes.ewm(span=period, adjust=False).mean()
        else:
            series = _rsi(closes, period)
        rows = [
            {"trade_date": _clean(index), "value": _clean(value)}
            for index, value in series.items()
            if not pd.isna(value)
        ]
        _require(
            rows,
            category="technical_indicators",
            symbol=symbol,
            what=f"{name}({period}) values",
        )
        if limit is not None and limit > 0:
            rows = rows[-int(limit) :]
        return rows

    # -- fundamental_data --------------------------------------------------

    def fundamental_data(self, *, symbol: str, **_: Any) -> dict[str, Any]:
        """Financial statements for *symbol*.

        Attribute access is guarded for yfinance version drift:
        ``income_stmt``/``balance_sheet``/``cashflow`` are the current names and
        ``financials``/``balancesheet``/``cashflow`` the deprecated aliases, so
        each is tried in turn.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.

        Returns:
            ``{"symbol": ..., "periods": [{"end_date", ...line items}],
            "statements": {"income_statement": [...], "balance_sheet": [...],
            "cash_flow": [...]}}`` (each statement newest period first).

        Raises:
            ProviderHTTPError: When all three statements are empty.
        """
        ticker = self._ticker(symbol)

        def _attr(*names: str) -> Any:
            for attr in names:
                frame = getattr(ticker, attr, None)
                if frame is not None and not getattr(frame, "empty", True):
                    return frame
            return None

        income = self._call(
            "fundamental_data",
            symbol,
            "income statement",
            lambda: _attr("income_stmt", "financials"),
        )
        balance = self._call(
            "fundamental_data",
            symbol,
            "balance sheet",
            lambda: _attr("balance_sheet", "balancesheet"),
        )
        cash = self._call(
            "fundamental_data",
            symbol,
            "cash flow",
            lambda: _attr("cashflow", "cash_flow"),
        )
        statements = {
            "income_statement": _statement_periods(income),
            "balance_sheet": _statement_periods(balance),
            "cash_flow": _statement_periods(cash),
        }
        _require(
            any(statements.values()),
            category="fundamental_data",
            symbol=symbol,
            what="financial statements",
        )
        periods = sorted(
            statements["income_statement"],
            key=lambda row: row.get("end_date") or "",
            reverse=True,
        )
        return {"symbol": symbol, "periods": periods, "statements": statements}

    # -- analyst_ratings ---------------------------------------------------

    def analyst_ratings(self, *, symbol: str, **_: Any) -> dict[str, Any]:
        """Analyst recommendation consensus and price-target summary.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.

        Returns:
            ``{"symbol": ..., "consensus": {"strongBuy", "buy", "hold",
            "sell", "strongSell"}, "summary": ...}``.

        Raises:
            ProviderHTTPError: When Yahoo returned neither a recommendation
                trend nor price targets.
        """
        ticker = self._ticker(symbol)
        recommendations = self._call(
            "analyst_ratings", symbol, "recommendations", lambda: ticker.recommendations
        )
        targets = self._call(
            "analyst_ratings",
            symbol,
            "analyst price targets",
            lambda: ticker.analyst_price_targets,
        )
        consensus: dict[str, Any] = {}
        if recommendations is not None and not recommendations.empty:
            frame = recommendations
            # 'period' picks the current-month row; guard for version drift
            # where the column is renamed, falling back to the first row.
            if "period" in frame.columns:
                current = frame[frame["period"] == "0m"]
                row = current.iloc[0] if not current.empty else frame.iloc[0]
            else:
                row = frame.iloc[0]
            for column in ("strongBuy", "buy", "hold", "sell", "strongSell"):
                if column in frame.columns:
                    consensus[column] = _clean(row.get(column))
        summary = ""
        if isinstance(targets, dict) and targets:
            summary = (
                f"mean target {targets.get('mean')} "
                f"(range {targets.get('low')}-{targets.get('high')})"
            )
        _require(
            consensus or summary,
            category="analyst_ratings",
            symbol=symbol,
            what="analyst ratings",
        )
        return {"symbol": symbol, "consensus": consensus, "summary": summary}

    # -- earnings_calendar -------------------------------------------------

    def earnings_calendar(
        self, *, symbol: str, limit: int = 20, **_: Any
    ) -> list[dict[str, Any]]:
        """Upcoming (and recent) earnings dates for *symbol*.

        Reads the ``calendar`` module for the scheduled date/estimates and
        falls back to the ``earnings_dates`` frame, whose shape differs between
        yfinance versions.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            limit: Maximum rows.

        Returns:
            Rows of ``{symbol, date, eps_estimate, revenue_estimate, hour}``.

        Raises:
            ProviderHTTPError: When neither surface yielded a date.
        """
        ticker = self._ticker(symbol)
        calendar = self._call(
            "earnings_calendar", symbol, "calendar", lambda: ticker.calendar
        )
        rows: list[dict[str, Any]] = []
        if isinstance(calendar, dict):
            dates = calendar.get("Earnings Date") or []
            if not isinstance(dates, list):
                dates = [dates]
            for date in dates:
                rows.append(
                    {
                        "symbol": symbol,
                        "date": _clean(date),
                        "eps_estimate": _clean(calendar.get("Earnings Average")),
                        "revenue_estimate": _clean(calendar.get("Revenue Average")),
                        "hour": None,
                    }
                )
        if not rows:
            frame = self._call(
                "earnings_calendar",
                symbol,
                "earnings dates",
                lambda: ticker.earnings_dates,
            )
            for index, row in (frame.iterrows() if frame is not None and not frame.empty else []):
                rows.append(
                    {
                        "symbol": symbol,
                        "date": _clean_day(index),
                        "eps_estimate": _clean(row.get("EPS Estimate")),
                        "revenue_estimate": None,
                        "hour": None,
                    }
                )
        _require(
            rows,
            category="earnings_calendar",
            symbol=symbol,
            what="earnings dates",
        )
        if limit > 0:
            rows = rows[: int(limit)]
        return rows

    # -- news_data ---------------------------------------------------------

    def _search_news(self, query: str, limit: int, *, category: str) -> list[dict[str, Any]]:
        """Fetch Yahoo news search hits for *query* via the un-gated search API."""
        payload = fetch_json(
            provider=self.name,
            category=category,
            url=_NEWS_SEARCH_URL,
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params={
                "q": query,
                "newsCount": max(1, min(int(limit), 50)),
                "quotesCount": 0,
            },
        )
        return [item for item in ((payload or {}).get("news") or []) if isinstance(item, dict)]

    def news_data(
        self,
        *,
        symbol: str | None = None,
        query: str | None = None,
        limit: int = 20,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """News headlines for a ticker or a free-text query.

        Two Yahoo surfaces are used, in order: the yfinance ``Ticker.news``
        property (whose payload shape differs by version — newer releases nest
        the story under ``content``, older ones keep ``title``/``link``/
        ``providerPublishTime`` flat; both are normalised), then the un-gated
        ``/v1/finance/search`` endpoint, which is what the repo's
        ``backtest/loaders/yahoo_client.search_news`` uses. On this machine
        ``Ticker.news`` returns an **empty list** for ``AAPL``/``MSFT``, so the
        search fallback is what actually serves headlines here.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            query: Free-text search string. Used instead of ``symbol`` when
                given; either ``symbol`` or ``query`` is required.
            limit: Maximum rows.

        Returns:
            Rows of ``{title, url, published, source, summary}``.

        Raises:
            ProviderHTTPError: When neither ``symbol`` nor ``query`` was
                supplied, or Yahoo returned no headlines from either surface.
        """
        if not symbol and not query:
            raise ProviderHTTPError(
                self.name,
                "news_data needs a symbol or a query",
                category="news_data",
            )
        rows: list[dict[str, Any]] = []
        if symbol and not query:
            ticker = self._ticker(symbol)
            try:
                items = self._call(
                    "news_data", symbol, "news", lambda: ticker.news
                )
            except ProviderHTTPError:
                items = []
            rows = _normalize_news(items)
        if not rows:
            raw = self._search_news(
                query or _map_symbol(symbol or ""), limit, category="news_data"
            )
            rows = _normalize_news(raw)
        _require(
            rows,
            category="news_data",
            symbol=query or symbol or "",
            what="news items",
        )
        if limit > 0:
            rows = rows[: int(limit)]
        return rows

    # -- options_data ------------------------------------------------------

    def options_data(
        self, *, symbol: str, expiration: str | None = None, **_: Any
    ) -> dict[str, Any]:
        """Option chain (one expiry) for *symbol*.

        Yahoo's ``/v7/finance/options`` endpoint is crumb-gated; yfinance
        performs the handshake and retries once on a 401. When Yahoo refuses
        anyway the library error is converted to :class:`ProviderHTTPError` so
        the chain fails over (``options_data``'s chain is ``moomoo, yfinance``).

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            expiration: Optional ``YYYY-MM-DD`` expiry; the nearest is used when
                omitted.

        Returns:
            ``{"expirations": [...], "contracts": [{contract_symbol, strike,
            type, expiration, last_price, bid, ask, volume, open_interest,
            implied_volatility, in_the_money}]}``.

        Raises:
            ProviderHTTPError: When Yahoo has no expiries, the chain is empty,
                or the crumb-gated request was refused.
        """
        ticker = self._ticker(symbol)
        expirations = self._call(
            "options_data", symbol, "option expirations", lambda: list(ticker.options or ())
        )
        _require(
            expirations,
            category="options_data",
            symbol=symbol,
            what="option expirations",
        )
        target = expiration or expirations[0]
        chain = self._call(
            "options_data",
            symbol,
            f"option chain {target}",
            lambda: ticker.option_chain(target),
        )
        contracts: list[dict[str, Any]] = []
        for side, frame in (("call", chain.calls), ("put", chain.puts)):
            for _, row in frame.iterrows():
                contracts.append(
                    {
                        "contract_symbol": row.get("contractSymbol"),
                        "strike": _clean(row.get("strike")),
                        "type": side,
                        "expiration": target,
                        "last_price": _clean(row.get("lastPrice")),
                        "bid": _clean(row.get("bid")),
                        "ask": _clean(row.get("ask")),
                        "volume": _clean(row.get("volume")),
                        "open_interest": _clean(row.get("openInterest")),
                        "implied_volatility": _clean(row.get("impliedVolatility")),
                        "in_the_money": _clean(row.get("inTheMoney")),
                    }
                )
        _require(
            contracts, category="options_data", symbol=symbol, what="option contracts"
        )
        return {"expirations": [_clean(exp) for exp in expirations], "contracts": contracts}

    # -- institution_data --------------------------------------------------

    def institution_data(self, *, symbol: str, **_: Any) -> dict[str, Any]:
        """Institutional and major-holder breakdown for *symbol*.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.

        Returns:
            ``{"symbol": ..., "institutions": [...], "major_holders": [...]}``.

        Raises:
            ProviderHTTPError: When both holder surfaces were empty.
        """
        ticker = self._ticker(symbol)
        institutions = self._call(
            "institution_data",
            symbol,
            "institutional holders",
            lambda: ticker.institutional_holders,
        )
        major = self._call(
            "institution_data",
            symbol,
            "major holders",
            lambda: ticker.major_holders,
        )
        holders = _records(institutions)
        breakdown = _records(major)
        _require(
            holders or breakdown,
            category="institution_data",
            symbol=symbol,
            what="holder data",
        )
        return {"symbol": symbol, "institutions": holders, "major_holders": breakdown}

    # -- short_interest ----------------------------------------------------

    def short_interest(self, *, symbol: str, **_: Any) -> list[dict[str, Any]]:
        """Point-in-time short-interest snapshot for *symbol*.

        Yahoo exposes short interest only as quote-summary fields, not as a
        settlement series, so a single row is returned.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.

        Returns:
            A one-row list of ``{date, shares_short, short_percent,
            days_to_cover}`` (``date`` is ``None``: the field is undated).

        Raises:
            ProviderHTTPError: When the quote summary carried no
                ``sharesShort`` (the crumb-gated call may have been refused).
        """
        ticker = self._ticker(symbol)
        info = self._call("short_interest", symbol, "quote summary", lambda: ticker.info)
        info = info if isinstance(info, dict) else {}
        shares_short = info.get("sharesShort")
        if shares_short is None:
            raise ProviderHTTPError(
                self.name,
                f"no sharesShort for {symbol!r} (crumb-gated quote summary may be refused)",
                category="short_interest",
            )
        return [
            {
                "date": None,
                "shares_short": shares_short,
                "short_percent": info.get("shortPercentOfFloat")
                or info.get("sharesShortPercentOfFloat"),
                "days_to_cover": info.get("shortRatio"),
            }
        ]

    # -- smart_money -------------------------------------------------------

    def smart_money(self, *, symbol: str, limit: int = 100, **_: Any) -> dict[str, Any]:
        """Insider transactions for *symbol*.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            limit: Maximum transactions.

        Returns:
            ``{"symbol": ..., "trades": [{Shares, Value, Insider, Position,
            Text, ...}]}``.

        Raises:
            ProviderHTTPError: When no insider transactions were returned.
        """
        ticker = self._ticker(symbol)
        frame = self._call(
            "smart_money",
            symbol,
            "insider transactions",
            lambda: ticker.insider_transactions,
        )
        trades = _records(frame)
        _require(
            trades, category="smart_money", symbol=symbol, what="insider transactions"
        )
        if limit > 0:
            trades = trades[: int(limit)]
        return {"symbol": symbol, "trades": trades}

    # -- corporate_actions -------------------------------------------------

    def corporate_actions(
        self, *, symbol: str, kind: str = "dividends", **_: Any
    ) -> list[dict[str, Any]]:
        """Dividends and/or splits for *symbol*.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            kind: ``dividends``, ``splits`` or ``all``.

        Returns:
            Rows of ``{type, date, amount|ratio}`` ascending by date.

        Raises:
            ProviderHTTPError: For an unknown ``kind``, or when the requested
                action history was empty.
        """
        if kind not in {"dividends", "splits", "all"}:
            raise ProviderHTTPError(
                self.name,
                f"unsupported corporate action kind {kind!r}",
                category="corporate_actions",
            )
        ticker = self._ticker(symbol)
        rows: list[dict[str, Any]] = []
        if kind in {"dividends", "all"}:
            dividends = self._call(
                "corporate_actions", symbol, "dividends", lambda: ticker.dividends
            )
            for index, value in (dividends.items() if dividends is not None else []):
                rows.append(
                    {"type": "dividends", "date": _clean_day(index), "amount": _clean(value)}
                )
        if kind in {"splits", "all"}:
            splits = self._call(
                "corporate_actions", symbol, "splits", lambda: ticker.splits
            )
            for index, value in (splits.items() if splits is not None else []):
                rows.append(
                    {"type": "splits", "date": _clean_day(index), "ratio": _clean(value)}
                )
        _require(
            rows, category="corporate_actions", symbol=symbol, what=f"{kind} history"
        )
        rows.sort(key=lambda row: row.get("date") or "")
        return rows

    # -- equity_screener / market_movers -----------------------------------

    def _screen(self, screen_id: str, count: int, *, category: str) -> list[dict[str, Any]]:
        """Run one predefined Yahoo screener and normalise its quotes.

        Args:
            screen_id: Yahoo ``scrIds`` value, e.g. ``day_gainers``.
            count: Maximum quotes.
            category: Category being served.

        Returns:
            Rows of ``{symbol, name, price, change_percent, volume}``.

        Raises:
            ProviderHTTPError: When Yahoo returned no quote block (an unknown
                screen id or a throttled session).
        """
        payload = fetch_json(
            provider=self.name,
            category=category,
            url=_SCREENER_URL,
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params={"scrIds": screen_id, "count": max(1, int(count))},
        )
        result = ((payload or {}).get("finance") or {}).get("result") or []
        quotes = result[0].get("quotes") if result and isinstance(result[0], dict) else None
        _require(
            quotes,
            category=category,
            symbol=screen_id,
            what="screener quotes",
        )
        return [
            {
                "symbol": quote.get("symbol"),
                "name": quote.get("shortName") or quote.get("longName"),
                "price": quote.get("regularMarketPrice"),
                "change_percent": quote.get("regularMarketChangePercent"),
                "volume": quote.get("regularMarketVolume"),
            }
            for quote in quotes
            if isinstance(quote, dict)
        ]

    def equity_screener(
        self, *, screen_id: str = "day_gainers", count: int = 25, **_: Any
    ) -> list[dict[str, Any]]:
        """Predefined Yahoo screener result set.

        Args:
            screen_id: Yahoo ``scrIds`` (``day_gainers``, ``day_losers``,
                ``most_actives``, ``undervalued_growth_stocks``, ...).
            count: Maximum rows.

        Returns:
            Rows of ``{symbol, name, price, change_percent, volume}``.

        Raises:
            ProviderHTTPError: When Yahoo returned no quotes.
        """
        return self._screen(screen_id, count, category="equity_screener")

    def market_movers(
        self, *, direction: str = "gainers", count: int = 25, **_: Any
    ) -> list[dict[str, Any]]:
        """Top gainers/losers/most-active US names.

        Args:
            direction: ``gainers``, ``losers`` or ``most_actives``.
            count: Maximum rows.

        Returns:
            Rows of ``{symbol, name, price, change_percent, volume}``.

        Raises:
            ProviderHTTPError: For an unknown ``direction``, or when Yahoo
                returned no quotes.
        """
        screen_id = _MOVER_SCREENS.get(direction.strip().lower())
        if screen_id is None:
            raise ProviderHTTPError(
                self.name,
                f"unsupported market movers direction {direction!r}",
                category="market_movers",
            )
        return self._screen(screen_id, count, category="market_movers")
