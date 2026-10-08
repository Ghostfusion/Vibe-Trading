"""Massive provider adapter.

``https://api.massive.com`` is the current brand of the Polygon.io market-data
API: same endpoint surface, same ticker conventions, same response envelope. The
credential (``MASSIVE_API_KEY``) travels either as the ``apiKey`` query
parameter or as an ``Authorization: Bearer`` header; this adapter uses the query
parameter so every call goes through one :func:`fetch_json` code path.

Ticker convention is case-sensitive and prefix-carrying: stocks are bare
(``AAPL``), options ``O:AAPL261009C00110000``, forex ``C:EURUSD``, crypto
``X:BTCUSD``, indices ``I:SPX``. The project's own US convention appends
``.US``, so ``AAPL.US`` -> ``AAPL`` here; every other symbol passes through
unchanged (a prefixed ticker must never be case-folded or suffixed).

Pagination is cursor-based: list endpoints answer with a ``next_url`` that must
be followed verbatim (it already carries the cursor). :meth:`MassiveProvider._paged`
walks it until ``limit`` rows are collected or the pages run out.

Tier note (measured against the configured key on 2026-10-08, free tier):
``/v2/aggs/ticker/{t}/prev``, ``/v2/aggs/ticker/{t}/range/...`` (recent
windows only), ``/v2/reference/news``, ``/v3/reference/tickers``,
``/v3/reference/dividends``, ``/v3/reference/splits``,
``/stocks/v1/dividends``, ``/stocks/v1/splits``,
``/v3/reference/options/contracts`` and ``/stocks/v1/short-interest`` answer
200. ``/v2/snapshot/locale/us/markets/stocks/{gainers,losers}``,
``/v3/snapshot/options/{t}`` and ``/stocks/financials/v1/*`` answer **403
NOT_AUTHORIZED / not entitled** on this key. Those are implemented anyway and
correctly — the resolver's job is to fail over to the next provider, not to
retry or to pretend the endpoint is absent.

Endpoint documentation follows the project's house style (path, auth, response
keys), matching ``backtest/loaders/finnhub_loader.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider, env_value
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_BASE_URL = "https://api.massive.com"
_API_KEY_ENV = "MASSIVE_API_KEY"
_HOST_KEY = "massive"
_MIN_INTERVAL_ENV = "VIBE_TRADING_MASSIVE_MIN_INTERVAL"
# Free tier is 5 requests/minute (per asset class); paid plans are unlimited.
# The default is deliberately above the free-tier burst so a process that fans
# out across categories does not trip the limiter, and the env override lets a
# paid key lower it.
_DEFAULT_MIN_INTERVAL_S = 1.0

# Hard cap on cursor pages walked per call. A runaway next_url loop must not
# turn one request into an unbounded crawl.
_MAX_PAGES = 10

# Interval suffix -> the (multiplier, timespan) pair the range endpoint wants.
_TIMESPAN_ALIASES: dict[str, str] = {
    "m": "minute",
    "min": "minute",
    "minute": "minute",
    "h": "hour",
    "hour": "hour",
    "d": "day",
    "day": "day",
    "wk": "week",
    "w": "week",
    "week": "week",
    "mo": "month",
    "month": "month",
    "y": "year",
    "year": "year",
}

# Financial-statement family: category key -> endpoint. Same envelope and the
# same `tickers` / `timeframe` / `limit` parameters for all three.
_STATEMENT_PATHS: dict[str, str] = {
    "income_statement": "/stocks/financials/v1/income-statements",
    "balance_sheet": "/stocks/financials/v1/balance-sheets",
    "cash_flow": "/stocks/financials/v1/cash-flow-statements",
}


def _to_massive_ticker(symbol: str) -> str:
    """Translate a project symbol into Massive's ticker convention.

    Args:
        symbol: Project-side symbol, e.g. ``AAPL.US`` or ``I:SPX``.

    Returns:
        ``AAPL`` for a ``.US`` equity; every other symbol unchanged, so a
        prefix-carrying ticker (``O:...``, ``C:...``, ``X:...``, ``I:...``) is
        never mangled.
    """
    cleaned = symbol.strip()
    if cleaned.upper().endswith(".US"):
        return cleaned[: -len(".US")].upper()
    return cleaned


def _split_interval(interval: str) -> tuple[int, str]:
    """Split an interval string into the range endpoint's multiplier/timespan.

    Args:
        interval: Interval such as ``1d``, ``5m``, ``1wk`` or ``day``.

    Returns:
        ``(multiplier, timespan)``, e.g. ``(5, "minute")``.

    Raises:
        ProviderHTTPError: The interval suffix is not a recognised timespan.
    """
    text = interval.strip().lower()
    digits = ""
    while text and text[0].isdigit():
        digits += text[0]
        text = text[1:]
    timespan = _TIMESPAN_ALIASES.get(text)
    if timespan is None:
        raise ProviderHTTPError(
            "massive",
            f"unsupported aggregate interval {interval!r}",
            category="core_stock_apis",
        )
    return (int(digits) if digits else 1), timespan


def _epoch_ms_to_date(value: Any) -> str | None:
    """Render an epoch-millisecond timestamp as an ISO ``YYYY-MM-DD`` date.

    Massive stamps daily aggregates at the session open in exchange time; the
    UTC calendar date of that instant is the trading day for every US session.
    """
    if not isinstance(value, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).date().isoformat()
    except (OverflowError, OSError, ValueError):
        return None


@register_provider
class MassiveProvider(Provider):
    """Key-gated Massive (Polygon.io) REST adapter."""

    name = "massive"
    env_keys = (_API_KEY_ENV,)
    capabilities = {
        "core_stock_apis": "core_stock_apis",
        "news_data": "news_data",
        "exchange_symbols": "exchange_symbols",
        "corporate_actions": "corporate_actions",
        "market_movers": "market_movers",
        "fundamental_data": "fundamental_data",
        "options_data": "options_data",
        "short_interest": "short_interest",
    }

    # -- transport ---------------------------------------------------------

    def _checked(self, payload: Any, *, category: str) -> Any:
        """Reject a Massive in-band error body served with HTTP 200.

        Polygon answers a refused request with ``{"status": "ERROR", "error":
        "..."}`` and HTTP 200, so :func:`fetch_json` cannot see the failure. An
        in-band error must become a :class:`ProviderHTTPError` here, both so
        the chain fails over on the first page and so a *later* cursor page
        cannot be mistaken for a completed page sequence (which would otherwise
        return a silently truncated row set).

        Args:
            payload: Decoded JSON body.
            category: Category the call belongs to.

        Returns:
            *payload* unchanged when it is a usable body.

        Raises:
            ProviderHTTPError: When the body is not a dict/list, or carries an
                ``error``, or reports ``status == "ERROR"``.
        """
        if not isinstance(payload, (dict, list)):
            raise ProviderHTTPError(
                self.name,
                f"unexpected {type(payload).__name__} response body",
                category=category,
            )
        if isinstance(payload, dict):
            if payload.get("error"):
                raise ProviderHTTPError(
                    self.name,
                    f"upstream error: {payload.get('error')}".strip(),
                    category=category,
                )
            if str(payload.get("status", "")).upper() == "ERROR":
                detail = payload.get("error") or payload.get("message") or ""
                raise ProviderHTTPError(
                    self.name,
                    f"upstream status ERROR: {detail}".strip(),
                    category=category,
                )
        return payload

    def _request(
        self, path: str, *, category: str, params: dict[str, Any] | None = None
    ) -> Any:
        """Call one Massive path with the configured key.

        Args:
            path: Path beginning with ``/`` (``_BASE_URL`` is prepended).
            category: Category the call belongs to, for the raised error.
            params: Query parameters, merged under the ``apiKey`` credential.

        Returns:
            The decoded JSON body.

        Raises:
            ProviderHTTPError: When the key is not configured, or the upstream
                refused (transport error, non-2xx, non-JSON body, or an in-band
                ``status: ERROR`` body served with HTTP 200).
        """
        token = env_value(_API_KEY_ENV)
        if not token:
            raise ProviderHTTPError(
                self.name, f"{_API_KEY_ENV} is not configured", category=category
            )
        query = dict(params or {})
        query["apiKey"] = token
        payload = fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}{path}",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=query,
        )
        return self._checked(payload, category=category)

    def _request_next(self, next_url: str, *, category: str) -> Any:
        """Follow a ``next_url`` cursor verbatim, re-attaching the credential.

        ``next_url`` already carries the cursor, so it must not be rebuilt; the
        credential is appended only when the URL does not already carry it.
        """
        token = env_value(_API_KEY_ENV)
        url = next_url
        if "apiKey=" not in url:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}apiKey={token}"
        payload = fetch_json(
            provider=self.name,
            category=category,
            url=url,
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
        )
        return self._checked(payload, category=category)

    def _paged(
        self,
        path: str,
        *,
        category: str,
        params: dict[str, Any] | None = None,
        limit: int | None = None,
        rows_key: str = "results",
    ) -> list[dict[str, Any]]:
        """Collect ``rows_key`` rows across cursor pages, up to *limit* rows.

        Args:
            path: First-page path.
            category: Category the call belongs to.
            params: First-page query parameters.
            limit: Maximum rows to return; ``None`` collects the first page only.
            rows_key: Envelope key holding the row array (``results``/``tickers``).

        Returns:
            The concatenated rows, capped at *limit*.
        """
        payload = self._request(path, category=category, params=params)
        rows: list[dict[str, Any]] = [
            row for row in (payload.get(rows_key) or []) if isinstance(row, dict)
        ]
        pages = 1
        while (
            payload.get("next_url")
            and pages < _MAX_PAGES
            and (limit is None or len(rows) < limit)
        ):
            payload = self._request_next(str(payload["next_url"]), category=category)
            rows.extend(
                row for row in (payload.get(rows_key) or []) if isinstance(row, dict)
            )
            pages += 1
        if limit is not None and limit > 0:
            return rows[:limit]
        return rows

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
        """Daily (or intraday) aggregate bars for *symbol*.

        With neither ``start`` nor ``end`` the previous session's bar is
        returned via ``/v2/aggs/ticker/{ticker}/prev``; otherwise the range
        endpoint is used. Massive stamps ``adjusted=true`` responses with
        split/dividend-adjusted OHLC, so ``adjusted_close`` mirrors ``close``.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US`` or ``I:SPX``.
            start: Inclusive ``YYYY-MM-DD`` lower bound (range endpoint only).
            end: Inclusive ``YYYY-MM-DD`` upper bound (range endpoint only).
            interval: Bar size (``1d``, ``5m``, ``1h``, ``1wk``, ``1mo``).
            limit: Keep only the most recent ``limit`` rows.

        Returns:
            Ascending rows of ``{trade_date, open, high, low, close,
            adjusted_close, volume}``.

        Raises:
            ProviderHTTPError: When the key is missing, the interval is
                unsupported, or the upstream refused (a plan whose history
                window does not cover the requested range answers 403).
        """
        ticker = _to_massive_ticker(symbol)
        if not start and not end:
            payload = self._request(
                f"/v2/aggs/ticker/{ticker}/prev", category="core_stock_apis"
            )
            raw = payload.get("results") or []
        else:
            multiplier, timespan = _split_interval(interval)
            upper = end or datetime.now(tz=timezone.utc).date().isoformat()
            lower = start or (
                datetime.fromisoformat(upper).date() - timedelta(days=365)
            ).isoformat()
            payload = self._request(
                f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{lower}/{upper}",
                category="core_stock_apis",
                params={"adjusted": "true", "sort": "asc"},
            )
            raw = payload.get("results") or []

        rows: list[dict[str, Any]] = []
        for bar in raw:
            if not isinstance(bar, dict):
                continue
            rows.append(
                {
                    "trade_date": _epoch_ms_to_date(bar.get("t")) or "",
                    "open": bar.get("o"),
                    "high": bar.get("h"),
                    "low": bar.get("l"),
                    "close": bar.get("c"),
                    "adjusted_close": bar.get("c"),
                    "volume": bar.get("v"),
                }
            )
        if limit is not None and limit > 0:
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
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Ticker-scoped financial news, most recent first.

        Args:
            symbol: Ticker filter (``ticker`` upstream). Required: Massive's
                news endpoint has no free-text ``q`` parameter, so a
                query-only request is refused here and the chain moves on.
            query: Accepted for chain compatibility; see *symbol*.
            limit: Maximum rows (Massive caps at 1000 per page).
            start: Inclusive ``published_utc`` lower bound (``YYYY-MM-DD``).
            end: Inclusive ``published_utc`` upper bound (``YYYY-MM-DD``).

        Returns:
            Rows of ``{title, url, published, source, summary}``.

        Raises:
            ProviderHTTPError: When no ticker was supplied.
        """
        if not symbol:
            raise ProviderHTTPError(
                self.name,
                "Massive news is ticker-scoped; a query-only request is unsupported",
                category="news_data",
            )
        params: dict[str, Any] = {
            "ticker": _to_massive_ticker(symbol),
            "limit": max(1, min(int(limit), 1000)),
            "order": "desc",
        }
        if start:
            params["published_utc.gte"] = start
        if end:
            params["published_utc.lte"] = end
        rows = self._paged("/v2/reference/news", category="news_data", params=params, limit=limit)
        out: list[dict[str, Any]] = []
        for row in rows:
            publisher = row.get("publisher") if isinstance(row.get("publisher"), dict) else {}
            out.append(
                {
                    "title": row.get("title"),
                    "url": row.get("article_url") or row.get("amp_url"),
                    "published": row.get("published_utc"),
                    "source": publisher.get("name"),
                    "summary": (row.get("description") or "")[:500] or None,
                }
            )
        return out

    # -- exchange_symbols --------------------------------------------------

    def exchange_symbols(
        self,
        *,
        exchange: str | None = None,
        search: str | None = None,
        type: str | None = None,
        active: bool = True,
        limit: int = 1000,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Listed symbols from the reference ticker directory.

        Args:
            exchange: Primary-exchange MIC filter, e.g. ``XNAS``.
            search: Free-text symbol/name search.
            type: Massive ticker type code, e.g. ``CS`` (common stock).
            active: Restrict to currently listed tickers.
            limit: Maximum rows requested.

        Returns:
            Rows of ``{symbol, name, exchange, type, currency}``.
        """
        params: dict[str, Any] = {
            "market": "stocks",
            "limit": max(1, min(int(limit), 1000)),
            "active": "true" if active else "false",
        }
        if exchange:
            params["exchange"] = exchange.upper()
        if search:
            params["search"] = search
        if type:
            params["type"] = type
        rows = self._paged(
            "/v3/reference/tickers",
            category="exchange_symbols",
            params=params,
            limit=limit,
        )
        return [
            {
                "symbol": row.get("ticker"),
                "name": row.get("name"),
                "exchange": row.get("primary_exchange"),
                "type": row.get("type"),
                "currency": row.get("currency_name"),
            }
            for row in rows
        ]

    # -- corporate_actions -------------------------------------------------

    def corporate_actions(
        self,
        *,
        symbol: str,
        kind: str = "dividends",
        start: str | None = None,
        end: str | None = None,
        limit: int = 1000,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Dividends or splits for *symbol*.

        Uses the ``/v3/reference/*`` forms (verified 200 on this key). Massive
        documents non-deprecated aliases at ``/stocks/v1/dividends`` and
        ``/stocks/v1/splits`` with the same envelope; either pair serves the
        category.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            kind: ``dividends`` or ``splits``.
            start: Inclusive date lower bound (ex-dividend / execution date).
            end: Inclusive date upper bound.
            limit: Maximum rows requested.

        Returns:
            Rows tagged with ``type`` plus the endpoint's own keys.

        Raises:
            ProviderHTTPError: For an unknown ``kind``.
        """
        if kind not in {"dividends", "splits"}:
            raise ProviderHTTPError(
                self.name,
                f"unsupported corporate action kind {kind!r}",
                category="corporate_actions",
            )
        if kind == "dividends":
            path = "/v3/reference/dividends"
            date_field = "ex_dividend_date"
        else:
            path = "/v3/reference/splits"
            date_field = "execution_date"
        # ``/v3/reference/*`` takes ``sort=<field>`` + ``order=asc|desc`` (the
        # ``field.asc`` suffix is rejected with 400), and defaults to the most
        # recent action first.
        params: dict[str, Any] = {
            "ticker": _to_massive_ticker(symbol),
            "limit": max(1, min(int(limit), 5000)),
            "sort": date_field,
            "order": "desc",
        }
        if start:
            params[f"{date_field}.gte"] = start
        if end:
            params[f"{date_field}.lte"] = end
        rows = self._paged(path, category="corporate_actions", params=params, limit=limit)
        # The canonical row echoes the plural request-kind token and always
        # carries a ``date`` key, sourced from this endpoint's own date field
        # (``ex_dividend_date`` for dividends, ``execution_date`` for splits).
        return [
            dict(row, type=kind, date=row.get(date_field) or row.get("date"))
            for row in rows
        ]

    # -- market_movers -----------------------------------------------------

    def market_movers(
        self,
        *,
        direction: str = "gainers",
        limit: int = 20,
        include_otc: bool = False,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Top US gainers or losers from the snapshot endpoint.

        Args:
            direction: ``gainers`` or ``losers``.
            limit: Keep only the first ``limit`` rows (the upstream returns 20).
            include_otc: Include OTC securities.

        Returns:
            Rows of ``{symbol, name, price, change_percent, volume}``.

        Raises:
            ProviderHTTPError: For an unknown ``direction``, or (on the free
                tier) the upstream's 403 "not entitled".
        """
        if direction not in {"gainers", "losers"}:
            raise ProviderHTTPError(
                self.name,
                f"unsupported market movers direction {direction!r}",
                category="market_movers",
            )
        payload = self._request(
            f"/v2/snapshot/locale/us/markets/stocks/{direction}",
            category="market_movers",
            params={"include_otc": "true" if include_otc else "false"},
        )
        rows: list[dict[str, Any]] = []
        for item in payload.get("tickers") or []:
            if not isinstance(item, dict):
                continue
            day = item.get("day") if isinstance(item.get("day"), dict) else {}
            rows.append(
                {
                    "symbol": item.get("ticker"),
                    "name": None,
                    "price": day.get("c"),
                    "change_percent": item.get("todaysChangePerc"),
                    "volume": day.get("v"),
                }
            )
        if limit > 0:
            rows = rows[: int(limit)]
        return rows

    # -- fundamental_data --------------------------------------------------

    def fundamental_data(
        self,
        *,
        symbol: str,
        timeframe: str = "annual",
        limit: int = 4,
        **_: Any,
    ) -> dict[str, Any]:
        """Financial statements for *symbol* (plan-gated on the free tier).

        Fetches the income statement, balance sheet and cash-flow statement and
        returns each statement's period rows verbatim under ``statements`` —
        field names are passed through rather than renamed, because Massive's
        financial schema is wide and versioned per endpoint.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            timeframe: ``annual``, ``quarterly`` or ``trailing_twelve_months``.
            limit: Maximum periods per statement.

        Returns:
            ``{"symbol": ..., "periods": [{"end_date", "fiscal_year",
            "fiscal_quarter", "timeframe"}], "statements": {"income_statement":
            [...], "balance_sheet": [...], "cash_flow": [...]}}``.

        Raises:
            ProviderHTTPError: When the key is missing, or (on the free tier)
                the upstream's 403 "not entitled".
        """
        ticker = _to_massive_ticker(symbol)
        page_limit = max(1, min(int(limit), 100))
        statements: dict[str, list[dict[str, Any]]] = {}
        periods: list[dict[str, Any]] = []
        for name, path in _STATEMENT_PATHS.items():
            rows = self._paged(
                path,
                category="fundamental_data",
                params={
                    "tickers": ticker,
                    "timeframe": timeframe,
                    "limit": page_limit,
                    "sort": "period_end.desc",
                },
                limit=page_limit,
            )
            statements[name] = rows
            if name == "income_statement":
                periods = [
                    {
                        "end_date": row.get("period_end"),
                        "fiscal_year": row.get("fiscal_year"),
                        "fiscal_quarter": row.get("fiscal_quarter"),
                        "timeframe": row.get("timeframe"),
                    }
                    for row in rows
                ]
        return {"symbol": ticker, "periods": periods, "statements": statements}

    # -- options_data ------------------------------------------------------

    def options_data(
        self,
        *,
        symbol: str,
        expiration: str | None = None,
        contract_type: str | None = None,
        limit: int = 250,
        **_: Any,
    ) -> dict[str, Any]:
        """Option contract reference data for *symbol*.

        Served from ``/v3/reference/options/contracts`` (verified 200 on this
        key), which carries contract metadata but no greeks/quotes. The greek
        snapshot at ``/v3/snapshot/options/{underlying}`` is plan-gated (403 on
        the free tier) and is intentionally not used here.

        Args:
            symbol: Underlying project ticker, e.g. ``AAPL.US``.
            expiration: Optional ``YYYY-MM-DD`` expiry filter.
            contract_type: Optional ``call`` or ``put`` filter.
            limit: Maximum contracts requested.

        Returns:
            ``{"expirations": [YYYY-MM-DD, ...], "contracts": [{contract_symbol,
            strike, type, expiration, exercise_style, shares_per_contract,
            underlying}]}``.
        """
        underlying = _to_massive_ticker(symbol)
        params: dict[str, Any] = {
            "underlying_ticker": underlying,
            "limit": max(1, min(int(limit), 1000)),
        }
        if expiration:
            params["expiration_date"] = expiration
        if contract_type:
            params["contract_type"] = contract_type
        rows = self._paged(
            "/v3/reference/options/contracts",
            category="options_data",
            params=params,
            limit=limit,
        )
        expirations = sorted(
            {
                str(row["expiration_date"])
                for row in rows
                if row.get("expiration_date")
            }
        )
        contracts = [
            {
                "contract_symbol": row.get("ticker"),
                "strike": row.get("strike_price"),
                "type": row.get("contract_type"),
                "expiration": row.get("expiration_date"),
                "exercise_style": row.get("exercise_style"),
                "shares_per_contract": row.get("shares_per_contract"),
                "underlying": row.get("underlying_ticker"),
            }
            for row in rows
        ]
        return {"expirations": expirations, "contracts": contracts}

    # -- short_interest ----------------------------------------------------

    def short_interest(
        self,
        *,
        symbol: str,
        limit: int = 50,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Bi-weekly FINRA short-interest settlements for *symbol*.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            limit: Maximum settlements requested (most recent first, returned
                ascending by settlement date).

        Returns:
            Ascending rows of ``{date, shares_short, short_percent,
            days_to_cover}``. Massive reports ``short_interest``,
            ``avg_daily_volume`` and ``days_to_cover`` but no share count, so
            ``short_percent`` is ``None`` rather than a fabricated ratio.
        """
        rows = self._paged(
            "/stocks/v1/short-interest",
            category="short_interest",
            params={
                "ticker": _to_massive_ticker(symbol),
                "limit": max(1, min(int(limit), 5000)),
                "sort": "settlement_date.desc",
            },
            limit=limit,
        )
        out = [
            {
                "date": row.get("settlement_date"),
                "shares_short": row.get("short_interest"),
                "short_percent": None,
                "days_to_cover": row.get("days_to_cover"),
            }
            for row in rows
        ]
        out.reverse()
        return out
