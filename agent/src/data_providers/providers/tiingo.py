"""Tiingo provider adapter.

Base URL ``https://api.tiingo.com``. Every endpoint is authenticated with the
``Authorization: Token <TIINGO_API_KEY>`` header rather than a ``token=`` query
parameter, so the credential never lands in a URL, a log line or an error
message. Tickers are bare US symbols (``AAPL``, ``BRK-B``); the project's
``AAPL.US`` convention is stripped before the request.

Endpoints used, all measured live on 2026-10-08 against the configured key:

* ``GET /tiingo/daily/{ticker}/prices`` — daily end-of-day bars (200).
* ``GET /iex?tickers={ticker}`` — real-time top-of-book quote (200).
* ``GET /tiingo/daily/{ticker}`` — ticker metadata (200).
* ``GET /tiingo/fundamentals/{ticker}/statements`` — statement periods (200).
* ``GET /tiingo/fundamentals/{ticker}/daily`` — daily valuation metrics (200).
* ``GET /tiingo/news`` — financial news. Implemented correctly and expected to
  fail on this key: Tiingo answers **403**
  ``{"detail": "You do not have permission to access the News API"}``, which is
  exactly the failover signal the resolver wants, so the adapter does not
  special-case it.

Endpoints that do **not** exist and are therefore deliberately not implemented
(all measured 404, not 403): ``/tiingo/fundamentals/{ticker}/overview``,
``/tiingo/fundamentals/{ticker}/meta`` and
``/tiingo/fundamentals/{ticker}/statements/{statementType}``. The ticker
metadata endpoint is ``/tiingo/daily/{ticker}``.

Tiingo publishes no technical-indicator endpoint — ``/tiingo/technicals/{ticker}``
and ``/tiingo/daily/{ticker}/technicals`` both answer 404, and the API's families
are EOD, IEX/BOATS realtime, Crypto, FX, Fundamentals, News, corporate actions
and symbol search — so there is no ``technical_indicators`` capability here;
none was invented. Tiingo *does* carry corporate actions
(``/tiingo/corporate-actions/{ticker}/distributions`` and ``.../splits``), but
this key answers **403** on both and the capability was outside this adapter's
scope, so it is reported rather than implemented.
"""

from __future__ import annotations

from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_BASE_URL = "https://api.tiingo.com"
_API_KEY_ENV = "TIINGO_API_KEY"
_HOST_KEY = "tiingo"
_MIN_INTERVAL_ENV = "VIBE_TRADING_TIINGO_MIN_INTERVAL"
_DEFAULT_MIN_INTERVAL_S = 0.5

#: Tiingo caps the news ``limit`` parameter at 1000.
_MAX_NEWS_LIMIT = 1000

#: Trailing daily-valuation rows kept in a fundamentals bundle. The upstream
#: series starts in 2023 and would otherwise dominate the payload.
_DEFAULT_DAILY_FUNDAMENTALS = 90

#: News descriptions run to thousands of characters; keep the bundle tool-sized.
_SUMMARY_CHARS = 500


def _iso_date(value: Any) -> str | None:
    """Return the ``YYYY-MM-DD`` head of a Tiingo timestamp, or ``None``.

    Tiingo mixes plain dates (``2026-06-27``) with ISO timestamps
    (``2026-09-25T00:00:00.000Z``); both truncate to the same calendar day.

    Args:
        value: Raw ``date`` field from any Tiingo payload.

    Returns:
        The ten-character ISO date, or ``None`` when the field is missing or
        not shaped like one (a row with no usable date is not a bar).
    """
    text = str(value if value is not None else "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return None


def _as_int(value: Any) -> int:
    """Coerce a possibly-missing fiscal-period number to ``int`` (``0``)."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


@register_provider
class TiingoProvider(Provider):
    """Key-gated Tiingo REST adapter."""

    name = "tiingo"
    env_keys = (_API_KEY_ENV,)
    capabilities = {
        "core_stock_apis": "core_stock_apis",
        "fundamental_data": "fundamental_data",
        "news_data": "news_data",
    }

    def _get(
        self, path: str, *, category: str, params: dict[str, Any] | None = None
    ) -> Any:
        """Call one Tiingo endpoint with the configured token.

        Args:
            path: Endpoint path beginning with ``/``.
            category: Category the call belongs to, for the raised error.
            params: Query parameters (never carries the token).

        Returns:
            The decoded JSON body.

        Raises:
            ProviderHTTPError: No configured ``TIINGO_API_KEY``, or the endpoint
                refused the request (Tiingo reports 401/403/404 as JSON or HTML
                under a non-2xx status).
        """
        from src.data_providers.base import env_value  # noqa: PLC0415

        token = env_value(_API_KEY_ENV)
        if not token:
            raise ProviderHTTPError(
                self.name, f"{_API_KEY_ENV} is not configured", category=category
            )
        return fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}{path}",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=params,
            headers={"Authorization": f"Token {token}"},
        )

    def _ticker(self, symbol: str, *, category: str) -> str:
        """Map a project symbol onto a Tiingo US-equity ticker.

        Args:
            symbol: Project ticker such as ``AAPL.US``, ``AAPL`` or ``BRK-B``.
            category: Category the call belongs to, for the raised error.

        Returns:
            The bare upper-case ticker Tiingo expects.

        Raises:
            ProviderHTTPError: For a blank symbol or a non-US market suffix
                (``.HK``, ``.L``, ...), which these endpoints cannot serve.
                Class-share dashes (``BRK-B``) are Tiingo's own convention and
                pass through.
        """
        upper = str(symbol or "").strip().upper()
        if upper.endswith(".US"):
            upper = upper[:-3]
        if not upper or "." in upper:
            raise ProviderHTTPError(
                self.name,
                f"{symbol!r} is not a Tiingo US-equity ticker",
                category=category,
            )
        return upper

    # -- core_stock_apis ---------------------------------------------------

    def core_stock_apis(
        self,
        *,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        kind: str = "eod",
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Daily bars for *symbol*, or its real-time IEX quote.

        Args:
            symbol: Project ticker, e.g. ``AAPL.US`` or ``AAPL``.
            start: Inclusive ``YYYY-MM-DD`` lower bound (``startDate``).
            end: Inclusive ``YYYY-MM-DD`` upper bound (``endDate``).
            limit: Keep only the most recent *limit* rows (``kind="eod"``).
            kind: ``"eod"`` for ``/tiingo/daily/{ticker}/prices``, ``"iex"`` for
                the single top-of-book row from ``/iex``. Cost is the same
                either way; IEX ignores the date window and *limit*.

        Returns:
            Ascending rows of ``{trade_date, open, high, low, close,
            adjusted_close, volume}`` for ``"eod"`` (raw close plus Tiingo's
            split/dividend-adjusted ``adjClose``, matching the documented
            normal form); a one-row list with the same keys plus live-quote
            extras for ``"iex"``.

        Raises:
            ProviderHTTPError: Unknown ``kind``, a non-US symbol, or an ``iex``
                ticker Tiingo carries no quote for (IEX answers ``[]`` for a
                ticker outside its universe, which is a symbol this endpoint
                cannot serve rather than an empty bar series).
        """
        ticker = self._ticker(symbol, category="core_stock_apis")
        if kind == "iex":
            return self._iex_row(ticker)
        if kind != "eod":
            raise ProviderHTTPError(
                self.name,
                f"unsupported core_stock_apis kind {kind!r}; use 'eod' or 'iex'",
                category="core_stock_apis",
            )

        params: dict[str, Any] = {}
        if start:
            params["startDate"] = start
        if end:
            params["endDate"] = end
        payload = self._get(
            f"/tiingo/daily/{ticker}/prices",
            category="core_stock_apis",
            params=params or None,
        )
        if not isinstance(payload, list):
            raise ProviderHTTPError(
                self.name,
                "prices payload was not a JSON array",
                category="core_stock_apis",
            )
        rows = [
            {
                "trade_date": _iso_date(row.get("date")),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "adjusted_close": row.get("adjClose"),
                "volume": row.get("volume"),
            }
            for row in payload
            if isinstance(row, dict)
        ]
        rows = [row for row in rows if row["trade_date"]]
        if limit and limit > 0:
            rows = rows[-int(limit):]
        return rows

    def _iex_row(self, ticker: str) -> list[dict[str, Any]]:
        """Return the real-time IEX top-of-book quote as a one-row bar list.

        Args:
            ticker: Bare Tiingo ticker.

        Returns:
            A single row carrying the bar keys plus bid/ask/mid/prev_close and
            the raw quote timestamp.

        Raises:
            ProviderHTTPError: The endpoint returned no quote for *ticker*.
        """
        payload = self._get(
            "/iex", category="core_stock_apis", params={"tickers": ticker}
        )
        quotes = [
            row for row in (payload if isinstance(payload, list) else []) if isinstance(row, dict)
        ]
        if not quotes:
            raise ProviderHTTPError(
                self.name,
                f"/iex returned no quote for {ticker!r}",
                category="core_stock_apis",
            )
        row = quotes[0]
        last = row.get("tngoLast")
        return [
            {
                "trade_date": _iso_date(row.get("timestamp")),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": last if last is not None else row.get("last"),
                "volume": row.get("volume"),
                "prev_close": row.get("prevClose"),
                "mid": row.get("mid"),
                "bid": row.get("bidPrice"),
                "ask": row.get("askPrice"),
                "timestamp": row.get("timestamp"),
            }
        ]

    # -- fundamental_data --------------------------------------------------

    def fundamental_data(
        self,
        *,
        symbol: str,
        daily_limit: int = _DEFAULT_DAILY_FUNDAMENTALS,
        **_: Any,
    ) -> dict[str, Any]:
        """Fundamentals bundle for *symbol*.

        Three endpoints are combined because Tiingo splits the data: statement
        periods from ``/tiingo/fundamentals/{ticker}/statements``, daily
        valuation metrics from ``/tiingo/fundamentals/{ticker}/daily``, and
        ticker metadata from ``/tiingo/daily/{ticker}`` (the same call that
        rejects an unknown ticker with HTTP 404, so a bad symbol fails over
        instead of returning an empty bundle).

        Args:
            symbol: Project ticker, e.g. ``AAPL.US``.
            daily_limit: Trailing daily-valuation rows to keep (``<= 0`` keeps
                the whole series).

        Returns:
            ``{"symbol", "periods": [{date, year, quarter}], "statements":
            {<name>: [{date, year, quarter, dataCode, value}]}, "daily": [...],
            "meta": {...}}``. ``periods`` and ``statements`` are the documented
            normal form, oldest period first; ``daily`` and ``meta`` are the
            provider extras.

        Raises:
            ProviderHTTPError: A non-US symbol, an unknown ticker (404 from the
                metadata endpoint), or a payload of the wrong JSON type.
        """
        ticker = self._ticker(symbol, category="fundamental_data")

        statements_payload = self._get(
            f"/tiingo/fundamentals/{ticker}/statements", category="fundamental_data"
        )
        if not isinstance(statements_payload, list):
            raise ProviderHTTPError(
                self.name,
                "statements payload was not a JSON array",
                category="fundamental_data",
            )
        periods, statements = _split_statements(statements_payload)

        daily_payload = self._get(
            f"/tiingo/fundamentals/{ticker}/daily", category="fundamental_data"
        )
        daily = [
            {
                "date": _iso_date(row.get("date")),
                "market_cap": row.get("marketCap"),
                "enterprise_value": row.get("enterpriseVal"),
                "pe_ratio": row.get("peRatio"),
                "pb_ratio": row.get("pbRatio"),
                "trailing_peg_1y": row.get("trailingPEG1Y"),
            }
            for row in (daily_payload if isinstance(daily_payload, list) else [])
            if isinstance(row, dict)
        ]
        if daily_limit and daily_limit > 0:
            daily = daily[-int(daily_limit):]

        meta = self._get(f"/tiingo/daily/{ticker}", category="fundamental_data")
        if not isinstance(meta, dict):
            raise ProviderHTTPError(
                self.name,
                "ticker metadata payload was not an object",
                category="fundamental_data",
            )

        return {
            "symbol": ticker,
            "periods": periods,
            "statements": statements,
            "daily": daily,
            "meta": meta,
        }

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
        """Financial news, most recent first.

        Measured against the configured key on 2026-10-08: HTTP **403**
        ``{"detail": "You do not have permission to access the News API"}``.
        That is reported as a plain :class:`ProviderHTTPError` on purpose — the
        endpoint is implemented correctly, and the entitlement is the
        operator's to fix; meanwhile the chain fails over.

        Args:
            symbol: Ticker filter (``tickers``). Required: Tiingo has no
                free-text news search, so a ``query``-only request is refused
                here and the chain moves to a provider that does search text.
            query: Accepted for chain compatibility; ignored when *symbol* is
                present, refused when it is not.
            limit: Maximum rows requested (Tiingo caps at 1000).
            start: ``YYYY-MM-DD`` lower bound (``startDate``).
            end: ``YYYY-MM-DD`` upper bound (``endDate``).

        Returns:
            Rows of ``{title, url, published, source, summary}``.

        Raises:
            ProviderHTTPError: No ticker supplied, or the key is not entitled to
                the News API (HTTP 403).
        """
        if not symbol:
            raise ProviderHTTPError(
                self.name,
                "Tiingo news is ticker-scoped; a query-only request is unsupported",
                category="news_data",
            )
        params: dict[str, Any] = {
            "tickers": self._ticker(symbol, category="news_data"),
            "limit": max(1, min(int(limit), _MAX_NEWS_LIMIT)),
        }
        if start:
            params["startDate"] = start
        if end:
            params["endDate"] = end
        payload = self._get("/tiingo/news", category="news_data", params=params)
        rows = payload if isinstance(payload, list) else []
        return [
            {
                "title": row.get("title"),
                "url": row.get("url"),
                "published": row.get("publishedDate"),
                "source": row.get("source"),
                "summary": (row.get("description") or "")[:_SUMMARY_CHARS] or None,
            }
            for row in rows
            if isinstance(row, dict)
        ]


def _split_statements(
    payload: list[Any],
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Split Tiingo statement periods into period headers and flat rows.

    The endpoint returns one object per fiscal period carrying ``date``,
    ``year``, ``quarter`` and a ``statementData`` object that maps a statement
    name (``cashFlow``, ``incomeStatement``, ``balanceSheet``, ``overview``) to
    ``{dataCode, value}`` observations. The normal form is shallow, so the
    observations are flattened to one record per (period, dataCode).

    Args:
        payload: The ``/statements`` array, newest period first upstream.

    Returns:
        ``(periods, statements)`` sorted oldest period first, where ``periods``
        is ``[{date, year, quarter}]`` and ``statements`` is
        ``{statement name: [{date, year, quarter, dataCode, value}]}``.
    """
    entries = [entry for entry in payload if isinstance(entry, dict)]
    entries.sort(
        key=lambda entry: (
            str(entry.get("date") or ""),
            _as_int(entry.get("year")),
            _as_int(entry.get("quarter")),
        )
    )

    periods: list[dict[str, Any]] = []
    statements: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        period = {
            "date": _iso_date(entry.get("date")),
            "year": entry.get("year"),
            "quarter": entry.get("quarter"),
        }
        periods.append(period)
        block = entry.get("statementData")
        if not isinstance(block, dict):
            continue
        for name, observations in block.items():
            if not isinstance(observations, list):
                continue
            target = statements.setdefault(str(name), [])
            for observation in observations:
                if not isinstance(observation, dict):
                    continue
                target.append(
                    dict(
                        period,
                        dataCode=observation.get("dataCode"),
                        value=observation.get("value"),
                    )
                )
    return periods, statements
