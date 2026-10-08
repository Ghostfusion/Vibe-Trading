"""StockData provider adapter.

Base URL ``https://api.stockdata.org``; a single ``api_token`` query parameter
gates every endpoint (StockData has no header form). Responses are JSON
(``format=csv`` is a query parameter). Every payload is an envelope:
``{"meta": {...}, "data": [...]}`` for lists, and ``{"error": {"code",
"message"}}`` for refusals — the latter arrives with a 4xx status, but this
adapter also rejects an in-band error body so a 200-plus-error envelope is never
mistaken for data. Tickers are bare US symbols (``AAPL``); the project's ``.US``
suffix is stripped.

Measured live against the configured key on 2026-10-08, with the endpoints this
adapter implements:

* ``/v1/data/eod`` — **HTTP 200**, ``data`` rows ``{date, open, high, low,
  close, volume}``, newest-first by default; ``sort=asc`` reverses it. The date
  is a full ISO timestamp (``2026-10-05T00:00:00.000Z``).
* ``/v1/data/intraday`` — **HTTP 200**, ``data`` rows ``{date, ticker,
  data: {open, high, low, close, volume, is_extended_hours}}`` (the bar is
  nested under ``data``).
* ``/v1/news/all`` — **HTTP 200**, ``data`` rows ``{uuid, title, description,
  keywords, snippet, url, image_url, language, published_at, source,
  relevance_score, entities, similar}``.
* ``/v1/entity/search`` — **HTTP 200**, ``data`` rows ``{symbol, name, type,
  industry, exchange, exchange_long, mic_code, country}``.

Endpoints deliberately **not** implemented:

* ``/v1/tickerlist`` — **HTTP 404** ``{"error":{"code":"invalid_api_endpoint",
  ...}}``; there is no such path, so nothing references it.
* ``/v1/data/splits`` and ``/v1/data/dividends`` — **HTTP 403**
  ``endpoint_access_restricted`` on this plan. They exist and are implemented
  later only if the plan changes; the free key cannot serve ``corporate_actions``
  from StockData, so the category is not declared.

Free tier: **100 requests/day**, ~2 news articles per request, 1 symbol per
intraday request, 1 month of history, and a per-60-second ceiling (the numeric
value is not published). A rate/quota refusal is HTTP 429/402 and becomes a
:class:`ProviderHTTPError` via :func:`fetch_json` — terminal, never retried.
Each method issues exactly one request.
"""

from __future__ import annotations

from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_BASE_URL = "https://api.stockdata.org"
_API_KEY_ENV = "STOCKDATA_API_KEY"
_HOST_KEY = "stockdata"
_MIN_INTERVAL_ENV = "VIBE_TRADING_STOCKDATA_MIN_INTERVAL"

# The free tier is 100 requests/day with an unpublished per-60-second ceiling;
# a mild spacing keeps a fan-out across categories from tripping it, and the
# env override lets a paid key lower it.
_DEFAULT_MIN_INTERVAL_S = 1.5

#: EOD intervals StockData accepts; intraday ones select the other endpoint.
_EOD_INTERVALS = {"day", "week", "month", "quarter", "year"}
_INTRADAY_INTERVALS = {"minute", "hour"}

#: Twelve Data / project interval spelling -> StockData EOD spelling.
_INTERVAL_ALIASES = {
    "1day": "day",
    "1d": "day",
    "d": "day",
    "1week": "week",
    "1w": "week",
    "w": "week",
    "1month": "month",
    "1mo": "month",
    "m": "month",
    "1min": "minute",
    "5min": "minute",
    "15min": "minute",
    "30min": "minute",
    "1h": "hour",
    "1hour": "hour",
}


def _sd_symbol(symbol: str) -> str:
    """Translate a project symbol into StockData's ticker convention.

    StockData carries US tickers bare, so a ``.US`` suffix is stripped;
    everything else is passed through uppercased.

    Args:
        symbol: Project-side symbol, e.g. ``AAPL.US`` or a bare ``MSFT``.

    Returns:
        The bare, uppercased ticker.
    """
    upper = str(symbol).strip().upper()
    if upper.endswith(".US"):
        return upper[: -len(".US")]
    return upper


def _trade_date(value: Any) -> str:
    """Return the ``YYYY-MM-DD`` part of a StockData ISO timestamp.

    Args:
        value: ``date`` field, e.g. ``2026-10-05T00:00:00.000Z``.

    Returns:
        The date part, or the input as a string when it has no ``T``.
    """
    text = str(value or "")
    return text.split("T", 1)[0]


@register_provider
class StockDataProvider(Provider):
    """Key-gated StockData REST adapter."""

    name = "stockdata"
    env_keys = (_API_KEY_ENV,)
    capabilities = {
        "core_stock_apis": "core_stock_apis",
        "news_data": "news_data",
        "exchange_symbols": "exchange_symbols",
    }

    def _get(
        self, path: str, *, category: str, params: dict[str, Any] | None = None
    ) -> Any:
        """Call one StockData endpoint with the configured token.

        Args:
            path: Endpoint path, e.g. ``/v1/data/eod``.
            category: Category the call belongs to, for error reporting.
            params: Query parameters (the token is added here).

        Returns:
            The decoded JSON body.

        Raises:
            ProviderHTTPError: When the token is unconfigured, the upstream
                refused, or the body carries an in-band error envelope.
        """
        from src.data_providers.base import env_value  # noqa: PLC0415

        token = env_value(_API_KEY_ENV)
        if not token:
            raise ProviderHTTPError(
                self.name, f"{_API_KEY_ENV} is not configured", category=category
            )
        query = dict(params or {})
        query["api_token"] = token
        payload = fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}{path}",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=query,
        )
        if isinstance(payload, dict):
            err = payload.get("error")
            if "error" in payload:
                if isinstance(err, dict):
                    code = err.get("code")
                    message = err.get("message", "")
                else:
                    code, message = None, str(err)
                raise ProviderHTTPError(
                    self.name,
                    f"upstream error {code}: {message}".strip(),
                    category=category,
                )
            if str(payload.get("status", "")).lower() == "error":
                raise ProviderHTTPError(
                    self.name,
                    f"upstream error: {payload.get('message', '')}".strip(),
                    category=category,
                )
            meta = payload.get("meta")
            if isinstance(meta, dict) and meta.get("error"):
                raise ProviderHTTPError(
                    self.name,
                    f"upstream error: {meta['error']}",
                    category=category,
                )
        return payload

    def _data_rows(self, payload: Any, category: str) -> list[dict[str, Any]]:
        """Return the ``data`` list of a StockData envelope, or refuse.

        A body without a ``data`` list is not an empty answer: it is a shape
        this adapter does not model (a provider-native refusal, say), so the
        chain must fail over rather than treat it as a successful empty result.
        An empty list is returned only when the upstream genuinely sent one.

        Args:
            payload: Decoded JSON body.
            category: Category the call belongs to.

        Returns:
            The row objects (non-mapping entries dropped).

        Raises:
            ProviderHTTPError: When the body has no list at ``data``.
        """
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return [row for row in payload["data"] if isinstance(row, dict)]
        raise ProviderHTTPError(
            self.name,
            f"unexpected {self.name} payload for {category!r}: "
            "expected a list at 'data'",
            category=category,
        )

    # -- core_stock_apis ---------------------------------------------------

    def core_stock_apis(
        self,
        *,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        interval: str = "day",
        **_: Any,
    ) -> list[dict[str, Any]]:
        """EOD or intraday bars for *symbol*.

        StockData serves the two on separate endpoints, so ``interval`` selects
        which is called: ``minute``/``hour`` hit ``/v1/data/intraday`` (bars are
        nested under ``data``), anything else hits ``/v1/data/eod``.

        Args:
            symbol: Ticker, e.g. ``AAPL`` or ``AAPL.US``.
            start: Inclusive ``YYYY-MM-DD`` lower bound (``date_from``).
            end: Inclusive ``YYYY-MM-DD`` upper bound (``date_to``).
            limit: Maximum rows kept, counted from the most recent bar.
            interval: ``minute``/``hour`` selects intraday; otherwise an EOD
                interval (``day``, ``week``, ``month``, ``quarter``, ``year``).

        Returns:
            Ascending rows of ``{trade_date, open, high, low, close, volume}``.
        """
        raw_interval = str(interval).strip().lower()
        canonical = _INTERVAL_ALIASES.get(raw_interval, raw_interval)
        params: dict[str, Any] = {"symbols": _sd_symbol(symbol), "interval": canonical}
        if start:
            params["date_from"] = start
        if end:
            params["date_to"] = end
        if canonical in _INTRADAY_INTERVALS:
            payload = self._get("/v1/data/intraday", category="core_stock_apis", params=params)
            rows = [
                {
                    "trade_date": _trade_date(row.get("date")),
                    "open": (row.get("data") or {}).get("open"),
                    "high": (row.get("data") or {}).get("high"),
                    "low": (row.get("data") or {}).get("low"),
                    "close": (row.get("data") or {}).get("close"),
                    "volume": (row.get("data") or {}).get("volume"),
                }
                for row in self._data_rows(payload, "core_stock_apis")
                if isinstance(row.get("data"), dict)
            ]
        else:
            if canonical not in _EOD_INTERVALS:
                raise ProviderHTTPError(
                    self.name,
                    f"unsupported interval {interval!r} for StockData bars",
                    category="core_stock_apis",
                )
            payload = self._get("/v1/data/eod", category="core_stock_apis", params=params)
            rows = [
                {
                    "trade_date": _trade_date(row.get("date")),
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "volume": row.get("volume"),
                }
                for row in self._data_rows(payload, "core_stock_apis")
            ]
        rows.sort(key=lambda row: row["trade_date"])
        if limit and limit > 0:
            rows = rows[-int(limit):]
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
        """Financial news, filterable by ticker and/or free text.

        Args:
            symbol: Ticker filter (``symbols``).
            query: Free-text filter (``search``).
            limit: Maximum rows requested (the free tier caps articles per
                request well below this).
            start: ``YYYY-MM-DD`` lower bound (``published_after``).
            end: ``YYYY-MM-DD`` upper bound (``published_before``).

        Returns:
            Rows of ``{title, url, published, source, summary}``.
        """
        params: dict[str, Any] = {"limit": max(1, min(int(limit), 100))}
        if symbol:
            params["symbols"] = _sd_symbol(symbol)
        if query:
            params["search"] = query
        if start:
            params["published_after"] = start
        if end:
            params["published_before"] = end
        payload = self._get("/v1/news/all", category="news_data", params=params)
        return [
            {
                "title": row.get("title"),
                "url": row.get("url"),
                "published": row.get("published_at"),
                "source": row.get("source"),
                "summary": row.get("snippet") or row.get("description"),
            }
            for row in self._data_rows(payload, "news_data")
        ]

    # -- exchange_symbols --------------------------------------------------

    def exchange_symbols(
        self,
        *,
        symbol: str | None = None,
        query: str | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Instruments matching a ticker or free-text term.

        ``/v1/entity/search`` returns at most 50 rows per page; no term is
        required (an unfiltered call returns the first page of the catalogue).

        Args:
            symbol: Ticker filter (``symbols``).
            query: Free-text filter (``search``).
            limit: Maximum rows kept from the response.

        Returns:
            Rows of ``{symbol, name, exchange, type, currency}``.
        """
        params: dict[str, Any] = {}
        if symbol:
            params["symbols"] = _sd_symbol(symbol)
        if query:
            params["search"] = query
        payload = self._get("/v1/entity/search", category="exchange_symbols", params=params)
        out = [
            {
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "exchange": row.get("exchange"),
                "type": row.get("type"),
                "currency": row.get("currency"),
            }
            for row in self._data_rows(payload, "exchange_symbols")
        ]
        if limit and limit > 0:
            out = out[: int(limit)]
        return out
