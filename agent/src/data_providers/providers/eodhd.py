"""EODHD provider adapter.

Base URL ``https://eodhd.com/api``; one ``api_token`` query parameter for every
endpoint; ``fmt=json`` selects JSON (EODHD negotiates none of this by Accept
header). Ticker convention is ``SYMBOL.EXCHANGE`` — ``AAPL.US``, ``VTI.US``,
``GSPC.INDX``, ``EURUSD.FOREX``, ``BTC-USD.CC`` — which is also this project's
own convention for US equities, so ``.US`` symbols pass through unchanged.

Tier note (measured against the configured key on 2026-10-08): ``/eod``,
``/real-time``, ``/news`` and ``/exchange-symbol-list`` answer 200 on the
standard plan, while ``/fundamentals``, ``/intraday``, ``/technical``,
``/sentiments`` and the ``/calendar/*`` endpoints answer **403 Forbidden**.
Those are implemented anyway, and correctly — the resolver's job is to fail over
to the next provider rather than to retry or to pretend the endpoint is absent.
"""

from __future__ import annotations

from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_BASE_URL = "https://eodhd.com/api"
_API_KEY_ENV = "EODHD_API_KEY"
_HOST_KEY = "eodhd"
_MIN_INTERVAL_ENV = "VIBE_TRADING_EODHD_MIN_INTERVAL"
_DEFAULT_MIN_INTERVAL_S = 0.3


def _sentiment_label(score: Any) -> str | None:
    """Map EODHD's normalised sentiment score onto a plain label.

    Args:
        score: The ``normalized`` field, roughly ``-1..1``.

    Returns:
        ``"positive"``/``"negative"``/``"neutral"``, or ``None`` when the score
        is missing or not numeric — an unlabelled row is better than a guess.
    """
    try:
        value = float(score)
    except (TypeError, ValueError):
        return None
    if value > 0:
        return "positive"
    if value < 0:
        return "negative"
    return "neutral"


@register_provider
class EodhdProvider(Provider):
    """Key-gated EODHD REST adapter."""

    name = "eodhd"
    env_keys = (_API_KEY_ENV,)
    capabilities = {
        "core_stock_apis": "core_stock_apis",
        "news_data": "news_data",
        "news_sentiment": "news_sentiment",
        "corporate_actions": "corporate_actions",
        "fundamental_data": "fundamental_data",
        "exchange_symbols": "exchange_symbols",
    }

    def _get(
        self, path: str, *, category: str, params: dict[str, Any] | None = None
    ) -> Any:
        """Call one EODHD endpoint with the configured token."""
        from src.data_providers.base import env_value  # noqa: PLC0415

        token = env_value(_API_KEY_ENV)
        if not token:
            raise ProviderHTTPError(
                self.name, f"{_API_KEY_ENV} is not configured", category=category
            )
        query = dict(params or {})
        query.update({"api_token": token, "fmt": "json"})
        return fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}{path}",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=query,
        )

    # -- core_stock_apis ---------------------------------------------------

    def core_stock_apis(
        self,
        *,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Daily end-of-day bars for *symbol*.

        Args:
            symbol: EODHD ticker, e.g. ``AAPL.US``.
            start: Inclusive ``YYYY-MM-DD`` lower bound (``from`` upstream).
            end: Inclusive ``YYYY-MM-DD`` upper bound (``to`` upstream).
            limit: Ignored; EODHD returns the full range and the caller slices.

        Returns:
            Ascending rows of ``{trade_date, open, high, low, close,
            adjusted_close, volume}``.
        """
        params: dict[str, Any] = {}
        if start:
            params["from"] = start
        if end:
            params["to"] = end
        payload = self._get(f"/eod/{symbol.upper()}", category="core_stock_apis", params=params)
        rows = payload if isinstance(payload, list) else []
        if limit and limit > 0:
            rows = rows[-int(limit):]
        return [
            {
                "trade_date": str(row.get("date", "")),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "adjusted_close": row.get("adjusted_close"),
                "volume": row.get("volume"),
            }
            for row in rows
            if isinstance(row, dict)
        ]

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
        """Financial news, most recent first.

        Args:
            symbol: Ticker filter (``tickers`` upstream). Required: EODHD's news
                endpoint is ticker-scoped, so a query-only request is refused
                here and the chain moves to a provider that does search text.
            query: Accepted for chain compatibility. EODHD has no free-text news
                search, so a request with only ``query`` fails over.
            limit: Maximum rows requested (EODHD caps at 1000).
            start: ``YYYY-MM-DD`` lower bound.
            end: ``YYYY-MM-DD`` upper bound.

        Returns:
            Rows of ``{title, url, published, source, summary}``.

        Raises:
            ProviderHTTPError: When no ticker was supplied.
        """
        if not symbol:
            raise ProviderHTTPError(
                self.name,
                "EODHD news is ticker-scoped; a query-only request is unsupported",
                category="news_data",
            )
        params: dict[str, Any] = {"limit": max(1, min(int(limit), 1000))}
        if symbol:
            params["tickers"] = symbol.upper()
        if start:
            params["from"] = start
        if end:
            params["to"] = end
        payload = self._get("/news", category="news_data", params=params)
        rows = payload if isinstance(payload, list) else []
        return [
            {
                "title": row.get("title"),
                "url": row.get("link") or row.get("url"),
                "published": row.get("date"),
                "source": row.get("source"),
                "summary": (row.get("content") or "")[:500] or None,
            }
            for row in rows
            if isinstance(row, dict)
        ]

    # -- news_sentiment ----------------------------------------------------

    def news_sentiment(
        self, *, symbol: str, limit: int = 20, **_: Any
    ) -> list[dict[str, Any]]:
        """Per-ticker news sentiment.

        The upstream parameter is ``s`` (not ``tickers``); sending the wrong name
        yields a 422 with ``The s field is required``.

        The response is **keyed by ticker** — ``{"AAPL.US": [{...}, ...]}`` — not
        a bare array, and each entry is a *daily aggregate* (``date``, ``count``,
        ``normalized``), not an article. Articles therefore carry no title or URL,
        and ``score`` is the normalised aggregate with ``sentiment`` its sign;
        ``count`` is preserved so a caller can weigh a thin day. Treating the
        dict as "no rows" would have left the category empty and stopped the
        chain before Alpha Vantage and GDELT ever ran.

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.
            limit: Maximum rows requested (most recent first).

        Returns:
            Rows of ``{title, url, published, sentiment, score, count}``.
        """
        ticker = symbol.upper()
        payload = self._get(
            "/sentiments",
            category="news_sentiment",
            params={"s": ticker, "limit": max(1, int(limit))},
        )
        if isinstance(payload, dict):
            rows = payload.get(ticker)
            if rows is None:
                # A ticker-keyed dict that does not carry the ticker is an empty
                # answer, not a malformed one.
                rows = []
        else:
            rows = payload if isinstance(payload, list) else []
        out: list[dict[str, Any]] = []
        for row in rows[: max(1, int(limit))]:
            if not isinstance(row, dict):
                continue
            score = row.get("normalized")
            out.append(
                {
                    "title": None,
                    "url": None,
                    "published": row.get("date"),
                    "sentiment": _sentiment_label(score),
                    "score": score,
                    "count": row.get("count"),
                }
            )
        return out

    # -- corporate_actions -------------------------------------------------

    def corporate_actions(
        self, *, symbol: str, kind: str = "dividends", start: str | None = None, **_: Any
    ) -> list[dict[str, Any]]:
        """Dividends or splits for *symbol*.

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.
            kind: ``dividends`` or ``splits``.
            start: Optional ``YYYY-MM-DD`` lower bound (``filter[date_gte]``).

        Returns:
            Rows tagged with ``type`` plus the endpoint's own keys.

        Raises:
            ProviderHTTPError: For an unknown ``kind``.
        """
        if kind not in {"dividends", "splits"}:
            raise ProviderHTTPError(
                self.name, f"unsupported corporate action kind {kind!r}", category="corporate_actions"
            )
        params: dict[str, Any] = {"filter[symbol]": symbol.upper()}
        if start:
            params["filter[date_gte]"] = start
        payload = self._get(
            f"/calendar/{kind}", category="corporate_actions", params=params
        )
        rows = payload if isinstance(payload, list) else []
        return [dict(row, type=kind) for row in rows if isinstance(row, dict)]

    # -- fundamental_data --------------------------------------------------

    def fundamental_data(self, *, symbol: str, **_: Any) -> dict[str, Any]:
        """Fundamentals bundle for *symbol* (premium on the standard plan).

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.

        Returns:
            ``{"symbol": ..., "general": {...}, "highlights": {...},
            "financials": {...}}``.
        """
        payload = self._get(
            f"/fundamentals/{symbol.upper()}", category="fundamental_data"
        )
        if not isinstance(payload, dict):
            raise ProviderHTTPError(
                self.name,
                "fundamentals payload was not an object",
                category="fundamental_data",
            )
        return {
            "symbol": symbol.upper(),
            "general": payload.get("General") or {},
            "highlights": payload.get("Highlights") or {},
            "financials": payload.get("Financials") or {},
        }

    # -- exchange_symbols --------------------------------------------------

    def exchange_symbols(self, *, exchange: str = "US", **_: Any) -> list[dict[str, Any]]:
        """Every symbol listed on *exchange*.

        Args:
            exchange: EODHD exchange code, e.g. ``US``.

        Returns:
            Rows of ``{symbol, name, exchange, type, currency, isin}``.
        """
        payload = self._get(
            f"/exchange-symbol-list/{exchange.upper()}", category="exchange_symbols"
        )
        rows = payload if isinstance(payload, list) else []
        return [
            {
                "symbol": row.get("Code"),
                "name": row.get("Name"),
                "exchange": row.get("Exchange"),
                "type": row.get("Type"),
                "currency": row.get("Currency"),
                "isin": row.get("Isin"),
            }
            for row in rows
            if isinstance(row, dict)
        ]
