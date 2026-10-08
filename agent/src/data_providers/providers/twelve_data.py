"""Twelve Data provider adapter.

Base URL ``https://api.twelvedata.com``; a single ``apikey`` query parameter
gates every endpoint (the same key is accepted as an ``Authorization: apikey
<key>`` header, which the docs recommend, but this adapter keeps one credential
path so the AST env gate and the throttle layer see one shape). Responses are
JSON (``format=CSV`` is negotiated by a query parameter, never by ``Accept``).
Ticker convention is a bare symbol — ``AAPL``, ``BRK.A``, ``EUR/USD``,
``ETH/BTC`` — with the exchange chosen by a separate parameter, not a suffix,
so the project's ``.US`` suffix is stripped here (see :func:`_td_symbol`).

Measured live against the configured key on 2026-10-08, with the endpoints this
adapter implements:

* ``/time_series``, ``/rsi``, ``/sma``, ``/ema``, ``/income_statement``,
  ``/statistics``, ``/earnings``, ``/symbol_search``, ``/dividends``,
  ``/splits`` — **HTTP 200**.
* ``/stocks`` (asset catalogue) — HTTP 200, but not wired here (the request
  vocabulary for ``exchange_symbols`` is a search term, which ``/symbol_search``
  answers directly).

Endpoints deliberately **not** implemented, because they cannot serve the
category:

* ``/options/chain`` — **HTTP 404** ``{"code":404,"message":"The options
  unavailable"}``. There is no working options path, so ``options_data`` is not
  a capability.
* ``/market_movers/stocks`` — **HTTP 403** ("available exclusively with pro or
  ultra ... plans"). No working path on this key, so ``market_movers`` is not a
  capability.
* ``/news`` — **HTTP 404 page not found** (there is no free-text news search;
  the closest endpoint, ``/press_releases``, is per-ticker only). Left out.

Credits: the free tier is **8 API credits/minute** (and 800/day). A burst of 9
calls answered ``429 "You have run out of API credits for the current
minute."`` A 429 is a terminal refusal: :func:`fetch_json` turns it into
:class:`ProviderHTTPError` and the resolver fails over — this adapter never
retries. Each method below issues the smallest number of requests that can
answer its category: ``fundamental_data`` is the one exception, because the
category's normalised shape needs both the statement rows and the statistics
block, and those live on two endpoints (two credits, not one).
"""

from __future__ import annotations

from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_BASE_URL = "https://api.twelvedata.com"
_API_KEY_ENV = "TWELVEDATA_API_KEY"
_HOST_KEY = "twelve_data"
_MIN_INTERVAL_ENV = "VIBE_TRADING_TWELVEDATA_MIN_INTERVAL"

# Free tier is 8 credits/minute, so a process that fans out across categories
# must not burst. The first request in a process is immediate; later ones are
# spaced to this by the shared per-host throttle. A paid key (or a test run with
# its own env override) lowers it.
_DEFAULT_MIN_INTERVAL_S = 7.5

#: category -> indicator endpoints this adapter can drive.
_INDICATOR_PATHS: dict[str, str] = {"rsi": "rsi", "sma": "sma", "ema": "ema"}

#: indicator -> default ``time_period`` when the caller does not supply one.
_INDICATOR_DEFAULT_PERIOD: dict[str, int] = {"rsi": 14, "sma": 20, "ema": 20}

#: Dict keys a successful Twelve Data body uses to carry a payload. An error
#: envelope can arrive without a ``status`` field (``/options/chain`` answers
#: ``{"code":404,"message":"The options unavailable"}``), so a numeric ``code``
#: with none of these keys present is treated as a refusal rather than data.
_PAYLOAD_KEYS = frozenset(
    {
        "data",
        "values",
        "earnings",
        "dividends",
        "splits",
        "income_statement",
        "statistics",
    }
)


def _numeric_code(value: Any) -> bool:
    """Whether *value* is a provider-native numeric error code."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, str) and value.strip().isdigit()


def _td_symbol(symbol: str) -> str:
    """Translate a project symbol into Twelve Data's ticker convention.

    Twelve Data selects the venue with its own ``exchange``/``mic_code``
    parameter, so a ``.US`` suffix is not part of the ticker and must be
    stripped. Everything else is passed through uppercased (``BRK.A``,
    ``EUR/USD``, ``ETH/BTC``).

    Args:
        symbol: Project-side symbol, e.g. ``AAPL.US`` or a bare ``MSFT``.

    Returns:
        The bare, uppercased ticker.
    """
    upper = str(symbol).strip().upper()
    if upper.endswith(".US"):
        return upper[: -len(".US")]
    return upper


def _num(value: Any) -> float | None:
    """Coerce a Twelve Data string number to a float.

    Twelve Data serialises OHLCV and indicator values as *strings* (``"336.83"``)
    while the rest of this layer (EODHD, StockData) answers with JSON numbers;
    coercing here keeps one normalised shape for a chain regardless of which
    provider answered.

    Args:
        value: Raw field value.

    Returns:
        The float value, or ``None`` when absent/unparseable.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ascending(rows: list[dict[str, Any]], key: str = "datetime") -> list[dict[str, Any]]:
    """Return *rows* sorted ascending by their date field.

    Twelve Data returns series newest-first; the normalised shape is ascending.

    Args:
        rows: Raw value rows.
        key: Date field name (``datetime`` for series, ``date`` elsewhere).

    Returns:
        A new list ordered oldest-first.
    """
    return sorted(rows, key=lambda row: str(row.get(key, "")))


@register_provider
class TwelveDataProvider(Provider):
    """Key-gated Twelve Data REST adapter."""

    name = "twelve_data"
    env_keys = (_API_KEY_ENV,)
    capabilities = {
        "core_stock_apis": "core_stock_apis",
        "technical_indicators": "technical_indicators",
        "fundamental_data": "fundamental_data",
        "earnings_calendar": "earnings_calendar",
        "exchange_symbols": "exchange_symbols",
        "corporate_actions": "corporate_actions",
    }

    def _get(
        self, path: str, *, category: str, params: dict[str, Any] | None = None
    ) -> Any:
        """Call one Twelve Data endpoint with the configured key.

        Args:
            path: Endpoint path, e.g. ``/time_series``.
            category: Category the call belongs to, for error reporting.
            params: Query parameters (the key is added here).

        Returns:
            The decoded JSON body.

        Raises:
            ProviderHTTPError: When the key is unconfigured, the upstream
                refused, or the body carries an in-band error envelope.
        """
        from src.data_providers.base import env_value  # noqa: PLC0415

        token = env_value(_API_KEY_ENV)
        if not token:
            raise ProviderHTTPError(
                self.name, f"{_API_KEY_ENV} is not configured", category=category
            )
        query = dict(params or {})
        query["apikey"] = token
        payload = fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}{path}",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=query,
        )
        return self._raise_on_error(payload, category)

    def _raise_on_error(self, payload: Any, category: str) -> Any:
        """Reject a Twelve Data error envelope.

        Twelve Data answers most refusals with ``{"code": N, "message": ...,
        "status": "error"}``, but some — notably ``/options/chain``, whose
        quoted body is ``{"code":404,"message":"The options unavailable"}`` —
        omit ``status``. A dict body carrying a numeric ``code`` and none of the
        payload keys in :data:`_PAYLOAD_KEYS` is therefore refused too, so a
        200-plus-error envelope is never mistaken for data.

        Args:
            payload: Decoded JSON body.
            category: Category the call belongs to.

        Returns:
            *payload* unchanged when it is not an error envelope.

        Raises:
            ProviderHTTPError: When the body is an error envelope.
        """
        if isinstance(payload, dict):
            code = payload.get("code")
            error_status = str(payload.get("status", "")).lower() == "error"
            refusal_without_status = _numeric_code(code) and not (
                _PAYLOAD_KEYS & payload.keys()
            )
            if error_status or refusal_without_status:
                raise ProviderHTTPError(
                    self.name,
                    f"upstream error {code}: {payload.get('message', '')}".strip(),
                    category=category,
                    status=code if isinstance(code, int) else None,
                )
        return payload

    def _rows(self, payload: Any, key: str, category: str) -> list[dict[str, Any]]:
        """Return the list of row objects at *key*, or refuse.

        A body without a list at *key* is not an empty answer: it is a shape
        this adapter does not model, so the chain must fail over rather than
        treat a provider-native refusal as a successful empty result. An empty
        list is returned only when the upstream genuinely sent one.

        Args:
            payload: Decoded JSON body.
            key: Payload key holding the rows (``values``, ``data``, ...).
            category: Category the call belongs to.

        Returns:
            The row objects (non-mapping entries dropped).

        Raises:
            ProviderHTTPError: When the body has no list at *key*.
        """
        if isinstance(payload, dict) and isinstance(payload.get(key), list):
            return [row for row in payload[key] if isinstance(row, dict)]
        raise ProviderHTTPError(
            self.name,
            f"unexpected {self.name} payload for {category!r}: "
            f"expected a list at {key!r}",
            category=category,
        )

    def _block(self, payload: Any, key: str, category: str) -> dict[str, Any]:
        """Return the mapping at *key*, or refuse.

        Counterpart to :meth:`_rows` for object payloads such as
        ``/statistics``'s ``statistics`` block.

        Args:
            payload: Decoded JSON body.
            key: Payload key holding the mapping.
            category: Category the call belongs to.

        Returns:
            The mapping.

        Raises:
            ProviderHTTPError: When the body has no mapping at *key*.
        """
        if isinstance(payload, dict) and isinstance(payload.get(key), dict):
            return payload[key]
        raise ProviderHTTPError(
            self.name,
            f"unexpected {self.name} payload for {category!r}: "
            f"expected an object at {key!r}",
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
        interval: str = "1day",
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Time-series bars for *symbol*.

        Args:
            symbol: Ticker, e.g. ``AAPL`` or ``AAPL.US``.
            start: Inclusive ``YYYY-MM-DD`` lower bound (``start_date``).
            end: Inclusive ``YYYY-MM-DD`` upper bound (``end_date``).
            limit: Maximum rows requested (``outputsize``, capped at 5000).
            interval: Twelve Data interval, e.g. ``1day``, ``1h``. Intraday
                intervals are one credit each and are not covered by the daily
                loader.

        Returns:
            Ascending rows of ``{trade_date, open, high, low, close, volume}``.
        """
        params: dict[str, Any] = {
            "symbol": _td_symbol(symbol),
            "interval": interval,
            "order": "asc",
        }
        if limit and limit > 0:
            params["outputsize"] = max(1, min(int(limit), 5000))
        if start:
            params["start_date"] = start
        if end:
            params["end_date"] = end
        payload = self._get("/time_series", category="core_stock_apis", params=params)
        rows = self._rows(payload, "values", "core_stock_apis")
        return [
            {
                "trade_date": str(row.get("datetime", "")),
                "open": _num(row.get("open")),
                "high": _num(row.get("high")),
                "low": _num(row.get("low")),
                "close": _num(row.get("close")),
                "volume": _num(row.get("volume")),
            }
            for row in _ascending(rows)
        ]

    # -- technical_indicators ----------------------------------------------

    def technical_indicators(
        self,
        *,
        symbol: str,
        indicator: str = "rsi",
        interval: str = "1day",
        time_period: int | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """One technical indicator series for *symbol*.

        Each indicator is its own endpoint, and therefore its own credit.

        Args:
            symbol: Ticker, e.g. ``AAPL``.
            indicator: One of ``rsi``, ``sma``, ``ema``.
            interval: Twelve Data interval, e.g. ``1day``.
            time_period: Look-back window; per-indicator default when omitted.
            limit: Maximum rows requested (``outputsize``).

        Returns:
            Ascending rows of ``{trade_date, value}``.

        Raises:
            ProviderHTTPError: For an indicator this adapter does not drive.
        """
        key = str(indicator).strip().lower()
        path = _INDICATOR_PATHS.get(key)
        if path is None:
            supported = ", ".join(sorted(_INDICATOR_PATHS))
            raise ProviderHTTPError(
                self.name,
                f"unsupported indicator {indicator!r}; supported: {supported}",
                category="technical_indicators",
            )
        params: dict[str, Any] = {
            "symbol": _td_symbol(symbol),
            "interval": interval,
            "time_period": int(time_period or _INDICATOR_DEFAULT_PERIOD[key]),
            "order": "asc",
        }
        if limit and limit > 0:
            params["outputsize"] = max(1, min(int(limit), 5000))
        payload = self._get(
            f"/{path}", category="technical_indicators", params=params
        )
        rows = self._rows(payload, "values", "technical_indicators")
        return [
            {
                "trade_date": str(row.get("datetime", "")),
                "value": _num(row.get(key)),
            }
            for row in _ascending(rows)
        ]

    # -- fundamental_data --------------------------------------------------

    def fundamental_data(self, *, symbol: str, **_: Any) -> dict[str, Any]:
        """Income-statement rows plus the statistics block for *symbol*.

        The category's normalised shape asks for both a period list and a
        statements map; Twelve Data splits those across ``/income_statement``
        and ``/statistics``, so this is the one method that spends two credits.

        Args:
            symbol: Ticker, e.g. ``AAPL``.

        Returns:
            ``{"symbol", "periods", "statements": {"income_statement": [...]},
            "statistics": {...}}``.
        """
        ticker = _td_symbol(symbol)
        statement_payload = self._get(
            "/income_statement", category="fundamental_data", params={"symbol": ticker}
        )
        rows = self._rows(statement_payload, "income_statement", "fundamental_data")
        stats_payload = self._get(
            "/statistics", category="fundamental_data", params={"symbol": ticker}
        )
        statistics = self._block(stats_payload, "statistics", "fundamental_data")
        return {
            "symbol": ticker,
            "periods": rows,
            "statements": {"income_statement": rows},
            "statistics": statistics,
        }

    # -- earnings_calendar -------------------------------------------------

    def earnings_calendar(
        self, *, symbol: str | None = None, limit: int | None = None, **_: Any
    ) -> list[dict[str, Any]]:
        """Reported-vs-estimated EPS history for *symbol*.

        Twelve Data's ``/earnings`` endpoint is ticker-scoped (it requires a
        ``symbol``); the date-range calendar on the same host
        (``/earnings_calendar``) answered **HTTP 403** on this key, so it is not
        used. A request without a ticker is refused here so the chain can fail
        over rather than return an empty list.

        Args:
            symbol: Ticker, e.g. ``AAPL``.
            limit: Maximum rows requested (``outputsize``).

        Returns:
            Rows of ``{symbol, date, eps_estimate, revenue_estimate, hour}``;
            ``revenue_estimate`` is ``None`` because the endpoint reports EPS
            only.

        Raises:
            ProviderHTTPError: When no ticker was supplied.
        """
        if not symbol:
            raise ProviderHTTPError(
                self.name,
                "Twelve Data earnings are ticker-scoped; a tickerless calendar "
                "request is unsupported on this plan",
                category="earnings_calendar",
            )
        params: dict[str, Any] = {"symbol": _td_symbol(symbol)}
        if limit and limit > 0:
            params["outputsize"] = max(1, min(int(limit), 5000))
        payload = self._get("/earnings", category="earnings_calendar", params=params)
        rows = self._rows(payload, "earnings", "earnings_calendar")
        meta_symbol = ""
        if isinstance(payload, dict) and isinstance(payload.get("meta"), dict):
            meta_symbol = str(payload["meta"].get("symbol", ""))
        return [
            {
                "symbol": meta_symbol or _td_symbol(symbol),
                "date": str(row.get("date", "")),
                "eps_estimate": _num(row.get("eps_estimate")),
                "revenue_estimate": None,
                "hour": row.get("time"),
            }
            for row in _ascending(rows, key="date")
        ]

    # -- corporate_actions -------------------------------------------------

    def corporate_actions(
        self, *, symbol: str, kind: str = "dividends", **_: Any
    ) -> list[dict[str, Any]]:
        """Dividends or splits for *symbol*.

        Args:
            symbol: Ticker, e.g. ``AAPL``.
            kind: ``dividends`` or ``splits``.

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
        payload = self._get(
            f"/{kind}", category="corporate_actions", params={"symbol": _td_symbol(symbol)}
        )
        rows = self._rows(payload, kind, "corporate_actions")
        out: list[dict[str, Any]] = []
        for row in _ascending(rows, key="date" if kind == "splits" else "ex_date"):
            record = dict(row, type=kind)
            # Dividends key their date as ``ex_date``; splits as ``date``.
            record.setdefault("date", row.get("ex_date"))
            out.append(record)
        return out

    # -- exchange_symbols --------------------------------------------------

    def exchange_symbols(
        self,
        *,
        symbol: str | None = None,
        query: str | None = None,
        exchange: str | None = None,
        country: str | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Instruments matching a search term.

        ``/symbol_search`` needs a term; a request without one is refused so the
        chain fails over instead of returning a catalogue-sized payload.

        Args:
            symbol: Exact-ish ticker term, e.g. ``AAPL``.
            query: Free-text term; used when ``symbol`` is absent.
            exchange: Optional exchange filter passed upstream.
            country: Optional country filter passed upstream.
            limit: Maximum rows kept from the response.

        Returns:
            Rows of ``{symbol, name, exchange, type, currency}``.

        Raises:
            ProviderHTTPError: When neither ``symbol`` nor ``query`` is given.
        """
        term = (symbol or query or "").strip()
        if not term:
            raise ProviderHTTPError(
                self.name,
                "Twelve Data symbol search requires a symbol or query term",
                category="exchange_symbols",
            )
        params: dict[str, Any] = {"symbol": term.upper()}
        if exchange:
            params["exchange"] = exchange
        if country:
            params["country"] = country
        payload = self._get("/symbol_search", category="exchange_symbols", params=params)
        rows = self._rows(payload, "data", "exchange_symbols")
        out = [
            {
                "symbol": row.get("symbol"),
                "name": row.get("instrument_name"),
                "exchange": row.get("exchange"),
                "type": row.get("instrument_type"),
                "currency": row.get("currency"),
            }
            for row in rows
        ]
        if limit and limit > 0:
            out = out[: int(limit)]
        return out
