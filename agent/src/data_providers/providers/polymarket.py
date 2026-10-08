"""Polymarket provider adapter.

Two public, no-auth hosts, split into two throttle buckets because they are
different origins with different rate budgets (the same split
``src/tools/prediction_market_tool.py`` uses, which is the live-verified
reference for every field name below):

* **Gamma** ``https://gamma-api.polymarket.com`` — catalogue and current quotes.
  ``GET /events`` (``limit``, ``active``, ``closed``, ``slug``), ``GET
  /events/<id>`` and ``GET /public-search?q=&limit_per_type=`` all measured 200
  on 2026-10-08; a miss on an id is HTTP 404.
* **CLOB** ``https://clob.polymarket.com`` — price history.
  ``GET /prices-history?market=<clob token id>&interval=&fidelity=`` measured
  200, answering ``{"history": [{"t": epoch_seconds, "p": price}, …]}``.

``https://data-api.polymarket.com/markets`` is **not** used: it is the wrong
path (404) — the Data API's market routes live under ``/v2/...`` and its
wallet/feed surfaces serve no category in this vocabulary. Nothing here needs
it, because Gamma already carries the CLOB token ids.

Gotchas carried over from the tool: Gamma serializes ``outcomes``,
``outcomePrices`` and ``clobTokenIds`` as JSON *strings*, not arrays, so they
must be decoded; ``/prices-history`` is keyed by a CLOB token id (one series per
outcome), not by a market id; and ``closed`` (trading ended) is not ``resolved``
(oracle settled). This adapter only projects what the category shape needs —
``src/tools/prediction_market_tool.py`` remains the full-resolution
implementation.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_GAMMA_BASE = "https://gamma-api.polymarket.com"
_CLOB_BASE = "https://clob.polymarket.com"

_GAMMA_HOST_KEY = "polymarket_gamma"
_CLOB_HOST_KEY = "polymarket_clob"
_MIN_INTERVAL_ENV = "VIBE_TRADING_POLYMARKET_MIN_INTERVAL"
_DEFAULT_MIN_INTERVAL_S = 0.35
_TIMEOUT_S = 20.0

#: Defensive caps, mirroring the tool's, so one event cannot blow up a payload.
_MAX_MARKETS_PER_EVENT = 30
_MAX_OUTCOMES_PER_MARKET = 20
_MAX_EVENTS = 50
_DEFAULT_EVENTS = 20
_MAX_HISTORY_POINTS = 1000

#: Lifecycle labels. Shared vocabulary with ``prediction_market_tool`` so the
#: two layers cannot describe the same upstream flags with different words.
_STATUS_OPEN = "open"
_STATUS_CLOSED = "closed"
_STATUS_RESOLVED = "resolved"
_STATUS_INACTIVE = "inactive"
_STATUS_ARCHIVED = "archived"

#: ``umaResolutionStatus`` values that mean the oracle answer is final. Every
#: other non-null value (the observed one is ``"proposed"``) is still in flight.
_UMA_FINAL = frozenset({"resolved", "settled"})

#: Server-accepted history windows; ``1m`` is one month, not one minute.
_INTERVALS: dict[str, int] = {"1h": 1, "6h": 10, "1d": 60, "1w": 360, "1m": 1440, "max": 1440}
_DEFAULT_INTERVAL = "1d"


@register_provider
class PolymarketProvider(Provider):
    """No-key Polymarket adapter (Gamma catalogue + CLOB price history)."""

    name = "polymarket"
    env_keys = ()
    capabilities = {"prediction_markets": "prediction_markets"}

    def _gamma(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """GET one Gamma endpoint under the Gamma throttle bucket."""
        return fetch_json(
            provider=self.name,
            category="prediction_markets",
            url=f"{_GAMMA_BASE}{path}",
            host_key=_GAMMA_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=params,
            timeout=_TIMEOUT_S,
        )

    def _clob(self, path: str, *, params: dict[str, Any]) -> Any:
        """GET one CLOB endpoint under the CLOB throttle bucket."""
        return fetch_json(
            provider=self.name,
            category="prediction_markets",
            url=f"{_CLOB_BASE}{path}",
            host_key=_CLOB_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=params,
            timeout=_TIMEOUT_S,
        )

    # -- prediction_markets ------------------------------------------------

    def prediction_markets(
        self,
        *,
        query: str | None = None,
        slug: str | None = None,
        event_id: str | int | None = None,
        limit: int = _DEFAULT_EVENTS,
        status: str | None = None,
        token_id: str | None = None,
        interval: str = _DEFAULT_INTERVAL,
        **_: Any,
    ) -> dict[str, Any]:
        """Event contracts from the Gamma catalogue, optionally with a price series.

        Args:
            query: Free-text keyword search (``/public-search``).
            slug: Exact event slug (the match is exact, not a prefix).
            event_id: Gamma numeric event id (``event_id`` wins over ``slug``
                and ``query``).
            limit: Maximum events returned (1-50).
            status: Lifecycle filter, matched case-insensitively:
                ``open``/``active``/``event_active`` send
                ``events_status=active`` and ``closed``/``event_closed`` send
                ``resolved`` (which selects ``closed == true`` — not a
                settlement filter); any other value sends no upstream filter.
                The ``event_*`` spellings are ``moomoo``'s, accepted here so a
                caller's spelling cannot decide which chain member answers.
                With no selector, a closed token lists the closed catalogue and
                anything else lists the active one.
            token_id: Optional CLOB outcome token id; when given, the response
                also carries ``price_history`` for that outcome.
            interval: History window, one of ``1h``, ``6h``, ``1d``, ``1w``,
                ``1m`` (one month) or ``max``.

        Returns:
            ``{"events": [...], "count": int}``; each event is
            ``{event_id, title, slug, ticker, status, trading_closed,
            start_date, end_date, closed_time, volume_usd, volume_24h_usd,
            liquidity_usd, market_count, markets: [...]}`` and each market
            carries ``outcomes`` of ``{outcome, implied_probability, price_usd,
            clob_token_id}``. ``price_history`` (``{token_id, interval, points:
            [{timestamp, price}]}``) is present only when ``token_id`` is given.

        Raises:
            ProviderHTTPError: For a blank selector that would need one, an
                exact slug with no match, an unknown history interval, or a
                refused/junk upstream response.
        """
        events = self._events(
            query=query,
            slug=slug,
            event_id=event_id,
            limit=_clamp_limit(limit, _DEFAULT_EVENTS, _MAX_EVENTS),
            status=status,
        )
        payload: dict[str, Any] = {"events": events, "count": len(events)}
        if token_id is not None and str(token_id).strip():
            payload["price_history"] = self._price_history(
                str(token_id).strip(), interval=interval
            )
        return payload

    def _events(
        self,
        *,
        query: str | None,
        slug: str | None,
        event_id: str | int | None,
        limit: int,
        status: str | None,
    ) -> list[dict[str, Any]]:
        """Fetch and normalise the event rows for one selector.

        Raises:
            ProviderHTTPError: On a bad selector, an exact-slug miss, or a
                refused/junk upstream response.
        """
        if event_id is not None and str(event_id).strip():
            payload = self._gamma(f"/events/{str(event_id).strip()}")
            if not isinstance(payload, dict):
                raise ProviderHTTPError(
                    self.name,
                    f"event {event_id} payload was not an object",
                    category="prediction_markets",
                )
            return [_normalize_event(payload)]

        if slug is not None and slug.strip():
            payload = self._gamma("/events", params={"slug": slug.strip()})
            rows = payload if isinstance(payload, list) else []
            if not rows:
                raise ProviderHTTPError(
                    self.name,
                    f"no event matches slug {slug.strip()!r} (the match is exact)",
                    category="prediction_markets",
                )
            return [_normalize_event(row) for row in rows if isinstance(row, dict)][:limit]

        if query is not None and query.strip():
            params: dict[str, Any] = {
                "q": query.strip(),
                "limit_per_type": limit,
            }
            upstream_status = _status_filter(status)
            if upstream_status:
                params["events_status"] = upstream_status
            payload = self._gamma("/public-search", params=params)
            rows = payload.get("events") if isinstance(payload, dict) else None
            rows = rows if isinstance(rows, list) else []
            return [_normalize_event(row) for row in rows if isinstance(row, dict)][:limit]

        params: dict[str, Any] = {"limit": limit}
        if _status_filter(status) == "resolved":
            # The catalogue default is the active set; asking for closed events
            # must not be answered with open ones.
            params["closed"] = "true"
        else:
            params["active"] = "true"
            params["closed"] = "false"
        payload = self._gamma("/events", params=params)
        rows = payload if isinstance(payload, list) else []
        return [_normalize_event(row) for row in rows if isinstance(row, dict)][:limit]

    def _price_history(self, token_id: str, *, interval: str) -> dict[str, Any]:
        """Fetch one outcome token's implied-probability time series.

        Args:
            token_id: CLOB outcome token id (``/prices-history`` is keyed by
                token, not by market or condition id).
            interval: One of :data:`_INTERVALS`.

        Returns:
            ``{"token_id", "interval", "points": [{"timestamp", "price"}],
            "count"}``, oldest first, capped at 1000 points.

        Raises:
            ProviderHTTPError: For an unknown interval, or a refused/junk
                upstream response.
        """
        key = interval.strip().lower() if isinstance(interval, str) else ""
        if key not in _INTERVALS:
            raise ProviderHTTPError(
                self.name,
                f"unknown history interval {interval!r}; known: {', '.join(_INTERVALS)}",
                category="prediction_markets",
            )
        payload = self._clob(
            "/prices-history",
            params={"market": token_id, "interval": key, "fidelity": _INTERVALS[key]},
        )
        raw = payload.get("history") if isinstance(payload, dict) else None
        if not isinstance(raw, list):
            raise ProviderHTTPError(
                self.name,
                f"price history for token {token_id} carried no history array",
                category="prediction_markets",
            )
        points: list[dict[str, Any]] = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            price = _to_float(entry.get("p"))
            epoch = _to_float(entry.get("t"))
            if price is None or epoch is None:
                continue
            stamp = datetime.fromtimestamp(epoch, tz=timezone.utc)
            points.append(
                {
                    "timestamp": stamp.isoformat().replace("+00:00", "Z"),
                    "price": price,
                }
            )
        points = points[-_MAX_HISTORY_POINTS:]
        return {
            "token_id": token_id,
            "interval": key,
            "points": points,
            "count": len(points),
        }


def _normalize_event(raw: dict[str, Any]) -> dict[str, Any]:
    """Shape one Gamma event into the category's event record.

    Args:
        raw: A Gamma event object.

    Returns:
        An event record carrying identifiers, the lifecycle ``status``, dates,
        volume/liquidity and the normalised markets beneath it.
    """
    raw_markets = raw.get("markets")
    usable = [m for m in raw_markets if isinstance(m, dict)] if isinstance(raw_markets, list) else []
    markets = [_normalize_market(m) for m in usable[:_MAX_MARKETS_PER_EVENT]]
    closed = bool(raw.get("closed"))
    return {
        "event_id": str(raw.get("id")) if raw.get("id") is not None else None,
        "title": raw.get("title"),
        "slug": raw.get("slug"),
        "ticker": raw.get("ticker"),
        "status": _status(raw, closed=closed),
        "trading_closed": closed,
        "start_date": raw.get("startDate"),
        "end_date": raw.get("endDate"),
        "closed_time": raw.get("closedTime"),
        "volume_usd": _to_float(raw.get("volume")),
        "volume_24h_usd": _to_float(raw.get("volume24hr")),
        "liquidity_usd": _to_float(raw.get("liquidity")),
        "market_count": len(usable),
        "markets": markets,
    }


def _normalize_market(raw: dict[str, Any]) -> dict[str, Any]:
    """Shape one Gamma market into the category's market record.

    Args:
        raw: A Gamma market object (as nested under ``events[].markets``).

    Returns:
        A market record with identifiers, the lifecycle ``status``, per-outcome
        implied probabilities and top-of-book quote fields.
    """
    names = [str(name) for name in _json_list(raw.get("outcomes"))][:_MAX_OUTCOMES_PER_MARKET]
    prices = _json_list(raw.get("outcomePrices"))[:_MAX_OUTCOMES_PER_MARKET]
    tokens = [str(token) for token in _json_list(raw.get("clobTokenIds"))][:_MAX_OUTCOMES_PER_MARKET]
    outcomes: list[dict[str, Any]] = []
    for index, name in enumerate(names):
        price = _to_float(prices[index]) if index < len(prices) else None
        outcome: dict[str, Any] = {
            "outcome": name,
            "implied_probability": price,
            "price_usd": price,
        }
        if index < len(tokens):
            outcome["clob_token_id"] = tokens[index]
        outcomes.append(outcome)
    closed = bool(raw.get("closed"))
    return {
        "market_id": str(raw.get("id")) if raw.get("id") is not None else None,
        "question": raw.get("question"),
        "slug": raw.get("slug"),
        "condition_id": raw.get("conditionId"),
        "status": _status(raw, closed=closed),
        "trading_closed": closed,
        "end_date": raw.get("endDate"),
        "closed_time": raw.get("closedTime"),
        "outcomes": outcomes,
        "volume_usd": _to_float(raw.get("volumeNum") or raw.get("volume")),
        "liquidity_usd": _to_float(raw.get("liquidityNum") or raw.get("liquidity")),
        "best_bid": _to_float(raw.get("bestBid")),
        "best_ask": _to_float(raw.get("bestAsk")),
        "spread": _to_float(raw.get("spread")),
        "last_trade_price": _to_float(raw.get("lastTradePrice")),
    }


#: Request spellings of the ``status`` filter -> the ``events_status`` value
#: this adapter sends. ``moomoo`` (the chain's second entry) spells the same
#: concept with ``ECStatus`` member names, so both are accepted: a caller's
#: spelling must not decide which provider can answer. Upstream's ``resolved``
#: selects ``closed == true`` and nothing more — it is not a settlement filter.
_STATUS_FILTERS: dict[str, str] = {
    "open": "active",
    "active": "active",
    "event_active": "active",
    "closed": "resolved",
    "event_closed": "resolved",
}


def _status_filter(status: Any) -> str | None:
    """Map a requested ``status`` spelling onto an ``events_status`` value.

    Args:
        status: ``None``, or a token such as ``open``, ``event_active`` or
            ``closed`` (matched case-insensitively).

    Returns:
        ``"active"`` / ``"resolved"`` for a recognised open/closed token, else
        ``None`` to send no upstream filter.
    """
    token = str(status).strip().casefold() if status is not None else ""
    return _STATUS_FILTERS.get(token)


def _status(raw: dict[str, Any], *, closed: bool) -> str:
    """Map upstream flags onto one lifecycle label.

    ``archived`` is checked first (it describes the catalogue entry), then
    ``inactive``; settlement is only claimed when the oracle status is final,
    because ``closed`` alone means trading ended, not that a winner exists.

    Args:
        raw: Gamma event or market object.
        closed: The object's ``closed`` flag, already coerced to ``bool``.

    Returns:
        One of ``open``, ``closed``, ``resolved``, ``inactive``, ``archived``.
    """
    if raw.get("archived"):
        return _STATUS_ARCHIVED
    if raw.get("active") is False:
        return _STATUS_INACTIVE
    if closed:
        uma = raw.get("umaResolutionStatus")
        if isinstance(uma, str) and uma.strip().lower() in _UMA_FINAL:
            return _STATUS_RESOLVED
        return _STATUS_CLOSED
    return _STATUS_OPEN


def _json_list(value: Any) -> list[Any]:
    """Decode a Gamma field that may arrive as a JSON string or a real list.

    Args:
        value: Raw field value (``'["Yes", "No"]'`` or ``["Yes", "No"]``).

    Returns:
        The decoded list, or ``[]`` when the value is absent or malformed.
    """
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except (ValueError, TypeError):
            return []
        return decoded if isinstance(decoded, list) else []
    return []


def _to_float(value: Any) -> float | None:
    """Coerce a possibly-string numeric field to ``float``, or ``None``.

    ``float()`` accepts ``"nan"``/``"inf"``, which are not usable data here.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _clamp_limit(value: Any, default: int, high: int) -> int:
    """Coerce a requested event count into ``1..high``."""
    try:
        count = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return max(1, min(count, high))
