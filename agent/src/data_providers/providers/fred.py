"""FRED provider adapter.

Base URL ``https://api.stlouisfed.org/fred``. Every endpoint needs the free
``api_key`` query parameter *and* ``file_type=json``: FRED negotiates XML by
default, so the parameter is what makes a response decodable here at all. A
series is addressed by its short uppercase id — ``DGS10`` (10-year Treasury
constant-maturity yield), ``DGS3MO``, ``CPIAUCSL``, ``UNRATE``, ``FEDFUNDS``.

Endpoints used (all measured 200 against the configured ``FRED_API_KEY`` on
2026-10-08):

* ``GET /series/observations?series_id=DGS10&limit=2`` — the ``{date, value}``
  rows both categories are built from. ``value`` is a string and a lone ``"."``
  marks a gap; that gap is normalised to ``None`` and kept, because dropping it
  would silently turn a hole into a shorter series.

``GET /series?series_id=DGS10``, ``GET /series/search?search_text=treasury`` and
``GET /releases`` also answer 200, but no category in the vocabulary maps to
series metadata, free-text series search or release calendars, so they are
deliberately not implemented — an unmapped endpoint is not a category.

``risk_free_curve`` is built by fanning ``/series/observations`` over the
constant-maturity Treasury ids FRED publishes (``DGS3MO`` … ``DGS30``) and
re-keying every point to ``{date, tenor, rate}``. The category's chain names
``federal_reserve`` first and this adapter second, so the two must agree on the
normal form: both **emit** the compact tenor vocabulary (``3M``, ``10Y``,
``30Y``) and both **accept** the Treasury CSV spelling as input (``"10 Yr"``
becomes ``10Y``), so a caller's spelling can never decide which provider
answers. Their coverage still differs — FRED has no ``1.5M``/``2M``/``4M``
series, and a request for one of those raises a clear error naming what this
adapter does publish rather than silently serving a shorter curve.
"""

from __future__ import annotations

from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider, env_value
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_BASE_URL = "https://api.stlouisfed.org/fred"
_API_KEY_ENV = "FRED_API_KEY"
_HOST_KEY = "fred"
_MIN_INTERVAL_ENV = "VIBE_TRADING_FRED_MIN_INTERVAL"
# The FRED tool (src/tools/fred_macro_tool.py) already spaces this host at 0.6s
# under the same bucket; keeping the numbers identical means the tool and this
# adapter share one rate budget instead of each spending the other's quota.
_DEFAULT_MIN_INTERVAL_S = 0.6
_TIMEOUT_S = 15.0

#: FRED's sentinel for a missing reading inside an otherwise valid series.
_MISSING_VALUE = "."

#: Constant-maturity Treasury series published by FRED, tenor label -> series id.
_TENOR_SERIES: dict[str, str] = {
    "1M": "DGS1MO",
    "3M": "DGS3MO",
    "6M": "DGS6MO",
    "1Y": "DGS1",
    "2Y": "DGS2",
    "3Y": "DGS3",
    "5Y": "DGS5",
    "7Y": "DGS7",
    "10Y": "DGS10",
    "20Y": "DGS20",
    "30Y": "DGS30",
}

#: Tenors returned when the caller does not name any: the points that define the
#: short, belly and long end of the curve without spending eleven requests.
_DEFAULT_TENORS: tuple[str, ...] = ("3M", "2Y", "5Y", "10Y", "30Y")

#: Accepted request spellings -> compact tenor. The compact form is what this
#: adapter emits and what the sibling ``federal_reserve`` adapter emits; the
#: Treasury CSV label spelling (``"3 Mo"``, ``"10 Yr"``) is accepted on input
#: too, because the category's chain has both providers and a caller's spelling
#: must not decide which one can answer.
_TENOR_ALIASES: dict[str, str] = {
    spelling: label
    for label in _TENOR_SERIES
    for spelling in (label, f"{label[:-1]} {'MO' if label[-1] == 'M' else 'YR'}")
}


@register_provider
class FredProvider(Provider):
    """Key-gated FRED REST adapter."""

    name = "fred"
    env_keys = (_API_KEY_ENV,)
    capabilities = {
        "macro_data": "macro_data",
        "risk_free_curve": "risk_free_curve",
    }

    def _get(
        self, path: str, *, category: str, params: dict[str, Any] | None = None
    ) -> Any:
        """Call one FRED endpoint with the configured key.

        Args:
            path: Endpoint path below the FRED base, e.g. ``/series/observations``.
            category: Category the call belongs to, for the raised error.
            params: Endpoint-specific query parameters.

        Returns:
            The decoded JSON body.

        Raises:
            ProviderHTTPError: When ``FRED_API_KEY`` is empty, or when the
                endpoint refuses / returns junk (raised by ``fetch_json``).
        """
        api_key = env_value(_API_KEY_ENV)
        if not api_key:
            raise ProviderHTTPError(
                self.name, f"{_API_KEY_ENV} is not configured", category=category
            )
        query = dict(params or {})
        query.update({"api_key": api_key, "file_type": "json"})
        return fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}{path}",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=query,
            timeout=_TIMEOUT_S,
        )

    def _observations(
        self,
        series_id: str,
        *,
        category: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch one series' observations, oldest first.

        Args:
            series_id: FRED series id, e.g. ``DGS10``.
            category: Category the call belongs to, for the raised error.
            start: Inclusive ``YYYY-MM-DD`` lower bound (``observation_start``).
            end: Inclusive ``YYYY-MM-DD`` upper bound (``observation_end``).
            limit: Keep only the ``limit`` most recent observations.

        Returns:
            Ascending rows of ``{date, value}``, gaps kept as ``None``.

        Raises:
            ProviderHTTPError: When the endpoint refuses, or answers without an
                ``observations`` array.
        """
        params: dict[str, Any] = {"series_id": series_id}
        if start:
            params["observation_start"] = start
        if end:
            params["observation_end"] = end
        if limit and limit > 0:
            # FRED's own cap is applied newest-first, then reversed below, so the
            # most recent N are kept without downloading a multi-decade history.
            params["sort_order"] = "desc"
            params["limit"] = int(limit)
        payload = self._get("/series/observations", category=category, params=params)
        if not isinstance(payload, dict):
            raise ProviderHTTPError(
                self.name,
                f"observations payload for {series_id} was not an object",
                category=category,
            )
        rows = payload.get("observations")
        if not isinstance(rows, list):
            raise ProviderHTTPError(
                self.name,
                f"observations payload for {series_id} carried no observations array",
                category=category,
            )
        out = [_normalize_observation(row) for row in rows]
        normalized = [row for row in out if row is not None]
        if limit and limit > 0:
            # Re-establish the ascending order the shape documents.
            normalized.reverse()
        return normalized

    # -- macro_data --------------------------------------------------------

    def macro_data(
        self,
        *,
        series_id: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Observations of one FRED macroeconomic series.

        Args:
            series_id: FRED series id, e.g. ``CPIAUCSL``, ``UNRATE``, ``DGS10``.
            start: Inclusive ``YYYY-MM-DD`` lower bound.
            end: Inclusive ``YYYY-MM-DD`` upper bound.
            limit: Keep only the ``limit`` most recent observations.

        Returns:
            Ascending rows of ``{date, value}``; a FRED ``"."`` gap becomes
            ``value: None`` rather than being dropped.

        Raises:
            ProviderHTTPError: For a blank ``series_id``, a missing key, or a
                refused/junk upstream response.
        """
        if not isinstance(series_id, str) or not series_id.strip():
            raise ProviderHTTPError(
                self.name, "'series_id' is required for macro_data", category="macro_data"
            )
        return self._observations(
            series_id.strip().upper(),
            category="macro_data",
            start=start,
            end=end,
            limit=limit,
        )

    # -- risk_free_curve ---------------------------------------------------

    def risk_free_curve(
        self,
        *,
        tenor: str | None = None,
        tenors: str | list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = 1,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """The constant-maturity Treasury curve, one row per date and tenor.

        Args:
            tenor: A single tenor label — compact (``10Y``) or the Treasury CSV
                spelling (``10 Yr``) (``tenors`` wins when both are given).
            tenors: Tenor labels as a comma string (``"3M,10Y"``) or a list, in
                either spelling; defaults to :data:`_DEFAULT_TENORS`.
            start: Inclusive ``YYYY-MM-DD`` lower bound, applied to every tenor.
            end: Inclusive ``YYYY-MM-DD`` upper bound, applied to every tenor.
            limit: Most recent observations kept *per tenor* (default ``1``, i.e.
                the latest curve). Raise it for a time series.

        Returns:
            Rows of ``{date, tenor, rate}`` with ``tenor`` always in the compact
            vocabulary (the same one ``federal_reserve`` emits), sorted by date
            then curve order; a gap on a tenor/date becomes ``rate: None``.

        Raises:
            ProviderHTTPError: For an unknown tenor label, a missing key, or a
                refused/junk upstream response.
        """
        labels = _resolve_tenors(tenors if tenors is not None else tenor)
        rows: list[dict[str, Any]] = []
        for label in labels:
            series_id = _TENOR_SERIES[label]
            for observation in self._observations(
                series_id,
                category="risk_free_curve",
                start=start,
                end=end,
                limit=limit,
            ):
                rows.append(
                    {
                        "date": observation["date"],
                        "tenor": label,
                        "rate": observation["value"],
                    }
                )
        order = {label: index for index, label in enumerate(_TENOR_SERIES)}
        rows.sort(key=lambda row: (row["date"], order.get(row["tenor"], 0)))
        return rows


def _canonical_tenor(token: str) -> str | None:
    """Map one requested tenor spelling onto the compact vocabulary.

    Case, internal whitespace and a trailing period are not information, so the
    token is upper-cased, whitespace-collapsed and stripped of a trailing ``.``
    before the lookup — ``"10 yr."``, ``"10 Yr"`` and ``"10Y"`` all name the
    same column.

    Args:
        token: A single requested tenor, e.g. ``"10 Yr"``.

    Returns:
        The compact tenor (``"10Y"``), or ``None`` when the spelling is not one
        this adapter publishes.
    """
    text = " ".join(token.strip().upper().rstrip(".").split())
    return _TENOR_ALIASES.get(text)


def _resolve_tenors(requested: Any) -> list[str]:
    """Turn a ``tenor``/``tenors`` request into a validated label list.

    Args:
        requested: ``None`` (the defaults), a single label, a comma string, or a
            list of labels; compact or Treasury spellings.

    Returns:
        De-duplicated compact tenor labels in request order.

    Raises:
        ProviderHTTPError: When the value is not a string/list of strings, or
            names a tenor FRED does not publish.
    """
    if requested is None:
        return list(_DEFAULT_TENORS)
    if isinstance(requested, str):
        raw: list[Any] = requested.split(",")
    elif isinstance(requested, (list, tuple)):
        raw = list(requested)
    else:
        raise ProviderHTTPError(
            "fred",
            f"'tenors' must be a string or list, got {type(requested).__name__}",
            category="risk_free_curve",
        )
    labels: list[str] = []
    for item in raw:
        token = str(item).strip()
        if not token:
            continue
        label = _canonical_tenor(token)
        if label is None:
            raise ProviderHTTPError(
                "fred",
                f"unknown Treasury tenor {token!r}; this adapter publishes "
                f"{', '.join(_TENOR_SERIES)} (Treasury spellings like '10 Yr' are "
                "accepted; FRED has no 1.5M/2M/4M series)",
                category="risk_free_curve",
            )
        if label not in labels:
            labels.append(label)
    if not labels:
        raise ProviderHTTPError(
            "fred", "no tenor requested", category="risk_free_curve"
        )
    return labels


def _normalize_observation(row: Any) -> dict[str, Any] | None:
    """Map one raw FRED observation to ``{date, value}``, or ``None`` if unusable.

    A row without a date carries no anchor and is dropped; one bad row never
    aborts the batch.

    Args:
        row: One element of a FRED ``observations`` array.

    Returns:
        ``{"date": str, "value": float | None}``, or ``None`` when the row is
        not a mapping or has no date.
    """
    if not isinstance(row, dict):
        return None
    date = row.get("date")
    if not isinstance(date, str) or not date.strip():
        return None
    return {"date": date.strip(), "value": _to_number(row.get("value"))}


def _to_number(value: Any) -> float | None:
    """Coerce a FRED value cell to ``float``, or ``None`` for a gap.

    FRED encodes a missing reading as the string ``"."``; that and any other
    non-numeric cell map to ``None`` so consumers see an explicit hole instead
    of a silently shortened series.
    """
    if value is None or value == "" or value == _MISSING_VALUE:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
