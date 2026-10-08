"""U.S. Treasury provider adapter (no API key).

Two public, key-less Treasury sources, on two different hosts:

* ``risk_free_curve`` — the Treasury's daily par yield-curve CSV, measured 200
  on 2026-10-08::

      GET https://home.treasury.gov/resource-center/data-chart-center/interest-rates/
              daily-treasury-rates.csv/{YYYY}/all
          ?type=daily_treasury_yield_curve&field_tdr_date_value={YYYY}&page&_format=csv

  The endpoint serves **one calendar year per request**. Its header names the
  tenors exactly (``Date,"1 Mo","1.5 Month","2 Mo","3 Mo","4 Mo","6 Mo",
  "1 Yr","2 Yr","3 Yr","5 Yr","7 Yr","10 Yr","20 Yr","30 Yr"``) and each row is
  ``10/07/2026,4.07,...,5.67``; those labels are mapped onto the compact curve
  vocabulary ``fred`` uses (``"1 Mo"`` -> ``"1M"``, ``"10 Yr"`` -> ``"10Y"``),
  which is both what a caller may request and what the normalised ``tenor``
  field carries. A year that has not been published yet answers HTTP 200 with
  an **empty body**, which is treated as junk rather than as an empty curve.

* ``macro_data`` — fiscaldata's *Average Interest Rates on U.S. Treasury
  Securities* series (``/v2/accounting/od/avg_interest_rates``). The dataset is
  monthly and one row per (month, security type), so a single ``{date, value}``
  series requires a selector: ``series_id`` is matched against the dataset's
  ``security_desc`` column (e.g. ``"Treasury Notes"``, ``"Treasury Bills"``,
  ``"Treasury Bonds"``, ``"Total Interest-bearing Debt"``). The upstream filter
  is case-sensitive and silently returns zero rows for an unknown name, which
  the adapter refuses instead of reporting as an empty series.

Federal Reserve heads the ``risk_free_curve`` chain; ``fred`` is appended after
it as an extension, so the two must agree on what an unqualified call means —
hence the shared compact tenor vocabulary and the latest-curve default.
``federal_reserve`` is not listed in the ``macro_data`` chain (``fred``,
``moomoo``); the capability exists so the operator can add it where wanted.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from typing import Any

from src.data_providers._http import fetch_json, fetch_text
from src.data_providers.base import Provider
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_CSV_BASE_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates"
    "/daily-treasury-rates.csv"
)
_FISCALDATA_URL = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
    "/v2/accounting/od/avg_interest_rates"
)

# Separate throttle buckets: two different hosts must not share a rate budget.
_TREASURY_HOST_KEY = "treasury"
_FISCALDATA_HOST_KEY = "fiscaldata"
_TREASURY_MIN_INTERVAL_ENV = "VIBE_TRADING_TREASURY_MIN_INTERVAL"
_FISCALDATA_MIN_INTERVAL_ENV = "VIBE_TRADING_FISCALDATA_MIN_INTERVAL"
_MIN_INTERVAL_S = 0.5

#: Curve tenors in the compact vocabulary shared with ``fred``, mapped to the
#: exact column label the Treasury CSV publishes. Insertion order is the curve
#: order (the CSV column order). The compact key is what this adapter *emits*
#: and what a caller may request; the Treasury label is accepted on input too,
#: so ``"10Y"`` and ``"10 Yr"`` name the same column.
_TENOR_LABELS: dict[str, str] = {
    "1M": "1 Mo",
    "1.5M": "1.5 Month",
    "2M": "2 Mo",
    "3M": "3 Mo",
    "4M": "4 Mo",
    "6M": "6 Mo",
    "1Y": "1 Yr",
    "2Y": "2 Yr",
    "3Y": "3 Yr",
    "5Y": "5 Yr",
    "7Y": "7 Yr",
    "10Y": "10 Yr",
    "20Y": "20 Yr",
    "30Y": "30 Yr",
}

#: Curve order used to sort rows within one date (the CSV column order).
_TENOR_ORDER: tuple[str, ...] = tuple(_TENOR_LABELS)

#: Accepted request spellings -> compact tenor, matched case-insensitively. Both
#: the compact form (``"10Y"``) and the Treasury label (``"10 Yr"``) resolve to
#: the same key so a caller's spelling cannot cost a request.
_TENOR_ALIASES: dict[str, str] = {
    spelling.casefold(): compact
    for compact, label in _TENOR_LABELS.items()
    for spelling in (compact, label)
}

#: Treasury CSV label -> compact tenor, the inverse of :data:`_TENOR_LABELS`.
_LABEL_TO_TENOR: dict[str, str] = {
    label.casefold(): compact for compact, label in _TENOR_LABELS.items()
}

#: The CSV is fetched one calendar year per request; a wider window would mean
#: that many round trips, so it is refused rather than silently truncated.
_MAX_YEARS = 12
#: fiscaldata's documented maximum page size.
_MAX_FISCALDATA_PAGE = 10_000
#: fiscaldata field names (``record_date`` etc.), kept as constants so the
#: normalised keys and the upstream keys cannot drift apart.
_DATE_COLUMN = "Date"


def _to_float(value: Any) -> float | None:
    """Coerce a CSV/fiscaldata cell to ``float``; ``None`` when unusable.

    A blank or non-numeric cell is a missing observation, not a zero rate: the
    Treasury CSV leaves tenors blank on days a tenor was not published.
    """
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _us_date_to_iso(value: Any) -> str | None:
    """Convert the CSV's ``MM/DD/YYYY`` day to ISO ``YYYY-MM-DD``, or ``None``."""
    text = str(value if value is not None else "").strip()
    try:
        return datetime.strptime(text, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def _tenor_rank(tenor: str) -> int:
    """Sort key placing tenors in curve order, unknown labels last."""
    try:
        return _TENOR_ORDER.index(tenor)
    except ValueError:
        return len(_TENOR_ORDER)


def _latest_curve(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the most recent observation of each tenor — the latest curve.

    ``fred`` defaults to one row per tenor for the same category, so this is
    what makes an unqualified ``risk_free_curve`` call mean the same thing on
    either provider. Input order does not matter.

    Args:
        rows: Normalised ``{date, tenor, rate}`` records with compact tenors.

    Returns:
        One row per tenor (the newest date it was published on), in curve order.
    """
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        current = latest.get(row["tenor"])
        if current is None or row["date"] > current["date"]:
            latest[row["tenor"]] = row
    return sorted(latest.values(), key=lambda row: _tenor_rank(row["tenor"]))


def _parse_curve_csv(text: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse one year of the Treasury daily yield-curve CSV.

    The header is the authority on tenors — older years publish fewer columns
    (the 1990 file has only ``3 Mo`` through ``30 Yr``) — so nothing is
    hard-coded about which tenors a year carries. Each published label is
    mapped onto the compact vocabulary shared with ``fred`` (``"10 Yr"`` ->
    ``"10Y"``); an unrecognised column keeps its own label rather than being
    dropped.

    Args:
        text: Raw CSV body.

    Returns:
        ``(rows, tenors)`` where ``rows`` are ``{date, tenor, rate}`` records in
        file order (the upstream file is newest-first) with ``tenor`` compact,
        and ``tenors`` are the compact labels of the columns present. Both are
        empty for an empty body or a body whose header carries no ``Date``
        column (junk, not an empty curve — the caller decides).
    """
    reader = csv.DictReader(io.StringIO(text))
    fieldnames = [name for name in (reader.fieldnames or []) if name is not None]
    date_key = next(
        (name for name in fieldnames if name.strip().casefold() == _DATE_COLUMN.casefold()),
        None,
    )
    if date_key is None:
        return [], []
    columns = [
        (name, _LABEL_TO_TENOR.get(name.strip().casefold(), name.strip()))
        for name in fieldnames
        if name != date_key
    ]
    tenors = [compact for _, compact in columns]

    rows: list[dict[str, Any]] = []
    for record in reader:
        day = _us_date_to_iso(record.get(date_key))
        if day is None:
            continue
        for name, compact in columns:
            rate = _to_float(record.get(name))
            if rate is None:
                continue
            rows.append({"date": day, "tenor": compact, "rate": rate})
    return rows, tenors


@register_provider
class FederalReserveProvider(Provider):
    """Key-less U.S. Treasury adapter (daily curve CSV + fiscaldata)."""

    name = "federal_reserve"
    env_keys: tuple[str, ...] = ()
    capabilities = {
        "risk_free_curve": "risk_free_curve",
        "macro_data": "macro_data",
    }

    # -- risk_free_curve ---------------------------------------------------

    def risk_free_curve(
        self,
        *,
        tenor: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Treasury daily par yield-curve observations.

        Args:
            tenor: Optional tenor filter, accepted either as the compact curve
                form (``"3M"``, ``"1Y"``, ``"10Y"``, ``"30Y"``) or as the
                Treasury CSV label (``"3 Mo"``, ``"1 Yr"``, ``"10 Yr"``, ...);
                both name the same column.
            start: Inclusive ``YYYY-MM-DD`` lower bound.
            end: Inclusive ``YYYY-MM-DD`` upper bound.
            limit: Keep only the most recent *limit* rows overall after
                filtering (a time series). Omitted, the call returns the latest
                curve — the most recent observation of each tenor — matching
                ``fred``'s default for the same category.

        Returns:
            Rows of ``{date, tenor, rate}`` with ``tenor`` in the compact form
            shared with ``fred``. With a ``limit`` they ascend by date and curve
            order; without one there is exactly one row per tenor, in curve
            order.

        Raises:
            ProviderHTTPError: A malformed date bound, a tenor spelling the
                curve does not publish, a window wider than :data:`_MAX_YEARS`,
                or a body with no usable row (an unpublished year answers 200
                with an empty body).
        """
        start_bound = self._bound(start, "start", category="risk_free_curve")
        end_bound = self._bound(end, "end", category="risk_free_curve")
        wanted = None
        if tenor is not None and str(tenor).strip():
            wanted = _TENOR_ALIASES.get(str(tenor).strip().casefold())
            if wanted is None:
                known = ", ".join(_TENOR_ORDER)
                raise ProviderHTTPError(
                    self.name,
                    f"unknown Treasury tenor {tenor!r}; known: {known}",
                    category="risk_free_curve",
                )

        rows: list[dict[str, Any]] = []
        available: list[str] = []
        for year in self._years(start_bound, end_bound, category="risk_free_curve"):
            body = fetch_text(
                provider=self.name,
                category="risk_free_curve",
                url=_curve_csv_url(year),
                host_key=_TREASURY_HOST_KEY,
                min_interval_env=_TREASURY_MIN_INTERVAL_ENV,
                min_interval_default=_MIN_INTERVAL_S,
            )
            year_rows, year_tenors = _parse_curve_csv(body)
            rows.extend(year_rows)
            for label in year_tenors:
                if label not in available:
                    available.append(label)

        if wanted:
            rows = [row for row in rows if row["tenor"] == wanted]
            if not rows:
                labels = ", ".join(sorted(available, key=_tenor_rank)) or "none"
                raise ProviderHTTPError(
                    self.name,
                    f"unknown Treasury tenor {tenor!r}; the daily curve publishes: {labels}",
                    category="risk_free_curve",
                )

        if start_bound:
            rows = [row for row in rows if row["date"] >= start_bound]
        if end_bound:
            rows = [row for row in rows if row["date"] <= end_bound]
        if not rows:
            raise ProviderHTTPError(
                self.name,
                "the Treasury daily yield-curve CSV returned no usable rows"
                f" for {start_bound or 'the current year'}..{end_bound or 'today'}",
                category="risk_free_curve",
            )

        rows.sort(key=lambda row: (row["date"], _tenor_rank(row["tenor"])))
        if limit and limit > 0:
            return rows[-int(limit):]
        return _latest_curve(rows)

    # -- macro_data --------------------------------------------------------

    def macro_data(
        self,
        *,
        series_id: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = 1000,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Average interest rates the Treasury pays on its securities.

        Args:
            series_id: A ``security_desc`` value from the fiscaldata
                *avg_interest_rates* dataset, matched case-sensitively by the
                upstream filter (e.g. ``"Treasury Notes"``, ``"Treasury
                Bills"``, ``"Treasury Bonds"``, ``"Total Interest-bearing
                Debt"``). Required: the dataset is one row per (month, security
                type), so without a selector there is no single series.
            start: Inclusive ``YYYY-MM-DD`` lower bound (``record_date``).
            end: Inclusive ``YYYY-MM-DD`` upper bound (``record_date``).
            limit: Keep only the most recent *limit* rows (``<= 0`` keeps all).

        Returns:
            Ascending rows of ``{date, value}`` with the provider's
            ``security``/``security_type`` extras.

        Raises:
            ProviderHTTPError: No ``series_id``; an envelope without a ``data``
                array; or zero rows from a series that was *not* narrowed by a
                date window — fiscaldata answers an unknown ``security_desc``
                with an empty 200, and a silently empty series would hide the
                typo from the chain.
        """
        if not series_id:
            raise ProviderHTTPError(
                self.name,
                "macro_data needs series_id (a fiscaldata security_desc, "
                "e.g. 'Treasury Notes')",
                category="macro_data",
            )
        start_bound = self._bound(start, "start", category="macro_data")
        end_bound = self._bound(end, "end", category="macro_data")

        filters = [f"security_desc:eq:{str(series_id).strip()}"]
        if start_bound:
            filters.append(f"record_date:gte:{start_bound}")
        if end_bound:
            filters.append(f"record_date:lte:{end_bound}")

        payload = fetch_json(
            provider=self.name,
            category="macro_data",
            url=_FISCALDATA_URL,
            host_key=_FISCALDATA_HOST_KEY,
            min_interval_env=_FISCALDATA_MIN_INTERVAL_ENV,
            min_interval_default=_MIN_INTERVAL_S,
            params={
                "filter": ",".join(filters),
                "sort": "record_date",
                "page[size]": _MAX_FISCALDATA_PAGE,
                "fields": (
                    "record_date,security_type_desc,security_desc,"
                    "avg_interest_rate_amt"
                ),
            },
        )
        records = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(records, list):
            raise ProviderHTTPError(
                self.name,
                "fiscaldata payload had no data array",
                category="macro_data",
            )

        rows = [
            {
                "date": str(record.get("record_date") or "") or None,
                "value": _to_float(record.get("avg_interest_rate_amt")),
                "security": record.get("security_desc"),
                "security_type": record.get("security_type_desc"),
            }
            for record in records
            if isinstance(record, dict)
        ]
        rows = [row for row in rows if row["date"] and row["value"] is not None]

        if not rows and not (start_bound or end_bound):
            raise ProviderHTTPError(
                self.name,
                f"fiscaldata has no avg_interest_rates series named {series_id!r} "
                "(security_desc is case-sensitive)",
                category="macro_data",
            )
        if limit and limit > 0:
            rows = rows[-int(limit):]
        return rows

    # -- helpers -----------------------------------------------------------

    def _bound(self, value: Any, field: str, *, category: str) -> str | None:
        """Validate an optional ``YYYY-MM-DD`` request bound.

        Args:
            value: Caller-supplied bound (``None``/blank means "no bound").
            field: Parameter name, for the raised error.
            category: Category the call belongs to, for the raised error.

        Returns:
            The ISO date, or ``None`` when no bound was requested.

        Raises:
            ProviderHTTPError: The bound is not a parseable date.
        """
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
        except ValueError as exc:
            raise ProviderHTTPError(
                self.name,
                f"{field} must be YYYY-MM-DD, got {text!r}",
                category=category,
            ) from exc

    def _years(self, start: str | None, end: str | None, *, category: str) -> list[int]:
        """Calendar years to request for a date window, oldest first.

        Args:
            start: Validated inclusive lower bound, or ``None``.
            end: Validated inclusive upper bound, or ``None``.
            category: Category the call belongs to, for the raised error.

        Returns:
            One year per CSV request: the window's years, the current UTC year
            when the window is open-ended, and exactly the current year when no
            bound was given. A start year still in the future is requested as
            itself (the file is empty, which the caller reports) rather than
            silently swapped for the current year.

        Raises:
            ProviderHTTPError: The window spans more than :data:`_MAX_YEARS`.
        """
        today = datetime.now(timezone.utc).year
        first = int(start[:4]) if start else int(end[:4]) if end else today
        last = int(end[:4]) if end else today
        if last < first:
            last = first
        if last - first + 1 > _MAX_YEARS:
            raise ProviderHTTPError(
                self.name,
                f"window spans {last - first + 1} years; the daily curve CSV is "
                "one calendar year per request",
                category=category,
            )
        return list(range(first, last + 1))


def _curve_csv_url(year: int) -> str:
    """Build the per-year daily yield-curve CSV URL.

    Args:
        year: Calendar year to fetch.

    Returns:
        The fully-qualified URL. The empty ``page`` parameter is what the
        Treasury site itself sends; dropping it made no measurable difference
        on 2026-10-08 and it is kept to stay byte-identical to the verified URL.
    """
    return (
        f"{_CSV_BASE_URL}/{year}/all"
        f"?type=daily_treasury_yield_curve&field_tdr_date_value={year}"
        f"&page&_format=csv"
    )
