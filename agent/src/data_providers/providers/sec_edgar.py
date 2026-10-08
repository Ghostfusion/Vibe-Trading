"""SEC EDGAR provider adapter.

The U.S. SEC publishes free, no-auth JSON for every reporting company: a
ticker→CIK directory, a recent-filing index ("submissions") and the full set of
XBRL concepts a company has reported ("companyfacts"). This adapter serves the
single-vendor ``sec_filings`` category from the submissions document and, as a
proposed chain addition, ``fundamental_data`` from companyfacts.

Endpoints used (all measured 200 with a declared ``User-Agent`` on 2026-10-08):

* ``https://data.sec.gov/submissions/CIK##########.json`` — the filing index.
* ``https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`` — XBRL facts.
* ``https://www.sec.gov/files/company_tickers.json`` — ticker→CIK map, fetched
  through the shared client below rather than here.
* ``api/xbrl/companyconcept`` and ``api/xbrl/frames`` answer 200 too, but the
  concept endpoint is a single-concept slice of companyfacts and the frames
  endpoint is a cross-company slice; neither maps to a category, so neither is
  implemented.

Fair access is not optional here. SEC rate-limits by source IP and asks every
client for a descriptive ``User-Agent`` carrying a contact address; bursting
without one earns a temporary block. So this adapter reuses
:mod:`backtest.loaders.sec_edgar_client` for the three things that must not
drift — the ``VIBE_TRADING_SEC_UA`` override plus default contact UA, the
``"sec"`` throttle bucket with its 0.12 s spacing floor (≈8 req/s, under the
10 req/s ceiling), and the ticker→CIK resolution with its memoized table — and
issues the requests through :func:`~src.data_providers._http.fetch_json` so a
404/403/429 is classified as :class:`ProviderHTTPError` for the failover layer
exactly like every other adapter.
"""

from __future__ import annotations

from typing import Any

import requests

from backtest.loaders.sec_edgar_client import (
    _COMPANY_FACTS_URL,
    _DEFAULT_SEC_UA,
    _HOST_KEY,
    _MIN_INTERVAL_DEFAULT,
    _MIN_INTERVAL_ENV,
    _SUBMISSIONS_URL,
    _UA_ENV,
    cik_for,
)
from src.data_providers._http import fetch_json
from src.data_providers.base import Provider, env_value
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_TIMEOUT_S = 30.0

#: SEC primary-document URLs are built from the un-padded CIK and the accession
#: number with its dashes stripped.
_DOC_BASE = "https://www.sec.gov/Archives/edgar/data"

#: Points kept per concept when building ``fundamental_data`` statements.
_DEFAULT_POINT_LIMIT = 4

#: Curated us-gaap concepts per primary statement. Deliberately short: the
#: category's shape is a period/statement summary, and companyfacts carries
#: hundreds of concepts whose full dump belongs in the raw client, not here.
_STATEMENT_CONCEPTS: dict[str, tuple[str, ...]] = {
    "income": (
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "CostOfRevenue",
        "GrossProfit",
        "OperatingIncomeLoss",
        "NetIncomeLoss",
        "EarningsPerShareBasic",
        "EarningsPerShareDiluted",
    ),
    "balance": (
        "Assets",
        "AssetsCurrent",
        "CashAndCashEquivalentsAtCarryingValue",
        "Liabilities",
        "LiabilitiesCurrent",
        "StockholdersEquity",
        "LongTermDebtNoncurrent",
    ),
    "cashflow": (
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInInvestingActivities",
        "NetCashProvidedByUsedInFinancingActivities",
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsOfDividends",
        "PaymentsForRepurchaseOfCommonStock",
    ),
}


def _user_agent() -> str:
    """Return the declared SEC User-Agent: env override or the client default.

    The default and the ``VIBE_TRADING_SEC_UA`` override are the ones
    ``backtest.loaders.sec_edgar_client`` already uses, so this adapter and the
    tool/loader layers present the same identity to the SEC.

    Returns:
        A non-empty User-Agent string carrying a contact address.
    """
    return env_value(_UA_ENV) or _DEFAULT_SEC_UA


@register_provider
class SecEdgarProvider(Provider):
    """No-key SEC EDGAR adapter (declared User-Agent, shared SEC throttle)."""

    name = "sec_edgar"
    env_keys = ()
    capabilities = {
        "sec_filings": "sec_filings",
        "fundamental_data": "fundamental_data",
    }

    def _get(self, url: str, *, category: str) -> Any:
        """GET one SEC JSON document with the declared User-Agent.

        Args:
            url: Fully-qualified SEC endpoint URL.
            category: Category the call belongs to, for the raised error.

        Returns:
            The decoded JSON body.

        Raises:
            ProviderHTTPError: On a transport error, a non-2xx status, or a body
                that is not JSON (all classified by ``fetch_json``).
        """
        return fetch_json(
            provider=self.name,
            category=category,
            url=url,
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_MIN_INTERVAL_DEFAULT,
            headers={"User-Agent": _user_agent(), "Accept": "application/json"},
            timeout=_TIMEOUT_S,
        )

    def _cik(self, ticker: str, *, category: str) -> str:
        """Resolve *ticker* to a zero-padded CIK via the shared SEC table.

        Args:
            ticker: U.S. equity ticker, e.g. ``AAPL`` or ``AAPL.US``.
            category: Category the call belongs to, for the raised error.

        Returns:
            The CIK as exactly 10 digits.

        Raises:
            ProviderHTTPError: When the shared ticker-table fetch fails, or the
                ticker is absent from it (EDGAR covers U.S. filers only).
        """
        try:
            cik = cik_for(ticker)
        except requests.RequestException as exc:
            raise ProviderHTTPError(
                self.name,
                f"SEC ticker table request failed: {type(exc).__name__}: {exc}",
                category=category,
            ) from exc
        if not cik:
            raise ProviderHTTPError(
                self.name,
                f"ticker {ticker!r} not found in the SEC company table (US only)",
                category=category,
            )
        return cik

    # -- sec_filings -------------------------------------------------------

    def sec_filings(
        self,
        *,
        symbol: str | None = None,
        ticker: str | None = None,
        form: str | None = None,
        limit: int = 20,
        **_: Any,
    ) -> dict[str, Any]:
        """Recent SEC filings for one U.S. ticker.

        Args:
            symbol: Ticker, e.g. ``AAPL`` (``ticker`` wins when both are given).
            ticker: Alias for ``symbol``.
            form: Optional SEC form filter, case-insensitive (``10-K``, ``8-K``).
            limit: Maximum filings to return, newest first.

        Returns:
            ``{"symbol", "cik", "name", "filings": [...]}`` where each filing is
            ``{form, accession_number, filing_date, report_date,
            primary_document, description, document_url}``.

        Raises:
            ProviderHTTPError: For a blank ticker, an unresolvable ticker, or a
                refused/junk submissions response.
        """
        requested = ticker if ticker is not None else symbol
        if not isinstance(requested, str) or not requested.strip():
            raise ProviderHTTPError(
                self.name,
                "'symbol' (a U.S. ticker) is required for sec_filings",
                category="sec_filings",
            )
        ticker_symbol = requested.strip().upper()
        cik = self._cik(ticker_symbol, category="sec_filings")
        payload = self._get(
            _SUBMISSIONS_URL.format(cik=cik), category="sec_filings"
        )
        if not isinstance(payload, dict):
            raise ProviderHTTPError(
                self.name, "submissions payload was not an object", category="sec_filings"
            )
        form_filter = form.strip().upper() if isinstance(form, str) and form.strip() else None
        filings = _parse_filings(payload, form_filter, cik)
        if isinstance(limit, int) and limit > 0:
            filings = filings[:limit]
        return {
            "symbol": ticker_symbol,
            "cik": cik,
            "name": payload.get("name"),
            "filings": filings,
        }

    # -- fundamental_data --------------------------------------------------

    def fundamental_data(
        self,
        *,
        symbol: str | None = None,
        ticker: str | None = None,
        limit: int = _DEFAULT_POINT_LIMIT,
        **_: Any,
    ) -> dict[str, Any]:
        """XBRL facts for one U.S. ticker, summarised per period and statement.

        Args:
            symbol: Ticker, e.g. ``AAPL`` (``ticker`` wins when both are given).
            ticker: Alias for ``symbol``.
            limit: Most recent points kept per concept (and the period cap).

        Returns:
            ``{"symbol", "cik", "name", "currency", "periods": [...],
            "statements": {"income": [...], "balance": [...], "cashflow": [...]}}``.
            Each period is ``{period_type, start, end, fiscal_year,
            fiscal_period, form, accession, frame}``; each statement row is
            ``{concept, label, unit, value, start, end, fiscal_year,
            fiscal_period, form, accession, period_type, frame}``.

        Raises:
            ProviderHTTPError: For a blank ticker, an unresolvable ticker, or a
                refused/junk companyfacts response.
        """
        requested = ticker if ticker is not None else symbol
        if not isinstance(requested, str) or not requested.strip():
            raise ProviderHTTPError(
                self.name,
                "'symbol' (a U.S. ticker) is required for fundamental_data",
                category="fundamental_data",
            )
        ticker_symbol = requested.strip().upper()
        cik = self._cik(ticker_symbol, category="fundamental_data")
        payload = self._get(
            _COMPANY_FACTS_URL.format(cik=cik), category="fundamental_data"
        )
        facts = payload.get("facts") if isinstance(payload, dict) else None
        us_gaap = facts.get("us-gaap") if isinstance(facts, dict) else None
        if not isinstance(us_gaap, dict):
            raise ProviderHTTPError(
                self.name,
                "companyfacts payload carried no us-gaap facts",
                category="fundamental_data",
            )
        points = limit if isinstance(limit, int) and limit > 0 else _DEFAULT_POINT_LIMIT
        statements = {
            statement: _statement_rows(us_gaap, concepts, points)
            for statement, concepts in _STATEMENT_CONCEPTS.items()
        }
        statements = {name: rows for name, rows in statements.items() if rows}
        return {
            "symbol": ticker_symbol,
            "cik": cik,
            "name": payload.get("entityName"),
            "currency": "USD",
            "periods": _periods(statements, points),
            "statements": statements,
        }


def _parse_filings(
    submissions: dict[str, Any], form_filter: str | None, cik: str
) -> list[dict[str, Any]]:
    """Extract recent filings from a submissions payload, newest first.

    The SEC stores the recent filing index as parallel arrays under
    ``filings.recent`` (``form``, ``accessionNumber``, ``filingDate``, ...); the
    arrays are positionally aligned. A row whose form does not match
    ``form_filter`` is skipped and a malformed row never aborts the batch.

    Args:
        submissions: Decoded submissions document.
        form_filter: Upper-cased form type to keep, or ``None`` for all forms.
        cik: Padded CIK, used to build primary-document URLs.

    Returns:
        Normalized filing rows in the SEC's served (newest-first) order.
    """
    filings = submissions.get("filings")
    recent = filings.get("recent") if isinstance(filings, dict) else None
    if not isinstance(recent, dict):
        return []

    forms = recent.get("form") if isinstance(recent.get("form"), list) else []
    accessions = recent.get("accessionNumber") or []
    filing_dates = recent.get("filingDate") or []
    report_dates = recent.get("reportDate") or []
    primary_docs = recent.get("primaryDocument") or []
    descriptions = recent.get("primaryDocDescription") or []

    out: list[dict[str, Any]] = []
    for index, raw_form in enumerate(forms):
        form_type = str(raw_form).strip() if raw_form is not None else ""
        if form_filter is not None and form_type.upper() != form_filter:
            continue
        accession = _at(accessions, index)
        primary_doc = _at(primary_docs, index)
        out.append(
            {
                "form": form_type or None,
                "accession_number": accession,
                "filing_date": _at(filing_dates, index),
                "report_date": _at(report_dates, index) or None,
                "primary_document": primary_doc or None,
                "description": _at(descriptions, index) or None,
                "document_url": _document_url(cik, accession, primary_doc),
            }
        )
    return out


def _at(sequence: Any, index: int) -> Any:
    """Return ``sequence[index]`` when it exists, else ``None``.

    The SEC's parallel arrays can be ragged; a short array must not raise.
    """
    if isinstance(sequence, list) and index < len(sequence):
        return sequence[index]
    return None


def _document_url(cik: str, accession: Any, primary_doc: Any) -> str | None:
    """Build the SEC primary-document URL, or ``None`` when a part is missing.

    Args:
        cik: Padded or un-padded CIK; leading zeros are dropped for the URL.
        accession: Accession number like ``0000320193-23-000106``.
        primary_doc: Primary document filename within the filing.

    Returns:
        A fully-qualified ``sec.gov`` document URL, or ``None``.
    """
    if not accession or not primary_doc:
        return None
    cik_digits = str(cik).lstrip("0") or "0"
    accession_nodash = str(accession).replace("-", "")
    return f"{_DOC_BASE}/{cik_digits}/{accession_nodash}/{primary_doc}"


def _statement_rows(
    us_gaap: dict[str, Any], concepts: tuple[str, ...], limit: int
) -> list[dict[str, Any]]:
    """Flatten a statement's curated concepts into recent point rows.

    Args:
        us_gaap: The ``facts["us-gaap"]`` mapping.
        concepts: Concept names to look for, in display order.
        limit: Most recent points kept per concept.

    Returns:
        Rows of ``{concept, label, unit, value, start, end, fiscal_year,
        fiscal_period, form, accession, period_type, frame}``.
    """
    from backtest.loaders.sec_frames import classify_span, span_days  # noqa: PLC0415

    rows: list[dict[str, Any]] = []
    for concept in concepts:
        entry = us_gaap.get(concept)
        if not isinstance(entry, dict):
            continue
        unit, points = _densest_unit(entry)
        if not points:
            continue
        ordered = sorted(
            (point for point in points if isinstance(point, dict)),
            key=lambda point: (str(point.get("end") or ""), str(point.get("start") or "")),
        )
        label = entry.get("label") or concept
        for point in ordered[-limit:]:
            rows.append(
                {
                    "concept": concept,
                    "label": label,
                    "unit": unit,
                    "value": point.get("val"),
                    "start": point.get("start"),
                    "end": point.get("end"),
                    "fiscal_year": point.get("fy"),
                    "fiscal_period": point.get("fp"),
                    "form": point.get("form"),
                    "accession": point.get("accn"),
                    "period_type": classify_span(span_days(point)),
                    "frame": point.get("frame"),
                }
            )
    return rows


def _densest_unit(entry: dict[str, Any]) -> tuple[str | None, list[Any]]:
    """Return the unit bucket with the most rows for one companyfacts concept.

    A concept is reported in one or more units (``USD``, ``USD/shares``,
    ``shares``); the densest bucket is the one whose series is complete enough to
    summarize.

    Args:
        entry: A ``facts["us-gaap"][concept]`` object.

    Returns:
        ``(unit_name, rows)``; ``(None, [])`` when the concept carries no units.
    """
    units = entry.get("units")
    if not isinstance(units, dict):
        return None, []
    best_name: str | None = None
    best_rows: list[Any] = []
    for name, rows in units.items():
        if isinstance(rows, list) and len(rows) > len(best_rows):
            best_name, best_rows = str(name), rows
    return best_name, best_rows


def _periods(
    statements: dict[str, list[dict[str, Any]]], limit: int
) -> list[dict[str, Any]]:
    """Derive the distinct reporting periods named by the statement rows.

    The identity of a period is its ``(start, end)`` span, not its ``end`` date:
    a 10-Q reports the true quarter and the year-to-date frame under the same
    ``end``. ``period_type`` comes from ``backtest.loaders.sec_frames`` so the
    threshold rule lives in one place.

    Args:
        statements: Statement name -> rows from :func:`_statement_rows`.
        limit: Maximum periods returned, newest first.

    Returns:
        Rows of ``{period_type, start, end, fiscal_year, fiscal_period, form,
        accession, frame}``.
    """
    seen: dict[tuple[Any, ...], dict[str, Any]] = {}
    for rows in statements.values():
        for row in rows:
            key = (
                row.get("start"),
                row.get("end"),
                row.get("fiscal_year"),
                row.get("fiscal_period"),
                row.get("form"),
                row.get("accession"),
            )
            if key in seen:
                continue
            seen[key] = {
                "period_type": row.get("period_type"),
                "start": row.get("start"),
                "end": row.get("end"),
                "fiscal_year": row.get("fiscal_year"),
                "fiscal_period": row.get("fiscal_period"),
                "form": row.get("form"),
                "accession": row.get("accession"),
                "frame": row.get("frame"),
            }
    ordered = sorted(
        seen.values(),
        key=lambda row: (str(row.get("end") or ""), str(row.get("start") or "")),
        reverse=True,
    )
    return ordered[:limit]
