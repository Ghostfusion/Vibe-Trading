"""Benzinga provider adapter.

Base URL ``https://api.benzinga.com/api``; the credential rides in the ``token``
query parameter for every endpoint (env name ``BENZINGA_API_KEY``). XML was the
legacy default output format, so every request here sends
``Accept: application/json`` explicitly — without the header some routes answer
with an XML body that :func:`~src.data_providers._http.fetch_json` would reject
as non-JSON.

Two response envelopes are in play and both are handled below:

* News routes answer with a **bare JSON array**.
* Calendar routes wrap their rows (``{"earnings": [...]}``, ``{"ratings": [...]}``,
  ``{"dividends": [...]}``, ...); the wrapper key is the endpoint's own name.

Symbol convention: the project writes US equities as ``AAPL.US`` while Benzinga
carries US tickers bare and answers a suffixed ticker with an empty array rather
than an error, so every symbol is passed through :func:`_benzinga_symbol` (which
strips ``.US``) before it reaches the query string.

Tier note (measured against the configured key on 2026-10-08 with
``?token=<BENZINGA_API_KEY>``). Implemented and answering 200: ``/v2/news``,
``/v2/news-removed``, ``/v1/consensus-ratings`` and the ``/v2.1/calendar/*``
endpoints for earnings, dividends, splits, offerings, guidance, economics, fda
and ratings. Products sold separately answer **401 auth_failed** on this key —
``/v2.1/fundamentals``, ``/v2.1/fundamentals/financials``, ``/api/v2/bars``,
``/api/v2/quoteDelayed``, ``/api/v1/market/movers``, ``/api/v1/shortinterest`` —
and are deliberately **not** implemented: a capability that can only ever fail
would be a fake chain slot. Three documented routes answer **403 Unauthorized
for Route** (``/api/v2/calendar/events``, ``/api/v1/analyst/insights``,
``/api/v1/bulls_bears_say``) and are also omitted. ``/v2.1/insider`` is
**404 no Route matched** — the real insider route is
``/api/v1/sec/insider_transactions/{filings|transactions}``, which answers 200
but lands in no category this adapter is asked to serve.
"""

from __future__ import annotations

from email.utils import parsedate_to_datetime
from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider, env_value
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_PROVIDER = "benzinga"
_BASE_URL = "https://api.benzinga.com/api"
_API_KEY_ENV = "BENZINGA_API_KEY"
_HOST_KEY = "benzinga"
_MIN_INTERVAL_ENV = "VIBE_TRADING_BENZINGA_MIN_INTERVAL"
_DEFAULT_MIN_INTERVAL_S = 0.5

#: XML is the legacy default on this API; every route wants JSON instead.
_JSON_HEADERS = {"Accept": "application/json"}

#: Calendar endpoints cap the page size at 1000; news caps at 100.
_CALENDAR_MAX_PAGE_SIZE = 1000
_NEWS_MAX_PAGE_SIZE = 100

#: Calendar routes and the key their rows are wrapped under.
_EARNINGS_PATH = "/v2.1/calendar/earnings"
_DIVIDENDS_PATH = "/v2.1/calendar/dividends"
_SPLITS_PATH = "/v2.1/calendar/splits"
_OFFERINGS_PATH = "/v2.1/calendar/offerings"
_GUIDANCE_PATH = "/v2.1/calendar/guidance"
_ECONOMICS_PATH = "/v2.1/calendar/economics"
_FDA_PATH = "/v2.1/calendar/fda"
_RATINGS_PATH = "/v2.1/calendar/ratings"
_CONSENSUS_PATH = "/v1/consensus-ratings"
_NEWS_PATH = "/v2/news"
_NEWS_REMOVED_PATH = "/v2/news-removed"


def _benzinga_symbol(code: str) -> str:
    """Translate a project symbol into Benzinga's ticker convention.

    The project writes US equities as ``AAPL.US`` while Benzinga carries US
    tickers bare and never accepts the suffixed form — a suffixed request
    answers 200 with an empty array, which would read as "no news/rows" rather
    than a wrong request. Any other suffix is a global code and passes through.

    Args:
        code: Project-side symbol, e.g. ``AAPL.US`` or a bare ``MSFT``.

    Returns:
        The Benzinga ticker.
    """
    upper = code.strip().upper()
    if upper.endswith(".US"):
        return upper[: -len(".US")]
    return upper


def _num(value: Any) -> float | None:
    """Coerce a Benzinga string-encoded number to ``float``.

    Benzinga encodes every numeric field as a string (``"355.0000"``) and uses
    ``""`` for "not reported", so a plain ``float()`` is not safe.

    Args:
        value: Raw field value.

    Returns:
        The parsed float, or ``None`` when the field is empty or non-numeric.
    """
    if value is None or isinstance(value, bool) or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _iso(value: Any) -> str | None:
    """Normalise Benzinga's RFC-2822 timestamps to ISO-8601.

    News items carry ``created``/``updated`` as ``Thu, 08 Oct 2026 13:44:04
    -0400``; the project's payload convention is ISO-8601 strings, so the two
    differ and this conversion keeps news comparable with every other provider.

    Args:
        value: Raw timestamp string.

    Returns:
        The ISO-8601 rendering (offset preserved), the input when it cannot be
        parsed, or ``None`` when absent.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        return parsedate_to_datetime(value).isoformat()
    except (TypeError, ValueError):
        return value


def _hour(time_of_day: Any) -> str | None:
    """Map Benzinga's release clock time to the chain's ``bmo``/``amc`` hour.

    Benzinga reports a wall-clock ``time`` (``"16:00:00"``) rather than the
    ``bmo``/``amc`` vocabulary the rest of the earnings chain uses, so the raw
    value is kept in the row's ``time`` extra and the ``hour`` field is derived:
    a release at or after 16:00 is after the close, one before 09:30 is before
    the open, anything in between is left unknown rather than guessed.

    Args:
        time_of_day: Raw ``HH:MM:SS`` release time.

    Returns:
        ``"amc"``, ``"bmo"``, or ``None`` when the value is absent/ambiguous.
    """
    if not isinstance(time_of_day, str) or ":" not in time_of_day:
        return None
    try:
        hour, minute = (int(part) for part in time_of_day.split(":")[:2])
    except ValueError:
        return None
    if hour >= 16:
        return "amc"
    if (hour, minute) < (9, 30):
        return "bmo"
    return None


def _page_size(limit: Any) -> int:
    """Clamp a caller's ``limit`` to Benzinga's calendar page-size window."""
    try:
        size = int(limit)
    except (TypeError, ValueError):
        size = 100
    return max(1, min(size, _CALENDAR_MAX_PAGE_SIZE))


def _rows(payload: Any, key: str, *, category: str) -> list[dict[str, Any]]:
    """Return the row list from a Benzinga response body.

    News routes answer with a bare array; calendar routes wrap their rows under
    the endpoint's name. Anything else (a dict where a list is expected, an XML
    body parsed as text) is junk and must fail the chain rather than look like
    an empty result.

    Args:
        payload: Decoded JSON body.
        key: Wrapper key for the calendar form.
        category: Category the call belongs to, for the raised error.

    Returns:
        The row dicts, in upstream order.

    Raises:
        ProviderHTTPError: The body did not carry an array of rows.
    """
    rows = payload.get(key) if isinstance(payload, dict) else payload
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    raise ProviderHTTPError(
        _PROVIDER,
        f"expected an array under {key!r}, got {type(rows).__name__}",
        category=category,
    )


@register_provider
class BenzingaProvider(Provider):
    """Key-gated Benzinga REST adapter (news + calendar products)."""

    name = _PROVIDER
    env_keys = (_API_KEY_ENV,)
    capabilities = {
        "news_data": "news_data",
        "news_retractions": "news_retractions",
        "analyst_ratings": "analyst_ratings",
        "analyst_actions": "analyst_actions",
        "earnings_calendar": "earnings_calendar",
        "corporate_actions": "corporate_actions",
        "offerings_calendar": "offerings_calendar",
        "guidance_revisions": "guidance_revisions",
        "fda_calendar": "fda_calendar",
        "economic_calendar": "economic_calendar",
    }

    def _get(self, path: str, *, category: str, params: dict[str, Any] | None = None) -> Any:
        """Call one Benzinga endpoint with the configured token.

        Args:
            path: Route below ``/api``, e.g. ``/v2.1/calendar/earnings``.
            category: Category the call belongs to, for error reporting.
            params: Extra query parameters.

        Returns:
            The decoded JSON body.

        Raises:
            ProviderHTTPError: The key is missing, the transport failed, the
                upstream refused (401/403/404), or the body is not JSON.
        """
        token = env_value(_API_KEY_ENV)
        if not token:
            raise ProviderHTTPError(
                self.name, f"{_API_KEY_ENV} is not configured", category=category
            )
        query = dict(params or {})
        query["token"] = token
        payload = fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}{path}",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=query,
            headers=_JSON_HEADERS,
        )
        # Benzinga reports some failures as a 200 body with ``ok: false``; a
        # silent empty payload here would look like "no news today".
        if isinstance(payload, dict) and payload.get("ok") is False:
            raise ProviderHTTPError(
                self.name,
                f"error envelope: {payload.get('errors')!r}"[:240],
                category=category,
            )
        return payload

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
            symbol: Ticker filter (``tickers`` upstream), e.g. ``AAPL.US`` (the
                ``.US`` suffix is stripped). Required — Benzinga's
                news feed is ticker-scoped and has no free-text search, so a
                query-only request is refused and the chain moves on.
            query: Accepted for chain compatibility; ignored when ``symbol`` is
                set, because Benzinga only filters by ticker/channel/topic.
            limit: Maximum rows (Benzinga caps ``pageSize`` at 100).
            start: ``YYYY-MM-DD`` lower bound (``dateFrom``).
            end: ``YYYY-MM-DD`` upper bound (``dateTo``).

        Returns:
            Rows of ``{title, url, published, source, summary}`` plus ``author``
            and ``id`` extras.

        Raises:
            ProviderHTTPError: When no ticker was supplied.
        """
        if not symbol:
            raise ProviderHTTPError(
                self.name,
                "Benzinga news is ticker-scoped; a query-only news search is unsupported",
                category="news_data",
            )
        params: dict[str, Any] = {
            "tickers": _benzinga_symbol(symbol),
            "pageSize": max(1, min(int(limit), _NEWS_MAX_PAGE_SIZE)),
            "sort": "created:desc",
            # ``abstract`` fills ``teaser``; the default ``headline`` leaves the
            # body empty, which would make every summary None.
            "displayOutput": "abstract",
        }
        if start:
            params["dateFrom"] = start
        if end:
            params["dateTo"] = end
        rows = _rows(
            self._get(_NEWS_PATH, category="news_data", params=params),
            "news",
            category="news_data",
        )
        return [
            {
                "title": row.get("title"),
                "url": row.get("url"),
                "published": _iso(row.get("created")),
                "source": "benzinga",
                "summary": row.get("teaser") or None,
                "author": row.get("author"),
                "id": row.get("id"),
            }
            for row in rows
        ]

    # -- news_retractions --------------------------------------------------

    def news_retractions(
        self, *, limit: int = 100, since: str | int | None = None, **_: Any
    ) -> list[dict[str, Any]]:
        """Articles Benzinga has retracted or replaced.

        ``/v2/news-removed`` is a genuine retraction feed, but it is keyed by
        article id only — the endpoint does not echo the headline or url — so
        those two fields are ``None`` and the id is kept under ``article_id``.
        ``published`` carries the removal timestamp, which is what makes the
        row useful: it says *when* a story was pulled.

        Args:
            limit: Maximum rows to return (the endpoint's own page size is 100).
            since: Unix-seconds lower bound (``updatedSince``), when known.

        Returns:
            Rows of ``{title, url, published, source}`` plus ``article_id``.
        """
        params: dict[str, Any] = {}
        if since is not None and str(since) != "":
            params["updatedSince"] = str(since)
        rows = _rows(
            self._get(_NEWS_REMOVED_PATH, category="news_retractions", params=params),
            "news_removed",
            category="news_retractions",
        )
        return [
            {
                "title": None,
                "url": None,
                "published": _iso(row.get("updated")),
                "source": "benzinga",
                "article_id": row.get("id"),
            }
            for row in rows[: max(1, int(limit))]
        ]

    # -- analyst_ratings ---------------------------------------------------

    def analyst_ratings(self, *, symbol: str | None = None, **_: Any) -> dict[str, Any]:
        """Aggregate analyst consensus for *symbol*.

        ``/v1/consensus-ratings`` is queried with ``parameters[tickers]``; a
        tickerless call genuinely answers with an empty array (measured), so it
        is refused here rather than returned as an empty consensus.

        Args:
            symbol: Project symbol, e.g. ``AAPL.US`` (the ``.US`` suffix is
                stripped, since Benzinga carries US tickers bare).

        Returns:
            ``{"symbol", "consensus": {buy, hold, sell}, "summary"}`` plus the
            consensus rating, price targets and analyst count.

        Raises:
            ProviderHTTPError: When no ticker was supplied.
        """
        if not symbol:
            raise ProviderHTTPError(
                self.name,
                "Benzinga aggregate ratings are ticker-scoped; a tickerless request "
                "returns an empty set",
                category="analyst_ratings",
            )
        payload = self._get(
            _CONSENSUS_PATH,
            category="analyst_ratings",
            params={"parameters[tickers]": _benzinga_symbol(symbol)},
        )
        if not isinstance(payload, dict) or not payload:
            # A bare ``[]`` means the endpoint found nothing to aggregate; that
            # is an empty consensus, not a failure.
            return {"symbol": _benzinga_symbol(symbol), "consensus": {}, "summary": ""}
        aggregate = payload.get("aggregate_ratings")
        consensus = dict(aggregate) if isinstance(aggregate, dict) else {}
        rating = payload.get("consensus_rating")
        count = payload.get("total_analyst_count")
        target = payload.get("consensus_price_target")
        return {
            "symbol": _benzinga_symbol(symbol),
            "consensus": consensus,
            "summary": f"{rating or 'no'} consensus from {count or 0} analyst(s);"
            f" price target {target}",
            "consensus_rating": rating,
            "consensus_rating_value": payload.get("consensus_rating_val"),
            "consensus_price_target": target,
            "low_price_target": payload.get("low_price_target"),
            "high_price_target": payload.get("high_price_target"),
            "analyst_count": count,
            "unique_analyst_count": payload.get("unique_analyst_count"),
            "updated_at": payload.get("updated_at"),
        }

    # -- analyst_actions ---------------------------------------------------

    def analyst_actions(
        self,
        *,
        symbol: str | None = None,
        limit: int = 100,
        start: str | None = None,
        end: str | None = None,
        action: str | None = None,
        **_,
    ) -> list[dict[str, Any]]:
        """Individual analyst rating / price-target actions.

        Args:
            symbol: Ticker filter, e.g. ``AAPL.US`` (``.US`` stripped upstream).
            limit: Maximum rows (``pagesize`` upstream, capped at 1000).
            start: ``YYYY-MM-DD`` lower bound (``parameters[date_from]``).
            end: ``YYYY-MM-DD`` upper bound (``parameters[date_to]``).
            action: Upstream action filter, e.g. ``Downgrades``.

        Returns:
            Rows of ``{symbol, date, action, rating, target_price, firm}`` plus
            the prior rating/target and the analyst name.
        """
        params: dict[str, Any] = {"pagesize": _page_size(limit)}
        if symbol:
            params["parameters[tickers]"] = _benzinga_symbol(symbol)
        if start:
            params["parameters[date_from]"] = start
        if end:
            params["parameters[date_to]"] = end
        if action:
            params["parameters[action]"] = action
        rows = _rows(
            self._get(_RATINGS_PATH, category="analyst_actions", params=params),
            "ratings",
            category="analyst_actions",
        )
        return [
            {
                "symbol": row.get("ticker"),
                "date": row.get("date"),
                "action": row.get("action_company"),
                "rating": row.get("rating_current"),
                "target_price": _num(row.get("adjusted_pt_current") or row.get("pt_current")),
                "firm": row.get("analyst"),
                "prior_rating": row.get("rating_prior"),
                "prior_target_price": _num(row.get("adjusted_pt_prior") or row.get("pt_prior")),
                "unadjusted_target_price": _num(row.get("pt_current")),
                "analyst": row.get("analyst_name"),
                "target_change": row.get("action_pt"),
                "importance": row.get("importance"),
                "url": row.get("url"),
            }
            for row in rows[: _page_size(limit)]
        ]

    # -- earnings_calendar -------------------------------------------------

    def earnings_calendar(
        self,
        *,
        symbol: str | None = None,
        limit: int = 100,
        start: str | None = None,
        end: str | None = None,
        **_,
    ) -> list[dict[str, Any]]:
        """Earnings dates with estimates and (once reported) actuals.

        Args:
            symbol: Ticker filter, e.g. ``AAPL.US`` (``.US`` stripped upstream).
            limit: Maximum rows (``pagesize`` upstream, capped at 1000).
            start: ``YYYY-MM-DD`` lower bound (``parameters[date_from]``).
            end: ``YYYY-MM-DD`` upper bound (``parameters[date_to]``).

        Returns:
            Rows of ``{symbol, date, eps_estimate, revenue_estimate, hour}``
            plus actuals, surprise and period extras.
        """
        params: dict[str, Any] = {"pagesize": _page_size(limit)}
        if symbol:
            params["parameters[tickers]"] = _benzinga_symbol(symbol)
        if start:
            params["parameters[date_from]"] = start
        if end:
            params["parameters[date_to]"] = end
        rows = _rows(
            self._get(_EARNINGS_PATH, category="earnings_calendar", params=params),
            "earnings",
            category="earnings_calendar",
        )
        return [
            {
                "symbol": row.get("ticker"),
                "date": row.get("date"),
                "eps_estimate": _num(row.get("eps_est")),
                "revenue_estimate": _num(row.get("revenue_est")),
                "hour": _hour(row.get("time")),
                "eps_actual": _num(row.get("eps")),
                "eps_prior": _num(row.get("eps_prior")),
                "eps_surprise": _num(row.get("eps_surprise")),
                "eps_surprise_percent": _num(row.get("eps_surprise_percent")),
                "revenue_actual": _num(row.get("revenue")),
                "revenue_surprise": _num(row.get("revenue_surprise")),
                "revenue_surprise_percent": _num(row.get("revenue_surprise_percent")),
                "period": row.get("period"),
                "period_year": row.get("period_year"),
                "date_confirmed": bool(row.get("date_confirmed")),
                "name": row.get("name"),
                "time": row.get("time"),
                "importance": row.get("importance"),
            }
            for row in rows[: _page_size(limit)]
        ]

    # -- corporate_actions -------------------------------------------------

    def corporate_actions(
        self,
        *,
        symbol: str | None = None,
        kind: str = "dividends",
        limit: int = 100,
        start: str | None = None,
        end: str | None = None,
        **_,
    ) -> list[dict[str, Any]]:
        """Dividends or splits, for one ticker or the whole market.

        Args:
            symbol: Ticker filter, e.g. ``AAPL.US`` (``.US`` stripped upstream);
                optional.
            kind: ``dividends`` or ``splits``.
            limit: Maximum rows (``pagesize`` upstream, capped at 1000).
            start: ``YYYY-MM-DD`` lower bound (``parameters[date_from]``).
            end: ``YYYY-MM-DD`` upper bound (``parameters[date_to]``).

        Returns:
            Rows tagged with ``type`` (``dividend``/``split``) plus the
            endpoint's own fields.

        Raises:
            ProviderHTTPError: For an unknown ``kind``.
        """
        if kind not in {"dividends", "splits"}:
            raise ProviderHTTPError(
                self.name,
                f"unsupported corporate action kind {kind!r}",
                category="corporate_actions",
            )
        params: dict[str, Any] = {"pagesize": _page_size(limit)}
        if symbol:
            params["parameters[tickers]"] = _benzinga_symbol(symbol)
        if start:
            params["parameters[date_from]"] = start
        if end:
            params["parameters[date_to]"] = end
        path = _DIVIDENDS_PATH if kind == "dividends" else _SPLITS_PATH
        rows = _rows(
            self._get(path, category="corporate_actions", params=params),
            kind,
            category="corporate_actions",
        )[: _page_size(limit)]
        if kind == "dividends":
            return [
                {
                    "type": "dividend",
                    "date": row.get("date"),
                    "symbol": row.get("ticker"),
                    "name": row.get("name"),
                    "amount": _num(row.get("dividend")),
                    "prior_amount": _num(row.get("dividend_prior")),
                    "currency": row.get("currency"),
                    "dividend_type": row.get("dividend_type"),
                    "ex_dividend_date": row.get("ex_dividend_date"),
                    "payable_date": row.get("payable_date"),
                    "record_date": row.get("record_date"),
                    "frequency": row.get("frequency"),
                    "yield": _num(row.get("dividend_yield")),
                    "end_regular_dividend": row.get("end_regular_dividend"),
                    "importance": row.get("importance"),
                }
                for row in rows
            ]
        return [
            {
                "type": "split",
                "date": row.get("date_ex"),
                "symbol": row.get("ticker"),
                "name": row.get("name"),
                "ratio": row.get("ratio"),
                "split_type": row.get("split_type"),
                "date_announced": row.get("date_announced"),
                "date_recorded": row.get("date_recorded"),
                "date_distribution": row.get("date_distribution"),
                "optionable": row.get("optionable"),
                "importance": row.get("importance"),
            }
            for row in rows
        ]

    # -- offerings_calendar ------------------------------------------------

    def offerings_calendar(
        self,
        *,
        symbol: str | None = None,
        limit: int = 100,
        start: str | None = None,
        end: str | None = None,
        **_,
    ) -> list[dict[str, Any]]:
        """Public offerings (secondaries, follow-ons, debt and shelf deals).

        The sibling ``/v2.1/calendar/ipos`` route answers 200 but returned an
        empty array on every probe, so its unverified field names are not
        guessed at here.

        Args:
            symbol: Ticker filter, e.g. ``AAPL.US`` (``.US`` stripped upstream);
                optional.
            limit: Maximum rows (``pagesize`` upstream, capped at 1000).
            start: ``YYYY-MM-DD`` lower bound (``parameters[date_from]``).
            end: ``YYYY-MM-DD`` upper bound (``parameters[date_to]``).

        Returns:
            Rows of ``{date, symbol, name, offering_type, amount}`` plus price,
            share count and shelf extras.
        """
        params: dict[str, Any] = {"pagesize": _page_size(limit)}
        if symbol:
            params["parameters[tickers]"] = _benzinga_symbol(symbol)
        if start:
            params["parameters[date_from]"] = start
        if end:
            params["parameters[date_to]"] = end
        rows = _rows(
            self._get(_OFFERINGS_PATH, category="offerings_calendar", params=params),
            "offerings",
            category="offerings_calendar",
        )
        return [
            {
                "date": row.get("date"),
                "symbol": row.get("ticker"),
                "name": row.get("name"),
                "offering_type": row.get("offering_type") or None,
                "amount": _num(row.get("proceeds") or row.get("dollar_shares")),
                "price": _num(row.get("price")),
                "number_shares": _num(row.get("number_shares")),
                "dollar_shares": _num(row.get("dollar_shares")),
                "exchange": row.get("exchange"),
                "currency": row.get("currency"),
                "shelf": row.get("shelf"),
                "url": row.get("url"),
                "importance": row.get("importance"),
            }
            for row in rows[: _page_size(limit)]
        ]

    # -- guidance_revisions ------------------------------------------------

    def guidance_revisions(
        self,
        *,
        symbol: str | None = None,
        limit: int = 100,
        start: str | None = None,
        end: str | None = None,
        primary_only: bool = False,
        **_,
    ) -> list[dict[str, Any]]:
        """Company guidance records (EPS and/or revenue).

        One upstream record can revise both EPS and revenue, so a record yields
        one row per populated metric — a single row would have to drop one of
        the two and silently lose data.

        Args:
            symbol: Ticker filter, e.g. ``AAPL.US`` (``.US`` stripped upstream);
                optional.
            limit: Maximum records to read (``pagesize`` upstream).
            start: ``YYYY-MM-DD`` lower bound (``parameters[date_from]``).
            end: ``YYYY-MM-DD`` upper bound (``parameters[date_to]``).
            primary_only: Restrict to primary guidance (``parameters[is_primary]``).

        Returns:
            Rows of ``{symbol, date, metric, prior, current}`` plus the min/max
            guidance band and the fiscal period.
        """
        params: dict[str, Any] = {"pagesize": _page_size(limit)}
        if symbol:
            params["parameters[tickers]"] = _benzinga_symbol(symbol)
        if start:
            params["parameters[date_from]"] = start
        if end:
            params["parameters[date_to]"] = end
        if primary_only:
            params["parameters[is_primary]"] = "Y"
        rows = _rows(
            self._get(_GUIDANCE_PATH, category="guidance_revisions", params=params),
            "guidance",
            category="guidance_revisions",
        )
        out: list[dict[str, Any]] = []
        for row in rows[: _page_size(limit)]:
            for prefix, metric in (("eps_guidance", "eps"), ("revenue_guidance", "revenue")):
                band = {
                    suffix: row.get(f"{prefix}_{suffix}")
                    for suffix in ("est", "min", "max", "prior_min", "prior_max")
                }
                if not any(value for value in band.values()):
                    continue
                out.append(
                    {
                        "symbol": row.get("ticker"),
                        "date": row.get("date"),
                        "metric": metric,
                        "prior": _band(band["prior_min"], band["prior_max"]),
                        "current": _band(band["min"], band["max"]) or band["est"] or None,
                        "estimate": _num(band["est"]),
                        "guidance_min": _num(band["min"]),
                        "guidance_max": _num(band["max"]),
                        "prior_min": _num(band["prior_min"]),
                        "prior_max": _num(band["prior_max"]),
                        "period": row.get("period"),
                        "period_year": row.get("period_year"),
                        "preliminary": row.get("prelim"),
                        "is_primary": row.get("is_primary"),
                        "name": row.get("name"),
                        "importance": row.get("importance"),
                    }
                )
        return out

    # -- fda_calendar ------------------------------------------------------

    def fda_calendar(
        self,
        *,
        symbol: str | None = None,
        limit: int = 100,
        start: str | None = None,
        end: str | None = None,
        **_,
    ) -> list[dict[str, Any]]:
        """FDA / clinical-trial events tied to listed issuers.

        Args:
            symbol: Ticker filter, e.g. ``AAPL.US`` (``.US`` stripped upstream);
                optional.
            limit: Maximum rows (``pagesize`` upstream, capped at 1000).
            start: ``YYYY-MM-DD`` lower bound (``parameters[date_from]``).
            end: ``YYYY-MM-DD`` upper bound (``parameters[date_to]``).

        Returns:
            Rows of ``{date, symbol, name, event}`` plus drug and outcome extras.
        """
        params: dict[str, Any] = {"pagesize": _page_size(limit)}
        if symbol:
            params["parameters[securities]"] = _benzinga_symbol(symbol)
        if start:
            params["parameters[date_from]"] = start
        if end:
            params["parameters[date_to]"] = end
        rows = _rows(
            self._get(_FDA_PATH, category="fda_calendar", params=params),
            "fda",
            category="fda_calendar",
        )
        out: list[dict[str, Any]] = []
        for row in rows[: _page_size(limit)]:
            companies = row.get("companies")
            company = companies[0] if isinstance(companies, list) and companies else {}
            if not isinstance(company, dict):
                company = {}
            securities = company.get("securities")
            security = securities[0] if isinstance(securities, list) and securities else {}
            if not isinstance(security, dict):
                security = {}
            drug = row.get("drug")
            drug = drug if isinstance(drug, dict) else {}
            out.append(
                {
                    "date": row.get("date"),
                    "symbol": security.get("symbol"),
                    "name": company.get("name"),
                    "event": row.get("event_type"),
                    "time": row.get("time"),
                    "drug": drug.get("name"),
                    "indication": drug.get("indication_symptom"),
                    "outcome": row.get("outcome") or row.get("outcome_brief") or None,
                    "status": row.get("status") or None,
                    "source_type": row.get("source_type"),
                    "url": row.get("source_link"),
                    "companies": [c.get("name") for c in companies if isinstance(c, dict)]
                    if isinstance(companies, list)
                    else [],
                    "importance": row.get("importance"),
                    "id": row.get("id"),
                }
            )
        return out

    # -- economic_calendar -------------------------------------------------

    def economic_calendar(
        self,
        *,
        country: str | None = None,
        event_name: str | None = None,
        limit: int = 100,
        start: str | None = None,
        end: str | None = None,
        **_,
    ) -> list[dict[str, Any]]:
        """Macro event calendar with consensus/actual readings.

        Args:
            country: Three-letter country code, e.g. ``USA``.
            event_name: Comma-separated event-name filter.
            limit: Maximum rows (``pagesize`` upstream, capped at 1000).
            start: ``YYYY-MM-DD`` lower bound (``parameters[date_from]``).
            end: ``YYYY-MM-DD`` upper bound (``parameters[date_to]``).

        Returns:
            Rows of ``{title, timestamp, country, importance, previous,
            consensus, actual}`` plus the event category and unit extras.
        """
        params: dict[str, Any] = {"pagesize": _page_size(limit)}
        if country:
            params["country"] = country.upper()
        if event_name:
            params["event_name"] = event_name
        if start:
            params["parameters[date_from]"] = start
        if end:
            params["parameters[date_to]"] = end
        rows = _rows(
            self._get(_ECONOMICS_PATH, category="economic_calendar", params=params),
            "economics",
            category="economic_calendar",
        )
        return [
            {
                "title": row.get("event_name") or row.get("description"),
                "timestamp": _datetime(row.get("date"), row.get("time")),
                "country": row.get("country"),
                "importance": row.get("importance"),
                "previous": _num(row.get("prior")),
                "consensus": _num(row.get("consensus")),
                "actual": _num(row.get("actual")),
                "previous_unit": row.get("prior_t"),
                "consensus_unit": row.get("consensus_t"),
                "actual_unit": row.get("actual_t"),
                "event_category": row.get("event_category"),
                "event_period": row.get("event_period"),
                "period_year": row.get("period_year"),
                "description": row.get("description"),
                "id": row.get("id"),
            }
            for row in rows[: _page_size(limit)]
        ]


def _band(low: Any, high: Any) -> str | None:
    """Render a guidance band as ``"low-high"`` (whichever bounds exist).

    Args:
        low: Lower bound of the band (may be empty).
        high: Upper bound of the band (may be empty).

    Returns:
        The band string, a single bound, or ``None`` when neither is present.
    """
    if low and high:
        return f"{low}-{high}"
    return low or high or None


def _datetime(date: Any, time_of_day: Any) -> str | None:
    """Join Benzinga's separate ``date`` and ``time`` fields into ISO-8601.

    Args:
        date: ``YYYY-MM-DD`` date.
        time_of_day: ``HH:MM:SS`` time (may be empty).

    Returns:
        ``YYYY-MM-DDTHH:MM:SS``, the bare date, or ``None`` when no date.
    """
    if not isinstance(date, str) or not date:
        return None
    if isinstance(time_of_day, str) and time_of_day:
        return f"{date}T{time_of_day}"
    return date
