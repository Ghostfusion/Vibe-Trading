"""GDELT provider adapter.

Base URL ``https://api.gdeltproject.org/api/v2``; no key. The Doc API answers
``GET /doc/doc`` in JSON when ``format=json`` is passed (the default is HTML),
and every request needs a ``query`` — GDELT is a full-text news search, so there
is no ticker-scoped endpoint at all.

Measured on 2026-10-08:

* ``GET /doc/doc?query=apple&mode=timelinetone&format=json`` → ``200`` with
  ``{"query_details": …, "timeline": [{"series": "Average Tone", "data":
  [{"date": "20260716T000000Z", "value": 0.6728}, …]}]}``.
* ``GET /doc/doc?query=…&mode=artlist&format=json&maxrecords=N`` → ``200`` with
  ``{"articles": [{"url", "title", "seendate", "domain", "language",
  "sourcecountry"}, …]}``.
* ``mode=timelinevol`` also answers 200; no category maps to a volume series, so
  it is not implemented.
* **A second request within 5 seconds returns HTTP 429**, and one query can take
  10-17 s to compute, so the adapter spaces requests at 5 s and waits up to 45 s
  per request. Those are the defaults below, not tunables chosen for taste.

``news_sentiment`` is the tone timeline re-keyed to ``{title, url, published,
sentiment, score}``. GDELT's ``Average Tone`` is the *mean* tone of every
matching article over a whole day — an aggregate, not the sentiment of one
article — so each row carries a synthesized title (a timeline point has no
headline), no URL, and the tone value as ``score``; ``sentiment`` is only the
sign of that aggregate (``positive``/``negative``/``neutral``). The
``news_data`` chain does not currently name ``gdelt`` (the sentiment chain
does); artlist is implemented here and proposed as an addition.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_BASE_URL = "https://api.gdeltproject.org/api/v2"
_HOST_KEY = "gdelt"
_MIN_INTERVAL_ENV = "VIBE_TRADING_GDELT_MIN_INTERVAL"
# GDELT answers HTTP 429 to two requests inside 5 seconds, so this is a hard
# floor rather than politeness. The timeout covers a 10-17 s query plus slack.
_DEFAULT_MIN_INTERVAL_S = 5.0
_TIMEOUT_S = 45.0

#: The series name GDELT gives the tone timeline, and the fallback used when a
#: response carries exactly one unnamed series.
_TONE_SERIES = "Average Tone"

#: Doc API article cap; GDELT refuses ``maxrecords`` above this.
_MAX_RECORDS = 250
_DEFAULT_LIMIT = 50

#: House US symbol suffix. GDELT is a text search, so ``AAPL.US`` has to become
#: ``AAPL`` before it can match anything.
_MARKET_SUFFIX = ".US"


@register_provider
class GdeltProvider(Provider):
    """No-key GDELT Doc API adapter (5 s spacing, long timeout)."""

    name = "gdelt"
    env_keys = ()
    capabilities = {
        "news_sentiment": "news_sentiment",
        "news_data": "news_data",
    }

    def _get(self, *, category: str, params: dict[str, Any]) -> Any:
        """Call ``/doc/doc`` once with the GDELT spacing and timeout.

        Args:
            category: Category the call belongs to, for the raised error.
            params: Doc API query parameters.

        Returns:
            The decoded JSON body.

        Raises:
            ProviderHTTPError: On a transport error, a non-2xx status (429 when
                requests are not spaced), or a body that is not JSON.
        """
        return fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}/doc/doc",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=params,
            timeout=_TIMEOUT_S,
        )

    # -- news_sentiment ----------------------------------------------------

    def news_sentiment(
        self,
        *,
        symbol: str | None = None,
        query: str | None = None,
        limit: int = _DEFAULT_LIMIT,
        timespan: str | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Daily average news tone for a search phrase.

        Args:
            symbol: Ticker to search for, e.g. ``AAPL`` (``query`` wins when
                both are given); a trailing ``.US`` is stripped.
            query: Free-text search phrase.
            limit: Maximum most-recent timeline points to return.
            timespan: Optional GDELT window such as ``1week`` or ``3months``.

        Returns:
            Rows of ``{title, url, published, sentiment, score}``, oldest
            first. ``score`` is GDELT's aggregate average tone for that day
            (roughly -10..+10), ``sentiment`` is its sign, ``published`` is the
            point's ISO-8601 timestamp, ``title`` is synthesized (a timeline
            point has no headline) and ``url`` is ``None``.

        Raises:
            ProviderHTTPError: When neither ``symbol`` nor ``query`` is given,
                or the upstream refuses / returns junk.
        """
        text = _search_text(query, symbol, category="news_sentiment")
        params: dict[str, Any] = {
            "query": text,
            "mode": "timelinetone",
            "format": "json",
        }
        if timespan:
            params["timespan"] = timespan
        payload = self._get(category="news_sentiment", params=params)
        if not isinstance(payload, dict):
            raise ProviderHTTPError(
                self.name,
                "tone timeline payload was not an object",
                category="news_sentiment",
            )
        series = _tone_series(payload)
        rows: list[dict[str, Any]] = []
        for point in series:
            if not isinstance(point, dict):
                continue
            published = _iso_timestamp(point.get("date"))
            score = _to_float(point.get("value"))
            if published is None or score is None:
                continue
            rows.append(
                {
                    "title": f"{text} - GDELT average tone",
                    "url": None,
                    "published": published,
                    "sentiment": _tone_label(score),
                    "score": score,
                }
            )
        if isinstance(limit, int) and limit > 0:
            rows = rows[-limit:]
        return rows

    # -- news_data ---------------------------------------------------------

    def news_data(
        self,
        *,
        symbol: str | None = None,
        query: str | None = None,
        limit: int = _DEFAULT_LIMIT,
        timespan: str | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Recent news articles matching a search phrase (``mode=artlist``).

        Args:
            symbol: Ticker to search for, e.g. ``AAPL`` (``query`` wins when
                both are given); a trailing ``.US`` is stripped.
            query: Free-text search phrase.
            limit: Maximum articles requested (GDELT caps at 250).
            timespan: Optional GDELT window such as ``1week`` or ``3months``.

        Returns:
            Rows of ``{title, url, published, source, summary}``, where
            ``source`` is the article's domain, ``published`` its ISO-8601
            seen-date and ``summary`` is ``None`` (artlist carries no body).

        Raises:
            ProviderHTTPError: When neither ``symbol`` nor ``query`` is given,
                or the upstream refuses / returns junk.
        """
        text = _search_text(query, symbol, category="news_data")
        records = _clamp_limit(limit)
        params: dict[str, Any] = {
            "query": text,
            "mode": "artlist",
            "format": "json",
            "maxrecords": records,
        }
        if timespan:
            params["timespan"] = timespan
        payload = self._get(category="news_data", params=params)
        if not isinstance(payload, dict):
            raise ProviderHTTPError(
                self.name, "artlist payload was not an object", category="news_data"
            )
        raw = payload.get("articles")
        if not isinstance(raw, list):
            # A valid JSON answer without an articles array is GDELT's "no
            # matches" shape; a non-JSON body already raised inside fetch_json.
            return []
        rows: list[dict[str, Any]] = []
        for article in raw:
            if not isinstance(article, dict):
                continue
            rows.append(
                {
                    "title": article.get("title"),
                    "url": article.get("url"),
                    "published": _iso_timestamp(article.get("seendate")),
                    "source": article.get("domain"),
                    "summary": None,
                }
            )
        return rows[:records]


def _search_text(query: Any, symbol: Any, *, category: str) -> str:
    """Pick and clean the GDELT search phrase for a request.

    Args:
        query: Free-text phrase, preferred when present.
        symbol: Ticker fallback; a trailing ``.US`` is stripped because GDELT
            matches text and the house US convention appends that suffix.
        category: Category the call belongs to, for the raised error.

    Returns:
        A non-empty search phrase.

    Raises:
        ProviderHTTPError: When neither value carries text.
    """
    if isinstance(query, str) and query.strip():
        return query.strip()
    if isinstance(symbol, str) and symbol.strip():
        text = symbol.strip()
        if text.upper().endswith(_MARKET_SUFFIX):
            text = text[: -len(_MARKET_SUFFIX)]
        if text:
            return text
    raise ProviderHTTPError(
        "gdelt",
        f"'query' (or 'symbol') is required for {category}; GDELT is a text search",
        category=category,
    )


def _clamp_limit(value: Any) -> int:
    """Coerce a requested article count into GDELT's ``1..250`` range."""
    try:
        count = int(value)
    except (TypeError, ValueError, OverflowError):
        return _DEFAULT_LIMIT
    return max(1, min(count, _MAX_RECORDS))


def _tone_series(payload: dict[str, Any]) -> list[Any]:
    """Return the tone timeline's data points from a timelinetone payload.

    Args:
        payload: Decoded timelinetone JSON.

    Returns:
        The ``data`` list of the series named ``Average Tone``, or of the only
        series present when the name is absent. ``[]`` when the payload carries
        no timeline at all.
    """
    timeline = payload.get("timeline")
    if not isinstance(timeline, list):
        return []
    chosen: list[Any] = []
    for entry in timeline:
        if not isinstance(entry, dict):
            continue
        data = entry.get("data")
        if not isinstance(data, list):
            continue
        if entry.get("series") == _TONE_SERIES:
            return data
        if not chosen:
            chosen = data
    return chosen


def _tone_label(score: float) -> str:
    """Label an aggregate tone by its sign.

    GDELT's average tone is a mean over every matching article, so only its sign
    is unambiguous; no magnitude threshold is invented here.
    """
    if score > 0:
        return "positive"
    if score < 0:
        return "negative"
    return "neutral"


def _iso_timestamp(value: Any) -> str | None:
    """Convert a GDELT ``YYYYMMDDTHHMMSSZ`` stamp to ISO-8601, or ``None``.

    Args:
        value: Raw GDELT date field (``"20260716T000000Z"``).

    Returns:
        ``"2026-07-16T00:00:00Z"``, or ``None`` when the value is absent or not
        in GDELT's compact format.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        stamp = datetime.strptime(value.strip(), "%Y%m%dT%H%M%SZ")
    except ValueError:
        return None
    return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_float(value: Any) -> float | None:
    """Coerce a possibly-string numeric field to ``float``, or ``None``."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
