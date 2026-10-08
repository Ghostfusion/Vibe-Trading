"""NewsAPI provider adapter.

Base URL ``https://newsapi.org/v2``; one ``apiKey`` query parameter gates every
endpoint (the same key is accepted as an ``X-Api-Key`` header, but one credential
path keeps this module uniform). Responses are JSON. Every payload is
``{"status": "ok", "totalResults": N, "articles": [...]}`` for the two article
endpoints and ``{"status": "ok", "sources": [...]}`` for the source catalogue.

Measured live against the configured key on 2026-10-08:

* ``/top-headlines?country=us`` — **HTTP 200**, article rows ``{source:
  {id, name}, author, title, description, url, urlToImage, publishedAt,
  content}``.
* ``/everything?q=AAPL`` — **HTTP 200**, the same article shape.
* ``/sources`` — **HTTP 200**, ``{id, name, description, url, category,
  language, country}`` (not wired here: ``exchange_symbols`` is an instrument
  catalogue, not a news-source list).

Refusals come back with a 4xx status and
``{"status": "error", "code": ..., "message": ...}``. Measured: a
``/top-headlines`` request mixing ``sources`` with ``country`` answered **HTTP
400** ``parametersIncompatible``; a ``/everything`` request with an empty ``q``
answered **HTTP 400** ``parametersMissing``. Both reach :func:`fetch_json` as a
:class:`ProviderHTTPError`, which is terminal for this provider — the resolver
fails over, this adapter never retries.

``news_sentiment`` is **not** a capability: NewsAPI exposes no sentiment field on
any article (the article shape above is the whole of it), and inventing a score
from the text would fabricate data. The category is omitted rather than faked.

Free (Developer) tier: **100 requests/day**, articles delayed ~24h, search
limited to ~1 month of history, ``pageSize`` capped at 100. Each method issues
exactly one request.
"""

from __future__ import annotations

from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_BASE_URL = "https://newsapi.org/v2"
_API_KEY_ENV = "NEWSAPI_API_KEY"
_HOST_KEY = "newsapi"
_MIN_INTERVAL_ENV = "VIBE_TRADING_NEWSAPI_MIN_INTERVAL"

# The free tier is 100 requests/day; a mild per-host spacing keeps a fan-out
# from looking like a burst, and the env override lets a paid key lower it.
_DEFAULT_MIN_INTERVAL_S = 1.5

#: NewsAPI caps pageSize at 100 on every plan.
_MAX_PAGE_SIZE = 100


@register_provider
class NewsApiProvider(Provider):
    """Key-gated NewsAPI REST adapter."""

    name = "newsapi"
    env_keys = (_API_KEY_ENV,)
    capabilities = {
        "news_data": "news_data",
    }

    def _get(
        self, path: str, *, category: str, params: dict[str, Any] | None = None
    ) -> Any:
        """Call one NewsAPI endpoint with the configured key.

        Args:
            path: Endpoint path, e.g. ``/everything``.
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
        query["apiKey"] = token
        payload = fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}{path}",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
            params=query,
        )
        if isinstance(payload, dict) and payload.get("status") == "error":
            raise ProviderHTTPError(
                self.name,
                f"upstream error {payload.get('code')}: {payload.get('message', '')}".strip(),
                category=category,
            )
        return payload

    def _articles(self, payload: Any, category: str) -> list[dict[str, Any]]:
        """Return the ``articles`` list of a NewsAPI envelope, or refuse.

        A body without an ``articles`` list is not an empty answer: it is a
        shape this adapter does not model (a provider-native refusal, say), so
        the chain must fail over rather than treat it as a successful empty
        result. An empty list is returned only when the upstream genuinely sent
        one.

        Args:
            payload: Decoded JSON body.
            category: Category the call belongs to.

        Returns:
            The article objects (non-mapping entries dropped).

        Raises:
            ProviderHTTPError: When the body has no list at ``articles``.
        """
        if isinstance(payload, dict) and isinstance(payload.get("articles"), list):
            return [a for a in payload["articles"] if isinstance(a, dict)]
        raise ProviderHTTPError(
            self.name,
            f"unexpected {self.name} payload for {category!r}: "
            "expected a list at 'articles'",
            category=category,
        )

    # -- news_data ---------------------------------------------------------

    def news_data(
        self,
        *,
        symbol: str | None = None,
        query: str | None = None,
        limit: int = 20,
        start: str | None = None,
        end: str | None = None,
        country: str = "us",
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Financial news for a ticker, a text query, or the top headlines.

        Routing: a ``symbol`` or ``query`` searches ``/everything`` with that
        term as ``q``; with neither, ``/top-headlines`` for *country* is used.

        Args:
            symbol: Ticker to search for (used as the ``q`` term).
            query: Free-text term, used when ``symbol`` is absent.
            limit: Maximum articles requested (``pageSize``, capped at 100).
            start: Inclusive ``YYYY-MM-DD`` lower bound (``from``).
            end: Inclusive ``YYYY-MM-DD`` upper bound (``to``).
            country: Headline country when no term is given.

        Returns:
            Rows of ``{title, url, published, source, summary}``.
        """
        term = (symbol or query or "").strip()
        page_size = max(1, min(int(limit), _MAX_PAGE_SIZE))
        if term:
            params: dict[str, Any] = {"q": term, "pageSize": page_size}
            if start:
                params["from"] = start
            if end:
                params["to"] = end
            payload = self._get("/everything", category="news_data", params=params)
        else:
            params = {"country": country or "us", "pageSize": page_size}
            payload = self._get("/top-headlines", category="news_data", params=params)
        return [
            {
                "title": article.get("title"),
                "url": article.get("url"),
                "published": article.get("publishedAt"),
                "source": self._source_name(article.get("source")),
                "summary": article.get("description"),
            }
            for article in self._articles(payload, "news_data")
        ]

    @staticmethod
    def _source_name(source: Any) -> str | None:
        """Extract the publisher name from a NewsAPI ``source`` object.

        Args:
            source: ``{"id", "name"}`` mapping, a string, or ``None``.

        Returns:
            The source name, or ``None`` when unavailable.
        """
        if isinstance(source, dict):
            name = source.get("name")
            return str(name) if name is not None else None
        if source is None:
            return None
        return str(source)
