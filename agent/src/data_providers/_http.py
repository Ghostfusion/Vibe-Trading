"""HTTP helpers for provider adapters.

Every provider call goes through :func:`fetch_json` so the failover contract is
enforced in one place:

* one request per provider attempt — there is no retry loop here, and the
  resolver does not add one;
* any non-2xx status is a terminal failure for that provider, reported as
  :class:`~src.data_providers.errors.ProviderHTTPError` so the chain moves on;
* requests are spaced by the shared per-host throttle from
  ``backtest.loaders._http`` so a process-wide rate budget is respected across
  the loader layer and this layer alike.
"""

from __future__ import annotations

from typing import Any

import requests

from backtest.loaders._http import resolve_min_interval, throttled_get
from src.data_providers.errors import ProviderHTTPError


def fetch_json(
    *,
    provider: str,
    category: str | None,
    url: str,
    host_key: str,
    min_interval_env: str,
    min_interval_default: float,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15.0,
) -> Any:
    """GET *url* once and decode the body as JSON.

    Args:
        provider: Provider name, for the raised error.
        category: Category the call belongs to, for the raised error.
        url: Fully-qualified URL.
        host_key: Throttle bucket (one per provider host).
        min_interval_env: Env var overriding the spacing (seconds).
        min_interval_default: Spacing used when the env var is absent/invalid.
        params: Query parameters.
        headers: Extra headers merged over the default browser User-Agent.
        timeout: Per-request socket timeout in seconds.

    Returns:
        The decoded JSON body.

    Raises:
        ProviderHTTPError: On a transport error, a non-2xx status, or a body
            that is not JSON. Never retried by this layer.
    """
    try:
        response = throttled_get(
            url,
            host_key=host_key,
            min_interval=resolve_min_interval(min_interval_env, min_interval_default),
            params=params,
            headers=headers,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise ProviderHTTPError(
            provider,
            f"transport error: {type(exc).__name__}: {exc}",
            category=category,
        ) from exc

    if response.status_code >= 400:
        raise ProviderHTTPError(
            provider,
            f"HTTP {response.status_code} from {url}",
            category=category,
            status=response.status_code,
        )

    try:
        return response.json()
    except ValueError as exc:
        snippet = response.text[:120].replace("\n", " ")
        raise ProviderHTTPError(
            provider,
            f"HTTP {response.status_code} but the body was not JSON: {snippet!r}",
            category=category,
            status=response.status_code,
        ) from exc


def fetch_text(
    *,
    provider: str,
    category: str | None,
    url: str,
    host_key: str,
    min_interval_env: str,
    min_interval_default: float,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 20.0,
) -> str:
    """GET *url* once and return the body as text (CSV/XML endpoints).

    Same failure contract as :func:`fetch_json`.
    """
    try:
        response = throttled_get(
            url,
            host_key=host_key,
            min_interval=resolve_min_interval(min_interval_env, min_interval_default),
            params=params,
            headers=headers,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise ProviderHTTPError(
            provider,
            f"transport error: {type(exc).__name__}: {exc}",
            category=category,
        ) from exc

    if response.status_code >= 400:
        raise ProviderHTTPError(
            provider,
            f"HTTP {response.status_code} from {url}",
            category=category,
            status=response.status_code,
        )
    return response.text
