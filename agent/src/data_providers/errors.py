"""Error taxonomy for the category-based provider failover layer.

The resolver never retries a provider. Every failure is therefore terminal for
that provider *for the request*, and the only question left is which provider
the chain tries next. The classes below exist so a caller can tell the three
cases apart:

* :class:`ProviderUnavailable` — the provider was never a candidate (missing
  key, gateway down). No request was made, so no quota was spent.
* :class:`ProviderHTTPError` — the provider was called and refused (HTTP 4xx/5xx,
  an OpenD ``ret_code != 0``, or an in-band error envelope). This is the case
  the failover rule exists for: 404/403/429 must move to the next provider, not
  be retried against the same one.
* :class:`ChainExhausted` — every provider in the chain was unavailable,
  unsupported or failed. Carries the per-provider attempts, because "all
  sources failed" without the reason list is unactionable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence


class ProviderError(RuntimeError):
    """Base class for every provider-layer failure.

    Args:
        provider: Provider name as it appears in a category chain.
        reason: Operator-facing explanation, safe to log and to return.
        category: Category the request was for, when known.
        status: HTTP status code or provider-native error code, when known.
    """

    def __init__(
        self,
        provider: str,
        reason: str,
        *,
        category: str | None = None,
        status: int | None = None,
    ) -> None:
        self.provider = provider
        self.reason = reason
        self.category = category
        self.status = status
        super().__init__(f"{provider}: {reason}")


class ProviderUnavailable(ProviderError):
    """A provider that cannot be a candidate (missing key, gateway down).

    Raised by :meth:`Provider.available`-adjacent code paths, never after a
    successful request. The resolver records it and moves on without calling
    the provider, so an unconfigured source costs nothing.
    """


class ProviderHTTPError(ProviderError):
    """A provider was called and returned an error instead of data.

    Covers HTTP 4xx/5xx, OpenD non-zero ``ret_code`` values, and providers that
    answer HTTP 200 with an in-band error body (Alpha Vantage's ``Information``
    and ``Note`` envelopes are the canonical example). Failover, never retry.
    """


class ChainExhausted(ProviderError):
    """Every provider in a category chain was skipped or failed.

    Args:
        category: The category that could not be served.
        attempts: One entry per provider tried, in chain order, each describing
            why it could not serve the request.
    """

    def __init__(self, category: str, attempts: "Sequence[Any]") -> None:
        parts = []
        for attempt in attempts:
            provider = getattr(attempt, "provider", "?")
            outcome = getattr(attempt, "outcome", "?")
            detail = getattr(attempt, "detail", "")
            parts.append(f"{provider}={outcome}({detail})" if detail else f"{provider}={outcome}")
        lines = "; ".join(parts)
        self.category = category
        self.attempts = tuple(attempts)
        super().__init__(
            category,
            f"no provider in the {category!r} chain could serve the request"
            + (f" [{lines}]" if lines else ""),
        )
