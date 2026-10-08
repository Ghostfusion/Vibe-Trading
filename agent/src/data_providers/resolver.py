"""Single-chain failover resolver.

The rule this module implements, stated once so it cannot drift:

1. A category has exactly one ordered chain (``categories.CATEGORY_CHAINS``).
2. Each provider in that chain is attempted **once**, in order.
3. Any failure — HTTP 404/403/429/5xx, an in-band error envelope, a
   provider-native permission error, a transport error — is terminal for that
   provider and moves straight to the next. Nothing is retried, backoff applied,
   or the same provider asked twice.
4. The first provider that answers wins and its answer is returned as-is. The
   chain does not blend two providers' data into one answer, because a mixed
   answer has no single provenance or caliber.
5. When the chain runs out, :class:`~src.data_providers.errors.ChainExhausted`
   is raised with the per-provider reasons. "No data" without the reason list is
   unactionable, so the failure always carries its trace.

Skipping before calling is deliberate and free: a provider with no configured
key, a provider with no implementation for the category, and a provider with no
adapter module at all are all recorded without spending a request.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from src.data_providers.base import Provider, ProviderResult
from src.data_providers.categories import CATEGORY_CHAINS, chain_for
from src.data_providers.errors import ChainExhausted, ProviderError
from src.data_providers.registry import get_provider

logger = logging.getLogger(__name__)

#: Outcome vocabulary for :class:`Attempt`.
OUTCOME_OK = "ok"
OUTCOME_FAILED = "failed"
OUTCOME_UNAVAILABLE = "unavailable"
OUTCOME_UNSUPPORTED = "unsupported"
OUTCOME_UNREGISTERED = "unregistered"


@dataclass(frozen=True)
class Attempt:
    """One provider's slot in a resolution trace.

    Attributes:
        provider: Provider name from the chain.
        outcome: One of the module-level ``OUTCOME_*`` values.
        detail: Human-readable reason (error text, missing key, ...).
        status: HTTP status or provider-native error code when the provider was
            actually called and refused.
        elapsed_ms: Wall-clock duration of the call, ``0`` when never called.
    """

    provider: str
    outcome: str
    detail: str = ""
    status: int | None = None
    elapsed_ms: int = 0


@dataclass(frozen=True)
class Resolution:
    """A served answer plus the full trace of how it was reached.

    Attributes:
        result: The winning provider's answer.
        attempts: Every chain slot, in order, including the winners and the
            providers skipped before it.
    """

    result: ProviderResult
    attempts: tuple[Attempt, ...] = field(default_factory=tuple)

    @property
    def provider(self) -> str:
        """Name of the provider that served the answer."""
        return self.result.provider

    def trace_lines(self) -> list[str]:
        """Render the trace as ``provider: outcome (detail)`` lines."""
        return [
            f"{a.provider}: {a.outcome}" + (f" ({a.detail})" if a.detail else "")
            for a in self.attempts
        ]


def _resolve_category(category: str) -> tuple[str, ...]:
    """Return the chain for *category*, raising on an unknown category."""
    if category not in CATEGORY_CHAINS:
        raise ValueError(
            f"unknown data category {category!r}; known categories: "
            f"{', '.join(sorted(CATEGORY_CHAINS))}"
        )
    return chain_for(category)


def _call_once(
    provider: Provider, category: str, params: dict[str, Any]
) -> tuple[ProviderResult | None, Attempt]:
    """Attempt *provider* exactly once and classify the outcome."""
    started = time.monotonic()
    try:
        data = provider.fetch(category, **params)
    except ProviderError as exc:
        elapsed = int((time.monotonic() - started) * 1000)
        logger.info(
            "provider %s failed category %s: %s", provider.name, category, exc.reason
        )
        return None, Attempt(
            provider=provider.name,
            outcome=OUTCOME_FAILED,
            detail=exc.reason,
            status=exc.status,
            elapsed_ms=elapsed,
        )
    elapsed = int((time.monotonic() - started) * 1000)
    return (
        ProviderResult(
            provider=provider.name, category=category, data=data, elapsed_ms=elapsed
        ),
        Attempt(
            provider=provider.name, outcome=OUTCOME_OK, elapsed_ms=elapsed
        ),
    )


def resolve(category: str, **params: Any) -> ProviderResult:
    """Serve *category* from the first provider in its chain that answers.

    Args:
        category: Category name; must exist in the chain table.
        **params: Category-specific request parameters, passed unchanged to the
            adapter.

    Returns:
        The :class:`~src.data_providers.base.ProviderResult` from the first
        provider that succeeded.

    Raises:
        ValueError: ``category`` is not in the chain table.
        ChainExhausted: Every provider was unavailable, unsupported, missing, or
            failed. The exception carries the per-provider trace.
    """
    return resolve_with_trace(category, **params).result


def resolve_with_trace(category: str, **params: Any) -> Resolution:
    """Like :func:`resolve`, but also return every attempt made.

    Args:
        category: Category name; must exist in the chain table.
        **params: Category-specific request parameters.

    Returns:
        A :class:`Resolution` carrying the answer and the full trace.

    Raises:
        ValueError: ``category`` is not in the chain table.
        ChainExhausted: The chain could not serve the request.
    """
    chain = _resolve_category(category)
    attempts: list[Attempt] = []

    for name in chain:
        provider = get_provider(name)
        if provider is None:
            # A chain entry with no adapter module. Recorded, never silent: an
            # unimplemented slot must not look like an empty result.
            attempts.append(
                Attempt(
                    provider=name,
                    outcome=OUTCOME_UNREGISTERED,
                    detail="no adapter registered for this provider",
                )
            )
            continue

        reason = provider.unavailable_reason()
        if reason:
            attempts.append(
                Attempt(provider=name, outcome=OUTCOME_UNAVAILABLE, detail=reason)
            )
            continue

        if not provider.supports(category):
            attempts.append(
                Attempt(
                    provider=name,
                    outcome=OUTCOME_UNSUPPORTED,
                    detail=f"adapter has no {category} capability",
                )
            )
            continue

        result, attempt = _call_once(provider, category, params)
        attempts.append(attempt)
        if result is not None:
            return Resolution(result=result, attempts=tuple(attempts))

    raise ChainExhausted(category, attempts)


def call_provider(provider_name: str, category: str, **params: Any) -> ProviderResult:
    """Call one named provider directly, bypassing the chain.

    For explicit requests (``source=<provider>``) where the caller has stated
    which provenance it wants and a fallback would return a different caliber.

    Args:
        provider_name: Exact provider name.
        category: Category the caller wants from that provider.
        **params: Category-specific request parameters.

    Returns:
        The provider's answer.

    Raises:
        ProviderError: The provider is missing, unavailable, unsupported for the
            category, or the call itself failed.
    """
    from src.data_providers.errors import ProviderUnavailable  # noqa: PLC0415

    provider = get_provider(provider_name)
    if provider is None:
        raise ProviderUnavailable(
            provider_name, "no adapter registered for this provider", category=category
        )
    reason = provider.unavailable_reason()
    if reason:
        raise ProviderUnavailable(provider_name, reason, category=category)
    if not provider.supports(category):
        raise ProviderUnavailable(
            provider_name,
            f"adapter has no {category} capability",
            category=category,
        )
    # The provider's own error propagates unchanged: an explicit caller needs
    # the status code and subclass to tell "you asked for a tier you do not
    # have" from "this provider is down".
    started = time.monotonic()
    data = provider.fetch(category, **params)
    return ProviderResult(
        provider=provider_name,
        category=category,
        data=data,
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )


def describe_chains() -> dict[str, list[str]]:
    """Return the chain table as ``{category: [provider, ...]}`` for display."""
    return {category: list(chain) for category, chain in CATEGORY_CHAINS.items()}
