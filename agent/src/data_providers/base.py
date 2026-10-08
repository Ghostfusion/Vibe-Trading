"""Provider contract for the category-based failover layer.

A provider is a thin adapter over one upstream (its REST API, or the local
OpenD gateway). It declares:

* ``name`` — the identifier used in :data:`~src.data_providers.categories.CATEGORY_CHAINS`;
* ``env_keys`` — env var names that must be non-empty for the provider to be a
  candidate at all (credentials, or a gateway host);
* ``capabilities`` — category -> method name, so the resolver can skip a
  provider that simply has no implementation for the requested category
  instead of calling it and hoping.

Adapters only ever raise :class:`~src.data_providers.errors.ProviderError`
subclasses. Anything else escaping an adapter is a bug in the adapter, not a
failover signal, so the resolver does not swallow it.

Credentials are read through :class:`~src.config.env_schema.DataConfig`; reading
``os.environ`` here would violate the repository's AST env gate.
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, ClassVar, Mapping

from src.config.accessor import get_env_config
from src.config.env_schema import DataConfig
from src.data_providers.errors import ProviderHTTPError, ProviderUnavailable

#: env alias -> DataConfig field name, so an adapter can look up a credential by
#: the env var name it declares instead of duplicating the field name.
_ALIAS_TO_FIELD: dict[str, str] = {
    field.alias: field_name
    for field_name, field in DataConfig.model_fields.items()
    if field.alias
}


def env_value(env_name: str) -> str:
    """Return the configured value of *env_name*, or an empty string.

    Args:
        env_name: The env var name as declared in ``DataConfig``'s aliases.

    Returns:
        The value as a stripped string; ``""`` when unset, empty, or not a
        recognised configuration key.
    """
    field_name = _ALIAS_TO_FIELD.get(env_name)
    if field_name is None:
        return ""
    value = getattr(get_env_config().data, field_name, "")
    return str(value).strip() if value is not None else ""


def data_config() -> DataConfig:
    """Return the shared :class:`DataConfig` (credentials, hosts, tunables)."""
    return get_env_config().data


@dataclass(frozen=True)
class ProviderResult:
    """One successful provider answer.

    Attributes:
        provider: Provider that served the request.
        category: Category that was requested.
        data: Provider payload, already normalised by the adapter.
        elapsed_ms: Wall-clock duration of the adapter call.
    """

    provider: str
    category: str
    data: Any
    elapsed_ms: int


class Provider(ABC):
    """Base class for provider adapters.

    Subclasses declare :attr:`capabilities` as ``{category: method_name}`` and
    implement each named method with a keyword-only signature matching the
    category's request vocabulary. ``fetch`` is the only entry point the
    resolver uses.
    """

    #: Provider identifier used in category chains.
    name: ClassVar[str] = ""
    #: Env var names that gate availability; empty tuple = always available.
    env_keys: ClassVar[tuple[str, ...]] = ()
    #: category -> adapter method name.
    capabilities: ClassVar[Mapping[str, str]] = {}

    def available(self) -> bool:
        """Whether this provider can be attempted at all.

        The default checks that every declared ``env_keys`` entry is non-empty.
        Providers with a runtime prerequisite (a local gateway, a socket) must
        override this.
        """
        return not self.unavailable_reason()

    def unavailable_reason(self) -> str:
        """Why this provider cannot be attempted; ``""`` when it can."""
        for key in self.env_keys:
            if not env_value(key):
                return f"{key} is not configured"
        return ""

    def supports(self, category: str) -> bool:
        """Whether this provider implements *category*."""
        return category in self.capabilities

    def fetch(self, category: str, **params: Any) -> Any:
        """Serve *category* with *params*.

        Args:
            category: Category name; must be in :attr:`capabilities`.
            **params: Category-specific request parameters.

        Returns:
            The adapter's normalised payload.

        Raises:
            ProviderUnavailable: When the category is not implemented. The
                resolver pre-filters with :meth:`supports`, so this only fires
                on direct misuse.
            ProviderHTTPError: When the request cannot be bound to the adapter's
                parameters at all (a missing ``symbol``, say) — a refusal the
                chain should fail over on rather than an opaque ``TypeError``.
        """
        method_name = self.capabilities.get(category)
        if method_name is None:
            raise ProviderUnavailable(
                self.name,
                f"does not implement category {category!r}",
                category=category,
            )
        handler = getattr(self, method_name)
        try:
            return handler(**params)
        except TypeError as exc:
            # A TypeError whose innermost frame IS this call is an argument
            # binding failure: the caller asked for a request shape this
            # provider cannot express (usually a missing symbol). That is a
            # provider-level refusal, so the chain must fail over rather than
            # the turn dying on an opaque TypeError. A TypeError raised *inside*
            # the adapter body is a real defect and is re-raised.
            if exc.__traceback__ is not None and exc.__traceback__.tb_next is None:
                raise ProviderHTTPError(
                    self.name,
                    f"cannot serve {category!r} with the given parameters: {exc}",
                    category=category,
                ) from exc
            raise
