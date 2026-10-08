"""Tests for the category-based provider failover resolver.

Every provider is a fake registered into the live registry for the duration of a
test; nothing here reaches the network. The behaviour under test is the rule the
layer exists for: one chain, first answer wins, and a refusing provider is
*skipped*, never retried.
"""

from __future__ import annotations

import json

import pytest

from src.data_providers import (
    CATEGORIES,
    CATEGORY_CHAINS,
    CATEGORY_CHAIN_EXTENSIONS,
    ChainExhausted,
    ProviderHTTPError,
    call_provider,
    chain_for,
    registry,
    resolve,
    resolve_with_trace,
)
from src.data_providers.base import Provider

# A category that does not exist in the shipped table; monkeypatched per test so
# the suite never depends on a real provider being configured on the machine.
CATEGORY = "test_category"

#: ``(category, provider)`` pairs the operator's priority table names but no
#: adapter implements. Each entry is a measured, documented gap — the resolver
#: reports the slot as ``unsupported`` rather than pretending it is a source.
_KNOWN_BASE_CHAIN_GAPS: frozenset[tuple[str, str]] = frozenset(
    {
        # Benzinga ships news/calendars/ratings; it has no bars capability, so
        # the table's third-position entry for core_stock_apis cannot serve it.
        ("core_stock_apis", "benzinga"),
    }
)


def _chain(monkeypatch: pytest.MonkeyPatch, *names: str) -> None:
    """Point ``CATEGORY`` at exactly *names*, in order."""
    monkeypatch.setitem(CATEGORY_CHAINS, CATEGORY, tuple(names))


def _fake(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    *,
    outcome: str = "ok",
    available: bool = True,
    supports: bool = True,
    status: int | None = None,
    calls: list[str] | None = None,
    payload: object | None = None,
    defect: type[BaseException] | None = None,
) -> None:
    """Register a fake provider named *name* under ``CATEGORY``.

    Args:
        monkeypatch: Active monkeypatch fixture.
        name: Provider name to register.
        outcome: ``ok`` to answer, ``error`` to raise a provider error.
        available: Whether ``unavailable_reason()`` reports a blocker.
        supports: Whether the adapter declares ``CATEGORY``.
        status: HTTP status carried by a raised error.
        calls: List the fake appends to when it is actually invoked, so a test
            can prove a skipped provider was never called.
    """

    class _Fake(Provider):
        pass

    _Fake.name = name
    _Fake.env_keys = ()
    _Fake.capabilities = {CATEGORY: "handle"} if supports else {}

    def _unavailable_reason(_self: Provider) -> str:
        return "" if available else f"{name} has no credentials"

    def _handle(_self: Provider, **params: object) -> object:
        if calls is not None:
            calls.append(name)
        if defect is not None:
            raise defect("adapter bug")
        if outcome == "ok":
            return {"served_by": name, "params": params} if payload is None else payload
        raise ProviderHTTPError(name, "upstream refused", category=CATEGORY, status=status)

    _Fake.unavailable_reason = _unavailable_reason  # type: ignore[method-assign]
    _Fake.handle = _handle  # type: ignore[attr-defined]
    monkeypatch.setitem(registry.PROVIDERS, name, _Fake())


class TestFailover:
    """The chain contract: first answer wins, failures fail over, no retries."""

    def test_head_of_chain_that_answers_is_the_only_call(self, monkeypatch):
        calls: list[str] = []
        _chain(monkeypatch, "a", "b")
        _fake(monkeypatch, "a", calls=calls)
        _fake(monkeypatch, "b", calls=calls)

        result = resolve(CATEGORY, symbol="X")

        assert result.provider == "a"
        assert calls == ["a"], "later providers must not be called once one answered"

    def test_http_error_fails_over_and_is_called_exactly_once(self, monkeypatch):
        calls: list[str] = []
        _chain(monkeypatch, "a", "b")
        _fake(monkeypatch, "a", outcome="error", status=404, calls=calls)
        _fake(monkeypatch, "b", calls=calls)

        result = resolve(CATEGORY)

        assert result.provider == "b"
        assert calls == ["a", "b"], "403/404/429 must fail over once, never retry"

    def test_status_is_recorded_for_every_failure(self, monkeypatch):
        _chain(monkeypatch, "a")
        _fake(monkeypatch, "a", outcome="error", status=429)

        with pytest.raises(ChainExhausted) as excinfo:
            resolve(CATEGORY)

        assert excinfo.value.attempts[0].status == 429
        assert excinfo.value.attempts[0].outcome == "failed"

    def test_unconfigured_provider_is_skipped_without_a_call(self, monkeypatch):
        calls: list[str] = []
        _chain(monkeypatch, "a", "b")
        _fake(monkeypatch, "a", available=False, calls=calls)
        _fake(monkeypatch, "b", calls=calls)

        result = resolve(CATEGORY)

        assert result.provider == "b"
        assert calls == ["b"], "an unconfigured provider must cost no request"

    def test_unsupported_provider_is_skipped_without_a_call(self, monkeypatch):
        calls: list[str] = []
        _chain(monkeypatch, "a", "b")
        _fake(monkeypatch, "a", supports=False, calls=calls)
        _fake(monkeypatch, "b", calls=calls)

        result = resolve(CATEGORY)

        assert result.provider == "b"
        assert calls == ["b"]

    def test_missing_required_param_fails_over_instead_of_raising(self, monkeypatch):
        """A request shape the adapter cannot express is a refusal, not a bug."""
        calls: list[str] = []
        _chain(monkeypatch, "a", "b")

        class _NeedsSymbol(Provider):
            name = "needs_symbol"
            env_keys = ()
            capabilities = {CATEGORY: "handle"}

            def handle(self: Provider, *, symbol: str) -> str:  # noqa: ARG002
                return symbol

        monkeypatch.setitem(registry.PROVIDERS, "a", _NeedsSymbol())
        _fake(monkeypatch, "b", calls=calls)

        result = resolve(CATEGORY)  # no symbol given

        assert result.provider == "b"
        assert calls == ["b"]

    def test_type_error_inside_an_adapter_still_propagates(self, monkeypatch):
        """Only binding failures are reclassified; real defects stay loud."""
        _chain(monkeypatch, "a")
        _fake(monkeypatch, "a", defect=TypeError)

        with pytest.raises(TypeError):
            resolve(CATEGORY)

    def test_unregistered_provider_is_reported_not_silent(self, monkeypatch):
        _chain(monkeypatch, "definitely_not_a_provider", "b")
        _fake(monkeypatch, "b")

        resolution = resolve_with_trace(CATEGORY)

        assert resolution.result.provider == "b"
        assert resolution.attempts[0].outcome == "unregistered"

    def test_exhausted_chain_lists_every_provider_and_reason(self, monkeypatch):
        _chain(monkeypatch, "a", "b")
        _fake(monkeypatch, "a", outcome="error", status=403)
        _fake(monkeypatch, "b", available=False)

        with pytest.raises(ChainExhausted) as excinfo:
            resolve(CATEGORY)

        assert [a.provider for a in excinfo.value.attempts] == ["a", "b"]
        assert [a.outcome for a in excinfo.value.attempts] == ["failed", "unavailable"]

    def test_params_reach_the_adapter_unchanged(self, monkeypatch):
        _chain(monkeypatch, "a")
        _fake(monkeypatch, "a")

        result = resolve(CATEGORY, symbol="AAPL.US", limit=5)

        assert result.data["params"] == {"symbol": "AAPL.US", "limit": 5}

    def test_unknown_category_is_a_caller_error(self):
        with pytest.raises(ValueError, match="unknown data category"):
            resolve("no_such_category")

    def test_explicit_provider_bypasses_the_chain(self, monkeypatch):
        calls: list[str] = []
        _chain(monkeypatch, "a", "b")
        _fake(monkeypatch, "a", outcome="error", status=403, calls=calls)
        _fake(monkeypatch, "b", calls=calls)

        result = call_provider("b", CATEGORY)

        assert result.provider == "b"
        assert calls == ["b"], "an explicit provider must not fall back to another"

    def test_explicit_provider_that_fails_does_not_fall_back(self, monkeypatch):
        calls: list[str] = []
        _chain(monkeypatch, "a", "b")
        _fake(monkeypatch, "a", outcome="error", status=404, calls=calls)
        _fake(monkeypatch, "b", calls=calls)

        with pytest.raises(ProviderHTTPError):
            call_provider("a", CATEGORY)

        assert calls == ["a"], "no silent fallback when the caller named a source"


class TestChainTable:
    """The shipped table must stay internally consistent and fully backed."""

    def test_every_category_has_a_chain(self):
        assert set(CATEGORIES) == set(CATEGORY_CHAINS)

    def test_no_chain_is_empty_and_names_are_trimmed(self):
        for category, chain in CATEGORY_CHAINS.items():
            assert chain, f"{category} has an empty chain"
            for name in chain:
                assert name == name.strip() and name == name.lower(), name

    def test_every_chain_provider_has_an_adapter(self):
        """A chain naming a provider with no adapter is a documented gap, not data."""
        missing = {
            name
            for category in CATEGORY_CHAINS
            for name in chain_for(category)
            if registry.get_provider(name) is None
        }
        assert not missing, f"chain providers without adapters: {sorted(missing)}"

    def test_operator_order_is_never_reordered_by_extensions(self):
        """Extensions may only be appended; the stated priority must survive."""
        for category, base in CATEGORY_CHAINS.items():
            assert chain_for(category)[: len(base)] == base

    def test_every_extension_is_backed_by_a_declared_capability(self):
        """An extension that cannot serve the category is a phantom fallback."""
        for category, extras in CATEGORY_CHAIN_EXTENSIONS.items():
            for name in extras:
                adapter = registry.get_provider(name)
                assert adapter is not None, f"no adapter for extension {name!r}"
                assert adapter.supports(category), (
                    f"{name} is listed as a {category} fallback but declares no "
                    f"{category} capability"
                )

    def test_base_chain_entries_without_a_capability_are_documented_gaps(self):
        """Every ``(category, provider)`` gap in the operator's table is listed.

        The operator's priority table names a provider per category; an adapter
        that ships no capability for one of those categories is a *documented*
        gap, never a silent one. Pinning the exact set means a newly added
        phantom entry (a chain slot nothing can ever serve) fails here instead
        of quietly costing a wasted request at runtime.
        """
        gaps = {
            (category, name)
            for category, chain in CATEGORY_CHAINS.items()
            for name in chain
            if registry.get_provider(name) is not None
            and not registry.get_provider(name).supports(category)
        }
        assert gaps == _KNOWN_BASE_CHAIN_GAPS, (
            f"undocumented gaps: {sorted(gaps - _KNOWN_BASE_CHAIN_GAPS)}; "
            f"stale entries: {sorted(_KNOWN_BASE_CHAIN_GAPS - gaps)}"
        )

    def test_extensions_do_not_duplicate_the_operator_list(self):
        for category, extras in CATEGORY_CHAIN_EXTENSIONS.items():
            assert not set(extras) & set(CATEGORY_CHAINS[category])


class TestToolEnvelope:
    """The agent-facing tool surfaces the trace and never hides a failure."""

    def test_success_envelope_carries_provider_and_attempts(self, monkeypatch):
        from src.tools.category_data_tool import CategoryDataTool

        _chain(monkeypatch, "a", "b")
        _fake(monkeypatch, "a", outcome="error", status=403)
        out = _fake(monkeypatch, "b") or None
        assert out is None

        payload = json.loads(CategoryDataTool().execute(category=CATEGORY))

        assert payload["ok"] is True
        assert payload["provider"] == "b"
        assert [a["outcome"] for a in payload["attempts"]] == ["failed", "ok"]

    def test_failure_envelope_names_every_provider(self, monkeypatch):
        from src.tools.category_data_tool import CategoryDataTool

        _chain(monkeypatch, "a", "b")
        _fake(monkeypatch, "a", outcome="error", status=403)
        _fake(monkeypatch, "b", available=False)

        payload = json.loads(CategoryDataTool().execute(category=CATEGORY))

        assert payload["ok"] is False
        assert [a["provider"] for a in payload["attempts"]] == ["a", "b"]

    def test_oversized_mapping_stays_valid_json_and_keeps_data(self, monkeypatch):
        """A payload over the character budget must shrink, not get cut."""
        from src.config.limits import TOOL_RESULT_LIMIT
        from src.tools.category_data_tool import CategoryDataTool

        huge = {
            "periods": [f"p{i}" for i in range(500)],
            "statements": {
                "income": [
                    {"period": f"2026-Q{i}", "value": "x" * 200} for i in range(200)
                ]
            },
        }
        _chain(monkeypatch, "a")
        _fake(monkeypatch, "a", payload=huge)

        raw = CategoryDataTool().execute(category=CATEGORY)

        assert len(raw) <= TOOL_RESULT_LIMIT
        payload = json.loads(raw)  # would raise on a character-truncated body
        assert payload["ok"] is True
        assert payload["data"] is not None, "shrinking must keep the payload, not drop it"
        assert payload.get("truncation"), "the cut must be reported"

    def test_adapter_defect_returns_an_envelope_not_an_exception(self, monkeypatch):
        """A non-ProviderError is an adapter bug; it must not kill the turn."""
        from src.tools.category_data_tool import CategoryDataTool

        _chain(monkeypatch, "a")
        _fake(monkeypatch, "a", defect=TypeError)

        payload = json.loads(CategoryDataTool().execute(category=CATEGORY))

        assert payload["ok"] is False
        assert "adapter defect" in payload["error"]
        assert "did not fail over" in payload["hint"]

    def test_explain_makes_no_request(self, monkeypatch):
        from src.tools.category_data_tool import CategoryDataTool

        calls: list[str] = []
        _chain(monkeypatch, "a")
        _fake(monkeypatch, "a", calls=calls)

        payload = json.loads(
            CategoryDataTool().execute(category=CATEGORY, explain=True)
        )

        assert payload["explained"] is True
        assert payload["providers"][0]["state"] == "ready"
        assert calls == [], "explain must not spend quota"
