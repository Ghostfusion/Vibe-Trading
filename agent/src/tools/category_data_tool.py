"""Read-only tool: category-based data retrieval with single-chain failover.

One call answers "give me <category> for <symbol>" by walking that category's
ordered provider chain (`src.data_providers.categories`) and returning the first
provider that answers. Providers are never retried: the moment one refuses
(HTTP 404/403/429/5xx, an in-band error envelope, or a local-gateway permission
error) the next provider in the chain is tried, once.

The envelope always carries the attempt trace, so a caller can see exactly which
provider served the data and which ones were skipped because they are
unconfigured, unimplemented, or refused. ``explain=true`` returns that trace
without making any request, which is the way to ask "who would serve this?"
before spending quota.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from src.agent.tools import BaseTool
from src.config.limits import TOOL_RESULT_LIMIT
from src.data_providers import (
    CATEGORY_CHAINS,
    CATEGORY_SHAPES,
    ChainExhausted,
    ProviderError,
    call_provider,
    chain_for,
    get_provider,
    resolve_with_trace,
)
from src.tools._result_paging import fit_records

logger = logging.getLogger(__name__)

#: Rows delivered per call before paging kicks in.
_DEFAULT_ROW_LIMIT = 200


class CategoryDataTool(BaseTool):
    """Fetch a data category from the first provider that can serve it."""

    name = "get_category_data"
    description = (
        "Fetch one kind of market data (a 'category') from a chain of providers, "
        "returning the first provider that answers. Use this when you need data "
        "that several vendors carry and you do not care which one supplies it: "
        "core_stock_apis (daily bars), news_data, fundamental_data, "
        "analyst_ratings, earnings_calendar, technical_indicators, options_data, "
        "short_interest, institution_data, macro_data, prediction_markets, "
        "corporate_actions, news_sentiment, capital_flow, earnings_catalyst, "
        "earnings_surprise, economic_calendar, expected_move, fed_watch, "
        "market_breadth, revenue_breakdown, smart_money, analyst_actions, "
        "fda_calendar, guidance_revisions, offerings_calendar, or the "
        "single-vendor categories sec_filings, risk_free_curve, options_surface, "
        "exchange_symbols, equity_screener, market_movers. "
        "Providers are never retried: a 404/403/429 or a permission error moves "
        "straight to the next provider in the chain. The envelope reports which "
        "provider served the request and which candidates were skipped or failed. "
        "Set explain=true to list the chain and every provider's state without "
        "fetching anything. "
        'Example: {"category": "core_stock_apis", "params": {"symbol": "AAPL.US", '
        '"start": "2026-09-01", "limit": 30}}.'
    )
    parameters = {
        "type": "object",
        "properties": {
            "category": {
                "type": "string",
                "enum": sorted(CATEGORY_CHAINS),
                "description": "The kind of data to fetch.",
            },
            "params": {
                "type": "object",
                "description": (
                    "Request parameters passed unchanged to the provider "
                    "adapter. Common keys: symbol, market, start, end, interval, "
                    "limit, query. Provider-specific keys are accepted too "
                    "(series_id for macro_data, kind for corporate_actions, "
                    "exchange for exchange_symbols, tenor for risk_free_curve)."
                ),
                "additionalProperties": True,
            },
            "provider": {
                "type": "string",
                "description": (
                    "Optional explicit provider. Bypasses the chain entirely: "
                    "use it only when the caller has asked for a specific "
                    "source, because there is no failover and a provider that "
                    "cannot serve the request fails the call."
                ),
            },
            "explain": {
                "type": "boolean",
                "description": (
                    "When true, return the category's chain and each provider's "
                    "state (available / unconfigured / unimplemented) without "
                    "making any network request."
                ),
                "default": False,
            },
            "limit": {
                "type": "integer",
                "description": (
                    f"Maximum records to deliver per call (default "
                    f"{_DEFAULT_ROW_LIMIT}). A longer payload is paged whole-"
                    "record rather than cut mid-structure."
                ),
                "default": _DEFAULT_ROW_LIMIT,
            },
        },
        "required": ["category"],
    }
    repeatable = True
    is_readonly = True

    def execute(self, **kwargs: Any) -> str:
        """Serve one category through its failover chain.

        Args:
            **kwargs: ``category`` (required), plus optional ``params`` (dict
                forwarded to the adapter), ``provider`` (explicit source,
                no failover), ``explain`` (chain state only) and ``limit``
                (records per page).

        Returns:
            A JSON string envelope. On success:
            ``{"ok": true, "category", "provider", "elapsed_ms", "attempts":
            [{"provider", "outcome", "detail", "status"}], "data": ...}``.
            On a fully exhausted chain: ``{"ok": false, "category",
            "error", "attempts": [...]}``. The ``attempts`` list is present in
            both cases so a failure names every provider that could not serve
            the request, in chain order.
        """
        category = str(kwargs.get("category") or "").strip()
        if not category:
            return _error("category is required")
        if category not in CATEGORY_CHAINS:
            return _error(
                f"unknown category {category!r}", hint=f"known: {sorted(CATEGORY_CHAINS)}"
            )

        if bool(kwargs.get("explain")):
            return _serialize(_explain_payload(category))

        params = kwargs.get("params")
        request: dict[str, Any] = dict(params) if isinstance(params, dict) else {}
        row_limit = _clamp_limit(kwargs.get("limit"), _DEFAULT_ROW_LIMIT)

        provider_name = str(kwargs.get("provider") or "").strip()
        attempts: list[dict[str, Any]] = []
        try:
            if provider_name:
                result = call_provider(provider_name, category, **request)
                attempts = [{"provider": provider_name, "outcome": "ok"}]
            else:
                resolution = resolve_with_trace(category, **request)
                result = resolution.result
                attempts = [_attempt_dict(a) for a in resolution.attempts]
        except ChainExhausted as exc:
            return _serialize(
                {
                    "ok": False,
                    "category": category,
                    "error": exc.reason,
                    "attempts": [_attempt_dict(a) for a in exc.attempts],
                    "chain": list(chain_for(category)),
                }
            )
        except ProviderError as exc:
            return _serialize(
                {
                    "ok": False,
                    "category": category,
                    "provider": exc.provider,
                    "error": exc.reason,
                    "attempts": attempts,
                }
            )
        except ValueError as exc:
            return _error(str(exc))
        except Exception as exc:  # noqa: BLE001 - an adapter defect must not kill the turn
            # A non-ProviderError from an adapter is an adapter bug: the resolver
            # deliberately does not swallow it (a failover would hide it). The
            # agent-facing envelope still has to survive it, and must say
            # plainly that no failover happened.
            logger.exception("category_data adapter defect on %s", category)
            return _serialize(
                {
                    "ok": False,
                    "category": category,
                    "error": f"provider adapter defect: {type(exc).__name__}: {exc}",
                    "attempts": attempts,
                    "hint": (
                        "This is not an upstream failure, so the chain did not "
                        "fail over. Report it against the adapter named in "
                        "attempts."
                    ),
                }
            )

        envelope = {
            "ok": True,
            "category": category,
            "provider": result.provider,
            "elapsed_ms": result.elapsed_ms,
            "attempts": attempts,
            "shape": CATEGORY_SHAPES.get(category, ""),
        }
        data = result.data
        if isinstance(data, list):
            text = fit_records(
                data,
                0,
                lambda page, paging: dict(envelope, data=page, paging=paging),
                max_records=row_limit,
            )
            if len(text) <= TOOL_RESULT_LIMIT:
                return text
            # A single record wider than the whole budget: let the shrinker cut
            # it down rather than hand back a fragment.
            return _serialize(dict(envelope, data=data))
        return _serialize(dict(envelope, data=data))

    @classmethod
    def check_available(cls) -> bool:
        """Always available: the tool's job is to report which providers are.

        Returning ``False`` here would hide the diagnostic value of the tool on
        a machine with no provider credentials — the case where an operator most
        needs to see why a category cannot be served.
        """
        return True


def _explain_payload(category: str) -> dict[str, Any]:
    """Build the no-request payload describing a category's providers."""
    providers: list[dict[str, Any]] = []
    for name in chain_for(category):
        adapter = get_provider(name)
        if adapter is None:
            providers.append(
                {
                    "provider": name,
                    "state": "unimplemented",
                    "detail": "no adapter registered",
                }
            )
            continue
        reason = adapter.unavailable_reason()
        if reason:
            state, detail = "unconfigured", reason
        elif not adapter.supports(category):
            state, detail = "unimplemented", f"adapter has no {category} capability"
        else:
            state, detail = "ready", ""
        providers.append({"provider": name, "state": state, "detail": detail})
    return {
        "ok": True,
        "category": category,
        "explained": True,
        "chain": list(chain_for(category)),
        "providers": providers,
        "shape": CATEGORY_SHAPES.get(category, ""),
    }


def _attempt_dict(attempt: Any) -> dict[str, Any]:
    """Render one resolver :class:`Attempt` for the envelope."""
    out: dict[str, Any] = {
        "provider": attempt.provider,
        "outcome": attempt.outcome,
    }
    if attempt.detail:
        out["detail"] = attempt.detail
    if attempt.status is not None:
        out["status"] = attempt.status
    return out


def _clamp_limit(value: Any, default: int) -> int:
    """Coerce a requested row limit into a sane positive range."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, min(n, 5000))


def _serialize(payload: dict[str, Any]) -> str:
    """Serialize an envelope, shrinking it structurally when it is oversized.

    Character-truncating a JSON envelope leaves an unparseable fragment, so an
    oversized payload is reduced by halving its largest embedded list instead.
    The cut is recorded under ``truncation`` so a partial answer stays
    self-describing, and the result is always valid JSON within the shared
    character budget.
    """
    text = json.dumps(payload, ensure_ascii=False, default=str)
    if len(text) <= TOOL_RESULT_LIMIT:
        return text

    shrinkable = json.loads(text)  # deep copy that is safe to mutate
    for _ in range(200):
        # Scale the largest list proportionally to the overflow rather than
        # halving it: a 62 KB fundamentals bundle is 6x the budget, and halving
        # alone would need many rounds to converge (and would give up first).
        cut = _shrink_largest_list(shrinkable, ratio=TOOL_RESULT_LIMIT / len(text))
        if cut is None:
            break
        key, removed = cut
        truncation = shrinkable.setdefault("truncation", {})
        truncation[key] = truncation.get(key, 0) + removed
        text = json.dumps(shrinkable, ensure_ascii=False, default=str)
        if len(text) <= TOOL_RESULT_LIMIT:
            return text

    # A single record is itself larger than the budget: return a valid summary
    # rather than a broken fragment, and say what was withheld.
    summary = {
        key: value
        for key, value in payload.items()
        if key not in {"data", "attempts"}
    }
    summary["truncated"] = True
    summary["payload_bytes"] = len(text)
    data = payload.get("data")
    summary["data_keys"] = sorted(data) if isinstance(data, dict) else f"list[{len(data)}]" if isinstance(data, list) else None
    summary["hint"] = (
        "The payload exceeded the tool result budget even after paging; narrow "
        "the request with params such as limit, start/end or a single symbol."
    )
    return json.dumps(summary, ensure_ascii=False, default=str)[:TOOL_RESULT_LIMIT]


def _shrink_largest_list(root: dict[str, Any], *, ratio: float) -> tuple[str, int] | None:
    """Cut the longest list anywhere inside *root* down toward *ratio*.

    Args:
        root: The envelope to mutate in place.
        ratio: Fraction of each list to keep (``limit / current_size``), clamped
            to ``[1/len, 1/2]`` so a cut always makes progress and never empties
            a list.

    Returns:
        ``(key, removed)`` for the list that was cut, or ``None`` when nothing
        list-shaped was found.
    """
    best: tuple[int, dict[str, Any], str] | None = None
    stack: list[Any] = [root]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, list) and value:
                    if best is None or len(value) > best[0]:
                        best = (len(value), node, key)
                    stack.extend(item for item in value if isinstance(item, (dict, list)))
                elif isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(node, list):
            stack.extend(item for item in node if isinstance(item, (dict, list)))
    if best is None:
        return None
    _, container, key = best
    original = container[key]
    keep = int(len(original) * max(min(ratio, 0.5), 1.0 / len(original)))
    keep = max(1, min(keep, len(original) - 1))
    container[key] = original[:keep]
    return key, len(original) - keep


def _error(message: str, *, hint: str | None = None) -> str:
    """Render a request-level failure envelope."""
    payload: dict[str, Any] = {"ok": False, "error": message}
    if hint:
        payload["hint"] = hint
    return _serialize(payload)
