"""Cboe provider adapter (no API key).

Cboe publishes *delayed* quotes for listed options and their underlyings on a
public CDN, one JSON document per underlying — both measured 200 on 2026-10-08::

    GET https://cdn.cboe.com/api/global/delayed_quotes/options/{UNDERLYING}.json
    GET https://cdn.cboe.com/api/global/delayed_quotes/quotes/{UNDERLYING}.json

The options document is the one this adapter uses: it carries the whole chain

    {"timestamp": ..., "data": {"options": [{"option": "AAPL261009C00110000",
      "bid", "bid_size", "ask", "ask_size", "iv", "open_interest", "volume",
      "delta", "gamma", "vega", "theta", "rho", "theo", "last_trade_price", ...}],
      "current_price", "bid", "ask", ...}}

and the same object already holds the underlying's own quote
(``data.current_price`` and friends), so the ``/quotes/`` document is
documented above but deliberately not fetched — one request answers both
categories.

``UNDERLYING`` is the bare ticker for an equity or ETF (``AAPL``, ``SPY``) and
the leading-underscore index symbol for a cash index (``_SPX``, ``_VIX``,
``_NDX``, ``_RUT``, ``_DJX``, ``_OEX``, ``_XSP``): the same symbol *without* the
underscore answers **403** with an S3 ``AccessDenied`` body, measured on
2026-10-08 for SPX, VIX, NDX, RUT, DJX, OEX and XSP alike, and a lower-case
ticker answers 403 too. Index products outside that verified set can be asked
for with the underscore spelled out (``"_VXN"``).

Contract symbols use the OCC layout (``ROOT + YYMMDD + C/P + strike*1000``), so
expiry, strike and right are derived from the symbol itself — the quoted numbers
are prices, not contract identity. There is no bulk or per-expiry endpoint: the
SPX chain is ~30k contracts, so ``expiration``/``option_type``/``limit`` exist
to keep a caller's payload finite.

``cboe`` is the only source in its ``options_surface`` chain. Its
``options_data`` capability is appended as an extension *after* the operator's
stated order (``moomoo``, ``yfinance``), so Cboe is only asked once both of
those refuse; the contract rows it emits follow the same
``contract_symbol``/``expiration`` vocabulary as those siblings.
"""

from __future__ import annotations

from typing import Any

from src.data_providers._http import fetch_json
from src.data_providers.base import Provider
from src.data_providers.errors import ProviderHTTPError
from src.data_providers.registry import register_provider

_BASE_URL = "https://cdn.cboe.com/api/global/delayed_quotes"
_HOST_KEY = "cboe"
_MIN_INTERVAL_ENV = "VIBE_TRADING_CBOE_MIN_INTERVAL"
_DEFAULT_MIN_INTERVAL_S = 0.4

#: CDN path segments for the two document kinds.
_OPTIONS_KIND = "options"

#: Index products Cboe serves under a leading-underscore symbol (verified 200
#: on 2026-10-08; each answers 403 without the underscore).
_INDEX_ROOTS = frozenset({"SPX", "VIX", "NDX", "RUT", "DJX", "OEX", "XSP"})

#: OCC strikes are integers in thousandths (``00200000`` is 200.000).
_STRIKE_SCALE = 1000.0

#: Length of the fixed-width OCC tail: YYMMDD (6) + C/P (1) + strike (8).
_OCC_TAIL_LEN = 15

_CALL = "call"
_PUT = "put"
_RIGHT_NAMES = {"C": _CALL, "P": _PUT}
_TYPE_ALIASES = {"c": _CALL, "call": _CALL, "p": _PUT, "put": _PUT}


def _parse_occ(contract: str) -> tuple[str | None, str | None, str | None, float | None]:
    """Split an OCC option symbol into ``(root, expiry, right, strike)``.

    Args:
        contract: OCC symbol such as ``SPX261016C00200000``.

    Returns:
        ``root`` (e.g. ``SPX``), ISO ``expiry`` (``2026-10-16``), ``right``
        (``call``/``put``) and ``strike`` (e.g. ``200.0``). Every element is
        ``None`` when the symbol is shorter than the fixed-width tail or the
        tail is not the expected digits/right — a non-standard symbol is kept in
        the payload with a null identity rather than dropped, because a dropped
        contract is invisible and a null one is not.
    """
    text = str(contract or "").strip()
    if len(text) <= _OCC_TAIL_LEN:
        return None, None, None, None
    tail = text[-_OCC_TAIL_LEN:]
    root = text[:-_OCC_TAIL_LEN]
    year, month, day = tail[0:2], tail[2:4], tail[4:6]
    right = tail[6]
    digits = tail[7:_OCC_TAIL_LEN]
    if not (
        root
        and year.isdigit()
        and month.isdigit()
        and day.isdigit()
        and right in _RIGHT_NAMES
        and digits.isdigit()
    ):
        return None, None, None, None
    return (
        root,
        f"20{year}-{month}-{day}",
        _RIGHT_NAMES[right],
        int(digits) / _STRIKE_SCALE,
    )


def _contract_row(contract: str, raw: dict[str, Any]) -> dict[str, Any]:
    """Normalise one Cboe option object.

    Args:
        contract: The OCC symbol from the ``option`` field.
        raw: The contract object as published by Cboe.

    Returns:
        A flat record: contract identity derived from the symbol plus the
        quoted prices, size, implied volatility, open interest, volume, greeks
        and theoretical value. Cboe sends the same field names for every
        contract, so the record shape is uniform across an entire chain.
    """
    root, expiry, right, strike = _parse_occ(contract)
    return {
        "contract_symbol": contract,
        "underlying": root,
        "expiration": expiry,
        "type": right,
        "strike": strike,
        "bid": raw.get("bid"),
        "ask": raw.get("ask"),
        "bid_size": raw.get("bid_size"),
        "ask_size": raw.get("ask_size"),
        "iv": raw.get("iv"),
        "open_interest": raw.get("open_interest"),
        "volume": raw.get("volume"),
        "delta": raw.get("delta"),
        "gamma": raw.get("gamma"),
        "theta": raw.get("theta"),
        "vega": raw.get("vega"),
        "rho": raw.get("rho"),
        "theo": raw.get("theo"),
        "last_trade_price": raw.get("last_trade_price"),
        "last_trade_time": raw.get("last_trade_time"),
        "prev_close": raw.get("prev_day_close"),
    }


def _underlying_quote(data: dict[str, Any]) -> dict[str, Any]:
    """Shape the underlying's delayed quote out of an options document.

    Args:
        data: The ``data`` object of a Cboe options document.

    Returns:
        The underlying's prices and 30-day implied volatility, keyed in this
        project's conventions (``last``/``prev_close`` rather than
        ``current_price``/``prev_day_close``).
    """
    return {
        "symbol": data.get("symbol"),
        "last": data.get("current_price"),
        "change": data.get("price_change"),
        "change_percent": data.get("price_change_percent"),
        "bid": data.get("bid"),
        "ask": data.get("ask"),
        "bid_size": data.get("bid_size"),
        "ask_size": data.get("ask_size"),
        "open": data.get("open"),
        "high": data.get("high"),
        "low": data.get("low"),
        "close": data.get("close"),
        "prev_close": data.get("prev_day_close"),
        "volume": data.get("volume"),
        "iv30": data.get("iv30"),
    }


@register_provider
class CboeProvider(Provider):
    """Key-less Cboe delayed-quote adapter."""

    name = "cboe"
    env_keys: tuple[str, ...] = ()
    capabilities = {
        "options_surface": "options_surface",
        "options_data": "options_data",
    }

    # -- options_surface ---------------------------------------------------

    def options_surface(
        self,
        *,
        symbol: str | None = None,
        underlying: str | None = None,
        expiration: str | None = None,
        option_type: str | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """Every listed contract on one underlying, as a flat surface.

        Args:
            symbol: Underlying ticker — ``AAPL``, ``SPY``, or an index with its
                CDN underscore (``SPX`` is mapped to ``_SPX`` automatically).
            expiration: Optional ``YYYY-MM-DD`` expiry filter.
            option_type: Optional ``"call"``/``"put"`` (``c``/``p`` accepted).
            limit: Keep only the first *limit* contracts after filtering
                (upstream order: expiry, then strike).

        Returns:
            ``{"underlying", "as_of", "spot", "delayed", "contracts": [...]}``
            where each contract is the record built by :func:`_contract_row`.

        Raises:
            ProviderHTTPError: Blank/unsupported symbol, a symbol Cboe has no
                document for (403 ``AccessDenied`` for an index written without
                its underscore), an unknown ``option_type``, or a payload
                without a ``data.options`` array.
        """
        ticker = symbol or underlying
        if not ticker:
            raise ProviderHTTPError(
                self.name,
                "options_surface requires a symbol (or underlying)",
                category="options_surface",
            )
        stem, payload, contracts = self._chain(ticker, category="options_surface")
        contracts = self._select(
            contracts,
            expiration=expiration,
            option_type=option_type,
            category="options_surface",
        )
        if limit and limit > 0:
            contracts = contracts[: int(limit)]
        return {
            "underlying": stem.lstrip("_"),
            "as_of": payload.get("timestamp"),
            "spot": payload["data"].get("current_price"),
            "delayed": True,
            "contracts": contracts,
        }

    # -- options_data ------------------------------------------------------

    def options_data(
        self,
        *,
        symbol: str,
        expiration: str | None = None,
        option_type: str | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """One underlying's option chain, grouped under its expirations.

        Same endpoint and same contract records as :meth:`options_surface`; the
        difference is the payload shape, which names the expirations present so
        a caller can iterate a chain without scanning every contract.

        Args:
            symbol: Underlying ticker, as in :meth:`options_surface`.
            expiration: Optional ``YYYY-MM-DD`` expiry filter.
            option_type: Optional ``"call"``/``"put"`` filter.
            limit: Keep only the first *limit* contracts after filtering.

        Returns:
            ``{"symbol", "underlying_quote": {...}, "expirations": [...],
            "as_of", "delayed", "contracts": [...]}``. ``expirations`` is
            sorted, and holds only expiries that survive the filters.

        Raises:
            ProviderHTTPError: As in :meth:`options_surface`.
        """
        stem, payload, contracts = self._chain(symbol, category="options_data")
        contracts = self._select(
            contracts,
            expiration=expiration,
            option_type=option_type,
            category="options_data",
        )
        if limit and limit > 0:
            contracts = contracts[: int(limit)]
        expirations = sorted(
            {row["expiration"] for row in contracts if row["expiration"]}
        )
        return {
            "symbol": stem.lstrip("_"),
            "underlying_quote": _underlying_quote(payload["data"]),
            "expirations": expirations,
            "as_of": payload.get("timestamp"),
            "delayed": True,
            "contracts": contracts,
        }

    # -- helpers -----------------------------------------------------------

    def _chain(
        self, symbol: str, *, category: str
    ) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
        """Fetch one underlying's options document and normalise its contracts.

        Args:
            symbol: Underlying ticker as supplied by the caller.
            category: Category the call belongs to, for the raised error.

        Returns:
            ``(file_stem, payload, contracts)`` — the CDN file stem actually
            requested, the whole decoded document (its ``data`` object is the
            chain, its ``timestamp`` the quote time) and the normalised contract
            records in upstream order.

        Raises:
            ProviderHTTPError: Blank/unsupported symbol, or a document without
                a usable ``data.options`` array.
        """
        stem = self._underlying(symbol, category=category)
        payload = fetch_json(
            provider=self.name,
            category=category,
            url=f"{_BASE_URL}/{_OPTIONS_KIND}/{stem}.json",
            host_key=_HOST_KEY,
            min_interval_env=_MIN_INTERVAL_ENV,
            min_interval_default=_DEFAULT_MIN_INTERVAL_S,
        )
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            raise ProviderHTTPError(
                self.name,
                "Cboe options payload had no data object",
                category=category,
            )
        raw = data.get("options")
        if not isinstance(raw, list):
            raise ProviderHTTPError(
                self.name,
                "Cboe options payload had no data.options array",
                category=category,
            )
        contracts = [
            _contract_row(str(row.get("option") or ""), row)
            for row in raw
            if isinstance(row, dict)
        ]
        return stem, payload, contracts

    def _underlying(self, symbol: str, *, category: str) -> str:
        """Map a project symbol onto Cboe's CDN file stem.

        Args:
            symbol: Project ticker, e.g. ``SPX``, ``_SPX``, ``AAPL``.
            category: Category the call belongs to, for the raised error.

        Returns:
            The file stem: the ticker upper-cased with a ``.US`` suffix
            stripped, and a leading underscore for the verified index products.

        Raises:
            ProviderHTTPError: A blank symbol or one carrying a market suffix
                (``.HK``, ``.L``) that Cboe's US CDN does not list.
        """
        upper = str(symbol or "").strip().upper()
        if upper.endswith(".US"):
            upper = upper[:-3]
        if upper and "." not in upper and upper.replace("_", "").isalnum():
            if upper.startswith("_"):
                return upper
            return f"_{upper}" if upper in _INDEX_ROOTS else upper
        raise ProviderHTTPError(
            self.name,
            f"{symbol!r} is not a Cboe US optionable underlying",
            category=category,
        )

    def _select(
        self,
        contracts: list[dict[str, Any]],
        *,
        expiration: str | None,
        option_type: str | None,
        category: str,
    ) -> list[dict[str, Any]]:
        """Apply the chain filters to normalised contracts.

        Args:
            contracts: Records from the options document, in upstream order.
            expiration: Optional ``YYYY-MM-DD`` expiry to keep.
            option_type: Optional call/put selector.
            category: Category the call belongs to, for the raised error.

        Returns:
            The surviving records (the full list when both filters are absent).

        Raises:
            ProviderHTTPError: An ``option_type`` that is neither a call nor a
                put, so the chain fails over instead of returning a filtered
                payload the caller cannot interpret.
        """
        wanted_expiry = str(expiration).strip() if expiration else None
        if option_type:
            wanted_type = _TYPE_ALIASES.get(str(option_type).strip().casefold())
            if wanted_type is None:
                raise ProviderHTTPError(
                    self.name,
                    f"unsupported option_type {option_type!r}; use 'call' or 'put'",
                    category=category,
                )
        else:
            wanted_type = None

        selected = contracts
        if wanted_expiry:
            selected = [
                row for row in selected if row["expiration"] == wanted_expiry
            ]
        if wanted_type:
            selected = [row for row in selected if row["type"] == wanted_type]
        return selected
