"""Moomoo (Futu) adapter over the local OpenD gateway.

Moomoo data does not come from a REST host: the ``futu-api`` SDK talks to
``OpenD``, a user-owned gateway that listens on ``127.0.0.1:11111`` by default.
The endpoint is read from ``MOOMOO_HOST``/``MOOMOO_PORT`` and falls back to the
``FUTU_HOST``/``FUTU_PORT`` names (the same gateway serves the futu OHLCV loader
and trading connector), then to ``127.0.0.1``/``11111``. No port env var is
required, so the provider is "available" whenever ``futu`` imports *and* a TCP
socket to the gateway accepts — OpenD being down is an unavailability, not a
failed request.

One ``OpenQuoteContext`` is opened per call and closed in a ``finally``: the
gateway connection is a scarce, stateful resource and a process-global socket
would leak subscriptions and mask a dead gateway.

Every SDK read returns a tuple whose first element is a ``ret_code``
(``RET_OK == 0``). A non-zero code — including a permission refusal such as
``"No permission to get quotes for US.AAPL. Please check US MarketOptions quote
permissions."`` — is mapped to :class:`ProviderHTTPError`, so the chain fails
over to the next provider instead of treating a permission wall as "no data".
Exceptions raised by the SDK are mapped the same way.

Tier notes measured on this account (2026-10-08, ``futu-api`` 10.11.7108):

* nearly everything answers ``ret_code == 0`` — snapshots, klines, capital
  flow, breadth, movers, earnings, macro, fundamentals, short interest,
  institutions, ARK/insider, ratings and the stock filter;
* ``get_option_expiration_date`` / ``get_option_chain`` answer ``ret_code ==
  -1`` "No permission to get quotes for US.AAPL. Please check US MarketOptions
  quote permissions." — implemented correctly anyway, so the options chain
  falls over to yfinance;
* ``get_corporate_actions_buybacks`` answers ``ret_code == -1`` "Only HK and A-share
  stocks and funds are supported; ..." for US — implemented, but not usable for US;
* ``get_search_news`` answered ``ret_code == 0`` with 3 rows for ``"Apple"``; it
  is a keyword search, so ``news_data`` is served as one;
* technical indicators have no synchronous value endpoint: ``get_indicator_list``
  is a catalog (``ret_code == 0``, 187 entries) and ``request_indicator_calc_async``
  returns only a ``calc_id`` (``ret_code == 0``) whose values arrive on the
  ``Qot_PushIndicatorCalc`` push. ``technical_indicators`` therefore waits for
  that push within the same call (measured: ``MA`` over ``US.AAPL`` returns
  ``MA1``..``MA9`` rows).

Symbols are accepted in either convention: this project's ``AAPL.US`` /
``00700.HK`` / ``600519.SH`` and Futu's ``US.AAPL`` / ``HK.00700`` /
``SH.600519``. A bare ticker defaults to the US market.
"""

from __future__ import annotations

import importlib
import math
import socket
import threading
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Any, Iterator, Mapping

from src.data_providers.base import Provider, env_value
from src.data_providers.errors import ProviderHTTPError, ProviderUnavailable
from src.data_providers.registry import register_provider

#: Two-letter exchange prefixes Futu uses in ``MARKET.CODE`` symbols. A left
#: half in this set is already a market prefix; anything else is swapped.
_MARKET_CODES = frozenset(
    {"US", "HK", "SH", "SZ", "SG", "JP", "AU", "CA", "MY", "CN", "UK", "IN", "KR", "TW", "VN"}
)

#: Default local OpenD endpoint when neither MOOMOO_* nor FUTU_* is configured.
_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 11111

#: TCP probe budget; the resolver calls :meth:`unavailable_reason` per request.
_PROBE_TIMEOUT_S = 0.5

#: Interval token -> Futu ``KLType`` attribute name used by ``request_history_kline``.
_KLTYPE = {
    "1m": "K_1M",
    "3m": "K_3M",
    "5m": "K_5M",
    "15m": "K_15M",
    "30m": "K_30M",
    "60m": "K_60M",
    "1h": "K_60M",
    "2h": "K_120M",
    "4h": "K_240M",
    "1d": "K_DAY",
    "day": "K_DAY",
    "1w": "K_WEEK",
    "week": "K_WEEK",
    "1mo": "K_MON",
    "1M": "K_MON",
}

#: Adjustment token -> Futu ``AuType`` attribute name.
_AUTYPE = {"qfq": "QFQ", "hfq": "HFQ", "none": "NONE"}

#: Interval-change token -> Futu ``RankPeriodType`` attribute name, so callers
#: may write ``5d``/``ytd`` instead of the SDK's ``FIVE_DAY``/``YTD``.
_PERIOD_CHANGE = {
    "5min": "FIVE_MIN",
    "5m": "FIVE_MIN",
    "1d": "ONE_DAY",
    "1day": "ONE_DAY",
    "5d": "FIVE_DAY",
    "5day": "FIVE_DAY",
    "20d": "TWENTY_DAY",
    "60d": "SIXTY_DAY",
    "120d": "ONE_TWENTY_DAY",
    "250d": "TWO_FIFTY_DAY",
    "ytd": "YTD",
}

#: ``get_earnings_calendar`` refuses an inclusive begin/end span wider than
#: seven days, so the furthest allowed end is ``begin + 6d`` (measured: a
#: ``2026-10-08``..``2026-10-15`` window answers ``ret_code == -1`` "Date range
#: must not exceed 7 days", while ``..2026-10-14`` succeeds).
_MAX_CALENDAR_WINDOW_DAYS = 6

#: ``prediction_markets`` bounds: events returned by default, the most a caller
#: may request, and how many series the selector-less walk scans before giving up.
_DEFAULT_EVENTS = 20
_MAX_EVENTS = 50
_MAX_SERIES_SCAN = 60

#: Keys Futu may carry an event's own date under, newest-first preference, used
#: to guarantee a ``date`` key on every ``corporate_actions`` row.
_CORPORATE_DATE_KEYS = (
    "date",
    "ex_date",
    "dir_deci_pub_date_str",
    "pub_date",
    "dividend_payable_date",
    "record_date",
)


def _clean(value: Any) -> Any:
    """Return *value* as a JSON-safe scalar (NaN/numpy unwrapped)."""
    if value is None:
        return None
    item = getattr(value, "item", None)
    if callable(item):
        try:
            value = item()
        except Exception:  # noqa: BLE001 - exotic scalar; keep the original
            return value
    if isinstance(value, float) and math.isnan(value):
        return None
    iso = getattr(value, "isoformat", None)
    if callable(iso):
        try:
            return value.isoformat()
        except Exception:  # noqa: BLE001 - pandas NaT and friends
            return str(value)
    return value


def _records(data: Any) -> list[dict[str, Any]]:
    """Normalise a Futu payload into a list of JSON-safe row dicts."""
    if data is None:
        return []
    if hasattr(data, "columns") and callable(getattr(data, "to_dict", None)):
        try:
            rows = list(data.to_dict("records"))
        except Exception:  # noqa: BLE001 - fall through to generic handling
            rows = []
        return [{k: _clean(v) for k, v in row.items()} for row in rows]
    if isinstance(data, Mapping):
        return [{str(k): _clean(v) for k, v in data.items()}]
    if isinstance(data, (list, tuple)):
        out: list[dict[str, Any]] = []
        for item in data:
            if isinstance(item, Mapping):
                out.append({str(k): _clean(v) for k, v in item.items()})
            elif hasattr(item, "__dict__"):
                out.append({k: _clean(v) for k, v in vars(item).items()})
        return out
    return []


def _first(row: Mapping[str, Any], names: tuple[str, ...], default: Any = None) -> Any:
    """Return the first present key from *names* in *row*."""
    for name in names:
        value = row.get(name)
        if value is not None:
            return value
    return default


def _tcp_open(host: str, port: int) -> bool:
    """Return whether a TCP connection to *host*:*port* is accepted."""
    try:
        with socket.create_connection((host, int(port)), timeout=_PROBE_TIMEOUT_S):
            return True
    except OSError:
        return False


def _env_port(value: Any) -> int:
    """Parse an OpenD port from an env value, or ``0`` when it is unset.

    ``env_value`` returns the ``DataConfig`` default as a string, so an unset
    ``MOOMOO_PORT`` yields ``"0"`` and an unset ``FUTU_PORT`` yields ``"11111"``.
    Treating ``""``, ``"0"`` and non-numeric text as unset lets the caller fall
    through to the next name (and then to the hard-coded default) instead of
    short-circuiting on the truthy ``"0"``.
    """
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        port = int(text)
    except (TypeError, ValueError):
        return 0
    return port if port > 0 else 0


def _clamp_count(value: Any, default: int, high: int) -> int:
    """Coerce *value* into ``1..high``, or *default* when it is not a number."""
    try:
        count = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return max(1, min(count, high))


@register_provider
class MoomooProvider(Provider):
    """Read-only adapter over the local Futu OpenD gateway."""

    name = "moomoo"
    #: No credential gates availability; the gateway socket does (see
    #: :meth:`unavailable_reason`). A gateway is optional infrastructure, so an
    #: empty env must not read as "unconfigured".
    env_keys: tuple[str, ...] = ()
    capabilities = {
        "core_stock_apis": "core_stock_apis",
        "news_data": "news_data",
        "technical_indicators": "technical_indicators",
        "capital_flow": "capital_flow",
        "market_breadth": "market_breadth",
        "market_movers": "market_movers",
        "options_data": "options_data",
        "expected_move": "expected_move",
        "earnings_calendar": "earnings_calendar",
        "earnings_surprise": "earnings_surprise",
        "earnings_catalyst": "earnings_catalyst",
        "economic_calendar": "economic_calendar",
        "fed_watch": "fed_watch",
        "macro_data": "macro_data",
        "prediction_markets": "prediction_markets",
        "corporate_actions": "corporate_actions",
        "short_interest": "short_interest",
        "institution_data": "institution_data",
        "smart_money": "smart_money",
        "analyst_ratings": "analyst_ratings",
        "analyst_actions": "analyst_actions",
        "fundamental_data": "fundamental_data",
        "revenue_breakdown": "revenue_breakdown",
        "exchange_symbols": "exchange_symbols",
        "equity_screener": "equity_screener",
    }

    # -- gateway plumbing --------------------------------------------------

    def _endpoint(self) -> tuple[str, int]:
        """Resolve the OpenD host and port from the environment.

        Returns:
            ``(host, port)``: ``MOOMOO_*`` first, then ``FUTU_*``, then the
            ``127.0.0.1:11111`` default. An empty host or a non-positive/garbled
            port is treated as unset, so an operator's ``FUTU_PORT`` is honoured
            even though ``MOOMOO_PORT`` resolves to its ``"0"`` config default.
        """
        host = env_value("MOOMOO_HOST") or env_value("FUTU_HOST") or _DEFAULT_HOST
        port = _env_port(env_value("MOOMOO_PORT")) or _env_port(env_value("FUTU_PORT"))
        return host.strip(), port if port > 0 else _DEFAULT_PORT

    def unavailable_reason(self) -> str:
        """Return why the gateway cannot be used, or ``""``.

        Non-empty when ``futu-api`` is not importable or nothing accepts a TCP
        connection at the resolved endpoint. This is the gateway-down case the
        resolver must skip without spending a request.
        """
        try:
            importlib.import_module("futu")
        except ImportError:
            return "futu-api is not installed"
        host, port = self._endpoint()
        if not _tcp_open(host, port):
            return (
                f"OpenD gateway is not reachable at {host}:{port} "
                "(OpenD must already be running and logged in)"
            )
        return ""

    @contextmanager
    def _context(self, category: str) -> Iterator[tuple[Any, Any]]:
        """Open one ``OpenQuoteContext`` for a single call and close it after.

        Args:
            category: Category being served, for error reporting.

        Yields:
            ``(futu_module, quote_context)``.

        Raises:
            ProviderUnavailable: ``futu`` is missing or the gateway is down.
        """
        try:
            futu = importlib.import_module("futu")
        except ImportError as exc:
            raise ProviderUnavailable(
                self.name, "futu-api is not installed", category=category
            ) from exc
        host, port = self._endpoint()
        if not _tcp_open(host, port):
            raise ProviderUnavailable(
                self.name,
                f"OpenD gateway is not reachable at {host}:{port}",
                category=category,
            )
        try:
            ctx = futu.OpenQuoteContext(host=host, port=port)
        except Exception as exc:  # noqa: BLE001 - gateway boundary, one attempt
            raise ProviderUnavailable(
                self.name,
                f"OpenD connection failed: {type(exc).__name__}: {exc}",
                category=category,
            ) from exc
        try:
            yield futu, ctx
        finally:
            # Never let a teardown error masquerade as a data failure, and never
            # leave a socket behind (the SDK would keep it for the process).
            try:
                ctx.close()
            except Exception:  # noqa: BLE001 - teardown is best-effort
                pass

    def _call(self, category: str, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Invoke one SDK read and enforce the ``ret_code`` contract.

        Args:
            category: Category being served, for error reporting.
            fn: Bound SDK method.
            *args: Positional arguments for *fn*.
            **kwargs: Keyword arguments for *fn*.

        Returns:
            The raw SDK tuple, whose ``[0]`` is ``RET_OK``.

        Raises:
            ProviderHTTPError: The SDK raised, or returned a non-zero
                ``ret_code`` (a refusal or permission error).
        """
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - gateway boundary, one attempt
            raise ProviderHTTPError(
                self.name, f"{type(exc).__name__}: {exc}", category=category
            ) from exc
        if not isinstance(result, (tuple, list)) or len(result) < 2:
            raise ProviderHTTPError(
                self.name, "unexpected Futu response shape", category=category
            )
        ret_code = result[0]
        if ret_code != 0:
            status = ret_code if isinstance(ret_code, int) else None
            raise ProviderHTTPError(
                self.name, str(result[1]), category=category, status=status
            )
        return result

    def _code(self, symbol: Any, category: str) -> str:
        """Translate a project symbol into Futu's ``MARKET.CODE`` convention.

        Args:
            symbol: ``AAPL.US``, ``00700.HK``, ``600519.SH``, an already-Futu
                ``US.AAPL``, or a bare ticker (assumed US).
            category: Category being served, for error reporting.

        Returns:
            Futu symbol, e.g. ``US.AAPL``.

        Raises:
            ProviderHTTPError: *symbol* is empty.
        """
        text = str(symbol or "").strip().upper()
        if not text:
            raise ProviderHTTPError(self.name, "symbol is required", category=category)
        if "." not in text:
            return f"US.{text}"
        left, right = text.split(".", 1)
        if left in _MARKET_CODES:
            return f"{left}.{right}"
        return f"{right}.{left}"

    @staticmethod
    def _rank_dir(futu: Any, direction: str | None) -> Any:
        """Map a ``gainers``/``losers`` token to a ``RankSortDir`` member."""
        descending = str(direction or "gainers").lower() not in {"losers", "down", "ascending"}
        enum = getattr(futu, "RankSortDir")
        return enum.DESCENDING if descending else enum.ASCENDING

    @staticmethod
    def _table(result: Any) -> list[dict[str, Any]]:
        """Return rows from a rank call, which wraps frames as ``(count, df)``."""
        data = result[1]
        if isinstance(data, tuple) and len(data) == 2 and hasattr(data[1], "columns"):
            data = data[1]
        return _records(data)

    @staticmethod
    def _market_frame(result: Any, market: str) -> Any:
        """Pick the US or HK frame from short-interest style ``(ret, us, hk)``.

        Args:
            result: The SDK tuple ``(ret_code, us_frame, hk_frame)``.
            market: Market code; anything other than ``HK`` uses the US frame.

        Returns:
            The chosen frame (a DataFrame), or ``None`` when absent.
        """
        if len(result) > 2 and str(market).upper() == "HK":
            return result[2]
        return result[1] if len(result) > 1 else None

    # -- core_stock_apis ---------------------------------------------------

    def core_stock_apis(
        self,
        *,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        interval: str = "1d",
        adjust: str = "qfq",
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Daily (or intraday) bars for *symbol* via ``request_history_kline``.

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.
            start: Inclusive ``YYYY-MM-DD`` lower bound.
            end: Inclusive ``YYYY-MM-DD`` upper bound.
            limit: Keep only the most recent *limit* bars. The whole
                ``start``..``end`` window (default: trailing 365 days) is
                fetched in one request and sliced, because the SDK's
                ``max_count`` returns the *earliest* N bars of the window plus a
                page key — the opposite of "latest N".
            interval: Bar size token, e.g. ``1d``/``1h``/``5m`` (default daily).
            adjust: ``qfq`` (default), ``hfq`` or ``none``.

        Returns:
            Ascending rows of ``{trade_date, open, high, low, close, volume,
            adjusted_close, turnover, change_rate, last_close, pe_ratio,
            turnover_rate}``.
        """
        with self._context("core_stock_apis") as (futu, ctx):
            ktype = getattr(futu.KLType, _KLTYPE.get(interval, "K_DAY"))
            autype = getattr(futu.AuType, _AUTYPE.get(str(adjust).lower(), "QFQ"))
            kwargs: dict[str, Any] = {"ktype": ktype, "autype": autype, "max_count": None}
            if start:
                kwargs["start"] = start
            if end:
                kwargs["end"] = end
            result = self._call(
                "core_stock_apis", ctx.request_history_kline, self._code(symbol, "core_stock_apis"), **kwargs
            )
        adjusted = str(adjust).lower() != "none"
        out: list[dict[str, Any]] = []
        for row in _records(result[1]):
            close = _first(row, ("close",))
            out.append(
                {
                    "trade_date": str(_first(row, ("time_key",), ""))[:10],
                    "open": _first(row, ("open",)),
                    "high": _first(row, ("high",)),
                    "low": _first(row, ("low",)),
                    "close": close,
                    "adjusted_close": close if adjusted else None,
                    "volume": _first(row, ("volume",)),
                    "turnover": _first(row, ("turnover",)),
                    "change_rate": _first(row, ("change_rate",)),
                    "last_close": _first(row, ("last_close",)),
                    "pe_ratio": _first(row, ("pe_ratio",)),
                    "turnover_rate": _first(row, ("turnover_rate",)),
                }
            )
        if limit and limit > 0:
            out = out[-int(limit):]
        return out

    # -- news_data ---------------------------------------------------------

    def news_data(
        self,
        *,
        query: str | None = None,
        keyword: str | None = None,
        symbol: str | None = None,
        limit: int = 10,
        sub_type: str | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Keyword news search via ``get_search_news``.

        moomoo's news search is keyword-based, not ticker-scoped: a ``symbol``
        request is served by searching the ticker text with its market suffix
        stripped (``AAPL.US``/``US.AAPL`` -> ``AAPL``), which can over-match
        noisy tickers. Prefer passing ``query`` explicitly when precision
        matters. No article body is returned, so ``summary`` is always ``None``.

        Args:
            query: Search text (preferred).
            keyword: Alias for ``query``.
            symbol: Fallback search term derived from the ticker.
            limit: Maximum articles (SDK default 10).
            sub_type: ``ALL``/``NEWS``/``NOTICE``/``RATING`` (SDK default ALL).

        Returns:
            Rows of ``{title, url, published, source, summary}``.

        Raises:
            ProviderHTTPError: No search term could be derived.
        """
        term = str(query or keyword or "").strip()
        if not term and symbol:
            text = str(symbol).strip()
            left, _, right = text.partition(".")
            term = right if right and left.upper() in _MARKET_CODES else left
        if not term:
            raise ProviderHTTPError(
                self.name, "news_data requires query/keyword or symbol", category="news_data"
            )
        with self._context("news_data") as (futu, ctx):
            kwargs: dict[str, Any] = {"max_count": max(1, int(limit))}
            if sub_type:
                member = getattr(futu.NewsSubType, str(sub_type).upper(), None)
                if member is not None:
                    kwargs["news_sub_type"] = member
            result = self._call("news_data", ctx.get_search_news, term, **kwargs)
        return [
            {
                "title": _first(row, ("title",)),
                "url": _first(row, ("url",)),
                "published": _first(row, ("publish_time",)),
                "source": _first(row, ("source",)),
                "summary": None,
            }
            for row in _records(result[1])
        ]

    # -- technical_indicators ----------------------------------------------

    def technical_indicators(
        self,
        *,
        symbol: str,
        indicator: str | None = None,
        name: str | None = None,
        output: str | int | None = None,
        interval: str = "1d",
        lang_type: str = "python",
        limit: int | None = None,
        start: str | None = None,
        end: str | None = None,
        input_params: list[dict[str, Any]] | None = None,
        timeout_s: float = 6.0,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Compute one technical indicator over *symbol*'s klines.

        Futu has no synchronous indicator-value endpoint: ``get_indicator_list``
        is a catalog and ``request_indicator_calc_async`` returns a ``calc_id``
        whose values arrive on the ``Qot_PushIndicatorCalc`` push. This adapter
        therefore fetches the klines, registers a one-shot push handler, issues
        the async calc, and waits (bounded by ``timeout_s``) for the matching
        ``calc_id`` inside the same call — still one ``OpenQuoteContext``, still
        one request, no retry.

        An indicator can have several output lines (``MA`` yields ``MA1``..``MA9``
        from the server's input defaults). ``output`` selects one by name or
        index; by default the first line is used.

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.
            indicator: Indicator short name as listed by ``indicator_list``
                (e.g. ``MA``/``RSI``/``MACD``).
            name: Alias for ``indicator``.
            output: Output line selector (name or 0-based index); default first.
            interval: Bar size token, e.g. ``1d`` (default daily).
            lang_type: ``python`` (default) or ``mylang``.
            limit: Keep only the most recent *limit* computed points.
            start: Optional kline window start.
            end: Optional kline window end.
            input_params: Raw SDK input overrides, ``[{index, value}, ...]``.
            timeout_s: Seconds to await the result push before failing.

        Returns:
            Ascending rows of ``{trade_date, value}`` for the chosen output line.

        Raises:
            ProviderHTTPError: No indicator name, an unknown ``output``, a
                gateway refusal, or no push within ``timeout_s``.
        """
        code = self._code(symbol, "technical_indicators")
        short = str(indicator or name or "").strip().upper()
        if not short:
            raise ProviderHTTPError(
                self.name, "technical_indicators requires indicator", category="technical_indicators"
            )
        lang = 1 if str(lang_type).lower() in {"mylang", "1"} else 2
        ktype = _KLTYPE.get(interval, "K_DAY")
        with self._context("technical_indicators") as (futu, ctx):
            kl_kwargs: dict[str, Any] = {
                "ktype": getattr(futu.KLType, ktype),
                "autype": getattr(futu.AuType, "QFQ"),
                "max_count": None,
            }
            if start:
                kl_kwargs["start"] = start
            if end:
                kl_kwargs["end"] = end
            klines = self._call("technical_indicators", ctx.request_history_kline, code, **kl_kwargs)[1]
            handler = self._calc_handler(futu)
            ctx.set_handler(handler)
            result = self._call(
                "technical_indicators",
                ctx.request_indicator_calc_async,
                short,
                lang,
                code,
                getattr(futu.KLType, ktype),
                klines,
                None,
                input_params,
            )
            calc_id = str(result[1])
            if not handler.wait(timeout_s):
                raise ProviderHTTPError(
                    self.name,
                    f"no indicator result push for {short!r} within {timeout_s}s",
                    category="technical_indicators",
                )
            content = handler.get(calc_id)
        if not isinstance(content, Mapping):
            raise ProviderHTTPError(
                self.name,
                f"no indicator result matched calc_id {calc_id!r}",
                category="technical_indicators",
            )
        outputs = content.get("outputs") or []
        index = self._pick_output(outputs, output, short)
        rows: list[dict[str, Any]] = []
        for row in content.get("output_rows") or []:
            values = row.get("values") or []
            rows.append(
                {
                    "trade_date": str(row.get("time", ""))[:10],
                    "value": _clean(values[index]) if index < len(values) else None,
                }
            )
        if limit and limit > 0:
            rows = rows[-int(limit):]
        return rows

    @staticmethod
    def _calc_handler(futu: Any) -> Any:
        """Build a one-shot push handler that captures indicator results.

        Defined per call because ``futu`` is imported lazily; the handler
        signals a :class:`threading.Event` from the SDK's callback thread so the
        caller can block synchronously.

        Args:
            futu: The imported ``futu`` module.

        Returns:
            An unbound handler instance with ``wait(timeout)`` and ``get(id)``.
        """

        class _IndicatorHandler(futu.IndicatorCalcHandlerBase):  # type: ignore[misc]
            def __init__(self) -> None:
                super().__init__()
                self._results: dict[str, Any] = {}
                self._event = threading.Event()

            def on_recv_rsp(self, rsp_pb: Any) -> Any:
                ret_code, content = super().on_recv_rsp(rsp_pb)
                if isinstance(content, Mapping) and content.get("calc_id"):
                    self._results[str(content["calc_id"])] = content
                self._event.set()
                return ret_code, content

            def wait(self, timeout: float) -> bool:
                return self._event.wait(timeout)

            def get(self, calc_id: str) -> Any:
                return self._results.get(calc_id)

        return _IndicatorHandler()

    @staticmethod
    def _pick_output(outputs: Any, output: str | int | None, short: str) -> int:
        """Resolve which output line of a multi-line indicator to read.

        Args:
            outputs: The push ``outputs`` metadata (list of ``{index, name}``).
            output: Requested line name or index; ``None`` picks the first.
            short: Indicator short name, used to match a line name.

        Returns:
            0-based index into each row's ``values`` list.

        Raises:
            ProviderHTTPError: A named/indexed output does not exist.
        """
        lines = [dict(o) for o in outputs] if isinstance(outputs, list) else []
        if isinstance(output, int):
            if 0 <= output < len(lines) or not lines:
                return output
            raise ProviderHTTPError(
                MoomooProvider.name,
                f"indicator output index {output} out of range",
                category="technical_indicators",
            )
        wanted = str(output or short).strip().upper()
        for i, line in enumerate(lines):
            if str(line.get("name", "")).strip().upper() == wanted:
                return i
        if output is not None and lines:
            raise ProviderHTTPError(
                MoomooProvider.name,
                f"unknown indicator output {output!r}",
                category="technical_indicators",
            )
        return 0

    def indicator_list(
        self, *, search_key: str | None = None, lang_type: int = 0, search_mode: int = 0, **_: Any
    ) -> list[dict[str, Any]]:
        """Catalog of available technical indicators (not a chain category).

        Args:
            search_key: Optional substring/exact name to filter by.
            lang_type: ``0``=any, ``1``=MyLang, ``2``=Python.
            search_mode: ``0``=partial (default), ``1``=exact.

        Returns:
            Rows of ``{short_name, full_name, language, inputs, outputs}`` — one
            per (indicator, language) pair, so a name may appear twice.
        """
        with self._context("technical_indicators") as (_futu, ctx):
            result = self._call(
                "technical_indicators",
                ctx.get_indicator_list,
                search_key=search_key,
                lang_type=lang_type,
                search_mode=search_mode,
            )
        out: list[dict[str, Any]] = []
        for entry in result[1] or []:
            if not isinstance(entry, Mapping):
                continue
            for language, info in entry.items():
                if not isinstance(info, Mapping):
                    continue
                out.append(
                    {
                        "short_name": info.get("short_name"),
                        "full_name": info.get("full_name"),
                        "language": language,
                        "inputs": _clean(info.get("inputs")),
                        "outputs": _clean(info.get("outputs")),
                    }
                )
        return out

    # -- capital_flow ------------------------------------------------------

    def capital_flow(
        self,
        *,
        symbol: str,
        period_type: str = "INTRADAY",
        start: str | None = None,
        end: str | None = None,
        kind: str = "flow",
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Capital inflow/outflow series, or today's distribution snapshot.

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.
            period_type: Intraday or daily aggregation (SDK default intraday).
            start: Optional start date for the flow range.
            end: Optional end date for the flow range.
            kind: ``flow`` (default) for ``get_capital_flow``, or
                ``distribution`` for the ``get_capital_distribution`` snapshot.
            limit: Keep only the last ``limit`` flow rows.

        Returns:
            ``flow``: rows of ``{time, in_flow, super_in_flow, big_in_flow,
            mid_in_flow, sml_in_flow, main_in_flow}``. ``distribution``: a
            single row of ``{time, capital_in_*, capital_out_*}``.

        Raises:
            ProviderHTTPError: Unknown ``kind``.
        """
        code = self._code(symbol, "capital_flow")
        with self._context("capital_flow") as (_futu, ctx):
            if kind == "distribution":
                result = self._call("capital_flow", ctx.get_capital_distribution, code)
                return [
                    {
                        "time": _first(row, ("update_time",)),
                        **_clean(row),
                    }
                    for row in _records(result[1])
                ]
            if kind != "flow":
                raise ProviderHTTPError(
                    self.name, f"unsupported capital_flow kind {kind!r}", category="capital_flow"
                )
            result = self._call(
                "capital_flow",
                ctx.get_capital_flow,
                code,
                period_type=period_type,
                start=start,
                end=end,
            )
        rows = [
            {
                "time": _first(row, ("capital_flow_item_time", "last_valid_time", "update_time")),
                "in_flow": _first(row, ("in_flow",)),
                "super_in_flow": _first(row, ("super_in_flow",)),
                "big_in_flow": _first(row, ("big_in_flow",)),
                "mid_in_flow": _first(row, ("mid_in_flow",)),
                "sml_in_flow": _first(row, ("sml_in_flow",)),
                "main_in_flow": _first(row, ("main_in_flow",)),
            }
            for row in _records(result[1])
        ]
        if limit and limit > 0:
            rows = rows[-int(limit):]
        return rows

    # -- market_breadth ----------------------------------------------------

    def market_breadth(
        self, *, market: str = "US", security: str | None = None, **_: Any
    ) -> dict[str, Any]:
        """Advance/decline distribution across a market or a plate.

        Args:
            market: Market, e.g. ``US`` (used when *security* is absent).
            security: Optional plate code, e.g. ``US.BK2024``.

        Returns:
            ``{market, rise, fall, equal, distribution: [{type, left_border,
            right_border, stock_count}, ...]}``. ``rise``/``fall``/``equal`` are
            summed from the ranges: positive ranges count as advancing, negative
            as declining, the zero range as unchanged.
        """
        with self._context("market_breadth") as (_futu, ctx):
            result = self._call(
                "market_breadth",
                ctx.get_rise_fall_distribution,
                security=security,
                market=market,
            )
        payload = result[1] if isinstance(result[1], Mapping) else {}
        ranges = payload.get("range_list") or []
        rise = fall = equal = 0
        for item in ranges:
            if not isinstance(item, Mapping):
                continue
            count = int(item.get("stock_count") or 0)
            kind = str(item.get("type") or "")
            left = item.get("left_border") or 0
            right = item.get("right_border") or 0
            if kind == "POSITIVE_INFINITY":
                rise += count
            elif kind == "NEGATIVE_INFINITY":
                fall += count
            elif left == 0 and right == 0:
                equal += count
            elif left >= 0:
                rise += count
            else:
                fall += count
        return {
            "market": payload.get("plate") or market,
            "rise": rise,
            "fall": fall,
            "equal": equal,
            "distribution": [_clean(dict(item)) for item in ranges if isinstance(item, Mapping)],
        }

    # -- market_movers -----------------------------------------------------

    def market_movers(
        self,
        *,
        market: str = "US",
        count: int = 10,
        direction: str = "gainers",
        kind: str = "top",
        period: str | None = None,
        offset: int = 0,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Movers board: top gainers/losers, period change, or a hot list.

        Args:
            market: Market, e.g. ``US``.
            count: Rows to request (SDK caps at 200).
            direction: ``gainers`` (default) or ``losers``.
            kind: ``top`` (default, in-session movers), ``period`` (interval
                change) or ``hot`` (discussion heat).
            period: For ``kind="period"``, a ``RankPeriodType`` token such as
                ``5MIN``/``5D``/``YTD``.
            offset: Paging offset.

        Returns:
            Rows of ``{symbol, name, price, change_percent, volume, ...}`` (extra
            columns such as ``turnover``/``market_cap``/``volume_ratio`` are kept).

        Raises:
            ProviderHTTPError: Unknown ``kind``.
        """
        with self._context("market_movers") as (futu, ctx):
            sort_dir = self._rank_dir(futu, direction)
            if kind == "hot":
                result = self._call(
                    "market_movers",
                    ctx.get_hot_list,
                    market,
                    sort_dir=sort_dir,
                    count=count,
                    offset=offset,
                )
            elif kind == "period":
                kwargs: dict[str, Any] = {"sort_dir": sort_dir, "count": count, "offset": offset}
                if period:
                    token = str(period).upper()
                    attr = _PERIOD_CHANGE.get(str(period).lower(), token)
                    member = getattr(futu.RankPeriodType, attr, None)
                    if member is None:
                        raise ProviderHTTPError(
                            self.name,
                            f"unknown period change interval {period!r}",
                            category="market_movers",
                        )
                    kwargs["period_type"] = member
                result = self._call("market_movers", ctx.get_period_change_rank, market, **kwargs)
            elif kind == "top":
                result = self._call(
                    "market_movers",
                    ctx.get_top_movers_rank,
                    market,
                    sort_dir=sort_dir,
                    count=count,
                    offset=offset,
                )
            else:
                raise ProviderHTTPError(
                    self.name, f"unsupported market_movers kind {kind!r}", category="market_movers"
                )
        out: list[dict[str, Any]] = []
        for row in self._table(result):
            out.append(
                {
                    "symbol": _first(row, ("security", "code")),
                    "name": _first(row, ("name",)),
                    "price": _first(row, ("cur_price", "last_price", "close_price")),
                    "change_percent": _first(row, ("change_ratio", "change_rate")),
                    "volume": _first(row, ("volume",)),
                    **{k: v for k, v in row.items() if k not in {"security", "code", "name"}},
                }
            )
        return out

    # -- options_data ------------------------------------------------------

    def options_data(
        self,
        *,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """Option expirations, and the chain within a ≤30-day window.

        On the configured account every options endpoint refuses with a US
        MarketOptions permission error, so this raises :class:`ProviderHTTPError`
        and the ``options_data`` chain falls over to yfinance.

        Args:
            symbol: Underlying, e.g. ``AAPL.US``.
            start: Chain window start, ``YYYY-MM-DD`` (expiry date).
            end: Chain window end, ``YYYY-MM-DD`` (expiry date).

        Returns:
            ``{symbol, expirations: [str], contracts: [dict]}``. ``contracts`` is
            empty unless a ``start``/``end`` window was supplied (the SDK caps a
            chain request at 30 days). Each contract carries the canonical
            ``contract_symbol``/``expiration`` keys (mapped from Futu's ``code``
            and ``strike_time``) alongside ``strike``/``type`` and the SDK's raw
            fields.

        Raises:
            ProviderHTTPError: The gateway refused (e.g. missing options
                permission).
        """
        code = self._code(symbol, "options_data")
        with self._context("options_data") as (_futu, ctx):
            expirations = [
                str(_first(row, ("strike_time",), ""))
                for row in _records(self._call("options_data", ctx.get_option_expiration_date, code)[1])
            ]
            contracts: list[dict[str, Any]] = []
            if start or end:
                result = self._call(
                    "options_data", ctx.get_option_chain, code, start=start, end=end
                )
                contracts = [
                    dict(
                        row,
                        contract_symbol=_first(row, ("code", "contract_symbol")),
                        expiration=_first(row, ("strike_time", "expiration")),
                        strike=_first(row, ("strike_price", "strike")),
                        type=_first(row, ("option_type", "type")),
                    )
                    for row in _records(result[1])
                ]
        return {"symbol": code, "expirations": expirations, "contracts": contracts}

    # -- expected_move -----------------------------------------------------

    def expected_move(self, *, symbol: str, **_: Any) -> dict[str, Any]:
        """Pre-earnings implied move and IV crush for *symbol*.

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.

        Returns:
            ``{symbol, expected_move, expected_move_ratio, iv, basis,
            strike_date_iv_crush, period}`` from the current fiscal period's row
            of ``get_financials_earnings_price_history``.

        Raises:
            ProviderHTTPError: The gateway refused the request.
        """
        code = self._code(symbol, "expected_move")
        with self._context("expected_move") as (_futu, ctx):
            result = self._call("expected_move", ctx.get_financials_earnings_price_history, code)
        rows = _records(result[1])
        current = [r for r in rows if str(_first(r, ("is_current",), "")).lower() in {"true", "1"}] or rows
        row = current[0] if current else {}
        return {
            "symbol": code,
            "expected_move": _first(row, ("predict_vola_val_newest",)),
            "expected_move_ratio": _first(row, ("predict_vola_ratio_newest",)),
            "iv": _first(row, ("option_iv_crush",)),
            "strike_date_iv_crush": _first(row, ("option_strike_date_iv_crush",)),
            "period": _first(row, ("period_text",)),
            "basis": "financials_earnings_price_history",
        }

    # -- prediction_markets ------------------------------------------------

    def prediction_markets(
        self,
        *,
        event_category: str | None = None,
        series_code: str | None = None,
        event_code: str | None = None,
        status: str | None = None,
        limit: int = _DEFAULT_EVENTS,
        **_: Any,
    ) -> dict[str, Any]:
        """Event contracts from the gateway, shaped as ``{events: [...]}``.

        The gateway exposes a three-level catalogue: a *category* holds
        *series*, a series holds *events*, and an event holds binary *contracts*.
        A plain call (no selector) lists the series catalogue and walks it until
        ``limit`` events are found, returning only the active ones. An event's
        contract share price **is** its implied probability (a $0.62 YES price
        means the market prices the outcome at 62%), so each contract carries
        ``implied_probability`` alongside its raw ``price``.

        Args:
            event_category: Optional category name (e.g. ``Sports``) whose series
                are searched. Named ``event_category`` because ``category`` is
                the failover chain's own request key.
            series_code: Optional ``EC.xxx.SERIES`` code to list events for.
            event_code: Optional ``EC.xxx.EVENT`` code; when set, the response is
                just that event.
            status: Event-status filter; a left-normalised ``ECStatus`` member
                name (e.g. ``EVENT_ACTIVE``, ``EVENT_CLOSED``). ``all`` disables
                the filter. The default is active events only.
            limit: Maximum events to return (1-50).

        Returns:
            ``{"events": [...]}`` plus ``count``; each event is ``{event_id,
            event_code, title, sub_title, series_code, category, tags, status,
            start_date, end_date, mutually_exclusive, market_count, markets}``
            and each market carries ``{contract_symbol, contract_code, title,
            status, contract_type, expiration, close_time, implied_probability,
            price, yes_bid, yes_ask, no_bid, no_ask, volume, open_interest,
            last_trade_time, result, settlement_value}``.

        Raises:
            ProviderHTTPError: For an unknown code, or any gateway refusal (a
                non-zero ``ret_code`` such as a permission error). Note the
                streaming endpoints (``get_event_contract_order_book`` /
                ``get_event_contract_ticker`` / ``get_event_contract_kline``)
                answer ``ret_code == -1`` "subscribe ... first" and so are not
                served here.
        """
        count = _clamp_count(limit, _DEFAULT_EVENTS, _MAX_EVENTS)
        with self._context("prediction_markets") as (futu, ctx):
            event_status = self._event_status(futu, status)
            rows = self._select_events(
                ctx, event_category, series_code, event_code, event_status, count
            )
            events = [self._event_record(ctx, row) for row in rows[:count]]
        return {"events": events, "count": len(events)}

    def _select_events(
        self,
        ctx: Any,
        category: str | None,
        series_code: str | None,
        event_code: str | None,
        status: Any,
        count: int,
    ) -> list[dict[str, Any]]:
        """Resolve the event rows for one selector (see :meth:`prediction_markets`)."""
        wanted_event = str(event_code).strip() if event_code else ""
        if wanted_event:
            series = self._event_series_code(ctx, wanted_event)
            if not series:
                return []
            rows = self._series_events(ctx, series, status)
            return [r for r in rows if str(_first(r, ("event_code",), "")) == wanted_event][:count]
        wanted_series = str(series_code).strip() if series_code else ""
        if wanted_series:
            return self._series_events(ctx, wanted_series, status)[:count]
        rows: list[dict[str, Any]] = []
        for series in self._series_codes(ctx, category):
            rows.extend(self._series_events(ctx, series, status))
            if len(rows) >= count:
                break
        return rows[:count]

    def _series_codes(self, ctx: Any, category: str | None) -> list[str]:
        """Return series codes for *category* (or the whole catalogue)."""
        wanted = str(category).strip() if category else ""
        if wanted:
            result = self._call(
                "prediction_markets", ctx.get_event_contract_series_list, wanted
            )
        else:
            result = self._call("prediction_markets", ctx.get_event_contract_series_list)
        return [
            str(_first(row, ("series_code",), ""))
            for row in _records(result[1])
            if _first(row, ("series_code",))
        ][:_MAX_SERIES_SCAN]

    def _series_events(self, ctx: Any, series_code: str, status: Any) -> list[dict[str, Any]]:
        """Return the event rows for one series code."""
        result = self._call(
            "prediction_markets", ctx.get_event_contract_event_list, series_code, status=status
        )
        return _records(result[1])

    def _event_series_code(self, ctx: Any, event_code: str) -> str:
        """Find the series code owning *event_code* via its contract list."""
        payload = self._call("prediction_markets", ctx.get_event_contract, event_code)[1]
        payload = payload if isinstance(payload, Mapping) else {}
        for row in _records(payload.get("contract_list")):
            series = _first(row, ("series_code",))
            if series:
                return str(series)
        return ""

    def _event_record(self, ctx: Any, row: Mapping[str, Any]) -> dict[str, Any]:
        """Expand one event row with its contracts and their quotes."""
        code = str(_first(row, ("event_code",), ""))
        markets: list[dict[str, Any]] = []
        if code:
            payload = self._call("prediction_markets", ctx.get_event_contract, code)[1]
            payload = payload if isinstance(payload, Mapping) else {}
            contracts = _records(payload.get("contract_list"))
            quotes: dict[str, dict[str, Any]] = {}
            codes = [str(c.get("contract_code")) for c in contracts if c.get("contract_code")]
            if codes:
                snapshots = self._call(
                    "prediction_markets", ctx.get_event_contract_snapshot, codes
                )[1]
                quotes = {str(q.get("code")): q for q in _records(snapshots)}
            markets = [
                self._market_record(contract, quotes.get(str(contract.get("contract_code"))))
                for contract in contracts
            ]
        return {
            "event_id": code or None,
            "event_code": code or None,
            "title": _first(row, ("event_name",)),
            "sub_title": _first(row, ("event_sub_name",)),
            "series_code": _first(row, ("series_code",)),
            "category": _first(row, ("category",)),
            "tags": row.get("tags"),
            "status": _first(row, ("status",)),
            "start_date": _first(row, ("start_date",)),
            "end_date": _first(row, ("end_date",)),
            "mutually_exclusive": row.get("mutually_exclusive"),
            "market_count": len(markets),
            "markets": markets,
        }

    @staticmethod
    def _market_record(
        contract: Mapping[str, Any], quote: Mapping[str, Any] | None
    ) -> dict[str, Any]:
        """Shape one binary contract plus its snapshot as a market record.

        The contract's share price **is** its implied probability, so ``price``
        from the snapshot is echoed as ``implied_probability``.
        """
        quote = quote or {}
        price = _first(quote, ("price",))
        return {
            "contract_symbol": contract.get("contract_code"),
            "contract_code": contract.get("contract_code"),
            "title": contract.get("title"),
            "status": _first(quote, ("status",)) or contract.get("status"),
            "contract_type": contract.get("contract_type"),
            "expiration": contract.get("latest_expiration_time") or contract.get("close_time"),
            "close_time": contract.get("close_time"),
            "implied_probability": price,
            "price": price,
            "yes_bid": quote.get("yes_bid"),
            "yes_ask": quote.get("yes_ask"),
            "no_bid": quote.get("no_bid"),
            "no_ask": quote.get("no_ask"),
            "volume": quote.get("cumulative_volume"),
            "open_interest": quote.get("open_interest"),
            "last_trade_time": quote.get("last_trade_time"),
            "result": contract.get("result"),
            "settlement_value": contract.get("settlement_value"),
        }

    @staticmethod
    def _event_status(futu: Any, status: str | None) -> Any:
        """Map a status token onto an ``ECStatus`` member (default: active)."""
        enum = getattr(futu, "ECStatus", None)
        if enum is None:
            return None
        token = str(status).strip().upper() if status is not None else ""
        if token == "ALL":
            return None
        if not token:
            token = "EVENT_ACTIVE"
        return getattr(enum, token, None) or getattr(enum, "EVENT_ACTIVE", None)

    # -- earnings_calendar -------------------------------------------------

    def earnings_calendar(
        self,
        *,
        market: str = "US",
        start: str | None = None,
        end: str | None = None,
        begin_date: str | None = None,
        end_date: str | None = None,
        count: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Earnings release dates and consensus for a market.

        The gateway refuses a window wider than seven days, so ``end`` is
        clamped to ``begin + 6d`` (an inclusive seven-day span) when the caller
        asks for more.

        Args:
            market: Market, e.g. ``US``.
            start: Alias for ``begin_date``.
            end: Alias for ``end_date``.
            begin_date: Window start, ``YYYY-MM-DD``.
            end_date: Window end, ``YYYY-MM-DD`` (clamped to begin+6d).
            count: Accepted for chain compatibility; the SDK endpoint has no
                page-size parameter and returns the whole ≤7-day window.

        Returns:
            Rows of ``{symbol, name, date, period, hour, eps_estimate,
            eps_actual, revenue_estimate, revenue_actual, iv, iv_rank,
            iv_percentile}``.
        """
        begin = begin_date or start
        finish = end_date or end
        if begin and finish:
            finish = self._clamp_window(begin, finish)
        with self._context("earnings_calendar") as (_futu, ctx):
            result = self._call(
                "earnings_calendar",
                ctx.get_earnings_calendar,
                market,
                begin_date=begin,
                end_date=finish,
            )
        return [
            {
                "symbol": _first(row, ("security",)),
                "name": _first(row, ("name",)),
                "date": _first(row, ("earnings_date",)),
                "period": _first(row, ("period_text",)),
                "hour": _first(row, ("pub_type",)),
                "eps_estimate": _first(row, ("eps_predict",)),
                "eps_actual": _first(row, ("eps_actual",)),
                "revenue_estimate": _first(row, ("revenue_predict",)),
                "revenue_actual": _first(row, ("revenue_actual",)),
                "iv": _first(row, ("iv",)),
                "iv_rank": _first(row, ("iv_rank",)),
                "iv_percentile": _first(row, ("iv_percentile",)),
            }
            for row in _records(result[1])
        ]

    @staticmethod
    def _clamp_window(begin: str, finish: str) -> str:
        """Return *finish* clamped so the span is at most seven days."""
        try:
            started = date.fromisoformat(begin)
            ended = date.fromisoformat(finish)
        except ValueError:
            return finish
        limit = started + timedelta(days=_MAX_CALENDAR_WINDOW_DAYS)
        return limit.isoformat() if ended > limit else finish

    # -- earnings_surprise -------------------------------------------------

    def earnings_surprise(
        self,
        *,
        market: str = "US",
        count: int | None = None,
        beat_type: str = "EPS",
        term: str | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Earnings-beat ranking for a market.

        Args:
            market: Market, e.g. ``US``.
            count: Rows to request (SDK caps at 300).
            beat_type: ``EPS`` (default), ``REVENUE`` or ``EBIT``.
            term: Optional fiscal-period filter.

        Returns:
            Rows of ``{symbol, name, period, eps_estimate, eps_actual,
            surprise_percent, released_date, change_rate, market_cap}``.
        """
        with self._context("earnings_surprise") as (futu, ctx):
            kind = getattr(futu.BeatType, str(beat_type).upper(), futu.BeatType.EPS)
            result = self._call(
                "earnings_surprise",
                ctx.get_earnings_beat_rank,
                market,
                kind,
                count=count,
                term=term,
            )
        return [
            {
                "symbol": _first(row, ("security",)),
                "name": _first(row, ("name",)),
                "period": _first(row, ("term",)),
                "eps_estimate": _first(row, ("estimate",)),
                "eps_actual": _first(row, ("actual",)),
                "surprise_percent": _first(row, ("beat_ratio",)),
                "released_date": _first(row, ("released_date",)),
                "change_rate": _first(row, ("change_rate",)),
                "market_cap": _first(row, ("market_cap",)),
            }
            for row in self._table(result)
        ]

    # -- earnings_catalyst -------------------------------------------------

    def earnings_catalyst(self, *, symbol: str, **_: Any) -> dict[str, Any]:
        """Per-day price behaviour around past earnings, plus unusual-flow notes.

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.

        Returns:
            ``{symbol, events: [{event, period, day_offset, trading_day, close,
            option_iv, option_hv}], unusual: {time_range, content}}``.
        """
        code = self._code(symbol, "earnings_catalyst")
        with self._context("earnings_catalyst") as (_futu, ctx):
            move = self._call("earnings_catalyst", ctx.get_financials_earnings_price_move, code)
            unusual = self._call("earnings_catalyst", ctx.get_financial_unusual, code)[1]
        events = [
            {
                "event": "earnings_price_move",
                "period": _first(row, ("period_text",)),
                "day_offset": _first(row, ("day_offset",)),
                "trading_day": _first(row, ("trading_day_str",)),
                "close": _first(row, ("close_price",)),
                "option_iv": _first(row, ("option_iv",)),
                "option_hv": _first(row, ("option_hv",)),
            }
            for row in _records(move[1])
        ]
        return {
            "symbol": code,
            "events": events,
            "unusual": _clean(unusual) if isinstance(unusual, Mapping) else {},
        }

    # -- economic_calendar -------------------------------------------------

    def economic_calendar(
        self,
        *,
        begin_date: str | None = None,
        start: str | None = None,
        end_date: str | None = None,
        end: str | None = None,
        count: int | None = None,
        importance: Any = None,
        market_list: list[str] | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Macro-economic events from *begin_date* onward.

        Args:
            begin_date: Window start, ``YYYY-MM-DD`` (required by the gateway).
            start: Alias for ``begin_date``.
            end_date: Window end, ``YYYY-MM-DD``.
            end: Alias for ``end_date``.
            count: Rows per page (SDK caps at 100).
            importance: Optional urgency filter (SDK ``EconomicImportance``).
            market_list: Optional markets filter, e.g. ``["US"]``.

        Returns:
            Rows of ``{title, timestamp, country, importance, previous,
            consensus, actual}``.

        Raises:
            ProviderHTTPError: The gateway refused the request.
        """
        # The gateway requires a start date, and ``economic_calendar`` is a
        # single-provider category — refusing a plain request would make the
        # category unusable without the caller knowing a gateway detail. "From
        # today" is the query a caller means when it names no window.
        begin = begin_date or start or date.today().isoformat()
        with self._context("economic_calendar") as (_futu, ctx):
            result = self._call(
                "economic_calendar",
                ctx.get_economic_calendar,
                begin,
                end_date=end_date or end,
                count=count,
                importance=importance,
                market_list=market_list,
            )
        return [
            {
                "title": _first(row, ("title",)),
                "timestamp": _first(row, ("timestamp",)),
                "country": _first(row, ("country",)),
                "importance": _first(row, ("star",)),
                "previous": _first(row, ("previous",)),
                "consensus": _first(row, ("consensus",)),
                "actual": _first(row, ("actual",)),
            }
            for row in _records(result[1])
        ]

    # -- fed_watch ---------------------------------------------------------

    def fed_watch(self, **_: Any) -> dict[str, Any]:
        """FedWatch target-rate probabilities and the dot plot.

        Returns:
            ``{target_rate: [{meeting_date, target_range, probability}],
            dot_plot: [{year, rate, vote_count, is_median, median_rate,
            current_rate}]}``.
        """
        with self._context("fed_watch") as (_futu, ctx):
            target = self._call("fed_watch", ctx.get_fed_watch_target_rate)
            dot = self._call("fed_watch", ctx.get_fed_watch_dot_plot)
        return {"target_rate": _records(target[1]), "dot_plot": _records(dot[1])}

    # -- macro_data --------------------------------------------------------

    def macro_data(
        self,
        *,
        indicator_id: Any = None,
        series_id: Any = None,
        time: str | None = None,
        start: str | None = None,
        max_count: int | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Historical values of one macro indicator.

        Args:
            indicator_id: Numeric indicator id from ``macro_indicator_list``.
            series_id: Alias for ``indicator_id``.
            time: ``YYYY-MM-DD`` anchor; values are pulled backwards from it.
            start: Alias for ``time``.
            max_count: Rows to pull (SDK caps at 1000).
            limit: Alias for ``max_count``.

        Returns:
            Rows of ``{date, value, release_time, predict_value,
            previous_value, unit_type}``.

        Raises:
            ProviderHTTPError: No numeric indicator id was supplied — Futu's
                history endpoint is keyed by its own ids, not FRED series ids,
                so a request without one cannot be served and must fail over.
        """
        raw_id = indicator_id if indicator_id is not None else series_id
        if raw_id is None:
            raise ProviderHTTPError(
                self.name,
                "macro_data requires indicator_id (use macro_indicator_list to discover ids)",
                category="macro_data",
            )
        try:
            numeric_id = int(raw_id)
        except (TypeError, ValueError) as exc:
            raise ProviderHTTPError(
                self.name,
                f"macro_data indicator_id {raw_id!r} is not a Futu indicator id",
                category="macro_data",
            ) from exc
        with self._context("macro_data") as (_futu, ctx):
            result = self._call(
                "macro_data",
                ctx.get_macro_indicator_history,
                numeric_id,
                time=time or start,
                max_count=max_count or limit,
            )
        return [
            {
                "date": _first(row, ("data_time",)),
                "value": _first(row, ("value",)),
                "release_time": _first(row, ("release_time",)),
                "predict_value": _first(row, ("predict_value",)),
                "previous_value": _first(row, ("previous_value",)),
                "unit_type": _first(row, ("unit_type",)),
            }
            for row in _records(result[1])
        ]

    def macro_indicator_list(self, *, region: str = "US", **_: Any) -> list[dict[str, Any]]:
        """Catalog of macro indicators for *region* (not a chain category).

        Args:
            region: Macro region, e.g. ``US``.

        Returns:
            Rows of ``{category, indicator_id, name}``.
        """
        with self._context("macro_data") as (_futu, ctx):
            result = self._call("macro_data", ctx.get_macro_indicator_list, region)
        return [
            {
                "category": _first(row, ("category_name",)),
                "indicator_id": _first(row, ("indicator_id",)),
                "name": _first(row, ("name",)),
            }
            for row in _records(result[1])
        ]

    # -- corporate_actions -------------------------------------------------

    def corporate_actions(
        self,
        *,
        symbol: str | None = None,
        kind: str = "dividends",
        market: str = "US",
        date: str | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Dividends, splits, buybacks, the dividend calendar, or rehab factors.

        Args:
            symbol: Ticker, required for every ``kind`` except ``calendar``.
            kind: ``dividends`` (default), ``splits``, ``buybacks``,
                ``calendar`` or ``rehab``.
            market: Market for ``kind="calendar"``.
            date: ``YYYY-MM-DD`` for ``kind="calendar"``.

        Returns:
            Rows tagged with ``type`` (equal to the requested ``kind``, in the
            plural request vocabulary: ``dividends``/``splits``/``buybacks``)
            plus a guaranteed ``date`` key and the endpoint's own keys.

        Raises:
            ProviderHTTPError: Missing ``symbol``/``date`` for the requested
                ``kind``, an unknown ``kind``, or a gateway refusal. Buybacks are
                HK/A-share-only upstream and so answer ``ret_code == -1`` for US.
        """
        with self._context("corporate_actions") as (_futu, ctx):
            if kind == "calendar":
                if not date:
                    raise ProviderHTTPError(
                        self.name, "corporate_actions calendar requires date", category="corporate_actions"
                    )
                result = self._call("corporate_actions", ctx.get_dividend_calendar, market, date)
                return [self._corporate_row(row, kind) for row in self._table(result)]
            if kind == "dividends":
                result = self._call(
                    "corporate_actions",
                    ctx.get_corporate_actions_dividends,
                    self._code(symbol, "corporate_actions"),
                )
                return [
                    self._corporate_row(row, kind)
                    for row in _records((result[1] or {}).get("dividend_list"))
                ]
            if kind == "splits":
                result = self._call(
                    "corporate_actions",
                    ctx.get_corporate_actions_stock_splits,
                    self._code(symbol, "corporate_actions"),
                )
                return [
                    self._corporate_row(row, kind)
                    for row in _records((result[1] or {}).get("split_list"))
                ]
            if kind == "buybacks":
                result = self._call(
                    "corporate_actions",
                    ctx.get_corporate_actions_buybacks,
                    self._code(symbol, "corporate_actions"),
                )
                return [self._corporate_row(row, kind) for row in _records(result[1])]
            if kind == "rehab":
                result = self._call(
                    "corporate_actions", ctx.get_rehab, self._code(symbol, "corporate_actions")
                )
                return [self._corporate_row(row, kind) for row in _records(result[1])]
            raise ProviderHTTPError(
                self.name, f"unsupported corporate_actions kind {kind!r}", category="corporate_actions"
            )

    @staticmethod
    def _corporate_row(row: Mapping[str, Any], kind: str) -> dict[str, Any]:
        """Tag one corporate-action row with the plural ``type`` and a ``date``."""
        return dict(row, type=kind, date=_first(row, _CORPORATE_DATE_KEYS))

    # -- short_interest ----------------------------------------------------

    def short_interest(
        self,
        *,
        symbol: str | None = None,
        market: str = "US",
        kind: str = "interest",
        count: int = 10,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Short interest, daily short volume, or a short-selling rank.

        Args:
            symbol: Ticker, required for ``interest`` and ``daily``.
            market: Market, used to pick the US/HK frame and for ``rank``.
            kind: ``interest`` (default), ``daily`` (daily short volume) or
                ``rank`` (short-selling movers board).
            count: Rows for ``kind="rank"`` (SDK caps at 35).

        Returns:
            Rows of ``{date, shares_short, short_percent, days_to_cover, ...}``
            (the rank variant carries ``symbol``/``name`` and its own columns).

        Raises:
            ProviderHTTPError: Missing ``symbol``, or an unknown ``kind``.
        """
        if kind == "rank":
            with self._context("short_interest") as (_futu, ctx):
                result = self._call(
                    "short_interest", ctx.get_short_selling_rank, market, count=count
                )
            return [
                {
                    "symbol": _first(row, ("security",)),
                    "name": _first(row, ("name",)),
                    "date": None,
                    "shares_short": _first(row, ("short_number",)),
                    "short_percent": _first(row, ("short_ratio",)),
                    "days_to_cover": _first(row, ("days_to_cover",)),
                    **{k: v for k, v in row.items() if k not in {"security", "name"}},
                }
                for row in self._table(result)
            ]
        code = self._code(symbol, "short_interest")
        with self._context("short_interest") as (_futu, ctx):
            if kind == "daily":
                result = self._call("short_interest", ctx.get_daily_short_volume, code)
                frame = self._market_frame(result, market)
                return [
                    {
                        "date": _first(row, ("timestamp_str", "timestamp")),
                        "shares_short": _first(row, ("total_shares_short", "shares_traded")),
                        "short_percent": _first(row, ("short_percent", "daily_trade_avg_ratio")),
                        "days_to_cover": None,
                        "volume": _first(row, ("volume",)),
                        "close_price": _first(row, ("close_price",)),
                    }
                    for row in _records(frame)
                ]
            if kind != "interest":
                raise ProviderHTTPError(
                    self.name, f"unsupported short_interest kind {kind!r}", category="short_interest"
                )
            result = self._call("short_interest", ctx.get_short_interest, code)
            # The gateway answers ``(ret, us_df, hk_df)`` in one call.
            frame = self._market_frame(result, market)
        return [
            {
                "date": _first(row, ("timestamp_str", "timestamp")),
                "shares_short": _first(row, ("shares_short",)),
                "short_percent": _first(row, ("short_percent",)),
                "days_to_cover": _first(row, ("days_to_cover",)),
                "avg_daily_share_volume": _first(row, ("avg_daily_share_volume",)),
                "close_price": _first(row, ("close_price",)),
            }
            for row in _records(frame)
        ]

    # -- institution_data --------------------------------------------------

    def institution_data(
        self,
        *,
        market: str = "US",
        kind: str = "institutions",
        institution_id: int | None = None,
        symbol: str | None = None,
        count: int | None = None,
        page: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """Institution catalog, holdings, changes, distribution, or shareholders.

        Args:
            market: Market, e.g. ``US``.
            kind: ``institutions`` (default), ``holdings``, ``changes``,
                ``distribution``, ``shareholders`` or ``institutional``.
            institution_id: Required for ``holdings``/``changes``/``distribution``.
            symbol: Required for ``shareholders``/``institutional``.
            count: Maximum rows.
            page: Paging cursor.

        Returns:
            ``{institutions: [...]}``, ``{holdings: [...]}``,
            ``{distribution: [...]}`` or ``{shareholders: {...}}``.

        Raises:
            ProviderHTTPError: A required id/symbol is missing, or an unknown kind.
        """
        with self._context("institution_data") as (_futu, ctx):
            if kind == "institutions":
                result = self._call(
                    "institution_data", ctx.get_institution_list, market, count=count, page=page
                )
                return {"institutions": _records(result[1])}
            if kind in {"holdings", "changes"}:
                if institution_id is None:
                    raise ProviderHTTPError(
                        self.name,
                        f"institution_data {kind} requires institution_id",
                        category="institution_data",
                    )
                method = (
                    ctx.get_institution_holding_list
                    if kind == "holdings"
                    else ctx.get_institution_holding_change
                )
                result = self._call(
                    "institution_data", method, market, institution_id, count=count, page=page
                )
                return {"holdings": _records(result[1])}
            if kind == "distribution":
                if institution_id is None:
                    raise ProviderHTTPError(
                        self.name,
                        "institution_data distribution requires institution_id",
                        category="institution_data",
                    )
                result = self._call(
                    "institution_data", ctx.get_institution_distribution, market, institution_id
                )
                return {"distribution": _records(result[1])}
            if kind in {"shareholders", "institutional"}:
                code = self._code(symbol, "institution_data")
                if kind == "shareholders":
                    payload = self._call("institution_data", ctx.get_shareholders_overview, code)[1]
                    if not isinstance(payload, Mapping):
                        return {"shareholders": {}}
                    return {
                        "shareholders": {
                            str(key): _records(value)
                            if hasattr(value, "columns") or isinstance(value, list)
                            else _clean(value)
                            for key, value in payload.items()
                        }
                    }
                result = self._call("institution_data", ctx.get_shareholders_institutional, code)
                return {"holdings": _records(result[1])}
            raise ProviderHTTPError(
                self.name, f"unsupported institution_data kind {kind!r}", category="institution_data"
            )

    # -- smart_money -------------------------------------------------------

    def smart_money(
        self,
        *,
        kind: str = "ark_holdings",
        symbol: str | None = None,
        count: int | None = None,
        page: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """ARK fund/active-transaction boards and insider trades/holders.

        Args:
            kind: ``ark_holdings`` (default), ``ark_active``,
                ``insider_trades`` or ``insider_holders``.
            symbol: Required for the insider kinds.
            count: Maximum rows.
            page: Paging cursor.

        Returns:
            ``{holdings: [...]}`` for holding-style kinds, ``{trades: [...]}``
            for trade-style kinds.

        Raises:
            ProviderHTTPError: A required symbol is missing, or an unknown kind.
        """
        with self._context("smart_money") as (_futu, ctx):
            if kind == "ark_holdings":
                result = self._call("smart_money", ctx.get_ark_fund_holding, count=count, page=page)
                return {"holdings": _records(result[1])}
            if kind == "ark_active":
                result = self._call(
                    "smart_money", ctx.get_ark_active_transaction, count=count, page=page
                )
                return {"trades": _records(result[1])}
            code = self._code(symbol, "smart_money")
            if kind == "insider_trades":
                result = self._call("smart_money", ctx.get_insider_trade_list, code)
                return {"trades": _records(result[1])}
            if kind == "insider_holders":
                result = self._call("smart_money", ctx.get_insider_holder_list, code)
                return {"holdings": _records(result[1])}
            raise ProviderHTTPError(
                self.name, f"unsupported smart_money kind {kind!r}", category="smart_money"
            )

    # -- analyst_ratings ---------------------------------------------------

    def analyst_ratings(self, *, symbol: str, **_: Any) -> dict[str, Any]:
        """Analyst consensus and per-institution rating summaries.

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.

        Returns:
            ``{symbol, consensus: {buy, hold, sell, rating, total, highest,
            average, lowest, update_time}, summary: str, ratings: [...]}``.
        """
        code = self._code(symbol, "analyst_ratings")
        with self._context("analyst_ratings") as (_futu, ctx):
            summary = self._call("analyst_ratings", ctx.get_research_rating_summary, code)[1]
            consensus = self._call("analyst_ratings", ctx.get_research_analyst_consensus, code)[1]
        consensus = consensus if isinstance(consensus, Mapping) else {}
        summary = summary if isinstance(summary, Mapping) else {}
        keys = ("buy", "hold", "sell", "rating", "total", "highest", "average", "lowest", "update_time")
        facts = {key: _clean(consensus.get(key)) for key in keys}
        text = (
            f"{facts.get('rating') or 'n/a'} from {facts.get('total') or 0} analysts: "
            f"buy {facts.get('buy')}, hold {facts.get('hold')}, sell {facts.get('sell')}"
        )
        return {
            "symbol": code,
            "consensus": facts,
            "summary": text,
            "ratings": _records(summary.get("inst_rating_summary_list")),
            "next_key": _clean(summary.get("next_key")),
        }

    # -- analyst_actions ---------------------------------------------------

    def analyst_actions(
        self,
        *,
        market: str = "US",
        count: int | None = None,
        change_type: Any = None,
        page: str | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Rating changes from institutions.

        Args:
            market: Market; the upstream supports US only.
            count: Rows to request (SDK caps at 20).
            change_type: Optional ``RatingChangeType`` filter (default up).
            page: Paging cursor.

        Returns:
            Rows of ``{symbol, date, action, rating, target_price, firm, name,
            last_rating, last_target_price}``.
        """
        with self._context("analyst_actions") as (_futu, ctx):
            result = self._call(
                "analyst_actions",
                ctx.get_rating_change,
                market,
                change_type=change_type,
                count=count,
                page=page,
            )
        return [
            {
                "symbol": _first(row, ("security",)),
                "date": _first(row, ("recommendation_date",)),
                "action": _first(row, ("change_type",)),
                "rating": _first(row, ("rating",)),
                "target_price": _first(row, ("target_price",)),
                "firm": _first(row, ("institution_name",)),
                "name": _first(row, ("name",)),
                "last_rating": _first(row, ("last_rating",)),
                "last_target_price": _first(row, ("last_target_price",)),
            }
            for row in _records(result[1])
        ]

    # -- fundamental_data --------------------------------------------------

    def fundamental_data(
        self, *, symbol: str, statement_type: Any = None, **_: Any
    ) -> dict[str, Any]:
        """Company profile, financial statements, efficiency and valuation.

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.
            statement_type: Optional statement selector (SDK ``StatementType``).

        Returns:
            ``{symbol, profile: {name: value}, periods: [dict], statements:
            {report_list: [...], structure_list: [...]},
            operational_efficiency: {...}, valuation: {...}}``. ``periods`` holds
            the period rows themselves (the same records as
            ``statements["report_list"]``), matching the ``list[dict]`` shape the
            other ``fundamental_data`` providers emit.
        """
        code = self._code(symbol, "fundamental_data")
        with self._context("fundamental_data") as (_futu, ctx):
            profile = self._call("fundamental_data", ctx.get_company_profile, code)[1]
            statements = self._call(
                "fundamental_data", ctx.get_financials_statements, code, statement_type=statement_type
            )[1]
            efficiency = self._call("fundamental_data", ctx.get_company_operational_efficiency, code)[1]
            valuation = self._call("fundamental_data", ctx.get_valuation_detail, code)[1]
        statements = statements if isinstance(statements, Mapping) else {}
        reports = statements.get("report_list") or []
        period_rows = _records(reports)
        return {
            "symbol": code,
            "profile": {
                str(_first(row, ("name",), "")): _first(row, ("value",))
                for row in _records(profile)
            },
            "periods": period_rows,
            "statements": {
                "report_list": period_rows,
                "structure_list": _records(statements.get("structure_list")),
            },
            "operational_efficiency": _clean(efficiency) if isinstance(efficiency, Mapping) else {},
            "valuation": _clean(valuation) if isinstance(valuation, Mapping) else {},
        }

    # -- revenue_breakdown -------------------------------------------------

    def revenue_breakdown(
        self,
        *,
        symbol: str,
        date: str | None = None,
        financial_type: Any = None,
        currency_code: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """Revenue split by business/geography for a reporting period.

        Args:
            symbol: Ticker, e.g. ``AAPL.US``.
            date: Optional report date selector.
            financial_type: Optional ``FinancialType`` selector.
            currency_code: Optional currency override.

        Returns:
            ``{period, currency, items: [...], screen_date_list: [...]}``.
        """
        code = self._code(symbol, "revenue_breakdown")
        with self._context("revenue_breakdown") as (_futu, ctx):
            payload = self._call(
                "revenue_breakdown",
                ctx.get_financials_revenue_breakdown,
                code,
                date=date,
                financial_type=financial_type,
                currency_code=currency_code,
            )[1]
        payload = payload if isinstance(payload, Mapping) else {}
        return {
            "period": _clean(payload.get("period")),
            "currency": _clean(payload.get("currency_code")),
            "items": _records(payload.get("breakdown_list")),
            "screen_date_list": _records(payload.get("screen_date_list")),
        }

    # -- exchange_symbols --------------------------------------------------

    def exchange_symbols(
        self, *, exchange: str = "US", stock_type: str = "STOCK", **_: Any
    ) -> list[dict[str, Any]]:
        """Every listed security in *exchange* (e.g. 13,133 US stocks).

        Args:
            exchange: Market code, e.g. ``US``.
            stock_type: Security type, e.g. ``STOCK``/``ETF``/``WARRANT``.

        Returns:
            Rows of ``{symbol, name, exchange, type, listing_date, lot_size,
            stock_id}``.
        """
        with self._context("exchange_symbols") as (_futu, ctx):
            result = self._call("exchange_symbols", ctx.get_stock_basicinfo, exchange, stock_type)
        return [
            {
                "symbol": _first(row, ("code",)),
                "name": _first(row, ("name",)),
                "exchange": _first(row, ("exchange_type",)),
                "type": _first(row, ("stock_type",)),
                "listing_date": _first(row, ("listing_date",)),
                "lot_size": _first(row, ("lot_size",)),
                "stock_id": _first(row, ("stock_id",)),
            }
            for row in _records(result[1])
        ]

    # -- equity_screener ---------------------------------------------------

    def equity_screener(
        self,
        *,
        market: str = "US",
        filter_list: Any = None,
        begin: int = 0,
        num: int = 200,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Run a field filter over a market via ``get_stock_filter``.

        The SDK rejects a bare ``StockField`` list ("the item of filter_list is
        wrong"): each filter must be a ``SimpleFilter``/``AccumulateFilter``/
        ``FinancialFilter``/``PatternFilter``/``CustomIndicatorFilter`` instance
        whose ``stock_field`` is a ``StockField`` member. This adapter builds
        ``SimpleFilter`` objects from declarative dicts so the caller layer never
        imports futu:

            {"field": "CUR_PRICE", "min": 100, "max": 200, "sort": "DESCEND"}

        Callers may instead pass ready-made filter objects, which are forwarded
        unchanged.

        Args:
            market: Market, e.g. ``US``.
            filter_list: A filter dict, a list of filter dicts, or a list of
                native filter objects.
            begin: Paging offset.
            num: Rows per page.

        Returns:
            Rows of ``{symbol, name, price, change_percent, volume, ...}``; extra
            keys are whichever fields the filter selected.
        """
        with self._context("equity_screener") as (_futu, ctx):
            filters = self._build_filters(filter_list)
            result = self._call(
                "equity_screener", ctx.get_stock_filter, market, filter_list=filters, begin=begin, num=num
            )
        # get_stock_filter returns (ret, (last_page, all_count, data_list)).
        data = result[1]
        items = data[2] if isinstance(data, tuple) and len(data) == 3 else data
        out: list[dict[str, Any]] = []
        for item in items or []:
            row = {
                "symbol": getattr(item, "stock_code", None),
                "name": getattr(item, "stock_name", None),
                "price": getattr(item, "cur_price", None),
                "change_percent": getattr(item, "change_rate", None),
                "volume": getattr(item, "volume", None),
            }
            for attr in dir(item):
                if attr.startswith("_") or attr in {"stock_code", "stock_name"}:
                    continue
                value = getattr(item, attr)
                if callable(value):
                    continue
                row.setdefault(attr, _clean(value))
            out.append(row)
        return out

    @staticmethod
    def _build_filters(spec: Any) -> Any:
        """Turn declarative filter dicts into ``SimpleFilter`` objects.

        Args:
            spec: ``None``, a single dict/object, or a list of either.

        Returns:
            A list of native filter objects, or ``[]``.

        Raises:
            ProviderHTTPError: An unknown field name was requested.
        """
        if spec is None:
            return []
        items = spec if isinstance(spec, list) else [spec]
        futu = importlib.import_module("futu")
        out: list[Any] = []
        for item in items:
            if isinstance(item, Mapping):
                field_name = str(item.get("field") or "").upper()
                field = getattr(futu.StockField, field_name, None)
                if field is None:
                    raise ProviderHTTPError(
                        MoomooProvider.name,
                        f"unknown StockField {field_name!r}",
                        category="equity_screener",
                    )
                flt = futu.SimpleFilter()
                flt.stock_field = field
                flt.filter_min = item.get("min")
                flt.filter_max = item.get("max")
                sort = item.get("sort")
                if sort:
                    flt.sort = getattr(futu.SortDir, str(sort).upper(), None)
                flt.is_no_filter = item.get("min") is None and item.get("max") is None
                out.append(flt)
            else:
                out.append(item)
        return out
