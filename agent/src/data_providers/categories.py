"""Category vocabulary and the single-chain failover order.

One *category* is one kind of data that several providers can serve (daily
bars, news, an earnings calendar, an options surface...). The chain is the
single authority on who is asked, and in what order.

Ordering here is the operator's stated preference, not a measured quality
ranking; the resolver never re-orders it and never retries a provider within a
request. When the head of the chain refuses — HTTP 404/403/429, an in-band
error envelope, or a provider-native permission error — the next provider is
tried, exactly once, and so on to the end.

Categories whose chain has a single provider have no failover by design: the
data has exactly one source in this project, and a failure is reported rather
than papered over with a different caliber.
"""

from __future__ import annotations

#: Every category the project routes by. Order is documentation order only.
CATEGORIES: tuple[str, ...] = (
    "core_stock_apis",
    "news_data",
    "fundamental_data",
    "analyst_ratings",
    "earnings_calendar",
    "technical_indicators",
    "options_data",
    "short_interest",
    "institution_data",
    "macro_data",
    "prediction_markets",
    "corporate_actions",
    "news_sentiment",
    "sec_filings",
    "risk_free_curve",
    "options_surface",
    "exchange_symbols",
    "equity_screener",
    "market_movers",
    "capital_flow",
    "earnings_catalyst",
    "earnings_surprise",
    "economic_calendar",
    "expected_move",
    "fed_watch",
    "market_breadth",
    "revenue_breakdown",
    "smart_money",
    "analyst_actions",
    "fda_calendar",
    "guidance_revisions",
    "news_retractions",
    "offerings_calendar",
)

#: category -> ordered provider names. First entry is tried first.
CATEGORY_CHAINS: dict[str, tuple[str, ...]] = {
    "core_stock_apis": (
        "eodhd", "moomoo", "yfinance", "tiingo", "twelve_data", "stockdata", "benzinga",
    ),
    "news_data": (
        "eodhd", "benzinga", "moomoo", "yfinance", "alpha_vantage", "stockdata", "newsapi",
    ),
    "fundamental_data": ("moomoo", "yfinance", "tiingo", "alpha_vantage"),
    "analyst_ratings": ("moomoo", "finnhub", "yfinance", "benzinga"),
    "earnings_calendar": ("moomoo", "finnhub", "yfinance", "benzinga"),
    "technical_indicators": ("moomoo", "yfinance", "alpha_vantage"),
    "options_data": ("moomoo", "yfinance"),
    "short_interest": ("moomoo", "yfinance"),
    "institution_data": ("moomoo", "yfinance"),
    "macro_data": ("fred", "moomoo"),
    "prediction_markets": ("polymarket", "moomoo"),
    "corporate_actions": ("eodhd", "moomoo", "benzinga"),
    "news_sentiment": ("eodhd", "alpha_vantage", "gdelt"),
    # Single-vendor categories: one source, no failover.
    "sec_filings": ("sec_edgar",),
    "risk_free_curve": ("federal_reserve",),
    "options_surface": ("cboe",),
    "exchange_symbols": ("eodhd",),
    "equity_screener": ("yfinance",),
    "market_movers": ("yfinance",),
    "capital_flow": ("moomoo",),
    "earnings_catalyst": ("moomoo",),
    "earnings_surprise": ("moomoo",),
    "economic_calendar": ("moomoo",),
    "expected_move": ("moomoo",),
    "fed_watch": ("moomoo",),
    "market_breadth": ("moomoo",),
    "revenue_breakdown": ("moomoo",),
    "smart_money": ("moomoo",),
    "analyst_actions": ("benzinga",),
    "fda_calendar": ("benzinga",),
    "guidance_revisions": ("benzinga",),
    "news_retractions": ("benzinga",),
    "offerings_calendar": ("benzinga",),
}


#: Extra fallbacks appended **after** the operator's stated order, one key per
#: category. An entry here is a provider measured to serve that category on the
#: configured credentials but not named in the operator's priority list; it is
#: only ever added at the tail, so the stated preference is never reshuffled.
#: ``tests/test_data_provider_failover.py`` asserts every entry both has an
#: adapter and declares the capability, so this table cannot drift into a
#: fallback that never actually runs.
CATEGORY_CHAIN_EXTENSIONS: dict[str, tuple[str, ...]] = {
    # Each entry was measured returning data on the configured credentials. A
    # capability that could only 403 on this machine is deliberately absent
    # (Massive's fundamental_data and market_movers are tier-gated here), so an
    # extension never costs a guaranteed wasted request.
    "core_stock_apis": ("massive", "alpha_vantage"),
    # GDELT goes last on purpose: it is measured usable but slow (18-25 s per
    # call) and its per-IP limiter answers 429 to two requests inside 5 s, so it
    # is a last resort rather than a peer of the fast news providers.
    "news_data": ("massive", "finnhub", "gdelt"),
    "exchange_symbols": (
        "moomoo", "massive", "twelve_data", "stockdata", "finnhub", "alpha_vantage",
    ),
    "corporate_actions": ("twelve_data", "yfinance", "massive"),
    # Treasury publishes the curve; FRED's tenor series (DGS3MO/DGS10/...) and
    # Alpha Vantage's TREASURY_YIELD are measured second/third sources.
    "risk_free_curve": ("fred", "alpha_vantage"),
    # CBOE publishes the chain keyless; Massive serves contract metadata.
    "options_data": ("cboe", "massive"),
    "short_interest": ("massive",),
    "technical_indicators": ("twelve_data",),
    # finnhub is deliberately absent from fundamental_data: it serves a flat
    # metrics/profile bundle ({"metrics": {...}, "profile": {...}}), not the
    # {periods, statements} series shape the category's other providers share,
    # and a chain that changes shape by which provider answered is worse than a
    # shorter chain.
    "fundamental_data": ("sec_edgar", "twelve_data"),
    "earnings_calendar": ("twelve_data",),
    # moomoo owns the category; Benzinga's economics calendar is a measured
    # second source (HTTP 200 with an ``economics`` array).
    "economic_calendar": ("benzinga",),
    "market_movers": ("moomoo",),
    "equity_screener": ("moomoo",),
    "analyst_actions": ("moomoo",),
    "smart_money": ("yfinance", "finnhub"),
    "macro_data": ("alpha_vantage",),
}


def chain_for(category: str) -> tuple[str, ...]:
    """Return the ordered provider chain for *category*.

    The operator's stated order first, then any measured extra fallbacks from
    :data:`CATEGORY_CHAIN_EXTENSIONS`, in that order.

    Args:
        category: One of :data:`CATEGORIES`.

    Returns:
        The ordered chain, or an empty tuple for an unknown category.
    """
    base = CATEGORY_CHAINS.get(category, ())
    if not base:
        return ()
    return base + CATEGORY_CHAIN_EXTENSIONS.get(category, ())


def known_categories() -> tuple[str, ...]:
    """Return every category name in the chain table (sorted)."""
    return tuple(sorted(CATEGORY_CHAINS))


def providers_in_chains() -> tuple[str, ...]:
    """Return every provider named by at least one chain, extensions included."""
    names = {p for chain in CATEGORY_CHAINS.values() for p in chain}
    names |= {p for chain in CATEGORY_CHAIN_EXTENSIONS.values() for p in chain}
    return tuple(sorted(names))


def categories_for_provider(provider: str) -> tuple[str, ...]:
    """Return every category whose (extended) chain contains *provider*."""
    names = set(CATEGORY_CHAINS) | set(CATEGORY_CHAIN_EXTENSIONS)
    return tuple(sorted(c for c in names if provider in chain_for(c)))
