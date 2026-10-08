---
name: data-routing
category: data-source
description: The single ROUTER for every data need. Load this skill BEFORE any backtest, data-fetch, or research task to pick the best available source/tool, honour auth (env) requirements, and avoid ban-risk providers.
---
# Data Routing (Router)

This is the one router. It maps (a) every registered backtest data **source** to its
markets / auth / skill, and (b) every research **data need** to the concrete **tool**
that serves it, its market, and the env key it requires. Source names below are a
strict subset of `backtest.loaders.registry.VALID_SOURCES` (enforced by
`tests/test_data_routing_sources_subset.py`).

## Source Overview

Every name here is a registered OHLCV/backtest source in `VALID_SOURCES`.
"Runner-internal" sources are selected by the backtest runner, not authored as a
per-source skill.

| Source | Markets | Auth (env key) | Network | Skill |
|--------|---------|----------------|---------|-------|
| tushare | A-shares, funds, futures, macro | Yes (`TUSHARE_TOKEN`) | China network | tushare |
| akshare | A-shares, US, HK, futures, macro, forex | No | Unrestricted | akshare |
| yfinance | US, HK, Canada (TSX/TSXV) stocks, ETFs | No | Needs Yahoo access | yfinance |
| okx | Crypto (OKX exchange) | No | Needs okx.com access | okx-market |
| nobitex | Crypto, IRT/Toman-quoted pairs (BTC-IRT, USDT-IRT — Iran market premium) | No | Needs apiv2.nobitex.ir access | data-routing (explicit `*IRT` routing only) |
| wallex | Crypto, TMN/Toman-quoted pairs (USDT-TMN, BTC-TMN — Iran market premium) | No | Needs api.wallex.ir access | data-routing (explicit `*TMN` routing only; truly serves 1m/1h/1d) |
| ccxt | Crypto (100+ exchanges) | No | Needs exchange access | ccxt |
| baostock | A-shares (free daily/min) | No | China network | data-routing |
| tencent | A-shares, HK, US (never-banned) | No | Unrestricted | data-routing |
| mootdx | A-shares (TDX servers, never-banned) | No | China network | data-routing |
| futu | A/HK/US via OpenD gateway | Yes (OpenD running) | Local gateway | data-routing (runner-internal) |
| mt5 | Forex & metals (your broker's MT5 feed) | Yes (running, logged-in MT5 terminal; optional `~/.vibe-trading/mt5.json`) | Local terminal (Windows) | data-routing (runner-internal) |
| tickerall | Forex & metals (same broker MT5 feed, hosted) | Yes (`TICKERALL_API_KEY` + `TICKERALL_ACCOUNT_ID`; read-only) | Hosted API (any OS, no terminal) | data-routing (runner-internal; **explicit `source=tickerall` only**) |
| local | User CSV/parquet on disk | No | Offline | data-routing (runner-internal) |
| eastmoney | A-shares, HK, US equities | No (IP-throttled) | Unrestricted | data-routing |
| sina | US equities (daily OHLCV) | No (IP-throttled) | Unrestricted | data-routing |
| stooq | US equities (daily OHLCV) | No | Unrestricted | data-routing |
| yahoo | US, HK, Canada (TSX/TSXV) equities | No (IP-throttled) | Needs Yahoo access | data-routing |
| finnhub | US equities | Yes (`FINNHUB_API_KEY`) | Unrestricted | data-routing |
| alphavantage | US equities | Yes (`ALPHAVANTAGE_API_KEY`) | Unrestricted | data-routing |
| tiingo | US equities | Yes (`TIINGO_API_KEY`) | Unrestricted | data-routing |
| fmp | US equities | Yes (`FMP_API_KEY`) | Unrestricted | data-routing |
| gildata | A-shares (Hundsun Juyuan commercial feed, forward-adjusted dailies) | Yes (`GILDATA_TOKEN` / Settings) | Gildata MCP API | data-routing |
| qveris | Global multi-asset (paid, credits) | Yes (`QVERIS_API_KEY` / Settings) | QVeris API | qveris <!-- QVERIS-INTEGRATION --> |

## Capability → Tool Routing

Pick the tool by the data need. "Market" is the universe the tool covers; "Env key"
is required only where listed (no key listed = free / no auth).

| Data need | Tool | Market | Env key |
|-----------|------|--------|---------|
| OHLCV price bars | `get_market_data` | A-share / US / HK / Canada / crypto / futures / forex | per-source (see Source Overview) |
| Fund flow (资金流向) | `get_fund_flow` | A-share, HK, US | — |
| Dragon-tiger (龙虎榜) | `get_dragon_tiger` | A-share | — |
| Northbound flow (北向资金) | `get_northbound_flow` | A-share | — |
| Margin trading (融资融券) | `get_margin_trading` | A-share | — |
| Block trades (大宗交易) | `get_block_trades` | A-share | — |
| Shareholder count (股东户数) | `get_shareholder_count` | A-share | — |
| Lockup expiry (限售解禁) | `get_lockup_expiry` | A-share | — |
| Sector / board taxonomy (板块) | `get_sector_info` | A-share | — |
| Sell-side research reports | `get_research_reports` | A-share | — |
| Stock news | `get_stock_news` | A-share, US, HK | — |
| SEC filings (EDGAR) | `get_sec_filings` | US | — |
| Financial statements | `get_financial_statements` | A-share, US, HK | — |
| Options chain | `get_options_chain` | US | — |
| Stock profile / fundamentals | `get_stock_profile` | US | — |
| Market screen | `screen_market` | A-share | — |
| Symbol search | `search_symbol` | A-share, US, HK, Canada, crypto/index/FX | — |
| Macro / FRED series | `get_macro_series` | Macro (US/global) | `FRED_API_KEY` |
| iWenCai NL search (问财) | `iwencai_search` | A-share | `VIBE_TRADING_IWENCAI_KEY` |

Notes:
- `get_financial_statements` reads US statements from SEC EDGAR companyfacts
  (ticker -> CIK -> XBRL concepts) and A-share/HK statements from the Eastmoney
  datacenter report API (per-market F10 report names).
- `get_stock_news` routes A-share (SH/SZ/BJ) to an Eastmoney news client and
  US (.US) / HK (.HK) to a Yahoo search client; a failure on one upstream is
  returned as an error envelope, never raised, so a single bad symbol never
  aborts a batch.

## Category Provider Chains (`get_category_data`)

A second, coarser routing axis for data several vendors carry (news, fundamentals,
calendars, options, macro, flows). `get_category_data` asks the first provider in
the category's chain that can serve it and returns that provider's answer.

The failover rule, which is the whole point of the layer: a provider that refuses
— HTTP 404/403/429/5xx, an in-band error body (Alpha Vantage answers **200** with
`Information` for a premium gate), or a local OpenD permission error — is skipped
for that request and the **next** provider is tried. Nothing is retried; the same
provider is never asked twice within a request, and two providers' answers are
never blended. `explain=true` lists the chain and every provider's state
(`ready` / `unconfigured` / `unimplemented`) without spending a request, and the
envelope of a normal call carries the full attempt trace.

The operator's priority order per category (a fenced block on purpose: these are
category names, not OHLCV source names, and the Source Overview guard test reads
bare first-column identifiers in tables as source names):

```text
core_stock_apis      eodhd > moomoo > yfinance > tiingo > twelve_data > stockdata > benzinga
news_data            eodhd > benzinga > moomoo > yfinance > alpha_vantage > stockdata > newsapi
fundamental_data     moomoo > yfinance > tiingo > alpha_vantage
analyst_ratings      moomoo > finnhub > yfinance > benzinga
earnings_calendar    moomoo > finnhub > yfinance > benzinga
technical_indicators moomoo > yfinance > alpha_vantage
options_data         moomoo > yfinance
short_interest       moomoo > yfinance
institution_data     moomoo > yfinance
macro_data           fred > moomoo
prediction_markets   polymarket > moomoo
corporate_actions    eodhd > moomoo > benzinga
news_sentiment       eodhd > alpha_vantage > gdelt
sec_filings          sec_edgar
risk_free_curve      federal_reserve
options_surface      cboe
exchange_symbols     eodhd
equity_screener      yfinance
market_movers        yfinance
capital_flow         moomoo
earnings_catalyst    moomoo
earnings_surprise    moomoo
economic_calendar    moomoo
expected_move        moomoo
fed_watch            moomoo
market_breadth       moomoo
revenue_breakdown    moomoo
smart_money          moomoo
analyst_actions      benzinga
fda_calendar         benzinga
guidance_revisions   benzinga
news_retractions     benzinga
offerings_calendar   benzinga
```

Notes:
- The list above is the operator's stated priority order, **not** a measured
  quality ranking, and it is not env-overridable. (The backtest OHLCV chains are the other
  axis and *are* reorderable via `MARKET_DATA_ORDER_<MARKET>`; that knob does not
  touch these chains.)
- `src/data_providers/categories.py` is the authority. Providers measured to
  serve a category but not named in this priority order are appended **after**
  it (`CATEGORY_CHAIN_EXTENSIONS`), so the stated order is never reshuffled —
  call `get_category_data(explain=true)` for a category's effective chain and
  each provider's current state (`ready` / `unconfigured` / `unimplemented`).
- Not every provider in a chain implements every category it appears under; an
  unimplemented slot is reported as `unimplemented` and skipped, never served as
  an empty result.
- A provider named in a chain with no adapter, or an adapter with no
  implementation for that category, is reported as `unimplemented` — never as an
  empty result.
- Provider credentials and per-provider request spacing are documented in
  `agent/.env.example` ("Category-based provider failover").

## Decision Tree

### Backtest scenario (writing config.json)

Use `source: "auto"` — the runner routes by symbol pattern and falls back across
same-market sources automatically. Only set a concrete source when the user asks.

### Analysis / research scenario

1. Identify the data need, then read the Capability table for the tool + env key.
2. If the need is plain OHLCV, call `get_market_data` and let source fallback run.
3. Set any required env key before calling a key-gated tool; if it is missing,
   report the missing key rather than failing silently.

### Source priority (for OHLCV by market)

- **A-shares**: tencent / mootdx (never banned) > tushare (`TUSHARE_TOKEN`) >
  baostock / akshare > eastmoney (throttled).
- **US stocks**: stooq / yahoo > tiingo / finnhub / fmp / alphavantage (key-gated) >
  sina / eastmoney (throttled) > yfinance.
- **HK stocks**: tencent (never banned, daily) > eastmoney / yahoo > futu (local
  OpenD) > akshare (Eastmoney-backed, daily only) > yfinance > tushare
  (`TUSHARE_TOKEN`) / longbridge (key-gated).
- **Canada (TSX/TSXV)**: yahoo > yfinance > local; use Yahoo's canonical
  `.TO` / `.V` suffixes (for example `TD.TO`, `PNG.V`).
- **Crypto**: okx (single exchange) > ccxt (multi-exchange).
- **Futures / macro**: tushare > akshare.
- **Forex / metals**: mt5 (local MetaTrader 5 terminal, Windows) > akshare. TickerAll (`source="tickerall"`) is a hosted, no-terminal alternative to the *same* broker feed (any OS) — opt-in and **explicit-only**: it never joins this automatic chain, so a user's broker credential is used only when deliberately requested.

## Symbol Format Reference

| Market | Format | Examples |
|--------|--------|----------|
| A-shares | `NNNNNN.SZ/SH/BJ` | 000001.SZ, 600000.SH, 430139.BJ |
| US stocks | `TICKER.US` | AAPL.US, MSFT.US |
| HK stocks | `NNNNN.HK` | 00700.HK, 09988.HK |
| Canada TSX / TSXV | `TICKER.TO` / `TICKER.V` | TD.TO, BBD-B.TO, PNG.V |
| Crypto | `SYMBOL-USDT` | BTC-USDT, ETH-USDT |
| Futures | `XXNNNN.EXCHANGE` | CU2406.SHFE |
| Forex | `XXX/YYY` | USD/CNY, EUR/USD |

## Ban-Risk & Fallback Notes

- **Prefer never-banned sources**: `tencent` and `mootdx` have no observed IP ban;
  reach for them first for A-share OHLCV when no token is set.
- **Eastmoney rate-limits by IP and must be throttled.** Every Eastmoney-backed
  tool/loader routes through the shared per-host throttle; do not hammer it. On a
  throttle/timeout, fall back to the same-market source above (tencent/baostock).
- **Sina / Yahoo also throttle by IP** — same per-host wrapper, same fallback rule.
- **Key-gated sources need their env key** (`FINNHUB_API_KEY`,
  `ALPHAVANTAGE_API_KEY`, `TIINGO_API_KEY`, `FMP_API_KEY`, `FRED_API_KEY`,
  `VIBE_TRADING_IWENCAI_KEY`, `TUSHARE_TOKEN`). If the key is absent the tool/loader
  is unavailable — route to a free same-market source instead of erroring out.
- A single failing symbol or transient HTTP error is reported inside the envelope;
  it never aborts the surrounding batch.

## Data Verification Discipline

When a number will drive a conclusion (valuation, screening, report), do not trust a
single source. Cross-check it:

- **Verify material figures across ≥2 independent sources** before citing them.
  Prioritize original disclosures (company annual/quarterly reports, exchange filings)
  over third-party aggregators.
- **Flag any deviation >1%** between sources as a ⚠️ caliber mismatch — usually a
  definition difference (GAAP vs Non-GAAP, consolidated vs parent-only, currency,
  TTM vs annual). Do not silently pick one; state both and which you adopt.
- **Use the `financial_rigor` tool's `cross_validate` command** to do this exactly:
  pass `{source: value, ...}` and it returns the median consensus + per-source
  deviation + an `all_consistent` flag at a configurable tolerance (default 2%).
- **Mark unverified numbers** as "single-source" or "estimate" — never present an
  uncorroborated figure as established fact.

This discipline is what separates analysis from aggregation.
