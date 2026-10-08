"""Normalised payload shapes per category.

The resolver returns the first provider that answers, so two providers serving
the same category must produce the same *shape* or a caller silently gets a
different structure when the head of the chain is down. Each adapter therefore
converts its native response into the shape described here. Field names are the
project's own conventions (``trade_date``/``open``/``high``/``low``/``close``/
``volume`` for bars, ISO-8601 strings for dates) so the payload can flow into
existing tooling without a second translation layer.

Shapes are deliberately shallow: a list of flat records, or a small dict. Any
provider-specific extra is kept under its own key rather than being promoted,
so a caller can always tell the base shape from the extension.
"""

from __future__ import annotations

#: category -> human description of the normalised payload.
CATEGORY_SHAPES: dict[str, str] = {
    "core_stock_apis": "list[dict]: {trade_date, open, high, low, close, volume} ascending; "
    "extra keys allowed (adjusted_close)",
    "news_data": "list[dict]: {title, url, published, source, summary}",
    "fundamental_data": "dict: {periods: list[dict], statements: dict[str, list[dict]]}",
    "analyst_ratings": "dict: {symbol, consensus: {buy, hold, sell, ...}, summary: str}",
    "earnings_calendar": "list[dict]: {symbol, date, eps_estimate, revenue_estimate, hour}",
    "technical_indicators": "list[dict]: {trade_date, value} for the requested indicator",
    "options_data": "dict: {expirations: list[str], contracts: list[dict]}",
    "short_interest": "list[dict]: {date, shares_short, short_percent, days_to_cover}",
    "institution_data": "dict: {institutions: list[dict]} or {holdings: list[dict]}",
    "macro_data": "list[dict]: {date, value} for the requested series",
    "prediction_markets": "dict: {events: list[dict]}",
    "corporate_actions": "list[dict]: {type, date, ...} (dividend/split/buyback specific keys)",
    "news_sentiment": "list[dict]: {title, url, published, sentiment, score}",
    "sec_filings": "dict: {filings: list[dict]}",
    "risk_free_curve": "list[dict]: {date, tenor, rate}",
    "options_surface": "dict: {underlying, contracts: list[dict]}",
    "exchange_symbols": "list[dict]: {symbol, name, exchange, type, currency}",
    "equity_screener": "list[dict]: {symbol, name, price, change_percent, volume}",
    "market_movers": "list[dict]: {symbol, name, price, change_percent, volume}",
    "capital_flow": "list[dict]: {time, in_flow, main_in_flow, ...}",
    "earnings_catalyst": "dict: {events: list[dict]}",
    "earnings_surprise": "list[dict]: {symbol, period, eps_estimate, eps_actual, surprise_percent}",
    "economic_calendar": "list[dict]: {title, timestamp, country, importance, previous, consensus, actual}",
    "expected_move": "dict: {symbol, expected_move, iv, basis}",
    "fed_watch": "dict: {target_rate: list[dict], dot_plot: list[dict]}",
    "market_breadth": "dict: {rise, fall, equal, distribution: list[dict]}",
    "revenue_breakdown": "dict: {period, currency, items: list[dict]}",
    "smart_money": "dict: {holdings: list[dict]} or {trades: list[dict]}",
    "analyst_actions": "list[dict]: {symbol, date, action, rating, target_price, firm}",
    "fda_calendar": "list[dict]: {date, symbol, name, event}",
    "guidance_revisions": "list[dict]: {symbol, date, metric, prior, current}",
    "news_retractions": "list[dict]: {title, url, published, source}",
    "offerings_calendar": "list[dict]: {date, symbol, name, offering_type, amount}",
}


def shape_for(category: str) -> str:
    """Return the documented normal form for *category* (empty when unknown)."""
    return CATEGORY_SHAPES.get(category, "")
