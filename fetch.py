#!/usr/bin/env python3
"""
Stock Screener — Daily Data Fetcher
=====================================
Universe : S&P 600 small-cap index components (via Wikipedia)
Data     : yfinance — free, no API key required
Scoring  : analyst consensus upside %, analyst spread, confidence

Run      : python3 fetch.py
Cron     : 0 6 * * * cd /path/to/screener && python3 fetch.py >> fetch.log 2>&1
"""

import json
import os
import time
from datetime import datetime

import io

import pandas as pd
import requests
import yfinance as yf

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "screener_data.json")

MARKET_CAP_MAX = 1_000_000_000  # $1B
MARKET_CAP_MIN = 100_000_000    # $100M floor — filter micro-cap noise

# S&P 600 GICS sector names → screener display labels
SECTOR_MAP = {
    "Information Technology": "Technology",
    "Consumer Discretionary": "Consumer",
    "Consumer Staples":       "Consumer",
}

# Industries we re-label as "Hardware" (otherwise they'd show as Technology)
HARDWARE_INDUSTRIES = {
    "Consumer Electronics",
    "Electronic Components",
    "Electronics & Computer Distribution",
    "Computer Hardware",
    "Semiconductors",
    "Semiconductor Equipment & Materials",
    "Technology Hardware, Storage & Peripherals",
}

SP600_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"


# ---------------------------------------------------------------------------
# Step 1 — Universe
# ---------------------------------------------------------------------------

def get_universe():
    """
    Pull S&P 600 components from Wikipedia and return those in target sectors.
    Wikipedia table columns: Company, Symbol, GICS Sector, GICS Sub-Industry, ...
    """
    print("  Fetching S&P 600 components from Wikipedia...")
    resp = requests.get(SP600_URL, headers={"User-Agent": "Mozilla/5.0 (screener-bot/1.0)"}, timeout=15)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    df = tables[0]

    filtered = df[df["GICS Sector"].isin(SECTOR_MAP.keys())].copy()
    print(f"  {len(filtered)} tickers in target sectors")

    universe = []
    for _, row in filtered.iterrows():
        symbol = str(row["Symbol"]).replace(".", "-")  # yfinance uses BRK-B not BRK.B
        universe.append({
            "symbol":       symbol,
            "name":         row["Security"],
            "wiki_sector":  row["GICS Sector"],
            "sub_industry": row.get("GICS Sub-Industry", ""),
        })

    return universe


# ---------------------------------------------------------------------------
# Step 2 — Fetch per-stock data from yfinance
# ---------------------------------------------------------------------------

def fetch_yf(symbol):
    """
    Returns a dict of raw fields from yfinance info, or None if data is missing.
    Key analyst fields yfinance exposes:
      targetMeanPrice   — analyst consensus mean price target
      targetHighPrice   — highest analyst target
      targetLowPrice    — lowest analyst target
      targetMedianPrice — median target
      numberOfAnalystOpinions — analyst count
    """
    try:
        info = yf.Ticker(symbol).info

        price      = info.get("currentPrice") or info.get("regularMarketPrice")
        target_mean   = info.get("targetMeanPrice")
        target_high   = info.get("targetHighPrice")
        target_low    = info.get("targetLowPrice")
        market_cap    = info.get("marketCap")
        analyst_count = int(info.get("numberOfAnalystOpinions") or 0)
        industry      = info.get("industry") or ""

        if not price or not target_mean:
            return None

        return {
            "price":         price,
            "target_mean":   target_mean,
            "target_high":   target_high,
            "target_low":    target_low,
            "market_cap":    market_cap,
            "analyst_count": analyst_count,
            "industry":      industry,
        }

    except Exception as e:
        print(f"    WARN: {symbol} — {e}")
        return None


# ---------------------------------------------------------------------------
# Step 3 — Score
# ---------------------------------------------------------------------------

def compute_scores(data):
    """
    Metrics:
      upside_pct       — % from current price to analyst mean target (main rank)
      gap_pct          — absolute value (catches overvalued too)
      disagreement_pct — (high target − low target) / mean target; high = analysts split
      combined_score   — upside × confidence (rewards conviction + upside together)
      confidence       — normalized analyst count (10 analysts = 100%)
    """
    price         = data["price"]
    target_mean   = data["target_mean"]
    target_high   = data.get("target_high")
    target_low    = data.get("target_low")
    analyst_count = data.get("analyst_count", 0)

    upside_pct = (target_mean - price) / price * 100
    gap_pct    = abs(upside_pct)

    if target_high and target_low and target_mean:
        disagreement_pct = round((target_high - target_low) / target_mean * 100, 1)
    else:
        disagreement_pct = None

    confidence     = round(min(1.0, analyst_count / 10.0), 2)
    combined_score = round(upside_pct * confidence, 1)

    return {
        "weighted_avg_target": round(target_mean, 2),  # field name kept for frontend compat
        "upside_pct":          round(upside_pct, 1),
        "gap_pct":             round(gap_pct, 1),
        "analyst_count":       analyst_count,
        "disagreement_pct":    disagreement_pct,
        "combined_score":      combined_score,
        "confidence":          confidence,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    started = datetime.now()
    print(f"=== Screener run started at {started.isoformat()} ===\n")

    print("[1/3] Building universe from S&P 600...")
    try:
        universe = get_universe()
    except Exception as e:
        print(f"ERROR: Could not fetch universe — {e}")
        return

    print(f"\n[2/3] Fetching yfinance data for {len(universe)} tickers...")
    results = []

    for i, stock in enumerate(universe, 1):
        symbol = stock["symbol"]
        print(f"  [{i:>3}/{len(universe)}] {symbol}")

        data = fetch_yf(symbol)

        if data is None:
            time.sleep(0.3)
            continue

        # Market cap filter
        mc = data.get("market_cap")
        if mc is None or mc < MARKET_CAP_MIN or mc > MARKET_CAP_MAX:
            time.sleep(0.3)
            continue

        # Sector display label
        industry = data["industry"]
        if industry in HARDWARE_INDUSTRIES:
            display_sector = "Hardware"
        elif stock["wiki_sector"] == "Information Technology":
            display_sector = "Technology"
        else:
            display_sector = "Consumer"

        scores = compute_scores(data)

        results.append({
            "symbol":     symbol,
            "name":       stock["name"],
            "sector":     display_sector,
            "industry":   industry,
            "market_cap": mc,
            "price":      data["price"],
            **scores,
        })

        time.sleep(0.5)  # be polite to yfinance — avoid getting throttled

    # Default sort: highest upside first
    results.sort(key=lambda x: x["upside_pct"], reverse=True)

    output = {
        "updated_at":        started.isoformat(),
        "run_completed_at":  datetime.now().isoformat(),
        "count":             len(results),
        "stocks":            results,
    }

    print(f"\n[3/3] Writing {len(results)} stocks to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    elapsed = (datetime.now() - started).seconds
    print(f"\nDone in {elapsed}s — {len(results)} stocks scored.")


if __name__ == "__main__":
    run()
