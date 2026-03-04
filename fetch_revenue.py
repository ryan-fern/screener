#!/usr/bin/env python3
"""
Revenue Re-Acceleration Screener — Daily Data Fetcher
=======================================================
Universe : S&P 600 small-cap index components (via Wikipedia)
Data     : yfinance — free, no API key required

Scoring  : YoY revenue acceleration — how much faster (or slower) is the
           company growing vs the prior quarter's YoY rate?
           score = current_yoy_growth - prior_yoy_growth  (in ppt)
           Positive = re-accelerating. Negative = decelerating.

           YoY comparison naturally strips seasonality, so the same score
           applies to both seasonal and non-seasonal companies.

Run      : python3 fetch_revenue.py
Cron     : runs via .github/workflows/daily-fetch.yml
"""

import io
import json
import os
import statistics
import time
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "revenue_data.json")

MARKET_CAP_MAX = 5_000_000_000  # $5B
MARKET_CAP_MIN = 100_000_000    # $100M

SECTOR_MAP = {
    "Information Technology": "Technology",
    "Consumer Discretionary": "Consumer",
    "Consumer Staples":       "Consumer",
}

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

# Revenue row labels yfinance may use (try in order)
REVENUE_LABELS = ["Total Revenue", "Revenue", "Net Revenue", "Sales"]


# ---------------------------------------------------------------------------
# Universe (same as analyst screener)
# ---------------------------------------------------------------------------

def get_universe():
    print("  Fetching S&P 600 components from Wikipedia...")
    resp = requests.get(SP600_URL, headers={"User-Agent": "Mozilla/5.0 (screener-bot/1.0)"}, timeout=15)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    df = tables[0]

    filtered = df[df["GICS Sector"].isin(SECTOR_MAP.keys())].copy()
    print(f"  {len(filtered)} tickers in target sectors")

    universe = []
    for _, row in filtered.iterrows():
        symbol = str(row["Symbol"]).replace(".", "-")
        universe.append({
            "symbol":      symbol,
            "name":        row["Security"],
            "wiki_sector": row["GICS Sector"],
        })
    return universe


# ---------------------------------------------------------------------------
# Revenue data
# ---------------------------------------------------------------------------

def quarter_label(ts):
    """Convert a Timestamp to e.g. Q3'24"""
    q = (ts.month - 1) // 3 + 1
    return f"Q{q}'{ts.year % 100:02d}"


def get_revenue_series(symbol):
    """
    Returns a list of (timestamp, revenue) tuples sorted newest-first,
    or None if data is unavailable.
    """
    try:
        ticker = yf.Ticker(symbol)
        stmt = ticker.quarterly_income_stmt

        if stmt is None or stmt.empty:
            return None

        rev_row = None
        for label in REVENUE_LABELS:
            if label in stmt.index:
                rev_row = stmt.loc[label].dropna()
                break

        if rev_row is None or len(rev_row) < 5:
            return None

        rev_row = rev_row.sort_index(ascending=False)  # newest first
        return list(zip(rev_row.index, rev_row.values))

    except Exception as e:
        print(f"    WARN: {symbol} — {e}")
        return None


def compute_revenue_scores(series, market_cap):
    """
    series : list of (timestamp, revenue) newest-first, at least 4 entries
    Returns a dict of metrics or None.

    Scoring approach:
      yfinance returns ~5 quarters of data — enough for current YoY (Q0/Q4)
      but NOT enough for prior-quarter YoY acceleration (would need Q1/Q5).

      Instead, we use QoQ acceleration as the primary re-acceleration signal:
        score = (Q0/Q1 - 1) - (Q1/Q2 - 1)  [in ppt]
      Positive = sequential growth is speeding up.

      For seasonal companies (avg QoQ swing >20%), QoQ is noisy so we
      surface YoY as the main context metric and flag them clearly.
    """
    vals  = [v for _, v in series]
    dates = [d for d, _ in series]

    if len(vals) < 4:
        return None

    q0, q1, q2, q3 = vals[0], vals[1], vals[2], vals[3]
    q4 = vals[4] if len(vals) > 4 else None

    # YoY (context — shown in table but not used for score)
    yoy_current = round((q0 / q4 - 1) * 100, 1) if q4 else None

    # QoQ sequential growth rates
    qoq_current = round((q0 / q1 - 1) * 100, 1) if q1 else None
    qoq_prior   = round((q1 / q2 - 1) * 100, 1) if q2 else None
    qoq_prior2  = round((q2 / q3 - 1) * 100, 1) if q3 else None

    # QoQ acceleration = change in QoQ growth rate (ppt)
    qoq_accel = round(qoq_current - qoq_prior, 1) if (qoq_current is not None and qoq_prior is not None) else None

    # Seasonality: flag if average absolute QoQ swing > 20%
    swings = [abs(vals[i] / vals[i+1] - 1) for i in range(min(5, len(vals)-1)) if vals[i+1]]
    seasonal = statistics.mean(swings) > 0.20 if swings else False

    # Score = QoQ acceleration for all companies.
    # Seasonal flag lets users interpret cautiously.
    score = qoq_accel

    return {
        "latest_quarter": quarter_label(dates[0]),
        "revenue_m":      round(q0 / 1e6, 1),
        "yoy_pct":        yoy_current,
        "qoq_pct":        qoq_current,
        "prior_qoq_pct":  qoq_prior,
        "qoq_accel":      qoq_accel,
        "seasonal":       bool(seasonal),
        "score":          score,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    started = datetime.now()
    print(f"=== Revenue screener run started at {started.isoformat()} ===\n")

    print("[1/3] Building universe from S&P 600...")
    try:
        universe = get_universe()
    except Exception as e:
        print(f"ERROR: Could not fetch universe — {e}")
        return

    print(f"\n[2/3] Fetching quarterly revenue for {len(universe)} tickers...")
    results = []

    for i, stock in enumerate(universe, 1):
        symbol = stock["symbol"]
        print(f"  [{i:>3}/{len(universe)}] {symbol}")

        # Get market cap from yfinance info (needed for filter)
        try:
            info = yf.Ticker(symbol).info
            mc       = info.get("marketCap")
            industry = info.get("industry") or ""
            price    = info.get("currentPrice") or info.get("regularMarketPrice")
        except Exception:
            time.sleep(0.3)
            continue

        if mc is None or mc < MARKET_CAP_MIN or mc > MARKET_CAP_MAX:
            time.sleep(0.3)
            continue

        series = get_revenue_series(symbol)
        if not series:
            time.sleep(0.3)
            continue

        scores = compute_revenue_scores(series, mc)
        if scores is None or scores["score"] is None:
            time.sleep(0.3)
            continue

        # Sector label
        if industry in HARDWARE_INDUSTRIES:
            display_sector = "Hardware"
        elif stock["wiki_sector"] == "Information Technology":
            display_sector = "Technology"
        else:
            display_sector = "Consumer"

        results.append({
            "symbol":   symbol,
            "name":     stock["name"],
            "sector":   display_sector,
            "industry": industry,
            "market_cap": mc,
            "price":    price,
            **scores,
        })

        time.sleep(0.5)

    # Sort: highest re-acceleration score first
    results.sort(key=lambda x: x["score"], reverse=True)

    output = {
        "updated_at":       started.isoformat(),
        "run_completed_at": datetime.now().isoformat(),
        "count":            len(results),
        "stocks":           results,
    }

    print(f"\n[3/3] Writing {len(results)} stocks to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    elapsed = (datetime.now() - started).seconds
    print(f"\nDone in {elapsed}s — {len(results)} stocks scored.")


if __name__ == "__main__":
    run()
