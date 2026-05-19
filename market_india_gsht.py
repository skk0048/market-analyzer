"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  INDIA MARKET ANALYSIS  v5.2 — market_india.py                            ║
║  Run: python market_india.py                                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
import os, sys, time, warnings
import numpy as np, pandas as pd, yfinance as yf
from datetime import datetime, timedelta
warnings.filterwarnings("ignore")

SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
INDEX_DATA_DIR = r"C:\Users\sudhi\Documents\Trading\SectorRotation\IndexData"
OUTPUT_FILE    = os.path.join(SCRIPT_DIR, "India_Market_Analysis_v10.xlsx")
STOCK_CSV      = os.path.join(INDEX_DATA_DIR, "ind_niftytotalmarket_list.csv")
MAX_STOCKS          = 500
PERIOD_DAYS         = 420   # ↑ from 300 → 420 calendar days (covers RS_252d & 12M%)
ENABLE_PATTERNS     = False
PATTERN_MAX         = 400
FETCH_FINANCIALS    = True
SIGNAL_MAX_STOCKS   = 400
# ── Primary RS period for sector ranking & rotation ──────────────────────────
# Options: 22, 55, 120  (120 only works in Strength, not Rotation)
# Change this one number to switch which RS period drives all sector decisions.
PRIMARY_RS_PERIOD = 22   # ← change to 22 to test RS22 as primary
# Compute MST/LST/RS30 signals + Supertrend + Swing SL
ENABLE_SIGNALS      = True
# How many stocks to run full OHLCV signal computation on (needs High/Low data)
# Set lower for speed (e.g. 200), set same as PATTERN_MAX or MAX_STOCKS for full coverage
SIGNAL_MAX_STOCKS   = 400

# ── Historical analysis mode ─────────────────────────────────────────────────
# Set END_DATE to a past date to analyse historical data (e.g. "2024-06-30").
# Leave as None to use today's date (live / current mode).
END_DATE = None  # e.g. "2024-06-30"  — None = live / today

sys.path.insert(0, SCRIPT_DIR)
from market_signals import build_dashboard_df
from market_engine import (
    INDIA_INDEX, INDIA_SECTORS, INDIA_INDUSTRY_TO_SECTOR, INDIA_BREADTH_INDICES,
    RS_PERIODS, SIGNAL_PERIODS, fetch_close_batch, fetch_ohlcv_batch,
    fetch_ohlcv_with_cache, _normalize, calc_rs, calc_rsi, load_csv_constituents,
    safe_download,
    build_market_snapshot, build_sector_strength, build_sector_rotation,
    build_industry_rotation, build_market_breadth, build_sector_performance,
    build_stock_strength, build_top_picks_buy, build_top_picks_sell,
    build_chart_patterns_df, build_trade_setups, run_pattern_detection,
    build_rs_sleeve_list, build_country_etf_df, build_commodity_df,
)
from market_excel import build_workbook
from market_html  import build_html_report


def load_india_universe():
    path = STOCK_CSV if os.path.exists(STOCK_CSV) else os.path.join(SCRIPT_DIR,"ind_niftytotalmarket_list.csv")
    if not os.path.exists(path): print(f"  ❌ Universe CSV not found: {path}"); sys.exit(1)
    df = pd.read_csv(path); df.columns = df.columns.str.strip()
    if "Symbol"       not in df.columns: df = df.rename(columns={df.columns[2]:"Symbol"})
    if "Industry"     not in df.columns: df = df.rename(columns={df.columns[1]:"Industry"})
    if "Company Name" not in df.columns: df = df.rename(columns={df.columns[0]:"Company Name"})
    df["Symbol"]=df["Symbol"].str.strip(); df["Industry"]=df["Industry"].str.strip()
    df["Company Name"]=df["Company Name"].str.strip()
    df["Yahoo"]=df["Symbol"]+".NS"
    df["Sector"]=df["Industry"].map(INDIA_INDUSTRY_TO_SECTOR).fillna("Finance")
    if MAX_STOCKS > 0: df = df.head(MAX_STOCKS)
    print(f"  ✅ Universe: {len(df)} stocks | {df['Sector'].nunique()} sectors | {df['Industry'].nunique()} industries")
    return df


def fetch_india_sector_prices(universe):
    result = {}
    for sname, cfg in INDIA_SECTORS.items():
        ysym = cfg.get("yahoo")
        if ysym:
            try:
                raw = safe_download(ysym, days=PERIOD_DAYS, end_date=END_DATE, auto_adjust=True, progress=False)
                if not raw.empty and len(raw) >= 22:
                    cl = raw["Close"]
                    if isinstance(cl, pd.DataFrame): cl = cl.squeeze()
                    result[sname] = _normalize(cl.dropna()); print(f"    ✓ {sname:<22} {ysym}"); continue
            except: pass
        csv_f = cfg.get("csv")
        if csv_f:
            path = os.path.join(INDEX_DATA_DIR, csv_f)
            syms = load_csv_constituents(path, is_nse=True) if os.path.exists(path) else []
            if syms:
                try:
                    raw = safe_download(syms[:30], days=PERIOD_DAYS, end_date=END_DATE, auto_adjust=True, progress=False)
                    cls = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw[["Close"]]
                    cls = cls.dropna(how="all")
                    if len(cls) >= 22:
                        norm = cls/cls.iloc[0]*1000; result[sname]=_normalize(norm.mean(axis=1))
                        print(f"    ✓ {sname:<22} (constituent avg)"); continue
                except: pass
        print(f"    ✗ {sname:<22} not available")
    print(f"  ✅ Sector prices: {len(result)}/{len(INDIA_SECTORS)}")
    return result


def main():
    print("\n"+"═"*68)
    print("  INDIA MARKET ANALYSIS  v5.2")
    print(f"  {datetime.now().strftime('%d %b %Y  %H:%M IST')}")
    print(f"  Stocks:{MAX_STOCKS}  Patterns:{PATTERN_MAX}  Signals:{SIGNAL_MAX_STOCKS}  Fin:{FETCH_FINANCIALS}")
    if END_DATE:
        print(f"  ⚠  HISTORICAL MODE  — data clipped to {END_DATE}")
    print("═"*68+"\n")
    t0 = time.time()

    print("📂 Loading universe …"); universe = load_india_universe()

    # ── Price data ─────────────────────────────────────────────────────────────
    USE_CACHE = False
    try: from price_cache import PriceCache; USE_CACHE = True
    except: print("  ℹ  price_cache.py not found — downloading directly")

    stock_syms  = universe["Yahoo"].tolist()
    sector_syms = [cfg["yahoo"] for cfg in INDIA_SECTORS.values() if cfg.get("yahoo")]

    if USE_CACHE:
        print("\n📦 Loading via PriceCache …")
        cache    = PriceCache()
        start_dt = (datetime.now()-timedelta(days=PERIOD_DAYS+10)).strftime("%Y-%m-%d")
        all_syms = [INDIA_INDEX]+sector_syms+stock_syms
        close_all, _ = cache.get(all_syms, start_dt)
        # ── Clip to END_DATE if historical mode ────────────────────────────
        if END_DATE:
            close_all = close_all[close_all.index <= pd.Timestamp(END_DATE)]
        idx_col = close_all[INDIA_INDEX] if INDIA_INDEX in close_all.columns else pd.Series()
        if isinstance(idx_col, pd.DataFrame): idx_col = idx_col.squeeze()
        index_prices = idx_col.dropna()
        if index_prices.empty: print(f"  ❌ Cannot load {INDIA_INDEX}"); return
        sector_prices = {}
        for sname, cfg in INDIA_SECTORS.items():
            ysym = cfg.get("yahoo")
            if ysym and ysym in close_all.columns:
                s = close_all[ysym].dropna()
                if len(s) >= 22: sector_prices[sname] = s
        price_data = close_all[[s for s in stock_syms if s in close_all.columns]]
    else:
        print(f"\n📡 Fetching index ({INDIA_INDEX}) …")
        _end_str = END_DATE  # None = today
        raw = safe_download(INDIA_INDEX, days=PERIOD_DAYS, end_date=END_DATE, auto_adjust=True, progress=False)
        if raw.empty: print("  ❌ Cannot fetch index"); return
        cl = raw["Close"]
        if isinstance(cl, pd.DataFrame): cl = cl.squeeze()
        index_prices = _normalize(cl.dropna()); print(f"  ✅ Index: {len(index_prices)} days")
        print("\n📡 Sector indices …"); sector_prices = fetch_india_sector_prices(universe)
        print(f"\n📡 Fetching {len(stock_syms)} stock closes …")
        price_data = fetch_close_batch(stock_syms, PERIOD_DAYS, end_date=END_DATE)

    print(f"  ✅ Index:{len(index_prices)}d | Sectors:{len(sector_prices)} | Stocks:{len(price_data.columns)}")

    # ── OHLCV for patterns + signals ───────────────────────────────────────────
    ohlcv_dict = {}
    max_ohlcv  = max(PATTERN_MAX, SIGNAL_MAX_STOCKS if ENABLE_SIGNALS else 0)
    if max_ohlcv > 0:
        print(f"\n📦 OHLCV (cached fetch) for {max_ohlcv} stocks …")
        cands = [s for s in stock_syms if s in price_data.columns and len(price_data[s].dropna())>=60][:max_ohlcv]
        ohlcv_dict = fetch_ohlcv_with_cache(cands, days=PERIOD_DAYS)
        # Clip OHLCV to END_DATE in historical mode
        if END_DATE:
            end_ts = pd.Timestamp(END_DATE)
            ohlcv_dict = {s: df[df.index <= end_ts] for s, df in ohlcv_dict.items() if not df[df.index <= end_ts].empty}
        print(f"  ✅ OHLCV ready: {len(ohlcv_dict)} stocks")

    # ── Chart patterns ─────────────────────────────────────────────────────────
    patterns_by_sym = {}; patterns_list = []
    if ENABLE_PATTERNS:
        print(f"\n📐 Detecting chart patterns …")
        pat_dict = {k: v for k, v in ohlcv_dict.items() if len(v) >= 60}
        patterns_by_sym, patterns_list = run_pattern_detection(pat_dict)

    # ── Build DataFrames ───────────────────────────────────────────────────────
    print("\n📸 Market Snapshot …");       snap_df     = build_market_snapshot("INDIA")
    print("\n🏭 Sector Strength …");       sec_str_df  = build_sector_strength(universe, price_data, index_prices, sector_prices, primary_rs=PRIMARY_RS_PERIOD)
    print("\n🔄 Sector Rotation …");       sec_rot_df  = build_sector_rotation(universe, price_data, index_prices, primary_rs=PRIMARY_RS_PERIOD)
    print("\n🏭 Industry Rotation …");     ind_rot_df  = build_industry_rotation(universe, price_data, index_prices)
    print("\n📊 Market Breadth …")
    breadth_df = build_market_breadth(price_data, index_prices, INDIA_BREADTH_INDICES, INDEX_DATA_DIR, market="INDIA")
    print("\n📈 Sector Performance …");    sec_perf_df = build_sector_performance(sector_prices, index_prices)
    print("\n📊 Stock Strength + Signals + Financials …")
    stock_df = build_stock_strength(
        universe, price_data, index_prices, sector_prices,
        patterns_by_sym, market="INDIA",
        fetch_financials=FETCH_FINANCIALS,
        ohlcv_dict=ohlcv_dict if ENABLE_SIGNALS else {},
        primary_rs=PRIMARY_RS_PERIOD,
    )
    print("\n🏆 Top Picks – Buy …");  top_buy_df  = build_top_picks_buy(stock_df,  sec_str_df, market="INDIA", primary_rs=PRIMARY_RS_PERIOD)
    print("\n🔴 Top Picks – Sell …"); top_sell_df = build_top_picks_sell(stock_df, sec_str_df, market="INDIA", primary_rs=PRIMARY_RS_PERIOD)
    print("\n📐 Chart Patterns (last 14d) …"); chart_df = build_chart_patterns_df(patterns_list, stock_df, market="INDIA")
    print("\n🎯 Trade Setups …");     trade_df    = build_trade_setups(stock_df, sec_str_df, market="INDIA", primary_rs=PRIMARY_RS_PERIOD)
    print("\n📋 RS Sleeve Lists (A/B/C) …")
    sleeve_df = build_rs_sleeve_list(
        stock_df, universe, INDEX_DATA_DIR,
        market="INDIA",
        run_time=datetime.now().strftime("%d %b %Y  %H:%M IST"),
        index_prices=index_prices,
        price_data=price_data,
        ohlcv_dict=ohlcv_dict,
        primary_rs=PRIMARY_RS_PERIOD,
    )

    print("\n🌍 Country ETF Strength …")
    country_etf_df = build_country_etf_df(index_prices, period_days=PERIOD_DAYS,
                                           primary_rs=PRIMARY_RS_PERIOD, end_date=END_DATE)
    print("\n🏅 Commodity Strength …")
    commodity_df   = build_commodity_df(period_days=PERIOD_DAYS,
                                         primary_rs=PRIMARY_RS_PERIOD, end_date=END_DATE)

    # ── Dashboard ──────────────────────────────────────────────────────────────
    run_time    = datetime.now().strftime("%d %b %Y  %H:%M IST")
    dashboard_df= build_dashboard_df(stock_df, sec_str_df, "INDIA", run_time, primary_rs=PRIMARY_RS_PERIOD)

    # ── Excel export ──────────────────────────────────────────────────────────
    out_path = OUTPUT_FILE
    if END_DATE:
        base, ext = os.path.splitext(OUTPUT_FILE)
        out_path = f"{base}_{END_DATE}{ext}"
    print("\n📊 Building Excel workbook …")
    print("\n📊 Building Excel …")
    build_workbook(
        market="INDIA", snapshot_df=snap_df, sector_str_df=sec_str_df,
        sector_rot_df=sec_rot_df, industry_rot_df=ind_rot_df,
        breadth_df=breadth_df, sector_perf_df=sec_perf_df, stock_str_df=stock_df,
        top_buy_df=top_buy_df, top_sell_df=top_sell_df,
        chart_pat_df=chart_df, trade_df=trade_df, output_path=out_path,
        dashboard_df=dashboard_df, sleeve_df=sleeve_df, primary_rs=PRIMARY_RS_PERIOD,
        country_etf_df=country_etf_df, commodity_df=commodity_df,
    )

    # ── HTML report ────────────────────────────────────────────────────────────
    print("\n🌐 Building HTML report …")
    html_path = out_path.replace(".xlsx", ".html")
    build_html_report(
        market="INDIA",
        snapshot_df=snap_df, sector_str_df=sec_str_df,
        sector_rot_df=sec_rot_df, industry_rot_df=ind_rot_df,
        breadth_df=breadth_df, sector_perf_df=sec_perf_df, stock_str_df=stock_df,
        top_buy_df=top_buy_df, top_sell_df=top_sell_df,
        chart_pat_df=chart_df, trade_df=trade_df,
        dashboard_df=dashboard_df, sleeve_df=sleeve_df,
        country_etf_df=country_etf_df, commodity_df=commodity_df,
        output_path=html_path, run_time=run_time, primary_rs=PRIMARY_RS_PERIOD,
    )

    # ── Console summary ────────────────────────────────────────────────────────
    elapsed = time.time()-t0
    print("\n"+"═"*68)
    print(f"  ✅  COMPLETE!  |  📁 {out_path}  |  ⏱ {elapsed:.0f}s")
    if not sec_str_df.empty:
        print(f"\n  🏭 SECTORS (top 8 by RS_55d%):")
        print(f"  {'#':<4} {'Sector':<22} {'Sig':<8} {'RS22d':>7} {'RS55d':>7} {'Trend'}")
        print("  "+"-"*60)
        for _, r in sec_str_df.head(8).iterrows():
            print(f"  {int(r['Rank']):<4} {str(r['Sector']):<22} {str(r['Signal']):<8} "
                  f"{r.get('RS_22d%',0):>7.1f}% {r.get('RS_55d%',0):>7.1f}% {r.get('Trend','')}")
    if not stock_df.empty:
        sb  = (stock_df["Enhanced"]=="Strong Buy").sum()
        b   = (stock_df["Signal"]=="Buy").sum()
        s   = (stock_df["Signal"]=="Sell").sum()
        mst = (stock_df.get("MST_Signal",pd.Series())=="Buy").sum() if "MST_Signal" in stock_df.columns else 0
        lst = (stock_df.get("LST_Signal",pd.Series())=="Buy").sum() if "LST_Signal" in stock_df.columns else 0
        r30 = (stock_df.get("RS30_Signal",pd.Series())=="Buy").sum() if "RS30_Signal" in stock_df.columns else 0
        print(f"\n  ⭐ Strong Buy:{sb} | ✅ Buy:{b} | 🔴 Sell:{s} | Neutral:{len(stock_df)-b-s}")
        print(f"  📅 MST Buy:{mst} | 📆 LST Buy:{lst} | 📊 RS30 Buy:{r30}")
    if not top_buy_df.empty and "Message" not in top_buy_df.columns:
        print(f"\n  🏆 Top Buy: {len(top_buy_df)} stocks across {top_buy_df['Sector'].nunique()} sectors")
    if not top_sell_df.empty and "Message" not in top_sell_df.columns:
        print(f"  🔴 Top Sell: {len(top_sell_df)} stocks across {top_sell_df['Sector'].nunique()} sectors")
    if not trade_df.empty:
        buys  = (trade_df["Action"]=="BUY").sum()
        sells = (trade_df["Action"]=="SELL").sum()
        print(f"  🎯 Trade Setups: {buys} BUY | {sells} SELL | {len(trade_df)-buys-sells} WAIT")
    print("═"*68)


if __name__ == "__main__":
    main()

