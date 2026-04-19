"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  US MARKET ANALYSIS — GOOGLE SHEETS EDITION  v5.4                         ║
║  market_usa_gsht.py  — GitHub Actions compatible                          ║
║                                                                            ║
║  Exact US counterpart of India_market_india_gsht.py v5.4.                 ║
║  All helper functions (GS_COLORS, _cell_bg, write_tab, write_dashboard,   ║
║  write_sleeve_tab) are identical — only the market-specific parts differ:  ║
║   • Benchmark : S&P 500 via SPY ETF                                        ║
║   • Universe  : us_sp500list.csv  (must be sorted by mkt cap, largest 1st)║
║   • Sectors   : 11 SPDR ETFs  (XLK, XLF, XLV, XLY, XLI …)               ║
║   • Sleeves   : US_A (Top 50, Monthly) · US_B (51-200, Ftn) ·             ║
║                 US_C (201-500, Weekly)                                     ║
║   • Timezone  : ET  (GitHub Actions UTC label)                             ║
║   • Sheet URL : GOOGLE_SHEET_URL_US env var  (separate spreadsheet)       ║
║                                                                            ║
║  HOW TO AUTHENTICATE:                                                      ║
║   GitHub Secrets:                                                          ║
║     GOOGLE_CREDENTIALS    → contents of service account JSON               ║
║     GOOGLE_SHEET_URL_US   → full US spreadsheet URL                        ║
║   (can share the same service-account JSON as the India sheet)             ║
║                                                                            ║
║  GitHub Actions writes the JSON to /tmp/creds.json and sets               ║
║     GOOGLE_CREDENTIALS_PATH=/tmp/creds.json                               ║
║                                                                            ║
║  SHEETS WRITTEN (13 tabs):                                                 ║
║   0. 📋 Dashboard        7. 📊 Stock Strength                              ║
║   1. 📸 Market Snapshot  8. 🏆 Top Picks - Buy                             ║
║   2. 🏭 Sector Strength  9. 🔴 Top Picks - Sell                            ║
║   3. 🔄 Sector Rotation  10. 📐 Chart Patterns                             ║
║   4. 🏭 Industry Rotat.  11. 🎯 Trade Setups                               ║
║   5. 📊 Market Breadth   12. 📋 RS Sleeve Lists                            ║
║   6. 📈 Sector Perf.                                                       ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
import os, sys, time, warnings
import numpy as np, pandas as pd, yfinance as yf
from datetime import datetime, timedelta
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import tenacity
warnings.filterwarnings("ignore")

SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
INDEX_DATA_DIR = os.path.join(SCRIPT_DIR, "IndexData")
STOCK_CSV      = os.path.join(INDEX_DATA_DIR, "us_sp500list.csv")

# ── Tunable constants ─────────────────────────────────────────────────────────
MAX_STOCKS        = 500
PERIOD_DAYS       = 420   # 420 calendar days covers RS_252d & 12M%
ENABLE_PATTERNS   = True
PATTERN_MAX       = 400
FETCH_FINANCIALS  = True   # always True in cloud — no cache
ENABLE_SIGNALS    = True
SIGNAL_MAX_STOCKS = 400

# ── Google Sheets auth ────────────────────────────────────────────────────────
CREDENTIALS_PATH = (
    os.environ.get("GOOGLE_CREDENTIALS_PATH")
    or os.path.join(SCRIPT_DIR, "google_credentials.json")
)
SHEET_URL = (
    # Separate env var so India and US can write to different spreadsheets
    os.environ.get("GOOGLE_SHEET_URL_US")
    or os.environ.get("GOOGLE_SHEET_URL")          # fallback: same sheet
    or "https://docs.google.com/spreadsheets/d/YOUR_US_SHEET_ID/edit"
)
GSCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

sys.path.insert(0, SCRIPT_DIR)
from market_signals import build_dashboard_df
from market_engine import (
    US_INDEX, US_SECTORS, US_INDUSTRY_TO_SECTOR, US_BREADTH_INDICES,
    RS_PERIODS, SIGNAL_PERIODS, fetch_close_batch, fetch_ohlcv_batch,
    fetch_ohlcv_with_cache, _normalize, calc_rs, calc_rsi,
    build_market_snapshot, build_sector_strength,
    build_sector_rotation, build_industry_rotation, build_market_breadth,
    build_sector_performance, build_stock_strength, build_top_picks_buy,
    build_top_picks_sell, build_chart_patterns_df, build_trade_setups,
    run_pattern_detection, build_rs_sleeve_list,
)

# ─────────────────────────────────────────────────────────────────────────────
#  GOOGLE SHEETS HELPERS
#  (identical to India gsheet — shared colour palette, formatting, API wrapper)
# ─────────────────────────────────────────────────────────────────────────────

GS_COLORS = {
    "navy":     {"red": 0.051, "green": 0.129, "blue": 0.216},
    "teal":     {"red": 0.000, "green": 0.537, "blue": 0.482},
    "green":    {"red": 0.106, "green": 0.365, "blue": 0.165},
    "red":      {"red": 0.835, "green": 0.153, "blue": 0.157},
    "white":    {"red": 1.000, "green": 1.000, "blue": 1.000},
    "lt_green": {"red": 0.784, "green": 0.902, "blue": 0.788},
    "lt_red":   {"red": 1.000, "green": 0.800, "blue": 0.800},
    "amber":    {"red": 1.000, "green": 0.973, "blue": 0.769},
    "lt_blue":  {"red": 0.878, "green": 0.937, "blue": 1.000},
}


def gs_connect():
    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(
            f"Credentials not found: {CREDENTIALS_PATH}\n"
            "Set env var GOOGLE_CREDENTIALS_PATH or place google_credentials.json "
            "in the same folder as this script."
        )
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=GSCOPE)
    gc    = gspread.authorize(creds)
    ss    = gc.open_by_url(SHEET_URL)
    print(f"  ✅ Connected: '{ss.title}'")
    return ss


def _get_ws(ss, title, rows=3000, cols=60):
    try:    return ss.worksheet(title)
    except: return ss.add_worksheet(title=title, rows=rows, cols=cols)


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=3, max=60),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type(
        (gspread.exceptions.APIError, ConnectionError)
    ),
    reraise=True,
)
def _api(fn, *args, **kwargs):
    return fn(*args, **kwargs)


def _cell_bg(val, col_name):
    """Return a GS colour dict based on column name and value."""
    col = col_name.lower()
    # Signal / action columns
    if col in ("signal", "enhanced", "action", "sec_signal", "rs_signal",
               "mst_signal", "lst_signal", "rs30_signal", "supertrend",
               "supertrend_w", "st_daily", "st_week"):
        v = str(val or "")
        if v in ("Strong Buy", "BUY", "Buy"):  return GS_COLORS["lt_green"]
        if v in ("Sell", "SELL"):              return GS_COLORS["lt_red"]
        if v in ("Neutral", "WAIT", "NA"):     return GS_COLORS["amber"]
        if v == "Watch":                        return GS_COLORS["lt_blue"]
    # Trend / zone
    if "trend" in col or "_zone" in col:
        v = str(val or "").lower()
        if "bullish" in v or "recovering" in v: return GS_COLORS["lt_green"]
        if "bearish" in v or "pulling" in v:    return GS_COLORS["lt_red"]
        if "neutral" in v or "mixed" in v:      return GS_COLORS["amber"]
    # ✓ / ✗ ticks
    if col.startswith("abv_") or "beats" in col:
        if str(val) == "✓": return GS_COLORS["lt_green"]
        if str(val) == "✗": return GS_COLORS["lt_red"]
    # Breadth % scores
    if col in ("rs22%", "rs55%", "rsi50%", "abvsma20%", "abvsma50%", "abvsma100%",
               "abvsma200%", "1m_score", "3m_score", "6m_score"):
        try:
            v = float(val or 0)
            if v >= 60: return GS_COLORS["lt_green"]
            if v >= 40: return GS_COLORS["amber"]
            return GS_COLORS["lt_red"]
        except: pass
    # ATR weight — higher = greener
    if col in ("atr_wt%", "equal_wt%"):
        try:
            v = float(val or 0)
            if v >= 10: return GS_COLORS["lt_green"]
            if v >= 5:  return GS_COLORS["amber"]
        except: pass
    # Daily std — lower = greener (less volatile = better)
    if col == "daily_std%":
        try:
            v = float(val or 0)
            if v <= 1.5: return GS_COLORS["lt_green"]
            if v <= 2.5: return GS_COLORS["amber"]
            return GS_COLORS["lt_red"]
        except: pass
    # Avg turnover — higher = greener
    if col == "avg_turnover":
        try:
            v = float(val or 0)
            if v >= 100: return GS_COLORS["lt_green"]
            if v >= 20:  return GS_COLORS["amber"]
            if v < 5:    return GS_COLORS["lt_red"]
        except: pass
    # SL grade
    if col == "sl_grade":
        pal = {"A": GS_COLORS["lt_green"], "B": GS_COLORS["lt_green"],
               "C": GS_COLORS["amber"],    "D": GS_COLORS["lt_red"],
               "F": GS_COLORS["lt_red"]}
        return pal.get(str(val), None)
    # Generic %: positive = green, negative = red
    pct_cols = {"chg_1d%", "chg_5d%", "rs_22d%", "rs_55d%", "rs_22d_idx%",
                "rs_55d_idx%", "rs_22d_sec%", "rs_55d_sec%", "rs_120d_idx%",
                "rs_252d_idx%", "w_rs21%", "w_rs30%", "m_rs12%", "rs_score",
                "total_score", "1m%", "3m%", "6m%", "12m%", "ytd%",
                "rs_1m%", "rs_3m%", "rs_6m%", "rs_12m%", "from_52w_high%",
                "sales_qoq%", "sales_yoy%", "pat_qoq%", "pat_yoy%",
                "margin%", "roe%", "ret_22d%", "ret_55d%", "fin_score"}
    if col in pct_cols or col.endswith("%"):
        try:
            v = float(val or 0)
            if v > 0:  return GS_COLORS["lt_green"]
            if v < 0:  return GS_COLORS["lt_red"]
        except: pass
    return None


def write_tab(ss, title, df, hdr_bg="navy", skip_cols=None):
    """
    Clear and write a DataFrame to a named tab.
    Applies header formatting + value colouring + left-align for text columns.
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        ws = _get_ws(ss, title); _api(ws.clear)
        _api(ws.update, "A1", [["No data available."]]); return

    display_cols = [c for c in df.columns if not c.startswith("_")]
    if skip_cols:
        display_cols = [c for c in display_cols if c not in skip_cols]
    df_out = df[display_cols].copy().replace([float("inf"), float("-inf")], "").fillna("")

    nr, nc = len(df_out) + 1, len(df_out.columns)
    ws = _get_ws(ss, title, rows=max(nr + 50, 500), cols=max(nc + 5, 30))

    _api(ws.clear); time.sleep(0.5)
    _api(set_with_dataframe, ws, df_out, resize=True); time.sleep(0.5)

    # Text columns that should be left-aligned
    _LEFT_COLS = {
        'symbol', 'tv_symbol', 'company', 'company name', 'name',
        'sector', 'industry', 'chart_pattern', 'notes', 'setup_desc',
        'signal_type', 'strategy', 'trend', 'action', 'signal',
        'enhanced', 'mst_signal', 'lst_signal', 'rs30_signal', 'supertrend',
    }

    try:
        col_end = gspread.utils.rowcol_to_a1(1, nc).replace("1", "")
        _api(ws.format, f"A1:{col_end}1", {
            "backgroundColor": GS_COLORS.get(hdr_bg, GS_COLORS["navy"]),
            "textFormat": {
                "foregroundColor": GS_COLORS["white"],
                "bold": True, "fontSize": 10,
            },
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
        })
        time.sleep(0.3)
        # Freeze row 1
        _api(ss.batch_update, {"requests": [{
            "updateSheetProperties": {
                "properties": {
                    "sheetId": ws.id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }]})
        time.sleep(0.2)
        # Left-align text columns
        align_reqs = []
        for ci, col in enumerate(df_out.columns, 1):
            if col.lower() in _LEFT_COLS:
                col_letter = gspread.utils.rowcol_to_a1(1, ci).replace("1", "")
                if nr > 1:
                    align_reqs.append({
                        "range": f"{col_letter}2:{col_letter}{nr + 1}",
                        "format": {"horizontalAlignment": "LEFT"},
                    })
        for i in range(0, len(align_reqs), 30):
            try:
                _api(ws.batch_format, align_reqs[i:i + 30]); time.sleep(0.2)
            except Exception: pass
    except Exception:
        pass

    # Value colouring — batch to avoid quota hits
    cell_fmts = []
    for ci, col in enumerate(df_out.columns, 1):
        col_letter = gspread.utils.rowcol_to_a1(1, ci).replace("1", "")
        for ri, val in enumerate(df_out[col], start=2):
            bg = _cell_bg(val, col)
            if bg:
                cell_fmts.append({"range": f"{col_letter}{ri}",
                                   "format": {"backgroundColor": bg}})
    for i in range(0, len(cell_fmts), 60):
        try:
            _api(ws.batch_format, cell_fmts[i:i + 60]); time.sleep(0.2)
        except Exception: pass

    time.sleep(0.8)
    print(f"    ✓ '{title}' — {len(df_out)} rows × {len(df_out.columns)} cols")


def write_dashboard_tab(ss, dash_df, market):
    """Write the Dashboard tab with special two-column layout."""
    title = "📋 Dashboard"
    ws    = _get_ws(ss, title, rows=200, cols=2)
    _api(ws.clear); time.sleep(0.4)

    if dash_df is None or dash_df.empty:
        _api(ws.update, "A1", [["No dashboard data."]]); return

    clean = dash_df.copy().fillna("").astype(str)
    _api(set_with_dataframe, ws, clean, resize=True); time.sleep(0.5)

    try:
        _api(ws.format, "A1:B1", {
            "backgroundColor": {"red": 0.039, "green": 0.086, "blue": 0.157},
            "textFormat": {"foregroundColor": {"red": 0, "green": 0.898, "blue": 1},
                           "bold": True, "fontSize": 13},
        })
        ws.columns_auto_resize(0, 1)
    except Exception:
        pass

    time.sleep(0.8)
    print(f"    ✓ '{title}' — {len(dash_df)} rows")


def write_sleeve_tab(ss, sleeve_df, market="US"):
    """
    Write the RS Sleeve / Smallcase Action List tab.
    Regime banner: green (BULL) / amber (CAUTION) / red (BEAR).
    Sleeve tiers: light blue (US_A) / light green (US_B) / light amber (US_C).
    """
    title = "📋 RS Sleeve Lists"
    if sleeve_df is None or (hasattr(sleeve_df, "empty") and sleeve_df.empty):
        ws = _get_ws(ss, title); _api(ws.clear)
        _api(ws.update, "A1", [["No sleeve data available."]]); return

    display_cols = list(sleeve_df.columns)
    df_out = sleeve_df[display_cols].copy().fillna("").astype(str)

    nr, nc = len(df_out) + 1, len(df_out.columns)
    ws = _get_ws(ss, title, rows=max(nr + 50, 300), cols=max(nc + 5, 30))
    _api(ws.clear); time.sleep(0.5)
    _api(set_with_dataframe, ws, df_out, resize=True); time.sleep(0.5)

    # Header row
    try:
        col_end = gspread.utils.rowcol_to_a1(1, nc).replace("1", "")
        _api(ws.format, f"A1:{col_end}1", {
            "backgroundColor": {"red": 0.102, "green": 0.227, "blue": 0.361},
            "textFormat": {"foregroundColor": {"red": 0, "green": 0.898, "blue": 1},
                           "bold": True, "fontSize": 10},
            "horizontalAlignment": "CENTER",
        })
        time.sleep(0.3)
        _api(ss.batch_update, {"requests": [{
            "updateSheetProperties": {
                "properties": {
                    "sheetId": ws.id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }]})
    except Exception:
        pass

    # Row-level colouring
    _DARK_NAV = {"red": 0.102, "green": 0.227, "blue": 0.361}
    _DARK_LEG = {"red": 0.149, "green": 0.196, "blue": 0.220}
    _REGIME_COLORS = {
        "BULL":    {"red": 0.106, "green": 0.365, "blue": 0.165},
        "CAUTION": {"red": 0.902, "green": 0.396, "blue": 0.000},
        "BEAR":    {"red": 0.718, "green": 0.110, "blue": 0.110},
    }
    _TIER_COLS = {
        "US_A": {"red": 0.886, "green": 0.945, "blue": 0.992},   # light blue
        "US_B": {"red": 0.910, "green": 0.961, "blue": 0.914},   # light green
        "US_C": {"red": 1.000, "green": 0.973, "blue": 0.882},   # light amber
    }
    cur_tier = "US_A"
    row_fmts = []

    for ri, (_, row) in enumerate(df_out.iterrows(), start=2):
        rank_val  = str(row.get("Rank", "") or "")
        is_div    = rank_val.startswith("━━━")
        is_blank  = all(str(v).strip() == "" for v in row.values)
        is_regime = is_div and "MARKET REGIME" in rank_val.upper()
        if is_blank:
            continue

        if is_div:
            # Update current tier colour
            for key in ["US_A", "US_B", "US_C"]:
                if f"SLEEVE {key}" in rank_val:
                    cur_tier = key; break
            if is_regime:
                sym_val = str(row.get("Symbol", "") or "")
                if "BEAR" in sym_val and "CAUTION" not in sym_val:
                    bg = _REGIME_COLORS["BEAR"]
                elif "CAUTION" in sym_val:
                    bg = _REGIME_COLORS["CAUTION"]
                else:
                    bg = _REGIME_COLORS["BULL"]
            elif "METHOD" in rank_val.upper():
                bg = _DARK_LEG
            else:
                bg = _DARK_NAV
            fg = {"red": 0, "green": 0.898, "blue": 1}
            row_fmts.append({
                "range": f"A{ri}:{gspread.utils.rowcol_to_a1(ri, nc).replace(str(ri), '')+str(ri)}",
                "format": {
                    "backgroundColor": bg,
                    "textFormat": {"foregroundColor": fg, "bold": True},
                }
            })
        else:
            bg = _TIER_COLS.get(cur_tier, _TIER_COLS["US_A"])
            row_fmts.append({
                "range": f"A{ri}:{gspread.utils.rowcol_to_a1(ri, nc).replace(str(ri), '')+str(ri)}",
                "format": {"backgroundColor": bg}
            })

    for i in range(0, len(row_fmts), 30):
        try:
            _api(ws.batch_format, row_fmts[i:i + 30]); time.sleep(0.3)
        except Exception: pass

    # Value-level colouring
    cell_fmts = []
    for ci, col in enumerate(df_out.columns, 1):
        col_letter = gspread.utils.rowcol_to_a1(1, ci).replace("1", "")
        for ri, val in enumerate(df_out[col], start=2):
            bg = _cell_bg(val, col)
            if bg:
                cell_fmts.append({"range": f"{col_letter}{ri}",
                                   "format": {"backgroundColor": bg}})
    for i in range(0, len(cell_fmts), 60):
        try:
            _api(ws.batch_format, cell_fmts[i:i + 60]); time.sleep(0.2)
        except Exception: pass

    time.sleep(0.8)
    print(f"    ✓ '{title}' — {len(df_out)} rows × {len(df_out.columns)} cols")


# ─────────────────────────────────────────────────────────────────────────────
#  UNIVERSE + SECTOR LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_us_universe():
    """
    Load S&P 500 universe from CSV.
    Detects column names flexibly (Symbol/Ticker, GICS Sector/Sector/Industry,
    Company Name/Security/Name).
    IMPORTANT: us_sp500list.csv must be sorted by market cap descending so that
    row ranges correctly separate Mega (0-49) / Large (50-199) / Mid (200-499).
    """
    for path in [STOCK_CSV,
                 os.path.join(SCRIPT_DIR,    "us_sp500list.csv"),
                 os.path.join(INDEX_DATA_DIR, "us_sp500list.csv")]:
        if os.path.exists(path):
            break
    else:
        print("  ❌ us_sp500list.csv not found"); sys.exit(1)

    df = pd.read_csv(path); df.columns = df.columns.str.strip()
    cm = {c.lower().replace(" ", "").replace("_", ""): c for c in df.columns}
    sc = next((cm[k] for k in ["symbol", "ticker", "sym"]         if k in cm), df.columns[0])
    ic = next((cm[k] for k in ["gicssector", "sector", "industry"] if k in cm), df.columns[1])
    nc = next((cm[k] for k in ["companyname", "security", "name", "company"] if k in cm),
              df.columns[2])
    df = df.rename(columns={sc: "Symbol", ic: "Industry", nc: "Company Name"})
    df["Symbol"]       = df["Symbol"].astype(str).str.strip()
    df["Industry"]     = df["Industry"].astype(str).str.strip()
    df["Company Name"] = df["Company Name"].astype(str).str.strip()
    df["Yahoo"]        = df["Symbol"]   # US tickers have no suffix
    df["Sector"]       = df["Industry"].map(US_INDUSTRY_TO_SECTOR).fillna("Technology")
    if MAX_STOCKS > 0:
        df = df.head(MAX_STOCKS)
    print(f"  ✅ Universe: {len(df)} stocks | {df['Sector'].nunique()} sectors "
          f"| {df['Industry'].nunique()} industries")
    return df


def fetch_us_sector_prices():
    """
    Fetch US sector prices via SPDR ETFs (XLK, XLF, XLV …).
    US sectors have direct Yahoo tickers — no constituent-avg fallback needed.
    """
    result = {}
    for sname, cfg in US_SECTORS.items():
        ysym = cfg.get("yahoo")
        if not ysym:
            continue
        try:
            raw = yf.download(ysym, period=f"{PERIOD_DAYS}d",
                              auto_adjust=True, progress=False)
            if not raw.empty and len(raw) >= 22:
                cl = raw["Close"]
                if isinstance(cl, pd.DataFrame): cl = cl.squeeze()
                result[sname] = _normalize(cl.dropna())
                print(f"    ✓ {sname:<22} {ysym}")
            else:
                print(f"    ✗ {sname:<22} (no data)")
        except Exception as e:
            print(f"    ✗ {sname:<22} {e}")
    print(f"  ✅ Sector ETFs: {len(result)}/{len(US_SECTORS)}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "═" * 68)
    print("  US MARKET — GOOGLE SHEETS EDITION  v5.4")
    # GitHub Actions runs UTC; label as ET so the dashboard shows the right timezone
    print(f"  {datetime.now().strftime('%d %b %Y  %H:%M ET')}")
    print(f"  Stocks:{MAX_STOCKS}  Patterns:{PATTERN_MAX}  Signals:{SIGNAL_MAX_STOCKS}")
    print("═" * 68 + "\n")
    t0 = time.time()

    # ── Connect ───────────────────────────────────────────────────────────────
    print("🔐 Connecting to Google Sheets …"); ss = gs_connect()

    # ── Universe ──────────────────────────────────────────────────────────────
    print("📂 Loading universe …"); universe = load_us_universe()

    # ── Index prices ──────────────────────────────────────────────────────────
    print(f"\n📡 Fetching index ({US_INDEX}) …")
    raw = yf.download(US_INDEX, period=f"{PERIOD_DAYS}d", auto_adjust=True, progress=False)
    if raw.empty:
        print("  ❌ Cannot fetch index"); return
    cl = raw["Close"]
    if isinstance(cl, pd.DataFrame): cl = cl.squeeze()
    index_prices = _normalize(cl.dropna())
    print(f"  ✅ Index: {len(index_prices)} days")

    # ── Sector prices ─────────────────────────────────────────────────────────
    print("\n📡 Sector ETFs …"); sector_prices = fetch_us_sector_prices()

    # ── Stock closes ──────────────────────────────────────────────────────────
    stock_syms = universe["Yahoo"].tolist()
    print(f"\n📡 Fetching {len(stock_syms)} stock closes …")
    price_data = fetch_close_batch(stock_syms, PERIOD_DAYS)
    print(f"  ✅ Stocks: {len(price_data.columns)} loaded")

    # ── OHLCV for patterns + signals ──────────────────────────────────────────
    ohlcv_dict = {}
    max_ohlcv  = max(PATTERN_MAX, SIGNAL_MAX_STOCKS if ENABLE_SIGNALS else 0)
    if max_ohlcv > 0:
        print(f"\n📡 Fetching OHLCV for {max_ohlcv} stocks …")
        cands = [s for s in stock_syms
                 if s in price_data.columns and len(price_data[s].dropna()) >= 60
                 ][:max_ohlcv]
        try:
            ohlcv_dict = fetch_ohlcv_with_cache(cands, days=PERIOD_DAYS)
        except Exception:
            ohlcv_dict = fetch_ohlcv_batch(cands, days=PERIOD_DAYS)
        print(f"  ✅ OHLCV: {len(ohlcv_dict)} stocks")

    # ── Chart patterns ────────────────────────────────────────────────────────
    patterns_by_sym = {}; patterns_list = []
    if ENABLE_PATTERNS:
        print("\n📐 Detecting chart patterns …")
        pat_dict = {k: v for k, v in ohlcv_dict.items() if len(v) >= 60}
        patterns_by_sym, patterns_list = run_pattern_detection(pat_dict)

    # ── Build all DataFrames ──────────────────────────────────────────────────
    # NOTE: run_time label is ET — GitHub Actions runs UTC so we label it
    #       explicitly. The strftime clock itself is UTC/server-local but
    #       marking it ET communicates the intended trading timezone to readers.
    run_time = datetime.now().strftime("%d %b %Y  %H:%M ET")

    print("\n📸 Market Snapshot …");   snap_df     = build_market_snapshot("US")
    print("🏭 Sector Strength …");    sec_str_df  = build_sector_strength(
                                          universe, price_data, index_prices, sector_prices)
    print("🔄 Sector Rotation …");    sec_rot_df  = build_sector_rotation(
                                          universe, price_data, index_prices)
    print("🏭 Industry Rotation …");  ind_rot_df  = build_industry_rotation(
                                          universe, price_data, index_prices)
    print("📊 Market Breadth …");     breadth_df  = build_market_breadth(
                                          price_data, index_prices,
                                          US_BREADTH_INDICES, INDEX_DATA_DIR,
                                          market="US")
    print("📈 Sector Performance …"); sec_perf_df = build_sector_performance(
                                          sector_prices, index_prices)
    print("📊 Stock Strength + Signals + Financials …")
    stock_df = build_stock_strength(
        universe, price_data, index_prices, sector_prices,
        patterns_by_sym, market="US",
        fetch_financials=FETCH_FINANCIALS,
        ohlcv_dict=ohlcv_dict if ENABLE_SIGNALS else {},
    )
    print("🏆 Top Picks – Buy …");    top_buy_df  = build_top_picks_buy(
                                          stock_df, sec_str_df, market="US")
    print("🔴 Top Picks – Sell …");   top_sell_df = build_top_picks_sell(
                                          stock_df, sec_str_df, market="US")
    print("📐 Chart Patterns …");     chart_df    = build_chart_patterns_df(
                                          patterns_list, stock_df, market="US")
    print("🎯 Trade Setups …");       trade_df    = build_trade_setups(
                                          stock_df, sec_str_df, market="US")
    print("📋 RS Sleeve Lists …");    sleeve_df   = build_rs_sleeve_list(
                                          stock_df, universe, INDEX_DATA_DIR,
                                          market="US", run_time=run_time,
                                          index_prices=index_prices,
                                          price_data=price_data,
                                          ohlcv_dict=ohlcv_dict)
    dashboard_df = build_dashboard_df(stock_df, sec_str_df, "US", run_time)

    # ── Write to Google Sheets ────────────────────────────────────────────────
    print("\n📊 Writing to Google Sheets …")
    write_dashboard_tab(ss, dashboard_df, "US")
    write_tab(ss, "📸 Market Snapshot",   snap_df,     "navy")
    write_tab(ss, "🏭 Sector Strength",    sec_str_df,  "teal")
    write_tab(ss, "🔄 Sector Rotation",    sec_rot_df,  "navy")
    write_tab(ss, "🏭 Industry Rotation",  ind_rot_df,  "navy")
    write_tab(ss, "📊 Market Breadth",     breadth_df,  "green")
    write_tab(ss, "📈 Sector Performance", sec_perf_df, "navy")
    write_tab(ss, "📊 Stock Strength",     stock_df,    "navy")
    write_tab(ss, "🏆 Top Picks - Buy",    top_buy_df,  "green")
    write_tab(ss, "🔴 Top Picks - Sell",   top_sell_df, "red")
    write_tab(ss, "📐 Chart Patterns",     chart_df,    "navy")
    write_tab(ss, "🎯 Trade Setups",       trade_df,    "navy")
    write_sleeve_tab(ss, sleeve_df, "US")

    # ── Console summary ───────────────────────────────────────────────────────
    elapsed = time.time() - t0
    print(f"\n{'═'*68}")
    print(f"  ✅  COMPLETE!  |  ⏱ {elapsed:.0f}s  |  🔗 {SHEET_URL}")
    if not stock_df.empty:
        b   = (stock_df["Signal"]  == "Buy").sum()
        s   = (stock_df["Signal"]  == "Sell").sum()
        sb  = (stock_df["Enhanced"]== "Strong Buy").sum()
        mst = (stock_df.get("MST_Signal",  pd.Series()) == "Buy").sum()
        lst = (stock_df.get("LST_Signal",  pd.Series()) == "Buy").sum()
        r30 = (stock_df.get("RS30_Signal", pd.Series()) == "Buy").sum()
        print(f"  ⭐ Strong Buy:{sb} | ✅ Buy:{b} | 🔴 Sell:{s}")
        print(f"  📅 MST Buy:{mst} | 📆 LST Buy:{lst} | 📊 RS30 Buy:{r30}")
    if not trade_df.empty:
        buys  = (trade_df["Action"] == "BUY").sum()
        sells = (trade_df["Action"] == "SELL").sum()
        print(f"  🎯 Trade Setups: {buys} BUY | {sells} SELL | {len(trade_df)-buys-sells} WAIT")
    if not sleeve_df.empty:
        data_rows = sleeve_df[
            ~sleeve_df["Rank"].astype(str).str.startswith("━") &
            (sleeve_df["Rank"].astype(str).str.strip() != "") &
            sleeve_df["Rank"].astype(str).str.match(r'^\d+$')
        ]
        print(f"  📋 RS Sleeves: {len(data_rows)} stocks across US_A/B/C")
    print("═" * 68)


if __name__ == "__main__":
    main()
