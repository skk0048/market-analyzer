"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  RS MARKET ANALYSIS SYSTEM v4.0 — USA — GOOGLE SHEETS EDITION             ║
║                                                                            ║
║  Identical analysis logic to the local Excel version.                     ║
║  Output: live Google Sheets (12 tabs, auto-formatted).                    ║
║                                                                            ║
║  SHEETS WRITTEN (12 tabs):                                                 ║
║   1. 📋 Dashboard          — Summary + Strong Buy list + TV watchlist      ║
║   2. 🏭 Sector RS          — Sector relative strength vs SPY               ║
║   3. 🏆 Top By Sector      — Best 10 stocks per sector                     ║
║   4. ⭐ Strong Buy         — Peer-filtered: all 5 conditions pass          ║
║   5. ✅ Buy Stocks         — RS Buy signals + RSI + SMA + patterns         ║
║   6. 🔴 Sell Stocks        — RS Sell signals                               ║
║   7. 📊 All Stocks RS      — Full S&P 500 universe                         ║
║   8. 📐 Chart Patterns     — VCP, Cup & Handle, Double Bottom, etc.        ║
║   9. 🔄 Sector Rotation    — RS%, RSI%, SMA%, Zone scores                  ║
║  10. 🏭 Industry Rotation  — Industry-level breadth metrics                ║
║  11. 📊 Market Breadth     — SPY/QQQ/DIA/IWM breadth                       ║
║  12. 📈 Sector Performance — QoQ/YoY sector returns                        ║
║                                                                            ║
║  STRONG BUY FILTER (v4.0):                                                 ║
║   Signal == Buy  AND                                                       ║
║   Stock return >= sector average (Beats_Sec)  AND                         ║
║   Stock return >= industry average (Beats_Ind)  AND                       ║
║   Sector RS vs SPY > 0 (Sec_Beats_SPY)  AND                               ║
║   Industry RS vs SPY > 0 (Ind_Beats_SPY)                                  ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
import os, warnings, time, sys, io
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta, date
from scipy.signal import argrelextrema
from dataclasses import dataclass
from typing import Optional, List, Dict

# Google Sheets output
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import tenacity

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
#  ❶  CONFIGURATION — EDIT HERE
# ══════════════════════════════════════════════════════════════════════════════

MARKET = "US"

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
BASE_DIR1 = os.path.join(BASE_DIR, "IndexData")

# ── Google Sheets authentication ─────────────────────────────────────────────
# Path to service account JSON — set env var GOOGLE_CREDENTIALS_PATH
# or place google_credentials.json next to this script
CREDENTIALS_PATH = (
    os.environ.get("GOOGLE_CREDENTIALS_PATH")
    or os.path.join(BASE_DIR, "google_credentials.json")
)

# Full URL of your Google Sheet — set env var GOOGLE_SHEET_URL
SHEET_URL = (
    os.environ.get("GOOGLE_SHEET_URL_USA")          # separate sheet for USA
    or os.environ.get("GOOGLE_SHEET_URL")            # fallback to same sheet as India
    or "https://docs.google.com/spreadsheets/d/YOUR_USA_SPREADSHEET_ID/edit"
)

GSCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Tab names written to Google Sheets
TABS = {
    "dashboard":        "📋 Dashboard",
    "sector_rs":        "🏭 Sector RS",
    "top_by_sector":    "🏆 Top By Sector",
    "strong_buy":       "⭐ Strong Buy",
    "buy_stocks":       "✅ Buy Stocks",
    "sell_stocks":      "🔴 Sell Stocks",
    "all_stocks":       "📊 All Stocks RS",
    "chart_patterns":   "📐 Chart Patterns",
    "sector_rotation":  "🔄 Sector Rotation",
    "industry_rotation":"🏭 Industry Rotation",
    "market_breadth":   "📊 Market Breadth",
    "sector_perf":      "📈 Sector Perf",
}

# Google Sheets colors (RGB 0.0–1.0)
GS = {
    "navy":     {"red": 0.051, "green": 0.129, "blue": 0.216},
    "teal":     {"red": 0.0,   "green": 0.537, "blue": 0.482},
    "green":    {"red": 0.106, "green": 0.365, "blue": 0.165},
    "red":      {"red": 0.835, "green": 0.153, "blue": 0.157},
    "white":    {"red": 1.0,   "green": 1.0,   "blue": 1.0  },
    "lt_green": {"red": 0.784, "green": 0.902, "blue": 0.788},
    "lt_red":   {"red": 1.0,   "green": 0.800, "blue": 0.800},
    "amber":    {"red": 1.0,   "green": 0.973, "blue": 0.769},
    "lt_grey":  {"red": 0.961, "green": 0.961, "blue": 0.961},
    "lt_blue":  {"red": 0.918, "green": 0.949, "blue": 1.000},
    "orange":   {"red": 1.0,   "green": 0.878, "blue": 0.706},
}

# ── RS Periods ────────────────────────────────────────────────────────────────
RS_PERIODS     = [22, 55, 120, 252]
SIGNAL_PERIODS = [22, 55]
HL_LOOKBACK    = 22

# ── Feature toggles ───────────────────────────────────────────────────────────
ENABLE_CHART_PATTERNS  = True     # Detect chart patterns (needs OHLCV download)
ENABLE_SECTOR_ROTATION = True     # Calculate sector rotation table
ENABLE_MARKET_BREADTH  = True     # Calculate market breadth
PATTERN_MAX_STOCKS     = 400      # Max stocks for pattern detection

# ── US Index & Sectors ────────────────────────────────────────────────────────
US_INDEX = "SPY"

US_SECTORS = {
    # Pure GICS sector ETFs only — broad market indices (QQQ, IWM) excluded
    # to keep sector RS analysis clean and non-overlapping
    "Industrials":      {"yahoo": "XLI",   "csv": "us_sector_industrials.csv"},
    "Financials":       {"yahoo": "XLF",   "csv": "us_sector_financials.csv"},
    "Healthcare":       {"yahoo": "XLV",   "csv": "us_sector_health_care.csv"},
    "ConsumerDisc":     {"yahoo": "XLY",   "csv": "us_sector_consumer_discretionary.csv"},
    "Technology":       {"yahoo": "XLK",   "csv": "us_sector_information_technology.csv"},
    "Utilities":        {"yahoo": "XLU",   "csv": "us_sector_utilities.csv"},
    "Materials":        {"yahoo": "XLB",   "csv": "us_sector_materials.csv"},
    "Energy":           {"yahoo": "XLE",   "csv": "us_sector_energy.csv"},
    "ConsumerStaples":  {"yahoo": "XLP",   "csv": "us_sector_consumer_staples.csv"},
    "CommServices":     {"yahoo": "XLC",   "csv": "us_sector_communication_services.csv"},
    "RealEstate":       {"yahoo": "XLRE",  "csv": "us_sector_real_estate.csv"},
}

US_INDUSTRY_TO_SECTOR = {
    "Industrials":                 "Industrials",
    "Financials":                  "Financials",
    "Health Care":                 "Healthcare",
    "Health Care Equipment":       "Healthcare",
    "Pharmaceuticals":             "Healthcare",
    "Biotechnology":               "Healthcare",
    "Consumer Discretionary":      "ConsumerDisc",
    "Information Technology":      "Technology",
    "Semiconductors":              "Technology",
    "Software":                    "Technology",
    "Utilities":                   "Utilities",
    "Materials":                   "Materials",
    "Chemicals":                   "Materials",
    "Energy":                      "Energy",
    "Consumer Staples":            "ConsumerStaples",
    "Communication Services":      "CommServices",
    "Media":                       "CommServices",
    "Real Estate":                 "RealEstate",
    "REITs":                       "RealEstate",
}

# US indices for Market Breadth
US_BREADTH_INDICES = {
    "S&P 500":       {"yahoo": "SPY", "csv": "us_sp500list.csv"},
    "Nasdaq 100":    {"yahoo": "QQQ", "csv": "us_nasdaq100list.csv"},
    "Dow Jones":     {"yahoo": "DIA", "csv": None},
    "Russell 2000":  {"yahoo": "IWM", "csv": None},
}

US_STOCK_FILE = os.path.join(BASE_DIR1, "us_sp500list.csv")

BATCH_SIZE  = 100
BATCH_DELAY = 1.0
MAX_STOCKS  = 500
PERIOD_DAYS = 300


# ══════════════════════════════════════════════════════════════════════════════
#  ❷  GOOGLE SHEETS CLIENT & WRITE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def gs_connect():
    """Authenticate with service account and return the target spreadsheet."""
    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(
            f"Google credentials not found: {CREDENTIALS_PATH}\n"
            "Set env var GOOGLE_CREDENTIALS_PATH or place google_credentials.json "
            "next to this script."
        )
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=GSCOPE)
    gc    = gspread.authorize(creds)
    ss    = gc.open_by_url(SHEET_URL)
    print(f"  ✅ Connected: \'{ss.title}\'")
    return ss


def _get_or_create_ws(ss, title, rows=2000, cols=50):
    """Get worksheet by title, creating it if it doesn't exist."""
    try:
        return ss.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type(
        (gspread.exceptions.APIError, ConnectionError)
    ),
    reraise=True,
)
def _api(func, *args, **kwargs):
    """Wrap every API call with exponential-backoff retry for quota errors."""
    return func(*args, **kwargs)


def write_tab(ss, tab_title, df, header_bg="navy", fmt_cols=None):
    """
    Clear and write a DataFrame to a Google Sheet tab.
    Creates the tab if it doesn't exist, applies header formatting.
    fmt_cols: optional dict {col_name: format_type} for column coloring
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        print(f"    ⚠ Skipping \'{tab_title}\' (empty)")
        return

    rows = max(len(df) + 10, 500)
    cols = max(len(df.columns) + 5, 30)
    ws   = _get_or_create_ws(ss, tab_title, rows=rows, cols=cols)

    clean = df.copy().replace([float("inf"), float("-inf")], "").fillna("")
    _api(ws.clear)
    time.sleep(0.5)
    _api(set_with_dataframe, ws, clean, resize=True)

    # Header formatting
    try:
        bg  = GS.get(header_bg, GS["navy"])
        col_end = gspread.utils.rowcol_to_a1(1, len(df.columns)).replace("1", "")
        _api(ws.format, f"A1:{col_end}1", {
            "backgroundColor": bg,
            "textFormat": {"foregroundColor": GS["white"], "bold": True, "fontSize": 10},
            "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
        })
        time.sleep(0.3)
        # Freeze row 1
        _api(ss.batch_update, {"requests": [{"updateSheetProperties": {
            "properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"
        }}]})
    except Exception:
        pass

    # Column formatting
    if fmt_cols and len(df) > 0:
        _apply_column_formats(ws, df, fmt_cols)

    time.sleep(0.8)
    print(f"    ✓ \'{tab_title}\' — {len(df)} rows × {len(df.columns)} cols")


def _apply_column_formats(ws, df, fmt_cols):
    """Apply green/red/amber backgrounds to specific columns."""
    cell_fmts = []
    for col_name, fmt_type in fmt_cols.items():
        if col_name not in df.columns:
            continue
        col_idx    = list(df.columns).index(col_name) + 1
        col_letter = gspread.utils.rowcol_to_a1(1, col_idx).replace("1", "")

        for row_i, val in enumerate(df[col_name], start=2):
            bg = _get_cell_bg(val, fmt_type)
            if bg:
                cell_fmts.append({
                    "range": f"{col_letter}{row_i}",
                    "format": {"backgroundColor": bg}
                })

    # Batch in chunks of 50
    for i in range(0, len(cell_fmts), 50):
        try:
            _api(ws.batch_format, cell_fmts[i:i+50])
            if i + 50 < len(cell_fmts):
                time.sleep(0.25)
        except Exception:
            pass


def _get_cell_bg(val, fmt_type):
    """Return a GS color dict for a cell value based on format type."""
    if fmt_type == "signal":
        v = str(val)
        if v in ("Strong Buy",):    return GS["teal"]
        elif v in ("Buy",):         return GS["lt_green"]
        elif v in ("Sell",):        return GS["lt_red"]
        elif v in ("NA",):          return GS["amber"]

    elif fmt_type == "positive":
        try:
            v = float(str(val).replace(",","").replace("%","") or 0)
            if v > 0:  return GS["lt_green"]
            elif v < 0: return GS["lt_red"]
        except (ValueError, TypeError):
            pass

    elif fmt_type == "positive_inv":  # inverted — smaller is better
        try:
            v = float(str(val).replace(",","").replace("%","") or 0)
            if v < 0: return GS["lt_green"]
            elif v > 0: return GS["lt_red"]
        except (ValueError, TypeError):
            pass

    elif fmt_type == "pct_bar":
        try:
            v = float(str(val).replace("%","") or 0)
            if v >= 60:   return GS["lt_green"]
            elif v >= 40: return GS["amber"]
            else:         return GS["lt_red"]
        except (ValueError, TypeError):
            pass

    elif fmt_type == "tick":
        if str(val) == "✓": return GS["lt_green"]
        elif str(val) == "✗": return GS["lt_red"]

    elif fmt_type == "sma_score":
        try:
            v = int(float(str(val) or 0))
            return {4: GS["lt_green"], 3: GS["lt_green"], 2: GS["amber"],
                    1: GS["orange"], 0: GS["lt_red"]}.get(v)
        except (ValueError, TypeError):
            pass

    elif fmt_type == "zone":
        v = str(val)
        if v == "Bullish": return GS["lt_green"]
        elif v == "Bearish": return GS["lt_red"]
        elif v == "Neutral": return GS["amber"]

    return None


# ══════════════════════════════════════════════════════════════════════════════
#  ❸  UNIVERSE & SECTOR CONSTITUENT LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_universe(market="US"):
    fpath = US_STOCK_FILE
    if not os.path.exists(fpath):
        fpath = os.path.join(BASE_DIR1, os.path.basename(fpath))
    df = pd.read_csv(fpath)
    df.columns = df.columns.str.strip()

    # Robust column detection — handles different CSV layouts
    col_map = {c.lower().replace(" ", ""): c for c in df.columns}
    sym_col  = next((col_map[k] for k in ["symbol", "ticker", "sym"] if k in col_map), df.columns[0])
    ind_col  = next((col_map[k] for k in ["industry", "sector", "gics sector"] if k in col_map), df.columns[1])
    name_col = next((col_map[k] for k in ["companyname", "name", "security", "company"] if k in col_map), df.columns[2])

    df = df.rename(columns={sym_col: "Symbol", ind_col: "Industry", name_col: "Company Name"})
    df["Symbol"]       = df["Symbol"].astype(str).str.strip()
    df["Industry"]     = df["Industry"].astype(str).str.strip()
    df["Company Name"] = df["Company Name"].astype(str).str.strip()
    df["Yahoo"]        = df["Symbol"]    # US tickers need no suffix
    df["Sector"]       = df["Industry"].map(US_INDUSTRY_TO_SECTOR).fillna("Technology")
    if MAX_STOCKS > 0:
        df = df.head(MAX_STOCKS)
    print(f"  ✅ Universe: {len(df)} stocks | Sectors: {df['Sector'].nunique()}")
    return df

def load_index_constituents(csv_name):
    """Load a named constituent CSV → list of symbols."""
    for base in [BASE_DIR, BASE_DIR1]:
        path = os.path.join(base, csv_name)
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                df.columns = df.columns.str.strip()
                syms = df["Symbol"].str.strip().tolist()
                return syms
            except Exception:
                pass
    return []

def load_sector_constituents(market="US"):
    sectors = US_SECTORS
    result = {}
    for sname, cfg in sectors.items():
        csvf = cfg.get("csv")
        if not csvf: continue
        syms = load_index_constituents(csvf)
        if syms:
            result[sname] = syms
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  ❹  PRICE DATA FETCHING  (Close + OHLCV)
# ══════════════════════════════════════════════════════════════════════════════

def _strip_tz(idx):
    try:
        if idx.tzinfo is not None or (hasattr(idx, "tz") and idx.tz is not None):
            return idx.tz_localize(None)
        return idx
    except Exception:
        try:
            return idx.tz_convert(None)
        except Exception:
            return idx

def normalize_series(s):
    """Always returns a plain 1-D pd.Series with timezone-naive date index."""
    try:
        if isinstance(s, pd.DataFrame):
            s = s.squeeze()
        idx = _strip_tz(s.index)
        idx = idx.normalize()
        s2  = pd.Series(s.values, index=idx)
        s2  = s2[~s2.index.duplicated(keep="last")]
        return s2.sort_index()
    except Exception:
        return s

def fetch_prices(symbols, days=PERIOD_DAYS):
    """Fetch Close prices only (fast, for RS calculations)."""
    end      = datetime.today() + timedelta(days=1)
    start    = end - timedelta(days=days + 1)
    all_data = {}
    failed   = []
    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i: i + BATCH_SIZE]
        try:
            raw = yf.download(
                tickers=batch, start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                auto_adjust=True, progress=False, threads=True,
            )
            if isinstance(raw.columns, pd.MultiIndex):
                close = raw["Close"] if "Close" in raw.columns.get_level_values(0) else pd.DataFrame()
            else:
                close = raw[["Close"]] if "Close" in raw.columns else pd.DataFrame()
                if len(batch) == 1 and not close.empty:
                    close.columns = [batch[0]]
            for sym in batch:
                if sym in close.columns:
                    col = close[sym]
                    if isinstance(col, pd.DataFrame): col = col.squeeze()
                    s = normalize_series(col.dropna())
                    if len(s) >= 22:
                        all_data[sym] = s
                    else:
                        failed.append(sym)
                else:
                    failed.append(sym)
        except Exception as e:
            print(f"    ⚠ Batch error: {e}")
            failed.extend(batch)
        if i + BATCH_SIZE < len(symbols):
            time.sleep(BATCH_DELAY)
    print(f"    ✅ Prices: {len(all_data)}/{len(symbols)}" +
          (f" | Failed: {len(failed)}" if failed else ""))
    if not all_data: return pd.DataFrame()
    return pd.DataFrame(all_data).sort_index()

def fetch_ohlcv_batch(symbols, days=PERIOD_DAYS):
    """Fetch full OHLCV for chart pattern detection."""
    end   = datetime.today() + timedelta(days=1)
    start = end - timedelta(days=days + 1)
    result = {}
    batch_size = 50
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i: i + batch_size]
        try:
            raw = yf.download(
                tickers=batch, start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                auto_adjust=True, progress=False, threads=True,
            )
            if raw.empty: continue
            if isinstance(raw.columns, pd.MultiIndex):
                price_cols = raw.columns.get_level_values(0).unique().tolist()
                for sym in batch:
                    frames = {}
                    for pc in ["Open", "High", "Low", "Close", "Volume"]:
                        if pc in price_cols and sym in raw[pc].columns:
                            cd = raw[pc][sym]
                            if isinstance(cd, pd.DataFrame): cd = cd.squeeze()
                            frames[pc] = normalize_series(cd.dropna())
                    if "Close" in frames and len(frames["Close"]) >= 60:
                        df_sym = pd.DataFrame(frames)
                        df_sym.dropna(subset=["Close", "High", "Low"], inplace=True)
                        if len(df_sym) >= 60:
                            result[sym] = df_sym
            else:
                sym = batch[0]
                if "Close" in raw.columns and len(raw) >= 60:
                    raw_clean = raw.rename(columns=str.capitalize)
                    raw_clean.index = normalize_series(raw_clean["Close"]).index
                    result[sym] = raw_clean
        except Exception as e:
            print(f"    ⚠ OHLCV batch error: {e}")
        if i + batch_size < len(symbols):
            time.sleep(BATCH_DELAY)
    print(f"    ✅ OHLCV data: {len(result)}/{len(symbols)} symbols")
    return result

def fetch_sector_indices(market="US"):
    sectors = US_SECTORS
    result  = {}
    const   = load_sector_constituents(market)
    for sname, cfg in sectors.items():
        yahoo_sym = cfg.get("yahoo")
        if yahoo_sym:
            try:
                raw = yf.download(yahoo_sym, period=f"{PERIOD_DAYS}d",
                                  auto_adjust=True, progress=False)
                if len(raw) >= 22:
                    c = raw["Close"]
                    if isinstance(c, pd.DataFrame): c = c.squeeze()
                    result[sname] = normalize_series(c.dropna())
                    print(f"    ✓ {sname:<22} {yahoo_sym}")
                    continue
            except Exception:
                pass
        if sname in const and const[sname]:
            batch = const[sname][:30]
            try:
                raw = yf.download(batch, period=f"{PERIOD_DAYS}d",
                                  auto_adjust=True, progress=False)
                if isinstance(raw.columns, pd.MultiIndex):
                    close = raw["Close"]
                else:
                    close = raw[["Close"]]
                    close.columns = [batch[0]] if len(batch) == 1 else close.columns
                close = close.dropna(how="all")
                if len(close) >= 22:
                    norm   = close / close.iloc[0] * 1000
                    result[sname] = normalize_series(norm.mean(axis=1))
                    print(f"    ✓ {sname:<22} (constituents)")
                    continue
            except Exception:
                pass
        print(f"    ✗ {sname:<22} not available")
    print(f"  ✅ Sector indices: {len(result)}/{len(sectors)}")
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  ❺  RS CALCULATIONS
# ══════════════════════════════════════════════════════════════════════════════

def calc_rs(stock, benchmark, period):
    try:
        s = normalize_series(stock.dropna())
        b = normalize_series(benchmark.dropna())
        common = s.index.intersection(b.index)
        if len(common) < period + 1:
            return np.nan
        s = s.loc[common]
        b = b.loc[common]
        s_cur  = float(s.iloc[-1])
        s_past = float(s.iloc[-(period + 1)])
        b_cur  = float(b.iloc[-1])
        b_past = float(b.iloc[-(period + 1)])
        if s_past == 0 or b_past == 0 or b_cur == 0:
            return np.nan
        return (s_cur / s_past) / (b_cur / b_past) - 1
    except Exception:
        return np.nan

def calc_hl_days(stock, lookback):
    try:
        s      = normalize_series(stock.dropna())
        if len(s) < lookback: return np.nan, np.nan
        recent = s.iloc[-lookback:]
        h_idx  = recent.idxmax()
        l_idx  = recent.idxmin()
        last   = s.index[-1]
        return int((last - h_idx).days), int((last - l_idx).days)
    except Exception:
        return np.nan, np.nan

def calc_group_peer_metrics(universe, price_data, index_prices, periods=None):
    """
    Peer-average returns and RS vs SPY for each Sector and Industry group.
    """
    if periods is None:
        periods = SIGNAL_PERIODS   # [22, 55]

    idx = normalize_series(index_prices.dropna())
    stock_rets = {}
    for sym in universe["Yahoo"].tolist():
        if sym not in price_data.columns: continue
        prices = price_data[sym].dropna()
        d = {}
        for p in periods:
            if len(prices) >= p + 1:
                cur  = float(prices.iloc[-1])
                past = float(prices.iloc[-(p + 1)])
                d[p] = (cur / past - 1) * 100 if past != 0 else np.nan
            else:
                d[p] = np.nan
        stock_rets[sym] = d

    spy_ret = {}
    for p in periods:
        if len(idx) >= p + 1:
            cur  = float(idx.iloc[-1])
            past = float(idx.iloc[-(p + 1)])
            spy_ret[p] = (cur / past - 1) * 100 if past != 0 else np.nan
        else:
            spy_ret[p] = np.nan

    def _group_metrics(group_col):
        ret_map = {}
        rs_map  = {}
        for grp in universe[group_col].unique():
            if not isinstance(grp, str) or not grp.strip(): continue
            syms = universe[universe[group_col] == grp]["Yahoo"].tolist()
            ret_map[grp] = {}
            rs_map[grp]  = {}
            for p in periods:
                vals = [stock_rets[s][p] for s in syms if s in stock_rets and not np.isnan(stock_rets[s].get(p, np.nan))]
                if vals:
                    avg_ret = float(np.mean(vals))
                    ret_map[grp][p] = round(avg_ret, 2)
                    n_ret = spy_ret.get(p, np.nan)
                    if not np.isnan(n_ret):
                        rs_map[grp][p] = round((1 + avg_ret / 100) / (1 + n_ret / 100) - 1, 4)
                    else:
                        rs_map[grp][p] = np.nan
                else:
                    ret_map[grp][p] = np.nan
                    rs_map[grp][p]  = np.nan
        return ret_map, rs_map

    sector_ret,   sector_rs_spy   = _group_metrics("Sector")
    industry_ret, industry_rs_spy = _group_metrics("Industry")
    print(f"    Peer metrics: {len(sector_ret)} sectors | {len(industry_ret)} industries")
    return sector_ret, industry_ret, sector_rs_spy, industry_rs_spy

def pct_from_high(stock, lookback=252):
    try:
        recent = stock.iloc[-lookback:]
        return (stock.iloc[-1] / recent.max() - 1) * 100
    except Exception:
        return np.nan


# ══════════════════════════════════════════════════════════════════════════════
#  ❻  TECHNICAL INDICATORS  (RSI, SMA)
# ══════════════════════════════════════════════════════════════════════════════

def calc_rsi(series, period=14):
    try:
        delta = series.diff().dropna()
        gain  = delta.clip(lower=0)
        loss  = (-delta).clip(lower=0)
        avg_g = gain.rolling(period).mean().iloc[-1]
        avg_l = loss.rolling(period).mean().iloc[-1]
        if avg_l == 0: return 100.0
        return round(100 - (100 / (1 + avg_g / avg_l)), 1)
    except Exception:
        return np.nan

def calc_sma_latest(series, period):
    try:
        if len(series) < period: return np.nan
        return float(series.dropna().iloc[-period:].mean())
    except Exception:
        return np.nan

def get_sma_signals(prices):
    cur = float(prices.iloc[-1]) if len(prices) > 0 else np.nan
    sma20  = calc_sma_latest(prices, 20)
    sma50  = calc_sma_latest(prices, 50)
    sma100 = calc_sma_latest(prices, 100)
    sma200 = calc_sma_latest(prices, 200)
    rsi    = calc_rsi(prices, 14)

    def above(price, sma):
        if np.isnan(price) or sma is None or np.isnan(sma): return False
        return price > sma

    sma_score = sum([
        1 if above(cur, sma20)  else 0,
        1 if above(cur, sma50)  else 0,
        1 if above(cur, sma100) else 0,
        1 if above(cur, sma200) else 0,
    ])

    return {
        "RSI_14":      rsi,
        "SMA20":       round(sma20,  2) if sma20  and not np.isnan(sma20)  else np.nan,
        "SMA50":       round(sma50,  2) if sma50  and not np.isnan(sma50)  else np.nan,
        "SMA100":      round(sma100, 2) if sma100 and not np.isnan(sma100) else np.nan,
        "SMA200":      round(sma200, 2) if sma200 and not np.isnan(sma200) else np.nan,
        "Abv_SMA20":   "✓" if above(cur, sma20)  else "✗",
        "Abv_SMA50":   "✓" if above(cur, sma50)  else "✗",
        "Abv_SMA100":  "✓" if above(cur, sma100) else "✗",
        "Abv_SMA200":  "✓" if above(cur, sma200) else "✗",
        "SMA_Score":   sma_score,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  ❼  CHART PATTERN DETECTOR
# ══════════════════════════════════════════════════════════════════════════════

CFG_PAT = {
    "pivot_order":      5,
    "tolerance":        0.03,
    "min_pattern_bars": 10,
    "max_pattern_bars": 120,
    "flag_pole_pct":    0.07,
    "cup_depth_min":    0.10,
    "cup_depth_max":    0.50,
    "lookback_days":    180,
}

@dataclass
class PatternSignal:
    symbol:      str
    pattern:     str
    direction:   str
    end_date:    str
    entry_price: float
    stop_loss:   float
    target:      float
    risk_reward: float
    confidence:  str
    win_rate_est:str
    notes:       str = ""

def _near_eq(a, b, tol=0.03):
    return abs(a - b) / max(abs(a), abs(b), 1e-9) <= tol

def _avg_vol(df, start, end):
    if "Volume" not in df.columns: return 1.0
    return float(df["Volume"].iloc[max(0, start): end].mean()) or 1.0

class PatternDetector:
    def __init__(self, df, symbol):
        lb = CFG_PAT["lookback_days"]
        self.df  = df.tail(lb).copy().reset_index(drop=False)
        self.sym = symbol
        self.signals: List[PatternSignal] = []
        close_arr = self.df["Close"].values
        high_arr  = self.df["High"].values if "High" in self.df.columns else close_arr
        low_arr   = self.df["Low"].values  if "Low"  in self.df.columns else close_arr
        self.highs = argrelextrema(high_arr, np.greater_equal, order=CFG_PAT["pivot_order"])[0]
        self.lows  = argrelextrema(low_arr,  np.less_equal,  order=CFG_PAT["pivot_order"])[0]

    def _date(self, idx):
        col = "Date" if "Date" in self.df.columns else self.df.columns[0]
        return str(self.df[col].iloc[min(idx, len(self.df) - 1)])[:10]
    def _close(self, idx): return float(self.df["Close"].iloc[idx])
    def _high(self, idx):
        col = "High" if "High" in self.df.columns else "Close"
        return float(self.df[col].iloc[idx])
    def _low(self, idx):
        col = "Low" if "Low" in self.df.columns else "Close"
        return float(self.df[col].iloc[idx])
    def _add(self, pat, direction, ei, entry, sl, target, conf, wr, notes=""):
        rr = abs(target - entry) / max(abs(entry - sl), 1e-9)
        self.signals.append(PatternSignal(
            symbol=self.sym, pattern=pat, direction=direction, end_date=self._date(ei),
            entry_price=round(entry, 2), stop_loss=round(sl, 2), target=round(target, 2),
            risk_reward=round(rr, 2), confidence=conf, win_rate_est=wr, notes=notes,
        ))

    def detect_double_bottom(self):
        for i in range(len(self.lows) - 1):
            l1, l2 = self.lows[i], self.lows[i + 1]
            span   = l2 - l1
            if not (CFG_PAT["min_pattern_bars"] <= span <= CFG_PAT["max_pattern_bars"]): continue
            p1, p2 = self._low(l1), self._low(l2)
            if not _near_eq(p1, p2): continue
            neck  = float(self.df["High"].iloc[l1:l2 + 1].max() if "High" in self.df.columns else self.df["Close"].iloc[l1:l2 + 1].max())
            conf  = "HIGH" if _avg_vol(self.df, l2, min(l2 + 10, len(self.df))) > _avg_vol(self.df, l1 - 10, l1) else "MEDIUM"
            self._add("Double Bottom", "BULLISH", l2, neck * 1.005, min(p1, p2) * 0.99, neck + (neck - min(p1, p2)), conf, "~65%", f"Neckline≈{neck:.0f}")

    def detect_double_top(self):
        for i in range(len(self.highs) - 1):
            h1, h2 = self.highs[i], self.highs[i + 1]
            span   = h2 - h1
            if not (CFG_PAT["min_pattern_bars"] <= span <= CFG_PAT["max_pattern_bars"]): continue
            p1, p2 = self._high(h1), self._high(h2)
            if not _near_eq(p1, p2): continue
            neck  = float(self.df["Low"].iloc[h1:h2 + 1].min() if "Low" in self.df.columns else self.df["Close"].iloc[h1:h2 + 1].min())
            conf  = "HIGH" if _avg_vol(self.df, h1 - 5, h1) > _avg_vol(self.df, h2 - 5, h2) else "MEDIUM"
            self._add("Double Top", "BEARISH", h2, neck * 0.995, max(p1, p2) * 1.01, neck - (max(p1, p2) - neck), conf, "~65%", f"Neckline≈{neck:.0f}")

    def detect_vcp(self):
        window = 60
        for end in range(window, len(self.df)):
            start = end - window
            seg   = self.df.iloc[start: end + 1]
            q     = max(1, len(seg) // 4)
            high_col = "High" if "High" in seg.columns else "Close"
            low_col  = "Low"  if "Low"  in seg.columns else "Close"
            swings = []
            for qi in range(4):
                s = seg.iloc[qi * q: (qi + 1) * q]
                if len(s) > 0: swings.append(float(s[high_col].max()) - float(s[low_col].min()))
            if len(swings) < 4: continue
            if not all(swings[j] > swings[j + 1] for j in range(3)): continue
            res     = float(seg[high_col].max())
            base_lo = float(seg[low_col].min())
            sl_base = float(seg[low_col].iloc[-q:].min())
            self._add("VCP", "BULLISH", end, res * 1.005, sl_base * 0.99, res * 1.005 + (res - base_lo) * 0.75, "HIGH", "~70%", "Vol Dry-up + Contractions")

    def run(self):
        self.detect_double_bottom()
        self.detect_double_top()
        self.detect_vcp()
        seen = {}
        for s in self.signals:
            if s.pattern not in seen or s.end_date > seen[s.pattern].end_date:
                seen[s.pattern] = s
        return list(seen.values())

def run_pattern_detection(ohlcv_dict, min_rr=1.5):
    patterns_by_sym = {}
    patterns_list   = []
    n = len(ohlcv_dict)
    print(f"  Detecting patterns in {n} stocks …")
    for i, (sym, df) in enumerate(ohlcv_dict.items()):
        if len(df) < 60: continue
        if "Date" not in df.columns:
            df = df.reset_index()
            if df.columns[0] != "Date": df = df.rename(columns={df.columns[0]: "Date"})
        try:
            det     = PatternDetector(df, sym)
            signals = det.run()
            signals = [s for s in signals if s.risk_reward >= min_rr]
            if signals:
                patterns_by_sym[sym] = signals
                patterns_list.extend(signals)
        except Exception:
            pass
        if (i + 1) % 100 == 0: print(f"    … {i + 1}/{n} done")
    bull = sum(1 for s in patterns_list if s.direction == "BULLISH")
    bear = sum(1 for s in patterns_list if s.direction == "BEARISH")
    print(f"  ✅ Patterns: {len(patterns_list)} total | {bull} Bullish | {bear} Bearish | {len(patterns_by_sym)} stocks")
    return patterns_by_sym, patterns_list


# ══════════════════════════════════════════════════════════════════════════════
#  ❽  SECTOR ROTATION ANALYSIS  (StockEdge-style)
# ══════════════════════════════════════════════════════════════════════════════

def _get_price_zone(price, high_n, low_n):
    rng = high_n - low_n
    if rng <= 0: return "Mid"
    pos = (price - low_n) / rng
    if pos >= 0.67: return "High"
    elif pos >= 0.33: return "Mid"
    else: return "Low"

def _zone_label(score):
    if score >= 60: return "Bullish"
    elif score >= 40: return "Neutral"
    else: return "Bearish"

def calc_rotation_row(group_stocks, price_data, index_prices, group_name):
    n_stocks    = len(group_stocks)
    rs55_above  = 0
    rsi_above   = 0
    sma20_above = 0; sma50_above = 0; sma100_above = 0
    zone_1m = {"High": 0, "Mid": 0, "Low": 0}
    zone_3m = {"High": 0, "Mid": 0, "Low": 0}
    zone_6m = {"High": 0, "Mid": 0, "Low": 0}
    valid = 0

    for sym in group_stocks:
        if sym not in price_data.columns: continue
        prices = price_data[sym].dropna()
        if len(prices) < 55: continue
        valid += 1
        cur = float(prices.iloc[-1])

        rs55 = calc_rs(prices, index_prices, 55)
        if rs55 is not None and not np.isnan(rs55) and rs55 > 0: rs55_above += 1

        rsi = calc_rsi(prices, 14)
        if rsi is not None and not np.isnan(rsi) and rsi > 50: rsi_above += 1

        sma20  = calc_sma_latest(prices, 20)
        sma50  = calc_sma_latest(prices, 50)
        sma100 = calc_sma_latest(prices, 100)
        if sma20  and not np.isnan(sma20)  and cur > sma20:  sma20_above += 1
        if sma50  and not np.isnan(sma50)  and cur > sma50:  sma50_above += 1
        if sma100 and not np.isnan(sma100) and cur > sma100: sma100_above += 1

        if len(prices) >= 22:
            zone_1m[_get_price_zone(cur, float(prices.iloc[-22:].max()), float(prices.iloc[-22:].min()))] += 1
        else: zone_1m["Low"] += 1
        if len(prices) >= 55:
            zone_3m[_get_price_zone(cur, float(prices.iloc[-55:].max()), float(prices.iloc[-55:].min()))] += 1
        else: zone_3m["Low"] += 1
        if len(prices) >= 120:
            zone_6m[_get_price_zone(cur, float(prices.iloc[-120:].max()), float(prices.iloc[-120:].min()))] += 1
        else: zone_6m["Low"] += 1

    if valid == 0: return None

    def pct(n): return round(n / valid * 100)
    def score(z): return round((z["High"] * 2 + z["Mid"]) / max(valid * 2, 1) * 100)

    s1m = score(zone_1m); s3m = score(zone_3m); s6m = score(zone_6m)
    return {
        "Name":              group_name,
        "Stocks":            n_stocks,
        "Valid_Data":        valid,
        "RS55_Above%":       pct(rs55_above),
        "RSI50_Above%":      pct(rsi_above),
        "SMA20_Above%":      pct(sma20_above),
        "SMA50_Above%":      pct(sma50_above),
        "SMA100_Above%":     pct(sma100_above),
        "Stk_RS55":          rs55_above,
        "Stk_RSI50":         rsi_above,
        "Stk_SMA20":         sma20_above,
        "Stk_SMA50":         sma50_above,
        "Stk_SMA100":        sma100_above,
        "1M_LowZone":        zone_1m["Low"], "1M_MidZone": zone_1m["Mid"], "1M_HighZone": zone_1m["High"],
        "3M_LowZone":        zone_3m["Low"], "3M_MidZone": zone_3m["Mid"], "3M_HighZone": zone_3m["High"],
        "6M_LowZone":        zone_6m["Low"], "6M_MidZone": zone_6m["Mid"], "6M_HighZone": zone_6m["High"],
        "1M_Score":          s1m, "1M_Zone": _zone_label(s1m),
        "3M_Score":          s3m, "3M_Zone": _zone_label(s3m),
        "6M_Score":          s6m, "6M_Zone": _zone_label(s6m),
    }

def analyse_sector_rotation(universe_df, price_data, index_prices):
    print("  Computing sector rotation …")
    rows = []
    sectors = universe_df["Sector"].unique()
    for sec in sorted(sectors):
        syms = universe_df[universe_df["Sector"] == sec]["Yahoo"].tolist()
        row  = calc_rotation_row(syms, price_data, index_prices, sec)
        if row: rows.append(row)
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("RS55_Above%", ascending=False).reset_index(drop=True)
    df.insert(0, "Rank", df.index + 1)
    print(f"  ✅ Sector rotation: {len(df)} sectors")
    return df

def analyse_industry_rotation(universe_df, price_data, index_prices):
    print("  Computing industry rotation …")
    rows = []
    industries = universe_df["Industry"].unique()
    for ind in sorted(industries):
        syms = universe_df[universe_df["Industry"] == ind]["Yahoo"].tolist()
        sec_vals = universe_df[universe_df["Industry"] == ind]["Sector"].unique()
        parent_sec = sec_vals[0] if len(sec_vals) > 0 else "—"
        row = calc_rotation_row(syms, price_data, index_prices, ind)
        if row:
            row["Sector"] = parent_sec
            rows.append(row)
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("RS55_Above%", ascending=False).reset_index(drop=True)
    df.insert(0, "Rank", df.index + 1)
    print(f"  ✅ Industry rotation: {len(df)} industries")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  ❾  MARKET BREADTH
# ══════════════════════════════════════════════════════════════════════════════

def analyse_market_breadth(price_data, index_prices, universe_df):
    print("  Computing market breadth …")
    rows = []
    today_str = datetime.now().strftime("%d-%b-%Y")

    for idx_name, cfg in US_BREADTH_INDICES.items():
        csv_file = cfg.get("csv")
        yahoo    = cfg.get("yahoo")
        syms     = load_index_constituents(csv_file) if csv_file else []
        if not syms: syms = universe_df["Yahoo"].tolist()

        idx_price = np.nan
        if yahoo:
            try:
                raw = yf.download(yahoo, period="5d", auto_adjust=True, progress=False)
                if not raw.empty:
                    cl = raw["Close"]
                    if isinstance(cl, pd.DataFrame): cl = cl.squeeze()
                    idx_price = round(float(cl.dropna().iloc[-1]), 2)
            except Exception:
                pass

        row = calc_rotation_row(syms, price_data, index_prices, idx_name)
        if row:
            row["Index_Price"]     = idx_price
            row["Date"]            = today_str
            sma200_cnt = 0; valid_cnt  = 0
            for sym in syms:
                if sym not in price_data.columns: continue
                pr = price_data[sym].dropna()
                if len(pr) < 200: continue
                valid_cnt += 1
                cur    = float(pr.iloc[-1])
                sma200 = calc_sma_latest(pr, 200)
                if sma200 and not np.isnan(sma200) and cur > sma200: sma200_cnt += 1
            row["SMA200_Above%"] = round(sma200_cnt / max(valid_cnt, 1) * 100)
            row["Stk_SMA200"]    = sma200_cnt
            rows.append(row)

    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    print(f"  ✅ Market breadth: {len(df)} indices")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  ❿  SECTOR PERFORMANCE (QoQ & YoY)
# ══════════════════════════════════════════════════════════════════════════════

def analyse_sector_performance(sector_prices, index_prices):
    rows = []
    periods = {"1M (22d)": 22, "3M (66d)": 66, "6M (132d)": 132, "9M (200d)": 200, "12M (252d)": 252, "YTD": None}
    for sec_name, prices in sector_prices.items():
        if len(prices) < 22: continue
        row = {"Sector": sec_name}
        if isinstance(prices, pd.DataFrame): prices = prices.squeeze()
        for label, days in periods.items():
            if label == "YTD":
                try:
                    jan1 = pd.Timestamp(f"{datetime.now().year}-01-01")
                    past = prices[prices.index <= jan1]
                    if len(past) > 0:
                        row["YTD%"] = round((float(prices.iloc[-1]) / float(past.iloc[-1]) - 1) * 100, 2)
                    else: row["YTD%"] = np.nan
                except Exception: row["YTD%"] = np.nan
                continue
            if len(prices) >= days + 1:
                cur_v  = float(prices.iloc[-1])
                past_v = float(prices.iloc[-(days + 1)])
                row[f"{label}%"] = round((cur_v / past_v - 1) * 100, 2) if past_v != 0 else np.nan
            else:
                row[f"{label}%"] = np.nan

        for label, days in [("RS_1M%", 22), ("RS_3M%", 55), ("RS_6M%", 120), ("RS_12M%", 252)]:
            rs = calc_rs(prices, index_prices, days)
            row[label] = round(rs * 100, 2) if rs is not None and not np.isnan(rs) else np.nan
        rows.append(row)

    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    sort_col = "3M (66d)%"
    if sort_col in df.columns and df[sort_col].notna().any():
        df = df.sort_values(sort_col, ascending=False, na_position="last")
    df = df.reset_index(drop=True)
    df.insert(0, "Rank", df.index + 1)
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  ⓫  ENHANCED STOCK ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

def analyse_sectors(index_prices, sector_prices, market="US"):
    rows   = []
    periods = [2, 5, 22, 34, 55, 66, 89, 100, 120, 150, 180, 200, 252]
    for sname, s_prices in sector_prices.items():
        row = {"Sector": sname}
        for p in periods:
            rs = calc_rs(s_prices, index_prices, p)
            row[f"RS_{p}d"] = round(rs * 100, 2) if rs == rs else np.nan
        r22 = row.get("RS_22d", np.nan)
        r55 = row.get("RS_55d", np.nan)
        if r22 == r22 and r55 == r55:
            if   r22 > 0 and r55 > 0: row["Signal"] = "Buy"
            elif r22 < 0 and r55 < 0: row["Signal"] = "Sell"
            else:                     row["Signal"] = "NA"
        else:
            row["Signal"] = "NA"
        h, l = calc_hl_days(s_prices, HL_LOOKBACK)
        row["H_Day"] = h
        row["L_Day"] = l
        row["RSI_14"] = calc_rsi(s_prices, 14)
        rows.append(row)
    df = pd.DataFrame(rows).sort_values("RS_55d", ascending=False).reset_index(drop=True)
    df.insert(0, "Rank", df.index + 1)
    return df

def analyse_stocks(universe, price_data, index_prices, sector_prices,
                   patterns_by_sym=None, market="US", sector_df=None):
    rows  = []
    total = len(universe)
    p1, p2 = SIGNAL_PERIODS[0], SIGNAL_PERIODS[1]

    if patterns_by_sym is None: patterns_by_sym = {}

    print("    Computing peer-group returns & RS …")
    sector_ret, industry_ret, sector_rs_spy, industry_rs_spy = \
        calc_group_peer_metrics(universe, price_data, index_prices)

    sec_idx_rs = {}
    if sector_df is not None and not sector_df.empty:
        for _, row in sector_df.iterrows():
            sname = row.get("Sector", "")
            if not sname: continue
            sec_idx_rs[sname] = {}
            for p in SIGNAL_PERIODS:
                val = row.get(f"RS_{p}d", np.nan)
                sec_idx_rs[sname][p] = float(val) / 100.0 if (val == val and val is not None) else np.nan

    def _best_sec_rs(sector_name, period):
        v = sec_idx_rs.get(sector_name, {}).get(period, np.nan)
        return v if v == v else sector_rs_spy.get(sector_name, {}).get(period, np.nan)

    def _tick(flag):
        if flag is True:  return "✓"
        if flag is False: return "✗"
        return "—"

    for i, (_, stock_row) in enumerate(universe.iterrows()):
        sym      = stock_row["Yahoo"]
        orig_sym = stock_row["Symbol"]
        name     = stock_row.get("Company Name", sym)
        industry = stock_row.get("Industry", "")
        sector   = stock_row.get("Sector", "")

        if sym not in price_data.columns: continue
        prices = price_data[sym].dropna()
        if len(prices) < 22: continue

        cur_price = prices.iloc[-1]
        s_prices  = sector_prices.get(sector)

        rs_idx = {p: calc_rs(prices, index_prices, p) for p in RS_PERIODS}
        rs_sec = {p: calc_rs(prices, s_prices, p) if s_prices is not None else np.nan for p in SIGNAL_PERIODS}

        r_i1 = rs_idx.get(p1, np.nan)
        r_i2 = rs_idx.get(p2, np.nan)
        r_s1 = rs_sec.get(p1, np.nan)
        r_s2 = rs_sec.get(p2, np.nan)
        valid_idx = (r_i1 == r_i1 and r_i2 == r_i2)
        valid_sec = (r_s1 == r_s1 and r_s2 == r_s2)

        if valid_idx and valid_sec:
            if   r_i1 > 0 and r_i2 > 0 and r_s1 > 0 and r_s2 > 0: signal = "Buy"
            elif r_i1 < 0 and r_i2 < 0 and r_s1 < 0 and r_s2 < 0: signal = "Sell"
            else: signal = "NA"
        elif valid_idx:
            if   r_i1 > 0 and r_i2 > 0: signal = "Buy"
            elif r_i1 < 0 and r_i2 < 0: signal = "Sell"
            else: signal = "NA"
        else: signal = "NA"

        h_day, l_day = calc_hl_days(prices, HL_LOOKBACK)
        from_52w     = pct_from_high(prices, 252)
        tech         = get_sma_signals(prices)

        pat_list  = patterns_by_sym.get(sym, [])
        bull_pats = [s.pattern for s in pat_list if s.direction == "BULLISH"]
        bear_pats = [s.pattern for s in pat_list if s.direction == "BEARISH"]
        chart_pattern = ""
        if bull_pats: chart_pattern = "🟢 " + ", ".join(bull_pats[:2])
        elif bear_pats: chart_pattern = "🔴 " + ", ".join(bear_pats[:2])

        def _raw_ret(p):
            if len(prices) >= p + 1:
                c, bk = float(prices.iloc[-1]), float(prices.iloc[-(p + 1)])
                return round((c / bk - 1) * 100, 2) if bk != 0 else np.nan
            return np.nan

        stock_ret_p2 = _raw_ret(p2)
        sec_avg_p2   = sector_ret.get(sector,    {}).get(p2, np.nan)
        ind_avg_p2   = industry_ret.get(industry, {}).get(p2, np.nan)

        def _beats(s_r, avg_r):
            if s_r == s_r and avg_r == avg_r: return s_r >= avg_r
            return None

        beats_sec = _beats(stock_ret_p2, sec_avg_p2)
        beats_ind = _beats(stock_ret_p2, ind_avg_p2)

        sec_rs_p2 = _best_sec_rs(sector, p2)
        ind_rs_p2 = industry_rs_spy.get(industry, {}).get(p2, np.nan)

        sec_beats_spy = (sec_rs_p2 > 0) if (sec_rs_p2 == sec_rs_p2) else None
        ind_beats_spy = (ind_rs_p2 > 0) if (ind_rs_p2 == ind_rs_p2) else None

        if (signal == "Buy" and beats_sec is True and beats_ind is True
                and sec_beats_spy is True and ind_beats_spy is True):
            enhanced_signal = "Strong Buy"
        else:
            enhanced_signal = signal

        tv_sym = f"{orig_sym},"

        row = {
            "Symbol":          orig_sym,
            "TV_Symbol":       tv_sym,
            "Company Name":    name,
            "Industry":        industry,
            "Sector":          sector,
            "Price":           round(cur_price, 2),
            f"RS_{p1}d_Idx%":  round(r_i1 * 100, 2) if r_i1 == r_i1 else np.nan,
            f"RS_{p2}d_Idx%":  round(r_i2 * 100, 2) if r_i2 == r_i2 else np.nan,
            f"RS_{p1}d_Sec%":  round(r_s1 * 100, 2) if r_s1 == r_s1 else np.nan,
            f"RS_{p2}d_Sec%":  round(r_s2 * 100, 2) if r_s2 == r_s2 else np.nan,
            "Signal":          signal,
            "Enhanced_Signal": enhanced_signal,
            "RS_120d_Idx%":    round(rs_idx.get(120, np.nan) * 100, 2) if rs_idx.get(120, np.nan) == rs_idx.get(120, np.nan) else np.nan,
            "RS_252d_Idx%":    round(rs_idx.get(252, np.nan) * 100, 2) if rs_idx.get(252, np.nan) == rs_idx.get(252, np.nan) else np.nan,
            "RSI_14":          tech["RSI_14"],
            "Abv_SMA20":       tech["Abv_SMA20"],
            "Abv_SMA50":       tech["Abv_SMA50"],
            "Abv_SMA100":      tech["Abv_SMA100"],
            "Abv_SMA200":      tech["Abv_SMA200"],
            "SMA_Score":       tech["SMA_Score"],
            "From_52W_High%":  round(from_52w, 1) if from_52w == from_52w else np.nan,
            "H_Day":           h_day,
            "L_Day":           l_day,
            "Chart_Pattern":   chart_pattern,
            f"Ret_{p2}d%":         stock_ret_p2,
            f"SecAvg_Ret_{p2}d%":  sec_avg_p2,
            f"IndAvg_Ret_{p2}d%":  ind_avg_p2,
            "Beats_Sec":           _tick(beats_sec),
            "Beats_Ind":           _tick(beats_ind),
            f"SecRS_{p2}d%":       round(sec_rs_p2 * 100, 2) if sec_rs_p2 == sec_rs_p2 else np.nan,
            f"IndRS_{p2}d%":       round(ind_rs_p2 * 100, 2) if ind_rs_p2 == ind_rs_p2 else np.nan,
            "Sec_Beats_SPY":       _tick(sec_beats_spy),
            "Ind_Beats_SPY":       _tick(ind_beats_spy),
        }
        rows.append(row)
        if (i + 1) % 100 == 0: print(f"    … {i + 1}/{total} stocks")

    df = pd.DataFrame(rows)
    if df.empty: return df

    rs1_col = f"RS_{p1}d_Idx%"
    rs2_col = f"RS_{p2}d_Idx%"

    def pct_rank(series):
        vals = series.dropna().values
        if len(vals) == 0: return pd.Series(np.nan, index=series.index)
        return pd.Series([round(np.sum(vals <= v) / len(vals) * 100, 1) for v in series], index=series.index)

    df["RS_Pctile_P1"] = pct_rank(df[rs1_col])
    df["RS_Pctile_P2"] = pct_rank(df[rs2_col])

    weights = [(rs1_col, 0.40), (rs2_col, 0.30), ("RS_120d_Idx%", 0.20), ("RS_252d_Idx%", 0.10)]
    score   = pd.Series(0.0, index=df.index)
    w_total = pd.Series(0.0, index=df.index)
    for col, w in weights:
        mask = df[col].notna()
        score[mask]   += df.loc[mask, col] * w
        w_total[mask] += w
    df["RS_Score"] = (score / w_total.replace(0, np.nan)).round(2)
    df["RS_Rank"]  = df["RS_Score"].rank(ascending=False, method="min").fillna(999).astype(int)

    sig_ord = {"Strong Buy": 0, "Buy": 1, "NA": 2, "Sell": 3}
    df["_ord"] = df["Enhanced_Signal"].map(sig_ord).fillna(2)
    df = df.sort_values(["_ord", "RS_Score"], ascending=[True, False]).drop(columns=["_ord"]).reset_index(drop=True)

    strong_buys = len(df[df["Enhanced_Signal"] == "Strong Buy"])
    buys        = len(df[df["Signal"] == "Buy"])
    sells       = len(df[df["Signal"] == "Sell"])
    print(f"  ✅ Stocks: {len(df)} | Strong Buy: {strong_buys} | Buy: {buys} | Sell: {sells} | NA: {len(df) - buys - sells}")
    return df

def sector_drilldown(stock_df, sector_df, top_n=10):
    rows = []
    for _, sec_row in sector_df.iterrows():
        sname  = sec_row["Sector"]
        stocks = stock_df[stock_df["Sector"] == sname]
        if stocks.empty: continue
        top = stocks.sort_values("RS_Score", ascending=False).head(top_n).copy()
        top.insert(0, "Sector_Rank",   int(sec_row["Rank"]))
        top.insert(1, "Sector_Signal", sec_row["Signal"])
        rows.append(top)
    if not rows: return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)



# ══════════════════════════════════════════════════════════════════════════════
#  ⓬  GOOGLE SHEETS EXPORT
# ══════════════════════════════════════════════════════════════════════════════

def build_dashboard_df(stock_df, sector_df, run_time):
    """Build the Dashboard tab as a two-column DataFrame."""
    p1, p2 = SIGNAL_PERIODS
    sb  = len(stock_df[stock_df["Enhanced_Signal"] == "Strong Buy"])
    rows = [
        ["USA RS ANALYSIS SYSTEM v4.0 — GOOGLE SHEETS", run_time],
        ["Data Source", "Yahoo Finance (yfinance)"],
        ["Index", f"{US_INDEX} (S&P 500 ETF)"],
        ["Signal", f"Buy = RS_{p1}d>0 AND RS_{p2}d>0 (vs Index AND vs Sector)"],
        ["Strong Buy", f"All 5 peer filters: stock beats sector + industry avg, sector+industry beat SPY"],
        ["", ""],
        ["── UNIVERSE ──", ""],
        ["Stocks Analysed",  len(stock_df)],
        ["⭐ Strong Buy",    sb],
        ["✅ Buy Signals",    len(stock_df[stock_df["Signal"] == "Buy"])],
        ["🔴 Sell Signals",   len(stock_df[stock_df["Signal"] == "Sell"])],
        ["NA",               len(stock_df[stock_df["Signal"] == "NA"])],
        ["", ""],
        ["── SECTOR RS (best → worst) ──", ""],
    ]
    for _, r in sector_df.iterrows():
        rows.append([
            f"#{int(r['Rank'])} {r['Sector']}",
            f"{r['Signal']} | RS_22d:{r.get('RS_22d',0):+.1f}% | RS_55d:{r.get('RS_55d',0):+.1f}% | RSI:{r.get('RSI_14','—')}",
        ])

    rows += [["", ""], ["── ⭐ STRONG BUY STOCKS ──", ""]]
    sb_df = stock_df[stock_df["Enhanced_Signal"] == "Strong Buy"]
    for _, r in sb_df.head(20).iterrows():
        rr55 = r.get(f"RS_{p2}d_Idx%", 0)
        rows.append([r["Symbol"], (
            f"RS55:{rr55:.1f}% | {r['Sector']} | RSI:{r.get('RSI_14','—')} | "
            f"SMA:{r.get('SMA_Score','—')}/4 | {r.get('Chart_Pattern','')}"
        )])

    rows += [["", ""], ["── TV WATCHLIST ──", ""],
             ["HOW TO", "Copy the value in the next row → TradingView → Watchlist → Import from clipboard"]]
    buy_df = stock_df[stock_df["Signal"].isin(["Buy", "Strong Buy"])]
    rows.append(["TV All Buy+Strong",  "".join(buy_df["TV_Symbol"].tolist())])
    rows.append(["TV Strong Buy Only", "".join(sb_df.nlargest(min(len(sb_df),50),"RS_Score")["TV_Symbol"].tolist())])
    rows.append(["TV Top-20 Buy",      "".join(buy_df.nlargest(20,"RS_Score")["TV_Symbol"].tolist())])

    rows += [["", ""], ["── COLUMN GUIDE ──", ""],
        ["RS_Nd_Idx%",      f"RS vs SPY over N days. >0 = outperforming S&P 500"],
        ["RS_Nd_Sec%",      "RS vs sector ETF (XLK, XLF, etc.)"],
        ["Enhanced_Signal", "Strong Buy = RS Buy + stock > sector avg + stock > industry avg + sector > SPY + industry > SPY"],
        ["Beats_Sec",       "✓ = stock return > sector peer-group average"],
        ["Beats_Ind",       "✓ = stock return > industry peer-group average"],
        ["Sec_Beats_SPY",   "✓ = sector average return > SPY return"],
        ["Ind_Beats_SPY",   "✓ = industry average return > SPY return"],
        ["SMA_Score",       "0–4: how many of SMA20/50/100/200 the stock is above"],
        ["H_Day",           "Days since 22-day high. H_Day=0 means AT HIGHS TODAY"],
        ["Chart_Pattern",   "🟢 Bullish or 🔴 Bearish detected pattern"],
        ["RS_Score",        "Composite: 1M×40% + 3M×30% + 6M×20% + 12M×10%"],
    ]
    return pd.DataFrame(rows, columns=["Key", "Value"])


def build_patterns_df(patterns_list, stock_df):
    """Build chart patterns tab as a flat DataFrame."""
    if not patterns_list:
        return pd.DataFrame({"Message": ["No chart patterns detected in this run."]})
    sym_map = {}
    if not stock_df.empty:
        for _, r in stock_df.iterrows():
            sym_map[r["Symbol"]] = {"Sector": r.get("Sector",""), "Signal": r.get("Signal","NA")}
    rows = []
    for s in sorted(patterns_list, key=lambda x: (0 if x.direction=="BULLISH" else 1, x.end_date)):
        info = sym_map.get(s.symbol, {})
        rows.append({
            "Symbol":       s.symbol,
            "Sector":       info.get("Sector",""),
            "RS_Signal":    info.get("Signal","NA"),
            "Pattern":      s.pattern,
            "Direction":    s.direction,
            "End_Date":     s.end_date,
            "Entry":        s.entry_price,
            "Stop_Loss":    s.stop_loss,
            "Target":       s.target,
            "RR_Ratio":     s.risk_reward,
            "Confidence":   s.confidence,
            "Win_Rate_Est": s.win_rate_est,
            "Notes":        s.notes,
        })
    return pd.DataFrame(rows)


def export_to_gsheets(ss, stock_df, sector_df, drilldown_df, patterns_list,
                      sector_rot_df, industry_rot_df, breadth_df, sector_perf_df):
    """Write all 12 tabs to Google Sheets."""
    run_time = datetime.now().strftime("%d %b %Y  %H:%M ET")
    p1, p2   = SIGNAL_PERIODS

    buy_df       = stock_df[stock_df["Signal"].isin(["Buy","Strong Buy"])].copy()
    sell_df      = stock_df[stock_df["Signal"] == "Sell"].copy()
    strong_buy_df= stock_df[stock_df["Enhanced_Signal"] == "Strong Buy"].copy()

    # Common format columns for stock sheets
    pct_cols  = [f"RS_{p1}d_Idx%", f"RS_{p2}d_Idx%", f"RS_{p1}d_Sec%", f"RS_{p2}d_Sec%",
                 "RS_120d_Idx%", "RS_252d_Idx%", "RS_Score",
                 f"Ret_{p2}d%", f"SecAvg_Ret_{p2}d%", f"IndAvg_Ret_{p2}d%",
                 f"SecRS_{p2}d%", f"IndRS_{p2}d%", "From_52W_High%"]
    tick_cols = ["Beats_Sec","Beats_Ind","Sec_Beats_SPY","Ind_Beats_SPY",
                 "Abv_SMA20","Abv_SMA50","Abv_SMA100","Abv_SMA200"]

    def stock_fmts(df):
        if df is None or df.empty: return {}
        fmts = {"Signal": "signal", "Enhanced_Signal": "signal",
                "Sector_Signal": "signal", "SMA_Score": "sma_score"}
        for c in pct_cols:
            if c in df.columns: fmts[c] = "positive"
        for c in tick_cols:
            if c in df.columns: fmts[c] = "tick"
        return fmts

    print("\n  Writing tabs to Google Sheets …")

    # 1. Dashboard
    dash_df = build_dashboard_df(stock_df, sector_df, run_time)
    write_tab(ss, TABS["dashboard"],    dash_df, "navy"); time.sleep(1)

    # 2. Sector RS
    sec_fmts = {"Signal":"signal"}
    for c in [f"RS_{p}d" for p in [22,55,120,252]]: sec_fmts[c] = "positive"
    write_tab(ss, TABS["sector_rs"],    sector_df, "navy", sec_fmts); time.sleep(1)

    # 3. Top By Sector
    write_tab(ss, TABS["top_by_sector"],drilldown_df, "navy", stock_fmts(drilldown_df)); time.sleep(1)

    # 4. Strong Buy
    write_tab(ss, TABS["strong_buy"],   strong_buy_df, "teal", stock_fmts(strong_buy_df)); time.sleep(1)

    # 5. Buy Stocks
    write_tab(ss, TABS["buy_stocks"],   buy_df, "green", stock_fmts(buy_df)); time.sleep(1)

    # 6. Sell Stocks
    write_tab(ss, TABS["sell_stocks"],  sell_df, "red", stock_fmts(sell_df)); time.sleep(1)

    # 7. All Stocks RS
    write_tab(ss, TABS["all_stocks"],   stock_df, "navy", stock_fmts(stock_df)); time.sleep(1)

    # 8. Chart Patterns
    pat_df = build_patterns_df(patterns_list, stock_df)
    write_tab(ss, TABS["chart_patterns"], pat_df, "navy"); time.sleep(1)

    # 9. Sector Rotation
    if not sector_rot_df.empty:
        rot_fmts = {c:"pct_bar" for c in ["RS55_Above%","RSI50_Above%","SMA20_Above%","SMA50_Above%","SMA100_Above%"]}
        rot_fmts.update({"1M_Zone":"zone","3M_Zone":"zone","6M_Zone":"zone"})
        write_tab(ss, TABS["sector_rotation"],  sector_rot_df,   "navy", rot_fmts); time.sleep(1)

    # 10. Industry Rotation
    if not industry_rot_df.empty:
        write_tab(ss, TABS["industry_rotation"], industry_rot_df, "navy", rot_fmts); time.sleep(1)

    # 11. Market Breadth
    if not breadth_df.empty:
        brd_fmts = {c:"pct_bar" for c in ["RS55_Above%","RSI50_Above%","SMA20_Above%",
                                            "SMA50_Above%","SMA100_Above%","SMA200_Above%"]}
        brd_fmts.update({"1M_Zone":"zone","3M_Zone":"zone","6M_Zone":"zone"})
        write_tab(ss, TABS["market_breadth"],   breadth_df,      "navy", brd_fmts); time.sleep(1)

    # 12. Sector Performance
    if not sector_perf_df.empty:
        perf_fmts = {c:"positive" for c in sector_perf_df.columns if "%" in str(c)}
        write_tab(ss, TABS["sector_perf"],      sector_perf_df,  "navy", perf_fmts)

    print(f"  ✅ All tabs written to: {SHEET_URL}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "═"*68)
    print(f"  USA RS ANALYSIS SYSTEM v4.0 — GOOGLE SHEETS EDITION")
    print(f"  {datetime.now().strftime('%d %b %Y  %H:%M')}")
    print(f"  Periods: {RS_PERIODS}  |  Signal: {SIGNAL_PERIODS}")
    print("═"*68 + "\n")

    # ── Connect to Google Sheets ───────────────────────────────────────────────
    print("🔐 Authenticating to Google Sheets …")
    ss = gs_connect()

    # ── Load universe ─────────────────────────────────────────────────────────
    print("\n📂 Loading universe …")
    universe = load_universe(MARKET)

    # ── Fetch prices ──────────────────────────────────────────────────────────
    USE_CACHE = False
    try:
        from price_cache import PriceCache
        USE_CACHE = True
    except ImportError:
        print("  ℹ  price_cache.py not found — direct download")

    index_sym   = US_INDEX
    sector_syms = [cfg["yahoo"] for cfg in US_SECTORS.values() if cfg.get("yahoo")]
    stock_syms  = universe["Yahoo"].tolist()

    if USE_CACHE:
        print("\n📦 Loading via PriceCache …")
        cache    = PriceCache()
        start_dt = (datetime.now() - timedelta(days=PERIOD_DAYS + 10)).strftime("%Y-%m-%d")
        close_all, _ = cache.get([index_sym] + sector_syms + stock_syms, start_dt)

        idx_c = close_all[index_sym] if index_sym in close_all.columns else pd.Series()
        if isinstance(idx_c, pd.DataFrame): idx_c = idx_c.squeeze()
        index_prices = idx_c.dropna()
        if index_prices.empty:
            print(f"  ❌ Index {index_sym} not in cache"); return

        sector_prices = {}
        const = load_sector_constituents(MARKET)
        for sname, cfg in US_SECTORS.items():
            ysym = cfg.get("yahoo")
            if ysym and ysym in close_all.columns:
                s = close_all[ysym].dropna()
                if len(s) >= 22: sector_prices[sname] = s
        price_data = close_all[[s for s in stock_syms if s in close_all.columns]]
    else:
        print("\n📡 Fetching prices …")
        idx_raw = yf.download(index_sym, period=f"{PERIOD_DAYS}d", auto_adjust=True, progress=False)
        if idx_raw.empty:
            print(f"  ❌ Cannot fetch {index_sym}"); return
        idx_c = idx_raw["Close"]
        if isinstance(idx_c, pd.DataFrame): idx_c = idx_c.squeeze()
        index_prices  = normalize_series(idx_c.dropna())
        print(f"  ✅ Index: {len(index_prices)} days")
        sector_prices = fetch_sector_indices(MARKET)
        print(f"\n📡 Fetching {len(universe)} stocks …")
        price_data = fetch_prices(stock_syms, PERIOD_DAYS)

    print(f"  ✅ Index:{len(index_prices)}d | Sectors:{len(sector_prices)} | Stocks:{len(price_data.columns)}")

    # ── Sector RS ─────────────────────────────────────────────────────────────
    print("\n🔢 Sector RS analysis …")
    sector_df = analyse_sectors(index_prices, sector_prices, MARKET)

    # ── Chart Patterns ────────────────────────────────────────────────────────
    patterns_by_sym, patterns_list = {}, []
    if ENABLE_CHART_PATTERNS:
        print(f"\n📐 Chart patterns (top {PATTERN_MAX_STOCKS} stocks) …")
        cands = [s for s in stock_syms if s in price_data.columns and
                 len(price_data[s].dropna()) >= 60][:PATTERN_MAX_STOCKS]
        ohlcv_dict = fetch_ohlcv_batch(cands, days=PERIOD_DAYS)
        patterns_by_sym, patterns_list = run_pattern_detection(ohlcv_dict)
        print(f"  ✅ {len(patterns_list)} patterns in {len(patterns_by_sym)} stocks")

    # ── Stock Analysis ────────────────────────────────────────────────────────
    print("\n🔢 Stock RS + peer analysis …")
    stock_df = analyse_stocks(universe, price_data, index_prices, sector_prices,
                               patterns_by_sym=patterns_by_sym, market=MARKET, sector_df=sector_df)
    if stock_df.empty:
        print("  ❌ No stock data"); return

    drilldown = sector_drilldown(stock_df, sector_df, top_n=10)

    # ── Sector / Industry Rotation ────────────────────────────────────────────
    sector_rot_df = pd.DataFrame()
    industry_rot_df = pd.DataFrame()
    if ENABLE_SECTOR_ROTATION:
        print("\n🔄 Sector rotation …")
        sector_rot_df   = analyse_sector_rotation(universe, price_data, index_prices)
        print("\n🏭 Industry rotation …")
        industry_rot_df = analyse_industry_rotation(universe, price_data, index_prices)

    # ── Market Breadth ────────────────────────────────────────────────────────
    breadth_df = pd.DataFrame()
    if ENABLE_MARKET_BREADTH:
        print("\n📊 Market breadth …")
        breadth_df = analyse_market_breadth(price_data, index_prices, universe)

    # ── Sector Performance ────────────────────────────────────────────────────
    print("\n📈 Sector performance (QoQ/YoY) …")
    sector_perf_df = analyse_sector_performance(sector_prices, index_prices)

    # ── Export to Google Sheets ───────────────────────────────────────────────
    print("\n📊 Exporting to Google Sheets …")
    export_to_gsheets(ss, stock_df, sector_df, drilldown, patterns_list,
                      sector_rot_df, industry_rot_df, breadth_df, sector_perf_df)

    # ── Console summary ───────────────────────────────────────────────────────
    p1, p2 = SIGNAL_PERIODS
    print("\n" + "═"*68)
    print("  ✅  COMPLETE!")
    print(f"  🔗  {SHEET_URL}")

    sb_df = stock_df[stock_df["Enhanced_Signal"] == "Strong Buy"]
    print(f"\n  Stocks:{len(stock_df)} | ⭐ Strong Buy:{len(sb_df)} | "
          f"Buy:{(stock_df['Signal']=='Buy').sum()} | Sell:{(stock_df['Signal']=='Sell').sum()}")

    print(f"\n  📊 SECTOR RS (sorted by RS_55d):")
    print(f"  {'Rank':<5} {'Sector':<22} {'Sig':<6} {'RS22d':>7} {'RS55d':>7}")
    print("  " + "-"*50)
    for _, r in sector_df.head(11).iterrows():
        print(f"  {int(r['Rank']):<5} {str(r['Sector']):<22} {str(r['Signal']):<6} "
              f"{r.get('RS_22d',0):>7.1f}% {r.get('RS_55d',0):>7.1f}%")

    if not sb_df.empty:
        print(f"\n  ⭐ STRONG BUY (top 10):")
        print(f"  {'Sym':<14} {'Sector':<16} {'RS55d':>6} {'BtSec':>6} {'BtInd':>6} {'SecSPY':>7}")
        print("  " + "-"*60)
        for _, r in sb_df.head(10).iterrows():
            print(f"  {r['Symbol']:<14} {str(r['Sector']):<16} "
                  f"{r.get(f'RS_{p2}d_Idx%',0):>6.1f}% "
                  f"{str(r.get('Beats_Sec','—')):>6} "
                  f"{str(r.get('Beats_Ind','—')):>6} "
                  f"{str(r.get('Sec_Beats_SPY','—')):>7}")

    if patterns_list:
        print(f"\n  📐 CHART PATTERNS: {len(patterns_list)} detected")
        by_pat = {}
        for s in patterns_list: by_pat[s.pattern] = by_pat.get(s.pattern, 0) + 1
        for p, c in sorted(by_pat.items(), key=lambda x: -x[1])[:6]:
            print(f"     {p:<28} {c}")
    print("═"*68)


if __name__ == "__main__":
    main()
