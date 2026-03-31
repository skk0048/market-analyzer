"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  INDIA RS MARKET ANALYSIS SYSTEM v3.0 — GOOGLE SHEETS EDITION             ║
║                                                                            ║
║  Replaces openpyxl Excel export with live Google Sheets output.           ║
║  All data processing logic is identical to the local version.             ║
║                                                                            ║
║  AUTHENTICATION:                                                           ║
║    Reads service account credentials from:                                 ║
║    1. Environment variable  GOOGLE_CREDENTIALS_PATH  (GitHub Actions)     ║
║    2. Local file  google_credentials.json            (local dev)           ║
║                                                                            ║
║  GOOGLE SHEET URL:                                                         ║
║    Read from environment variable  GOOGLE_SHEET_URL                       ║
║    Set in GitHub Secrets, or hardcode for local testing.                  ║
║                                                                            ║
║  SHEET TABS WRITTEN (in order):                                            ║
║    📋 Dashboard       — Run summary + top picks + SL quality               ║
║    🏭 Sector RS       — Sector relative strength table                     ║
║    🏆 Top By Sector   — Best stocks per sector                             ║
║    ✅ Buy Stocks      — RS Buy signals with SL + RR                        ║
║    🔴 Sell Stocks     — RS Sell signals                                    ║
║    📊 All Stocks RS   — Full 500-stock universe                            ║
║    📐 Chart Patterns  — Detected chart patterns                            ║
║    🔄 Sector Rotation — StockEdge-style rotation breadth                   ║
║    🏭 Industry Rotat. — Industry-level rotation                            ║
║    📊 Market Breadth  — Index breadth (Nifty 50/500/Bank/IT etc.)         ║
║    💰 FII DII         — Daily FII & DII net flows                          ║
║    📈 Sector Perf.    — QoQ / YoY sector returns                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ── Standard library ──────────────────────────────────────────────────────────
import os
import sys
import time
import warnings
import io
from datetime import datetime, timedelta, date

# ── Third-party ───────────────────────────────────────────────────────────────
import numpy as np
import pandas as pd
import yfinance as yf
import requests
from scipy.signal import argrelextrema
from dataclasses import dataclass
from typing import Optional, List

# ── Google Sheets ─────────────────────────────────────────────────────────────
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import tenacity

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
#  ❶  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

MARKET = os.environ.get("MARKET", "INDIA")

# ── Paths — resolved from environment (GitHub Actions) or local fallback ──────
CREDENTIALS_PATH = (
    os.environ.get("GOOGLE_CREDENTIALS_PATH")
    or os.path.join(os.path.dirname(os.path.abspath(__file__)), "google_credentials.json")
)

# The Google Sheet URL — set GOOGLE_SHEET_URL in GitHub Secrets
# Or hardcode your sheet URL here for local testing:
SHEET_URL = (
    os.environ.get("GOOGLE_SHEET_URL")
    or "https://docs.google.com/spreadsheets/d/YOUR_SPREADSHEET_ID_HERE/edit"
)

# ── Google Sheets API scopes ──────────────────────────────────────────────────
GSCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── Data paths (used when running locally) ────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
BASE_DIR1 = os.path.join(BASE_DIR, "IndexData")

# ── RS / Analysis parameters ─────────────────────────────────────────────────
RS_PERIODS     = [22, 55, 120, 252]
SIGNAL_PERIODS = [22, 55]
HL_LOOKBACK    = 22

ENABLE_CHART_PATTERNS  = True
ENABLE_SECTOR_ROTATION = True
ENABLE_MARKET_BREADTH  = True
ENABLE_FII_DII         = True
PATTERN_MAX_STOCKS     = 400

INDIA_INDEX = "^NSEI"
BATCH_SIZE  = 100
BATCH_DELAY = 1.0
MAX_STOCKS  = 500
PERIOD_DAYS = 300

# ── Sector → Yahoo Finance index ticker ───────────────────────────────────────
INDIA_SECTORS = {
    "Automobile":    {"yahoo": "^CNXAUTO",   "csv": "ind_niftyautolist.csv"},
    "IT":            {"yahoo": "^CNXIT",      "csv": "ind_niftyitlist.csv"},
    "Banking":       {"yahoo": "^NSEBANK",    "csv": "ind_niftybanklist.csv"},
    "Pharma":        {"yahoo": "^CNXPHARMA",  "csv": "ind_niftypharmalist.csv"},
    "FMCG":          {"yahoo": "^CNXFMCG",    "csv": "ind_niftyfmcglist.csv"},
    "Metal":         {"yahoo": "^CNXMETAL",   "csv": "ind_niftymetallist.csv"},
    "Oil & Gas":     {"yahoo": "^CNXENERGY",  "csv": "ind_niftyoilgaslist.csv"},
    "Finance":       {"yahoo": "^CNXFIN", "csv": "ind_niftyfinancelist.csv"},
    "Realty":        {"yahoo": "^CNXREALTY",  "csv": None},
    "Infra":         {"yahoo": "^CNXINFRA",   "csv": None},
    "Media":         {"yahoo": "^CNXMEDIA",   "csv": "ind_niftymedialist.csv"},
    "PSU Bank":      {"yahoo": "^CNXPSUBANK", "csv": "ind_niftypsubanklist.csv"},
    "Chemicals":     {"yahoo": None,           "csv": "ind_niftyChemicals_list.csv"},
    "Consumer Dur.": {"yahoo": None,           "csv": "ind_niftyconsumerdurableslist.csv"},
    "Healthcare":    {"yahoo": None,           "csv": "ind_niftyhealthcarelist.csv"},
    "Cement":        {"yahoo": None,           "csv": "ind_NiftyCement_list.csv"},
}

INDIA_INDUSTRY_TO_SECTOR = {
    "Automobile and Auto Components":     "Automobile",
    "Information Technology":             "IT",
    "Financial Services":                 "Finance",
    "Healthcare":                         "Healthcare",
    "Fast Moving Consumer Goods":         "FMCG",
    "Metals & Mining":                    "Metal",
    "Oil, Gas & Consumable Fuels":        "Oil & Gas",
    "Oil Gas & Consumable Fuels":         "Oil & Gas",
    "Realty":                             "Realty",
    "Capital Goods":                      "Infra",
    "Construction":                       "Infra",
    "Construction Materials":             "Cement",
    "Media, Entertainment & Publication": "Media",
    "Chemicals":                          "Chemicals",
    "Consumer Durables":                  "Consumer Dur.",
    "Consumer Services":                  "Consumer Dur.",
    "Power":                              "Oil & Gas",
    "Telecommunication":                  "IT",
    "Textiles":                           "Finance",
    "Utilities":                          "Finance",
    "Services":                           "Finance",
    "Diversified":                        "Finance",
    "Forest Materials":                   "Finance",
}

NSE_BREADTH_INDICES = {
    "Nifty 50":       {"yahoo": "^NSEI",      "csv": "ind_nifty50list.csv"},
    "Nifty 500":      {"yahoo": None,          "csv": "ind_nifty500list.csv"},
    "Nifty Bank":     {"yahoo": "^NSEBANK",    "csv": "ind_niftybanklist.csv"},
    "Nifty IT":       {"yahoo": "^CNXIT",      "csv": "ind_niftyitlist.csv"},
    "Nifty Midcap":   {"yahoo": "^CNXMDCP100", "csv": "ind_niftymidcap100list.csv"},
    "Nifty Smallcap": {"yahoo": "^CNXSC",      "csv": "ind_niftysmallcap100list.csv"},
    "Nifty Total Mkt":{"yahoo": None,          "csv": "ind_niftytotalmarket_list.csv"},
}

INDIA_STOCK_FILE = os.path.join(BASE_DIR1, "ind_niftytotalmarket_list.csv")

# ── Google Sheets tab names (no emoji in tab names to avoid encoding issues) ──
# Maps internal key → actual Google Sheet tab title
TABS = {
    "dashboard":        "📋 Dashboard",
    "sector_rs":        "🏭 Sector RS",
    "top_by_sector":    "🏆 Top By Sector",
    "buy_stocks":       "✅ Buy Stocks",
    "sell_stocks":      "🔴 Sell Stocks",
    "all_stocks":       "📊 All Stocks RS",
    "chart_patterns":   "📐 Chart Patterns",
    "sector_rotation":  "🔄 Sector Rotation",
    "industry_rotation":"🏭 Industry Rotation",
    "market_breadth":   "📊 Market Breadth",
    "fii_dii":          "💰 FII DII",
    "sector_perf":      "📈 Sector Perf",
}

# ── Google Sheets color palette (RGB 0.0–1.0 floats) ─────────────────────────
GS_COLORS = {
    "navy":       {"red": 0.051, "green": 0.129, "blue": 0.216},
    "dark_green": {"red": 0.106, "green": 0.365, "blue": 0.165},
    "med_green":  {"red": 0.200, "green": 0.494, "blue": 0.239},
    "lt_green":   {"red": 0.784, "green": 0.902, "blue": 0.788},
    "xl_green":   {"red": 0.863, "green": 0.929, "blue": 0.867},
    "red":        {"red": 0.835, "green": 0.153, "blue": 0.157},
    "lt_red":     {"red": 1.000, "green": 0.800, "blue": 0.800},
    "amber":      {"red": 1.000, "green": 0.973, "blue": 0.769},
    "white":      {"red": 1.000, "green": 1.000, "blue": 1.000},
    "lt_grey":    {"red": 0.961, "green": 0.961, "blue": 0.961},
    "orange":     {"red": 1.000, "green": 0.878, "blue": 0.706},
    "blue":       {"red": 0.737, "green": 0.843, "blue": 0.937},
    "yellow":     {"red": 1.000, "green": 1.000, "blue": 0.600},
}


# ══════════════════════════════════════════════════════════════════════════════
#  ❷  GOOGLE SHEETS CLIENT  (authentication + sheet management)
# ══════════════════════════════════════════════════════════════════════════════

def get_gspread_client():
    """Authenticate and return a gspread client using service account credentials."""
    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(
            f"Google credentials not found at: {CREDENTIALS_PATH}\n"
            "Set environment variable GOOGLE_CREDENTIALS_PATH or place "
            "'google_credentials.json' next to this script."
        )
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=GSCOPE)
    return gspread.authorize(creds)


def get_or_create_worksheet(spreadsheet, tab_title, rows=2000, cols=50):
    """
    Get an existing worksheet by title, or create it if it doesn't exist.
    Returns the worksheet object.
    """
    try:
        ws = spreadsheet.worksheet(tab_title)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        print(f"    Creating new tab: '{tab_title}'")
        ws = spreadsheet.add_worksheet(title=tab_title, rows=rows, cols=cols)
        return ws


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type(
        (gspread.exceptions.APIError, ConnectionError)
    ),
    reraise=True,
)
def _api_call(func, *args, **kwargs):
    """
    Wrap any gspread API call with exponential-backoff retry.
    Google Sheets API quota: 100 requests / 100 seconds per user.
    Handles: quota exceeded (429), server errors (5xx), transient failures.
    """
    return func(*args, **kwargs)


def clear_and_write_df(ws, df, include_index=False):
    """
    Clear a worksheet then write a DataFrame to it starting at A1.
    Uses set_with_dataframe for correct type handling.
    Retries automatically on quota errors.
    """
    if df is None or df.empty:
        _api_call(ws.clear)
        _api_call(ws.update, "A1", [["No data available for this section."]])
        return

    # Replace NaN with empty string (Google Sheets doesn't understand NaN)
    df_clean = df.copy()
    
    # Replace problematic values
    df_clean = df_clean.replace([float("inf"), float("-inf")], "")
    df_clean = df_clean.fillna("")
    
    # 🔥 CRITICAL FIX: Convert all numpy types → native Python
    df_clean = df_clean.astype(object)
    
    # Ensure all values are JSON serializable
    df_clean = df_clean.applymap(lambda x: 
        float(x) if isinstance(x, (np.floating,)) else
        int(x) if isinstance(x, (np.integer,)) else
        x
    )

    _api_call(ws.clear)
    time.sleep(0.5)   # small pause to avoid hitting quota
    _api_call(
        set_with_dataframe,
        ws, df_clean,
        include_index=include_index,
        resize=True,
    )


def apply_header_format(ws, n_cols, bg_color_key="navy"):
    """Apply dark background + white bold text to header row (row 1)."""
    bg = GS_COLORS.get(bg_color_key, GS_COLORS["navy"])
    col_end = gspread.utils.rowcol_to_a1(1, n_cols).replace("1", "")
    fmt = {
        "backgroundColor": bg,
        "textFormat": {
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            "bold": True,
            "fontSize": 10,
        },
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
    }
    try:
        _api_call(ws.format, f"A1:{col_end}1", fmt)
    except Exception:
        pass  # Formatting is non-critical


def apply_column_conditional_format(ws, df, col_name, n_rows,
                                    green_condition="positive",
                                    invert=False):
    """
    Apply green/red background to a column based on its values.
    green_condition:
      'positive'  → green if > 0, red if < 0
      'grade'     → green for A/B, yellow for C, red for D/F
      'pct_bar'   → green if >=60, yellow if >=40, red if <40
      'rr'        → green if >=3, yellow if >=2, red if <2
    invert: flip green/red (used for SL% columns — smaller is better)
    """
    if col_name not in df.columns:
        return
    col_idx = list(df.columns).index(col_name) + 1  # 1-based
    col_letter = gspread.utils.rowcol_to_a1(1, col_idx).replace("1", "")

    cell_formats = []
    for row_i, val in enumerate(df[col_name], start=2):
        cell_ref = f"{col_letter}{row_i}"
        bg = None

        if green_condition == "positive":
            try:
                v = float(str(val).replace(",", "").replace("%", "") or 0)
                if v > 0:
                    bg = GS_COLORS["lt_green"] if not invert else GS_COLORS["lt_red"]
                elif v < 0:
                    bg = GS_COLORS["lt_red"] if not invert else GS_COLORS["lt_green"]
            except (ValueError, TypeError):
                pass

        elif green_condition == "grade":
            v = str(val)
            if v in ("A",):   bg = GS_COLORS["lt_green"]
            elif v in ("B",): bg = GS_COLORS["xl_green"]
            elif v in ("C",): bg = GS_COLORS["amber"]
            elif v in ("D",): bg = GS_COLORS["orange"]
            elif v in ("F",): bg = GS_COLORS["lt_red"]

        elif green_condition == "signal":
            v = str(val)
            if v in ("Buy", "STRONG BUY", "BUY"):    bg = GS_COLORS["lt_green"]
            elif v in ("Sell", "STRONG SELL", "SELL"):bg = GS_COLORS["lt_red"]
            elif v in ("NA", "NEUTRAL"):               bg = GS_COLORS["amber"]

        elif green_condition == "pct_bar":
            try:
                v = float(str(val).replace("%", "") or 0)
                if v >= 60:   bg = GS_COLORS["lt_green"]
                elif v >= 40: bg = GS_COLORS["amber"]
                else:         bg = GS_COLORS["lt_red"]
            except (ValueError, TypeError):
                pass

        elif green_condition == "rr":
            try:
                v = float(str(val).replace("x", "") or 0)
                if v >= 3:   bg = GS_COLORS["lt_green"]
                elif v >= 2: bg = GS_COLORS["amber"]
                elif v > 0:  bg = GS_COLORS["lt_red"]
            except (ValueError, TypeError):
                pass

        elif green_condition == "sma_score":
            try:
                v = int(float(str(val) or 0))
                pal = {4: "lt_green", 3: "xl_green", 2: "amber", 1: "orange", 0: "lt_red"}
                bg = GS_COLORS.get(pal.get(v, "white"))
            except (ValueError, TypeError):
                pass

        if bg:
            cell_formats.append({
                "range": cell_ref,
                "format": {"backgroundColor": bg}
            })

    # Batch format to reduce API calls
    if cell_formats:
        try:
            # Split into chunks of 50 to stay under API limits
            for i in range(0, len(cell_formats), 50):
                chunk = cell_formats[i:i + 50]
                _api_call(ws.batch_format, chunk)
                if i + 50 < len(cell_formats):
                    time.sleep(0.3)
        except Exception as e:
            print(f"      ⚠ Formatting skipped for {col_name}: {e}")


def freeze_header_row(ws):
    """Freeze row 1 so headers stay visible when scrolling."""
    try:
        _api_call(
            ws.spreadsheet.batch_update,
            {"requests": [{
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws.id,
                        "gridProperties": {"frozenRowCount": 1}
                    },
                    "fields": "gridProperties.frozenRowCount"
                }
            }]}
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
#  ❸  ALL ORIGINAL DATA-PROCESSING FUNCTIONS (unchanged from local version)
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
    """Ensure s is always a plain 1-D pd.Series with timezone-naive date index."""
    try:
        # If a single-column DataFrame slipped through, squeeze it to a Series
        if isinstance(s, pd.DataFrame):
            s = s.squeeze()
        idx = _strip_tz(s.index)
        idx = idx.normalize()
        s2  = pd.Series(s.values, index=idx)
        s2  = s2[~s2.index.duplicated(keep="last")]
        return s2.sort_index()
    except Exception:
        return s


def load_universe():
    fpath = INDIA_STOCK_FILE
    if not os.path.exists(fpath):
        alt = os.path.join(BASE_DIR, "ind_niftytotalmarket_list.csv")
        if os.path.exists(alt):
            fpath = alt
        else:
            print("  ⚠ Universe CSV not found — using embedded 50-stock fallback")
            return _fallback_universe()

    df = pd.read_csv(fpath)
    df.columns = df.columns.str.strip()
    if "Symbol" not in df.columns:
        df = df.rename(columns={df.columns[2]: "Symbol"})
    if "Industry" not in df.columns:
        df = df.rename(columns={df.columns[1]: "Industry"})
    if "Company Name" not in df.columns:
        df = df.rename(columns={df.columns[0]: "Company Name"})
    df["Symbol"]   = df["Symbol"].str.strip()
    df["Industry"] = df["Industry"].str.strip()
    df["Yahoo"]    = df["Symbol"] + ".NS"
    df["Sector"]   = df["Industry"].map(INDIA_INDUSTRY_TO_SECTOR).fillna("Finance")
    if MAX_STOCKS > 0:
        df = df.head(MAX_STOCKS)
    print(f"  ✅ Universe: {len(df)} stocks")
    return df


def _fallback_universe():
    """Minimal fallback: top 50 Nifty stocks hardcoded."""
    stocks = [
        ("RELIANCE",  "Reliance Industries Ltd.",   "Oil, Gas & Consumable Fuels"),
        ("TCS",       "Tata Consultancy Services",  "Information Technology"),
        ("HDFCBANK",  "HDFC Bank Ltd.",             "Financial Services"),
        ("INFY",      "Infosys Ltd.",               "Information Technology"),
        ("ICICIBANK", "ICICI Bank Ltd.",            "Financial Services"),
        ("SBIN",      "State Bank of India",         "Financial Services"),
        ("BHARTIARTL","Bharti Airtel Ltd.",          "Telecommunication"),
        ("ITC",       "ITC Ltd.",                   "Fast Moving Consumer Goods"),
        ("KOTAKBANK", "Kotak Mahindra Bank",         "Financial Services"),
        ("LT",        "Larsen & Toubro Ltd.",        "Capital Goods"),
    ]
    df = pd.DataFrame(stocks, columns=["Symbol", "Company Name", "Industry"])
    df["Yahoo"]  = df["Symbol"] + ".NS"
    df["Sector"] = df["Industry"].map(INDIA_INDUSTRY_TO_SECTOR).fillna("Finance")
    return df


def load_index_constituents(csv_name):
    for base in [BASE_DIR, BASE_DIR1]:
        path = os.path.join(base, csv_name)
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                df.columns = df.columns.str.strip()
                return [s.strip() + ".NS" for s in df["Symbol"].tolist()]
            except Exception:
                pass
    return []


def load_sector_constituents():
    result = {}
    for sname, cfg in INDIA_SECTORS.items():
        csvf = cfg.get("csv")
        if not csvf:
            continue
        syms = load_index_constituents(csvf)
        if syms:
            result[sname] = syms
    return result


def fetch_prices(symbols, days=PERIOD_DAYS):
    end   = datetime.today() + timedelta(days=1)
    start = end - timedelta(days=days + 1)
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
                low_d = raw["Low"]   if "Low"   in raw.columns.get_level_values(0) else pd.DataFrame()
            else:
                close = raw[["Close"]] if "Close" in raw.columns else pd.DataFrame()
                low_d = raw[["Low"]]   if "Low"   in raw.columns else pd.DataFrame()
                if len(batch) == 1 and not close.empty:
                    close.columns = [batch[0]]
                    if not low_d.empty:
                        low_d.columns = [batch[0]]
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

    # Also fetch Low prices for SL calculations
    low_data_dict = {}
    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i: i + BATCH_SIZE]
        try:
            raw = yf.download(
                tickers=batch, start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                auto_adjust=True, progress=False, threads=True,
            )
            if isinstance(raw.columns, pd.MultiIndex):
                low_d = raw["Low"] if "Low" in raw.columns.get_level_values(0) else pd.DataFrame()
            else:
                low_d = raw[["Low"]] if "Low" in raw.columns else pd.DataFrame()
                if len(batch) == 1 and not low_d.empty:
                    low_d.columns = [batch[0]]
            for sym in batch:
                if sym in low_d.columns:
                    low_col = low_d[sym]
                    if isinstance(low_col, pd.DataFrame): low_col = low_col.squeeze()
                    l = normalize_series(low_col.dropna())
                    if len(l) >= 5:
                        low_data_dict[sym] = l
        except Exception:
            pass
        if i + BATCH_SIZE < len(symbols):
            time.sleep(BATCH_DELAY)

    print(f"    ✅ Prices: {len(all_data)}/{len(symbols)}" +
          (f" | Failed: {len(failed)}" if failed else ""))
    close_df = pd.DataFrame(all_data).sort_index() if all_data else pd.DataFrame()
    low_df   = pd.DataFrame(low_data_dict).sort_index() if low_data_dict else pd.DataFrame()
    return close_df, low_df


def fetch_sector_indices():
    result = {}
    const  = load_sector_constituents()
    for sname, cfg in INDIA_SECTORS.items():
        yahoo_sym = cfg.get("yahoo")
        if yahoo_sym:
            try:
                raw = yf.download(yahoo_sym, period=f"{PERIOD_DAYS}d",
                                  auto_adjust=True, progress=False)
                if len(raw) >= 22:
                    close_col = raw["Close"]
                    if isinstance(close_col, pd.DataFrame):
                        close_col = close_col.squeeze()
                    result[sname] = normalize_series(close_col.dropna())
                    continue
            except Exception:
                pass
        if sname in const and const[sname]:
            try:
                raw = yf.download(const[sname][:30], period=f"{PERIOD_DAYS}d",
                                  auto_adjust=True, progress=False)
                if isinstance(raw.columns, pd.MultiIndex):
                    cls = raw["Close"]
                else:
                    cls = raw[["Close"]]
                cls = cls.dropna(how="all")
                if len(cls) >= 22:
                    norm = cls / cls.iloc[0] * 1000
                    result[sname] = normalize_series(norm.mean(axis=1))
            except Exception:
                pass
    print(f"  ✅ Sector indices: {len(result)}/{len(INDIA_SECTORS)}")
    return result


# ── Technical indicator functions ─────────────────────────────────────────────

def calc_rs(stock, benchmark, period):
    try:
        s = normalize_series(stock.dropna())
        b = normalize_series(benchmark.dropna())
        common = s.index.intersection(b.index)
        if len(common) < period + 1:
            return np.nan
        s = s.loc[common]
        b = b.loc[common]
        s_cur, s_past = float(s.iloc[-1]), float(s.iloc[-(period + 1)])
        b_cur, b_past = float(b.iloc[-1]), float(b.iloc[-(period + 1)])
        if s_past == 0 or b_past == 0 or b_cur == 0:
            return np.nan
        return (s_cur / s_past) / (b_cur / b_past) - 1
    except Exception:
        return np.nan


def calc_hl_days(stock, lookback):
    try:
        s = normalize_series(stock.dropna())
        if len(s) < lookback:
            return np.nan, np.nan
        recent = s.iloc[-lookback:]
        last   = s.index[-1]
        return int((last - recent.idxmax()).days), int((last - recent.idxmin()).days)
    except Exception:
        return np.nan, np.nan


def pct_from_high(stock, lookback=252):
    try:
        recent = stock.iloc[-lookback:]
        return (stock.iloc[-1] / recent.max() - 1) * 100
    except Exception:
        return np.nan


def calc_rsi(series, period=14):
    try:
        delta = series.diff().dropna()
        gain  = delta.clip(lower=0)
        loss  = (-delta).clip(lower=0)
        avg_g = gain.rolling(period).mean().iloc[-1]
        avg_l = loss.rolling(period).mean().iloc[-1]
        if avg_l == 0:
            return 100.0
        return round(100 - (100 / (1 + avg_g / avg_l)), 1)
    except Exception:
        return np.nan


def calc_sma_latest(series, period):
    try:
        if len(series) < period:
            return np.nan
        return float(series.dropna().iloc[-period:].mean())
    except Exception:
        return np.nan


def get_sma_signals(prices):
    cur    = float(prices.iloc[-1]) if len(prices) > 0 else np.nan
    sma20  = calc_sma_latest(prices, 20)
    sma50  = calc_sma_latest(prices, 50)
    sma100 = calc_sma_latest(prices, 100)
    sma200 = calc_sma_latest(prices, 200)
    rsi    = calc_rsi(prices, 14)

    def above(p, s):
        return (not np.isnan(p)) and (s is not None) and (not np.isnan(s)) and (p > s)

    sma_score = sum([
        1 if above(cur, sma20)  else 0,
        1 if above(cur, sma50)  else 0,
        1 if above(cur, sma100) else 0,
        1 if above(cur, sma200) else 0,
    ])
    return {
        "RSI_14":     rsi,
        "Abv_SMA20":  "✓" if above(cur, sma20)  else "✗",
        "Abv_SMA50":  "✓" if above(cur, sma50)  else "✗",
        "Abv_SMA100": "✓" if above(cur, sma100) else "✗",
        "Abv_SMA200": "✓" if above(cur, sma200) else "✗",
        "SMA_Score":  sma_score,
    }


# ── Risk Management functions ─────────────────────────────────────────────────

def calc_atr(close_s, low_s=None, period=14):
    try:
        c = close_s.dropna()
        l = low_s.reindex(c.index).fillna(c) if low_s is not None else c
        h = c.rolling(2, min_periods=1).max()
        prev_c = c.shift(1)
        tr     = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
        atr    = tr.ewm(span=period, adjust=False, min_periods=period).mean()
        atr_v  = float(atr.iloc[-1])
        cur    = float(c.iloc[-1])
        return round(atr_v, 2), round(atr_v / cur * 100, 2) if cur > 0 else np.nan
    except Exception:
        return np.nan, np.nan


def calc_swing_sl(close_s, low_s=None, lookbacks=(5, 10, 22)):
    result = {}
    try:
        c = close_s.dropna()
        l = low_s.reindex(c.index).fillna(c) if low_s is not None else c
        cur = float(c.iloc[-1])
        if cur <= 0:
            return result
        for n in lookbacks:
            if len(l) < n:
                result[f"SL_{n}d_Price"] = np.nan
                result[f"SL_{n}d%"]      = np.nan
                continue
            swing_low = float(l.iloc[-n:].min())
            result[f"SL_{n}d_Price"] = round(swing_low, 2)
            result[f"SL_{n}d%"]      = round((cur - swing_low) / cur * 100, 2)
    except Exception:
        pass
    return result


def calc_sl_grade(rec_sl_pct):
    if rec_sl_pct is None or (isinstance(rec_sl_pct, float) and np.isnan(rec_sl_pct)):
        return "—"
    if   rec_sl_pct <= 3:  return "A"
    elif rec_sl_pct <= 5:  return "B"
    elif rec_sl_pct <= 8:  return "C"
    elif rec_sl_pct <= 12: return "D"
    else:                   return "F"


def calc_sl_bonus(rec_sl_pct, rr_ratio, h_day):
    bonus = 0.0
    if isinstance(rec_sl_pct, (int, float)) and not np.isnan(rec_sl_pct):
        if   rec_sl_pct <= 2: bonus += 4.0
        elif rec_sl_pct <= 4: bonus += 3.0
        elif rec_sl_pct <= 6: bonus += 2.0
        elif rec_sl_pct <= 9: bonus += 1.0
    if isinstance(rr_ratio, (int, float)) and not np.isnan(rr_ratio):
        if   rr_ratio >= 5: bonus += 2.0
        elif rr_ratio >= 3: bonus += 1.0
    if isinstance(h_day, (int, float)) and not np.isnan(h_day):
        if   h_day == 0: bonus += 2.0
        elif h_day <= 3: bonus += 1.5
        elif h_day <= 7: bonus += 1.0
        elif h_day <= 10:bonus += 0.5
    return round(bonus, 1)


def calc_risk_reward(from_52w_high_pct, rec_sl_pct):
    try:
        target = abs(from_52w_high_pct)
        if rec_sl_pct is None or (isinstance(rec_sl_pct, float) and np.isnan(rec_sl_pct)) or rec_sl_pct <= 0:
            return np.nan
        return round(target / rec_sl_pct, 2) if target > 0 else 0.0
    except Exception:
        return np.nan


# ── Chart Pattern Detector ────────────────────────────────────────────────────

CFG_PAT = {
    "pivot_order": 5, "tolerance": 0.03,
    "min_pattern_bars": 10, "max_pattern_bars": 120,
    "flag_pole_pct": 0.07, "cup_depth_min": 0.10,
    "cup_depth_max": 0.50, "lookback_days": 180,
}

@dataclass
class PatternSignal:
    symbol: str; pattern: str; direction: str
    end_date: str; entry_price: float; stop_loss: float
    target: float; risk_reward: float; confidence: str
    win_rate_est: str; notes: str = ""


def _near_eq(a, b, tol=0.03):
    return abs(a - b) / max(abs(a), abs(b), 1e-9) <= tol


class PatternDetector:
    def __init__(self, df, symbol):
        lb = CFG_PAT["lookback_days"]
        self.df  = df.tail(lb).copy().reset_index(drop=False)
        self.sym = symbol
        self.signals: List[PatternSignal] = []
        close_arr = self.df["Close"].values
        high_arr  = self.df["High"].values  if "High"  in self.df.columns else close_arr
        low_arr   = self.df["Low"].values   if "Low"   in self.df.columns else close_arr
        self.highs = argrelextrema(high_arr, np.greater_equal, order=CFG_PAT["pivot_order"])[0]
        self.lows  = argrelextrema(low_arr,  np.less_equal,    order=CFG_PAT["pivot_order"])[0]

    def _date(self, idx):
        col = "Date" if "Date" in self.df.columns else self.df.columns[0]
        return str(self.df[col].iloc[min(idx, len(self.df)-1)])[:10]

    def _c(self, i): return float(self.df["Close"].iloc[i])
    def _h(self, i): c = "High" if "High" in self.df.columns else "Close"; return float(self.df[c].iloc[i])
    def _l(self, i): c = "Low"  if "Low"  in self.df.columns else "Close"; return float(self.df[c].iloc[i])

    def _add(self, pat, direction, ei, entry, sl, target, conf, wr, notes=""):
        rr = abs(target - entry) / max(abs(entry - sl), 1e-9)
        self.signals.append(PatternSignal(
            symbol=self.sym, pattern=pat, direction=direction,
            end_date=self._date(ei), entry_price=round(entry, 2),
            stop_loss=round(sl, 2), target=round(target, 2),
            risk_reward=round(rr, 2), confidence=conf, win_rate_est=wr, notes=notes,
        ))

    def detect_double_bottom(self):
        for i in range(len(self.lows) - 1):
            l1, l2 = self.lows[i], self.lows[i+1]
            if not (CFG_PAT["min_pattern_bars"] <= l2-l1 <= CFG_PAT["max_pattern_bars"]): continue
            p1, p2 = self._l(l1), self._l(l2)
            if not _near_eq(p1, p2): continue
            neck = float(self.df["High"].iloc[l1:l2+1].max() if "High" in self.df.columns else self.df["Close"].iloc[l1:l2+1].max())
            self._add("Double Bottom","BULLISH",l2, neck*1.005, min(p1,p2)*0.99, neck+(neck-min(p1,p2)), "HIGH","~65%", f"Neckline≈{neck:.0f}")

    def detect_double_top(self):
        for i in range(len(self.highs) - 1):
            h1, h2 = self.highs[i], self.highs[i+1]
            if not (CFG_PAT["min_pattern_bars"] <= h2-h1 <= CFG_PAT["max_pattern_bars"]): continue
            p1, p2 = self._h(h1), self._h(h2)
            if not _near_eq(p1, p2): continue
            neck = float(self.df["Low"].iloc[h1:h2+1].min() if "Low" in self.df.columns else self.df["Close"].iloc[h1:h2+1].min())
            self._add("Double Top","BEARISH",h2, neck*0.995, max(p1,p2)*1.01, neck-(max(p1,p2)-neck), "HIGH","~65%", f"Neckline≈{neck:.0f}")

    def detect_cup_handle(self):
        for i in range(len(self.highs) - 1):
            left, right = self.highs[i], self.highs[i+1]
            if not (30 <= right-left <= CFG_PAT["max_pattern_bars"]): continue
            seg = self.df.iloc[left:right+1]
            if len(seg) == 0: continue
            lc  = "Low" if "Low" in seg.columns else "Close"
            bot = float(seg[lc].iloc[seg[lc].values.argmin()])
            top_l, top_r = self._h(left), self._h(right)
            if not _near_eq(top_l, top_r, 0.06): continue
            depth = (top_l - bot) / top_l
            if not (CFG_PAT["cup_depth_min"] <= depth <= CFG_PAT["cup_depth_max"]): continue
            if right + 3 >= len(self.df): continue
            lc2 = "Low" if "Low" in self.df.columns else "Close"
            handle_low = float(self.df[lc2].iloc[right:min(right+15, len(self.df))].min())
            if (top_r - handle_low) / top_r > 0.15: continue
            self._add("Cup & Handle","BULLISH",right, top_r*1.005, handle_low*0.99, top_r*1.005+(top_r-bot), "HIGH" if depth<0.35 else "MEDIUM","~65%", f"Depth={depth:.1%}")

    def detect_bull_flag(self):
        n = len(self.df)
        for i in range(n - 25):
            pe = i + 10
            if pe >= n: continue
            pm = (self._c(pe) - self._c(i)) / self._c(i)
            if pm < CFG_PAT["flag_pole_pct"]: continue
            fe = pe + 10
            if fe >= n: continue
            hc = "High" if "High" in self.df.columns else "Close"
            lc = "Low"  if "Low"  in self.df.columns else "Close"
            fh = float(self.df[hc].iloc[pe:fe+1].max())
            fl = float(self.df[lc].iloc[pe:fe+1].min())
            fr = (fh - self._c(fe)) / fh
            if not (0.01 <= fr <= 0.12): continue
            self._add("Bull Flag","BULLISH",fe, fh*1.003, fl*0.99, fh*1.003+(self._c(pe)-self._c(i)), "MEDIUM","~67%", f"Pole={pm:.1%}")

    def detect_vcp(self):
        for end in range(60, len(self.df)):
            start = end - 60
            seg   = self.df.iloc[start:end+1]
            q     = max(1, len(seg) // 4)
            hc = "High" if "High" in seg.columns else "Close"
            lc = "Low"  if "Low"  in seg.columns else "Close"
            swings = [float(seg.iloc[qi*q:(qi+1)*q][hc].max()) - float(seg.iloc[qi*q:(qi+1)*q][lc].min()) for qi in range(4)]
            if len(swings) < 4 or not all(swings[j] > swings[j+1] for j in range(3)): continue
            res     = float(seg[hc].max())
            base_lo = float(seg[lc].min())
            sl_base = float(seg[lc].iloc[-q:].min())
            self._add("VCP","BULLISH",end, res*1.005, sl_base*0.99, res*1.005+(res-base_lo)*0.75, "HIGH","~70%","Vol Dry-up + Contractions")

    def detect_ascending_triangle(self):
        for end in range(30, len(self.df)):
            start = end - 30
            seg_h = [h for h in self.highs if start <= h <= end]
            seg_l = [l for l in self.lows  if start <= l <= end]
            if len(seg_h) < 2 or len(seg_l) < 2: continue
            tops = [self._h(h) for h in seg_h]
            bots = [self._l(l) for l in seg_l]
            if (max(tops)-min(tops))/max(tops) > CFG_PAT["tolerance"]: continue
            if bots[-1] <= bots[0]: continue
            res = np.mean(tops)
            self._add("Ascending Triangle","BULLISH",end, res*1.005, min(bots)*0.99, res+(res-min(bots)), "HIGH","~68%", f"Resistance={res:.0f}")

    def run(self):
        self.detect_double_bottom()
        self.detect_double_top()
        self.detect_cup_handle()
        self.detect_bull_flag()
        self.detect_vcp()
        self.detect_ascending_triangle()
        seen = {}
        for s in self.signals:
            if s.pattern not in seen or s.end_date > seen[s.pattern].end_date:
                seen[s.pattern] = s
        return [s for s in seen.values() if s.risk_reward >= 1.5]


def run_pattern_detection(ohlcv_dict):
    patterns_by_sym = {}
    patterns_list   = []
    for sym, df in ohlcv_dict.items():
        if len(df) < 60: continue
        if "Date" not in df.columns:
            df = df.reset_index()
            df = df.rename(columns={df.columns[0]: "Date"})
        try:
            signals = PatternDetector(df, sym).run()
            if signals:
                patterns_by_sym[sym] = signals
                patterns_list.extend(signals)
        except Exception:
            pass
    return patterns_by_sym, patterns_list


def fetch_ohlcv_batch(symbols, days=PERIOD_DAYS):
    result = {}
    end    = datetime.today() + timedelta(days=1)
    start  = end - timedelta(days=days + 1)
    for i in range(0, len(symbols), 50):
        batch = symbols[i:i+50]
        try:
            raw = yf.download(
                tickers=batch, start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                auto_adjust=True, progress=False, threads=True,
            )
            if raw.empty: continue
            if isinstance(raw.columns, pd.MultiIndex):
                for sym in batch:
                    frames = {}
                    for pc in ["Open", "High", "Low", "Close", "Volume"]:
                        if pc in raw.columns.get_level_values(0) and sym in raw[pc].columns:
                            col_data = raw[pc][sym]
                            if isinstance(col_data, pd.DataFrame): col_data = col_data.squeeze()
                            frames[pc] = normalize_series(col_data.dropna())
                    if "Close" in frames and len(frames["Close"]) >= 60:
                        df_sym = pd.DataFrame(frames)
                        df_sym.dropna(subset=["Close", "High", "Low"], inplace=True)
                        if len(df_sym) >= 60:
                            result[sym] = df_sym
        except Exception:
            pass
        if i + 50 < len(symbols):
            time.sleep(BATCH_DELAY)
    return result


# ── Analysis functions ────────────────────────────────────────────────────────

def analyse_sectors(index_prices, sector_prices):
    rows = []
    for sname, s_prices in sector_prices.items():
        row = {"Sector": sname}
        for p in [2, 5, 22, 34, 55, 66, 89, 100, 120, 150, 180, 200, 252]:
            rs = calc_rs(s_prices, index_prices, p)
            row[f"RS_{p}d"] = round(rs * 100, 2) if rs == rs else np.nan
        r22, r55 = row.get("RS_22d", np.nan), row.get("RS_55d", np.nan)
        row["Signal"] = ("Buy"  if r22 == r22 and r55 == r55 and r22 > 0 and r55 > 0 else
                         "Sell" if r22 == r22 and r55 == r55 and r22 < 0 and r55 < 0 else "NA")
        h, l = calc_hl_days(s_prices, HL_LOOKBACK)
        row["H_Day"] = h; row["L_Day"] = l
        row["RSI_14"] = calc_rsi(s_prices, 14)
        rows.append(row)
    df = pd.DataFrame(rows).sort_values("RS_55d", ascending=False).reset_index(drop=True)
    df.insert(0, "Rank", df.index + 1)
    return df


def calc_rotation_row(group_stocks, price_data, index_prices, name):
    n_stocks = len(group_stocks)
    rs55_a = rsi_a = sma20_a = sma50_a = sma100_a = 0
    z1 = {"High": 0, "Mid": 0, "Low": 0}
    z3 = {"High": 0, "Mid": 0, "Low": 0}
    z6 = {"High": 0, "Mid": 0, "Low": 0}
    valid = 0

    for sym in group_stocks:
        if sym not in price_data.columns: continue
        p = price_data[sym].dropna()
        if len(p) < 55: continue
        valid += 1
        cur = float(p.iloc[-1])
        rs55 = calc_rs(p, index_prices, 55)
        if rs55 == rs55 and rs55 > 0: rs55_a += 1
        rsi = calc_rsi(p, 14)
        if rsi == rsi and rsi > 50: rsi_a += 1
        for period, cnt_dict, s_a in [(20, z1, sma20_a), (50, z3, sma50_a), (100, z6, sma100_a)]:
            sma = calc_sma_latest(p, period)
            pass
        s20 = calc_sma_latest(p, 20);  sma20_a  += (1 if s20 and not np.isnan(s20) and cur > s20 else 0)
        s50 = calc_sma_latest(p, 50);  sma50_a  += (1 if s50 and not np.isnan(s50) and cur > s50 else 0)
        s100= calc_sma_latest(p, 100); sma100_a += (1 if s100 and not np.isnan(s100) and cur > s100 else 0)

        def zone(prices_s, n):
            if len(prices_s) < n: return "Low"
            hi, lo = float(prices_s.iloc[-n:].max()), float(prices_s.iloc[-n:].min())
            r = hi - lo
            if r <= 0: return "Mid"
            pos = (cur - lo) / r
            return "High" if pos >= 0.67 else ("Mid" if pos >= 0.33 else "Low")

        z1[zone(p, 22)] += 1
        z3[zone(p, 55)] += 1
        z6[zone(p, 120)]+= 1

    if valid == 0: return None

    def pct(n): return round(n / valid * 100)
    def score(z): return round((z["High"]*2 + z["Mid"]) / max(valid*2, 1) * 100)

    s1, s3, s6 = score(z1), score(z3), score(z6)
    def zone_label(s): return "Bullish" if s >= 60 else ("Neutral" if s >= 40 else "Bearish")

    return {
        "Name": name, "Stocks": n_stocks, "Valid_Data": valid,
        "RS55_Above%": pct(rs55_a), "RSI50_Above%": pct(rsi_a),
        "SMA20_Above%": pct(sma20_a), "SMA50_Above%": pct(sma50_a), "SMA100_Above%": pct(sma100_a),
        "1M_LowZone": z1["Low"], "1M_MidZone": z1["Mid"], "1M_HighZone": z1["High"],
        "3M_LowZone": z3["Low"], "3M_MidZone": z3["Mid"], "3M_HighZone": z3["High"],
        "1M_Score": s1, "1M_Zone": zone_label(s1),
        "3M_Score": s3, "3M_Zone": zone_label(s3),
        "6M_Score": s6, "6M_Zone": zone_label(s6),
    }


def analyse_stocks(universe, price_data, index_prices, sector_prices, low_data=None):
    rows  = []
    total = len(universe)
    p1, p2 = SIGNAL_PERIODS[0], SIGNAL_PERIODS[1]

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
        rs_idx    = {p: calc_rs(prices, index_prices, p) for p in RS_PERIODS}
        rs_sec    = {p: (calc_rs(prices, s_prices, p) if s_prices is not None else np.nan) for p in SIGNAL_PERIODS}

        r_i1, r_i2 = rs_idx.get(p1, np.nan), rs_idx.get(p2, np.nan)
        r_s1, r_s2 = rs_sec.get(p1, np.nan), rs_sec.get(p2, np.nan)
        vi, vs     = (r_i1==r_i1 and r_i2==r_i2), (r_s1==r_s1 and r_s2==r_s2)

        if vi and vs:
            signal = ("Buy"  if r_i1>0 and r_i2>0 and r_s1>0 and r_s2>0 else
                      "Sell" if r_i1<0 and r_i2<0 and r_s1<0 and r_s2<0 else "NA")
        elif vi:
            signal = "Buy" if r_i1>0 and r_i2>0 else ("Sell" if r_i1<0 and r_i2<0 else "NA")
        else:
            signal = "NA"

        h_day, l_day = calc_hl_days(prices, HL_LOOKBACK)
        from_52w     = pct_from_high(prices, 252)
        tech         = get_sma_signals(prices)

        low_s = (low_data[sym].dropna() if low_data is not None and sym in low_data.columns else None)
        sl_dict   = calc_swing_sl(prices, low_s)
        sl_10d    = sl_dict.get("SL_10d%", np.nan)
        sl_22d    = sl_dict.get("SL_22d%", np.nan)
        _, atr_p  = calc_atr(prices, low_s)
        rec_sl    = round(max(sl_10d, 1.5*atr_p), 2) if (sl_10d==sl_10d and atr_p==atr_p) else sl_10d
        rr_ratio  = calc_risk_reward(from_52w if from_52w==from_52w else np.nan, rec_sl)
        sl_grade  = calc_sl_grade(rec_sl)
        sl_bonus  = calc_sl_bonus(rec_sl, rr_ratio, h_day if h_day==h_day else np.nan)

        rows.append({
            "Symbol":         orig_sym,
            "TV_Symbol":      f"NSE:{orig_sym},",
            "Company Name":   name,
            "Industry":       industry,
            "Sector":         sector,
            "Price":          round(cur_price, 2),
            f"RS_{p1}d_Idx%": round(r_i1*100,2) if r_i1==r_i1 else np.nan,
            f"RS_{p2}d_Idx%": round(r_i2*100,2) if r_i2==r_i2 else np.nan,
            f"RS_{p1}d_Sec%": round(r_s1*100,2) if r_s1==r_s1 else np.nan,
            f"RS_{p2}d_Sec%": round(r_s2*100,2) if r_s2==r_s2 else np.nan,
            "Signal":          signal,
            "RS_120d_Idx%":   round(rs_idx.get(120,np.nan)*100,2) if rs_idx.get(120,np.nan)==rs_idx.get(120,np.nan) else np.nan,
            "RS_252d_Idx%":   round(rs_idx.get(252,np.nan)*100,2) if rs_idx.get(252,np.nan)==rs_idx.get(252,np.nan) else np.nan,
            "RSI_14":          tech["RSI_14"],
            "Abv_SMA20":       tech["Abv_SMA20"],
            "Abv_SMA50":       tech["Abv_SMA50"],
            "Abv_SMA100":      tech["Abv_SMA100"],
            "Abv_SMA200":      tech["Abv_SMA200"],
            "SMA_Score":       tech["SMA_Score"],
            "From_52W_High%": round(from_52w,1) if from_52w==from_52w else np.nan,
            "H_Day":           h_day,
            "L_Day":           l_day,
            "SL_5d%":          sl_dict.get("SL_5d%",  np.nan),
            "SL_10d%":         sl_10d,
            "SL_22d%":         sl_22d,
            "SL_22d_Price":    sl_dict.get("SL_22d_Price", np.nan),
            "ATR_Pct%":        atr_p,
            "Rec_SL%":         rec_sl,
            "Target%":         round(abs(from_52w),1) if from_52w==from_52w else np.nan,
            "RR_Ratio":        rr_ratio,
            "SL_Grade":        sl_grade,
            "SL_Bonus":        sl_bonus,
        })
        if (i + 1) % 100 == 0:
            print(f"    … {i+1}/{total}")

    df = pd.DataFrame(rows)
    if df.empty: return df

    rs1_col, rs2_col = f"RS_{p1}d_Idx%", f"RS_{p2}d_Idx%"

    def pct_rank(s):
        v = s.dropna().values
        if len(v) == 0: return pd.Series(np.nan, index=s.index)
        return pd.Series([round(np.sum(v<=x)/len(v)*100,1) for x in s], index=s.index)

    df["RS_Pctile_P1"] = pct_rank(df[rs1_col])
    df["RS_Pctile_P2"] = pct_rank(df[rs2_col])

    score = pd.Series(0.0, index=df.index)
    wt    = pd.Series(0.0, index=df.index)
    for col, w in [(rs1_col,0.40),(rs2_col,0.30),("RS_120d_Idx%",0.20),("RS_252d_Idx%",0.10)]:
        m = df[col].notna()
        score[m] += df.loc[m, col] * w
        wt[m]    += w
    df["RS_Score"]      = (score / wt.replace(0, np.nan)).round(2)
    df["Enhanced_Score"]= (df["RS_Score"].fillna(0) + df["SL_Bonus"].fillna(0)).round(2)
    df["SL_Rank"]       = df["Rec_SL%"].rank(ascending=True, method="min", na_option="bottom").fillna(999).astype(int)
    df["RS_Rank"]       = df["RS_Score"].rank(ascending=False, method="min").fillna(999).astype(int)
    df["Enhanced_Rank"] = df["Enhanced_Score"].rank(ascending=False, method="min").fillna(999).astype(int)

    sig_ord = {"Buy": 1, "NA": 2, "Sell": 3}
    df["_o"] = df["Signal"].map(sig_ord).fillna(2)
    df = df.sort_values(["_o", "Enhanced_Score"], ascending=[True, False]).drop(columns=["_o"]).reset_index(drop=True)

    buys  = (df["Signal"]=="Buy").sum()
    sells = (df["Signal"]=="Sell").sum()
    print(f"  ✅ {len(df)} stocks | Buy:{buys} Sell:{sells} NA:{len(df)-buys-sells}")
    return df


def analyse_sector_performance(sector_prices, index_prices):
    rows = []
    for sec, prices in sector_prices.items():
        if len(prices) < 22: continue
        row = {"Sector": sec}
        # Ensure prices is always a plain 1-D Series (not a DataFrame column)
        if isinstance(prices, pd.DataFrame):
            prices = prices.squeeze()

        for label, days in [("1M(22d)%",22),("3M(66d)%",66),("6M(132d)%",132),("12M(252d)%",252)]:
            if len(prices) >= days+1:
                cur_v  = float(prices.iloc[-1])
                past_v = float(prices.iloc[-(days+1)])
                row[label] = round((cur_v/past_v-1)*100,2) if past_v != 0 else np.nan
            else:
                row[label] = np.nan
        try:
            jan1 = pd.Timestamp(f"{datetime.now().year}-01-01")
            past_ytd = prices[prices.index <= jan1]
            if len(past_ytd) > 0:
                row["YTD%"] = round((float(prices.iloc[-1])/float(past_ytd.iloc[-1])-1)*100,2)
            else:
                row["YTD%"] = np.nan
        except Exception:
            row["YTD%"] = np.nan
        for label, days in [("RS_1M%",22),("RS_3M%",55),("RS_6M%",120),("RS_12M%",252)]:
            rs = calc_rs(prices, index_prices, days)
            row[label] = round(rs*100,2) if rs==rs else np.nan
        rows.append(row)
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "3M(66d)%" in df.columns:
        df = df.sort_values("3M(66d)%", ascending=False)
    df.insert(0, "Rank", range(1, len(df)+1))
    return df.reset_index(drop=True)


def fetch_fii_dii_data():
    print("  Fetching FII/DII …")
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.nseindia.com/",
        "Accept":  "application/json",
    }
    try:
        session.get("https://www.nseindia.com", headers=headers, timeout=15)
        time.sleep(1.5)
        session.get("https://www.nseindia.com/market-data/fii-dii-trade-participation", headers=headers, timeout=15)
        time.sleep(1)
        resp = session.get("https://www.nseindia.com/api/fiidiiTradeReact", headers=headers, timeout=15)
        if resp.status_code == 200:
            payload = resp.json()
            data_list = payload if isinstance(payload, list) else payload.get("data", [])
            rows = []
            for item in data_list:
                try:
                    def _v(*keys):
                        for k in keys:
                            v = item.get(k)
                            if v is not None:
                                return float(str(v).replace(",","") or 0)
                        return 0.0
                    rows.append({
                        "Date":     item.get("date",""),
                        "FII_Buy":  _v("fiiBuyValue"),
                        "FII_Sell": _v("fiiSellValue"),
                        "FII_Net":  _v("fiiNetValue"),
                        "DII_Buy":  _v("diiBuyValue"),
                        "DII_Sell": _v("diiSellValue"),
                        "DII_Net":  _v("diiNetValue"),
                    })
                except Exception:
                    pass
            if rows:
                df = pd.DataFrame(rows)
                df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
                df = df.dropna(subset=["Date"]).sort_values("Date", ascending=False)
                df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")
                print(f"  ✅ FII/DII: {len(df)} days")
                return df
    except Exception as e:
        print(f"  ⚠ FII/DII fetch error: {e}")

    return pd.DataFrame({
        "Date":["N/A"], "FII_Buy":[np.nan], "FII_Sell":[np.nan], "FII_Net":[np.nan],
        "DII_Buy":[np.nan], "DII_Sell":[np.nan], "DII_Net":[np.nan],
        "Note":["Visit https://www.nseindia.com/market-data/fii-dii-trade-participation"],
    })


# ══════════════════════════════════════════════════════════════════════════════
#  ❹  GOOGLE SHEETS EXPORT LAYER
# ══════════════════════════════════════════════════════════════════════════════

def build_dashboard_df(stock_df, sector_df, run_time):
    """Build a plain DataFrame that represents the dashboard summary."""
    p2 = SIGNAL_PERIODS[1]
    rows = []

    rows.append(["INDIA RS ANALYSIS SYSTEM v3.0", run_time])
    rows.append(["", ""])
    rows.append(["── UNIVERSE ──", ""])
    rows.append(["Stocks Analysed",  len(stock_df)])
    rows.append(["Buy Signals",       (stock_df["Signal"]=="Buy").sum()])
    rows.append(["Sell Signals",      (stock_df["Signal"]=="Sell").sum()])
    rows.append(["NA",                (stock_df["Signal"]=="NA").sum()])

    if "SL_Grade" in stock_df.columns:
        buy_df = stock_df[stock_df["Signal"]=="Buy"]
        rows.append(["", ""])
        rows.append(["── SL QUALITY (Buy Stocks) ──", ""])
        for g, desc in [("A","Tight <=3%"),("B","Good 3-5%"),("C","Moderate 5-8%"),("D","Wide 8-12%"),("F","Avoid >12%")]:
            cnt = (buy_df["SL_Grade"] == g).sum()
            rows.append([f"Grade {g} ({desc})", cnt])

        rr3 = (buy_df["RR_Ratio"] >= 3).sum()
        rows.append(["", ""])
        rows.append(["R:R >= 3x (Excellent)", rr3])

    rows.append(["", ""])
    rows.append(["── SECTOR RS RANKING ──", ""])
    for _, r in sector_df.iterrows():
        rows.append([
            f"#{int(r['Rank'])} {r['Sector']}",
            f"{r['Signal']} | RS_22d: {r.get('RS_22d',0):+.1f}% | RS_55d: {r.get('RS_55d',0):+.1f}%"
        ])

    rows.append(["", ""])
    rows.append(["── TOP 10 BUY STOCKS (Enhanced_Score) ──", ""])
    top_buy = stock_df[stock_df["Signal"]=="Buy"].nlargest(10, "Enhanced_Score")
    for _, r in top_buy.iterrows():
        rr  = r.get("RR_Ratio", float("nan"))
        rows.append([
            r["Symbol"],
            (f"EnhSc:{r.get('Enhanced_Score',0):.1f} | RS55:{r.get(f'RS_{p2}d_Idx%',0):.1f}% | "
             f"RSI:{r.get('RSI_14','—')} | SL:{r.get('Rec_SL%','—')}% | "
             f"Grd:{r.get('SL_Grade','—')} | RR:{f'{rr:.1f}x' if rr==rr else '—'}")
        ])

    rows.append(["", ""])
    rows.append(["── PRIME SETUPS (Grade A/B + R:R>=3x) ──", ""])
    if "SL_Grade" in stock_df.columns:
        prime = stock_df[(stock_df["Signal"]=="Buy") &
                          (stock_df["SL_Grade"].isin(["A","B"])) &
                          (stock_df["RR_Ratio"] >= 3)]
        for _, r in prime.nlargest(10, "Enhanced_Score").iterrows():
            rr = r.get("RR_Ratio", float("nan"))
            rows.append([
                r["Symbol"],
                (f"EnhSc:{r.get('Enhanced_Score',0):.1f} | Sector:{r.get('Sector','')} | "
                 f"SL:{r.get('Rec_SL%','—')}% | RR:{f'{rr:.1f}x' if rr==rr else '—'} | "
                 f"H_Day:{r.get('H_Day','—')}")
            ])

    rows.append(["", ""])
    rows.append(["── TV WATCHLIST — Paste into TradingView ──", ""])
    rows.append(["HOW TO USE", "Copy the TV Symbols cell → TradingView → Watchlist → Import"])
    buy_stocks = stock_df[stock_df["Signal"]=="Buy"]
    all_tv     = "".join(buy_stocks["TV_Symbol"].tolist())
    top20_tv   = "".join(buy_stocks.nlargest(20,"Enhanced_Score")["TV_Symbol"].tolist())
    rows.append(["TV All Buy Stocks",   all_tv])
    rows.append(["TV Top-20 Enhanced",  top20_tv])

    rows.append(["", ""])
    rows.append(["── COLUMN GUIDE ──", ""])
    guide = [
        ("RS_Nd_Idx%",    "RS vs Nifty over N days. >0 = outperforming index"),
        ("Rec_SL%",       "Recommended SL = max(10d-swing-low, 1.5xATR). Place your stop here"),
        ("SL_Grade",      "A=Tight(<=3%) B=Good(3-5%) C=Moderate D=Wide F=Avoid"),
        ("RR_Ratio",      "Risk:Reward = Target-to-52W-high / Rec_SL%. Trade only if >=2x"),
        ("SL_Bonus",      "0-8 pts added to RS_Score: tight SL + good RR + near highs"),
        ("Enhanced_Score","RS_Score + SL_Bonus. PRIMARY RANK. Sort by this for best entries"),
        ("H_Day",         "Days since 22-day high. 0 = stock at highs TODAY = ideal entry timing"),
        ("SL_22d_Price",  "Actual rupee price of your stop loss (22-day swing low)"),
    ]
    for k, v in guide:
        rows.append([k, v])

    return pd.DataFrame(rows, columns=["Key", "Value"])


def push_to_gsheets(spreadsheet, tab_key, tab_title, df,
                    header_bg="navy", apply_formatting=True):
    """
    Write a DataFrame to a Google Sheet tab.
    Handles creation, clearing, writing, and basic formatting.
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        print(f"    ⚠ Skipping '{tab_title}' (no data)")
        return

    print(f"    Writing '{tab_title}' ({len(df)} rows × {len(df.columns)} cols) …", end=" ")
    ws = get_or_create_worksheet(spreadsheet, tab_title, rows=max(len(df)+10, 500), cols=max(len(df.columns)+5, 30))

    clear_and_write_df(ws, df)
    time.sleep(0.8)   # respect quota

    if apply_formatting and len(df) > 0:
        try:
            apply_header_format(ws, len(df.columns), header_bg)
            time.sleep(0.3)
            freeze_header_row(ws)
            time.sleep(0.3)
        except Exception as e:
            print(f"(formatting err: {e})", end=" ")

    print("✓")


def apply_stock_sheet_formatting(ws, df):
    """Apply signal and SL/RS color formatting to stock sheets."""
    if df is None or df.empty:
        return

    n_rows = len(df)

    # Signal column
    for col_name, condition in [
        ("Signal",        "signal"),
        ("Abv_SMA20",     "signal"),
        ("Abv_SMA50",     "signal"),
        ("Abv_SMA100",    "signal"),
        ("Abv_SMA200",    "signal"),
    ]:
        apply_column_conditional_format(ws, df, col_name, n_rows, condition)
        time.sleep(0.2)

    # Percentage columns (green if positive, red if negative)
    pct_cols = [c for c in df.columns if
                any(x in c for x in ["%", "RS_", "Score", "Pctile"]) and
                c not in ["SL_Grade", "SL_Bonus"]]
    # Inverted (tighter = better) for SL columns
    sl_cols  = ["SL_5d%", "SL_10d%", "SL_22d%", "ATR_Pct%", "Rec_SL%"]

    for col in pct_cols[:8]:   # limit to 8 to avoid quota exhaustion
        is_inverted = col in sl_cols
        apply_column_conditional_format(ws, df, col, n_rows, "positive", invert=is_inverted)
        time.sleep(0.2)

    # SL Grade
    apply_column_conditional_format(ws, df, "SL_Grade", n_rows, "grade")
    time.sleep(0.2)

    # RR Ratio
    apply_column_conditional_format(ws, df, "RR_Ratio", n_rows, "rr")
    time.sleep(0.2)

    # SMA Score
    apply_column_conditional_format(ws, df, "SMA_Score", n_rows, "sma_score")
    time.sleep(0.2)


def export_to_gsheets(
    spreadsheet,
    stock_df, sector_df, drilldown_df, patterns_list,
    sector_rot_df, industry_rot_df, breadth_df, fii_dii_df, sector_perf_df,
    market="INDIA",
):
    """
    Master export function — writes all tabs to the Google Sheet.
    Replaces the openpyxl export_report() entirely.
    """
    run_time = datetime.now().strftime("%d %b %Y  %H:%M IST")
    p1 = SIGNAL_PERIODS[0]
    p2 = SIGNAL_PERIODS[1]

    buy_df  = stock_df[stock_df["Signal"] == "Buy"].copy()  if not stock_df.empty else pd.DataFrame()
    sell_df = stock_df[stock_df["Signal"] == "Sell"].copy() if not stock_df.empty else pd.DataFrame()

    print("\n  Pushing tabs to Google Sheets …")

    # ── 1. Dashboard ──────────────────────────────────────────────────────────
    dash_df = build_dashboard_df(stock_df, sector_df, run_time)
    push_to_gsheets(spreadsheet, "dashboard", TABS["dashboard"], dash_df,
                    header_bg="navy", apply_formatting=True)
    time.sleep(1)

    # ── 2. Sector RS ──────────────────────────────────────────────────────────
    push_to_gsheets(spreadsheet, "sector_rs", TABS["sector_rs"], sector_df, "navy")
    time.sleep(1)
    if not sector_df.empty:
        ws = spreadsheet.worksheet(TABS["sector_rs"])
        apply_column_conditional_format(ws, sector_df, "Signal", len(sector_df), "signal")
        time.sleep(0.5)

    # ── 3. Top By Sector ──────────────────────────────────────────────────────
    push_to_gsheets(spreadsheet, "top_by_sector", TABS["top_by_sector"], drilldown_df)
    time.sleep(1)

    # ── 4. Buy Stocks ─────────────────────────────────────────────────────────
    push_to_gsheets(spreadsheet, "buy_stocks", TABS["buy_stocks"], buy_df, "dark_green")
    time.sleep(1)
    if not buy_df.empty:
        ws = spreadsheet.worksheet(TABS["buy_stocks"])
        apply_stock_sheet_formatting(ws, buy_df)
        time.sleep(1)

    # ── 5. Sell Stocks ────────────────────────────────────────────────────────
    push_to_gsheets(spreadsheet, "sell_stocks", TABS["sell_stocks"], sell_df, "red")
    time.sleep(1)

    # ── 6. All Stocks RS ──────────────────────────────────────────────────────
    push_to_gsheets(spreadsheet, "all_stocks", TABS["all_stocks"], stock_df)
    time.sleep(1)

    # ── 7. Chart Patterns ─────────────────────────────────────────────────────
    if patterns_list:
        pats_rows = []
        sym_info  = {}
        if not stock_df.empty:
            for _, r in stock_df.iterrows():
                sym_info[r["Symbol"]] = {"Sector": r.get("Sector",""), "Signal": r.get("Signal","NA")}
        for s in sorted(patterns_list, key=lambda x: (0 if x.direction=="BULLISH" else 1, x.end_date)):
            orig = s.symbol.replace(".NS","")
            info = sym_info.get(orig, {})
            pats_rows.append({
                "Symbol":       orig,
                "Company":      "",
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
        pat_df = pd.DataFrame(pats_rows)
        push_to_gsheets(spreadsheet, "chart_patterns", TABS["chart_patterns"], pat_df, "navy")
        time.sleep(1)
    else:
        # Clear the tab if no patterns
        ws = get_or_create_worksheet(spreadsheet, TABS["chart_patterns"])
        _api_call(ws.clear)
        _api_call(ws.update, "A1", [["No chart patterns detected in this run."]])

    # ── 8. Sector Rotation ────────────────────────────────────────────────────
    if not sector_rot_df.empty:
        push_to_gsheets(spreadsheet, "sector_rotation", TABS["sector_rotation"],
                        sector_rot_df, "dark_green")
        time.sleep(1)
        ws = spreadsheet.worksheet(TABS["sector_rotation"])
        for col in ["RS55_Above%", "RSI50_Above%", "SMA20_Above%", "SMA50_Above%"]:
            apply_column_conditional_format(ws, sector_rot_df, col, len(sector_rot_df), "pct_bar")
            time.sleep(0.2)
        apply_column_conditional_format(ws, sector_rot_df, "1M_Zone", len(sector_rot_df), "signal")
        apply_column_conditional_format(ws, sector_rot_df, "3M_Zone", len(sector_rot_df), "signal")

    # ── 9. Industry Rotation ──────────────────────────────────────────────────
    if not industry_rot_df.empty:
        push_to_gsheets(spreadsheet, "industry_rotation", TABS["industry_rotation"],
                        industry_rot_df)
        time.sleep(1)

    # ── 10. Market Breadth ────────────────────────────────────────────────────
    if not breadth_df.empty:
        push_to_gsheets(spreadsheet, "market_breadth", TABS["market_breadth"], breadth_df)
        time.sleep(1)

    # ── 11. FII DII ───────────────────────────────────────────────────────────
    if not fii_dii_df.empty:
        push_to_gsheets(spreadsheet, "fii_dii", TABS["fii_dii"], fii_dii_df, "red")
        time.sleep(1)
        if "FII_Net" in fii_dii_df.columns:
            ws = spreadsheet.worksheet(TABS["fii_dii"])
            apply_column_conditional_format(ws, fii_dii_df, "FII_Net", len(fii_dii_df), "positive")
            time.sleep(0.3)
            apply_column_conditional_format(ws, fii_dii_df, "DII_Net", len(fii_dii_df), "positive")

    # ── 12. Sector Performance ────────────────────────────────────────────────
    if not sector_perf_df.empty:
        push_to_gsheets(spreadsheet, "sector_perf", TABS["sector_perf"], sector_perf_df)
        time.sleep(1)
        ws = spreadsheet.worksheet(TABS["sector_perf"])
        for col in ["1M(22d)%", "3M(66d)%", "6M(132d)%", "12M(252d)%", "YTD%",
                    "RS_1M%", "RS_3M%", "RS_6M%", "RS_12M%"]:
            apply_column_conditional_format(ws, sector_perf_df, col, len(sector_perf_df), "positive")
            time.sleep(0.2)

    print(f"\n  ✅ All tabs written to Google Sheet")
    print(f"  🔗 {SHEET_URL}")


# ══════════════════════════════════════════════════════════════════════════════
#  ❺  MARKET BREADTH (standalone, uses price_data)
# ══════════════════════════════════════════════════════════════════════════════

def analyse_market_breadth(price_data, index_prices, universe_df):
    print("  Computing market breadth …")
    rows = []
    for idx_name, cfg in NSE_BREADTH_INDICES.items():
        csv_file = cfg.get("csv")
        syms     = load_index_constituents(csv_file) if csv_file else []
        if not syms:
            syms = universe_df["Yahoo"].tolist()
        row = calc_rotation_row(syms, price_data, index_prices, idx_name)
        if row:
            try:
                yahoo = cfg.get("yahoo")
                if yahoo:
                    raw = yf.download(yahoo, period="5d", auto_adjust=True, progress=False)
                    if not raw.empty:
                        row["Index_Price"] = round(float(raw["Close"].dropna().iloc[-1]), 2)
            except Exception:
                pass
            rows.append(row)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
#  ❻  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "═" * 68)
    print(f"  INDIA RS MARKET ANALYSIS — GOOGLE SHEETS EDITION v3.0")
    print(f"  {datetime.now().strftime('%d %b %Y  %H:%M')}")
    print("═" * 68 + "\n")

    # ── Authenticate to Google Sheets ─────────────────────────────────────────
    print("🔐 Authenticating to Google Sheets …")
    gc = get_gspread_client()
    spreadsheet = _api_call(gc.open_by_url, SHEET_URL)
    print(f"  ✅ Connected to: '{spreadsheet.title}'")

    # ── Load universe ─────────────────────────────────────────────────────────
    print("\n📂 Loading stock universe …")
    universe = load_universe()

    # ── Fetch prices ──────────────────────────────────────────────────────────
    index_sym   = INDIA_INDEX
    stock_syms  = universe["Yahoo"].tolist()

    # Try PriceCache (local); fall back to direct yfinance
    USE_CACHE = False
    try:
        from price_cache import PriceCache
        USE_CACHE = True
    except ImportError:
        pass

    if USE_CACHE:
        print("\n📦 Loading via PriceCache …")
        cache    = PriceCache()
        all_syms = [index_sym] + [cfg["yahoo"] for cfg in INDIA_SECTORS.values()
                                   if cfg.get("yahoo")] + stock_syms
        start_dt = (datetime.now() - timedelta(days=PERIOD_DAYS+10)).strftime("%Y-%m-%d")
        close_all, low_all = cache.get(all_syms, start_dt)
        index_prices = close_all[index_sym].dropna() if index_sym in close_all.columns else pd.Series()
        if index_prices.empty:
            print("  ❌ Index data missing"); return
        sector_prices = {}
        for sname, cfg in INDIA_SECTORS.items():
            ysym = cfg.get("yahoo")
            if ysym and ysym in close_all.columns:
                s = close_all[ysym].dropna()
                if len(s) >= 22:
                    sector_prices[sname] = s
        price_data = close_all[[s for s in stock_syms if s in close_all.columns]]
        low_data   = low_all[[s for s in stock_syms if s in low_all.columns]] if not low_all.empty else None
    else:
        print("\n📡 Fetching prices directly (no cache) …")
        idx_raw = yf.download(index_sym, period=f"{PERIOD_DAYS}d", auto_adjust=True, progress=False)
        if idx_raw.empty:
            print("  ❌ Cannot fetch index"); return
        idx_close = idx_raw["Close"]
        if isinstance(idx_close, pd.DataFrame):
            idx_close = idx_close.squeeze()
        index_prices  = normalize_series(idx_close.dropna())
        sector_prices = fetch_sector_indices()
        price_data, low_data = fetch_prices(stock_syms)

    print(f"  ✅ Index:{len(index_prices)}d | Sectors:{len(sector_prices)} | Stocks:{len(price_data.columns)}")

    # ── Analyses ──────────────────────────────────────────────────────────────
    print("\n🔢 Sector RS analysis …")
    sector_df = analyse_sectors(index_prices, sector_prices)

    patterns_by_sym, patterns_list = {}, []
    if ENABLE_CHART_PATTERNS:
        print(f"\n📐 Chart patterns (top {PATTERN_MAX_STOCKS} stocks) …")
        cands    = [s for s in stock_syms if s in price_data.columns and len(price_data[s].dropna())>=60][:PATTERN_MAX_STOCKS]
        ohlcv    = fetch_ohlcv_batch(cands)
        patterns_by_sym, patterns_list = run_pattern_detection(ohlcv)
        print(f"  ✅ {len(patterns_list)} patterns in {len(patterns_by_sym)} stocks")

    print("\n🔢 Stock RS + SL analysis …")
    stock_df = analyse_stocks(universe, price_data, index_prices, sector_prices, low_data)
    if stock_df.empty:
        print("  ❌ No stock data"); return

    # Add chart pattern column
    p2 = SIGNAL_PERIODS[1]
    def _pat(sym_ns):
        pats = patterns_by_sym.get(sym_ns, [])
        bp   = [s.pattern for s in pats if s.direction=="BULLISH"]
        bea  = [s.pattern for s in pats if s.direction=="BEARISH"]
        return ("🟢 "+", ".join(bp[:2]) if bp else ("🔴 "+", ".join(bea[:2]) if bea else ""))
    stock_df["Chart_Pattern"] = stock_df["Symbol"].apply(lambda s: _pat(s+".NS"))

    drilldown = pd.DataFrame()
    sort_c    = "Enhanced_Score" if "Enhanced_Score" in stock_df.columns else "RS_Score"
    rows      = []
    for _, sec_row in sector_df.iterrows():
        sn     = sec_row["Sector"]
        stocks = stock_df[stock_df["Sector"]==sn]
        if stocks.empty: continue
        top = stocks.sort_values(sort_c, ascending=False).head(10).copy()
        top.insert(0,"Sector_Rank", int(sec_row["Rank"]))
        top.insert(1,"Sector_Signal", sec_row["Signal"])
        rows.append(top)
    drilldown = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

    sector_rot_df = pd.DataFrame()
    if ENABLE_SECTOR_ROTATION:
        print("\n🔄 Sector rotation …")
        rot_rows = []
        for sec in universe["Sector"].unique():
            syms = universe[universe["Sector"]==sec]["Yahoo"].tolist()
            row  = calc_rotation_row(syms, price_data, index_prices, sec)
            if row: rot_rows.append(row)
        sector_rot_df = pd.DataFrame(rot_rows).sort_values("RS55_Above%",ascending=False).reset_index(drop=True) if rot_rows else pd.DataFrame()
        if not sector_rot_df.empty:
            sector_rot_df.insert(0,"Rank", sector_rot_df.index+1)

        print("\n🏭 Industry rotation …")
        ind_rows = []
        for ind in universe["Industry"].unique():
            syms     = universe[universe["Industry"]==ind]["Yahoo"].tolist()
            sec_val  = universe[universe["Industry"]==ind]["Sector"].iloc[0] if len(universe[universe["Industry"]==ind])>0 else "—"
            row      = calc_rotation_row(syms, price_data, index_prices, ind)
            if row:
                row["Sector"] = sec_val
                ind_rows.append(row)
        industry_rot_df = pd.DataFrame(ind_rows).sort_values("RS55_Above%",ascending=False).reset_index(drop=True) if ind_rows else pd.DataFrame()
        if not industry_rot_df.empty:
            industry_rot_df.insert(0,"Rank", industry_rot_df.index+1)
    else:
        industry_rot_df = pd.DataFrame()

    breadth_df = pd.DataFrame()
    if ENABLE_MARKET_BREADTH:
        print("\n📊 Market breadth …")
        breadth_df = analyse_market_breadth(price_data, index_prices, universe)

    fii_dii_df = pd.DataFrame()
    if ENABLE_FII_DII:
        fii_dii_df = fetch_fii_dii_data()

    print("\n📈 Sector performance …")
    sector_perf_df = analyse_sector_performance(sector_prices, index_prices)

    # ── Export to Google Sheets ───────────────────────────────────────────────
    print("\n📊 Exporting to Google Sheets …")
    export_to_gsheets(
        spreadsheet,
        stock_df, sector_df, drilldown, patterns_list,
        sector_rot_df, industry_rot_df, breadth_df, fii_dii_df, sector_perf_df,
    )

    # ── Console summary ───────────────────────────────────────────────────────
    print("\n" + "═" * 68)
    print("  ✅  COMPLETE!")
    print(f"  🔗  Sheet: {SHEET_URL}")
    buys  = (stock_df["Signal"]=="Buy").sum()
    sells = (stock_df["Signal"]=="Sell").sum()
    print(f"\n  Stocks: {len(stock_df)} | Buy: {buys} | Sell: {sells}")

    if "SL_Grade" in stock_df.columns:
        buy_df = stock_df[stock_df["Signal"]=="Buy"]
        grade_a = (buy_df["SL_Grade"]=="A").sum()
        grade_b = (buy_df["SL_Grade"]=="B").sum()
        rr3     = (buy_df["RR_Ratio"]>=3).sum()
        prime   = buy_df[(buy_df["SL_Grade"].isin(["A","B"]))&(buy_df["RR_Ratio"]>=3)]
        print(f"  SL Grade A:{grade_a} B:{grade_b} | R:R>=3x:{rr3} | Prime Setups:{len(prime)}")
        if not prime.empty:
            print(f"\n  🏆 TOP PRIME SETUPS:")
            for _, r in prime.nlargest(5,"Enhanced_Score").iterrows():
                rr = r.get("RR_Ratio",float("nan"))
                print(f"     {r['Symbol']:<14} EnhSc:{r.get('Enhanced_Score',0):.1f} "
                      f"SL:{r.get('Rec_SL%','—')}% RR:{f'{rr:.1f}x' if rr==rr else '—'}")
    print("═" * 68)


if __name__ == "__main__":
    main()

