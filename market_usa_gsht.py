"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  USA MARKET ANALYSIS — GOOGLE SHEETS EDITION  v5.2                        ║
║  market_usa_gsht.py  — GitHub Actions compatible                          ║
║                                                                            ║
║  GitHub Secrets needed:                                                    ║
║     GOOGLE_CREDENTIALS  → service account JSON content                    ║
║     GOOGLE_SHEET_URL_USA → full Google Sheet URL for US report            ║
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

MAX_STOCKS        = 500
PERIOD_DAYS       = 300
ENABLE_PATTERNS   = True
PATTERN_MAX       = 400
FETCH_FINANCIALS  = True
ENABLE_SIGNALS    = True
SIGNAL_MAX_STOCKS = 400

CREDENTIALS_PATH = (
    os.environ.get("GOOGLE_CREDENTIALS_PATH")
    or os.path.join(SCRIPT_DIR, "google_credentials.json")
)
SHEET_URL = (
    os.environ.get("GOOGLE_SHEET_URL_USA")
    or os.environ.get("GOOGLE_SHEET_URL")
    or "https://docs.google.com/spreadsheets/d/YOUR_USA_SHEET_ID/edit"
)
GSCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

sys.path.insert(0, SCRIPT_DIR)
from market_signals import build_dashboard_df
from market_engine import (
    US_INDEX, US_SECTORS, US_INDUSTRY_TO_SECTOR, US_BREADTH_INDICES,
    RS_PERIODS, SIGNAL_PERIODS, fetch_close_batch, fetch_ohlcv_batch, _normalize,
    build_market_snapshot, build_sector_strength, build_sector_rotation,
    build_industry_rotation, build_market_breadth, build_sector_performance,
    build_stock_strength, build_top_picks_buy, build_top_picks_sell,
    build_chart_patterns_df, build_trade_setups, run_pattern_detection,
)

# Reuse all GS helpers from india runner (identical logic)
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
        raise FileNotFoundError(f"Credentials not found: {CREDENTIALS_PATH}")
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=GSCOPE)
    gc = gspread.authorize(creds); ss = gc.open_by_url(SHEET_URL)
    print(f"  ✅ Connected: '{ss.title}'"); return ss

def _get_ws(ss, title, rows=3000, cols=60):
    try:    return ss.worksheet(title)
    except: return ss.add_worksheet(title=title, rows=rows, cols=cols)

@tenacity.retry(wait=tenacity.wait_exponential(min=5,max=120),stop=tenacity.stop_after_attempt(8),
                retry=tenacity.retry_if_exception_type((gspread.exceptions.APIError,ConnectionError)),reraise=True)
def _api(fn, *args, **kwargs): return fn(*args, **kwargs)

def _cell_bg(val, col_name):
    col = col_name.lower()
    if col in ("signal","enhanced","action","mst_signal","lst_signal","rs30_signal","supertrend","supertrend_w"):
        v = str(val or "")
        if v in ("Strong Buy","BUY","Buy"): return GS_COLORS["lt_green"]
        if v in ("Sell","SELL"):             return GS_COLORS["lt_red"]
        if v in ("Neutral","WAIT","NA"):     return GS_COLORS["amber"]
        if v == "Watch":                     return GS_COLORS["lt_blue"]
    if "trend" in col or "_zone" in col:
        v = str(val or "").lower()
        if "bullish" in v or "recovering" in v: return GS_COLORS["lt_green"]
        if "bearish" in v or "pulling" in v:    return GS_COLORS["lt_red"]
        if "neutral" in v or "mixed" in v:      return GS_COLORS["amber"]
    if col.startswith("abv_") or "beats" in col:
        if str(val)=="✓": return GS_COLORS["lt_green"]
        if str(val)=="✗": return GS_COLORS["lt_red"]
    if col=="sl_grade":
        return {"A":GS_COLORS["lt_green"],"B":GS_COLORS["lt_green"],
                "C":GS_COLORS["amber"],"D":GS_COLORS["lt_red"],"F":GS_COLORS["lt_red"]}.get(str(val))
    pct_cols={"chg_1d%","chg_5d%","rs_22d%","rs_55d%","rs_22d_idx%","rs_55d_idx%",
              "w_rs21%","w_rs30%","m_rs12%","rs_score","total_score",
              "1m%","3m%","6m%","12m%","ytd%","from_52w_high%","sales_qoq%","sales_yoy%","pat_qoq%","pat_yoy%"}
    if col in pct_cols or col.endswith("%"):
        try:
            v=float(val or 0)
            if v>0: return GS_COLORS["lt_green"]
            if v<0: return GS_COLORS["lt_red"]
        except: pass
    return None

def write_tab(ss, title, df, hdr_bg="navy"):
    if df is None or (hasattr(df,"empty") and df.empty):
        ws=_get_ws(ss,title); _api(ws.clear); _api(ws.update,"A1",[["No data."]]); return
    display_cols=[c for c in df.columns if not c.startswith("_")]
    df_out=df[display_cols].copy().replace([float("inf"),float("-inf")],"").fillna("")
    nr,nc=len(df_out)+1,len(df_out.columns)
    ws = _api(_get_ws, ss, title, rows=max(nr + 50, 500), cols=max(nc + 5, 30))
    _api(ws.clear); time.sleep(0.5)
    _api(set_with_dataframe,ws,df_out,resize=True); time.sleep(0.5)
    try:
        col_end=gspread.utils.rowcol_to_a1(1,nc).replace("1","")
        _api(ws.format,f"A1:{col_end}1",{"backgroundColor":GS_COLORS.get(hdr_bg,GS_COLORS["navy"]),
            "textFormat":{"foregroundColor":GS_COLORS["white"],"bold":True,"fontSize":10},
            "horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"})
        time.sleep(0.3)
        _api(ss.batch_update,{"requests":[{"updateSheetProperties":{"properties":{"sheetId":ws.id,
            "gridProperties":{"frozenRowCount":1}},"fields":"gridProperties.frozenRowCount"}}]})
    except Exception: pass
    cell_fmts=[]
    for ci,col in enumerate(df_out.columns,1):
        ltr=gspread.utils.rowcol_to_a1(1,ci).replace("1","")
        for ri,val in enumerate(df_out[col],start=2):
            bg=_cell_bg(val,col)
            if bg: cell_fmts.append({"range":f"{ltr}{ri}","format":{"backgroundColor":bg}})
    for i in range(0,len(cell_fmts),1000):
        try: _api(ws.batch_format,cell_fmts[i:i+1000]); time.sleep(0.2)
        except Exception: pass
    time.sleep(0.8)
    print(f"    ✓ '{title}' — {len(df_out)} rows × {len(df_out.columns)} cols")

def write_dashboard_tab(ss, dash_df, market):
    title="📋 Dashboard"; ws = _api(_get_ws, ss, title, rows=200, cols=2)
    _api(ws.clear); time.sleep(0.4)
    if dash_df is None or dash_df.empty: _api(ws.update,"A1",[["No data."]]); return
    clean=dash_df.copy().fillna("").astype(str)
    _api(set_with_dataframe,ws,clean,resize=True); time.sleep(0.5)
    try:
        _api(ws.format,"A1:B1",{"backgroundColor":{"red":0.039,"green":0.086,"blue":0.157},
            "textFormat":{"foregroundColor":{"red":0,"green":0.898,"blue":1},"bold":True,"fontSize":13}})
    except Exception: pass
    time.sleep(0.8); print(f"    ✓ '{title}' — {len(dash_df)} rows")

def load_us_universe():
    for path in [STOCK_CSV, os.path.join(SCRIPT_DIR,"us_sp500list.csv")]:
        if os.path.exists(path): break
    else: print("  ❌ US universe CSV not found"); sys.exit(1)
    df=pd.read_csv(path); df.columns=df.columns.str.strip()
    cm={c.lower().replace(" ","").replace("_",""):c for c in df.columns}
    sc=next((cm[k] for k in ["symbol","ticker","sym"] if k in cm),df.columns[0])
    ic=next((cm[k] for k in ["gicssector","sector","industry"] if k in cm),df.columns[1])
    nc=next((cm[k] for k in ["companyname","security","name","company"] if k in cm),df.columns[2])
    df=df.rename(columns={sc:"Symbol",ic:"Industry",nc:"Company Name"})
    df["Symbol"]=df["Symbol"].astype(str).str.strip()
    df["Industry"]=df["Industry"].astype(str).str.strip()
    df["Company Name"]=df["Company Name"].astype(str).str.strip()
    df["Yahoo"]=df["Symbol"]
    df["Sector"]=df["Industry"].map(US_INDUSTRY_TO_SECTOR).fillna("Technology")
    if MAX_STOCKS>0: df=df.head(MAX_STOCKS)
    print(f"  ✅ Universe: {len(df)} stocks | {df['Sector'].nunique()} sectors")
    return df

def fetch_us_sector_prices():
    result={}
    for sname,cfg in US_SECTORS.items():
        ysym=cfg.get("yahoo")
        if not ysym: continue
        try:
            raw=yf.download(ysym,period=f"{PERIOD_DAYS}d",auto_adjust=True,progress=False)
            if not raw.empty and len(raw)>=22:
                cl=raw["Close"]
                if isinstance(cl,pd.DataFrame): cl=cl.squeeze()
                result[sname]=_normalize(cl.dropna())
        except Exception: pass
    print(f"  ✅ Sector ETFs: {len(result)}/{len(US_SECTORS)}")
    return result

def main():
    print("\n"+"═"*68)
    print("  USA MARKET — GOOGLE SHEETS EDITION  v5.2")
    print(f"  {datetime.now().strftime('%d %b %Y  %H:%M ET')}")
    print(f"  Stocks:{MAX_STOCKS}  Patterns:{PATTERN_MAX}  Signals:{SIGNAL_MAX_STOCKS}")
    print("═"*68+"\n")
    t0=time.time()

    print("🔐 Connecting …"); ss=gs_connect()
    print("📂 Loading universe …"); universe=load_us_universe()

    print(f"\n📡 Fetching index ({US_INDEX}) …")
    raw=yf.download(US_INDEX,period=f"{PERIOD_DAYS}d",auto_adjust=True,progress=False)
    if raw.empty: print("  ❌ Cannot fetch index"); return
    cl=raw["Close"]
    if isinstance(cl,pd.DataFrame): cl=cl.squeeze()
    index_prices=_normalize(cl.dropna()); print(f"  ✅ Index: {len(index_prices)} days")

    print("\n📡 Sector ETFs …"); sector_prices=fetch_us_sector_prices()

    stock_syms=universe["Yahoo"].tolist()
    print(f"\n📡 Fetching {len(stock_syms)} closes …")
    price_data=fetch_close_batch(stock_syms,PERIOD_DAYS)
    print(f"  ✅ Stocks: {len(price_data.columns)}")

    ohlcv_dict={}
    max_ohlcv=max(PATTERN_MAX,SIGNAL_MAX_STOCKS if ENABLE_SIGNALS else 0)
    if max_ohlcv>0:
        print(f"\n📡 OHLCV for {max_ohlcv} stocks …")
        cands=[s for s in stock_syms if s in price_data.columns and len(price_data[s].dropna())>=60][:max_ohlcv]
        ohlcv_dict=fetch_ohlcv_batch(cands,days=PERIOD_DAYS)
        print(f"  ✅ OHLCV: {len(ohlcv_dict)}")

    patterns_by_sym={}; patterns_list=[]
    if ENABLE_PATTERNS:
        print("\n📐 Chart patterns …")
        pat_dict={k:v for k,v in ohlcv_dict.items() if len(v)>=60}
        patterns_by_sym,patterns_list=run_pattern_detection(pat_dict)

    print("\n📊 Building analysis DataFrames …")
    snap_df    =build_market_snapshot("US")
    sec_str_df =build_sector_strength(universe,price_data,index_prices,sector_prices)
    sec_rot_df =build_sector_rotation(universe,price_data,index_prices)
    ind_rot_df =build_industry_rotation(universe,price_data,index_prices)
    breadth_df =build_market_breadth(price_data,index_prices,US_BREADTH_INDICES,INDEX_DATA_DIR,market="US")
    sec_perf_df=build_sector_performance(sector_prices,index_prices)
    stock_df   =build_stock_strength(universe,price_data,index_prices,sector_prices,
                                      patterns_by_sym,market="US",
                                      fetch_financials=FETCH_FINANCIALS,
                                      ohlcv_dict=ohlcv_dict if ENABLE_SIGNALS else {})
    top_buy_df =build_top_picks_buy(stock_df,sec_str_df,market="US")
    top_sell_df=build_top_picks_sell(stock_df,sec_str_df,market="US")
    chart_df   =build_chart_patterns_df(patterns_list,stock_df,market="US")
    trade_df   =build_trade_setups(stock_df,sec_str_df,market="US")
    run_time   =datetime.now().strftime("%d %b %Y  %H:%M ET")
    dashboard_df=build_dashboard_df(stock_df,sec_str_df,"US",run_time)

    print("\n📊 Writing to Google Sheets …")
    write_dashboard_tab(ss,dashboard_df,"US")
    write_tab(ss,"📸 Market Snapshot",  snap_df,    "navy")
    write_tab(ss,"🏭 Sector Strength",   sec_str_df, "teal")
    write_tab(ss,"🔄 Sector Rotation",   sec_rot_df, "navy")
    write_tab(ss,"🏭 Industry Rotation", ind_rot_df, "navy")
    write_tab(ss,"📊 Market Breadth",    breadth_df, "green")
    write_tab(ss,"📈 Sector Performance",sec_perf_df,"navy")
    write_tab(ss,"📊 Stock Strength",    stock_df,   "navy")
    write_tab(ss,"🏆 Top Picks - Buy",   top_buy_df, "green")
    write_tab(ss,"🔴 Top Picks - Sell",  top_sell_df,"red")
    write_tab(ss,"📐 Chart Patterns",    chart_df,   "navy")
    write_tab(ss,"🎯 Trade Setups",      trade_df,   "navy")

    elapsed=time.time()-t0
    print(f"\n{'═'*68}")
    print(f"  ✅  COMPLETE!  |  ⏱ {elapsed:.0f}s  |  🔗 {SHEET_URL}")
    if not stock_df.empty:
        b=(stock_df["Signal"]=="Buy").sum(); s=(stock_df["Signal"]=="Sell").sum()
        mst=(stock_df.get("MST_Signal",pd.Series())=="Buy").sum()
        print(f"  ✅ Buy:{b} | 🔴 Sell:{s} | MST:{mst}")
    print("═"*68)

if __name__=="__main__": main()
