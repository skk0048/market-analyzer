"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  UNIFIED MARKET ANALYSIS ENGINE  v5.2                                      ║
║  Shared core — India & US markets                                          ║
║                                                                            ║
║  v5.2 additions:                                                           ║
║   • Supertrend (daily + weekly) — Pine Script–accurate                     ║
║   • Swing High/Low → SL_Buy%, SL_Sell%, SL_Grade                          ║
║   • Multi-TF RS: Weekly RS(21/30), Monthly RS(12)                         ║
║   • Multi-TF RSI: Weekly RSI(14), Monthly RSI(12)                         ║
║   • Weekly EMA(10), EMA(30), EMA(200)                                      ║
║   • MST / LST / RS30 signals per stock                                     ║
║   • Trade Setups: SL_Price, SL%, TP1%, TP2%, RR, SL_Grade, SL_Bonus      ║
║   • Dashboard sheet with methodology + run summary                         ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
import warnings, time, os, json
import numpy as np, pandas as pd, yfinance as yf
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from scipy.signal import argrelextrema
warnings.filterwarnings("ignore")

# Import signal module
from market_signals import (
    calc_supertrend, calc_supertrend_from_df,
    calc_swing_sl, sl_grade, sl_bonus,
    calc_rs_tf, calc_rsi_tf, calc_ema_tf, calc_pct_from_52w_high,
    calc_mst_signal, calc_lst_signal, calc_rs30_signal, classify_trade,
    MST_RS_ENTRY, MST_RS_HTF, MST_RSI_LEN, MST_ST_PERIOD, MST_ST_FACTOR,
    MST_TP1_MULT, MST_TP2_MULT,
    LST_RS_ENTRY, LST_RS_HTF, LST_RSI_LEN, LST_ST_PERIOD, LST_ST_FACTOR,
    LST_TP1_MULT, LST_TP2_MULT,
    RS30_RS_PERIOD, RS30_EMA_S, RS30_EMA_L,
)

# ─────────────────────────────────────────────────────────────────────────────
#  SNAPSHOT TICKERS
# ─────────────────────────────────────────────────────────────────────────────
SNAPSHOT_TICKERS = {
    "INDIA": [
        {"name":"Nifty 50",          "ticker":"^NSEI",       "type":"Index"},
        {"name":"Sensex",            "ticker":"^BSESN",      "type":"Index"},
        {"name":"Nifty Bank",        "ticker":"^NSEBANK",    "type":"Index"},
        {"name":"Nifty IT",          "ticker":"^CNXIT",      "type":"Index"},
        {"name":"Nifty Midcap 100",  "ticker":"^NSEMDCP100", "type":"Index"},
        {"name":"Nifty Smallcap 100","ticker":"^CNXSC",      "type":"Index"},
        {"name":"Nifty Finance",     "ticker":"^CNXFINANCE", "type":"Index"},
        {"name":"Nifty Auto",        "ticker":"^CNXAUTO",    "type":"Index"},
        {"name":"India VIX",         "ticker":"^INDIAVIX",   "type":"Volatility"},
        {"name":"Gold",              "ticker":"GC=F",        "type":"Commodity"},
        {"name":"Silver",            "ticker":"SI=F",        "type":"Commodity"},
        {"name":"Crude Oil WTI",     "ticker":"CL=F",        "type":"Commodity"},
        {"name":"Natural Gas",       "ticker":"NG=F",        "type":"Commodity"},
        {"name":"USD/INR",           "ticker":"USDINR=X",    "type":"Forex"},
        {"name":"EUR/INR",           "ticker":"EURINR=X",    "type":"Forex"},
        {"name":"DXY (USD Index)",   "ticker":"DX-Y.NYB",    "type":"Forex"},
    ],
    "US": [
        {"name":"S&P 500",           "ticker":"^GSPC",       "type":"Index"},
        {"name":"Nasdaq 100",        "ticker":"^NDX",        "type":"Index"},
        {"name":"Dow Jones",         "ticker":"^DJI",        "type":"Index"},
        {"name":"Russell 2000",      "ticker":"^RUT",        "type":"Index"},
        {"name":"VIX",               "ticker":"^VIX",        "type":"Volatility"},
        {"name":"SPY ETF",           "ticker":"SPY",         "type":"ETF"},
        {"name":"QQQ ETF",           "ticker":"QQQ",         "type":"ETF"},
        {"name":"Gold",              "ticker":"GC=F",        "type":"Commodity"},
        {"name":"Silver",            "ticker":"SI=F",        "type":"Commodity"},
        {"name":"Crude Oil WTI",     "ticker":"CL=F",        "type":"Commodity"},
        {"name":"Natural Gas",       "ticker":"NG=F",        "type":"Commodity"},
        {"name":"EUR/USD",           "ticker":"EURUSD=X",    "type":"Forex"},
        {"name":"USD/JPY",           "ticker":"USDJPY=X",    "type":"Forex"},
        {"name":"DXY (USD Index)",   "ticker":"DX-Y.NYB",    "type":"Forex"},
        {"name":"10Y Treasury Yield","ticker":"^TNX",        "type":"Bond"},
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
#  INDIA CONFIG
# ─────────────────────────────────────────────────────────────────────────────
INDIA_INDEX = "^NSEI"
INDIA_SECTORS = {
    "Automobile":    {"yahoo":"^CNXAUTO",   "csv":"ind_niftyautolist.csv"},
    "IT":            {"yahoo":"^CNXIT",      "csv":"ind_niftyitlist.csv"},
    "Banking":       {"yahoo":"^NSEBANK",    "csv":"ind_niftybanklist.csv"},
    "Pharma":        {"yahoo":"^CNXPHARMA",  "csv":"ind_niftypharmalist.csv"},
    "FMCG":          {"yahoo":"^CNXFMCG",    "csv":"ind_niftyfmcglist.csv"},
    "Metal":         {"yahoo":"^CNXMETAL",   "csv":"ind_niftymetallist.csv"},
    "Oil & Gas":     {"yahoo":"^CNXENERGY",  "csv":"ind_niftyoilgaslist.csv"},
    "Finance":       {"yahoo":"^CNXFINANCE", "csv":"ind_niftyfinancelist.csv"},
    "Realty":        {"yahoo":"^CNXREALTY",  "csv":"ind_niftyrealtylist.csv"},
    "Infra":         {"yahoo":"^CNXINFRA",   "csv":None},
    "Media":         {"yahoo":"^CNXMEDIA",   "csv":"ind_niftymedialist.csv"},
    "PSU Bank":      {"yahoo":"^CNXPSUBANK", "csv":"ind_niftypsubanklist.csv"},
    "Chemicals":     {"yahoo":None,           "csv":"ind_niftyChemicals_list.csv"},
    "Consumer Dur.": {"yahoo":None,           "csv":"ind_niftyconsumerdurableslist.csv"},
    "Healthcare":    {"yahoo":None,           "csv":"ind_niftyhealthcarelist.csv"},
    "Cement":        {"yahoo":None,           "csv":"ind_NiftyCement_list.csv"},
}
INDIA_INDUSTRY_TO_SECTOR = {
    "Automobile and Auto Components":"Automobile","Information Technology":"IT",
    "Financial Services":"Finance","Healthcare":"Healthcare",
    "Fast Moving Consumer Goods":"FMCG","Metals & Mining":"Metal",
    "Oil, Gas & Consumable Fuels":"Oil & Gas","Oil Gas & Consumable Fuels":"Oil & Gas",
    "Realty":"Realty","Capital Goods":"Infra","Construction":"Infra",
    "Construction Materials":"Cement","Media, Entertainment & Publication":"Media",
    "Chemicals":"Chemicals","Consumer Durables":"Consumer Dur.",
    "Consumer Services":"Consumer Dur.","Power":"Oil & Gas",
    "Telecommunication":"IT","Textiles":"Finance","Utilities":"Finance",
    "Services":"Finance","Diversified":"Finance","Forest Materials":"Finance",
}
INDIA_BREADTH_INDICES = {
    "Nifty 50":           {"yahoo":"^NSEI",       "csv":"ind_nifty50list.csv"},
    "Nifty Next 50":      {"yahoo":"^NSMIDCP",    "csv":"ind_niftynext50list.csv"},
    "Nifty 100":          {"yahoo":"^CNX100",     "csv":"ind_nifty100list.csv"},
    "Nifty 200":          {"yahoo":"^CNX200",     "csv":"ind_nifty200list.csv"},
    "Nifty 500":          {"yahoo":"^CNX500",     "csv":"ind_nifty500list.csv"},
    "Nifty Total Market": {"yahoo":None,           "csv":"ind_niftytotalmarket_list.csv"},
    "LargeMidcap 250":    {"yahoo":None,           "csv":"ind_niftylargemidcap250list.csv"},
    "Nifty Midcap 50":    {"yahoo":"^NIFMDCP50",  "csv":"ind_niftymidcap50list.csv"},
    "Nifty Midcap 100":   {"yahoo":"^NSEMDCP100", "csv":"ind_niftymidcap100list.csv"},
    "Nifty Midcap 150":   {"yahoo":None,           "csv":"ind_niftymidcap150list.csv"},
    "Nifty Smallcap 50":  {"yahoo":"^CNXSC50",    "csv":"ind_niftysmallcap50list.csv"},
    "Nifty Smallcap 100": {"yahoo":"^CNXSC",      "csv":"ind_niftysmallcap100list.csv"},
    "Nifty Smallcap 250": {"yahoo":None,           "csv":"ind_niftysmallcap250list.csv"},
    "MidSmall 400":       {"yahoo":None,           "csv":"ind_niftymidsmallcap400list.csv"},
    "NSE Full":           {"yahoo":None,           "csv":"ind_nsefull.csv"},
    "Nifty Bank":         {"yahoo":"^NSEBANK",    "csv":"ind_niftybanklist.csv"},
    "Private Bank":       {"yahoo":"^NIFPVTBNK",  "csv":"ind_nifty_privatebanklist.csv"},
    "PSU Bank":           {"yahoo":"^CNXPSUBANK",  "csv":"ind_niftypsubanklist.csv"},
    "MidSmall Finance":   {"yahoo":None,           "csv":"ind_niftymidsmallfinancailservice_list.csv"},
    "Nifty IT":           {"yahoo":"^CNXIT",      "csv":"ind_niftyitlist.csv"},
    "MidSmall IT&Tel":    {"yahoo":None,           "csv":"ind_niftymidsmallitAndtelecom_list.csv"},
    "Nifty FMCG":         {"yahoo":"^CNXFMCG",    "csv":"ind_niftyfmcglist.csv"},
    "Nifty Pharma":       {"yahoo":"^CNXPHARMA",  "csv":"ind_niftypharmalist.csv"},
    "Nifty Healthcare":   {"yahoo":None,           "csv":"ind_niftyhealthcarelist.csv"},
    "500 Healthcare":     {"yahoo":None,           "csv":"ind_nifty500Healthcare_list.csv"},
    "MidSmall Health":    {"yahoo":None,           "csv":"ind_niftymidsmallhealthcare_list.csv"},
    "Nifty Auto":         {"yahoo":"^CNXAUTO",    "csv":"ind_niftyautolist.csv"},
    "Nifty Metal":        {"yahoo":"^CNXMETAL",   "csv":"ind_niftymetallist.csv"},
    "Nifty Oil & Gas":    {"yahoo":"^CNXENERGY",  "csv":"ind_niftyoilgaslist.csv"},
    "Nifty Finance":      {"yahoo":"^CNXFINANCE", "csv":"ind_niftyfinancelist.csv"},
    "Nifty Chemicals":    {"yahoo":None,           "csv":"ind_niftyChemicals_list.csv"},
    "Consumer Dur.":      {"yahoo":None,           "csv":"ind_niftyconsumerdurableslist.csv"},
    "Nifty Cement":       {"yahoo":None,           "csv":"ind_NiftyCement_list.csv"},
    "Nifty Media":        {"yahoo":"^CNXMEDIA",   "csv":"ind_niftymedialist.csv"},
    "Nifty Realty":       {"yahoo":"^CNXREALTY",  "csv":"ind_niftyrealtylist.csv"},
    "Nifty FPI 150":      {"yahoo":None,           "csv":"ind_niftyIndiaFPI150_list.csv"},
}

# ─────────────────────────────────────────────────────────────────────────────
#  US CONFIG
# ─────────────────────────────────────────────────────────────────────────────
US_INDEX = "SPY"
US_SECTORS = {
    "Technology":      {"yahoo":"XLK",  "csv":"us_sector_information_technology.csv"},
    "Financials":      {"yahoo":"XLF",  "csv":"us_sector_financials.csv"},
    "Healthcare":      {"yahoo":"XLV",  "csv":"us_sector_health_care.csv"},
    "ConsumerDisc":    {"yahoo":"XLY",  "csv":"us_sector_consumer_discretionary.csv"},
    "Industrials":     {"yahoo":"XLI",  "csv":"us_sector_industrials.csv"},
    "CommServices":    {"yahoo":"XLC",  "csv":"us_sector_communication_services.csv"},
    "ConsumerStaples": {"yahoo":"XLP",  "csv":"us_sector_consumer_staples.csv"},
    "Energy":          {"yahoo":"XLE",  "csv":"us_sector_energy.csv"},
    "RealEstate":      {"yahoo":"XLRE", "csv":"us_sector_real_estate.csv"},
    "Materials":       {"yahoo":"XLB",  "csv":"us_sector_materials.csv"},
    "Utilities":       {"yahoo":"XLU",  "csv":"us_sector_utilities.csv"},
}
US_INDUSTRY_TO_SECTOR = {
    "Information Technology":"Technology","Semiconductors":"Technology","Software":"Technology",
    "Financials":"Financials","Health Care":"Healthcare","Pharmaceuticals":"Healthcare",
    "Biotechnology":"Healthcare","Consumer Discretionary":"ConsumerDisc",
    "Industrials":"Industrials","Communication Services":"CommServices","Media":"CommServices",
    "Consumer Staples":"ConsumerStaples","Energy":"Energy","Real Estate":"RealEstate",
    "REITs":"RealEstate","Materials":"Materials","Chemicals":"Materials","Utilities":"Utilities",
}
US_BREADTH_INDICES = {
    "S&P 500":         {"yahoo":"SPY",  "csv":"us_sp500list.csv"},
    "Nasdaq 100":      {"yahoo":"QQQ",  "csv":"us_nasdaq100list.csv"},
    "Dow Jones 30":    {"yahoo":"DIA",  "csv":"us_dji30list.csv"},
    "Russell 2000":    {"yahoo":"IWM",  "csv":None},
    "Technology":      {"yahoo":"XLK",  "csv":"us_sector_information_technology.csv"},
    "Financials":      {"yahoo":"XLF",  "csv":"us_sector_financials.csv"},
    "Healthcare":      {"yahoo":"XLV",  "csv":"us_sector_health_care.csv"},
    "ConsumerDisc":    {"yahoo":"XLY",  "csv":"us_sector_consumer_discretionary.csv"},
    "Industrials":     {"yahoo":"XLI",  "csv":"us_sector_industrials.csv"},
    "CommServices":    {"yahoo":"XLC",  "csv":"us_sector_communication_services.csv"},
    "ConsumerStaples": {"yahoo":"XLP",  "csv":"us_sector_consumer_staples.csv"},
    "Energy":          {"yahoo":"XLE",  "csv":"us_sector_energy.csv"},
    "RealEstate":      {"yahoo":"XLRE", "csv":"us_sector_real_estate.csv"},
    "Materials":       {"yahoo":"XLB",  "csv":"us_sector_materials.csv"},
    "Utilities":       {"yahoo":"XLU",  "csv":"us_sector_utilities.csv"},
}

# ─────────────────────────────────────────────────────────────────────────────
#  PARAMETERS
# ─────────────────────────────────────────────────────────────────────────────
RS_PERIODS     = [22, 55, 120, 252]
SIGNAL_PERIODS = [22, 55]
HL_LOOKBACK    = 22
PERIOD_DAYS    = 300
BATCH_SIZE     = 100
BATCH_DELAY    = 1.0

# ─────────────────────────────────────────────────────────────────────────────
#  PRICE UTILITIES
# ─────────────────────────────────────────────────────────────────────────────
def _normalize(s):
    try:
        if isinstance(s, pd.DataFrame): s=s.squeeze()
        idx=s.index
        if hasattr(idx,"tz") and idx.tz is not None:
            try: idx=idx.tz_localize(None)
            except: idx=idx.tz_convert(None)
        idx=idx.normalize()
        s2=pd.Series(s.values,index=idx,dtype=float)
        return s2[~s2.index.duplicated(keep="last")].sort_index()
    except: return s

def _sf(v):
    try: f=float(v); return f if np.isfinite(f) else np.nan
    except: return np.nan

def fetch_close_batch(symbols, days=PERIOD_DAYS):
    end=datetime.today()+timedelta(days=1); start=end-timedelta(days=days+5)
    data={}
    for i in range(0,len(symbols),BATCH_SIZE):
        batch=symbols[i:i+BATCH_SIZE]
        try:
            raw=yf.download(tickers=batch,start=start.strftime("%Y-%m-%d"),
                            end=end.strftime("%Y-%m-%d"),auto_adjust=True,progress=False,threads=True)
            if raw.empty: continue
            if isinstance(raw.columns,pd.MultiIndex): close=raw["Close"]
            else:
                close=raw[["Close"]] if "Close" in raw.columns else pd.DataFrame()
                if len(batch)==1 and not close.empty: close.columns=[batch[0]]
            for sym in batch:
                if sym in close.columns:
                    col=close[sym]
                    if isinstance(col,pd.DataFrame): col=col.squeeze()
                    s=_normalize(col.dropna())
                    if len(s)>=22: data[sym]=s
        except: pass
        if i+BATCH_SIZE<len(symbols): time.sleep(BATCH_DELAY)
    return pd.DataFrame(data).sort_index() if data else pd.DataFrame()

def fetch_ohlcv_batch(symbols, days=PERIOD_DAYS):
    end=datetime.today()+timedelta(days=1); start=end-timedelta(days=days+5)
    result={}
    for i in range(0,len(symbols),50):
        batch=symbols[i:i+50]
        try:
            raw=yf.download(tickers=batch,start=start.strftime("%Y-%m-%d"),
                            end=end.strftime("%Y-%m-%d"),auto_adjust=True,progress=False,threads=True)
            if raw.empty: continue
            if isinstance(raw.columns,pd.MultiIndex):
                for sym in batch:
                    fr={}
                    for pc in ["Open","High","Low","Close","Volume"]:
                        if pc in raw.columns.get_level_values(0) and sym in raw[pc].columns:
                            col=raw[pc][sym]
                            if isinstance(col,pd.DataFrame): col=col.squeeze()
                            fr[pc]=_normalize(col.dropna())
                    if "Close" in fr and len(fr["Close"])>=60:
                        df_s=pd.DataFrame(fr).dropna(subset=["Close","High","Low"])
                        if len(df_s)>=60: result[sym]=df_s
        except: pass
        if i+50<len(symbols): time.sleep(BATCH_DELAY)
    return result

# ─────────────────────────────────────────────────────────────────────────────
#  TECHNICALS (daily)
# ─────────────────────────────────────────────────────────────────────────────
def calc_rs(stock, benchmark, period):
    try:
        s=_normalize(stock.dropna()); b=_normalize(benchmark.dropna())
        common=s.index.intersection(b.index)
        if len(common)<period+1: return np.nan
        s,b=s.loc[common],b.loc[common]
        sc,sp=float(s.iloc[-1]),float(s.iloc[-(period+1)])
        bc,bp=float(b.iloc[-1]),float(b.iloc[-(period+1)])
        if sp==0 or bp==0 or bc==0: return np.nan
        return (sc/sp)/(bc/bp)-1
    except: return np.nan

def calc_rsi(series, period=14):
    try:
        d=series.diff().dropna()
        g=d.clip(lower=0).rolling(period,min_periods=period).mean().iloc[-1]
        l=(-d.clip(upper=0)).rolling(period,min_periods=period).mean().iloc[-1]
        if l==0 or np.isnan(l): return 100.0 if g>0 else 50.0
        return round(100-(100/(1+g/l)),1)
    except: return np.nan

def calc_sma(series, period):
    try:
        if len(series)<period: return np.nan
        return float(series.dropna().iloc[-period:].mean())
    except: return np.nan

def pct_change_n(series, n):
    try:
        if len(series)<n+1: return np.nan
        cur=float(series.iloc[-1]); past=float(series.iloc[-(n+1)])
        return (cur/past-1)*100 if past!=0 else np.nan
    except: return np.nan

def pct_from_52w_high(series):
    try:
        r=series.iloc[-252:] if len(series)>=252 else series
        return (series.iloc[-1]/r.max()-1)*100
    except: return np.nan

def days_since_high(series, lookback=22):
    try:
        s=_normalize(series.dropna()); r=s.iloc[-lookback:]
        return int((s.index[-1]-r.idxmax()).days)
    except: return np.nan

def get_technicals(prices):
    cur=_sf(prices.iloc[-1]) if len(prices)>0 else np.nan
    sma20=calc_sma(prices,20); sma50=calc_sma(prices,50)
    sma100=calc_sma(prices,100); sma200=calc_sma(prices,200)
    rsi=calc_rsi(prices)
    def ab(p,s): return not np.isnan(p) and s is not None and not np.isnan(s) and p>s
    ss=sum([ab(cur,sma20),ab(cur,sma50),ab(cur,sma100),ab(cur,sma200)])
    sc=0
    sc+=1 if ab(cur,sma20) else -1; sc+=1 if ab(cur,sma50) else -1
    if not np.isnan(sma20) and not np.isnan(sma50): sc+=1 if sma20>sma50 else -1
    if not np.isnan(rsi): sc+=1 if rsi>60 else (-1 if rsi<40 else 0)
    slope=pct_change_n(prices,10)
    if not np.isnan(slope): sc+=1 if slope>0.5 else (-1 if slope<-0.5 else 0)
    if sc>=4: trend="Strong Bullish"
    elif sc>=2: trend="Bullish"
    elif sc>=-1: trend="Neutral"
    elif sc>=-3: trend="Bearish"
    else: trend="Strong Bearish"
    return {"RSI_14":round(rsi,1) if rsi==rsi else np.nan,"SMA_Score":ss,
            "Abv_SMA20":"✓" if ab(cur,sma20) else "✗","Abv_SMA50":"✓" if ab(cur,sma50) else "✗",
            "Abv_SMA100":"✓" if ab(cur,sma100) else "✗","Abv_SMA200":"✓" if ab(cur,sma200) else "✗",
            "Trend":trend}

# ─────────────────────────────────────────────────────────────────────────────
#  FINANCIALS CACHE
# ─────────────────────────────────────────────────────────────────────────────
_FIN_CACHE={}
_FIN_FILE=os.path.join(os.path.expanduser("~"),"StockPriceCache","_fin_v52.json")

def _load_fin():
    global _FIN_CACHE
    if os.path.exists(_FIN_FILE):
        try:
            with open(_FIN_FILE) as f: _FIN_CACHE=json.load(f)
        except: _FIN_CACHE={}

def _save_fin():
    try:
        os.makedirs(os.path.dirname(_FIN_FILE),exist_ok=True)
        with open(_FIN_FILE,"w") as f: json.dump(_FIN_CACHE,f)
    except: pass

def _fetch_fin_one(sym):
    blank=dict(SalesQoQ=np.nan,SalesYoY=np.nan,PATQoQ=np.nan,PATYoY=np.nan,
               PATCurr=np.nan,Margin=np.nan,ROE=np.nan,DE=np.nan,EPS=np.nan,PE=np.nan,MktCap=np.nan)
    try:
        t=yf.Ticker(sym); info=t.info or {}
        mc=_sf(info.get("marketCap",np.nan))
        blank["MktCap"]=round(mc/1e9,2) if not np.isnan(mc) else np.nan
        blank["EPS"]=_sf(info.get("trailingEps",np.nan))
        blank["PE"]=_sf(info.get("trailingPE",np.nan))
        roe=_sf(info.get("returnOnEquity",np.nan)); blank["ROE"]=round(roe*100,1) if not np.isnan(roe) else np.nan
        blank["DE"]=_sf(info.get("debtToEquity",np.nan))
        pm=_sf(info.get("profitMargins",np.nan)); blank["Margin"]=round(pm*100,1) if not np.isnan(pm) else np.nan
        qf=t.quarterly_financials
        if qf is not None and not qf.empty and qf.shape[1]>=2:
            for k in ["Total Revenue","Revenue"]:
                if k in qf.index:
                    r=qf.loc[k].dropna()
                    if len(r)>=2: blank["SalesQoQ"]=round((r.iloc[0]/r.iloc[1]-1)*100,1)
                    if len(r)>=5: blank["SalesYoY"]=round((r.iloc[0]/r.iloc[4]-1)*100,1)
                    break
            for k in ["Net Income","Net Income Common Stockholders"]:
                if k in qf.index:
                    p=qf.loc[k].dropna()
                    if len(p)>=1: blank["PATCurr"]=round(_sf(p.iloc[0])/1e6,1)
                    if len(p)>=2 and p.iloc[1]!=0: blank["PATQoQ"]=round((p.iloc[0]/p.iloc[1]-1)*100,1)
                    if len(p)>=5 and p.iloc[4]!=0: blank["PATYoY"]=round((p.iloc[0]/p.iloc[4]-1)*100,1)
                    break
    except: pass
    return blank

def get_financials_batch(symbols, force=False):
    _load_fin(); result={}
    stale=(datetime.now()-timedelta(days=7)).strftime("%Y-%m-%d")
    to_fetch=[]
    for sym in symbols:
        c=_FIN_CACHE.get(sym)
        if c and not force and c.get("_d","2000-01-01")>stale: result[sym]=c
        else: to_fetch.append(sym)
    if to_fetch:
        print(f"    Financials: {len(to_fetch)} new / {len(symbols)-len(to_fetch)} cached")
        for i,sym in enumerate(to_fetch):
            fin=_fetch_fin_one(sym); fin["_d"]=datetime.now().strftime("%Y-%m-%d")
            _FIN_CACHE[sym]=fin; result[sym]=fin
            if (i+1)%25==0: _save_fin(); print(f"      …{i+1}/{len(to_fetch)}")
            time.sleep(0.12)
        _save_fin()
    return result

# ─────────────────────────────────────────────────────────────────────────────
#  CHART PATTERNS
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Pattern:
    symbol:str; pattern:str; direction:str; date:str
    entry:float; stop:float; target:float; rr:float
    confidence:str; notes:str=""

def _near(a,b,tol=0.03): return abs(a-b)/max(abs(a),abs(b),1e-9)<=tol

def detect_patterns(df, sym):
    df=df.tail(180).reset_index(drop=False)
    if len(df)<60: return []
    sigs=[]
    close=df["Close"].values
    high=df["High"].values if "High" in df.columns else close
    low=df["Low"].values if "Low" in df.columns else close
    hi=argrelextrema(high,np.greater_equal,order=5)[0]
    lo=argrelextrema(low,np.less_equal,order=5)[0]
    dc=df.columns[0] if df.columns[0] not in ("Close","High","Low") else (df.columns[1] if len(df.columns)>1 else "idx")
    def _date(i):
        try: return str(df.iloc[min(i,len(df)-1)].get(dc,""))[:10]
        except: return str(df.index[min(i,len(df)-1)])[:10]
    def _add(pat,dir_,ei,e,sl,tgt,conf,notes=""):
        rr=abs(tgt-e)/max(abs(e-sl),1e-9)
        if rr>=1.5: sigs.append(Pattern(sym,pat,dir_,_date(ei),round(e,2),round(sl,2),round(tgt,2),round(rr,2),conf,notes))
    for i in range(len(lo)-1):
        l1,l2=lo[i],lo[i+1]
        if not(10<=l2-l1<=120): continue
        p1,p2=low[l1],low[l2]
        if not _near(p1,p2): continue
        neck=float(high[l1:l2+1].max())
        _add("Double Bottom","BULLISH",l2,neck*1.005,min(p1,p2)*0.99,neck+(neck-min(p1,p2)),"HIGH",f"Neck≈{neck:.0f}")
    for i in range(len(hi)-1):
        h1,h2=hi[i],hi[i+1]
        if not(10<=h2-h1<=120): continue
        p1,p2=high[h1],high[h2]
        if not _near(p1,p2): continue
        neck=float(low[h1:h2+1].min())
        _add("Double Top","BEARISH",h2,neck*0.995,max(p1,p2)*1.01,neck-(max(p1,p2)-neck),"HIGH",f"Neck≈{neck:.0f}")
    for end in range(60,len(df)):
        seg=df.iloc[end-60:end+1]; q=max(1,len(seg)//4)
        hc="High" if "High" in seg.columns else "Close"; lc="Low" if "Low" in seg.columns else "Close"
        sw=[float(seg.iloc[qi*q:(qi+1)*q][hc].max())-float(seg.iloc[qi*q:(qi+1)*q][lc].min()) for qi in range(4) if len(seg.iloc[qi*q:(qi+1)*q])>0]
        if len(sw)<4 or not all(sw[j]>sw[j+1] for j in range(3)): continue
        res=float(seg[hc].max()); bl=float(seg[lc].min()); sl_b=float(seg[lc].iloc[-q:].min())
        _add("VCP","BULLISH",end,res*1.005,sl_b*0.99,res*1.005+(res-bl)*0.75,"HIGH","Vol dry-up")
    for i in range(len(hi)-1):
        l,r=hi[i],hi[i+1]
        if not(30<=r-l<=120): continue
        seg=df.iloc[l:r+1]; tl=float(high[l]); tr=float(high[r])
        if not _near(tl,tr,0.06): continue
        lc2="Low" if "Low" in seg.columns else "Close"
        bot=float(seg[lc2].min()); depth=(tl-bot)/tl
        if not(0.10<=depth<=0.50): continue
        if r+3>=len(df): continue
        lc3="Low" if "Low" in df.columns else "Close"
        hlo=float(df[lc3].iloc[r:min(r+15,len(df))].min())
        if (tr-hlo)/tr>0.15: continue
        _add("Cup & Handle","BULLISH",r,tr*1.005,hlo*0.99,tr*1.005+(tr-bot),"HIGH",f"Depth={depth:.1%}")
    for i in range(len(lo)-2):
        ls,lh,lr=lo[i],lo[i+1],lo[i+2]
        if not(10<=lr-ls<=120): continue
        slp=low[ls]; head=low[lh]; srp=low[lr]
        if not(head<slp and head<srp): continue
        if not _near(slp,srp,0.06): continue
        neck=(high[ls]+high[lr])/2
        _add("Inv H&S","BULLISH",lr,neck*1.005,head*0.99,neck+(neck-head),"HIGH",f"Head={head:.0f}")
    seen={}
    for s in sigs:
        if s.pattern not in seen or s.date>seen[s.pattern].date: seen[s.pattern]=s
    return list(seen.values())

def run_pattern_detection(ohlcv_dict):
    by_sym={}; all_pats=[]; n=len(ohlcv_dict)
    print(f"  Detecting patterns ({n} stocks)…")
    for i,(sym,df) in enumerate(ohlcv_dict.items()):
        if len(df)<60: continue
        if "Date" not in df.columns: df=df.reset_index()
        try:
            pats=detect_patterns(df,sym)
            if pats: by_sym[sym]=pats; all_pats.extend(pats)
        except: pass
        if (i+1)%100==0: print(f"    …{i+1}/{n}")
    bull=sum(1 for p in all_pats if p.direction=="BULLISH")
    print(f"  ✅ Patterns:{len(all_pats)} | {bull} Bullish | {len(all_pats)-bull} Bearish | {len(by_sym)} stocks")
    return by_sym,all_pats

# ─────────────────────────────────────────────────────────────────────────────
#  CSV LOADING
# ─────────────────────────────────────────────────────────────────────────────
def load_csv_constituents(csv_path, is_nse=True):
    if not os.path.exists(csv_path): return []
    try:
        df=pd.read_csv(csv_path); df.columns=df.columns.str.strip()
        if "Symbol" not in df.columns: return []
        if "Series" in df.columns:
            df=df[df["Series"].str.strip().str.upper().isin(["EQ",""])]
        syms=df["Symbol"].str.strip().dropna().tolist()
        return [s+".NS" for s in syms] if is_nse else syms
    except: return []

# ─────────────────────────────────────────────────────────────────────────────
#  PEER GROUP METRICS
# ─────────────────────────────────────────────────────────────────────────────
def peer_group_metrics(universe, price_data, index_prices, periods=None):
    if periods is None: periods=SIGNAL_PERIODS
    idx=_normalize(index_prices.dropna())
    bench_ret={p:pct_change_n(idx,p) for p in periods}
    stock_rets={}
    for sym in universe["Yahoo"].tolist():
        if sym not in price_data.columns: continue
        prices=price_data[sym].dropna()
        stock_rets[sym]={p:pct_change_n(prices,p) for p in periods}
    def _agg(gcol):
        rm={}; rsm={}
        for grp in universe[gcol].unique():
            if not isinstance(grp,str) or not grp.strip(): continue
            syms=universe[universe[gcol]==grp]["Yahoo"].tolist()
            rm[grp]={}; rsm[grp]={}
            for p in periods:
                vals=[stock_rets[s][p] for s in syms if s in stock_rets and not np.isnan(stock_rets[s].get(p,np.nan))]
                if vals:
                    avg=float(np.mean(vals)); rm[grp][p]=round(avg,2)
                    br=bench_ret.get(p,np.nan)
                    rsm[grp][p]=round((1+avg/100)/(1+br/100)-1,4) if not np.isnan(br) else np.nan
                else: rm[grp][p]=np.nan; rsm[grp][p]=np.nan
        return rm,rsm
    sr,srs=_agg("Sector"); ir,irs=_agg("Industry")
    return sr,ir,srs,irs

# ─────────────────────────────────────────────────────────────────────────────
#  ROTATION ROW
# ─────────────────────────────────────────────────────────────────────────────
def rotation_row(group_stocks, price_data, index_prices, name):
    rs55_a=rsi_a=sma20_a=sma50_a=sma100_a=adv=dec=unch=0
    z1={"H":0,"M":0,"L":0}; z3={"H":0,"M":0,"L":0}; z6={"H":0,"M":0,"L":0}
    valid=0
    for sym in group_stocks:
        if sym not in price_data.columns: continue
        prices=price_data[sym].dropna()
        if len(prices)<22: continue
        valid+=1; cur=float(prices.iloc[-1])
        c1=pct_change_n(prices,1)
        if not np.isnan(c1):
            if c1>0: adv+=1
            elif c1<0: dec+=1
            else: unch+=1
        rs55=calc_rs(prices,index_prices,55)
        if rs55==rs55 and rs55>0: rs55_a+=1
        rsi=calc_rsi(prices)
        if rsi==rsi and rsi>50: rsi_a+=1
        s20=calc_sma(prices,20); s50=calc_sma(prices,50); s100=calc_sma(prices,100)
        if s20==s20 and cur>s20: sma20_a+=1
        if s50==s50 and cur>s50: sma50_a+=1
        if s100==s100 and cur>s100: sma100_a+=1
        def _zone(n,zd):
            if len(prices)<n: zd["L"]+=1; return
            hi=float(prices.iloc[-n:].max()); lo=float(prices.iloc[-n:].min()); r=hi-lo
            if r<=0: zd["M"]+=1; return
            pos=(cur-lo)/r; zd["H" if pos>=0.67 else ("M" if pos>=0.33 else "L")]+=1
        _zone(22,z1); _zone(55,z3); _zone(120,z6)
    if valid==0: return None
    def pct(n): return round(n/valid*100)
    def score(z): return round((z["H"]*2+z["M"])/max(valid*2,1)*100)
    def zlbl(s): return "Bullish" if s>=60 else ("Neutral" if s>=40 else "Bearish")
    s1,s3,s6=score(z1),score(z3),score(z6)
    return {"Name":name,"Stocks":len(group_stocks),"Valid":valid,
            "Advancing":adv,"Declining":dec,"Unchanged":unch,"Adv/Dec":f"{adv}/{dec}",
            "RS55%":pct(rs55_a),"RSI50%":pct(rsi_a),
            "AbvSMA20%":pct(sma20_a),"AbvSMA50%":pct(sma50_a),"AbvSMA100%":pct(sma100_a),
            "1M_Score":s1,"1M_Zone":zlbl(s1),"3M_Score":s3,"3M_Zone":zlbl(s3),
            "6M_Score":s6,"6M_Zone":zlbl(s6)}

def _tv(orig, market):
    return f"NSE:{orig}," if market=="INDIA" else f"{orig},"

# ─────────────────────────────────────────────────────────────────────────────
#  ❶ MARKET SNAPSHOT
# ─────────────────────────────────────────────────────────────────────────────
def build_market_snapshot(market):
    tickers=SNAPSHOT_TICKERS.get(market,[]); syms=[t["ticker"] for t in tickers]
    try:
        raw=yf.download(syms,period="5d",auto_adjust=True,progress=False,threads=True)
        cdf=raw["Close"] if isinstance(raw.columns,pd.MultiIndex) else raw[["Close"]]
        if not isinstance(raw.columns,pd.MultiIndex) and len(syms)==1: cdf.columns=syms
    except: cdf=pd.DataFrame()
    rows=[]
    for t in tickers:
        sym=t["ticker"]; price=chg1=chg5=np.nan; trend="N/A"
        try:
            if sym in cdf.columns:
                col=cdf[sym].dropna()
                if len(col)>=2: price=round(float(col.iloc[-1]),2); chg1=round(pct_change_n(col,1),2)
                if len(col)>=5: chg5=round(pct_change_n(col,4),2)
                if not np.isnan(chg1) and not np.isnan(chg5):
                    if chg1>0 and chg5>0: trend="↑ Bullish"
                    elif chg1<0 and chg5<0: trend="↓ Bearish"
                    elif chg5>0: trend="→ Recovering"
                    else: trend="→ Pulling Back"
        except: pass
        rows.append({"Name":t["name"],"Type":t["type"],"Price":price,"Chg_1D%":chg1,"Chg_5D%":chg5,"Trend":trend})
    df=pd.DataFrame(rows)
    idxs=df[df["Type"]=="Index"]["Chg_1D%"].dropna()
    pct_up=(idxs>0).mean()*100 if len(idxs)>0 else 50
    bias="BULLISH" if pct_up>=70 else ("BEARISH" if pct_up<40 else "MIXED")
    df=pd.concat([df,pd.DataFrame([{"Name":f"── MACRO BIAS: {bias} ({len(idxs)} indices, {pct_up:.0f}% green) ──",
                                     "Type":"Summary","Price":np.nan,"Chg_1D%":np.nan,"Chg_5D%":np.nan,"Trend":bias}])],ignore_index=True)
    return df

# ─────────────────────────────────────────────────────────────────────────────
#  ❷ SECTOR STRENGTH
# ─────────────────────────────────────────────────────────────────────────────
def build_sector_strength(universe, price_data, index_prices, sector_prices):
    rows=[]
    for sec,s_pr in sector_prices.items():
        if isinstance(s_pr,pd.Series) and len(s_pr)<22: continue
        syms=universe[universe["Sector"]==sec]["Yahoo"].tolist() if not universe.empty else []
        rs22=calc_rs(s_pr,index_prices,22) if len(s_pr)>22 else np.nan
        rs55=calc_rs(s_pr,index_prices,55) if len(s_pr)>55 else np.nan
        rs120=calc_rs(s_pr,index_prices,120) if len(s_pr)>120 else np.nan
        rsi=calc_rsi(s_pr) if len(s_pr)>=14 else np.nan
        rs22p=round(rs22*100,2) if rs22==rs22 else np.nan
        rs55p=round(rs55*100,2) if rs55==rs55 else np.nan
        rs120p=round(rs120*100,2) if rs120==rs120 else np.nan
        adv=dec=0; chg1v=[]; chg5v=[]
        for sym in syms:
            if sym not in price_data.columns: continue
            p=price_data[sym].dropna()
            c1=pct_change_n(p,1); c5=pct_change_n(p,4)
            if not np.isnan(c1): chg1v.append(c1); adv+=1 if c1>0 else 0; dec+=1 if c1<0 else 0
            if not np.isnan(c5): chg5v.append(c5)
        avg1=round(float(np.mean(chg1v)),2) if chg1v else np.nan
        avg5=round(float(np.mean(chg5v)),2) if chg5v else np.nan
        vi=(rs22==rs22 and rs55==rs55)
        if vi:
            if rs22>0 and rs55>0: sig="Buy"
            elif rs22<0 and rs55<0: sig="Sell"
            else: sig="Neutral"
        else: sig="Neutral"
        trend="Bullish" if sig=="Buy" else ("Bearish" if sig=="Sell" else "Mixed")
        rows.append({"Sector":sec,"Stocks":len(syms),"Adv":adv,"Dec":dec,"Adv/Dec":f"{adv}/{dec}",
                     "Avg_Chg_1D%":avg1,"Avg_Chg_5D%":avg5,"RS_22d%":rs22p,"RS_55d%":rs55p,
                     "RS_120d%":rs120p,"RSI_14":round(rsi,1) if rsi==rsi else np.nan,"Signal":sig,"Trend":trend})
    if not rows: return pd.DataFrame()
    df=pd.DataFrame(rows).sort_values("RS_55d%",ascending=False,na_position="last").reset_index(drop=True)
    df.insert(0,"Rank",df.index+1); return df

# ─────────────────────────────────────────────────────────────────────────────
#  ❸ SECTOR ROTATION (trimmed)
# ─────────────────────────────────────────────────────────────────────────────
def build_sector_rotation(universe, price_data, index_prices):
    rows=[]
    for sec in sorted(universe["Sector"].unique()):
        syms=universe[universe["Sector"]==sec]["Yahoo"].tolist()
        row=rotation_row(syms,price_data,index_prices,sec)
        if row: rows.append(row)
    if not rows: return pd.DataFrame()
    df=pd.DataFrame(rows).sort_values("RS55%",ascending=False).reset_index(drop=True)
    df.insert(0,"Rank",df.index+1)
    keep=["Rank","Name","Stocks","Adv/Dec","RS55%","RSI50%","AbvSMA20%","AbvSMA50%","AbvSMA100%",
          "1M_Score","1M_Zone","3M_Score","3M_Zone","6M_Score","6M_Zone"]
    return df[[c for c in keep if c in df.columns]]

# ─────────────────────────────────────────────────────────────────────────────
#  ❹ INDUSTRY ROTATION (trimmed)
# ─────────────────────────────────────────────────────────────────────────────
def build_industry_rotation(universe, price_data, index_prices):
    rows=[]
    for ind in sorted(universe["Industry"].unique()):
        syms=universe[universe["Industry"]==ind]["Yahoo"].tolist()
        sec=universe[universe["Industry"]==ind]["Sector"].iloc[0] if len(syms)>0 else "—"
        row=rotation_row(syms,price_data,index_prices,ind)
        if row: row["Sector"]=sec; rows.append(row)
    if not rows: return pd.DataFrame()
    df=pd.DataFrame(rows).sort_values("RS55%",ascending=False).reset_index(drop=True)
    df.insert(0,"Rank",df.index+1)
    keep=["Rank","Name","Sector","Stocks","Adv/Dec","RS55%","RSI50%","AbvSMA20%","AbvSMA50%","AbvSMA100%",
          "1M_Score","1M_Zone","3M_Score","3M_Zone","6M_Score","6M_Zone"]
    return df[[c for c in keep if c in df.columns]]

# ─────────────────────────────────────────────────────────────────────────────
#  ❺ MARKET BREADTH
# ─────────────────────────────────────────────────────────────────────────────
def build_market_breadth(price_data, index_prices, breadth_cfg, index_data_dir, market="INDIA"):
    rows=[]; is_nse=(market=="INDIA")
    for iname,cfg in breadth_cfg.items():
        csv_f=cfg.get("csv"); syms=[]
        if csv_f and index_data_dir:
            path=os.path.join(index_data_dir,csv_f)
            if os.path.exists(path): syms=load_csv_constituents(path,is_nse=is_nse)
        if not syms: syms=list(price_data.columns)
        row=rotation_row(syms,price_data,index_prices,iname)
        if not row: continue
        sma200_a=v200=0
        for sym in syms:
            if sym not in price_data.columns: continue
            p=price_data[sym].dropna()
            if len(p)<200: continue
            v200+=1; s200=calc_sma(p,200)
            if s200==s200 and float(p.iloc[-1])>s200: sma200_a+=1
        row["AbvSMA200%"]=round(sma200_a/max(v200,1)*100)
        yahoo=cfg.get("yahoo"); row["Index_Price"]=np.nan
        if yahoo:
            try:
                raw=yf.download(yahoo,period="5d",auto_adjust=True,progress=False)
                if not raw.empty:
                    cl=raw["Close"]
                    if isinstance(cl,pd.DataFrame): cl=cl.squeeze()
                    row["Index_Price"]=round(float(cl.dropna().iloc[-1]),2)
            except: pass
        rows.append(row)
    if not rows: return pd.DataFrame()
    df=pd.DataFrame(rows)
    keep=["Name","Index_Price","Stocks","Valid","Advancing","Declining","Adv/Dec",
          "RS55%","RSI50%","AbvSMA20%","AbvSMA50%","AbvSMA100%","AbvSMA200%",
          "1M_Score","1M_Zone","3M_Score","3M_Zone","6M_Score","6M_Zone"]
    return df[[c for c in keep if c in df.columns]]

# ─────────────────────────────────────────────────────────────────────────────
#  ❻ SECTOR PERFORMANCE
# ─────────────────────────────────────────────────────────────────────────────
def build_sector_performance(sector_prices, index_prices):
    rows=[]
    for sec,prices in sector_prices.items():
        if isinstance(prices,pd.DataFrame): prices=prices.squeeze()
        if not isinstance(prices,pd.Series) or len(prices)<22: continue
        row={"Sector":sec}
        for lbl,n in [("1M%",22),("3M%",66),("6M%",132),("12M%",252)]:
            v=pct_change_n(prices,n); row[lbl]=round(v,2) if not np.isnan(v) else np.nan
        try:
            jan1=pd.Timestamp(f"{datetime.now().year}-01-01")
            past=prices[prices.index<=jan1]
            row["YTD%"]=round((float(prices.iloc[-1])/float(past.iloc[-1])-1)*100,2) if len(past)>0 else np.nan
        except: row["YTD%"]=np.nan
        for lbl2,n2 in [("RS_1M%",22),("RS_3M%",55),("RS_6M%",120)]:
            rs=calc_rs(prices,index_prices,n2); row[lbl2]=round(rs*100,2) if rs==rs else np.nan
        rows.append(row)
    if not rows: return pd.DataFrame()
    df=pd.DataFrame(rows).sort_values("3M%",ascending=False,na_position="last").reset_index(drop=True)
    df.insert(0,"Rank",df.index+1); return df

# ─────────────────────────────────────────────────────────────────────────────
#  ❼ STOCK STRENGTH  (v5.2 — full signal suite + swing SL)
# ─────────────────────────────────────────────────────────────────────────────
def build_stock_strength(universe, price_data, index_prices, sector_prices,
                          patterns_by_sym, ohlcv_dict=None,
                          market="INDIA", fetch_financials=True):
    """
    Per-stock columns added in v5.2:
    Supertrend (daily), Supertrend_W (weekly),
    W_RS21%, W_RS30%, W_RSI, M_RS12%, M_RSI,
    W_EMA10, W_EMA30, W_EMA10_gtEMA30,
    EMA200_D, Abv_EMA200,
    Swing_High_20d, Swing_Low_20d, Breakout_Up,
    SL_Buy%, SL_Sell%, SL_Buy_Price, SL_Grade,
    MST_Signal, LST_Signal, RS30_Signal,

    SIGNAL LOGIC:
    Buy:  RS_22d_Idx>0 AND RS_55d_Idx>0
          AND (if sector) RS_22d_Sec>0 AND RS_55d_Sec>0
    Sell: same conditions but all negative
    Strong Buy: Buy + Beats_Sec + Beats_Ind + Sec_Beats + Ind_Beats
    """
    p1,p2=SIGNAL_PERIODS; total=len(universe)
    sr,ir,srs,irs=peer_group_metrics(universe,price_data,index_prices)
    sec_idx_rs={}
    for sname,s_pr in sector_prices.items():
        if isinstance(s_pr,pd.Series) and len(s_pr)>=22:
            sec_idx_rs[sname]={p:calc_rs(s_pr,index_prices,p) for p in SIGNAL_PERIODS}
    def _best_sec_rs(sname,p):
        v=sec_idx_rs.get(sname,{}).get(p,np.nan)
        return v if v==v else srs.get(sname,{}).get(p,np.nan)
    fin_data=get_financials_batch(universe["Yahoo"].tolist()) if fetch_financials else {}
    rows=[]
    for i,(_,ur) in enumerate(universe.iterrows()):
        sym=ur["Yahoo"]; orig=ur["Symbol"]; name=ur.get("Company Name",sym)
        industry=ur.get("Industry",""); sector=ur.get("Sector","")
        if sym not in price_data.columns: continue
        prices=price_data[sym].dropna()
        if len(prices)<22: continue
        cur=float(prices.iloc[-1])
        s_pr=sector_prices.get(sector); s_ok=isinstance(s_pr,pd.Series) and len(s_pr)>=22
        rs_idx={p:calc_rs(prices,index_prices,p) for p in RS_PERIODS}
        rs_sec={p:(calc_rs(prices,s_pr,p) if s_ok else np.nan) for p in SIGNAL_PERIODS}
        r_i1,r_i2=rs_idx.get(p1,np.nan),rs_idx.get(p2,np.nan)
        r_s1,r_s2=rs_sec.get(p1,np.nan),rs_sec.get(p2,np.nan)
        vi=not np.isnan(r_i1) and not np.isnan(r_i2)
        vs=not np.isnan(r_s1) and not np.isnan(r_s2)
        # ── SIGNAL ──────────────────────────────────────────────────────────
        if vi and vs:
            if r_i1>0 and r_i2>0 and r_s1>0 and r_s2>0: sig="Buy"
            elif r_i1<0 and r_i2<0 and r_s1<0 and r_s2<0: sig="Sell"
            else: sig="Neutral"
        elif vi:
            if r_i1>0 and r_i2>0: sig="Buy"
            elif r_i1<0 and r_i2<0: sig="Sell"
            else: sig="Neutral"
        else: sig="Neutral"
        # ── PEER ────────────────────────────────────────────────────────────
        ret_p2=pct_change_n(prices,p2)
        sa=sr.get(sector,{}).get(p2,np.nan); ia=ir.get(industry,{}).get(p2,np.nan)
        def _b(a,b_): return a>=b_ if (a==a and b_==b_) else None
        b_sec=_b(ret_p2,sa); b_ind=_b(ret_p2,ia)
        sec_rs_p2=_best_sec_rs(sector,p2)
        ind_rs_p2=irs.get(industry,{}).get(p2,np.nan)
        s_beats=(sec_rs_p2>0) if sec_rs_p2==sec_rs_p2 else None
        i_beats=(ind_rs_p2>0) if ind_rs_p2==ind_rs_p2 else None
        def _t(f): return "✓" if f is True else ("✗" if f is False else "—")
        enh="Strong Buy" if (sig=="Buy" and b_sec is True and b_ind is True and s_beats is True and i_beats is True) else sig
        # ── BASIC TECHNICALS ────────────────────────────────────────────────
        tech=get_technicals(prices)
        h_day=days_since_high(prices,HL_LOOKBACK)
        from52=pct_from_52w_high(prices)
        chg1=pct_change_n(prices,1); chg5=pct_change_n(prices,4)
        pats=patterns_by_sym.get(sym,[])
        bp=[p.pattern for p in pats if p.direction=="BULLISH"]
        bep=[p.pattern for p in pats if p.direction=="BEARISH"]
        cp=("🟢 "+bp[0]) if bp else (("🔴 "+bep[0]) if bep else "")
        fin=fin_data.get(sym,{})
        # ── v5.2: OHLCV-BASED SIGNALS ───────────────────────────────────────
        ohlcv_df = ohlcv_dict.get(sym) if ohlcv_dict else None
        high_s = ohlcv_df["High"]  if ohlcv_df is not None and "High"  in ohlcv_df.columns else None
        low_s  = ohlcv_df["Low"]   if ohlcv_df is not None and "Low"   in ohlcv_df.columns else None

        # Supertrend daily
        st_line_d, st_dir_s = calc_supertrend_from_df(ohlcv_df, MST_ST_PERIOD, MST_ST_FACTOR)
        st_line_w, st_dir_w_s = calc_supertrend_from_df(ohlcv_df, LST_ST_PERIOD, LST_ST_FACTOR, freq='W')

        st_dir = "Buy" if len(st_dir_s) and int(st_dir_s.iloc[-1]) == 1 else "Sell" if len(st_dir_s) and int(st_dir_s.iloc[-1]) == -1 else "N/A"
        st_dir_w = "Buy" if len(st_dir_w_s) and int(st_dir_w_s.iloc[-1]) == 1 else "Sell" if len(st_dir_w_s) and int(st_dir_w_s.iloc[-1]) == -1 else "N/A"

        # Swing High/Low → SL
        swing_d = calc_swing_sl(prices, high_s, low_s, lookback=20)

        # Multi-TF RS
        w_rs21 = calc_rs_tf(prices, index_prices, MST_RS_HTF, 'W')   # Weekly RS(21)
        w_rs30 = calc_rs_tf(prices, index_prices, RS30_RS_PERIOD, 'W') # Weekly RS(30)
        m_rs12 = calc_rs_tf(prices, index_prices, LST_RS_HTF,  'M')   # Monthly RS(12)

        # Multi-TF RSI
        w_rsi  = calc_rsi_tf(prices, MST_RSI_LEN,  'W')   # Weekly RSI(14)
        m_rsi  = calc_rsi_tf(prices, LST_RSI_LEN,  'M')   # Monthly RSI(12)

        # Weekly EMA (for RS30 filter)
        w_ema10 = calc_ema_tf(prices, RS30_EMA_S, 'W')   # Weekly EMA(10)
        w_ema30 = calc_ema_tf(prices, RS30_EMA_L, 'W')   # Weekly EMA(30)

        # Daily EMA(200) for MST/LST entry filter
        ema200_d = calc_ema_tf(prices, 200, 'D')

        # ── MST SIGNAL ────────────────────────────────────────────────────────
        mst_sig = calc_mst_signal(
            prices,
            index_prices,
            st_dir,
            swing_d,
            r_i2,
            tech["RSI_14"],
            ema200_d,
            w_rs21,
            w_rsi,
        )
        # ── LST SIGNAL ────────────────────────────────────────────────────────
        lst_sig = calc_lst_signal(
            prices,        # daily close
            index_prices,  # benchmark
            st_dir_w,      # weekly Supertrend direction string
            swing_d,       # swing SL dict
            m_rs12,        # monthly RS(12) — HTF pre-condition
            m_rsi,         # monthly RSI(12) — HTF pre-condition
            fin,           # financial data for revenue/PAT check
        )
        # ── RS30 SIGNAL ───────────────────────────────────────────────────────
        rs30_sig = calc_rs30_signal(
            prices,        # daily close
            index_prices,  # benchmark
            swing_d,       # swing SL dict (has swing_high_break)
            fin,           # financial data
            w_rs30,        # weekly RS(30)
            w_ema10,       # weekly EMA(10)
            w_ema30,       # weekly EMA(30)
            market,        # "INDIA" or "US" for MCap threshold
        )

        sl_buy_pct  = swing_d.get("sl_buy_pct",  np.nan)
        sl_sell_pct = swing_d.get("sl_sell_pct", np.nan)
        active_sl   = sl_buy_pct if sig != "Sell" else sl_sell_pct
        grade       = sl_grade(active_sl)

        row={
            "Symbol":orig,"TV_Symbol":_tv(orig,market),"Company":name,
            "Sector":sector,"Industry":industry,"Price":round(cur,2),
            "Chg_1D%":round(chg1,2) if chg1==chg1 else np.nan,
            "Chg_5D%":round(chg5,2) if chg5==chg5 else np.nan,
            f"RS_{p1}d_Idx%":round(r_i1*100,2) if r_i1==r_i1 else np.nan,
            f"RS_{p2}d_Idx%":round(r_i2*100,2) if r_i2==r_i2 else np.nan,
            f"RS_{p1}d_Sec%":round(r_s1*100,2) if r_s1==r_s1 else np.nan,
            f"RS_{p2}d_Sec%":round(r_s2*100,2) if r_s2==r_s2 else np.nan,
            "RS_120d_Idx%": round(rs_idx.get(120,np.nan)*100,2) if rs_idx.get(120,np.nan)==rs_idx.get(120,np.nan) else np.nan,
            "RS_252d_Idx%": round(rs_idx.get(252,np.nan)*100,2) if rs_idx.get(252,np.nan)==rs_idx.get(252,np.nan) else np.nan,
            "Signal":sig,"Enhanced":enh,
            # Daily technicals
            "RSI_14":tech["RSI_14"],"Trend":tech["Trend"],"SMA_Score":tech["SMA_Score"],
            "Abv_SMA20":tech["Abv_SMA20"],"Abv_SMA50":tech["Abv_SMA50"],
            "Abv_SMA100":tech["Abv_SMA100"],"Abv_SMA200":tech["Abv_SMA200"],
            "EMA200_D":round(ema200_d,2) if ema200_d==ema200_d else np.nan,
            "Abv_EMA200":"✓" if (ema200_d==ema200_d and cur>ema200_d) else "✗",
            "H_Day":h_day,"From_52W_High%":round(from52,1) if from52==from52 else np.nan,
            # v5.2: Supertrend
            "Supertrend":st_dir,"Supertrend_W":st_dir_w,
            # v5.2: Multi-TF RS
            "W_RS21%":round(w_rs21*100,2) if w_rs21==w_rs21 else np.nan,
            "W_RS30%":round(w_rs30*100,2) if w_rs30==w_rs30 else np.nan,
            "M_RS12%":round(m_rs12*100,2) if m_rs12==m_rs12 else np.nan,
            # v5.2: Multi-TF RSI
            "W_RSI":round(w_rsi,1) if w_rsi==w_rsi else np.nan,
            "M_RSI":round(m_rsi,1) if m_rsi==m_rsi else np.nan,
            # v5.2: Weekly EMA for RS30
            "W_EMA10":round(w_ema10,2) if w_ema10==w_ema10 else np.nan,
            "W_EMA30":round(w_ema30,2) if w_ema30==w_ema30 else np.nan,
            "W_EMA10_gtEMA30":"✓" if (w_ema10==w_ema10 and w_ema30==w_ema30 and w_ema10>w_ema30) else "✗",
            # v5.2: Swing HL + SL
            "Swing_High_20d":swing_d.get("swing_high",np.nan),
            "Swing_Low_20d": swing_d.get("swing_low", np.nan),
            "Breakout_Up":   "✓" if swing_d.get("is_breakout_up") else "✗",
            "SL_Buy%":       sl_buy_pct,   # % from current to swing low (stop for buys)
            "SL_Sell%":      sl_sell_pct,  # % from swing high to current (stop for sells)
            "SL_Buy_Price":  swing_d.get("sl_buy_price",  np.nan),
            "SL_Sell_Price": swing_d.get("sl_sell_price", np.nan),
            "SL_Grade":      grade,
            # v5.2: Strategy signals
            "MST_Signal":mst_sig,"LST_Signal":lst_sig,"RS30_Signal":rs30_sig,
            # Peer comparison
            "Chart_Pattern":cp,"Beats_Sec":_t(b_sec),"Beats_Ind":_t(b_ind),
            "Sec_Beats":_t(s_beats),"Ind_Beats":_t(i_beats),
            f"Ret_{p2}d%":round(ret_p2,2) if ret_p2==ret_p2 else np.nan,
            # Financials
            "Sales_QoQ%":fin.get("SalesQoQ",np.nan),"Sales_YoY%":fin.get("SalesYoY",np.nan),
            "PAT_QoQ%":fin.get("PATQoQ",np.nan),"PAT_YoY%":fin.get("PATYoY",np.nan),
            "PAT_Curr_M":fin.get("PATCurr",np.nan),"Margin%":fin.get("Margin",np.nan),
            "ROE%":fin.get("ROE",np.nan),"D/E":fin.get("DE",np.nan),
            "EPS":fin.get("EPS",np.nan),"P/E":fin.get("PE",np.nan),
            "Mkt_Cap_B":fin.get("MktCap",np.nan),
        }
        rows.append(row)
        if (i+1)%100==0: print(f"    …{i+1}/{total}")
    df=pd.DataFrame(rows)
    if df.empty: return df
    rs1c,rs2c=f"RS_{p1}d_Idx%",f"RS_{p2}d_Idx%"
    sc=pd.Series(0.0,index=df.index); wt=pd.Series(0.0,index=df.index)
    for col,w in [(rs1c,0.35),(rs2c,0.30),("RS_120d_Idx%",0.20),("RS_252d_Idx%",0.15)]:
        m=df[col].notna(); sc[m]+=df.loc[m,col]*w; wt[m]+=w
    df["RS_Score"]=(sc/wt.replace(0,np.nan)).round(2)
    fs=pd.Series(0.0,index=df.index)
    for col,thresh,pts in [("Sales_YoY%",15,2),("PAT_YoY%",15,2),("ROE%",15,2),("Margin%",10,1)]:
        if col in df.columns: fs[df[col].notna()&(df[col]>=thresh)]+=pts
    if "D/E" in df.columns: fs[df["D/E"].notna()&(df["D/E"]<1)]+=1
    df["Fin_Score"]=fs.astype(int)
    # Enhanced score with SL bonus
    active_sl_col = df["SL_Buy%"].where(df["Signal"]!="Sell", df["SL_Sell%"])
    df["SL_Bonus"] = active_sl_col.apply(lambda x: sl_bonus(x))
    df["Total_Score"]=(df["RS_Score"].fillna(0)*0.6+df["Fin_Score"]*2+df["SL_Bonus"]).round(2)
    om={"Strong Buy":0,"Buy":1,"Neutral":2,"Sell":3}
    df["_o"]=df["Enhanced"].map(om).fillna(2)
    df=df.sort_values(["_o","Total_Score"],ascending=[True,False]).drop(columns=["_o"]).reset_index(drop=True)
    sb=(df["Enhanced"]=="Strong Buy").sum(); b=(df["Signal"]=="Buy").sum(); s=(df["Signal"]=="Sell").sum()
    mst_b=(df["MST_Signal"]=="Buy").sum(); lst_b=(df["LST_Signal"]=="Buy").sum()
    rs30_b=(df["RS30_Signal"]=="Buy").sum()
    print(f"  ✅ Stocks:{len(df)} | ⭐SB:{sb} | Buy:{b} | Sell:{s}")
    print(f"     MST Buy:{mst_b} | LST Buy:{lst_b} | RS30 Buy:{rs30_b}")
    return df

# ─────────────────────────────────────────────────────────────────────────────
#  ❽ TOP PICKS — BUY (strongest sector first, ≥5 stocks each)
# ─────────────────────────────────────────────────────────────────────────────
def build_top_picks_buy(stock_df, sector_str_df, market="INDIA", top_n=5):
    if stock_df.empty or sector_str_df.empty: return pd.DataFrame()
    p2=SIGNAL_PERIODS[1]; prefix="NSE:" if market=="INDIA" else ""
    sec_meta={}
    for _,r in sector_str_df.sort_values("RS_55d%",ascending=False,na_position="last").iterrows():
        sec_meta[r["Sector"]]={"rank":int(r.get("Rank",99)),"signal":r.get("Signal","Neutral"),
                                "rs55":r.get("RS_55d%",np.nan)}
    rows=[]; rank=0
    for sec in sorted(sec_meta.keys(),key=lambda s:sec_meta[s]["rank"]):
        m=sec_meta[sec]
        picks=stock_df[(stock_df["Sector"]==sec)&(stock_df["Signal"].isin(["Buy","Strong Buy"]))]
        picks=picks.sort_values("Total_Score",ascending=False).head(top_n)
        if picks.empty: continue
        for _,r in picks.iterrows():
            rank+=1
            sl=r.get("SL_Buy%",np.nan); gr=r.get("SL_Grade","—")
            rows.append({
                "Rank":rank,"Sec_Rank":m["rank"],"Sector":sec,
                "Sec_Signal":m["signal"],"Sec_RS55%":m["rs55"],
                "Symbol":r.get("Symbol",""),"TV_Symbol":f"{prefix}{r.get('Symbol','')},",
                "Company":r.get("Company",""),"Price":r.get("Price",np.nan),
                "Chg_1D%":r.get("Chg_1D%",np.nan),
                "RS_22d_Idx%":r.get(f"RS_{SIGNAL_PERIODS[0]}d_Idx%",np.nan),
                "RS_55d_Idx%":r.get(f"RS_{p2}d_Idx%",np.nan),
                "Signal":r.get("Signal",""),"Enhanced":r.get("Enhanced",""),
                "MST_Signal":r.get("MST_Signal",""),"LST_Signal":r.get("LST_Signal",""),
                "RS30_Signal":r.get("RS30_Signal",""),
                "Supertrend":r.get("Supertrend",""),"W_RS21%":r.get("W_RS21%",np.nan),
                "RSI_14":r.get("RSI_14",np.nan),"Trend":r.get("Trend",""),
                "SMA_Score":r.get("SMA_Score",np.nan),
                "SL_Buy%":sl,"SL_Grade":gr,"SL_Buy_Price":r.get("SL_Buy_Price",np.nan),
                "Breakout_Up":r.get("Breakout_Up",""),
                "Chart_Pattern":r.get("Chart_Pattern",""),
                "Fin_Score":r.get("Fin_Score",np.nan),"Total_Score":r.get("Total_Score",np.nan),
                "Sales_YoY%":r.get("Sales_YoY%",np.nan),"PAT_YoY%":r.get("PAT_YoY%",np.nan),
                "ROE%":r.get("ROE%",np.nan),"D/E":r.get("D/E",np.nan),
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame({"Message":["No Buy signals found."]})

# ─────────────────────────────────────────────────────────────────────────────
#  ❾ TOP PICKS — SELL (weakest sector first, ≥5 stocks each)
# ─────────────────────────────────────────────────────────────────────────────
def build_top_picks_sell(stock_df, sector_str_df, market="INDIA", top_n=5):
    if stock_df.empty or sector_str_df.empty: return pd.DataFrame()
    prefix="NSE:" if market=="INDIA" else ""
    sec_meta={}
    for _,r in sector_str_df.sort_values("RS_55d%",ascending=True,na_position="last").iterrows():
        sec_meta[r["Sector"]]={"rank":int(r.get("Rank",99)),"signal":r.get("Signal","Neutral"),
                                "rs55":r.get("RS_55d%",np.nan)}
    sorted_secs=sorted(sec_meta.keys(),key=lambda s:sec_meta[s]["rs55"] if not np.isnan(sec_meta[s]["rs55"]) else 999)
    rows=[]; rank=0
    for sec in sorted_secs:
        m=sec_meta[sec]
        picks=stock_df[(stock_df["Sector"]==sec)&(stock_df["Signal"]=="Sell")]
        picks=picks.sort_values("Total_Score",ascending=True).head(top_n)
        if picks.empty: continue
        for _,r in picks.iterrows():
            rank+=1
            sl=r.get("SL_Sell%",np.nan); gr=sl_grade(sl)
            rows.append({
                "Rank":rank,"Sec_Rank":m["rank"],"Sector":sec,
                "Sec_Signal":m["signal"],"Sec_RS55%":m["rs55"],
                "Symbol":r.get("Symbol",""),"TV_Symbol":f"{prefix}{r.get('Symbol','')},",
                "Company":r.get("Company",""),"Price":r.get("Price",np.nan),
                "Chg_1D%":r.get("Chg_1D%",np.nan),
                "RS_22d_Idx%":r.get(f"RS_{SIGNAL_PERIODS[0]}d_Idx%",np.nan),
                "RS_55d_Idx%":r.get(f"RS_{SIGNAL_PERIODS[1]}d_Idx%",np.nan),
                "Signal":"Sell","Supertrend":r.get("Supertrend",""),
                "W_RS21%":r.get("W_RS21%",np.nan),
                "RSI_14":r.get("RSI_14",np.nan),"Trend":r.get("Trend",""),
                "SMA_Score":r.get("SMA_Score",np.nan),
                "SL_Sell%":sl,"SL_Grade":gr,"SL_Sell_Price":r.get("SL_Sell_Price",np.nan),
                "Chart_Pattern":r.get("Chart_Pattern",""),
                "Total_Score":r.get("Total_Score",np.nan),
                "Sales_YoY%":r.get("Sales_YoY%",np.nan),"PAT_YoY%":r.get("PAT_YoY%",np.nan),
                "ROE%":r.get("ROE%",np.nan),"D/E":r.get("D/E",np.nan),
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame({"Message":["No Sell signals found."]})

# ─────────────────────────────────────────────────────────────────────────────
#  ❿ CHART PATTERNS (last 14 days, proper Dates column, with TV_Symbol)
# ─────────────────────────────────────────────────────────────────────────────
def build_chart_patterns_df(patterns_list, stock_df, market="INDIA"):
    cutoff=(datetime.now()-timedelta(days=14)).strftime("%Y-%m-%d")
    recent=[p for p in patterns_list if p.date>=cutoff]
    prefix="NSE:" if market=="INDIA" else ""
    sym_info={}
    if not stock_df.empty:
        for _,r in stock_df.iterrows():
            sym_info[r["Symbol"]]={"Sector":r.get("Sector",""),"Signal":r.get("Signal","Neutral"),
                                    "RS_Score":r.get("RS_Score",np.nan),"SL_Buy%":r.get("SL_Buy%",np.nan)}
    rows=[]
    for p in sorted(recent,key=lambda x:(0 if x.direction=="BULLISH" else 1,x.date)):
        orig=p.symbol.replace(".NS",""); info=sym_info.get(orig,{})
        rows.append({"Symbol":orig,"TV_Symbol":f"{prefix}{orig},","Sector":info.get("Sector",""),
                     "RS_Signal":info.get("Signal",""),"RS_Score":info.get("RS_Score",np.nan),
                     "SL_Buy%":info.get("SL_Buy%",np.nan),
                     "Pattern":p.pattern,"Direction":p.direction,"Dates":p.date,
                     "Entry":p.entry,"Stop":p.stop,"Target":p.target,"RR":p.rr,
                     "Confidence":p.confidence,"Notes":p.notes})
    if not rows:
        return pd.DataFrame({"Symbol":["—"],"TV_Symbol":["—"],"Pattern":[f"No patterns in last 14d (since {cutoff})"],
                             "Direction":["—"],"Dates":["—"]})
    df=pd.DataFrame(rows)
    df["Dates"]=pd.to_datetime(df["Dates"],errors="coerce").dt.strftime("%Y-%m-%d")
    return df

# ─────────────────────────────────────────────────────────────────────────────
#  ⓫ TRADE SETUPS  (v5.2: MST/LST/RS30 + SL + Targets + RR)
# ─────────────────────────────────────────────────────────────────────────────
def build_trade_setups(stock_df, sector_str_df, market="INDIA"):
    """
    Enhanced trade setups with:
    - Signal classification (RS30 > LST > MST > Strong Buy > RS Buy > Sell)
    - Swing-SL-based stop loss price and %
    - TP1 and TP2 targets based on strategy multipliers
    - Risk:Reward ratios
    - SL_Grade and SL_Bonus applied to Enhanced_Score
    """
    if stock_df.empty: return pd.DataFrame()
    prefix="NSE:" if market=="INDIA" else ""
    sec_sig={r["Sector"]:r.get("Signal","Neutral") for _,r in sector_str_df.iterrows()} if not sector_str_df.empty else {}
    sec_rs55={r["Sector"]:r.get("RS_55d%",np.nan) for _,r in sector_str_df.iterrows()} if not sector_str_df.empty else {}
    rows=[]
    for _,r in stock_df.iterrows():
        sig=r.get("Signal","Neutral"); enh=r.get("Enhanced","Neutral"); sector=r.get("Sector","")
        mst=r.get("MST_Signal","Neutral"); lst=r.get("LST_Signal","Neutral")
        rs30=r.get("RS30_Signal","Neutral")
        rs_sc=r.get("RS_Score",0) or 0; fin_sc=r.get("Fin_Score",0) or 0
        sl_buy_pct=r.get("SL_Buy%",np.nan); sl_sell_pct=r.get("SL_Sell%",np.nan)
        sl_buy_price=r.get("SL_Buy_Price",np.nan); sl_sell_price=r.get("SL_Sell_Price",np.nan)
        orig=r.get("Symbol",""); price=r.get("Price",np.nan) or 0
        st=r.get("Supertrend","N/A"); sec_signal=sec_sig.get(sector,"Neutral")

        # ── Classify trade ─────────────────────────────────────────────────────
        active_sl = sl_buy_pct if sig != "Sell" else sl_sell_pct
        active_sl_price = sl_buy_price if sig != "Sell" else sl_sell_price
        td = classify_trade(sig, enh, mst, lst, rs30, active_sl, fin_sc, rs_sc)

        # ── Target prices ──────────────────────────────────────────────────────
        def _price_tgt(pct_move):
            if pct_move==pct_move and price>0:
                if td["action"]=="BUY": return round(price*(1+pct_move/100),2)
                else: return round(price*(1-pct_move/100),2)
            return np.nan

        tp1_price = _price_tgt(td.get("tp1_pct",np.nan))
        tp2_price = _price_tgt(td.get("tp2_pct",np.nan))

        # ── Enhanced_Score (RS + Fin + SL bonus + RR bonus) ────────────────────
        rr_t1 = td.get("rr_t1",np.nan)
        sb = sl_bonus(active_sl, rr_t1)
        enhanced_score = round(rs_sc*0.6 + fin_sc*2 + sb, 2)

        # ── Setup type description ─────────────────────────────────────────────
        setup_desc = {
            "RS30 Buy": "🔥 RS30: Weekly RS30+EMA10>30+Near52W+Funda",
            "LST Buy":  "📈 LST:  Monthly pre-cond + Weekly entry + Supertrend",
            "MST Buy":  "🎯 MST:  Weekly pre-cond + Daily entry + Supertrend",
            "Strong Buy":"⭐ Strong Buy: RS Buy + All peer filters pass",
            "RS Buy":   "✅ RS Buy:  RS_22d>0 + RS_55d>0 (Index + Sector)",
            "Sell/Exit":"🔴 Sell: RS_22d<0 + RS_55d<0",
        }.get(td["signal_type"], f"⏳ {td['signal_type']}")

        # ── Notes ──────────────────────────────────────────────────────────────
        notes=[]
        try:
            h_day=r.get("H_Day",99)
            if h_day is not None and not np.isnan(float(h_day)) and int(h_day)<=3: notes.append("At highs")
        except: pass
        rsi=r.get("RSI_14",50) or 50
        if not np.isnan(float(rsi)):
            if rsi>70: notes.append("RSI overbought")
            elif rsi<30: notes.append("RSI oversold")
        if fin_sc>=6: notes.append("Strong funda")
        elif fin_sc==0 and td["action"]=="BUY": notes.append("Verify funda")
        sr55=sec_rs55.get(sector,np.nan)
        if not np.isnan(sr55): notes.append(f"Sec RS55:{sr55:+.1f}%")

        rows.append({
            "Rank":          0,
            "Symbol":        orig,
            "TV_Symbol":     f"{prefix}{orig},",
            "Company":       r.get("Company",""),
            "Sector":        sector,
            "Price":         price,
            "Action":        td["action"],
            "Signal_Type":   td["signal_type"],
            "Setup_Desc":    setup_desc,
            "Strategy":      td["strategy"],
            "Supertrend":    st,
            "MST_Signal":    mst,
            "LST_Signal":    lst,
            "RS30_Signal":   rs30,
            "SL_Price":      round(active_sl_price,2) if active_sl_price==active_sl_price else np.nan,
            "SL%":           round(active_sl,2) if active_sl==active_sl else np.nan,
            "SL_Grade":      sl_grade(active_sl),
            "TP1%":          td.get("tp1_pct",np.nan),
            "TP2%":          td.get("tp2_pct",np.nan),
            "TP1_Price":     tp1_price,
            "TP2_Price":     tp2_price,
            "RR_T1":         td.get("rr_t1",np.nan),
            "RR_T2":         td.get("rr_t2",np.nan),
            "SL_Bonus":      sb,
            "RS_Score":      round(rs_sc,1),
            "Fin_Score":     fin_sc,
            "Enhanced_Score":enhanced_score,
            "RSI_14":        r.get("RSI_14",np.nan),
            "Trend":         r.get("Trend",""),
            "SMA_Score":     r.get("SMA_Score",np.nan),
            "Chart_Pattern": r.get("Chart_Pattern",""),
            "Sec_Signal":    sec_signal,
            "Signal":        sig,
            "Enhanced":      enh,
            "Notes":         " | ".join(notes),
        })
    df=pd.DataFrame(rows)
    if df.empty: return df
    ao={"BUY":0,"WAIT":1,"SELL":2}
    df["_a"]=df["Action"].map(ao).fillna(1)
    # Within BUY: RS30>LST>MST>Strong Buy>RS Buy by Enhanced_Score
    st_ord={"RS30 Buy":0,"LST Buy":1,"MST Buy":2,"Strong Buy":3,"RS Buy":4}
    df["_st"]=df["Signal_Type"].map(st_ord).fillna(5)
    df["_es"]=df["Enhanced_Score"]
    df=df.sort_values(["_a","_st","_es"],ascending=[True,True,False]).drop(columns=["_a","_st","_es"]).reset_index(drop=True)
    df["Rank"]=range(1,len(df)+1); return df

# ─────────────────────────────────────────────────────────────────────────────
#  ⓬ DASHBOARD  (methodology + run summary)
# ─────────────────────────────────────────────────────────────────────────────
def build_dashboard(stock_df, sector_str_df, market, run_time):
    """
    Key information sheet explaining:
    - How signals are generated (RS/MST/LST/RS30 logic)
    - Run metadata (market, date/time, universe size)
    - Summary counts
    - Top sector ranking
    - Top buy stocks
    - TV watchlist
    """
    p1, p2 = SIGNAL_PERIODS
    rows = []
    def _r(k, v=""):
        rows.append({"Key": k, "Value": str(v)})

    _r(f"═══════════════════════════════════════════════════════════════")
    _r(f"  MARKET ANALYSIS SYSTEM v5.2  [{market}]")
    _r(f"  Generated     : {run_time}")
    _r(f"  Benchmark     : {'Nifty 50 (^NSEI)' if market == 'INDIA' else 'S&P 500 (SPY)'}")
    _r(f"  Signal Periods: RS({p1}d) + RS({p2}d) vs Index & Sector")
    _r(f"═══════════════════════════════════════════════════════════════")
    _r("")
    _r("── UNIVERSE SUMMARY ──")
    _r("Total Stocks",           len(stock_df))
    _r("⭐ Strong Buy",          (stock_df["Enhanced"]=="Strong Buy").sum())
    _r("✅ Buy Signals",         (stock_df["Signal"]=="Buy").sum())
    _r("🔴 Sell Signals",        (stock_df["Signal"]=="Sell").sum())
    _r("⏳ Neutral",             (stock_df["Signal"]=="Neutral").sum())
    if "MST_Signal" in stock_df.columns:
        _r("🎯 MST Buy",         (stock_df["MST_Signal"]=="Buy").sum())
    if "LST_Signal" in stock_df.columns:
        _r("📈 LST Buy",         (stock_df["LST_Signal"]=="Buy").sum())
    if "RS30_Signal" in stock_df.columns:
        _r("🔥 RS30 Buy",        (stock_df["RS30_Signal"]=="Buy").sum())
    _r("")
    _r("── SIGNAL LOGIC ──")
    _r("BUY Signal",     f"RS_{p1}d_Idx>0 AND RS_{p2}d_Idx>0 (vs Index AND vs Sector)")
    _r("SELL Signal",    f"RS_{p1}d_Idx<0 AND RS_{p2}d_Idx<0 (vs Index AND vs Sector)")
    _r("STRONG BUY",     "Buy + Stock>Sector avg + Stock>Industry avg + Sector>Market + Industry>Market")
    _r("RS_Score",       f"1M×35% + 3M×30% + 6M×20% + 12M×15%  (weighted composite)")
    _r("Total_Score",    "RS_Score×0.6 + Fin_Score×2 + SL_Bonus  (primary ranking)")
    _r("SL_Buy%",        "% from current price DOWN to 20-day swing low (stop for buy trades)")
    _r("SL_Sell%",       "% from 20-day swing high DOWN to current price (stop for sell trades)")
    _r("SL_Grade",       "A≤3%  B≤5%  C≤8%  D≤12%  F>12%  (A=tight=ideal)")
    _r("SL_Bonus",       "+4(≤2%) +3(≤4%) +2(≤6%) +1(≤9%) + RR≥4x(+2) RR≥2.5x(+1)")
    _r("")
    _r("── MST SIGNAL (Medium Swing 20-60 days) ──")
    _r("Source",         "Pine Script: RS All-TF v3 [Vivek Bajaj]")
    _r("Pre-condition",  "Weekly RS(21)>0  AND  Weekly RSI(14)>50")
    _r("Entry",          "Daily RS(55)>0 + Daily RSI(14)>50 + Supertrend=Buy + Close>EMA200 + Breakout")
    _r("Supertrend",     f"Period={MST_ST_PERIOD}, Factor={MST_ST_FACTOR} (ATR-based Wilder smoothing)")
    _r("Default SL",     "7% (use SL_Buy% column for actual swing-based SL)")
    _r("TP1 / TP2",      f"{MST_TP1_MULT}×SL  /  {MST_TP2_MULT}×SL  (risk-reward targets)")
    _r("Exit Triggers",  "Daily RS(55)<0  OR  Supertrend=Sell  OR  RSI>90 (partial)")
    _r("")
    _r("── LST SIGNAL (Long Swing 60-120+ days) ──")
    _r("Source",         "Pine Script: RS All-TF v3 [Vivek Bajaj]")
    _r("Pre-condition",  "Monthly RS(12)>0  AND  Monthly RSI(12)>50")
    _r("Entry",          "Weekly RS(21)>0 + Weekly RSI(12)>50 + Weekly Supertrend=Buy + Breakout")
    _r("Default SL",     "12% (use SL_Buy% column for actual swing-based SL)")
    _r("TP1 / TP2",      f"{LST_TP1_MULT}×SL  /  {LST_TP2_MULT}×SL")
    _r("Exit Triggers",  "Weekly RS(21)<0  OR  Weekly Supertrend=Sell  OR  RSI>90 (partial)")
    _r("")
    _r("── RS30 SIGNAL (Weekly momentum + fundamentals) ──")
    _r("Source",         "Pine Script: FundaTechno RS30 Screener")
    _r("Technical",      "Weekly RS(30)>0  +  Weekly EMA(10)>EMA(30)  +  Within 10% of 52W high")
    _r("Fundamental",    "Sales QoQ≥15%  +  PAT QoQ≥15%  +  MCap≥1000 Cr")
    _r("Entry",          "Breakout above 20-day swing high")
    _r("Target",         "2.5×SL (price-action based)")
    _r("")
    _r("── SECTOR STRENGTH (top 10) ──")
    if not sector_str_df.empty:
        for _, r in sector_str_df.head(10).iterrows():
            _r(f"  #{int(r['Rank'])} {r['Sector']}",
               f"{r['Signal']} | RS_22d:{r.get('RS_22d%',0):+.1f}% | RS_55d:{r.get('RS_55d%',0):+.1f}% | RSI:{r.get('RSI_14','—')}")
    _r("")
    _r("── TOP BUY STOCKS ──")
    buy_df = stock_df[stock_df["Signal"].isin(["Buy","Strong Buy"])].head(15)
    for _, r in buy_df.iterrows():
        _r(r["Symbol"],
           f"Enhanced:{r.get('Enhanced','')} | Sector:{r.get('Sector','')} | "
           f"RS55:{r.get('RS_55d_Idx%',0) if 'RS_55d_Idx%' in r and r.get('RS_55d_Idx%',0)==r.get('RS_55d_Idx%',0) else 'N/A':+.1f}% | "
           f"SL:{r.get('SL_Buy%','—')}% [{r.get('SL_Grade','—')}] | "
           f"MST:{r.get('MST_Signal','—')} | LST:{r.get('LST_Signal','—')} | RS30:{r.get('RS30_Signal','—')}")
    _r("")
    _r("── TV WATCHLIST (copy → TradingView → Watchlist → Import) ──")
    is_nse = (market == "INDIA")
    prefix = "NSE:" if is_nse else ""
    all_buy = stock_df[stock_df["Signal"].isin(["Buy","Strong Buy"])]
    sb_syms = stock_df[stock_df["Enhanced"]=="Strong Buy"]
    mst_buy = stock_df[stock_df["MST_Signal"]=="Buy"] if "MST_Signal" in stock_df.columns else pd.DataFrame()
    lst_buy = stock_df[stock_df["LST_Signal"]=="Buy"] if "LST_Signal" in stock_df.columns else pd.DataFrame()
    rs30_buy= stock_df[stock_df["RS30_Signal"]=="Buy"] if "RS30_Signal" in stock_df.columns else pd.DataFrame()
    if not sb_syms.empty:
        _r(f"TV Strong Buy ({len(sb_syms)})", "".join(f"{prefix}{s}," for s in sb_syms["Symbol"]))
    if not mst_buy.empty:
        _r(f"TV MST Buy ({len(mst_buy)})", "".join(f"{prefix}{s}," for s in mst_buy["Symbol"]))
    if not lst_buy.empty:
        _r(f"TV LST Buy ({len(lst_buy)})", "".join(f"{prefix}{s}," for s in lst_buy["Symbol"]))
    if not rs30_buy.empty:
        _r(f"TV RS30 Buy ({len(rs30_buy)})", "".join(f"{prefix}{s}," for s in rs30_buy["Symbol"]))
    _r(f"TV All Buy ({len(all_buy)})", "".join(f"{prefix}{s}," for s in all_buy["Symbol"]))
    _r(f"TV Top-20",  "".join(f"{prefix}{s}," for s in all_buy.head(20)["Symbol"]))

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
#  OHLCV CACHE  (persistent parquet per symbol, separate subfolder)
#  Avoids re-downloading High/Low/Volume every run.
#  Compatible with PriceCache; uses same CACHE_DIR base.
# ─────────────────────────────────────────────────────────────────────────────

import pathlib as _pl

def _ohlcv_cache_dir():
    """Return Path to OHLCV cache subfolder, creating it if needed."""
    try:
        from price_cache import CACHE_DIR
        base = _pl.Path(CACHE_DIR)
    except Exception:
        base = _pl.Path.home() / "StockPriceCache"
    p = base / "ohlcv"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _sym_to_fname(sym: str) -> str:
    return sym.replace("^","IDX_").replace("/","_").replace("\\","_") + ".parquet"


def save_ohlcv_cache(ohlcv_dict: dict):
    """
    Persist a dict {symbol: DataFrame(Open,High,Low,Close,Volume)} to parquet.
    Each symbol = one file.  Appends / overwrites latest rows.
    """
    if not ohlcv_dict:
        return
    cache_dir = _ohlcv_cache_dir()
    saved = 0
    for sym, df in ohlcv_dict.items():
        if df is None or df.empty:
            continue
        path = cache_dir / _sym_to_fname(sym)
        try:
            if path.exists():
                existing = pd.read_parquet(path)
                # Align columns
                for col in ["Open","High","Low","Close","Volume"]:
                    if col in df.columns and col not in existing.columns:
                        existing[col] = float("nan")
                combined = pd.concat([existing, df])
                combined = combined[~combined.index.duplicated(keep="last")].sort_index()
            else:
                combined = df
            combined.to_parquet(path)
            saved += 1
        except Exception:
            pass
    print(f"  💾 OHLCV cache: saved {saved}/{len(ohlcv_dict)} symbols")


def load_ohlcv_cache(symbols: list, days: int = 300) -> dict:
    """
    Load OHLCV from parquet cache for given symbols.
    Returns {symbol: DataFrame} for symbols found with enough data.
    """
    cache_dir = _ohlcv_cache_dir()
    cutoff    = pd.Timestamp.today() - pd.Timedelta(days=days+5)
    result    = {}
    for sym in symbols:
        path = cache_dir / _sym_to_fname(sym)
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path)
            df = df[df.index >= cutoff]
            if len(df) >= 60 and "Close" in df.columns and "High" in df.columns:
                result[sym] = df
        except Exception:
            pass
    return result


def fetch_ohlcv_with_cache(symbols: list, days: int = 300,
                            force_refresh: bool = False) -> dict:
    """
    Smart OHLCV fetch:
      1. Load from cache what's already there and fresh enough
      2. Download only missing / stale symbols
      3. Save new data back to cache
      4. Return combined dict

    'Stale' = last bar is more than 1 trading day old.
    """
    today     = pd.Timestamp.today().normalize()
    stale_cutoff = today - pd.Timedelta(days=1)

    cached    = {} if force_refresh else load_ohlcv_cache(symbols, days)
    fresh     = {s: df for s, df in cached.items()
                 if not df.empty and pd.Timestamp(df.index[-1]) >= stale_cutoff}
    to_fetch  = [s for s in symbols if s not in fresh]

    if to_fetch:
        print(f"  OHLCV cache: {len(fresh)} fresh | fetching {len(to_fetch)} new/stale …")
        new_data = fetch_ohlcv_batch(to_fetch, days=days)
        if new_data:
            save_ohlcv_cache(new_data)
            fresh.update(new_data)
    else:
        print(f"  OHLCV cache: {len(fresh)}/{len(symbols)} loaded (all fresh)")

    return fresh


# ─────────────────────────────────────────────────────────────────────────────
#  FINANCIAL CACHE  (JSON, keyed by symbol, expires after FIN_CACHE_DAYS)
#  Stores: SalesQoQ, SalesYoY, PATQoQ, PATYoY, Margin, ROE, DE, EPS, PE, MCap
# ─────────────────────────────────────────────────────────────────────────────

FIN_CACHE_DAYS = 7   # re-fetch fundamentals after this many days

def _fin_cache_path():
    try:
        from price_cache import CACHE_DIR
        base = _pl.Path(CACHE_DIR)
    except Exception:
        base = _pl.Path.home() / "StockPriceCache"
    base.mkdir(parents=True, exist_ok=True)
    return base / "_fin_v5.json"


def load_fin_cache() -> dict:
    """Load financial cache from JSON. Returns {symbol: {data..., '_ts': timestamp}}."""
    path = _fin_cache_path()
    if not path.exists():
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_fin_cache(cache: dict):
    """Write full financial cache dict to JSON."""
    try:
        with open(_fin_cache_path(), "w") as f:
            json.dump(cache, f, indent=2, default=str)
    except Exception as e:
        print(f"  ⚠ Financial cache write failed: {e}")


def _fin_is_fresh(entry: dict) -> bool:
    """Return True if the cached financial entry is not expired."""
    try:
        ts  = pd.Timestamp(entry.get("_ts", "2000-01-01"))
        age = (pd.Timestamp.today() - ts).days
        return age <= FIN_CACHE_DAYS
    except Exception:
        return False



# Alias — fetch_financials_with_cache delegates to the existing cached batch fetcher
def fetch_financials_with_cache(symbols, market="INDIA", force_refresh=False):
    """Wrapper around get_financials_batch (which already caches to JSON)."""
    return get_financials_batch(symbols, force=force_refresh)
