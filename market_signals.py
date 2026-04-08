"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  MARKET SIGNALS ENGINE  v5.1 — market_signals.py                          ║
║  SELF-CONTAINED (no imports from market_engine — avoids circular import)   ║
║                                                                            ║
║  Implements (from Pine Script RS All-TF v3 + RS30 Screener):              ║
║    • Supertrend  ATR=10, Factor=3.0  — Pine Script–accurate algorithm      ║
║    • MST Signal  Entry:Daily  HTF:Weekly                                   ║
║    • LST Signal  Entry:Weekly HTF:Monthly                                  ║
║    • RS30 Signal Weekly RS(30) + EMA(10)>EMA(30) + Fundamentals           ║
║    • Swing High/Low SL  (20-day price action)                              ║
║    • Dashboard DataFrame builder                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝

SIGNAL TRUTH TABLE:
  "Buy"     = All conditions met + breakout above swing high
  "Watch"   = HTF pre-conditions OK + entry conditions met, no breakout yet
  "Neutral" = Pre-conditions not fully met
"""

import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple, Dict

# ── Pine Script parameters (from RS All-TF v3) ────────────────────────────────
MST_RS_ENTRY  = 55    # Daily RS period
MST_RS_HTF    = 21    # Weekly RS period
MST_RSI_LEN   = 14    # RSI length (both TFs)
MST_RSI_THRESH= 50
MST_ST_PERIOD = 10    # Supertrend ATR period
MST_ST_FACTOR = 3.0   # Supertrend factor
MST_SWING_LB  = 20    # Swing high lookback
MST_TP1_MULT  = 2.5   # TP1 = 2.5× SL
MST_TP2_MULT  = 3.5   # TP2 = 3.5× SL
MST_MAX_SL    = 7.0   # Max acceptable SL %

LST_RS_ENTRY  = 21    # Weekly RS period
LST_RS_HTF    = 12    # Monthly RS period
LST_RSI_LEN   = 12    # RSI length (both TFs for LST)
LST_RSI_THRESH= 50
LST_ST_PERIOD = 10
LST_ST_FACTOR = 3.0
LST_SWING_LB  = 20
LST_TP1_MULT  = 3.0   # TP1 = 3× SL
LST_TP2_MULT  = 4.0   # TP2 = 4× SL
LST_MAX_SL    = 12.0

RS30_RS_PERIOD    = 30    # Weekly RS period (Pine: rs_period = 30 weeks)
RS30_EMA_S        = 10    # Weekly EMA short
RS30_EMA_L        = 30    # Weekly EMA long
RS30_NEAR_HIGH_PCT= 10.0  # Max % from 52W high
RS30_SWING_LB     = 20    # 20-day swing high breakout
RS30_MIN_SALES_QOQ= 15.0
RS30_MIN_PAT_QOQ  = 15.0


# ─────────────────────────────────────────────────────────────────────────────
#  INLINED UTILITIES  (duplicated from market_engine to avoid circular import)
# ─────────────────────────────────────────────────────────────────────────────

def _norm(s: pd.Series) -> pd.Series:
    """Normalize to timezone-naive, deduplicated, sorted Series."""
    try:
        if isinstance(s, pd.DataFrame): s = s.squeeze()
        idx = s.index
        if hasattr(idx, 'tz') and idx.tz is not None:
            try: idx = idx.tz_localize(None)
            except: idx = idx.tz_convert(None)
        idx = idx.normalize()
        s2  = pd.Series(s.values, index=idx, dtype=float)
        return s2[~s2.index.duplicated(keep='last')].sort_index()
    except: return s


def _rs(stock: pd.Series, bench: pd.Series, period: int) -> float:
    """Relative Strength: (stock_ret / bench_ret) - 1 over N periods."""
    try:
        s = _norm(stock.dropna()); b = _norm(bench.dropna())
        common = s.index.intersection(b.index)
        if len(common) < period + 1: return np.nan
        s, b = s.loc[common], b.loc[common]
        sc, sp = float(s.iloc[-1]), float(s.iloc[-(period+1)])
        bc, bp = float(b.iloc[-1]), float(b.iloc[-(period+1)])
        if sp == 0 or bp == 0 or bc == 0: return np.nan
        return (sc/sp) / (bc/bp) - 1
    except: return np.nan


def _rsi(series: pd.Series, period: int = 14) -> float:
    """RSI — works on any frequency series."""
    try:
        d = series.diff().dropna()
        g = d.clip(lower=0).rolling(period, min_periods=period).mean().iloc[-1]
        l = (-d.clip(upper=0)).rolling(period, min_periods=period).mean().iloc[-1]
        if l == 0 or np.isnan(l): return 100.0 if g > 0 else 50.0
        return round(100 - (100 / (1 + g / l)), 1)
    except: return np.nan


def _pct_n(series: pd.Series, n: int) -> float:
    try:
        if len(series) < n + 1: return np.nan
        cur = float(series.iloc[-1]); past = float(series.iloc[-(n+1)])
        return (cur/past - 1)*100 if past != 0 else np.nan
    except: return np.nan


# ─────────────────────────────────────────────────────────────────────────────
#  RESAMPLING
# ─────────────────────────────────────────────────────────────────────────────

def to_weekly(s: pd.Series) -> pd.Series:
    """Resample daily close to weekly Friday close."""
    try:
        s2 = _norm(s.dropna())
        return s2.resample('W-FRI').last().dropna()
    except: return pd.Series(dtype=float)


def to_monthly(s: pd.Series) -> pd.Series:
    """Resample daily close to month-end close."""
    try:
        s2 = _norm(s.dropna())
        try: return s2.resample('ME').last().dropna()
        except: return s2.resample('M').last().dropna()
    except: return pd.Series(dtype=float)


# ─────────────────────────────────────────────────────────────────────────────
#  ATR  (Wilder EWM — matches Pine Script ta.atr used in ta.supertrend)
# ─────────────────────────────────────────────────────────────────────────────

def calc_atr(close_s: pd.Series, high_s: pd.Series, low_s: pd.Series,
             period: int = 10) -> pd.Series:
    prev_c = close_s.shift(1)
    tr = pd.concat([
        high_s - low_s,
        (high_s - prev_c).abs(),
        (low_s  - prev_c).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False, min_periods=max(2, period//2)).mean()


# ─────────────────────────────────────────────────────────────────────────────
#  SUPERTREND  (iterative — matches Pine Script ta.supertrend(factor, atrLen))
#
#  Convention:
#    direction = +1 → Bullish (price above lower band) → BUY
#    direction = -1 → Bearish (price below upper band) → SELL
#
#  In Pine Script:  stBuy = direction < 0
#  (Pine uses -1 for buy, +1 for sell — our convention is opposite but consistent)
# ─────────────────────────────────────────────────────────────────────────────

def calc_supertrend(close_s: pd.Series, high_s: pd.Series, low_s: pd.Series,
                    period: int = 10, multiplier: float = 3.0
                    ) -> Tuple[pd.Series, pd.Series]:
    """
    Returns (supertrend_line, direction_series).
    direction: +1 = Buy, -1 = Sell.
    """
    n = len(close_s)
    if n < period + 2:
        return (pd.Series(np.nan, index=close_s.index),
                pd.Series(0,     index=close_s.index))

    atr  = calc_atr(close_s, high_s, low_s, period)
    hl2  = (high_s + low_s) / 2.0
    ub   = (hl2 + multiplier * atr).values
    lb   = (hl2 - multiplier * atr).values
    c    = close_s.values
    atr_v= atr.values

    f_ub = ub.copy(); f_lb = lb.copy()
    direction = np.ones(n, dtype=int)   # start bullish
    st        = np.zeros(n)

    for i in range(1, n):
        if np.isnan(atr_v[i]):
            direction[i] = direction[i-1]; st[i] = st[i-1]; continue

        # Upper: tighten only (price must stay below to remain bearish)
        f_ub[i] = ub[i] if (ub[i] < f_ub[i-1] or c[i-1] > f_ub[i-1]) else f_ub[i-1]
        # Lower: raise only (price must stay above to remain bullish)
        f_lb[i] = lb[i] if (lb[i] > f_lb[i-1] or c[i-1] < f_lb[i-1]) else f_lb[i-1]

        # Direction flip
        if   direction[i-1] == -1 and c[i] > f_ub[i]: direction[i] =  1  # Bearish→Bullish
        elif direction[i-1] ==  1 and c[i] < f_lb[i]: direction[i] = -1  # Bullish→Bearish
        else:                                           direction[i] = direction[i-1]

        st[i] = f_lb[i] if direction[i] == 1 else f_ub[i]

    return (pd.Series(st, index=close_s.index),
            pd.Series(direction, index=close_s.index))


def calc_supertrend_from_df(ohlcv_df, period: int = 10,
                             multiplier: float = 3.0,
                             freq: str = 'D') -> Tuple[pd.Series, pd.Series]:
    """
    Convenience wrapper for an OHLCV DataFrame.
    ohlcv_df: DataFrame with High/Low/Close columns, or None (returns empty).
    freq: 'D' = daily (default), 'W' = resample to weekly first.
    """
    _empty = pd.Series(dtype=float), pd.Series(dtype=int)
    if ohlcv_df is None or not isinstance(ohlcv_df, pd.DataFrame) or ohlcv_df.empty:
        return _empty
    try:
        h = ohlcv_df["High"]; l = ohlcv_df["Low"]; c = ohlcv_df["Close"]
        if freq == 'W':
            c = c.resample('W-FRI').last().dropna()
            h = h.resample('W-FRI').max().dropna()
            l = l.resample('W-FRI').min().dropna()
            idx = c.index.intersection(h.index).intersection(l.index)
            c, h, l = c.loc[idx], h.loc[idx], l.loc[idx]
        elif freq == 'M':
            try:
                c = c.resample('ME').last().dropna()
                h = h.resample('ME').max().dropna()
                l = l.resample('ME').min().dropna()
            except Exception:
                c = c.resample('M').last().dropna()
                h = h.resample('M').max().dropna()
                l = l.resample('M').min().dropna()
            idx = c.index.intersection(h.index).intersection(l.index)
            c, h, l = c.loc[idx], h.loc[idx], l.loc[idx]
        return calc_supertrend(c, h, l, period, multiplier)
    except Exception:
        return pd.Series(dtype=float), pd.Series(dtype=float)


def _st_dir_latest(close_s, high_s, low_s, period=10, mult=3.0) -> int:
    """Return latest Supertrend direction: +1 Buy, -1 Sell, 0 unknown."""
    if high_s is None or low_s is None: return 0
    try:
        c = _norm(close_s.dropna()); h = _norm(high_s.dropna()); l = _norm(low_s.dropna())
        cm = c.index.intersection(h.index).intersection(l.index)
        if len(cm) < period + 2: return 0
        _, ds = calc_supertrend(c.loc[cm], h.loc[cm], l.loc[cm], period, mult)
        return int(ds.iloc[-1])
    except: return 0


# ─────────────────────────────────────────────────────────────────────────────
#  SWING HIGH / LOW → STOP LOSS
# ─────────────────────────────────────────────────────────────────────────────

def calc_swing_sl(close_s: pd.Series,
                  high_s:  Optional[pd.Series] = None,
                  low_s:   Optional[pd.Series] = None,
                  lookback: int = 20) -> dict:
    """
    Compute swing-based stop loss from the last `lookback` trading days.

    Swing Low  = min of daily Lows  in last 20d  →  Stop Loss for BUY trades
    Swing High = max of daily Highs in last 20d  →  Stop Loss for SELL trades
    Falls back to Close if High/Low unavailable.

    Returns dict with:
      swing_low, swing_high          — price levels
      sl_buy_pct, sl_sell_pct       — % risk from current price
      sl_buy_grade, sl_sell_grade   — A/B/C/D/F
      swing_high_break              — bool: price > 20d swing high (breakout)
    """
    out = dict(swing_low=np.nan, swing_high=np.nan,
               sl_buy_pct=np.nan, sl_sell_pct=np.nan,
               sl_buy_grade="F", sl_sell_grade="F",
               swing_high_break=False)
    try:
        c = _norm(close_s.dropna())
        if len(c) < lookback + 1: return out
        cur = float(c.iloc[-1])
        if cur <= 0: return out

        # Swing Low (for buy SL)
        if low_s is not None and len(low_s) >= lookback:
            ls = _norm(low_s.dropna())
            sw_lo = float(ls.iloc[-lookback:].min())
        else:
            sw_lo = float(c.iloc[-lookback:].min())

        # Swing High (for sell SL + breakout check)
        if high_s is not None and len(high_s) >= lookback + 1:
            hs = _norm(high_s.dropna())
            sw_hi      = float(hs.iloc[-lookback:].max())
            sw_hi_prev = float(hs.iloc[-lookback-1:-1].max())  # excl. today
        else:
            sw_hi      = float(c.iloc[-lookback:].max())
            sw_hi_prev = float(c.iloc[-lookback-1:-1].max())

        sl_buy  = (cur - sw_lo) / cur * 100 if sw_lo > 0 else np.nan
        sl_sell = (sw_hi - cur) / cur * 100 if sw_hi > cur else 0.0

        out.update(dict(
            swing_low        = round(sw_lo, 2),
            swing_high       = round(sw_hi, 2),
            sl_buy_pct       = round(sl_buy,  2) if sl_buy  == sl_buy  else np.nan,
            sl_sell_pct      = round(sl_sell, 2) if sl_sell == sl_sell else np.nan,
            sl_buy_grade     = sl_grade(sl_buy),
            sl_sell_grade    = sl_grade(sl_sell),
            swing_high_break = (cur > sw_hi_prev),
        ))
    except: pass
    return out


def sl_grade(pct) -> str:
    """SL grade based on % distance (lower = tighter = better)."""
    if not isinstance(pct, (int, float)) or np.isnan(pct) or pct < 0: return "F"
    if   pct <= 3:  return "A"
    elif pct <= 5:  return "B"
    elif pct <= 8:  return "C"
    elif pct <= 12: return "D"
    else:           return "F"


def sl_bonus(grade: str) -> int:
    """Score bonus from SL grade: A=4 B=3 C=2 D=1 F=0."""
    return {"A":4,"B":3,"C":2,"D":1,"F":0}.get(grade, 0)


# ─────────────────────────────────────────────────────────────────────────────
#  MST SIGNAL  (Medium Swing — Entry:Daily, HTF:Weekly)
#
#  Pine Script RS All-TF v3, Section 3 GMST:
#    HTF   checks on Weekly timeframe:  RS(21)>0,  RSI(14)>50
#    Entry checks on Daily  timeframe:  RS(55)>0,  RSI(14)>50,
#                                       Supertrend(10,3.0)=Buy, Close>EMA(200),
#                                       Close > highest(high[1], 20)  ← breakout
# ─────────────────────────────────────────────────────────────────────────────

def calc_mst_signal(
    daily_close,   # pd.Series — daily close prices
    bench_close,   # pd.Series — benchmark (Nifty / SPY)
    st_daily,      # str — "Buy" / "Sell" / "N/A"  (daily Supertrend direction)
    swing_data,    # dict — from calc_swing_sl (has swing_high_break)
    rs_55d,        # float — Daily RS(55) vs index, decimal (0.05 = 5%)
    rsi_14d,       # float — Daily RSI(14)
    ema200_d,      # float — Daily EMA(200) price level
    w_rs21,        # float — Weekly RS(21) decimal
    w_rsi,         # float — Weekly RSI(14)
):
    """
    MST Signal: Medium Swing Trading (20-60 days)
    HTF  pre-cond (Weekly):  RS(21) > 0  AND  RSI(14) > 50
    Entry cond   (Daily):    RS(55) > 0  AND  RSI(14) > 50
                             Supertrend = Buy  AND  Close > EMA(200)
    Breakout:                Close > 20-day Swing High
    Returns: "Buy" / "Watch" / "Neutral"
    """
    try:
        # ── HTF: Weekly pre-conditions ────────────────────────────────────────
        htf_ok = (
            not (isinstance(w_rs21, float) and np.isnan(w_rs21)) and w_rs21 > 0 and
            not (isinstance(w_rsi,  float) and np.isnan(w_rsi))  and w_rsi  > MST_RSI_THRESH
        )
        if not htf_ok:
            return "Neutral"

        # ── Entry: Daily conditions ───────────────────────────────────────────
        rs_ok  = not (isinstance(rs_55d,  float) and np.isnan(rs_55d))  and rs_55d  > 0
        rsi_ok = not (isinstance(rsi_14d, float) and np.isnan(rsi_14d)) and rsi_14d > MST_RSI_THRESH
        st_ok  = (st_daily in ("Buy", "N/A"))   # N/A = no OHLCV data, don't block signal
        cur    = float(daily_close.iloc[-1])
        ema_ok = not (isinstance(ema200_d, float) and np.isnan(ema200_d)) and cur > ema200_d

        entry_ok = rs_ok and rsi_ok and st_ok and ema_ok

        # ── Breakout: close > 20-day swing high (excl. today) ────────────────
        breakout = bool(swing_data.get("swing_high_break", False))

        if entry_ok and breakout:
            return "Buy"
        elif entry_ok:
            return "Watch"
        else:
            return "Neutral"
    except Exception:
        return "Neutral"


# ─────────────────────────────────────────────────────────────────────────────
#  LST SIGNAL  (Long Swing — Entry:Weekly, HTF:Monthly)
#
#  Pine Script RS All-TF v3, Section 4 GLST:
#    HTF   checks on Monthly timeframe: RS(12)>0,  RSI(12)>50
#    Entry checks on Weekly  timeframe: RS(21)>0,  RSI(12)>50,
#                                       Supertrend(10,3.0)=Buy, Close>EMA(200),
#                                       Close > highest(high[1], 20)  ← weekly breakout
#    Fundamental (user spec): Revenue+, PAT+
# ─────────────────────────────────────────────────────────────────────────────

def calc_lst_signal(
    daily_close,    # pd.Series — daily close prices
    bench_close,    # pd.Series — benchmark
    st_daily_w,     # str — "Buy" / "Sell" / "N/A" (weekly Supertrend direction)
    swing_data,     # dict — from calc_swing_sl
    m_rs12=np.nan,  # float — Monthly RS(12) decimal (pre-computed by engine)
    m_rsi=np.nan,   # float — Monthly RSI(12) (pre-computed by engine)
    fin=None,       # dict — financial data
) -> str:
    """
    LST Signal: Long Swing / Momentum (60-120+ days)
    HTF  pre-cond (Monthly): RS(12) > 0  AND  RSI(12) > 50  AND Revenue+ AND PAT+
    Entry cond   (Weekly):   RS(21) > 0  AND  RSI(12) > 50
                             Supertrend = Buy  AND  Close > EMA(200)
    Breakout:                Weekly Close > 20-week Swing High
    Returns: "Buy" / "Watch" / "Neutral"
    """
    try:
        # ── HTF: Monthly pre-conditions ───────────────────────────────────────
        htf_ok = (
            not (isinstance(m_rs12, float) and np.isnan(m_rs12)) and m_rs12 > 0 and
            not (isinstance(m_rsi,  float) and np.isnan(m_rsi))  and m_rsi  > LST_RSI_THRESH
        )
        # Fundamental pre-conditions: Revenue+ AND PAT+
        if fin and htf_ok:
            sy = fin.get("SalesYoY", np.nan); py = fin.get("PATYoY", np.nan)
            if not np.isnan(sy) and sy <= 0: htf_ok = False
            if not np.isnan(py) and py <= 0: htf_ok = False
        if not htf_ok:
            return "Neutral"

        # ── Entry: Weekly conditions ──────────────────────────────────────────
        c  = _norm(daily_close.dropna()); b = _norm(bench_close.dropna())
        cw = to_weekly(c);               bw = to_weekly(b)
        rs_w21  = _rs(cw, bw, LST_RS_ENTRY)       # Weekly RS(21)
        rsi_w12 = _rsi(cw, LST_RSI_LEN)           # Weekly RSI(12)

        ema200_w = float(cw.ewm(span=200, adjust=False, min_periods=20).mean().iloc[-1])
        ema_ok   = float(cw.iloc[-1]) > ema200_w
        st_ok    = (st_daily_w in ("Buy", "N/A"))

        entry_ok = (
            not (isinstance(rs_w21,  float) and np.isnan(rs_w21))  and rs_w21  > 0 and
            not (isinstance(rsi_w12, float) and np.isnan(rsi_w12)) and rsi_w12 > LST_RSI_THRESH and
            st_ok and ema_ok
        )

        # ── Breakout: weekly close > 20-week swing high (excl. latest week) ──
        lb   = LST_SWING_LB
        sw_h = float(cw.iloc[-lb-1:-1].max()) if len(cw) >= lb+1 else float(cw.iloc[:-1].max())
        breakout = float(cw.iloc[-1]) > sw_h

        if entry_ok and breakout:
            return "Buy"
        elif entry_ok:
            return "Watch"
        else:
            return "Neutral"
    except Exception:
        return "Neutral"


# ─────────────────────────────────────────────────────────────────────────────
#  RS30 SIGNAL  (Weekly momentum — from RS30 Screener Pine Script)
#
#  Pine Script RS30 Screener:
#    rs_val  = (rs_ratio / rs_ratio[rs_period]) - 1  × 100   (30-week RS in %)
#    ema10   = ta.ema(close, 10) on weekly
#    ema30   = ta.ema(close, 30) on weekly
#    dist_52w= (ta.highest(high,52) - close) / ta.highest(high,52) × 100
#    is_breakout = close > ta.highest(high[1], swing_period)  (20-day)
#
#  Filter:  rs_val>0 AND ema10>ema30 AND dist_52w≤10
#           AND sales_growth≥15% AND pat_growth≥15% AND mcap≥1000Cr
# ─────────────────────────────────────────────────────────────────────────────

def calc_rs30_signal(
    daily_close,     # pd.Series — daily close prices
    bench_close,     # pd.Series — benchmark
    swing_data,      # dict — from calc_swing_sl (has swing_high_break)
    fin,             # dict — financial data (SalesQoQ, PATQoQ, MktCap)
    w_rs30,          # float — Weekly RS(30) decimal (pre-computed by engine)
    w_ema10,         # float — Weekly EMA(10) price level
    w_ema30,         # float — Weekly EMA(30) price level
    market="INDIA",  # str — "INDIA" or "US" for MCap threshold
):
    """
    RS30 Signal: Weekly Momentum Strategy (FundaTechno)
    Technical:    Weekly RS(30) > 0  AND  Weekly EMA(10) > EMA(30)
                  Price within 10% of 52W High
    Fundamental:  Sales QoQ >= 15%  AND  PAT QoQ >= 15%  AND  MCap >= 1000 Cr
    Breakout:     Close > 20-day Swing High
    Returns: "Buy" / "Watch" / "Neutral"
    """
    try:
        # ── Weekly RS(30) > 0 ────────────────────────────────────────────────
        if isinstance(w_rs30, float) and np.isnan(w_rs30):
            return "Neutral"
        if w_rs30 <= 0:
            return "Neutral"

        # ── Weekly EMA(10) > EMA(30) ─────────────────────────────────────────
        ema_valid = (
            not (isinstance(w_ema10, float) and np.isnan(w_ema10)) and
            not (isinstance(w_ema30, float) and np.isnan(w_ema30))
        )
        if not ema_valid or w_ema10 <= w_ema30:
            return "Neutral"

        # ── Price within 10% of 52-week high ─────────────────────────────────
        c   = _norm(daily_close.dropna())
        cur = float(c.iloc[-1])
        n52 = min(252, len(c))
        h52 = float(c.iloc[-n52:].max())
        dist      = (h52 - cur) / h52 * 100 if h52 > 0 else 100.0
        near_high = dist <= RS30_NEAR_HIGH_PCT

        # ── Fundamental filters ───────────────────────────────────────────────
        funda_ok = True
        if fin:
            sq = fin.get("SalesQoQ", np.nan)
            pq = fin.get("PATQoQ",   np.nan)
            mc = fin.get("MktCap",   np.nan)   # in billions
            sales_ok = np.isnan(sq) or sq >= RS30_MIN_SALES_QOQ
            pat_ok   = np.isnan(pq) or pq >= RS30_MIN_PAT_QOQ
            # India: MCap >= 1000 Cr = ~10B INR; US: >= $1B
            mc_thresh = 10.0 if market == "INDIA" else 1.0
            mcap_ok  = np.isnan(mc) or mc >= mc_thresh
            funda_ok = sales_ok and pat_ok and mcap_ok

        # ── Breakout: close > 20-day swing high (excl. today) ────────────────
        breakout = bool(swing_data.get("swing_high_break", False))

        if near_high and funda_ok:
            return "Buy" if breakout else "Watch"
        elif near_high:
            return "Watch"   # technical OK but fundamentals not met yet
        else:
            return "Neutral"
    except Exception:
        return "Neutral"


# ─────────────────────────────────────────────────────────────────────────────
#  UTILITY FUNCTIONS  (helpers for calc_ and grade)
# ─────────────────────────────────────────────────────────────────────────────

def calc_rs_tf(stock_d, bench_d, period, freq="D"):
    """RS on a given frequency (D=daily, W=weekly, M=monthly)."""
    try:
        c = _norm(stock_d.dropna()); b = _norm(bench_d.dropna())
        if freq == "W": c = to_weekly(c); b = to_weekly(b)
        elif freq == "M": c = to_monthly(c); b = to_monthly(b)
        return _rs(c, b, period)
    except: return np.nan


def calc_rsi_tf(series_d, period=14, freq="D"):
    """RSI on a given frequency."""
    try:
        s = _norm(series_d.dropna())
        if freq == "W": s = to_weekly(s)
        elif freq == "M": s = to_monthly(s)
        return _rsi(s, period)
    except: return np.nan


def calc_ema_tf(series_d, period, freq="D"):
    """Latest EMA on a given frequency."""
    try:
        s = _norm(series_d.dropna())
        if freq == "W": s = to_weekly(s)
        elif freq == "M": s = to_monthly(s)
        return float(s.ewm(span=period, adjust=False, min_periods=max(2,period//3)).mean().iloc[-1])
    except: return np.nan


def calc_pct_from_52w_high(series_d) -> float:
    try:
        s = _norm(series_d.dropna())
        n = min(252, len(s))
        return (float(s.iloc[-1]) / float(s.iloc[-n:].max()) - 1) * 100
    except: return np.nan


# [old classify_trade removed — see full version below]


# ─────────────────────────────────────────────────────────────────────────────
#  DASHBOARD BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_dashboard_df(stock_df: pd.DataFrame,
                        sector_str_df: pd.DataFrame,
                        market: str,
                        run_time: str) -> pd.DataFrame:
    """
    Build 📋 Dashboard as a two-column DataFrame with:
    system info, signal counts, methodology, sector ranking, TV watchlist.
    """
    def _cnt(col, val):
        if col not in stock_df.columns or stock_df.empty: return 0
        return int((stock_df[col] == val).sum())

    p1, p2 = 22, 55  # signal periods

    sb  = _cnt("Enhanced",    "Strong Buy")
    buy = _cnt("Signal",      "Buy")
    sel = _cnt("Signal",      "Sell")
    neu = (len(stock_df) - buy - sel) if not stock_df.empty else 0

    mst_b  = _cnt("MST_Signal",  "Buy");  mst_w  = _cnt("MST_Signal",  "Watch")
    lst_b  = _cnt("LST_Signal",  "Buy");  lst_w  = _cnt("LST_Signal",  "Watch")
    r30_b  = _cnt("RS30_Signal", "Buy");  r30_w  = _cnt("RS30_Signal", "Watch")

    sl_a = sl_b_g = sl_c = 0
    if "SL_Grade" in stock_df.columns and not stock_df.empty:
        bd = stock_df[stock_df["Signal"].isin(["Buy","Strong Buy"])]
        sl_a = int((bd["SL_Grade"]=="A").sum())
        sl_b_g=int((bd["SL_Grade"]=="B").sum())
        sl_c = int((bd["SL_Grade"]=="C").sum())

    is_nse = (market == "INDIA"); pfx = "NSE:" if is_nse else ""

    def _tv(mask_col, mask_val):
        if mask_col not in stock_df.columns or stock_df.empty: return ""
        syms = stock_df[stock_df[mask_col]==mask_val]["Symbol"].tolist()
        return "".join(f"{pfx}{s}," for s in syms)

    tv_all  = _tv("Signal",      "Buy")
    tv_sb   = _tv("Enhanced",    "Strong Buy")
    tv_mst  = _tv("MST_Signal",  "Buy")
    tv_lst  = _tv("LST_Signal",  "Buy")
    tv_rs30 = _tv("RS30_Signal", "Buy")

    rows = [
        # ── Header ─────────────────────────────────────────────────────────
        [f"FundaTechno Market Analysis  v5.1  [{market}]",  ""],
        ["Generated",           run_time],
        ["Data Source",         "Yahoo Finance (yfinance)"],
        ["Universe",            f"{len(stock_df)} stocks  |  Benchmark: {'NSE:NIFTY' if is_nse else 'SPY'}"],
        ["",                    ""],
        # ── Signal Summary ─────────────────────────────────────────────────
        ["── RS SIGNAL SUMMARY ──",   ""],
        ["⭐ Strong Buy (all 5 peer filters)", sb],
        ["✅ Buy",               buy],
        ["🔴 Sell",              sel],
        ["⬜ Neutral",           neu],
        ["",                    ""],
        ["── MULTI-TF SIGNAL SUMMARY ──", ""],
        ["MST Buy",             mst_b],
        ["MST Watch",           mst_w],
        ["LST Buy",             lst_b],
        ["LST Watch",           lst_w],
        ["RS30 Buy",            r30_b],
        ["RS30 Watch",          r30_w],
        ["",                    ""],
        ["── SL QUALITY (Buy stocks only) ──", ""],
        ["Grade A  ≤3%  (Ideal)",   sl_a],
        ["Grade B  3-5% (Good)",    sl_b_g],
        ["Grade C  5-8% (Acceptable)", sl_c],
        ["",                    ""],
        # ── RS Methodology ─────────────────────────────────────────────────
        ["── RS SIGNAL METHODOLOGY ──",   ""],
        ["RS Formula",          f"(Stock/Stock_{p1}d) / (Benchmark/Benchmark_{p1}d) - 1"],
        ["Buy Signal",          f"RS_{p1}d_Idx>0 AND RS_{p2}d_Idx>0 AND RS_{p1}d_Sec>0 AND RS_{p2}d_Sec>0"],
        ["Sell Signal",         f"All four RS values NEGATIVE"],
        ["Neutral",             "Mixed RS or insufficient data"],
        ["Strong Buy (5 filters)", "Buy + Stock_Ret55≥Sec_Avg + Stock_Ret55≥Ind_Avg + Sec_RS>0 + Ind_RS>0"],
        ["",                    ""],
        # ── MST ────────────────────────────────────────────────────────────
        ["── MST: MEDIUM SWING TRADING (20-60 days) ──", ""],
        ["Duration / Target / SL", "20-60 days  |  20-25% target  |  ≤7% SL  |  2.5x R:R"],
        ["HTF (Weekly) pre-cond",  "Weekly RS(21)>0  AND  Weekly RSI(14)>50"],
        ["Entry (Daily) cond",     "Daily RS(55)>0  AND  Daily RSI(14)>50  AND  Supertrend=Buy  AND  Close>EMA(200)"],
        ["Breakout trigger",       "Close > Highest High of previous 20 days"],
        ["Stop Loss",              "Swing Low of last 20 trading days (price action)"],
        ["Exit signals",           "Daily RS(21)<0  OR  Supertrend=Sell  OR  RSI>90 (partial exit)"],
        ["",                       ""],
        # ── LST ────────────────────────────────────────────────────────────
        ["── LST: LONG SWING / MOMENTUM (60-120+ days) ──", ""],
        ["Duration / Target / SL", "60-120+ days  |  30%+ target  |  ≤12% SL  |  3x R:R"],
        ["HTF (Monthly) pre-cond", "Monthly RS(12)>0  AND  Monthly RSI(12)>50  AND  Revenue+  AND  PAT+"],
        ["Entry (Weekly) cond",    "Weekly RS(21)>0  AND  Weekly RSI(12)>50  AND  Supertrend=Buy  AND  Close>EMA(200)"],
        ["Breakout trigger",       "Weekly Close > 20-week Swing High"],
        ["Stop Loss",              "Swing Low of last 20 days (wider, structure-based)"],
        ["Exit signals",           "Daily RS(55)<0  OR  Supertrend=Sell  OR  RSI>90 (partial exit)"],
        ["",                       ""],
        # ── RS30 ───────────────────────────────────────────────────────────
        ["── RS30: WEEKLY MOMENTUM (FundaTechno Strategy) ──", ""],
        ["Technical filters",      "Weekly RS(30)>0  AND  Weekly EMA(10)>EMA(30)  AND  Price within 10% of 52W High"],
        ["Fundamental filters",    "Sales QoQ>15%  AND  PAT QoQ>15%  AND  MCap>1000Cr (India)"],
        ["Breakout trigger",       "Close > 20-day Swing High"],
        ["SL (short-term holders)","Price below Weekly EMA(10)"],
        ["SL (long-term holders)", "Price below Weekly EMA(30)"],
        ["Avoid",                  "High pledge, high debt, increasing retail shareholding"],
        ["",                       ""],
        # ── Supertrend ─────────────────────────────────────────────────────
        ["── SUPERTREND INDICATOR ──", ""],
        ["Algorithm",              "ATR period=10, Multiplier=3.0  (matches Pine Script ta.supertrend)"],
        ["Buy direction",          "Price crosses above lower band (direction=+1)"],
        ["Sell direction",         "Price crosses below upper band (direction=-1)"],
        ["Used in",                "MST entry filter + LST entry filter"],
        ["",                       ""],
        # ── SL / Target guide ──────────────────────────────────────────────
        ["── SL GRADE & SCORE BONUS ──", ""],
        ["Grade A  ≤3%",           "+4 pts to Total_Score  (Ideal — very tight stop)"],
        ["Grade B  3-5%",          "+3 pts to Total_Score  (Good)"],
        ["Grade C  5-8%",          "+2 pts to Total_Score  (Acceptable for MST)"],
        ["Grade D  8-12%",         "+1 pt  to Total_Score  (Acceptable for LST)"],
        ["Grade F  >12%",          "+0 pts  (Too wide — avoid unless LST with strong conviction)"],
        ["Total_Score formula",    "RS_Score×0.6 + Fin_Score×2 + SL_Bonus"],
        ["",                       ""],
        # ── Sector ranking ─────────────────────────────────────────────────
        ["── SECTOR RANKING (RS_55d%, best to worst) ──", ""],
    ]

    if not sector_str_df.empty:
        for _, r in sector_str_df.head(16).iterrows():
            rs22 = r.get("RS_22d%", 0) or 0
            rs55 = r.get("RS_55d%", 0) or 0
            rows.append([
                f"  #{int(r['Rank'])}  {r['Sector']}",
                f"{r.get('Signal','')} | RS_22d:{rs22:+.1f}% | RS_55d:{rs55:+.1f}% | RSI:{r.get('RSI_14','—')}",
            ])
    rows.append(["", ""])

    # ── TV Watchlists ───────────────────────────────────────────────────────
    rows += [
        ["── 📺 TRADINGVIEW WATCHLISTS ──", ""],
        ["HOW TO USE",  "Copy cell value → TradingView → Watchlist icon → Import from clipboard"],
        ["All Buy",     tv_all],
        ["Strong Buy",  tv_sb],
        ["MST Buy",     tv_mst],
        ["LST Buy",     tv_lst],
        ["RS30 Buy",    tv_rs30],
    ]

    return pd.DataFrame(rows, columns=["Key", "Value"])


# ─────────────────────────────────────────────────────────────────────────────
#  UPDATED CLASSIFY_TRADE  (returns dict — matches market_engine.py usage)
#  This overrides the simpler version above
# ─────────────────────────────────────────────────────────────────────────────

def classify_trade(sig: str, enh: str, mst: str, lst: str, rs30: str,
                   active_sl: float = np.nan, fin_sc: int = 0,
                   rs_sc: float = 0.0) -> dict:
    """
    Full trade classification. Returns dict with:
      action      : "BUY" / "SELL" / "WAIT"
      signal_type : "RS30 Buy" / "LST Buy" / "MST Buy" / "Strong Buy" / "RS Buy" / "Sell/Exit" / "Watch"
      strategy    : detailed description
      tp1_pct     : TP1 target %  (based on SL × multiplier)
      tp2_pct     : TP2 target %
      rr_t1       : R:R at TP1
      rr_t2       : R:R at TP2

    Priority (highest to lowest):
      RS30 Buy > LST Buy > MST Buy > Strong Buy > RS Buy > Watch/Sell > Wait
    """
    sl = active_sl if (active_sl == active_sl and not np.isnan(float(active_sl))) else np.nan

    def _tp(mult):
        if sl == sl and sl > 0: return round(sl * mult, 2)
        return np.nan

    def _rr(tp_pct):
        if tp_pct == tp_pct and sl == sl and sl > 0: return round(tp_pct / sl, 2)
        return np.nan

    def _build(action, stype, strategy, tp1m, tp2m):
        tp1 = _tp(tp1m); tp2 = _tp(tp2m)
        return dict(action=action, signal_type=stype, strategy=strategy,
                    tp1_pct=tp1, tp2_pct=tp2, rr_t1=_rr(tp1), rr_t2=_rr(tp2))

    # Priority ordering
    if rs30 == "Buy" and lst == "Buy" and mst == "Buy":
        return _build("BUY", "RS30 Buy",
                      "🌟 RS30 + LST + MST triple confirmed (highest conviction)",
                      LST_TP1_MULT, LST_TP2_MULT)

    if rs30 == "Buy" and lst == "Buy":
        return _build("BUY", "RS30 Buy",
                      "🔥 RS30 + LST double confirmed — strong weekly momentum",
                      LST_TP1_MULT, LST_TP2_MULT)

    if rs30 == "Buy" and mst == "Buy":
        return _build("BUY", "RS30 Buy",
                      "🔥 RS30 + MST confirmed — breakout with weekly RS",
                      MST_TP1_MULT, MST_TP2_MULT)

    if lst == "Buy" and mst == "Buy":
        return _build("BUY", "LST Buy",
                      "📈 LST + MST double confirmed — multi-TF alignment",
                      LST_TP1_MULT, LST_TP2_MULT)

    if rs30 == "Buy":
        return _build("BUY", "RS30 Buy",
                      "📊 RS30: Weekly RS30>0 + EMA10>30 + Near52W + Funda",
                      MST_TP1_MULT, MST_TP2_MULT)

    if lst == "Buy":
        return _build("BUY", "LST Buy",
                      "📈 LST: Monthly pre-cond + Weekly entry + Supertrend",
                      LST_TP1_MULT, LST_TP2_MULT)

    if mst == "Buy":
        return _build("BUY", "MST Buy",
                      "🎯 MST: Weekly pre-cond + Daily entry + Supertrend",
                      MST_TP1_MULT, MST_TP2_MULT)

    if enh == "Strong Buy":
        return _build("BUY", "Strong Buy",
                      "⭐ Strong RS Buy: all 5 peer-comparison filters pass",
                      2.5, 3.5)

    if sig == "Buy":
        return _build("BUY", "RS Buy",
                      "✅ RS Buy: RS_22d>0 + RS_55d>0 (Index + Sector)",
                      2.5, 3.5)

    if sig == "Sell":
        return _build("SELL", "Sell/Exit",
                      "🔴 Sell: RS_22d<0 + RS_55d<0 (momentum breakdown)",
                      2.5, 3.5)

    # WAIT states
    watch_strats = [("MST" if mst=="Watch" else ""),
                    ("LST" if lst=="Watch" else ""),
                    ("RS30" if rs30=="Watch" else "")]
    ws = "+".join(s for s in watch_strats if s)
    if ws:
        return _build("WAIT", "Watch",
                      f"⏳ {ws} Watch — pre-conditions met, waiting for breakout",
                      np.nan, np.nan)

    return _build("WAIT", "No Signal",
                  "⬜ Consolidation / No signal — monitor for setup",
                  np.nan, np.nan)


# ─────────────────────────────────────────────────────────────────────────────
#  UPDATED SL_BONUS  (supports both 1-arg and 2-arg calls)
# ─────────────────────────────────────────────────────────────────────────────

def sl_bonus(sl_pct, rr_t1=np.nan) -> float:
    """
    Bonus score from SL quality + R:R.
    sl_pct : % stop loss
    rr_t1  : R:R ratio at TP1 (optional)

    Bonus from SL grade: A=4 B=3 C=2 D=1 F=0
    Bonus from R:R:      ≥3x=+2  ≥2x=+1  <2x=0
    Max total = 6 pts
    """
    g = sl_grade(sl_pct)
    base = {"A":4,"B":3,"C":2,"D":1,"F":0}.get(g, 0)
    rr_pts = 0
    try:
        rr = float(rr_t1)
        if not np.isnan(rr):
            if   rr >= 3.0: rr_pts = 2
            elif rr >= 2.0: rr_pts = 1
    except: pass
    return float(base + rr_pts)
