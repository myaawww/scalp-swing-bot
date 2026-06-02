"""
Scalp Swing Bot v10 – Python Signal Engine  (v3 — execution/risk upgrades)
Hyperliquid Perpetuals edition
Timeframe map: 4H bias / 1H middle / 15m + 5m execution
Runs on GitHub Actions every 15 min, sends Telegram alerts.
No orders are placed — signal only.

v2 FIXES (aligned to Pine Script v10 logic)
────────────────────────────────────────────
[FIX 1] h4_bull / h4_bear was incorrectly aliased to h1_bull / h1_bear.
        Now correctly computed from the 4H EMA arrays (ef4h > es4h).

[FIX 2] All HTF EMA/ADX values now use [-2] (last confirmed closed bar)
        instead of [-1], matching Pine's request.security lookahead_off
        which returns close[1] — the previous fully closed bar.
        Affects: 1H EMA (ef1h/es1h), 4H EMA (ef4h/es4h), 4H ADX, Daily 200 EMA.

[FIX 3] trend_held() offset corrected to -(offset+1) to align with the
        [-2]=bar[1] convention, matching Pine's nz(ef[1])>nz(es[1]) checks.

[FIX 4] pull_touched_long/short now use ef15_prev (previous bar's fast EMA)
        instead of ef15, matching Pine's localEMAf[1] reference.

[FIX 5] obv_slope indexing corrected to -(1+OBV_LEN) for clarity and
        consistency with Pine's obvVal > obvVal[obvLen].
"""

import os, json, time, math, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# ── CONFIG ───────────────────────────────────────────────────
# When imported as a library (from `signal_engine.py`), we don't want
# to hard-require Telegram env vars just to compute signals.
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID", "")
STATE_FILE   = "state.json"
SIGNAL_LOG_FILE = "signal_log.jsonl"

# ── HARDCODED WATCHLIST ───────────────────────────────────────
# Format: standard USDT pairs — hl_coin() strips the suffix automatically
WATCHLIST = [
    "BTCUSDT",
    "ETHUSDT",
    "HYPEUSDT",
    "ZECUSDT",
    "NEARUSDT",
    "ONDOUSDT",
    "SUIUSDT",
    "PUMPUSDT",
    "PENGUUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "TRXUSDT",
    "TONUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "DOTUSDT",
    "TAOUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "AAVEUSDT",
    "XRPUSDT",
    "XLMUSDT",
]

# ── INDICATOR LENGTHS ────────────────────────────────────────
FAST_LEN  = 21
SLOW_LEN  = 50
TREND_LEN = 200
RSI_LEN   = 14
ATR_LEN   = 14
ADX_LEN   = 14
BB_LEN    = 20
BB_MULT   = 2.0
VOL_LEN   = 20
OBV_LEN   = 3

# ── RISK / SCORE ─────────────────────────────────────────────
MIN_SCORE       = 4
TP1_MULT        = 1.0
TP2_MULT        = 1.5
SL_MULT         = 1.0
COOLDOWN_BARS   = 32
GLOBAL_COOLDOWN = 16
MIN_RR          = 1.15
MIN_SL_PCT      = 0.20

# ── FILTERS ──────────────────────────────────────────────────
ADX_BREAK_GATE  = 20.0
ADX_SCORE_MIN   = 20.0
RSI_LONG_MIN    = 45.0;  RSI_LONG_MAX  = 65.0
RSI_SHORT_MIN   = 35.0;  RSI_SHORT_MAX = 55.0
VOL_SCORE_MULT  = 1.0
MAX_ATR_PCT     = 10.0
MIN_ATR_PCT     = 0.2
WICK_FILTER     = 0.45
RANGE_PCT_BREAK = 0.30
PULL_ZONE_MULT  = 0.25
TREND_HOLD_BARS = 2
USE_ROLLING_VWAP  = True
ROLLING_VWAP_LEN  = 16
USE_D200_FILTER   = True
USE_DAILY_ADX     = True
MIN_DAILY_ADX     = 20.0
PULL_REQUIRES_4H  = True
USE_M5_CONFIRM    = False
USE_SESSION_FILTER = True
SESSION_UTC_START  = 0
SESSION_UTC_END    = 23

# ── CANDLE COUNTS PER TIMEFRAME ──────────────────────────────
# 15m: full 300 needed for indicators (ADX, BB, OBV, EMA200 not used here)
# 1H/4H: only need EMA21/50 + ADX14 — 60 candles is plenty
# Daily: needs EMA200 + small buffer
N_15M = 300
N_5M  = 300
N_1H  = 60
N_4H  = 60
N_1D  = 210

# ── HYPERLIQUID ENDPOINT ──────────────────────────────────────
HL_INFO_URL = "https://api.hyperliquid.xyz/info"

# Interval → milliseconds (for startTime calculation)
INTERVAL_MS = {
    "5m":   5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}


# ═══════════════════════════════════════════════════════════════
# HYPERLIQUID DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def hl_coin(symbol: str) -> str:
    """Convert BTCUSDT → BTC (Hyperliquid perp coin name)."""
    return symbol.replace("USDT", "")


def hl_post(payload: dict) -> any:
    """POST to Hyperliquid /info with retry."""
    for attempt in range(3):
        try:
            r = requests.post(
                HL_INFO_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(1.5 ** attempt)


def get_candles(symbol: str, interval: str, n: int) -> list[dict]:
    """
    Fetch last n closed candles for symbol on interval via Hyperliquid.
    Returns list of dicts: {t, o, h, l, c, v}  newest last.
    Hyperliquid candleSnapshot supports up to 5000 candles.
    """
    coin      = hl_coin(symbol)
    iv_ms     = INTERVAL_MS.get(interval, 60 * 60 * 1000)
    end_ms    = int(time.time() * 1000)
    start_ms  = end_ms - iv_ms * (n + 10)   # fetch a bit extra for safety

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin":      coin,
            "interval":  interval,
            "startTime": start_ms,
            "endTime":   end_ms,
        },
    }

    raw = hl_post(payload)
    if not isinstance(raw, list):
        raise ValueError(f"Unexpected candle response for {symbol} {interval}: {type(raw).__name__}")

    candles = []
    for c in raw:
        try:
            candles.append({
                "t":  int(c["t"]),
                "o":  float(c["o"]),
                "h":  float(c["h"]),
                "l":  float(c["l"]),
                "c":  float(c["c"]),
                "v":  float(c["v"]),
                "qv": float(c["v"]),   # HL returns base volume; reuse for qv
            })
        except (KeyError, TypeError, ValueError):
            continue

    # Drop the last (possibly still-open) candle, return the most recent n
    closed = candles[:-1][-n:]
    if len(closed) < max(30, n // 4):
        raise ValueError(f"Insufficient valid candles for {symbol} {interval}: got {len(closed)}")
    return closed

def fetch_all_candles(symbol: str) -> tuple[list, list, list, list, list] | None:
    """
    Fetch all 4 timeframes concurrently for one symbol.
    Returns (candles_15m, candles_5m, candles_1h, candles_4h, candles_d)
    or None if 15m data is insufficient.
    """
    # First fetch 15m alone so we can bail early without wasting the other 3 calls
    candles_15m = get_candles(symbol, "15m", N_15M)
    if len(candles_15m) < 50:
        return None

    # Now fetch the remaining 3 TFs in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {
            ex.submit(get_candles, symbol, "5m",  N_5M): "5m",
            ex.submit(get_candles, symbol, "1h",  N_1H): "1h",
            ex.submit(get_candles, symbol, "4h",  N_4H): "4h",
            ex.submit(get_candles, symbol, "1d",  N_1D): "1d",
        }
        for fut in as_completed(futures):
            tf = futures[fut]
            results[tf] = fut.result()   # propagates exceptions naturally

    return candles_15m, results["5m"], results["1h"], results["4h"], results["1d"]



# ═══════════════════════════════════════════════════════════════

def ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return [float("nan")] * len(values)
    k = 2.0 / (period + 1)
    result = [float("nan")] * len(values)
    result[period - 1] = sum(values[:period]) / period
    for i in range(period, len(values)):
        result[i] = values[i] * k + result[i - 1] * (1 - k)
    return result


def rsi(closes: list[float], period: int) -> list[float]:
    if len(closes) < period + 1:
        return [float("nan")] * len(closes)
    result = [float("nan")] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    rs = avg_g / avg_l if avg_l != 0 else float("inf")
    result[period] = 100 - 100 / (1 + rs)
    for i in range(period + 1, len(closes)):
        idx = i - 1
        avg_g = (avg_g * (period - 1) + gains[idx]) / period
        avg_l = (avg_l * (period - 1) + losses[idx]) / period
        rs = avg_g / avg_l if avg_l != 0 else float("inf")
        result[i] = 100 - 100 / (1 + rs)
    return result


def atr(highs, lows, closes, period: int) -> list[float]:
    trs = [float("nan")]
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)
    result = [float("nan")] * len(closes)
    if len(trs) < period + 1:
        return result
    result[period] = sum(trs[1:period + 1]) / period
    for i in range(period + 1, len(trs)):
        result[i] = (result[i - 1] * (period - 1) + trs[i]) / period
    return result


def bollinger(closes, period: int, mult: float):
    basis_arr = [float("nan")] * len(closes)
    upper_arr = [float("nan")] * len(closes)
    lower_arr = [float("nan")] * len(closes)
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1: i + 1]
        m  = sum(window) / period
        sd = math.sqrt(sum((x - m) ** 2 for x in window) / period)
        basis_arr[i] = m
        upper_arr[i] = m + mult * sd
        lower_arr[i] = m - mult * sd
    return basis_arr, upper_arr, lower_arr


def adx_dmi(highs, lows, closes, period: int):
    n = len(closes)
    plus_dm  = [0.0] * n
    minus_dm = [0.0] * n
    tr_arr   = [0.0] * n
    for i in range(1, n):
        up   = highs[i]    - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm[i]  = up   if (up > down  and up > 0)   else 0
        minus_dm[i] = down if (down > up  and down > 0) else 0
        tr_arr[i]   = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )

    def wilder_smooth(arr):
        res = [0.0] * n
        if n <= period:
            return res
        res[period] = sum(arr[1:period + 1])
        for i in range(period + 1, n):
            res[i] = res[i - 1] - res[i - 1] / period + arr[i]
        return res

    sm_tr   = wilder_smooth(tr_arr)
    sm_plus = wilder_smooth(plus_dm)
    sm_min  = wilder_smooth(minus_dm)

    di_plus  = [float("nan")] * n
    di_minus = [float("nan")] * n
    dx_arr   = [float("nan")] * n
    adx_arr  = [float("nan")] * n

    for i in range(period, n):
        if sm_tr[i] == 0:
            continue
        dp = 100 * sm_plus[i] / sm_tr[i]
        dm = 100 * sm_min[i]  / sm_tr[i]
        di_plus[i]  = dp
        di_minus[i] = dm
        dsum = dp + dm
        dx_arr[i] = 100 * abs(dp - dm) / dsum if dsum != 0 else 0

    first = next((i for i in range(period, n) if not math.isnan(dx_arr[i])), None)
    if first is None:
        return di_plus, di_minus, adx_arr
    seed_end = first + period
    if seed_end > n:
        return di_plus, di_minus, adx_arr
    valid = [dx_arr[i] for i in range(first, seed_end) if not math.isnan(dx_arr[i])]
    if len(valid) < period:
        return di_plus, di_minus, adx_arr
    adx_arr[seed_end - 1] = sum(valid) / period
    for i in range(seed_end, n):
        if not math.isnan(adx_arr[i - 1]) and not math.isnan(dx_arr[i]):
            adx_arr[i] = (adx_arr[i - 1] * (period - 1) + dx_arr[i]) / period

    return di_plus, di_minus, adx_arr


def obv(closes, volumes) -> list[float]:
    result = [0.0] * len(closes)
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            result[i] = result[i - 1] + volumes[i]
        elif closes[i] < closes[i - 1]:
            result[i] = result[i - 1] - volumes[i]
        else:
            result[i] = result[i - 1]
    return result


def sma(values, period: int) -> list[float]:
    result = [float("nan")] * len(values)
    for i in range(period - 1, len(values)):
        result[i] = sum(values[i - period + 1: i + 1]) / period
    return result


def rolling_vwap(closes, volumes, period: int) -> list[float]:
    pv  = [c * v for c, v in zip(closes, volumes)]
    spv = sma(pv,      period)
    sv  = sma(volumes, period)
    result = [float("nan")] * len(closes)
    for i in range(len(closes)):
        if not math.isnan(spv[i]) and sv[i] != 0:
            result[i] = spv[i] / sv[i]
    return result


def safe(v, fallback=0.0):
    return fallback if (v is None or math.isnan(v)) else v


# ═══════════════════════════════════════════════════════════════
# SIGNAL LOGIC
# ═══════════════════════════════════════════════════════════════

class SignalResult:
    def __init__(self):
        self.fire_long   = False
        self.fire_short  = False
        self.signal_type = ""
        self.score       = 0
        self.entry = self.tp1 = self.tp2 = self.sl = 0.0
        self.entry_low = self.entry_high = 0.0
        self.atr_pct = 0.0
        self.sl_pct = 0.0
        self.rr = 0.0
        self.leverage_hint = ""
        self.regime = ""
        self.atr_val  = 0.0
        self.breakdown = ""
        self.v10_gates = ""
        self.reject_reason = ""


def compute_signals(symbol, candles_15m, candles_5m, candles_1h, candles_4h, candles_d) -> SignalResult:
    res = SignalResult()

    def arrays(candles):
        o  = [c["o"] for c in candles]
        h  = [c["h"] for c in candles]
        l  = [c["l"] for c in candles]
        cl = [c["c"] for c in candles]
        v  = [c["v"] for c in candles]
        return o, h, l, cl, v

    # ── 15m ──────────────────────────────────────────────────
    o15, h15, l15, c15, v15 = arrays(candles_15m)
    ema_f15 = ema(c15, FAST_LEN);  ef15 = safe(ema_f15[-1])
    ema_s15 = ema(c15, SLOW_LEN);  es15 = safe(ema_s15[-1])
    rsi15   = rsi(c15, RSI_LEN);   r15  = safe(rsi15[-1])
    atr15   = atr(h15, l15, c15, ATR_LEN); a15 = safe(atr15[-1])
    _, _, adx15_arr = adx_dmi(h15, l15, c15, ADX_LEN)
    adx15    = safe(adx15_arr[-1], 25.0)
    bb_b15, bb_u15, bb_l15 = bollinger(c15, BB_LEN, BB_MULT)
    bb_basis = safe(bb_b15[-1])
    vol_ma15 = sma(v15, VOL_LEN);  vm15 = safe(vol_ma15[-1])
    obv15    = obv(c15, v15)
    rvwap15  = rolling_vwap(c15, v15, ROLLING_VWAP_LEN)

    # ── Execution-bar values (Pine: h1C/h1O/h1H/h1L via request.security close[1]) ──
    # get_candles() drops the still-open candle, so [-1] IS the last confirmed close.
    # Pine's HTF security call with lookahead_off returns the PREVIOUS HTF bar's close,
    # but the execution TF IS 15m (same as the chart), so h1C == close[1] on the NEXT bar.
    # To stay consistent: cur_* are the signal-bar values (confirmed close we evaluate now).
    cur_c = c15[-1]; cur_o = o15[-1]
    cur_h = h15[-1]; cur_l = l15[-1]
    cur_v = v15[-1]
    prev_l = l15[-2]; prev_h = h15[-2]
    ef15_prev = safe(ema_f15[-2])   # Pine: localEMAf[1] — previous bar's fast EMA

    atr_val  = a15 if a15 > 0 else (cur_c * 0.03 / 100)
    atr_pct  = atr_val / cur_c * 100
    market_ok = MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT

    rv = safe(rvwap15[-1])
    vwap_long  = cur_c > rv if USE_ROLLING_VWAP else True
    vwap_short = cur_c < rv if USE_ROLLING_VWAP else True

    rsi_long  = RSI_LONG_MIN  <= r15 <= RSI_LONG_MAX
    rsi_short = RSI_SHORT_MIN <= r15 <= RSI_SHORT_MAX

    bb_long  = cur_c > bb_basis
    bb_short = cur_c < bb_basis

    # OBV slope: Pine uses obvVal > obvVal[obvLen] on the current (confirmed) bar
    # get_candles() already strips the open candle, so [-1] is the last closed bar
    obv_slope_long  = obv15[-1] > obv15[-(1 + OBV_LEN)]
    obv_slope_short = obv15[-1] < obv15[-(1 + OBV_LEN)]

    adx_score_ok = adx15 >= ADX_SCORE_MIN
    vol_score_ok = True if vm15 == 0 else (cur_v >= vm15 * VOL_SCORE_MULT)

    # ── 1H ───────────────────────────────────────────────────
    o1h, h1h, l1h, c1h, v1h = arrays(candles_1h)
    ema_f1h = ema(c1h, FAST_LEN)
    ema_s1h = ema(c1h, SLOW_LEN)
    # Pine fetches HTF with lookahead_off → uses close[1] (last confirmed bar)
    # In Python: [-2] is the last fully closed 1H candle, [-1] may still be open
    ef1h = safe(ema_f1h[-2])
    es1h = safe(ema_s1h[-2])
    h1_bull = ef1h > es1h
    h1_bear = ef1h < es1h

    # ── 5M ───────────────────────────────────────────────────
    o5m, h5m, l5m, c5m, v5m = arrays(candles_5m)
    ema_f5m = ema(c5m, FAST_LEN)
    ema_s5m = ema(c5m, SLOW_LEN)
    rsi5m   = rsi(c5m, RSI_LEN)
    ef5m = safe(ema_f5m[-2])
    es5m = safe(ema_s5m[-2])
    r5m  = safe(rsi5m[-2], 50.0)
    m5_bull = ef5m > es5m and r5m >= 45
    m5_bear = ef5m < es5m and r5m <= 55

    # ── 4H ───────────────────────────────────────────────────
    o4h, h4h, l4h, c4h, v4h = arrays(candles_4h)
    ema_f4h = ema(c4h, FAST_LEN)
    ema_s4h = ema(c4h, SLOW_LEN)
    # Pine: lookahead_off → close[1], so use [-2] (last confirmed 4H close)
    ef4h = safe(ema_f4h[-2]); es4h = safe(ema_s4h[-2])
    _, _, adx4h_arr = adx_dmi(h4h, l4h, c4h, ADX_LEN)
    adx4h  = safe(adx4h_arr[-2], 25.0)   # confirmed 4H ADX value
    d_bull = ef4h > es4h
    d_bear = ef4h < es4h

    def trend_held(ef_arr, es_arr, bull: bool) -> bool:
        # Pine: nz(ef[1])>nz(es[1]) ... nz(ef[N])>nz(es[N])
        # Since we treat [-2] as bar[1], bar[2]=[-3], bar[N]=[-(N+1)]
        for offset in range(1, TREND_HOLD_BARS + 1):
            idx = -(offset + 1)
            if len(ef_arr) < offset + 2:
                return False
            if bull:
                if safe(ef_arr[idx]) <= safe(es_arr[idx]):
                    return False
            else:
                if safe(ef_arr[idx]) >= safe(es_arr[idx]):
                    return False
        return True

    d_trend_held_bull = trend_held(ema_f4h, ema_s4h, True)
    d_trend_held_bear = trend_held(ema_f4h, ema_s4h, False)

    # ── Daily 200 EMA ─────────────────────────────────────────
    if candles_d and len(candles_d) >= TREND_LEN:
        _, _, _, c_d, _ = arrays(candles_d)
        ema_d200 = ema(c_d, TREND_LEN)
        # Pine: lookahead_off → close[1]; use [-2] (last confirmed daily close)
        d200 = safe(ema_d200[-2])
    else:
        d200 = cur_c
    macro_long  = cur_c > d200 if USE_D200_FILTER else True
    macro_short = cur_c < d200 if USE_D200_FILTER else True

    daily_adx_ok = adx4h >= MIN_DAILY_ADX if USE_DAILY_ADX else True

    # h4_bull / h4_bear: sourced from actual 4H EMA alignment
    # (was incorrectly aliased to h1_bull / h1_bear — BUG FIX)
    h4_bull = ef4h > es4h
    h4_bear = ef4h < es4h

    full_long_align  = d_bull and d_trend_held_bull and h4_bull and h1_bull
    full_short_align = d_bear and d_trend_held_bear and h4_bear and h1_bear

    pull_long_align  = (d_bull and d_trend_held_bull and h4_bull and h1_bull) \
                        if PULL_REQUIRES_4H else \
                       (d_bull and d_trend_held_bull and h1_bull)
    pull_short_align = (d_bear and d_trend_held_bear and h4_bear and h1_bear) \
                        if PULL_REQUIRES_4H else \
                       (d_bear and d_trend_held_bear and h1_bear)

    rng = cur_h - cur_l + 1e-10

    def clean_bull_bar():
        return (cur_h - max(cur_o, cur_c)) / rng <= WICK_FILTER

    def clean_bear_bar():
        return (min(cur_o, cur_c) - cur_l) / rng <= WICK_FILTER

    def close_in_top_range():
        return (cur_c - cur_l) / rng >= (1.0 - RANGE_PCT_BREAK)

    def close_in_bot_range():
        return (cur_c - cur_l) / rng <= RANGE_PCT_BREAK

    adx_break_ok   = adx15 >= ADX_BREAK_GATE
    break_bull_bar = cur_c > cur_o and clean_bull_bar() and close_in_top_range()
    break_bear_bar = cur_c < cur_o and clean_bear_bar() and close_in_bot_range()

    pull_zone          = atr_val * PULL_ZONE_MULT
    pull_touched_long  = prev_l <= ef15_prev + pull_zone   # Pine: low[1] <= localEMAf[1] + zone
    pull_touched_short = prev_h >= ef15_prev - pull_zone   # Pine: high[1] >= localEMAf[1] - zone
    pull_recover_long  = cur_c > ef15 and cur_c > cur_o
    pull_recover_short = cur_c < ef15 and cur_c < cur_o
    pull_bull_bar      = cur_c > cur_o and clean_bull_bar()
    pull_bear_bar      = cur_c < cur_o and clean_bear_bar()

    bb_width_pct = ((safe(bb_u15[-1]) - safe(bb_l15[-1])) / max(cur_c, 1e-10)) * 100.0
    adx_rising = adx15 > safe(adx15_arr[-2], adx15)
    if adx15 >= 25 and bb_width_pct >= 1.00 and adx_rising:
        regime = "TRENDING"
    elif adx15 < 18 and bb_width_pct < 0.80:
        regime = "RANGING"
    else:
        regime = "MIXED"

    break_regime_ok = regime in ("TRENDING", "MIXED")
    pull_regime_ok  = regime in ("RANGING", "MIXED")

    long_score = sum([
        bb_long,
        d_bull and d_trend_held_bull,
        adx_score_ok,
        obv_slope_long,
        vol_score_ok,
    ])
    short_score = sum([
        bb_short,
        d_bear and d_trend_held_bear,
        adx_score_ok,
        obv_slope_short,
        vol_score_ok,
    ])

    m5_long_ok  = m5_bull if USE_M5_CONFIRM else True
    m5_short_ok = m5_bear if USE_M5_CONFIRM else True

    long_break  = (macro_long  and daily_adx_ok and full_long_align  and break_bull_bar
                   and adx_break_ok and vwap_long  and rsi_long  and long_score  >= MIN_SCORE and market_ok)
    short_break = (macro_short and daily_adx_ok and full_short_align and break_bear_bar
                   and adx_break_ok and vwap_short and rsi_short and short_score >= MIN_SCORE and market_ok)

    long_pull  = (macro_long  and daily_adx_ok and pull_long_align  and pull_touched_long
                  and pull_recover_long  and pull_bull_bar and vwap_long  and rsi_long
                  and long_score  >= MIN_SCORE and market_ok)
    short_pull = (macro_short and daily_adx_ok and pull_short_align and pull_touched_short
                  and pull_recover_short and pull_bear_bar and vwap_short and rsi_short
                  and short_score >= MIN_SCORE and market_ok)

    long_break  = long_break and break_regime_ok and m5_long_ok
    short_break = short_break and break_regime_ok and m5_short_ok
    long_pull   = long_pull and pull_regime_ok and m5_long_ok
    short_pull  = short_pull and pull_regime_ok and m5_short_ok

    long_sig  = long_break or long_pull
    short_sig = short_break or short_pull

    def dynamic_multipliers(local_atr_pct: float, local_regime: str, is_break: bool):
        if local_regime == "TRENDING":
            if local_atr_pct <= 1.2:
                return (1.2, 1.9, 0.9) if is_break else (1.0, 1.5, 0.85)
            return (1.0, 1.5, 1.0) if is_break else (0.8, 1.2, 0.95)
        if local_regime == "RANGING":
            return (0.8, 1.2, 0.8)
        return (TP1_MULT, TP2_MULT, SL_MULT)

    def rr_ok(entry: float, tp2: float, sl: float):
        sl_dist = abs(entry - sl)
        if sl_dist <= 1e-10:
            return False, 0.0, 0.0
        rr = abs(tp2 - entry) / sl_dist
        sl_pct = sl_dist / max(entry, 1e-10) * 100.0
        return rr >= MIN_RR and sl_pct >= MIN_SL_PCT, rr, sl_pct

    def leverage_hint(local_atr_pct: float, score: int) -> str:
        if local_atr_pct <= 0.60:
            low, high = 8, 12
        elif local_atr_pct <= 1.20:
            low, high = 5, 10
        elif local_atr_pct <= 2.00:
            low, high = 3, 7
        else:
            low, high = 2, 5
        if score == 4:
            high = max(low, high - 2)
        return f"{low}x - {high}x"

    def make_breakdown(is_long):
        bb_s  = "BB✓"  if (bb_long  if is_long else bb_short)  else "BB✗"
        d_s   = "4H✓"  if (d_bull and d_trend_held_bull if is_long else d_bear and d_trend_held_bear) else "4H✗"
        adx_s = "ADX✓" if adx_score_ok  else "ADX✗"
        obv_s = "OBV✓" if (obv_slope_long if is_long else obv_slope_short) else "OBV✗"
        vol_s = "VOL✓" if vol_score_ok   else "VOL✗"
        return f"{bb_s} {d_s} {adx_s} {obv_s} {vol_s}"

    def make_gates(is_long):
        macro = macro_long if is_long else macro_short
        h1    = h1_bull    if is_long else h1_bear
        m5 = m5_bull if is_long else m5_bear
        return (f"D200:{'✓' if macro else '✗'} "
                f"4HADX:{'✓' if daily_adx_ok else '✗'} "
                f"1H:{'✓' if h1 else '✗'} "
                f"5M:{'✓' if m5 else '✗'}")

    if long_sig:
        res.fire_long    = True
        res.signal_type  = "BREAK" if long_break else "PULL"
        res.score        = long_score
        res.entry        = cur_c
        tp1_m, tp2_m, sl_m = dynamic_multipliers(atr_pct, regime, long_break)
        zone_half_width  = atr_val * PULL_ZONE_MULT
        res.entry_low    = cur_c - zone_half_width
        res.entry_high   = cur_c + zone_half_width
        res.tp1          = cur_c + atr_val * tp1_m
        res.tp2          = cur_c + atr_val * tp2_m
        res.sl           = cur_c - atr_val * sl_m
        res.atr_val      = atr_val
        res.atr_pct      = atr_pct
        ok_quality, rr, sl_pct = rr_ok(res.entry, res.tp2, res.sl)
        if not ok_quality:
            res.fire_long = False
            res.fire_short = False
            res.reject_reason = f"quality_gate_failed rr={rr:.2f} sl_pct={sl_pct:.2f}%"
            return res
        res.rr           = rr
        res.sl_pct       = sl_pct
        res.leverage_hint = leverage_hint(atr_pct, long_score)
        res.regime       = regime
        res.breakdown    = make_breakdown(True)
        res.v10_gates    = make_gates(True)
    elif short_sig:
        res.fire_short   = True
        res.signal_type  = "BREAK" if short_break else "PULL"
        res.score        = short_score
        res.entry        = cur_c
        tp1_m, tp2_m, sl_m = dynamic_multipliers(atr_pct, regime, short_break)
        zone_half_width  = atr_val * PULL_ZONE_MULT
        res.entry_low    = cur_c - zone_half_width
        res.entry_high   = cur_c + zone_half_width
        res.tp1          = cur_c - atr_val * tp1_m
        res.tp2          = cur_c - atr_val * tp2_m
        res.sl           = cur_c + atr_val * sl_m
        res.atr_val      = atr_val
        res.atr_pct      = atr_pct
        ok_quality, rr, sl_pct = rr_ok(res.entry, res.tp2, res.sl)
        if not ok_quality:
            res.fire_long = False
            res.fire_short = False
            res.reject_reason = f"quality_gate_failed rr={rr:.2f} sl_pct={sl_pct:.2f}%"
            return res
        res.rr           = rr
        res.sl_pct       = sl_pct
        res.leverage_hint = leverage_hint(atr_pct, short_score)
        res.regime       = regime
        res.breakdown    = make_breakdown(False)
        res.v10_gates    = make_gates(False)

    return res


# ═══════════════════════════════════════════════════════════════
# COOLDOWN STATE
# ═══════════════════════════════════════════════════════════════

def load_state() -> dict:
    if Path(STATE_FILE).exists():
        try:
            return json.loads(Path(STATE_FILE).read_text())
        except Exception:
            pass
    return {}


def save_state(state: dict):
    Path(STATE_FILE).write_text(json.dumps(state, indent=2))


def check_cooldown(state, coin, direction, bar_index, score: int) -> bool:
    last_dir    = state.get(f"{coin}_{direction}", -9999)
    last_global = state.get(f"{coin}_any",         -9999)
    loss_streak = state.get(f"{coin}_loss_streak", 0)
    penalty = loss_streak * 8
    quality_bonus = -4 if score >= 5 else 0
    req_dir = max(8, COOLDOWN_BARS + penalty + quality_bonus)
    req_global = max(4, GLOBAL_COOLDOWN + (penalty // 2) + (quality_bonus // 2))
    return (bar_index - last_dir    >= req_dir and
            bar_index - last_global >= req_global)


def update_cooldown(state, coin, direction, bar_index):
    state[f"{coin}_{direction}"] = bar_index
    state[f"{coin}_any"]         = bar_index


def append_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, separators=(",", ":")) + "\n")


def settle_open_trades(state: dict, symbol: str, last_candle: dict):
    open_trades = state.get("open_trades", [])
    if not open_trades:
        return

    remaining = []
    for tr in open_trades:
        if tr.get("symbol") != symbol:
            remaining.append(tr)
            continue

        direction = tr["direction"]
        sl = float(tr["sl"])
        tp2 = float(tr["tp2"])
        low = float(last_candle["l"])
        high = float(last_candle["h"])
        hit = None

        # Conservative tie-break: SL first if both touched in one candle.
        if direction == "long":
            if low <= sl:
                hit = "SL"
            elif high >= tp2:
                hit = "TP2"
        else:
            if high >= sl:
                hit = "SL"
            elif low <= tp2:
                hit = "TP2"

        if hit is None:
            remaining.append(tr)
            continue

        won = hit == "TP2"
        if won:
            state[f"{symbol}_loss_streak"] = 0
        else:
            state[f"{symbol}_loss_streak"] = state.get(f"{symbol}_loss_streak", 0) + 1

        append_jsonl(SIGNAL_LOG_FILE, {
            "event": "trade_closed",
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "direction": direction,
            "opened_at": tr.get("opened_at"),
            "entry": tr.get("entry"),
            "tp2": tp2,
            "sl": sl,
            "close_reason": hit,
            "close_price_hint": float(last_candle["c"]),
            "loss_streak_after": state.get(f"{symbol}_loss_streak", 0),
        })

    state["open_trades"] = remaining


# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def stars(score: int) -> str:
    return "★" * score + "☆" * (5 - score)


def send_telegram(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("[TG] Missing TG_BOT_TOKEN/TG_CHAT_ID; skipping send.")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(url, json={
                "chat_id":    TG_CHAT_ID,
                "text":       text,
                "parse_mode": "HTML",
            }, timeout=10)
            r.raise_for_status()
            return
        except Exception as e:
            if attempt == 2:
                print(f"[TG ERROR] {e}")
            time.sleep(2)


def format_signal(symbol: str, sig: SignalResult) -> str:
    direction = "▲ LONG" if sig.fire_long else "▼ SHORT"
    emoji     = "🟢" if sig.fire_long else "🔴"
    ts        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def fmt(v):
        if v >= 1000: return f"{v:,.2f}"
        if v >= 1:    return f"{v:.4f}"
        return f"{v:.6f}"

    return (
        f"{emoji} <b>{direction} [{sig.signal_type}]</b>  {stars(sig.score)}\n"
        f"<b>Pair:</b>  {symbol}\n"
        f"<b>Entry:</b> {fmt(sig.entry)}\n"
        f"<b>Entry Zone:</b> {fmt(sig.entry_low)} - {fmt(sig.entry_high)}\n"
        f"<b>TP1:</b>   {fmt(sig.tp1)}\n"
        f"<b>TP2:</b>   {fmt(sig.tp2)}\n"
        f"<b>SL:</b>    {fmt(sig.sl)}\n"
        f"<b>Leverage:</b> {sig.leverage_hint}\n"
        f"<b>R:R (TP2/SL):</b> 1:{sig.rr:.2f}  |  <b>SL%:</b> {sig.sl_pct:.2f}%\n"
        f"<b>Regime:</b> {sig.regime}\n"
        f"<b>Score:</b> {sig.score}/5  |  {sig.breakdown}\n"
        f"<b>Gates:</b> {sig.v10_gates}\n"
        f"<i>Scalp Swing v10 [H4/H1/M15+M5] • Hyperliquid Perps • {ts}</i>"
    )


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def scan_symbol(symbol: str) -> dict:
    """
    Scan a single symbol without mutating shared state.
    Returns a dict containing the latest candle and computed signal.
    """
    coin = hl_coin(symbol)
    data = fetch_all_candles(symbol)
    if data is None:
        print(f"    Skipping {coin}: insufficient candles")
        return {"symbol": symbol, "status": "skip"}

    candles_15m, candles_5m, candles_1h, candles_4h, candles_d = data
    sig = compute_signals(symbol, candles_15m, candles_5m, candles_1h, candles_4h, candles_d)
    return {
        "symbol": symbol,
        "status": "ok",
        "last_candle": candles_15m[-1],
        "sig": sig,
    }


def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Scanner starting…")
    print(f"Watchlist ({len(WATCHLIST)} pairs): {[hl_coin(s) for s in WATCHLIST]}")

    bar_index_now = int(time.time() // (15 * 60))
    now_hour_utc = datetime.now(timezone.utc).hour
    if USE_SESSION_FILTER and not (SESSION_UTC_START <= now_hour_utc <= SESSION_UTC_END):
        print(f"Outside active session window UTC {SESSION_UTC_START:02d}:00-{SESSION_UTC_END:02d}:59. Skipping scan.")
        return

    state         = load_state()
    state.setdefault("open_trades", [])
    signals_fired = 0

    # Scan all symbols in parallel — up to 10 concurrent workers
    # (each worker fetches 15m first, then 4 TFs in parallel, so peak ≈ 50 connections)
    pending_scan_results = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(scan_symbol, sym): sym
                   for sym in WATCHLIST}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                result = fut.result()
                if result is not None:
                    pending_scan_results.append(result)
            except Exception as e:
                print(f"    ERROR processing {sym}: {e}")

    # Apply trade settlement in main thread only (avoids shared-state races).
    for item in pending_scan_results:
        if item.get("status") != "ok":
            continue
        settle_open_trades(state, item["symbol"], item["last_candle"])

    pending_signals = []
    for item in pending_scan_results:
        if item.get("status") != "ok":
            continue

        symbol = item["symbol"]
        sig = item["sig"]
        coin = hl_coin(symbol)

        if not (sig.fire_long or sig.fire_short):
            if getattr(sig, "reject_reason", ""):
                print(f"    {coin}: no signal ({sig.reject_reason})")
            else:
                print(f"    {coin}: no signal")
            continue

        direction = "long" if sig.fire_long else "short"
        if not check_cooldown(state, symbol, direction, bar_index_now, sig.score):
            print(f"    {coin} signal suppressed by cooldown")
            continue

        print(f"    🚀 SIGNAL: {coin} {direction.upper()} [{sig.signal_type}] score={sig.score}")
        pending_signals.append((symbol, format_signal(symbol, sig), direction, sig))

    # Send Telegram alerts sequentially (avoids flood and preserves cooldown ordering)
    for symbol, msg, direction, sig in pending_signals:
        send_telegram(msg)
        update_cooldown(state, symbol, direction, bar_index_now)
        state["open_trades"].append({
            "symbol": symbol,
            "direction": direction,
            "entry": sig.entry,
            "tp2": sig.tp2,
            "sl": sig.sl,
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "score": sig.score,
            "signal_type": sig.signal_type,
            "regime": sig.regime,
        })
        append_jsonl(SIGNAL_LOG_FILE, {
            "event": "signal_opened",
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "direction": direction,
            "signal_type": sig.signal_type,
            "entry": sig.entry,
            "entry_zone": [sig.entry_low, sig.entry_high],
            "tp1": sig.tp1,
            "tp2": sig.tp2,
            "sl": sig.sl,
            "score": sig.score,
            "regime": sig.regime,
            "rr": sig.rr,
            "sl_pct": sig.sl_pct,
            "atr_pct": sig.atr_pct,
            "leverage": sig.leverage_hint,
        })
        signals_fired += 1
        time.sleep(0.5)   # gentle rate limiting between TG sends

    save_state(state)
    print(f"Scan complete. {signals_fired} signal(s) fired.")



if __name__ == "__main__":
    main()
