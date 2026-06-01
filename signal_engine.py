"""
Scalp Swing Bot v10 – Python Signal Engine
Timeframe map: 4H bias / 1H middle / 15m execution
Runs on GitHub Actions every 15 min, sends Telegram alerts.
No orders are placed — signal only.
"""

import os, json, time, math, hmac, hashlib, requests
from datetime import datetime, timezone
from pathlib import Path

# ── CONFIG (from GitHub Secrets / env vars) ─────────────────────
TG_BOT_TOKEN  = os.environ["TG_BOT_TOKEN"]
TG_CHAT_ID    = os.environ["TG_CHAT_ID"]
STATE_FILE    = "state.json"          # persisted in repo via commit or artifact

# ── INDICATOR LENGTHS (mirror Pine inputs) ───────────────────────
FAST_LEN   = 21
SLOW_LEN   = 50
TREND_LEN  = 200
RSI_LEN    = 14
ATR_LEN    = 14
ADX_LEN    = 14
BB_LEN     = 20
BB_MULT    = 2.0
VOL_LEN    = 20
OBV_LEN    = 3

# ── RISK / SCORE ─────────────────────────────────────────────────
MIN_SCORE       = 4
TP1_MULT        = 1.0
TP2_MULT        = 1.5
SL_MULT         = 1.0
COOLDOWN_BARS   = 32    # ~8h on 15m
GLOBAL_COOLDOWN = 16    # ~4h on 15m

# ── FILTERS ──────────────────────────────────────────────────────
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
USE_ROLLING_VWAP     = True
ROLLING_VWAP_LEN     = 16   # ~4H session on 15m
USE_D200_FILTER      = True
USE_DAILY_ADX        = True
MIN_DAILY_ADX        = 20.0
PULL_REQUIRES_4H     = True

# ── VOLUME FILTER ─────────────────────────────────────────────────
MIN_24H_VOLUME_USD = 5_000_000   # 5M USD / 24h

# ── HYPERLIQUID ENDPOINTS ─────────────────────────────────────────
HL_INFO_URL = "https://api.hyperliquid.xyz/info"


# ═══════════════════════════════════════════════════════════════
# HYPERLIQUID DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def hl_post(payload: dict) -> dict:
    """POST to Hyperliquid info endpoint with retry."""
    for attempt in range(3):
        try:
            r = requests.post(HL_INFO_URL, json=payload, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def get_all_mids() -> dict[str, float]:
    """Return {coin: mid_price} for all perpetuals."""
    data = hl_post({"type": "allMids"})
    return {k: float(v) for k, v in data.items()}


def get_meta_and_24h() -> list[dict]:
    """Return metaAndAssetCtxs — includes 24h volume per asset."""
    data = hl_post({"type": "metaAndAssetCtxs"})
    # data[0] = universe meta, data[1] = asset contexts
    meta    = data[0]["universe"]
    ctx_arr = data[1]
    result  = []
    for i, asset in enumerate(meta):
        ctx = ctx_arr[i] if i < len(ctx_arr) else {}
        result.append({
            "coin":        asset["name"],
            "dayNtlVlm":   float(ctx.get("dayNtlVlm", 0)),   # 24h notional volume USD
            "markPx":      float(ctx.get("markPx", 0)),
        })
    return result


def get_candles(coin: str, interval: str, n: int) -> list[dict]:
    """
    Fetch last n closed candles for coin on interval.
    interval examples: "15m", "1h", "4h", "1d"
    Returns list of dicts: {t, o, h, l, c, v}  (newest last)
    """
    end_ms   = int(time.time() * 1000)
    # fetch extra buffer so we always get n confirmed-closed bars
    minutes  = {"1m":1,"3m":3,"5m":5,"15m":15,"30m":30,
                "1h":60,"2h":120,"4h":240,"1d":1440}[interval]
    start_ms = end_ms - (n + 5) * minutes * 60 * 1000

    raw = hl_post({
        "type":        "candleSnapshot",
        "req": {
            "coin":       coin,
            "interval":   interval,
            "startTime":  start_ms,
            "endTime":    end_ms,
        }
    })
    candles = []
    for c in raw:
        candles.append({
            "t": int(c["t"]),
            "o": float(c["o"]),
            "h": float(c["h"]),
            "l": float(c["l"]),
            "c": float(c["c"]),
            "v": float(c["v"]),
        })
    # sort ascending, drop last (potentially open bar)
    candles.sort(key=lambda x: x["t"])
    return candles[:-1][-n:]   # take last n closed bars


def get_filtered_coins() -> list[str]:
    """Return coins with 24h notional volume >= MIN_24H_VOLUME_USD."""
    assets = get_meta_and_24h()
    return [a["coin"] for a in assets if a["dayNtlVlm"] >= MIN_24H_VOLUME_USD]


# ═══════════════════════════════════════════════════════════════
# INDICATOR MATH
# ═══════════════════════════════════════════════════════════════

def ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return [float("nan")] * len(values)
    k = 2.0 / (period + 1)
    result = [float("nan")] * len(values)
    # seed with SMA
    seed_idx = period - 1
    result[seed_idx] = sum(values[:period]) / period
    for i in range(seed_idx + 1, len(values)):
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
    # Wilder smooth
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    rs = avg_g / avg_l if avg_l != 0 else float("inf")
    result[period] = 100 - 100 / (1 + rs)
    for i in range(period + 1, len(closes)):
        idx_g = i - 1
        avg_g = (avg_g * (period - 1) + gains[idx_g]) / period
        avg_l = (avg_l * (period - 1) + losses[idx_g]) / period
        rs = avg_g / avg_l if avg_l != 0 else float("inf")
        result[i] = 100 - 100 / (1 + rs)
    return result


def atr(highs, lows, closes, period: int) -> list[float]:
    trs = [float("nan")]
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i - 1]),
                 abs(lows[i]  - closes[i - 1]))
        trs.append(tr)
    result = [float("nan")] * len(closes)
    if len(trs) < period + 1:
        return result
    seed = sum(trs[1:period + 1]) / period
    result[period] = seed
    for i in range(period + 1, len(trs)):
        result[i] = (result[i - 1] * (period - 1) + trs[i]) / period
    return result


def bollinger(closes, period: int, mult: float):
    basis_arr  = [float("nan")] * len(closes)
    upper_arr  = [float("nan")] * len(closes)
    lower_arr  = [float("nan")] * len(closes)
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1: i + 1]
        m = sum(window) / period
        sd = math.sqrt(sum((x - m) ** 2 for x in window) / period)
        basis_arr[i] = m
        upper_arr[i] = m + mult * sd
        lower_arr[i] = m - mult * sd
    return basis_arr, upper_arr, lower_arr


def adx_dmi(highs, lows, closes, period: int):
    """Returns (plus_di, minus_di, adx) as lists."""
    n = len(closes)
    plus_dm  = [0.0] * n
    minus_dm = [0.0] * n
    tr_arr   = [0.0] * n
    for i in range(1, n):
        up   = highs[i]  - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm[i]  = up   if (up > down and up > 0)   else 0
        minus_dm[i] = down if (down > up and down > 0) else 0
        tr_arr[i]   = max(highs[i] - lows[i],
                          abs(highs[i] - closes[i - 1]),
                          abs(lows[i]  - closes[i - 1]))

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

    # Wilder-smooth DX into ADX
    first_dx_idx = next((i for i in range(period, n) if not math.isnan(dx_arr[i])), None)
    if first_dx_idx is None:
        return di_plus, di_minus, adx_arr
    seed_end = first_dx_idx + period
    if seed_end > n:
        return di_plus, di_minus, adx_arr
    valid_dx = [dx_arr[i] for i in range(first_dx_idx, seed_end) if not math.isnan(dx_arr[i])]
    if len(valid_dx) < period:
        return di_plus, di_minus, adx_arr
    adx_arr[seed_end - 1] = sum(valid_dx) / period
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
    pv   = [c * v for c, v in zip(closes, volumes)]
    spv  = sma(pv,      period)
    sv   = sma(volumes, period)
    result = [float("nan")] * len(closes)
    for i in range(len(closes)):
        if not math.isnan(spv[i]) and sv[i] != 0:
            result[i] = spv[i] / sv[i]
    return result


def safe(v, fallback=0.0):
    return fallback if (v is None or math.isnan(v)) else v


# ═══════════════════════════════════════════════════════════════
# SIGNAL LOGIC  (translated from Pine Script v10)
# ═══════════════════════════════════════════════════════════════

class SignalResult:
    def __init__(self):
        self.fire_long  = False
        self.fire_short = False
        self.signal_type = ""   # "BREAK" or "PULL"
        self.score       = 0
        self.entry = self.tp1 = self.tp2 = self.sl = 0.0
        self.atr_val = 0.0
        self.breakdown = ""
        self.v10_gates = ""


def compute_signals(coin: str, candles_15m, candles_1h, candles_4h, candles_d) -> SignalResult:
    """
    Run full v10 indicator stack on the supplied candle arrays.
    All arrays are lists of dicts {o,h,l,c,v}, oldest-first, last bar = confirmed close.
    """
    res = SignalResult()

    def arrays(candles):
        o = [c["o"] for c in candles]
        h = [c["h"] for c in candles]
        l = [c["l"] for c in candles]
        c = [c["c"] for c in candles]
        v = [c["v"] for c in candles]
        return o, h, l, c, v

    # ── 15m (execution TF) ────────────────────────────────────
    o15, h15, l15, c15, v15 = arrays(candles_15m)
    ema_f15 = ema(c15, FAST_LEN);  ef15 = safe(ema_f15[-1])
    ema_s15 = ema(c15, SLOW_LEN);  es15 = safe(ema_s15[-1])
    ema_t15 = ema(c15, TREND_LEN); et15 = safe(ema_t15[-1])
    rsi15   = rsi(c15, RSI_LEN);   r15  = safe(rsi15[-1])
    atr15   = atr(h15, l15, c15, ATR_LEN); a15 = safe(atr15[-1])
    _, _, adx15_arr = adx_dmi(h15, l15, c15, ADX_LEN)
    adx15 = safe(adx15_arr[-1], 25.0)
    bb_b15, bb_u15, bb_l15 = bollinger(c15, BB_LEN, BB_MULT)
    bb_basis = safe(bb_b15[-1])
    vol_ma15 = sma(v15, VOL_LEN);  vm15 = safe(vol_ma15[-1])
    obv15    = obv(c15, v15)
    rvwap15  = rolling_vwap(c15, v15, ROLLING_VWAP_LEN)

    cur_c  = c15[-1];  cur_o = o15[-1]
    cur_h  = h15[-1];  cur_l = l15[-1]
    cur_v  = v15[-1]
    prev_l = l15[-2];  prev_h = h15[-2]

    # ATR fallback
    atr_val  = a15 if a15 > 0 else (cur_c * 0.03 / 100)
    atr_pct  = atr_val / cur_c * 100
    market_ok = (MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT)

    # Rolling VWAP filter
    rv = safe(rvwap15[-1])
    vwap_long  = cur_c > rv if USE_ROLLING_VWAP else True
    vwap_short = cur_c < rv if USE_ROLLING_VWAP else True

    # RSI from 15m
    rsi_long  = RSI_LONG_MIN  <= r15 <= RSI_LONG_MAX
    rsi_short = RSI_SHORT_MIN <= r15 <= RSI_SHORT_MAX

    # BB midline
    bb_long  = cur_c > bb_basis
    bb_short = cur_c < bb_basis

    # OBV slope
    obv_slope_long  = obv15[-1] > obv15[-1 - OBV_LEN]
    obv_slope_short = obv15[-1] < obv15[-1 - OBV_LEN]

    # ADX score
    adx_score_ok = adx15 >= ADX_SCORE_MIN

    # Volume score
    vol_score_ok = True if vm15 == 0 else (cur_v >= vm15 * VOL_SCORE_MULT)

    # ── 1H (middle TF) ───────────────────────────────────────
    o1h, h1h, l1h, c1h, v1h = arrays(candles_1h)
    ema_f1h = ema(c1h, FAST_LEN); ef1h = safe(ema_f1h[-1])
    ema_s1h = ema(c1h, SLOW_LEN); es1h = safe(ema_s1h[-1])
    _, _, adx1h_arr = adx_dmi(h1h, l1h, c1h, ADX_LEN)
    adx1h   = safe(adx1h_arr[-1], 25.0)
    rsi1h   = rsi(c1h, RSI_LEN);  r1h = safe(rsi1h[-1])
    atr1h   = atr(h1h, l1h, c1h, ATR_LEN); a1h = safe(atr1h[-1])
    h1_bull = ef1h > es1h
    h1_bear = ef1h < es1h

    # ── 4H (bias TF) ─────────────────────────────────────────
    o4h, h4h, l4h, c4h, v4h = arrays(candles_4h)
    ema_f4h = ema(c4h, FAST_LEN); ef4h_arr = ema_f4h
    ema_s4h = ema(c4h, SLOW_LEN); es4h_arr = ema_s4h
    ef4h = safe(ef4h_arr[-1]); es4h = safe(es4h_arr[-1])
    _, _, adx4h_arr = adx_dmi(h4h, l4h, c4h, ADX_LEN)
    adx4h = safe(adx4h_arr[-1], 25.0)
    d_bull = ef4h > es4h
    d_bear = ef4h < es4h

    # 4H trend hold (trendHoldBars consecutive bars)
    def trend_held(ef_arr, es_arr, bull: bool) -> bool:
        for offset in range(1, TREND_HOLD_BARS + 1):
            idx = -offset
            if bull:
                if safe(ef_arr[idx]) <= safe(es_arr[idx]):
                    return False
            else:
                if safe(ef_arr[idx]) >= safe(es_arr[idx]):
                    return False
        return True

    d_trend_held_bull = trend_held(ef4h_arr, es4h_arr, True)
    d_trend_held_bear = trend_held(ef4h_arr, es4h_arr, False)

    # ── Daily 200 EMA macro filter (kept as Daily) ────────────
    if candles_d and len(candles_d) >= TREND_LEN:
        _, _, _, c_d, _ = arrays(candles_d)
        ema_d200 = ema(c_d, TREND_LEN)
        d200 = safe(ema_d200[-1])
    else:
        d200 = cur_c  # fallback: neutral
    macro_long  = cur_c > d200 if USE_D200_FILTER else True
    macro_short = cur_c < d200 if USE_D200_FILTER else True

    # ── [FIX 2] 4H ADX gate ───────────────────────────────────
    daily_adx_ok = adx4h >= MIN_DAILY_ADX if USE_DAILY_ADX else True

    # ── [FIX 3] PULL alignment ────────────────────────────────
    h4_bull = h1_bull   # 1H is the "middle" bridge in this TF version
    h4_bear = h1_bear

    # 3-TF alignment
    full_long_align  = d_bull and d_trend_held_bull and h4_bull and h1_bull
    full_short_align = d_bear and d_trend_held_bear and h4_bear and h1_bear

    pull_long_align  = (d_bull and d_trend_held_bull and h4_bull and h1_bull) \
                        if PULL_REQUIRES_4H else \
                       (d_bull and d_trend_held_bull and h1_bull)
    pull_short_align = (d_bear and d_trend_held_bear and h4_bear and h1_bear) \
                        if PULL_REQUIRES_4H else \
                       (d_bear and d_trend_held_bear and h1_bear)

    # ── Wick rejection ────────────────────────────────────────
    rng = cur_h - cur_l + 1e-10

    def clean_bull_bar():
        up_wick = cur_h - max(cur_o, cur_c)
        return up_wick / rng <= WICK_FILTER

    def clean_bear_bar():
        dn_wick = min(cur_o, cur_c) - cur_l
        return dn_wick / rng <= WICK_FILTER

    # ── Range position (BREAK only) ───────────────────────────
    def close_in_top_range():
        return (cur_c - cur_l) / rng >= (1.0 - RANGE_PCT_BREAK)

    def close_in_bot_range():
        return (cur_c - cur_l) / rng <= RANGE_PCT_BREAK

    # ── BREAK candle checks ───────────────────────────────────
    adx_break_ok    = adx15 >= ADX_BREAK_GATE
    break_bull_bar  = (cur_c > cur_o and clean_bull_bar() and close_in_top_range())
    break_bear_bar  = (cur_c < cur_o and clean_bear_bar() and close_in_bot_range())

    # ── PULL touch / recovery ─────────────────────────────────
    pull_zone = atr_val * PULL_ZONE_MULT
    pull_touched_long  = prev_l <= ef15 + pull_zone
    pull_touched_short = prev_h >= ef15 - pull_zone
    pull_recover_long  = cur_c > ef15 and cur_c > cur_o
    pull_recover_short = cur_c < ef15 and cur_c < cur_o
    pull_bull_bar = cur_c > cur_o and clean_bull_bar()
    pull_bear_bar = cur_c < cur_o and clean_bear_bar()

    # ── SCORING ───────────────────────────────────────────────
    long_score  = sum([
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

    # ── BREAK signal ──────────────────────────────────────────
    long_break  = (macro_long  and daily_adx_ok and full_long_align  and break_bull_bar
                   and adx_break_ok and vwap_long  and rsi_long  and long_score  >= MIN_SCORE and market_ok)
    short_break = (macro_short and daily_adx_ok and full_short_align and break_bear_bar
                   and adx_break_ok and vwap_short and rsi_short and short_score >= MIN_SCORE and market_ok)

    # ── PULL signal ───────────────────────────────────────────
    long_pull  = (macro_long  and daily_adx_ok and pull_long_align  and pull_touched_long
                  and pull_recover_long  and pull_bull_bar and vwap_long  and rsi_long
                  and long_score  >= MIN_SCORE and market_ok)
    short_pull = (macro_short and daily_adx_ok and pull_short_align and pull_touched_short
                  and pull_recover_short and pull_bear_bar and vwap_short and rsi_short
                  and short_score >= MIN_SCORE and market_ok)

    long_sig  = long_break  or long_pull
    short_sig = short_break or short_pull

    if long_sig:
        res.fire_long   = True
        res.signal_type = "BREAK" if long_break else "PULL"
        res.score       = long_score
        res.entry       = cur_c
        res.tp1         = cur_c + atr_val * TP1_MULT
        res.tp2         = cur_c + atr_val * TP2_MULT
        res.sl          = cur_c - atr_val * SL_MULT
        res.atr_val     = atr_val
        bb_s  = "BB✓" if bb_long  else "BB✗"
        d_s   = "4H✓" if (d_bull and d_trend_held_bull) else "4H✗"
        adx_s = "ADX✓" if adx_score_ok else "ADX✗"
        obv_s = "OBV✓" if obv_slope_long else "OBV✗"
        vol_s = "VOL✓" if vol_score_ok else "VOL✗"
        res.breakdown = f"{bb_s} {d_s} {adx_s} {obv_s} {vol_s}"
        res.v10_gates = (f"D200:{'✓' if macro_long else '✗'} "
                         f"4HADX:{'✓' if daily_adx_ok else '✗'} "
                         f"1H:{'✓' if h1_bull else '✗'}")

    elif short_sig:
        res.fire_short  = True
        res.signal_type = "BREAK" if short_break else "PULL"
        res.score       = short_score
        res.entry       = cur_c
        res.tp1         = cur_c - atr_val * TP1_MULT
        res.tp2         = cur_c - atr_val * TP2_MULT
        res.sl          = cur_c + atr_val * SL_MULT
        res.atr_val     = atr_val
        bb_s  = "BB✓" if bb_short  else "BB✗"
        d_s   = "4H✓" if (d_bear and d_trend_held_bear) else "4H✗"
        adx_s = "ADX✓" if adx_score_ok else "ADX✗"
        obv_s = "OBV✓" if obv_slope_short else "OBV✗"
        vol_s = "VOL✓" if vol_score_ok else "VOL✗"
        res.breakdown = f"{bb_s} {d_s} {adx_s} {obv_s} {vol_s}"
        res.v10_gates = (f"D200:{'✓' if macro_short else '✗'} "
                         f"4HADX:{'✓' if daily_adx_ok else '✗'} "
                         f"1H:{'✓' if h1_bear else '✗'}")

    return res


# ═══════════════════════════════════════════════════════════════
# COOLDOWN STATE  (persisted as JSON artifact in GitHub Actions)
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


def check_cooldown(state: dict, coin: str, direction: str, bar_index: int) -> bool:
    """Returns True if signal is allowed (cooldown cleared)."""
    key_dir    = f"{coin}_{direction}"
    key_global = f"{coin}_any"
    last_dir    = state.get(key_dir,    -9999)
    last_global = state.get(key_global, -9999)
    dir_ok    = (bar_index - last_dir)    >= COOLDOWN_BARS
    global_ok = (bar_index - last_global) >= GLOBAL_COOLDOWN
    return dir_ok and global_ok


def update_cooldown(state: dict, coin: str, direction: str, bar_index: int):
    state[f"{coin}_{direction}"] = bar_index
    state[f"{coin}_any"]         = bar_index


# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def stars(score: int) -> str:
    filled = "★" * score
    empty  = "☆" * (5 - score)
    return filled + empty


def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TG_CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
    }
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=10)
            r.raise_for_status()
            return
        except Exception as e:
            if attempt == 2:
                print(f"[TG ERROR] {e}")
            time.sleep(2)


def format_signal(coin: str, sig: SignalResult) -> str:
    direction = "▲ LONG" if sig.fire_long else "▼ SHORT"
    emoji     = "🟢" if sig.fire_long else "🔴"
    ts        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def fmt(v):
        if v >= 1000:
            return f"{v:,.2f}"
        if v >= 1:
            return f"{v:.4f}"
        return f"{v:.6f}"

    msg = (
        f"{emoji} <b>{direction} [{sig.signal_type}]</b>  {stars(sig.score)}\n"
        f"<b>Coin:</b> {coin}-PERP\n"
        f"<b>Entry:</b> {fmt(sig.entry)}\n"
        f"<b>TP1:</b>   {fmt(sig.tp1)}\n"
        f"<b>TP2:</b>   {fmt(sig.tp2)}\n"
        f"<b>SL:</b>    {fmt(sig.sl)}\n"
        f"<b>Score:</b> {sig.score}/5  |  {sig.breakdown}\n"
        f"<b>Gates:</b> {sig.v10_gates}\n"
        f"<i>Scalp Swing v10 [4H/15m] • {ts}</i>"
    )
    return msg


# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Scanner starting…")

    # --- Volume-filtered coin list ---
    print("Fetching 24h volume data…")
    coins = get_filtered_coins()
    print(f"Coins passing >{MIN_24H_VOLUME_USD/1e6:.0f}M volume filter: {len(coins)} → {coins}")

    if not coins:
        print("No coins pass volume filter. Exiting.")
        return

    # --- Bar index proxy (unix minutes / 15) ---
    bar_index_now = int(time.time() // (15 * 60))

    state = load_state()
    signals_fired = 0

    for coin in coins:
        try:
            print(f"  Processing {coin}…")

            # Fetch candle data for all required timeframes
            # Need enough bars for longest indicator (200 EMA = 200 bars minimum)
            n_15m = max(300, TREND_LEN + 50)
            n_1h  = max(300, TREND_LEN + 50)
            n_4h  = max(300, TREND_LEN + 50)
            n_d   = max(250, TREND_LEN + 50)

            candles_15m = get_candles(coin, "15m", n_15m)
            candles_1h  = get_candles(coin, "1h",  n_1h)
            candles_4h  = get_candles(coin, "4h",  n_4h)
            candles_d   = get_candles(coin, "1d",  n_d)

            if len(candles_15m) < 50:
                print(f"    Skipping {coin}: insufficient 15m candles ({len(candles_15m)})")
                continue

            sig = compute_signals(coin, candles_15m, candles_1h, candles_4h, candles_d)

            if sig.fire_long or sig.fire_short:
                direction = "long" if sig.fire_long else "short"

                # Check cooldown
                if not check_cooldown(state, coin, direction, bar_index_now):
                    print(f"    {coin} {direction.upper()} signal suppressed by cooldown")
                    continue

                # Fire!
                msg = format_signal(coin, sig)
                print(f"    🚀 SIGNAL: {coin} {direction.upper()} [{sig.signal_type}] score={sig.score}")
                send_telegram(msg)
                update_cooldown(state, coin, direction, bar_index_now)
                signals_fired += 1

                # Throttle between messages
                time.sleep(1)
            else:
                print(f"    {coin}: no signal")

        except Exception as e:
            print(f"    ERROR processing {coin}: {e}")
            continue

    save_state(state)
    print(f"Scan complete. {signals_fired} signal(s) fired.")


if __name__ == "__main__":
    main()
