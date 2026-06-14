"""
Scalp Swing Bot v10 – Python Signal Engine  (v2 — Pine parity fixes)
Hyperliquid Perpetuals edition
Timeframe map: 4H bias / 1H middle / 15m execution
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

REACTION FEATURE
────────────────────────────────────────────
On every run, active signals are checked against 15m candles fetched
from the signal's bar time up to now. Each candle's high/low is walked
oldest→newest to correctly resolve TP1→TP2 vs SL hit order, avoiding
false 😭 reactions when TP1 is hit first within the same 15m window.

  🔥  when TP1 is hit before SL
  🏆  when TP2 is also hit (full winner)
  😭 when SL is hit before TP1
Signal tracking state is persisted in state.json under "active_signals".
Signals are auto-expired after SIGNAL_MAX_AGE_BARS (≈12 h by default).
"""

import os, json, time, math, random, threading, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# ── CONFIG ───────────────────────────────────────────────────
TG_BOT_TOKEN          = os.environ["TG_BOT_TOKEN"]
TG_CHAT_ID            = os.environ["TG_CHAT_ID"]
STATE_FILE            = "state.json"
SCAN_WORKERS          = int(os.getenv("SCAN_WORKERS", "2"))
HL_MIN_INTERVAL_S     = float(os.getenv("HL_MIN_INTERVAL_S", "0.18"))
HL_MIN_INTERVAL_MAX_S = float(os.getenv("HL_MIN_INTERVAL_MAX_S", "0.60"))
HL_TF_WORKERS         = int(os.getenv("HL_TF_WORKERS", "1"))

_hl_request_lock    = threading.Lock()
_hl_last_request_ts = 0.0
_hl_min_interval_s  = HL_MIN_INTERVAL_S

_hl_session = requests.Session()

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
    "UNIUSDT",
    "LTCUSDT",
    "APTUSDT",
    "PENDLEUSDT",
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
MAX_SIGNALS_PER_SCAN = 2   # top N signals to fire per scan; rest are dropped

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

# ── ACCURACY FILTERS (v3) ─────────────────────────────────────
# Suppress signal when funding headwind exceeds this per-8h rate.
# Set to None to disable. Default: 0.10% (same as FUNDING_WARN_EXTREME).
FUNDING_SUPPRESS_EXTREME: float = 0.0010

# Minimum ATR-multiples of clearance required between entry and the
# nearest S/R level in the trade direction.
# e.g. 0.5 = resistance must be at least 0.5×ATR above entry for longs.
# Set to 0.0 to disable.
SR_CLEARANCE_ATR_MULT: float = 0.5

# OI trend: number of 4H candles to compare volume over to judge rising/falling.
# Rising 4H volume = confirmation of OI expansion behind the move.
OI_TREND_BARS: int = 3

# ── CANDLE COUNTS PER TIMEFRAME ──────────────────────────────
# 15m: full 300 needed for indicators (ADX, BB, OBV, EMA200 not used here)
# 1H/4H: only need EMA21/50 + ADX14 — 60 candles is plenty
# Daily: needs EMA200 + small buffer
N_15M = 300
N_1H  = 60
N_4H  = 60
N_1D  = 210

# ── REACTION SETTINGS ────────────────────────────────────────
REACT_TP1          = "🔥"   # TP1 hit before SL
REACT_TP2          = "🏆"   # TP2 hit (full winner)
REACT_SL           = "😭"   # SL hit before TP1
SIGNAL_MAX_AGE_BARS = 48    # auto-expire after ~12 h of 15m bars (12hr × 60min ÷ 15min = 48 bars)

# ── HYPERLIQUID ENDPOINT ──────────────────────────────────────
HL_INFO_URL = "https://api.hyperliquid.xyz/info"

# Interval → milliseconds (for startTime calculation)
INTERVAL_MS = {
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
    """POST to Hyperliquid /info with retry + 429 backoff."""
    max_attempts  = int(os.getenv("HL_MAX_ATTEMPTS", "6"))
    base_sleep_s  = float(os.getenv("HL_BASE_SLEEP_S", "0.75"))
    for attempt in range(max_attempts):
        try:
            global _hl_last_request_ts
            global _hl_min_interval_s
            # Global rate limiter across all threads.
            # This avoids request bursts that trigger Hyperliquid 429s.
            with _hl_request_lock:
                now     = time.time()
                elapsed = now - _hl_last_request_ts
                wait_s  = _hl_min_interval_s - elapsed
                if wait_s > 0:
                    time.sleep(wait_s)
                # Mark request time immediately so other threads respect spacing.
                _hl_last_request_ts = time.time()

            r = _hl_session.post(
                HL_INFO_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            if r.status_code == 429:
                # If we hit rate-limit, slow down the global throttle immediately.
                with _hl_request_lock:
                    _hl_min_interval_s = min(
                        HL_MIN_INTERVAL_MAX_S, _hl_min_interval_s * 1.25 + 0.02
                    )
                retry_after = r.headers.get("Retry-After")
                try:
                    retry_after_s = float(retry_after) if retry_after is not None else None
                except ValueError:
                    retry_after_s = None
                sleep_s = retry_after_s if retry_after_s is not None else (base_sleep_s * (2 ** attempt))
                # Cap 429 delay so the workflow doesn't run for too long.
                sleep_s = min(20.0, max(base_sleep_s, sleep_s)) + random.uniform(0.0, 0.35)
                time.sleep(sleep_s)
                continue
            r.raise_for_status()

            # On success, cautiously speed up over time (only a tiny amount each success).
            with _hl_request_lock:
                _hl_min_interval_s = max(HL_MIN_INTERVAL_S, _hl_min_interval_s - 0.0025)
            return r.json()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            sleep_s = min(20.0, base_sleep_s * (2 ** attempt)) + random.uniform(0.0, 0.25)
            time.sleep(sleep_s)


def get_candles(symbol: str, interval: str, n: int, start_time_ms: int | None = None) -> list[dict]:
    """
    Fetch last n closed candles for symbol on interval via Hyperliquid.
    Returns list of dicts: {t, o, h, l, c, v}  newest last.
    Hyperliquid candleSnapshot supports up to 5000 candles.

    If start_time_ms is provided it is used directly as the startTime in
    the Hyperliquid payload, and n is ignored for the time window
    calculation (though the final slice still caps results at n candles).
    This lets check_active_signals() fetch only the candles that have
    closed since a signal fired, without over-fetching.
    """
    coin   = hl_coin(symbol)
    iv_ms  = INTERVAL_MS.get(interval, 60 * 60 * 1000)
    end_ms = int(time.time() * 1000)

    if start_time_ms is not None:
        # Caller supplied an explicit window start — use it directly.
        computed_start_ms = start_time_ms
    else:
        # Default: back-calculate from candle count with a small safety buffer.
        computed_start_ms = end_ms - iv_ms * (n + 10)

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin":      coin,
            "interval":  interval,
            "startTime": computed_start_ms,
            "endTime":   end_ms,
        },
    }

    raw = hl_post(payload)

    candles = []
    for c in raw:
        candles.append({
            "t":  int(c["t"]),
            "o":  float(c["o"]),
            "h":  float(c["h"]),
            "l":  float(c["l"]),
            "c":  float(c["c"]),
            "v":  float(c["v"]),
            "qv": float(c["v"]),   # HL returns base volume; reuse for qv
        })

    # Drop the last (possibly still-open) candle, return the most recent n.
    # When start_time_ms is provided n acts only as a safety cap.
    return candles[:-1][-n:]


def fetch_all_candles(symbol: str) -> tuple[list, list, list, list] | None:
    """
    Fetch all 4 timeframes concurrently for one symbol.
    Returns (candles_15m, candles_1h, candles_4h, candles_d)
    or None if 15m data is insufficient.
    """
    # First fetch 15m alone so we can bail early without wasting the other 3 calls
    candles_15m = get_candles(symbol, "15m", N_15M)
    if len(candles_15m) < 50:
        return None

    # Now fetch the remaining 3 TFs in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=max(1, HL_TF_WORKERS)) as ex:
        futures = {
            ex.submit(get_candles, symbol, "1h", N_1H): "1h",
            ex.submit(get_candles, symbol, "4h", N_4H): "4h",
            ex.submit(get_candles, symbol, "1d", N_1D): "1d",
        }
        for fut in as_completed(futures):
            tf = futures[fut]
            results[tf] = fut.result()   # propagates exceptions naturally

    return candles_15m, results["1h"], results["4h"], results["1d"]


# ═══════════════════════════════════════════════════════════════
# FUNDING RATE + SUPPORT / RESISTANCE
# ═══════════════════════════════════════════════════════════════

# Thresholds for funding rate warnings (per 8h, as a decimal)
FUNDING_WARN_EXTREME   = 0.0010   # ±0.10% — strong headwind, flag it
FUNDING_WARN_HIGH      = 0.0005   # ±0.05% — elevated, note it

def get_market_context(symbol: str) -> dict | None:
    """
    Fetch funding rate + open interest for a Hyperliquid perpetual in one
    API call (metaAndAssetCtxs).

    Returns a dict:
      {
        "funding": float,       # per-8h rate as decimal (e.g. 0.0001 = 0.01%)
        "open_interest": float, # OI in USD notional
      }
    or None on failure.
    """
    coin = hl_coin(symbol)
    try:
        payload = {"type": "metaAndAssetCtxs"}
        data = hl_post(payload)
        # Response: [meta_dict, [assetCtx, ...]]
        meta       = data[0]
        asset_ctxs = data[1]
        universe   = meta.get("universe", [])
        for i, asset in enumerate(universe):
            if asset.get("name", "") == coin:
                ctx = asset_ctxs[i]
                funding = float(ctx["funding"])         if ctx.get("funding")         is not None else None
                oi      = float(ctx["openInterest"])    if ctx.get("openInterest")    is not None else None
                # OI from HL is in coin units; multiply by mark price to get USD notional
                mark    = float(ctx["markPx"])          if ctx.get("markPx")          is not None else None
                oi_usd  = oi * mark if (oi is not None and mark is not None) else None
                return {"funding": funding, "open_interest": oi_usd}
    except Exception as e:
        print(f"  [MARKET CTX] fetch failed for {coin}: {e}")
    return None


def format_funding(rate: float | None, direction: str) -> str:
    """Return a one-line funding rate summary with directional warning."""
    if rate is None:
        return "Funding: n/a"

    pct    = rate * 100
    per_8h = f"{pct:+.4f}%/8h"

    headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
    tailwind = (rate < 0 and direction == "long") or (rate > 0 and direction == "short")

    abs_rate = abs(rate)
    if headwind and abs_rate >= FUNDING_WARN_EXTREME:
        tag = "⚠️ EXTREME — longs paying heavily" if direction == "long" else "⚠️ EXTREME — shorts paying heavily"
    elif headwind and abs_rate >= FUNDING_WARN_HIGH:
        tag = "⚡ elevated against trade"
    elif tailwind and abs_rate >= FUNDING_WARN_HIGH:
        tag = "✅ in your favour"
    else:
        tag = "✅ neutral"

    return f"Funding: {per_8h}  {tag}"


def format_oi(oi_usd: float | None) -> str:
    """Return a compact OI string: e.g. OI: $1.23B  or  OI: $456.7M"""
    if oi_usd is None:
        return "OI: n/a"
    if oi_usd >= 1_000_000_000:
        return f"OI: ${oi_usd / 1_000_000_000:.2f}B"
    if oi_usd >= 1_000_000:
        return f"OI: ${oi_usd / 1_000_000:.1f}M"
    return f"OI: ${oi_usd:,.0f}"


def find_sr_levels(candles_15m: list[dict], n_levels: int = 2) -> tuple[list[float], list[float]]:
    """
    Identify the nearest swing-high (resistance) and swing-low (support)
    levels from recent 15m candles using a simple pivot approach.

    A pivot high  = bar whose high  is higher than both neighbours.
    A pivot low   = bar whose low   is lower  than both neighbours.

    Returns (supports, resistances) — each a list of up to n_levels prices,
    sorted closest-to-current-price first.
    """
    highs = [c["h"] for c in candles_15m]
    lows  = [c["l"] for c in candles_15m]
    cur   = candles_15m[-1]["c"]

    pivots_high = []
    pivots_low  = []

    # Use last 100 bars to keep it relevant; skip the most recent (open) bar
    window = candles_15m[-101:-1]
    for i in range(1, len(window) - 1):
        if window[i]["h"] > window[i-1]["h"] and window[i]["h"] > window[i+1]["h"]:
            pivots_high.append(window[i]["h"])
        if window[i]["l"] < window[i-1]["l"] and window[i]["l"] < window[i+1]["l"]:
            pivots_low.append(window[i]["l"])

    # Keep only levels above/below current price, sort by distance
    resistances = sorted([p for p in pivots_high if p > cur], key=lambda x: x - cur)[:n_levels]
    supports    = sorted([p for p in pivots_low  if p < cur], key=lambda x: cur - x)[:n_levels]

    return supports, resistances


# ═══════════════════════════════════════════════════════════════
# INDICATOR MATH
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
    n        = len(closes)
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
        self.atr_val  = 0.0
        self.breakdown = ""
        self.v10_gates = ""
        self.funding_rate:  float | None = None   # raw per-8h decimal
        self.open_interest: float | None = None   # USD notional OI
        self.supports:    list[float] = []        # nearest pivot lows  (below price)
        self.resistances: list[float] = []        # nearest pivot highs (above price)


def compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d) -> SignalResult:
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
    bb_b15, _, _ = bollinger(c15, BB_LEN, BB_MULT)
    bb_basis = safe(bb_b15[-1])
    vol_ma15 = sma(v15, VOL_LEN);  vm15 = safe(vol_ma15[-1])
    obv15    = obv(c15, v15)
    rvwap15  = rolling_vwap(c15, v15, ROLLING_VWAP_LEN)

    # ── Execution-bar values ──────────────────────────────────
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

    obv_slope_long  = obv15[-1] > obv15[-(1 + OBV_LEN)]
    obv_slope_short = obv15[-1] < obv15[-(1 + OBV_LEN)]

    adx_score_ok = adx15 >= ADX_SCORE_MIN
    vol_score_ok = True if vm15 == 0 else (cur_v >= vm15 * VOL_SCORE_MULT)

    # ── OI trend proxy: rising 4H volume = expanding open interest ──
    # Compare average volume of last OI_TREND_BARS candles vs the OI_TREND_BARS
    # candles before that on the 4H chart. No extra API call needed.
    if len(v4h) >= OI_TREND_BARS * 2:
        recent_vol = sum(v4h[-OI_TREND_BARS:])
        prior_vol  = sum(v4h[-(OI_TREND_BARS * 2):-OI_TREND_BARS])
        oi_expanding = recent_vol > prior_vol
    else:
        oi_expanding = True   # not enough data — don't penalise

    # ── 1H ───────────────────────────────────────────────────
    o1h, h1h, l1h, c1h, v1h = arrays(candles_1h)
    ema_f1h = ema(c1h, FAST_LEN)
    ema_s1h = ema(c1h, SLOW_LEN)
    ef1h = safe(ema_f1h[-2])
    es1h = safe(ema_s1h[-2])
    h1_bull = ef1h > es1h
    h1_bear = ef1h < es1h

    # ── 4H ───────────────────────────────────────────────────
    o4h, h4h, l4h, c4h, v4h = arrays(candles_4h)
    ema_f4h = ema(c4h, FAST_LEN)
    ema_s4h = ema(c4h, SLOW_LEN)
    ef4h = safe(ema_f4h[-2]); es4h = safe(ema_s4h[-2])
    _, _, adx4h_arr = adx_dmi(h4h, l4h, c4h, ADX_LEN)
    adx4h  = safe(adx4h_arr[-2], 25.0)
    d_bull = ef4h > es4h
    d_bear = ef4h < es4h

    def trend_held(ef_arr, es_arr, bull: bool) -> bool:
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
        d200 = safe(ema_d200[-2])
    else:
        d200 = cur_c
    macro_long  = cur_c > d200 if USE_D200_FILTER else True
    macro_short = cur_c < d200 if USE_D200_FILTER else True

    daily_adx_ok = adx4h >= MIN_DAILY_ADX if USE_DAILY_ADX else True

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
    pull_touched_long  = prev_l <= ef15_prev + pull_zone
    pull_touched_short = prev_h >= ef15_prev - pull_zone
    pull_recover_long  = cur_c > ef15 and cur_c > cur_o
    pull_recover_short = cur_c < ef15 and cur_c < cur_o
    pull_bull_bar      = cur_c > cur_o and clean_bull_bar()
    pull_bear_bar      = cur_c < cur_o and clean_bear_bar()

    long_score = sum([
        bb_long,
        d_bull and d_trend_held_bull,
        adx_score_ok,
        obv_slope_long,
        vol_score_ok,
        oi_expanding,   # 4H volume rising = OI conviction
    ])
    short_score = sum([
        bb_short,
        d_bear and d_trend_held_bear,
        adx_score_ok,
        obv_slope_short,
        vol_score_ok,
        oi_expanding,   # 4H volume rising = OI conviction
    ])

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

    long_sig  = long_break  or long_pull
    short_sig = short_break or short_pull

    def make_breakdown(is_long):
        bb_s  = "BB✓"  if (bb_long  if is_long else bb_short)  else "BB✗"
        d_s   = "4H✓"  if (d_bull and d_trend_held_bull if is_long else d_bear and d_trend_held_bear) else "4H✗"
        adx_s = "ADX✓" if adx_score_ok  else "ADX✗"
        obv_s = "OBV✓" if (obv_slope_long if is_long else obv_slope_short) else "OBV✗"
        vol_s = "VOL✓" if vol_score_ok   else "VOL✗"
        oi_s  = "OI✓"  if oi_expanding   else "OI✗"
        return f"{bb_s} {d_s} {adx_s} {obv_s} {vol_s} {oi_s}"

    def make_gates(is_long):
        macro = macro_long if is_long else macro_short
        h1    = h1_bull    if is_long else h1_bear
        return (f"D200:{'✓' if macro else '✗'} "
                f"4HADX:{'✓' if daily_adx_ok else '✗'} "
                f"1H:{'✓' if h1 else '✗'}")

    if long_sig:
        res.fire_long    = True
        res.signal_type  = "BREAK" if long_break else "PULL"
        res.score        = long_score
        res.entry        = cur_c
        zone_half_width  = atr_val * PULL_ZONE_MULT
        res.entry_low    = cur_c - zone_half_width
        res.entry_high   = cur_c + zone_half_width
        res.tp1          = cur_c + atr_val * TP1_MULT
        res.tp2          = cur_c + atr_val * TP2_MULT
        res.sl           = cur_c - atr_val * SL_MULT
        res.atr_val      = atr_val
        res.atr_pct      = atr_pct
        res.breakdown    = make_breakdown(True)
        res.v10_gates    = make_gates(True)
    elif short_sig:
        res.fire_short   = True
        res.signal_type  = "BREAK" if short_break else "PULL"
        res.score        = short_score
        res.entry        = cur_c
        zone_half_width  = atr_val * PULL_ZONE_MULT
        res.entry_low    = cur_c - zone_half_width
        res.entry_high   = cur_c + zone_half_width
        res.tp1          = cur_c - atr_val * TP1_MULT
        res.tp2          = cur_c - atr_val * TP2_MULT
        res.sl           = cur_c + atr_val * SL_MULT
        res.atr_val      = atr_val
        res.atr_pct      = atr_pct
        res.breakdown    = make_breakdown(False)
        res.v10_gates    = make_gates(False)

    # ── Support / Resistance (always compute when a signal fired) ──
    if res.fire_long or res.fire_short:
        res.supports, res.resistances = find_sr_levels(candles_15m)

    # ── S/R clearance filter ──────────────────────────────────────
    # Cancel signal if the nearest opposing S/R is too close to entry.
    # For longs: nearest resistance must be > SR_CLEARANCE_ATR_MULT × ATR above entry.
    # For shorts: nearest support must be > SR_CLEARANCE_ATR_MULT × ATR below entry.
    if SR_CLEARANCE_ATR_MULT > 0 and (res.fire_long or res.fire_short):
        min_clearance = atr_val * SR_CLEARANCE_ATR_MULT
        if res.fire_long and res.resistances:
            nearest_res = res.resistances[0]
            if nearest_res - res.entry < min_clearance:
                print(f"  [SR FILTER] {symbol} LONG suppressed — resistance {nearest_res:.4f} "
                      f"too close to entry {res.entry:.4f} (need {min_clearance:.4f} clearance)")
                res.fire_long = False
        if res.fire_short and res.supports:
            nearest_sup = res.supports[0]
            if res.entry - nearest_sup < min_clearance:
                print(f"  [SR FILTER] {symbol} SHORT suppressed — support {nearest_sup:.4f} "
                      f"too close to entry {res.entry:.4f} (need {min_clearance:.4f} clearance)")
                res.fire_short = False

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


def check_cooldown(state, coin, direction, bar_index) -> bool:
    last_dir    = state.get(f"{coin}_{direction}", -9999)
    last_global = state.get(f"{coin}_any",         -9999)
    return (bar_index - last_dir    >= COOLDOWN_BARS and
            bar_index - last_global >= GLOBAL_COOLDOWN)


def update_cooldown(state, coin, direction, bar_index):
    state[f"{coin}_{direction}"] = bar_index
    state[f"{coin}_any"]         = bar_index


# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def stars(score: int) -> str:
    return "★" * score + "☆" * (6 - score)


def send_telegram(text: str) -> int | None:
    """
    Send a Telegram message and return its message_id.
    Returns None if all attempts fail.
    """
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(url, json={
                "chat_id":    TG_CHAT_ID,
                "text":       text,
                "parse_mode": "HTML",
            }, timeout=10)
            r.raise_for_status()
            return r.json()["result"]["message_id"]
        except Exception as e:
            if attempt == 2:
                print(f"[TG ERROR] {e}")
            time.sleep(2)
    return None


def react_to_message(message_id: int, emoji: str):
    """
    Add a reaction emoji to an existing Telegram message.
    Uses setMessageReaction — requires the bot to be admin in the channel,
    or reactions to be allowed in group settings.
    Replaces any existing reaction on that message.
    """
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    try:
        r = requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "message_id": message_id,
            "reaction":   [{"type": "emoji", "emoji": emoji}],
            "is_big":     False,
        }, timeout=10)
        r.raise_for_status()
        print(f"  [REACT] {emoji} → msg_id {message_id}")
    except Exception as e:
        print(f"  [REACT ERROR] msg_id {message_id}: {e}")


RANK_MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}

def priority_score(sig: SignalResult) -> tuple:
    """
    Returns a sort key (higher = better priority).
    Tiebreaker order:
      1. Raw score (6 = best)
      2. Signal type: BREAK > PULL
      3. Funding tailwind (getting paid to hold)
      4. OI expanding (4H volume rising)
    All components are booleans/ints so the tuple sorts cleanly.
    """
    direction    = "long" if sig.fire_long else "short"
    rate         = sig.funding_rate or 0.0
    tailwind     = (rate < 0 and direction == "long") or (rate > 0 and direction == "short")
    is_break     = sig.signal_type == "BREAK"
    oi_expanding = "OI✓" in sig.breakdown
    return (sig.score, int(is_break), int(tailwind), int(oi_expanding))


def format_signal(symbol: str, sig: SignalResult, engine_tag: str = "V5", rank: int = 0) -> str:
    direction = "▲ LONG" if sig.fire_long else "▼ SHORT"
    dir_str   = "long" if sig.fire_long else "short"
    emoji     = "🟢" if sig.fire_long else "🔴"
    ts        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def fmt(v):
        if v >= 1000: return f"{v:,.2f}"
        if v >= 1:    return f"{v:.4f}"
        return f"{v:.6f}"

    def recommended_leverage(atr_pct: float, score: int) -> str:
        if atr_pct <= 0.60:
            low, high = 8, 12
        elif atr_pct <= 1.20:
            low, high = 5, 10
        elif atr_pct <= 2.00:
            low, high = 3, 7
        else:
            low, high = 2, 5
        if score <= 4:
            high = max(low, high - 2)   # low-confidence: cap leverage
        return f"{low}x - {high}x"

    lev_range = recommended_leverage(sig.atr_pct, sig.score)

    # ── Support / Resistance lines ──────────────────────────────
    sr_lines = ""
    if sig.resistances:
        sr_lines += "🔴 Resistance: " + "  |  ".join(f"<code>{fmt(r)}</code>" for r in sig.resistances) + "\n"
    if sig.supports:
        sr_lines += "🟢 Support:    " + "  |  ".join(f"<code>{fmt(s)}</code>" for s in sig.supports) + "\n"

    # ── Pre-trade checklist ─────────────────────────────────────
    # Trend: 4H + 1H aligned (gates already confirm this if signal fired)
    chk_trend   = "✅" if sig.v10_gates else "☐"
    # S/R marked: we found at least one level on each side
    chk_sr      = "✅" if (sig.supports or sig.resistances) else "☐"
    # Entry signal: score ≥ MIN_SCORE (always true here, but shown for context)
    chk_entry   = "✅"
    # Leverage: always present
    chk_lev     = "✅"
    # Funding: warn if extreme headwind, otherwise OK
    funding_str = format_funding(sig.funding_rate, dir_str)
    rate        = sig.funding_rate
    headwind    = rate is not None and (
        (rate > 0 and dir_str == "long") or (rate < 0 and dir_str == "short")
    )
    chk_funding = "⚠️" if (headwind and rate is not None and abs(rate) >= FUNDING_WARN_EXTREME) else "✅"

    checklist = (
        f"\n<b>Pre-Trade Checklist</b>\n"
        f"{chk_trend} Trend identified (4H/1H/15m aligned)\n"
        f"{chk_sr} Key S/R marked\n"
        f"{chk_entry} Clear entry signal ({sig.signal_type}, score {sig.score}/6)\n"
        f"{chk_lev} Leverage appropriate ({lev_range})\n"
        f"{chk_funding} {funding_str}\n"
        f"📊 {format_oi(sig.open_interest)}"
    )

    sr_block = f"\n{sr_lines}" if sr_lines else ""

    medal      = RANK_MEDALS.get(rank, "")
    rank_tag   = f"{medal} <b>Priority #{rank}</b>\n" if rank else ""

    return (
        f"{rank_tag}{emoji} <b>{direction} [{sig.signal_type}]</b>  {stars(sig.score)}\n"
        f"<b>Pair:</b>  {symbol}\n\n"
        f"<b>Entry:</b> <code>{fmt(sig.entry)}</code>\n"
        f"<b>Entry Zone:</b> <code>{fmt(sig.entry_low)}</code> – <code>{fmt(sig.entry_high)}</code>\n\n"
        f"<b>TP1:</b>   <code>{fmt(sig.tp1)}</code>\n"
        f"<b>TP2:</b>   <code>{fmt(sig.tp2)}</code>\n"
        f"<b>SL:</b>    <code>{fmt(sig.sl)}</code>\n\n"
        f"<b>Leverage:</b> {lev_range}\n"
        f"<b>Score:</b> {sig.score}/6  |  {sig.breakdown}\n"
        f"<b>Gates:</b> {sig.v10_gates}"
        f"{sr_block}"
        f"{checklist}\n\n"
        f"<i>Scalp Swing v10 [4H/15m] • Hyperliquid Perps • {ts}</i>"
    )


# ═══════════════════════════════════════════════════════════════
# ACTIVE SIGNAL TRACKING  (TP / SL reactions)
# ═══════════════════════════════════════════════════════════════

def track_signal(state: dict, symbol: str, direction: str,
                 msg_id: int, sig: SignalResult, bar_index: int):
    """
    Persist a newly fired signal so subsequent runs can poll its TP/SL.
    Each entry stored under state["active_signals"] as:
      {symbol, direction, msg_id, bar_index, signal_bar_time,
       tp1, tp2, sl, tp1_hit, resolved}

    signal_bar_time is a Unix millisecond timestamp recorded at signal
    fire time. check_active_signals() uses it as the startTime for
    candle fetches so only post-signal candles are walked.
    """
    active = state.setdefault("active_signals", [])
    active.append({
        "symbol":          symbol,
        "direction":       direction,
        "msg_id":          msg_id,
        "bar_index":       bar_index,
        "signal_bar_time": int(time.time() * 1000) + 900_000,  # Unix ms; +1 bar so walk starts on the NEXT candle after signal fires
        "tp1":             sig.tp1,
        "tp2":             sig.tp2,
        "sl":              sig.sl,
        "tp1_hit":         False,
        "resolved":        False,
    })


def check_active_signals(state: dict, bar_index_now: int):
    """
    Called once at the start of every run — before the scan loop.

    For each tracked signal, fetch 15m candles from signal_bar_time up
    to now and walk them oldest→newest, checking high/low on every bar.
    This correctly resolves TP1→TP2 hit order even when both levels are
    crossed between two consecutive 15-minute scans.

    Walk rules
    ──────────
    LONG  candle:
      • low  <= sl  AND tp1 not yet hit → 😭 resolve, stop walk
      • high >= tp1 AND tp1 not yet hit → 🔥 set tp1_hit = True
      • high >= tp2 AND tp1 already hit → 🏆 resolve, stop walk

    SHORT candle:
      • high >= sl  AND tp1 not yet hit → 😭 resolve, stop walk
      • low  <= tp1 AND tp1 not yet hit → 🔥 set tp1_hit = True
      • low  <= tp2 AND tp1 already hit → 🏆 resolve, stop walk

    If tp1_hit is already True in state (reacted in a prior scan), the
    walk starts with that state and skips the TP1 check — no duplicate
    🔥 reaction is emitted.

    Signals already resolved or older than SIGNAL_MAX_AGE_BARS are
    dropped. If the candle fetch fails, the signal is kept for retry.

    Resolved signals are appended to state["resolved_signals"] with
    their outcome ("sl", "tp2", or "tp1") before being dropped.
    """
    signals = state.get("active_signals", [])
    if not signals:
        return

    still_active = []

    for sig in signals:
        # ── Drop expired signals ──────────────────────────────
        age = bar_index_now - sig.get("bar_index", bar_index_now)
        if age > SIGNAL_MAX_AGE_BARS:
            print(f"  [TRACK] {sig['symbol']} expired after {age} bars — dropping")
            continue

        # ── Skip already-resolved signals ────────────────────
        if sig.get("resolved", False):
            continue

        symbol    = sig["symbol"]
        direction = sig["direction"]
        msg_id    = sig["msg_id"]
        tp1       = sig["tp1"]
        tp2       = sig["tp2"]
        sl        = sig["sl"]
        tp1_hit   = sig.get("tp1_hit", False)

        # signal_bar_time may be absent on entries created before this
        # version — fall back to fetching a fixed window via N_15M.
        signal_bar_time_ms: int | None = sig.get("signal_bar_time")

        # ── Fetch candles since signal fired ─────────────────
        try:
            candles = get_candles(
                symbol, "15m", N_15M,
                start_time_ms=signal_bar_time_ms,
            )
        except Exception as e:
            print(f"  [TRACK] candle fetch failed for {symbol}: {e}")
            still_active.append(sig)
            continue

        if not candles:
            still_active.append(sig)
            continue

        # ── Walk candles oldest → newest ──────────────────────
        # get_candles() returns newest-last, so index 0 is oldest — correct order.
        for candle in candles:
            c_high = candle["h"]
            c_low  = candle["l"]

            if direction == "long":
                # Priority 1: SL hit before TP1 → 😭
                if not tp1_hit and c_low <= sl:
                    react_to_message(msg_id, REACT_SL)
                    print(f"  [TRACK] {symbol} SL hit → {REACT_SL}")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "sl",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break

                # Priority 2: TP1 hit for the first time → 🔥
                if not tp1_hit and c_high >= tp1:
                    react_to_message(msg_id, REACT_TP1)
                    print(f"  [TRACK] {symbol} TP1 hit → {REACT_TP1}")
                    tp1_hit        = True
                    sig["tp1_hit"] = True

                # Priority 3: TP2 hit after TP1 → 🏆
                if tp1_hit and c_high >= tp2:
                    react_to_message(msg_id, REACT_TP2)
                    print(f"  [TRACK] {symbol} TP2 hit → {REACT_TP2}")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "tp2",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break

                # Priority 4: SL hit after TP1 — resolve silently (no new reaction)
                if tp1_hit and not sig.get("resolved", False) and c_low <= sl:
                    print(f"  [TRACK] {symbol} SL hit after TP1 — resolving silently")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "tp1",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break

            else:  # short
                # Priority 1: SL hit before TP1 → 😭
                if not tp1_hit and c_high >= sl:
                    react_to_message(msg_id, REACT_SL)
                    print(f"  [TRACK] {symbol} SL hit → {REACT_SL}")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "sl",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break

                # Priority 2: TP1 hit for the first time → 🔥
                if not tp1_hit and c_low <= tp1:
                    react_to_message(msg_id, REACT_TP1)
                    print(f"  [TRACK] {symbol} TP1 hit → {REACT_TP1}")
                    tp1_hit        = True
                    sig["tp1_hit"] = True

                # Priority 3: TP2 hit after TP1 → 🏆
                if tp1_hit and c_low <= tp2:
                    react_to_message(msg_id, REACT_TP2)
                    print(f"  [TRACK] {symbol} TP2 hit → {REACT_TP2}")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "tp2",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break

                # Priority 4: SL hit after TP1 — resolve silently (no new reaction)
                if tp1_hit and not sig.get("resolved", False) and c_high >= sl:
                    print(f"  [TRACK] {symbol} SL hit after TP1 — resolving silently")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "tp1",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break

        # ── Keep in list if not yet resolved ──────────────────
        if not sig.get("resolved", False):
            still_active.append(sig)

    state["active_signals"] = still_active


# ═══════════════════════════════════════════════════════════════
# DAILY SUMMARY
# ═══════════════════════════════════════════════════════════════

def should_send_summary() -> bool:
    """
    Returns True once per day at the 08:00 UTC scan.
    Matches any scan running between 08:00 and 08:14 UTC
    so a delayed GitHub Actions run doesn't miss the window.
    """
    now = datetime.now(timezone.utc)
    return now.hour == 8 and now.minute < 15


def send_summary(state: dict):
    """
    Build and send a Telegram summary of signals resolved
    in the last 24 hours, then prune old entries from
    state["resolved_signals"] keeping only the last 48h.

    Format (use exact emoji and layout):

      📊 Signal Summary (last 24h)
      ✅ Winners: N (🔥×A  🏆×B)
      ❌ Losers:  N (😭×C)

    If zero signals were resolved in the last 24h, do not
    send any message at all — return silently.
    """
    cutoff_24h = int(time.time()) - 86400
    cutoff_48h = int(time.time()) - 172800

    recent = [
        e for e in state.get("resolved_signals", [])
        if e["resolved_at"] >= cutoff_24h
    ]

    tp1_count = sum(1 for e in recent if e["outcome"] == "tp1")
    tp2_count = sum(1 for e in recent if e["outcome"] == "tp2")
    sl_count  = sum(1 for e in recent if e["outcome"] == "sl")
    winners   = tp1_count + tp2_count
    losers    = sl_count

    if winners == 0 and losers == 0:
        return

    line1 = "📊 Signal Summary (last 24h)"
    line2 = f"✅ Winners: {winners} (🔥×{tp1_count}  🏆×{tp2_count})"
    line3 = f"❌ Losers:  {losers} (😭×{sl_count})"
    text  = "\n".join([line1, line2, line3])

    send_telegram(text)

    state["resolved_signals"] = [
        e for e in state.get("resolved_signals", [])
        if e["resolved_at"] >= cutoff_48h
    ]


# ═══════════════════════════════════════════════════════════════
# SCAN
# ═══════════════════════════════════════════════════════════════

def scan_symbol(symbol: str, state: dict, bar_index_now: int) -> list[tuple[str, str, str, SignalResult]]:
    """
    Scan a single symbol using the core engine only.
    Returns list of (symbol, formatted_msg, direction, sig).
    """
    coin = hl_coin(symbol)
    data = fetch_all_candles(symbol)
    if data is None:
        print(f"    Skipping {coin}: insufficient candles")
        return []

    candles_15m, candles_1h, candles_4h, candles_d = data
    sig = compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d)
    if not (sig.fire_long or sig.fire_short):
        print(f"    {coin}: no signal")
        return []

    direction = "long" if sig.fire_long else "short"
    if not check_cooldown(state, symbol, direction, bar_index_now):
        print(f"    {coin} signal suppressed by cooldown")
        return []

    print(f"    🚀 SIGNAL: {coin} {direction.upper()} [{sig.signal_type}] score={sig.score}")

    # Fetch funding rate + OI now that we know a signal fired (avoids unnecessary API call)
    ctx = get_market_context(symbol)
    if ctx:
        sig.funding_rate  = ctx.get("funding")
        sig.open_interest = ctx.get("open_interest")
        pct = (sig.funding_rate * 100) if sig.funding_rate is not None else float("nan")
        print(f"    [MARKET CTX] {coin}: funding={pct:+.4f}%/8h  {format_oi(sig.open_interest)}")

    # ── Funding suppression filter ────────────────────────────────
    # Cancel signal if funding is an extreme headwind against the trade.
    if FUNDING_SUPPRESS_EXTREME is not None and sig.funding_rate is not None:
        rate      = sig.funding_rate
        headwind  = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
        if headwind and abs(rate) >= FUNDING_SUPPRESS_EXTREME:
            pct = rate * 100
            print(f"    [FUNDING FILTER] {coin} {direction.upper()} suppressed — "
                  f"funding {pct:+.4f}%/8h is extreme headwind")
            return []

    return [(symbol, format_signal(symbol, sig, "CORE"), direction, sig)]


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Scanner starting…")
    print(f"Watchlist ({len(WATCHLIST)} pairs): {[hl_coin(s) for s in WATCHLIST]}")

    bar_index_now = int(time.time() // (15 * 60))
    state         = load_state()

    # ── Step 1: Check pending signals for TP/SL reactions ────
    print("[TRACK] Checking active signals…")
    check_active_signals(state, bar_index_now)
    save_state(state)   # persist reaction updates immediately

    # ── Step 1.5: Send daily summary if 08:00 UTC window ────
    if should_send_summary():
        send_summary(state)
        save_state(state)   # persist pruned resolved_signals

    # ── Step 2: Scan all symbols in parallel ──────────────────
    pending_signals: list[tuple[str, str, str, SignalResult]] = []
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {
            ex.submit(scan_symbol, sym, state, bar_index_now): sym
            for sym in WATCHLIST
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                result = fut.result()
                if result:
                    pending_signals.extend(result)
            except Exception as e:
                print(f"    ERROR processing {sym}: {e}")

    # ── Step 3: Rank, cap, and send alerts ───────────────────
    # Sort all pending signals best-first, keep top MAX_SIGNALS_PER_SCAN.
    pending_signals.sort(key=lambda t: priority_score(t[3]), reverse=True)

    top_signals     = pending_signals[:MAX_SIGNALS_PER_SCAN]
    dropped_signals = pending_signals[MAX_SIGNALS_PER_SCAN:]

    if dropped_signals:
        dropped_names = [f"{hl_coin(s)} {d.upper()}" for s, _, d, _ in dropped_signals]
        print(f"  [RANK] Dropped {len(dropped_signals)} lower-priority signal(s): {', '.join(dropped_names)}")

    signals_fired = 0
    for rank, (symbol, _msg, direction, sig) in enumerate(top_signals, start=1):
        msg    = format_signal(symbol, sig, "CORE", rank=rank)
        msg_id = send_telegram(msg)
        update_cooldown(state, symbol, direction, bar_index_now)
        if msg_id:
            track_signal(state, symbol, direction, msg_id, sig, bar_index_now)
            print(f"  [TRACK] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"TP1={sig.tp1:.4f} TP2={sig.tp2:.4f} SL={sig.sl:.4f}")
        signals_fired += 1
        time.sleep(0.5)   # gentle rate limiting between TG sends

    # Still register dropped signals for cooldown so the same pair
    # doesn't fire again immediately on the next scan.
    for symbol, _msg, direction, sig in dropped_signals:
        update_cooldown(state, symbol, direction, bar_index_now)
        print(f"  [RANK] cooldown applied to dropped signal: {hl_coin(symbol)} {direction.upper()}")

    save_state(state)
    print(f"Scan complete. {signals_fired} signal(s) fired.")


if __name__ == "__main__":
    main()
