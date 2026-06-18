

import os, json, time, math, random, threading, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_FF_TZ = ZoneInfo("America/New_York")
OI_EXPECTED_INTERVAL_S: float = 15 * 60

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

WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]

CORR_GROUPS: dict[str, set[str]] = {
    "layer1":    {"ETHUSDT", "SOLUSDT", "AVAXUSDT", "NEARUSDT", "APTUSDT", "ADAUSDT", "DOTUSDT",
                  "SUIUSDT"},
    "defi":      {"AAVEUSDT", "UNIUSDT", "PENDLEUSDT", "ONDOUSDT"},
    "meme":      {"DOGEUSDT", "PENGUUSDT"},
    "btc_proxy": {"BTCUSDT", "LTCUSDT", "ZECUSDT", "BCHUSDT"},
    "xlm_xrp":   {"XLMUSDT", "XRPUSDT"},
    "l1_alt":    {"TAOUSDT", "TRXUSDT"},
    "bnb":       {"BNBUSDT"},
    "hype":      {"HYPEUSDT"},
    "oracle":    {"LINKUSDT"},
}

FAST_LEN  = 21
SLOW_LEN  = 50
TREND_LEN = 200
RSI_LEN   = 14
ATR_LEN   = 14
ADX_LEN   = 14
BB_LEN    = 20
BB_MULT   = 2.0
VOL_LEN   = 20
OBV_LEN   = 10

MIN_SCORE            = 4
MAX_SIGNALS_PER_SCAN = 3

USE_1H_RSI_DIVERGENCE: bool = True
DIVERGENCE_LOOKBACK: int = 3
DIVERGENCE_BONUS: int = 1

USE_FUNDING_CARRY: bool = True
FUNDING_CARRY_POSITIVE_THRESHOLD: float = 0.0005  # Above this, positive funding is tailwind for shorts
FUNDING_CARRY_NEGATIVE_THRESHOLD: float = -0.0005  # Below this, negative funding is tailwind for longs
FUNDING_CARRY_BONUS: int = 1

USE_DYNAMIC_MAX_SIGNALS: bool = True
MAX_SIGNALS_BULL_TREND: int = 5
MAX_SIGNALS_DEFAULT: int = 3
BREADTH_BULL_THRESHOLD: float = 0.70

USE_FALSE_BREAKOUT_DETECTION: bool = True
FALSE_BREAKOUT_LOOKBACK_BARS: int = 12
FALSE_BREAKOUT_BONUS: int = 1

ADX_BREAK_GATE  = 25.0
ADX_SCORE_MIN   = 20.0

RSI_BREAK_LONG_MIN  = 50.0;  RSI_BREAK_LONG_MAX  = 75.0
RSI_BREAK_SHORT_MIN = 25.0;  RSI_BREAK_SHORT_MAX = 50.0
RSI_PULL_LONG_MIN   = 38.0;  RSI_PULL_LONG_MAX   = 65.0
RSI_PULL_SHORT_MIN  = 38.0;  RSI_PULL_SHORT_MAX  = 62.0
RSI_1H_PULL_LONG_MAX  = 70.0
RSI_1H_PULL_SHORT_MIN = 30.0

VOL_SCORE_MULT  = 0.75
MAX_ATR_PCT     = 10.0
MIN_ATR_PCT     = 0.2
WICK_FILTER     = 0.45
RANGE_PCT_BREAK = 0.20
PULL_ZONE_MULT  = 0.25
PULL_TOUCH_LOOKBACK   = 2
PULL_RECOVER_ATR_MULT = 0.10
TREND_HOLD_BARS = 2
USE_EXHAUSTION_SHORT:       bool = True   # Enable exhaustion short alignment mode
EXHAUSTION_SHORT_SCORE_ADJ: int  = -1     # Score penalty when exhaustion mode fires
USE_EXHAUSTION_LONG:        bool = True   # Enable exhaustion long alignment mode
EXHAUSTION_LONG_SCORE_ADJ:  int  = -1     # Score penalty when exhaustion long mode fires
EXHAUSTION_SPREAD_LOOKBACK: int  = 4
USE_ROLLING_VWAP  = True
ROLLING_VWAP_LEN  = 16

USE_D200_FILTER   = True
D200_SOFT_ADJ     = 1

USE_DAILY_ADX     = True
MIN_DAILY_ADX     = 20.0
PULL_REQUIRES_4H  = True

FUNDING_SUPPRESS_EXTREME: float = 0.0010
SR_CLEARANCE_ATR_MULT: float    = 0.3
SUPPORT_PROXIMITY_ATR: float    = 0.75

BREAK_OI_FLAT_VOL_THRESHOLD: float = 1.5
FUNDING_PULL_WARN_MIN: float        = 0.0005

OI_HISTORY_DEPTH: int        = 6
OI_CHANGE_THRESHOLD_PCT: float = 1.0

# Score penalty for not meeting BREAK_VOL_MULT kept as secondary filter.
BREAK_VOL_MULT: float = 1.2
BREAK_VOL_ACCEL_BARS: int = 2   # Current bar volume must exceed this many prior bars

RS_TOP_PERCENTILE: float    = 0.20
RS_BOTTOM_PERCENTILE: float = 0.20
RS_BREAK_HARD_GATE_PCT: float = -3.0   # BREAK suppressed if RS vs BTC < this
RS_BREAK_SOFT_PERCENTILE: float = 0.25 # BREAK soft −1 only when RS percentile ≤ this

BREADTH_WEAK_LONG_THRESHOLD:  float = 0.20
BREADTH_WEAK_SHORT_THRESHOLD: float = 0.80
BREADTH_BREAK_LONG_SUPPRESS:  float = 0.95
BREADTH_CROWDED_LONG_THRESHOLD: float = 0.75
BREADTH_RS_COMPOUND_PENALTY: int   = -2
BREADTH_RS_NEGATIVE_THRESH: float  = 0.0

PULL_VOL_FLOOR: float          = 0.40
PULL_VOL_FLOOR_OVERBOUGHT: float  = 0.50
PULL_VOL_OVERBOUGHT_BREADTH: float = 0.80
PULL_REENTRY_COOLDOWN_S: int   = 1800

PROXIMITY_RS_MIN: float = -5.0
TP1_WALL_MIN_CLEARANCE: float = 0.40

# PULL signals emit a two-tranche entry plan:
#   Tranche A (50%): market fill at signal close (entry)
#   Tranche B (50%): limit bid/ask at EMA21 (better average)
# PULL_LIMIT_TRANCHE_PCT controls what fraction to display as the
# limit leg. PULL_LIMIT_MAX_ATR_DIST caps how far below entry the
# limit sits before we skip it (too far = gap risk on B tranche).
PULL_LIMIT_TRANCHE_PCT: float  = 0.50    # 50 % of position at limit
PULL_LIMIT_MAX_ATR_DIST: float = 1.50   # max ATR distance entry→EMA21 for limit to show

# Uses mark_px from metaAndAssetCtxs vs. the 15m close to estimate
# the implicit spread on low-cap coins.  Above SPREAD_WARN_PCT the
# signal gets a −1 score penalty; above SPREAD_SUPPRESS_PCT it is
# suppressed outright (cost makes the trade structurally negative EV).
SPREAD_WARN_PCT: float     = 0.20   # −1 score if mark diverges > 0.20 %
SPREAD_SUPPRESS_PCT: float = 0.40   # hard suppress if mark diverges > 0.40 %
# Symbols exempt from the spread filter (deep liquidity, tight markets)
SPREAD_EXEMPT: set[str] = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT",
}

WIN_RATE_MIN_SAMPLE: int    = 20
WIN_RATE_HIGH_THRESH: float = 0.65
WIN_RATE_LOW_THRESH: float  = 0.45
WIN_RATE_MIN_SAMPLE_FOR_ADJ: int = 80

MAX_OI_SCALE: float = 3.0

MIN_RR_RATIO: float = 1.0

ATR_FALLBACK_PCT: float = 0.30
WIN_RATE_LOOKBACK_DAYS:  int   = 30   # Only use signals this recent
WIN_RATE_RECENT_DAYS:    int   = 7    # Signals in this window count 2×
WIN_RATE_RECENT_WEIGHT:  float = 2.0  # Weight multiplier for recent signals

MACRO_WINDOW_BEFORE_MINS: int   = 60
MACRO_WINDOW_AFTER_MINS:  int   = 30
MACRO_HIGH_ATR_SUPPRESS_PCT: float = 3.0
MACRO_CACHE_TTL_S: int          = 3600

N_15M = 300
N_1H  = 60
N_4H  = 80
N_1D  = 210

REACT_TP1           = "🔥"
REACT_TP2           = "🏆"
REACT_SL            = "😭"
SIGNAL_MAX_AGE_BARS = 48

HL_INFO_URL = "https://api.hyperliquid.xyz/info"

INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}

# v12 UPGRADE CONSTANTS  (some relaxed in v12.3 — see FIX notes)

PULL_BODY_MIN_RATIO: float = 0.20

H4_STALE_AGE_FRACTION: float = 0.85
H4_STALE_SPREAD_MIN:   float = 0.15

OI_ACCEL_MIN_THRESHOLD: float = 1.0
OI_SCORE_CAP: int              = 2

SESSION_DEAD_ZONE_START_UTC: int = 3
SESSION_DEAD_ZONE_END_UTC:   int = 7
SESSION_LOW_ATR_PERCENTILE:  float = 0.10

TP1_MULT_BREAK: float = 1.0   # was 1.2
TP2_MULT_BREAK: float = 2.5
SL_MULT_BREAK:  float = 0.85
TP1_MULT_PULL:  float = 1.0   # was 1.2
TP2_MULT_PULL:  float = 2.0
SL_MULT_PULL:   float = 0.85
HIGH_ATR_THRESHOLD: float = 3.0
SL_HIGH_ATR_MULT:   float = 0.90

ATR_HIST_DEPTH:     int   = 48
ATR_LOW_PERCENTILE:  float = 0.10
ATR_HIGH_PERCENTILE: float = 0.90

EMA_VELOCITY_LOOKBACK:   int   = 4
EMA_VELOCITY_STRONG_MIN: float = 0.05
EMA_VELOCITY_WEAK_MAX:   float = 0.005

RSI_4H_PULL_LONG_MAX:  float = 70.0
RSI_4H_PULL_SHORT_MIN: float = 30.0

FUNDING_HISTORY_DEPTH: int = 4

# HYPERLIQUID DATA FETCHING

def hl_coin(symbol: str) -> str:
    return symbol.replace("USDT", "")

def hl_post(payload: dict) -> any:
    global _hl_last_request_ts, _hl_min_interval_s
    max_attempts  = int(os.getenv("HL_MAX_ATTEMPTS", "6"))
    base_sleep_s  = float(os.getenv("HL_BASE_SLEEP_S", "0.75"))
    for attempt in range(max_attempts):
        try:
            with _hl_request_lock:
                now     = time.time()
                elapsed = now - _hl_last_request_ts
                wait_s  = _hl_min_interval_s - elapsed
                if wait_s > 0:
                    time.sleep(wait_s)
                _hl_last_request_ts = time.time()

            r = _hl_session.post(
                HL_INFO_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            if r.status_code == 429:
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
                sleep_s = min(20.0, max(base_sleep_s, sleep_s)) + random.uniform(0.0, 0.35)
                time.sleep(sleep_s)
                continue
            r.raise_for_status()
            with _hl_request_lock:
                _hl_min_interval_s = max(HL_MIN_INTERVAL_S, _hl_min_interval_s - 0.0025)
            return r.json()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            sleep_s = min(20.0, base_sleep_s * (2 ** attempt)) + random.uniform(0.0, 0.25)
            time.sleep(sleep_s)

def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    iv_ms = INTERVAL_MS.get(interval, 60 * 60 * 1000)
    return (reference_ms // iv_ms) * iv_ms

def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]

def get_candles(symbol: str, interval: str, n: int,
                start_time_ms: int | None = None,
                reference_ms: int | None = None) -> list[dict]:
    coin   = hl_coin(symbol)
    iv_ms  = INTERVAL_MS.get(interval, 60 * 60 * 1000)
    ref_ms = int(time.time() * 1000) if reference_ms is None else reference_ms
    end_ms = current_bar_open_ms(ref_ms, interval)

    computed_start_ms = start_time_ms if start_time_ms is not None else end_ms - iv_ms * (n + 10)

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
        base_v  = float(c["v"])
        quote_v = float(c["q"]) if c.get("q") is not None else base_v
        candles.append({
            "t": int(c["t"]),   "o": float(c["o"]),
            "h": float(c["h"]), "l": float(c["l"]),
            "c": float(c["c"]), "v": base_v, "qv": quote_v,
        })
    candles = filter_closed_candles(candles, interval, ref_ms)
    return candles[-n:]

def fetch_all_candles(symbol: str, reference_ms: int | None = None) -> tuple[list, list, list, list] | None:
    candles_15m = get_candles(symbol, "15m", N_15M, reference_ms=reference_ms)
    if len(candles_15m) < 50:
        return None
    results = {}
    with ThreadPoolExecutor(max_workers=max(1, HL_TF_WORKERS)) as ex:
        futures = {
            ex.submit(get_candles, symbol, "1h", N_1H, None, reference_ms): "1h",
            ex.submit(get_candles, symbol, "4h", N_4H, None, reference_ms): "4h",
            ex.submit(get_candles, symbol, "1d", N_1D, None, reference_ms): "1d",
        }
        for fut in as_completed(futures):
            tf = futures[fut]
            try:
                results[tf] = fut.result()
            except Exception as e:
                print(f"  [CANDLES] {symbol} {tf} fetch failed: {e} — skipping symbol")
                return None
    # All three TFs must be present
    if not all(k in results for k in ("1h", "4h", "1d")):
        return None
    return candles_15m, results["1h"], results["4h"], results["1d"]

# FUNDING RATE + OPEN INTEREST

FUNDING_WARN_EXTREME = 0.0010
FUNDING_WARN_HIGH    = 0.0005

_meta_cache: dict | None = None
_meta_cache_lock = threading.Lock()
_meta_cache_fetched_at: float = 0.0
META_CACHE_TTL_S: float = 55.0

def get_meta_and_asset_ctxs() -> dict | None:
    global _meta_cache, _meta_cache_fetched_at
    with _meta_cache_lock:
        age = time.time() - _meta_cache_fetched_at
        if _meta_cache is not None and age < META_CACHE_TTL_S:
            return _meta_cache
    # Slow path: only one thread fetches; we hold the lock for the entire
    # fetch-and-write cycle so no second thread can slip through and make a
    # duplicate API call.  We re-check inside the lock in case another thread
    # already completed the fetch while we were waiting to acquire it.
    with _meta_cache_lock:
        age = time.time() - _meta_cache_fetched_at
        if _meta_cache is not None and age < META_CACHE_TTL_S:
            return _meta_cache
        try:
            data       = hl_post({"type": "metaAndAssetCtxs"})
            meta       = data[0]
            asset_ctxs = data[1]
            universe   = meta.get("universe", [])
            cache = {}
            for i, asset in enumerate(universe):
                name = asset.get("name", "")
                if not name:
                    continue
                ctx      = asset_ctxs[i]
                funding  = float(ctx["funding"])       if ctx.get("funding")      is not None else None
                oi_coins = float(ctx["openInterest"])  if ctx.get("openInterest") is not None else None
                mark     = float(ctx["markPx"])        if ctx.get("markPx")       is not None else None
                cache[name] = {"funding": funding, "open_interest_coins": oi_coins, "mark_px": mark}
            _meta_cache = cache
            _meta_cache_fetched_at = time.time()
            return _meta_cache
        except Exception as e:
            print(f"  [META CACHE] fetch failed: {e}")
    return None

def get_market_context(symbol: str) -> dict | None:
    coin  = hl_coin(symbol)
    cache = get_meta_and_asset_ctxs()
    if cache is None or coin not in cache:
        return None
    entry    = cache[coin]
    funding  = entry.get("funding")
    oi_coins = entry.get("open_interest_coins")
    mark     = entry.get("mark_px")
    oi_usd   = oi_coins * mark if (oi_coins is not None and mark is not None) else None
    return {"funding": funding, "open_interest": oi_usd, "open_interest_coins": oi_coins}

def update_funding_history(state: dict, symbol: str, rate: float | None) -> None:
    if rate is None:
        return
    with _state_lock:
        hist = state.setdefault("funding_history", {}).setdefault(symbol, [])
        hist.append({"ts": int(time.time()), "rate": rate})
        state["funding_history"][symbol] = hist[-FUNDING_HISTORY_DEPTH:]

def get_funding_trend(state: dict, symbol: str) -> str:
    hist = state.get("funding_history", {}).get(symbol, [])
    if len(hist) < 2:
        return "stable"
    recent = hist[-1]["rate"]
    prior  = hist[-2]["rate"]
    delta  = recent - prior
    if delta > 0.0001:
        return "rising"
    if delta < -0.0001:
        return "falling"
    return "stable"

_state_lock = threading.Lock()

def update_oi_history(state: dict, symbol: str, oi_usd: float | None) -> None:
    """Store OI in USD (not coins) so trend detection is price-change-neutral. [FIX-H4]"""
    if oi_usd is None:
        return
    with _state_lock:
        oi_hist     = state.setdefault("oi_history", {})
        symbol_hist = oi_hist.setdefault(symbol, [])
        symbol_hist.append({"ts": int(time.time()), "oi": oi_usd})
        oi_hist[symbol] = symbol_hist[-OI_HISTORY_DEPTH:]

def compute_oi_trend(state: dict, symbol: str, current_price: float,
                     price_direction: str, trade_direction: str) -> dict:
    oi_hist = state.get("oi_history", {}).get(symbol, [])
    if len(oi_hist) < 2:
        return {
            "oi_trend": "unknown", "oi_change_pct": None,
            "oi_acceleration": None, "score_adj": 0,
            "label": "OI Trend: Unknown", "breakdown_tag": "OI?",
            "condition": "Unknown",
        }
    recent = oi_hist[-1]["oi"]
    prior  = oi_hist[-2]["oi"]
    if prior == 0:
        return {
            "oi_trend": "unknown", "oi_change_pct": None,
            "oi_acceleration": None, "score_adj": 0,
            "label": "OI Trend: Unknown", "breakdown_tag": "OI?",
            "condition": "Unknown",
        }
    raw_change_pct = (recent - prior) / prior * 100.0
    elapsed_s      = max(1.0, oi_hist[-1]["ts"] - oi_hist[-2]["ts"])
    scale          = min(MAX_OI_SCALE, OI_EXPECTED_INTERVAL_S / elapsed_s)
    oi_change_pct  = raw_change_pct * scale

    oi_acceleration = None
    if len(oi_hist) >= 3:
        prior2 = oi_hist[-3]["oi"]
        if prior2 != 0:
            prev_elapsed = max(1.0, oi_hist[-2]["ts"] - oi_hist[-3]["ts"])
            prev_raw     = (prior - prior2) / prior2 * 100.0
            prev_scale   = min(MAX_OI_SCALE, OI_EXPECTED_INTERVAL_S / prev_elapsed)
            prev_change  = prev_raw * prev_scale
            oi_acceleration = oi_change_pct - prev_change

    oi_rising  = oi_change_pct >  OI_CHANGE_THRESHOLD_PCT
    oi_falling = oi_change_pct < -OI_CHANGE_THRESHOLD_PCT

    if oi_rising:
        oi_trend = "rising"
    elif oi_falling:
        oi_trend = "falling"
    else:
        oi_trend = "flat"

    bullish_confirm = price_direction == "up"   and oi_rising
    bearish_confirm = price_direction == "down" and oi_rising
    bullish_div     = price_direction == "up"   and oi_falling
    bearish_div     = price_direction == "down" and oi_falling

    if trade_direction == "long":
        if bullish_confirm:
            score_adj, condition, breakdown_tag = 1,  "Bullish Confirmation",                     "OI↑"
        elif bearish_confirm:
            score_adj, condition, breakdown_tag = -1, "Counter vs Long (OI rising on down move)", "OI Divergence"
        else:
            score_adj, condition, breakdown_tag = 0,  "Neutral",                                  "OI→"
    else:
        if bearish_confirm:
            score_adj, condition, breakdown_tag = 1,  "Bearish Confirmation",          "OI↑"
        elif bullish_confirm or bearish_div:
            score_adj, condition, breakdown_tag = -1, "Counter/Divergence vs Short",   "OI Divergence"
        else:
            score_adj, condition, breakdown_tag = 0,  "Neutral",                       "OI→"

    label = (
        f"OI Trend: {oi_trend.capitalize()}  "
        f"OI Change: {oi_change_pct:+.1f}% (norm)"
    )
    return {
        "oi_trend":        oi_trend,
        "oi_change_pct":   oi_change_pct,
        "oi_acceleration": oi_acceleration,
        "score_adj":       score_adj,
        "label":           label,
        "breakdown_tag":   breakdown_tag,
        "condition":       condition,
    }

def format_funding(rate: float | None, direction: str) -> str:
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
    if oi_usd is None:
        return "OI: n/a"
    if oi_usd >= 1_000_000_000:
        return f"OI: ${oi_usd / 1_000_000_000:.2f}B"
    if oi_usd >= 1_000_000:
        return f"OI: ${oi_usd / 1_000_000:.1f}M"
    return f"OI: ${oi_usd:,.0f}"

SR_PIVOT_LEFT_BARS  = 3
SR_PIVOT_RIGHT_BARS = 3
SR_CLUSTER_ATR_MULT = 0.3

def _cluster_levels(pivots: list[float], atr_val: float, tolerance: float = 0.3) -> list[float]:
    if not pivots or atr_val <= 0:
        return pivots
    zones: list[float] = []
    for p in sorted(pivots):
        if zones and abs(p - zones[-1]) < atr_val * tolerance:
            zones[-1] = (zones[-1] + p) / 2.0
        else:
            zones.append(p)
    return zones

def find_sr_levels(candles_15m: list[dict], n_levels: int = 2,
                   atr_val: float | None = None) -> tuple[list[float], list[float]]:
    cur          = candles_15m[-1]["c"]
    pivots_high  = []
    pivots_low   = []
    lb = SR_PIVOT_LEFT_BARS
    rb = SR_PIVOT_RIGHT_BARS
    # Original hardcoded [-101:-1] would return an empty/short window on new symbols
    # causing silent empty S/R lists and bypassing SR-clearance + TP1 cap.
    avail  = len(candles_15m) - 1          # exclude the current (last) bar
    window = candles_15m[max(0, avail - 100): avail]
    for i in range(lb, len(window) - rb):
        h = window[i]["h"]
        l = window[i]["l"]
        if all(h > window[i - k]["h"] for k in range(1, lb + 1)) and \
           all(h > window[i + k]["h"] for k in range(1, rb + 1)):
            pivots_high.append(h)
        if all(l < window[i - k]["l"] for k in range(1, lb + 1)) and \
           all(l < window[i + k]["l"] for k in range(1, rb + 1)):
            pivots_low.append(l)

    eff_atr = atr_val if atr_val and atr_val > 0 else (cur * 0.005)
    pivots_high = _cluster_levels(pivots_high, eff_atr, SR_CLUSTER_ATR_MULT)
    pivots_low  = _cluster_levels(pivots_low,  eff_atr, SR_CLUSTER_ATR_MULT)

    resistances = sorted([p for p in pivots_high if p > cur], key=lambda x: x - cur)[:n_levels]
    supports    = sorted([p for p in pivots_low  if p < cur], key=lambda x: cur - x)[:n_levels]
    return supports, resistances

# BTC MARKET REGIME FILTER

_btc_regime_cache: dict | None = None
_btc_regime_lock  = threading.Lock()

def compute_btc_regime(candles_1h: list[dict], candles_4h: list[dict]) -> dict:
    def _arr(candles):
        return [c["c"] for c in candles]

    c4h = _arr(candles_4h)
    c1h = _arr(candles_1h)

    ef4h = safe(ema(c4h, FAST_LEN)[-1])
    es4h = safe(ema(c4h, SLOW_LEN)[-1])
    ef1h = safe(ema(c1h, FAST_LEN)[-1])
    es1h = safe(ema(c1h, SLOW_LEN)[-1])

    btc_4h_momentum = len(c4h) >= 6 and c4h[-2] > c4h[-5]

    btc_bullish = (ef4h > es4h) and (ef1h > es1h) and btc_4h_momentum
    btc_bearish = (ef4h < es4h) and (ef1h < es1h)

    if btc_bullish:
        label = "BTC Regime: Bullish"
    elif btc_bearish:
        label = "BTC Regime: Bearish"
    else:
        label = "BTC Regime: Mixed"

    return {"bullish": btc_bullish, "bearish": btc_bearish, "label": label}

def set_btc_regime(regime: dict):
    global _btc_regime_cache
    with _btc_regime_lock:
        _btc_regime_cache = regime

def get_btc_regime() -> dict | None:
    with _btc_regime_lock:
        return _btc_regime_cache

LOW_BTC_CORR = {"TAOUSDT", "TRXUSDT", "PENDLEUSDT", "ONDOUSDT", "HYPEUSDT"}

def check_btc_regime_filter(direction: str, symbol: str,
                            signal_type: str = "") -> tuple[int, str]:
    if hl_coin(symbol) == "BTC":
        return 0, "BTC Regime: N/A (BTC itself)"

    regime = get_btc_regime()
    if regime is None:
        return 0, "BTC Regime: Unknown"

    label = regime["label"]

    if direction == "long" and regime["bearish"]:
        if symbol in LOW_BTC_CORR:
            return 0, f"{label} — counter-trend (exempt, decorrelated)"
        return -1, f"{label} — counter-trend (-1)"

    if direction == "short" and regime["bullish"]:
        if symbol in LOW_BTC_CORR:
            return 0, f"{label} — counter-trend (exempt, decorrelated)"
        return -1, f"{label} — counter-trend (-1)"

    if direction == "long" and regime["bullish"]:
        return +1, f"{label} — tailwind (+1)"
    if direction == "short" and regime["bearish"]:
        return +1, f"{label} — tailwind (+1)"

    if not regime["bullish"] and not regime["bearish"]:
        if signal_type == "BREAK":
            return 0, f"{label} — Mixed (neutral for BREAK)"
        else:
            return 0, f"{label} — Mixed (0)"

    return 0, f"{label} — Mixed (0)"

# MARKET BREADTH FILTER

_breadth_ema50_above: dict[str, bool] = {}
_breadth_snapshot:   dict[str, bool] | None = None
_breadth_lock = threading.Lock()

def reset_breadth_cache() -> None:
    global _breadth_snapshot
    with _breadth_lock:
        _breadth_ema50_above.clear()
        _breadth_snapshot = None

def record_breadth_result(symbol: str, price_above_ema50: bool):
    with _breadth_lock:
        _breadth_ema50_above[symbol] = price_above_ema50

def finalize_breadth_cache() -> None:
    global _breadth_snapshot
    with _breadth_lock:
        _breadth_snapshot = dict(_breadth_ema50_above)

def compute_market_breadth() -> dict:
    with _breadth_lock:
        results = dict(_breadth_snapshot if _breadth_snapshot is not None else _breadth_ema50_above)
    if not results:
        return {"breadth_50_pct": 0.5, "label": "Market Breadth: Unknown"}
    above = sum(1 for v in results.values() if v)
    pct   = above / len(results)
    if pct < BREADTH_WEAK_LONG_THRESHOLD:
        label = f"Market Breadth: {pct*100:.0f}% > EMA50 (Weak)"
    elif pct > BREADTH_WEAK_SHORT_THRESHOLD:
        label = f"Market Breadth: {pct*100:.0f}% > EMA50 (Overbought)"
    else:
        label = f"Market Breadth: {pct*100:.0f}% > EMA50 (Healthy)"
    return {"breadth_50_pct": pct, "label": label}

def apply_breadth_adjustment(direction: str, rs_pct: float | None = None) -> tuple[int, str]:
    breadth = compute_market_breadth()
    pct     = breadth["breadth_50_pct"]
    label   = breadth["label"]
    adj     = 0
    if direction == "long":
        if pct > 0.90:
            adj    = -2
            label += " (-2)"
        elif pct > BREADTH_CROWDED_LONG_THRESHOLD:
            rs_weak = rs_pct is not None and rs_pct <= BREADTH_RS_NEGATIVE_THRESH
            if rs_weak:
                adj    = -2
                label += " (-2, crowded+weak RS)"
            else:
                adj    = -1
                label += " (-1, crowded)"
        elif pct < BREADTH_WEAK_LONG_THRESHOLD:
            adj    = -1
            label += " (-1)"
    elif direction == "short" and pct > BREADTH_WEAK_SHORT_THRESHOLD:
        adj    = -1
        label += " (-1)"
    return adj, label

# RELATIVE STRENGTH RANKING

_rs_scores:   dict[str, float] = {}
_rs_snapshot: dict[str, float] | None = None
_rs_lock = threading.Lock()

def reset_rs_cache() -> None:
    global _rs_snapshot
    with _rs_lock:
        _rs_scores.clear()
        _rs_snapshot = None

def record_rs_return(symbol: str, return_7d_pct: float):
    with _rs_lock:
        _rs_scores[symbol] = return_7d_pct

def finalize_rs_cache() -> None:
    global _rs_snapshot
    with _rs_lock:
        _rs_snapshot = dict(_rs_scores)

def compute_relative_strength(symbol: str) -> dict:
    with _rs_lock:
        scores = dict(_rs_snapshot if _rs_snapshot is not None else _rs_scores)

    btc_return  = scores.get("BTCUSDT")
    coin_return = scores.get(symbol)

    if btc_return is None or coin_return is None:
        return {"rs_pct": None, "percentile": None, "score_adj": 0, "label": "Relative Strength: N/A"}

    rs     = coin_return - btc_return
    all_rs = sorted([v - btc_return for v in scores.values()])
    n      = len(all_rs)
    try:
        rank       = next(i for i, v in enumerate(all_rs) if v >= rs)
        percentile = rank / max(n - 1, 1)
    except StopIteration:
        percentile = 1.0

    if percentile >= (1.0 - RS_TOP_PERCENTILE):
        score_adj = 1
    elif percentile <= RS_BOTTOM_PERCENTILE:
        score_adj = -1
    else:
        score_adj = 0

    return {"rs_pct": rs, "percentile": percentile, "score_adj": score_adj,
            "label": f"Relative Strength: {rs:+.1f}%"}

# HISTORICAL PERFORMANCE ANALYTICS

def record_signal_history(state: dict, symbol: str, direction: str,
                           signal_type: str, score: int,
                           funding_rate: float | None, atr_pct: float,
                           oi_change_pct: float | None,
                           alignment_mode: str = "full",
                           sent: bool = True) -> str:
    hist     = state.setdefault("signal_history", [])
    entry_id = f"{symbol}_{int(time.time())}"
    hist.append({
        "id":              entry_id,
        "symbol":          symbol,
        "direction":       direction,
        "signal_type":     signal_type,
        "score":           score,
        "alignment_mode":  alignment_mode,
        "funding_rate":    funding_rate,
        "atr_pct":         atr_pct,
        "oi_change_pct":   oi_change_pct,
        "result":          None,
        "sent":            sent,
        "timestamp":       int(time.time()),
    })
    if len(hist) > 500:
        state["signal_history"] = hist[-2000:]
    return entry_id

def update_signal_result(state: dict, signal_id: str, result: str) -> None:
    for entry in state.get("signal_history", []):
        if entry.get("id") == signal_id:
            entry["result"] = result
            return

def compute_win_rates(state: dict) -> dict:
    now_ts       = int(time.time())
    lookback_cut = now_ts - WIN_RATE_LOOKBACK_DAYS * 86400
    recent_cut   = now_ts - WIN_RATE_RECENT_DAYS   * 86400

    raw_hist = [e for e in state.get("signal_history", [])
                if e.get("result") in ("tp1", "tp2", "sl")
                and e.get("timestamp", 0) >= lookback_cut
                and e.get("sent", True)]

    # Expand recent signals to simulate 2× weight
    hist = []
    for e in raw_hist:
        hist.append(e)
        if e.get("timestamp", 0) >= recent_cut:
            hist.append(e)   # duplicate = 2× weight

    wrs: dict = {"by_symbol": {}, "by_type": {}, "by_score": {}, "by_direction": {},
                 "by_alignment_mode": {}}

    def wr_for(entries):
        n    = len(entries)
        wins = sum(1 for e in entries if e["result"] in ("tp1", "tp2"))
        return (wins / n if n > 0 else 0.0), n

    for sym in set(e["symbol"] for e in hist):
        subset = [e for e in hist if e["symbol"] == sym]
        wr, n  = wr_for(subset)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_symbol"][sym] = {"wr": wr, "n": n}

    for st in ("BREAK", "PULL"):
        subset = [e for e in hist if e.get("signal_type") == st]
        wr, n  = wr_for(subset)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_type"][st] = {"wr": wr, "n": n}

    for sc in range(4, 8):
        subset = [e for e in hist if e.get("score") == sc]
        wr, n  = wr_for(subset)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_score"][str(sc)] = {"wr": wr, "n": n}

    for d in ("long", "short"):
        subset = [e for e in hist if e.get("direction") == d]
        wr, n  = wr_for(subset)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_direction"][d] = {"wr": wr, "n": n}

    for mode in ("full", "exhaustion"):
        subset = [e for e in hist if e.get("alignment_mode", "full") == mode]
        wr, n  = wr_for(subset)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_alignment_mode"][mode] = {"wr": wr, "n": n}

    return wrs

_win_rates_cache:      dict | None = None
_win_rates_cache_lock  = threading.Lock()

def reset_win_rates_cache() -> None:
    global _win_rates_cache
    with _win_rates_cache_lock:
        _win_rates_cache = None

def get_cached_win_rates(state: dict) -> dict:
    global _win_rates_cache
    with _win_rates_cache_lock:
        if _win_rates_cache is None:
            _win_rates_cache = compute_win_rates(state)
        return _win_rates_cache

def compute_win_rate_analytics(state: dict, symbol: str, direction: str,
                                signal_type: str, score: int,
                                alignment_mode: str = "full") -> dict:
    wrs       = get_cached_win_rates(state)
    mode_data = wrs.get("by_alignment_mode", {}).get(alignment_mode)
    if mode_data:
        wr, n = mode_data["wr"], mode_data["n"]
    else:
        sym_data = wrs["by_symbol"].get(symbol)
        if sym_data:
            wr, n = sym_data["wr"], sym_data["n"]
        else:
            dir_data = wrs["by_direction"].get(direction)
            if dir_data:
                wr, n = dir_data["wr"], dir_data["n"]
            else:
                return {"win_rate": None, "sample_size": 0, "score_adj": 0,
                        "label": "Win Rate: Insufficient data"}
    if wr >= WIN_RATE_HIGH_THRESH:
        score_adj = 1
    elif wr <= WIN_RATE_LOW_THRESH:
        score_adj = -1
    else:
        score_adj = 0
    if n < WIN_RATE_MIN_SAMPLE_FOR_ADJ:
        return {"win_rate": wr, "sample_size": n, "score_adj": 0,
                "label": f"Win Rate: {wr*100:.0f}% (n={n}, insufficient for adj)"}
    return {"win_rate": wr, "sample_size": n, "score_adj": score_adj,
            "label": f"Win Rate: {wr*100:.0f}%  Sample: {n}  (30d weighted)"}

# ECONOMIC CALENDAR FILTER

MACRO_EVENT_KEYWORDS = [
    "fomc", "federal funds", "interest rate decision",
    "cpi", "consumer price index",
    "ppi", "producer price index",
    "nfp", "nonfarm payroll", "non-farm payroll",
    "gdp", "gross domestic product",
    "ecb", "bank of england", "boe",
]

MACRO_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

def parse_forexfactory_event_utc(ev_date: str, ev_time: str) -> datetime | None:
    try:
        if "day" in ev_time.lower() or ev_time.strip() == "":
            dt_str   = f"{ev_date} 14:00"
            dt_local = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=_FF_TZ)
        else:
            dt_str   = f"{ev_date} {ev_time}"
            dt_local = datetime.strptime(dt_str, "%Y-%m-%d %I:%M%p").replace(tzinfo=_FF_TZ)
        return dt_local.astimezone(timezone.utc)
    except Exception:
        return None

def fetch_macro_calendar(state: dict) -> list[dict]:
    cache_entry = state.get("macro_calendar_cache", {})
    cached_at   = cache_entry.get("fetched_at", 0)
    now_ts      = int(time.time())
    if now_ts - cached_at < MACRO_CACHE_TTL_S:
        return cache_entry.get("events", [])
    _macro_max_attempts = 3
    _macro_base_sleep   = 1.0
    for _attempt in range(_macro_max_attempts):
        try:
            resp = requests.get(MACRO_CALENDAR_URL, timeout=10)
            resp.raise_for_status()
            raw_events = resp.json()
            break  # success
        except Exception as e:
            if _attempt == _macro_max_attempts - 1:
                print(f"  [MACRO CAL] fetch failed after {_macro_max_attempts} attempts: {e} — using cached/empty")
                return cache_entry.get("events", [])
            _sleep = min(10.0, _macro_base_sleep * (2 ** _attempt)) + random.uniform(0.0, 0.25)
            print(f"  [MACRO CAL] attempt {_attempt + 1} failed: {e} — retrying in {_sleep:.1f}s")
            time.sleep(_sleep)
    events = []
    for ev in raw_events:
        impact  = str(ev.get("impact", "")).lower()
        title   = str(ev.get("title",  "")).lower()
        ev_date = ev.get("date", "")
        ev_time = ev.get("time", "")
        if impact not in ("high",):
            continue
        if not any(kw in title for kw in MACRO_EVENT_KEYWORDS):
            continue
        dt_utc = parse_forexfactory_event_utc(ev_date, ev_time)
        if dt_utc is None:
            continue
        events.append({"name": ev.get("title", "Unknown event"),
                       "datetime_utc": dt_utc.isoformat(), "impact": impact})
    state["macro_calendar_cache"] = {"fetched_at": now_ts, "events": events}
    print(f"  [MACRO CAL] Loaded {len(events)} high-impact events")
    return events

def apply_macro_filter(state: dict, atr_pct: float,
                       reference_ms: int | None = None) -> dict:
    events      = fetch_macro_calendar(state)
    _ref_ts     = (reference_ms / 1000) if reference_ms is not None else time.time()
    now_utc     = datetime.fromtimestamp(_ref_ts, tz=timezone.utc)
    nearest_event = None
    nearest_mins  = None
    for ev in events:
        try:
            ev_dt = datetime.fromisoformat(ev["datetime_utc"])
        except Exception:
            continue
        mins_delta = (ev_dt - now_utc).total_seconds() / 60.0
        if -MACRO_WINDOW_AFTER_MINS <= mins_delta <= MACRO_WINDOW_BEFORE_MINS:
            if nearest_mins is None or abs(mins_delta) < abs(nearest_mins):
                nearest_event = ev["name"]
                nearest_mins  = mins_delta
    if nearest_event is None:
        return {"in_window": False, "event_name": None, "mins_to_event": None,
                "score_adj": 0, "label": "Macro Risk: None", "hard_suppress": False}
    score_adj     = -1
    hard_suppress = atr_pct >= MACRO_HIGH_ATR_SUPPRESS_PCT
    if nearest_mins >= 0:
        label = f"⚠️ Macro Event Nearby\n{nearest_event} in {int(nearest_mins)} minutes"
    else:
        label = f"⚠️ Macro Event Nearby\n{nearest_event} {int(abs(nearest_mins))} minutes ago"
    return {"in_window": True, "event_name": nearest_event, "mins_to_event": nearest_mins,
            "score_adj": score_adj, "label": label, "hard_suppress": hard_suppress}

# INDICATOR MATH

def ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return [float("nan")] * len(values)
    k      = 2.0 / (period + 1)
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
    rs    = avg_g / avg_l if avg_l != 0 else float("inf")
    result[period] = 100 - 100 / (1 + rs)
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i])  / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs    = avg_g / avg_l if avg_l != 0 else float("inf")
        result[i + 1] = 100 - 100 / (1 + rs)
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
        plus_dm[i]  = up   if (up > down   and up > 0)   else 0
        minus_dm[i] = down if (down > up   and down > 0) else 0
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

def detect_rsi_divergence(closes: list[float], rsi_values: list[float],
                          highs: list[float], lows: list[float],
                          lookback: int = 3) -> dict:
    """
    Detect 3-bar swing divergence between price and RSI.
    Uses highs for bearish divergence and lows for bullish divergence. [FIX-C4]
    Returns dict with divergence type and strength.
    """
    min_len = lookback + 2
    if (len(closes) < min_len or len(rsi_values) < min_len
            or len(highs) < min_len or len(lows) < min_len):
        return {"type": None, "strength": 0}
    
    recent_highs = highs[-(lookback + 2):]
    recent_lows  = lows[-(lookback + 2):]
    recent_rsi   = rsi_values[-(lookback + 2):]
    
    # Find swing highs in price (using highs array) [FIX-C4]
    price_highs = []
    for i in range(1, len(recent_highs) - 1):
        if recent_highs[i] > recent_highs[i - 1] and recent_highs[i] > recent_highs[i + 1]:
            price_highs.append((i, recent_highs[i]))
    
    # Find swing lows in price (using lows array) [FIX-C4]
    price_lows = []
    for i in range(1, len(recent_lows) - 1):
        if recent_lows[i] < recent_lows[i - 1] and recent_lows[i] < recent_lows[i + 1]:
            price_lows.append((i, recent_lows[i]))
    
    # Find corresponding RSI swing highs and lows (RSI is closes-derived — keep closes) [FIX-C4]
    rsi_highs = []
    rsi_lows = []
    for i in range(1, len(recent_rsi) - 1):
        if recent_rsi[i] > recent_rsi[i - 1] and recent_rsi[i] > recent_rsi[i + 1]:
            rsi_highs.append((i, recent_rsi[i]))
        if recent_rsi[i] < recent_rsi[i - 1] and recent_rsi[i] < recent_rsi[i + 1]:
            rsi_lows.append((i, recent_rsi[i]))
    
    # Check for bearish divergence (price higher highs, RSI lower highs)
    bearish_div = False
    if len(price_highs) >= 2 and len(rsi_highs) >= 2:
        if (price_highs[-1][1] > price_highs[-2][1] and 
            rsi_highs[-1][1] < rsi_highs[-2][1]):
            bearish_div = True
    
    # Check for bullish divergence (price lower lows, RSI higher lows)
    bullish_div = False
    if len(price_lows) >= 2 and len(rsi_lows) >= 2:
        if (price_lows[-1][1] < price_lows[-2][1] and 
            rsi_lows[-1][1] > rsi_lows[-2][1]):
            bullish_div = True
    
    if bearish_div:
        return {"type": "bearish", "strength": 1}
    elif bullish_div:
        return {"type": "bullish", "strength": 1}
    else:
        return {"type": None, "strength": 0}

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

# STATE HELPERS

def update_atr_history(state: dict, symbol: str, atr_pct: float) -> None:
    with _state_lock:
        hist = state.setdefault("atr_history", {}).setdefault(symbol, [])
        hist.append({"ts": int(time.time()), "atr_pct": atr_pct})
        if len(hist) > ATR_HIST_DEPTH:
            state["atr_history"][symbol] = hist[-ATR_HIST_DEPTH:]

def get_atr_percentile(state: dict, symbol: str, atr_pct: float) -> float | None:
    hist = state.get("atr_history", {}).get(symbol, [])
    vals = sorted(e["atr_pct"] for e in hist)
    if len(vals) < 10:
        return None
    below = sum(1 for v in vals if v < atr_pct)
    return below / len(vals)

def prune_state(state: dict) -> None:
    now = int(time.time())

    for sym in list(state.get("oi_history", {}).keys()):
        state["oi_history"][sym] = [e for e in state["oi_history"][sym]
                                    if now - e["ts"] < 86400]
        if not state["oi_history"][sym]:
            del state["oi_history"][sym]

    cutoff_cooldown = now - PULL_REENTRY_COOLDOWN_S * 3
    state["post_loss_cooldown"] = {
        k: v for k, v in state.get("post_loss_cooldown", {}).items()
        if v > cutoff_cooldown
    }

    for sym in list(state.get("atr_history", {}).keys()):
        state["atr_history"][sym] = [
            e for e in state["atr_history"][sym]
            if now - e["ts"] < 86400
        ]
        if not state["atr_history"][sym]:
            del state["atr_history"][sym]

    state["resolved_signals"] = [
        e for e in state.get("resolved_signals", [])
        if now - e.get("resolved_at", 0) < 259200
    ]

    for sym in list(state.get("funding_history", {}).keys()):
        state["funding_history"][sym] = [
            e for e in state["funding_history"][sym]
            if now - e["ts"] < 7200
        ]
        if not state["funding_history"][sym]:
            del state["funding_history"][sym]

    current_bar = int(time.time() * 1000) // (15 * 60 * 1000)
    state["signal_cooldowns"] = {
        k: v for k, v in state.get("signal_cooldowns", {}).items()
        if current_bar - v < 96
    }
    
    cutoff_bar = current_bar - 96
    for sym in list(state.get("failed_breakouts", {}).keys()):
        state["failed_breakouts"][sym] = [
            fb for fb in state["failed_breakouts"][sym]
            if fb.get("bar_index", 0) > cutoff_bar
        ]
        if not state["failed_breakouts"][sym]:
            del state["failed_breakouts"][sym]

# SIGNAL LOGIC

class SignalResult:
    def __init__(self):
        self.fire_long   = False
        self.fire_short  = False
        self.signal_type = ""
        self.score       = 0
        self.final_score = 0
        self.entry = self.tp1 = self.tp2 = self.sl = 0.0
        self.entry_low = self.entry_high = 0.0
        self.atr_pct = 0.0
        self.atr_val  = 0.0
        self.breakdown = ""
        self.v10_gates = ""
        self.funding_rate:  float | None = None
        self.open_interest: float | None = None
        self.supports:    list[float] = []
        self.resistances: list[float] = []
        self.oi_trend_data:    dict = {}
        self.btc_regime_label: str  = ""
        self.breadth_label:    str  = ""
        self.rs_data:          dict = {}
        self.win_rate_data:    dict = {}
        self.macro_data:       dict = {}
        self.d200_label:       str  = ""
        self.vol_ratio:        float | None = None
        self.alignment_mode:   str  = "full"
        self.score_adjustments: list[tuple[str, int]] = []
        self.limit_entry:       float | None = None   # EMA21 limit price (tranche B)
        self.limit_entry_dist:  float | None = None   # ATR-normalised distance entry→limit
        self.spread_pct:        float | None = None   # |mark_px − close| / close * 100

def record_market_inputs_from_candles(symbol: str,
                                      candles_15m: list[dict],
                                      candles_4h: list[dict]) -> None:
    c15 = [c["c"] for c in candles_15m]
    c4h = [c["c"] for c in candles_4h]
    ema_s4h = ema(c4h, SLOW_LEN)
    price_above_ema50_4h = c4h[-1] > safe(ema_s4h[-1])
    record_breadth_result(symbol, price_above_ema50_4h)
    if len(c4h) >= 42:
        ret_7d = (c4h[-1] - c4h[-42]) / c4h[-42] * 100.0 if c4h[-42] != 0 else 0.0
    elif len(c15) >= 672:
        ret_7d = (c15[-1] - c15[-672]) / c15[-672] * 100.0 if c15[-672] != 0 else 0.0
    else:
        ret_7d = 0.0
    record_rs_return(symbol, ret_7d)

def get_effective_min_score(btc_regime: dict | None, breadth_pct: float) -> int:
    if btc_regime is None:
        return MIN_SCORE
    bullish = btc_regime.get("bullish", False)
    bearish = btc_regime.get("bearish", False)
    mixed   = not bullish and not bearish
    if mixed and breadth_pct > 0.82:
        return MIN_SCORE + 1
    if bearish and breadth_pct > 0.75:
        return MIN_SCORE + 1
    return MIN_SCORE

def get_dynamic_max_signals(btc_regime: dict | None, breadth_pct: float) -> int:
    """
    [UPGRADE 3] Dynamic MAX_SIGNALS_PER_SCAN based on market conditions.
    Returns 5 signals in strong bull trends with healthy breadth, else 3.
    """
    if not USE_DYNAMIC_MAX_SIGNALS:
        return MAX_SIGNALS_PER_SCAN
    
    if btc_regime is None:
        return MAX_SIGNALS_DEFAULT
    
    bullish = btc_regime.get("bullish", False)
    if bullish and breadth_pct > BREADTH_BULL_THRESHOLD:
        return MAX_SIGNALS_BULL_TREND
    
    return MAX_SIGNALS_DEFAULT

def check_false_breakout_pattern(state: dict, symbol: str, direction: str,
                                 bar_index: int) -> tuple[bool, str]:
    """
    Check if this PULL signal is a false breakout re-entry pattern.
    Returns (is_pattern, description) tuple.
    """
    if not USE_FALSE_BREAKOUT_DETECTION:
        return False, ""
    
    failed_breakouts = state.get("failed_breakouts", {})
    symbol_breakouts = failed_breakouts.get(symbol, [])
    
    for fb in symbol_breakouts:
        # Check if failed breakout was in opposite direction
        fb_direction = fb.get("direction")
        if fb_direction != direction:
            # Check if within lookback period
            bars_since = bar_index - fb.get("bar_index", 0)
            if 1 <= bars_since <= FALSE_BREAKOUT_LOOKBACK_BARS:
                desc = f"False breakout re-entry ({fb_direction.upper()} BREAK failed {bars_since} bars ago)"
                return True, desc
    
    return False, ""

def record_failed_breakout(state: dict, symbol: str, direction: str,
                          signal_type: str, bar_index: int) -> None:
    """
    Record a failed BREAK signal for false breakout pattern detection.
    """
    if not USE_FALSE_BREAKOUT_DETECTION:
        return
    
    if signal_type != "BREAK":
        return
    
    failed_breakouts = state.setdefault("failed_breakouts", {})
    symbol_breakouts = failed_breakouts.setdefault(symbol, [])
    
    # Add this failed breakout
    symbol_breakouts.append({
        "direction": direction,
        "bar_index": bar_index,
        "timestamp": int(time.time()),
    })
    
    # Keep only recent entries (last 24 hours worth of bars = 96 bars)
    cutoff_bar = bar_index - 96
    failed_breakouts[symbol] = [
        fb for fb in symbol_breakouts if fb.get("bar_index", 0) > cutoff_bar
    ]

def compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d,
                    state: dict, record_market_inputs: bool = True,
                    reference_ms: int | None = None,
                    funding_rate: float | None = None) -> SignalResult:
    res = SignalResult()

    def arrays(candles):
        return ([c["o"] for c in candles], [c["h"] for c in candles],
                [c["l"] for c in candles], [c["c"] for c in candles],
                [c["v"] for c in candles])

    o15, h15, l15, c15, v15 = arrays(candles_15m)
    ema_f15 = ema(c15, FAST_LEN);  ef15 = safe(ema_f15[-1])
    ema_s15 = ema(c15, SLOW_LEN);  es15 = safe(ema_s15[-1])
    rsi15   = rsi(c15, RSI_LEN);   r15  = safe(rsi15[-1])
    atr15   = atr(h15, l15, c15, ATR_LEN); a15 = safe(atr15[-1])
    _, _, adx15_arr = adx_dmi(h15, l15, c15, ADX_LEN)
    adx15    = safe(adx15_arr[-1], 25.0)
    bb_b15, bb_u15, bb_l15 = bollinger(c15, BB_LEN, BB_MULT)
    bb_basis    = safe(bb_b15[-1])
    bb_upper_v  = safe(bb_u15[-1])
    bb_lower_v  = safe(bb_l15[-1])
    vol_ma15 = sma(v15, VOL_LEN);  vm15 = safe(vol_ma15[-1])
    obv15    = obv(c15, v15)
    rvwap15  = rolling_vwap(c15, v15, ROLLING_VWAP_LEN)

    cur_c = c15[-1]; cur_o = o15[-1]
    cur_h = h15[-1]; cur_l = l15[-1]
    cur_v = v15[-1]
    prev_l = l15[-2]; prev_h = h15[-2]
    ef15_prev = safe(ema_f15[-2])

    atr_val  = a15 if a15 > 0 else (cur_c * ATR_FALLBACK_PCT / 100)
    atr_pct  = atr_val / cur_c * 100
    market_ok = MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT

    update_atr_history(state, symbol, atr_pct)

    rv         = safe(rvwap15[-1])
    # BREAK still uses the same vwap_long/short — no change there.
    vwap_long  = cur_c > rv if USE_ROLLING_VWAP else True
    vwap_short = cur_c < rv if USE_ROLLING_VWAP else True

    rsi_break_long  = RSI_BREAK_LONG_MIN  <= r15 <= RSI_BREAK_LONG_MAX
    rsi_break_short = RSI_BREAK_SHORT_MIN <= r15 <= RSI_BREAK_SHORT_MAX
    rsi_pull_long   = RSI_PULL_LONG_MIN   <= r15 <= RSI_PULL_LONG_MAX
    rsi_pull_short  = RSI_PULL_SHORT_MIN  <= r15 <= RSI_PULL_SHORT_MAX

    bb_mid_upper = (bb_basis + bb_upper_v) / 2 if bb_upper_v > 0 else bb_basis
    bb_mid_lower = (bb_basis + bb_lower_v) / 2 if bb_lower_v > 0 else bb_basis
    bb_long  = cur_c >= bb_mid_upper
    bb_short = cur_c <= bb_mid_lower

    obv_slope_long  = obv15[-1] > obv15[-(1 + OBV_LEN)]
    obv_slope_short = obv15[-1] < obv15[-(1 + OBV_LEN)]

    adx_score_ok = adx15 >= ADX_SCORE_MIN
    vol_score_ok = True if vm15 == 0 else (cur_v >= vm15 * VOL_SCORE_MULT)

    vol_break_ok = True if vm15 == 0 else (cur_v >= vm15 * BREAK_VOL_MULT)
    vol_ratio    = (cur_v / vm15) if vm15 > 0 else None

    # Volume acceleration: current bar volume must exceed prior BREAK_VOL_ACCEL_BARS bars.
    # Kills single-spike fake breakouts where vol doesn't sustain.
    vol_accel_ok = all(
        cur_v > v15[-(i + 2)]
        for i in range(BREAK_VOL_ACCEL_BARS)
    ) if len(v15) > BREAK_VOL_ACCEL_BARS + 1 else False

    o1h, h1h, l1h, c1h, v1h = arrays(candles_1h)
    ema_f1h = ema(c1h, FAST_LEN)
    ema_s1h = ema(c1h, SLOW_LEN)
    ef1h = safe(ema_f1h[-1]); es1h = safe(ema_s1h[-1])
    h1_bull = ef1h > es1h
    h1_bear = ef1h < es1h
    rsi1h = rsi(c1h, RSI_LEN)
    r1h   = safe(rsi1h[-1])
    
    rsi_divergence = {"type": None, "strength": 0}
    if USE_1H_RSI_DIVERGENCE:
        rsi_divergence = detect_rsi_divergence(c1h, rsi1h, h1h, l1h, DIVERGENCE_LOOKBACK)

    o4h, h4h, l4h, c4h, v4h = arrays(candles_4h)
    ema_f4h = ema(c4h, FAST_LEN)
    ema_s4h = ema(c4h, SLOW_LEN)
    ef4h = safe(ema_f4h[-1]); es4h = safe(ema_s4h[-1])
    _, _, adx4h_arr = adx_dmi(h4h, l4h, c4h, ADX_LEN)
    adx4h  = safe(adx4h_arr[-1], 25.0)
    h4_bull = ef4h > es4h
    h4_bear = ef4h < es4h

    if record_market_inputs:
        record_market_inputs_from_candles(symbol, candles_15m, candles_4h)

    def trend_held(ef_arr, es_arr, bull: bool) -> bool:
        for offset in range(1, TREND_HOLD_BARS + 1):
            idx = -(offset + 1)
            if len(ef_arr) < offset + 2:
                return False
            ef_v = safe(ef_arr[idx])
            es_v = safe(es_arr[idx])
            if bull and ef_v <= es_v:
                return False
            if not bull and ef_v >= es_v:
                return False
        return True

    h4_trend_held_bull = trend_held(ema_f4h, ema_s4h, True)
    h4_trend_held_bear = trend_held(ema_f4h, ema_s4h, False)

    if candles_d and len(candles_d) >= TREND_LEN:
        o_d, h_d, l_d, c_d, v_d = arrays(candles_d)
        ema_d200 = ema(c_d, TREND_LEN)
        d200     = safe(ema_d200[-1])
        _, _, adx_d_arr = adx_dmi(h_d, l_d, c_d, ADX_LEN)
        adx_daily = safe(adx_d_arr[-1], 25.0)
    else:
        d200      = cur_c
        adx_daily = 25.0

    d200_above = cur_c > d200
    d200_below = cur_c < d200

    daily_adx_ok = adx_daily >= MIN_DAILY_ADX if USE_DAILY_ADX else True

    full_long_align  = h4_bull and h4_trend_held_bull and h1_bull
    full_short_align = h4_bear and h4_trend_held_bear and h1_bear

    # Exhaustion short: 4H bull trend losing hold + 1H already bearish.
    # Fires earlier than full_short_align on rollovers before the 4H EMA cross confirms.
    _f4h_back = safe(ema_f4h[-(EXHAUSTION_SPREAD_LOOKBACK + 2)])
    _s4h_back = safe(ema_s4h[-(EXHAUSTION_SPREAD_LOOKBACK + 2)])
    h4_spread_now  = ef4h - es4h
    h4_spread_prev = _f4h_back - _s4h_back
    if USE_EXHAUSTION_SHORT:
        h4_spread_narrowing_bull = (h4_spread_now < h4_spread_prev) and (h4_spread_now > 0)
        exhaustion_short_align = (
            h4_bull                     # 4H still technically bullish
            and not h4_trend_held_bull  # but failing to hold the trend
            and h4_spread_narrowing_bull  # and EMA spread is compressing
            and h1_bear                 # 1H has already flipped bearish
        )
    else:
        exhaustion_short_align = False

    # Exhaustion long: 4H bear trend losing hold + 1H already bullish.
    # Mirror of exhaustion short — fires before the bullish 4H EMA cross confirms.
    if USE_EXHAUSTION_LONG:
        h4_spread_narrowing_bear = (h4_spread_now > h4_spread_prev) and (h4_spread_now < 0)
        exhaustion_long_align = (
            h4_bear
            and not h4_trend_held_bear
            and h4_spread_narrowing_bear
            and h1_bull
        )
    else:
        exhaustion_long_align = False

    pull_long_align  = (h4_bull and h4_trend_held_bull and h1_bull) \
                        if PULL_REQUIRES_4H else (h4_trend_held_bull and h1_bull)
    pull_short_align = (
        (h4_bear and h4_trend_held_bear and h1_bear)
        if PULL_REQUIRES_4H
        else (h4_trend_held_bear and h1_bear)
    )
    # Exhaustion mode overrides pull_short_align when active
    if USE_EXHAUSTION_SHORT and exhaustion_short_align:
        pull_short_align = True
    if USE_EXHAUSTION_LONG and exhaustion_long_align:
        pull_long_align = True

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
    # -1) in addition to i=1..PULL_TOUCH_LOOKBACK (-2..-(LOOKBACK+1)). A valid
    # pattern where price wicks to EMA21 and recovers within the same bar was
    # previously rejected outright — this mainly cost fast V-shaped 15m
    # pullbacks.
    pull_touched_long  = any(
        l15[-(i + 1)] <= safe(ema_f15[-(i + 1)]) + pull_zone
        for i in range(0, PULL_TOUCH_LOOKBACK + 1)
    )
    pull_touched_short = any(
        h15[-(i + 1)] >= safe(ema_f15[-(i + 1)]) - pull_zone
        for i in range(0, PULL_TOUCH_LOOKBACK + 1)
    )
    pull_recover_long  = (cur_c > ef15 + atr_val * PULL_RECOVER_ATR_MULT) and cur_c > cur_o
    pull_recover_short = (cur_c < ef15 - atr_val * PULL_RECOVER_ATR_MULT) and cur_c < cur_o
    pull_bull_bar      = cur_c > cur_o and clean_bull_bar()
    pull_bear_bar      = cur_c < cur_o and clean_bear_bar()

    long_score = sum([
        bb_long,
        h4_bull and h4_trend_held_bull,
        adx_score_ok,
        obv_slope_long,
        vol_score_ok,
    ])
    short_score = sum([
        bb_short,
        h4_bear and h4_trend_held_bear,
        adx_score_ok,
        obv_slope_short,
        vol_score_ok,
    ])

    # Score penalty in Step 6 still applies as a secondary signal-quality indicator.
    long_break  = (daily_adx_ok and (full_long_align or exhaustion_long_align) and break_bull_bar
                   and adx_break_ok and rsi_break_long
                   and long_score  >= (MIN_SCORE - 1) and market_ok and vol_break_ok
                   and vol_accel_ok)
    short_break = (daily_adx_ok
                   and (full_short_align or exhaustion_short_align)
                   and break_bear_bar
                   and adx_break_ok and rsi_break_short
                   and short_score >= (MIN_SCORE - 1) and market_ok and vol_break_ok
                   and vol_accel_ok)

    # alignment, with exhaustion overrides already folded in) instead of
    # full_*_align or exhaustion_*_align directly. With PULL_REQUIRES_4H=True
    # (current default) pull_*_align is identical to full_*_align, so this is
    # a no-op today; it only matters if PULL_REQUIRES_4H is ever set False —
    # previously that relaxed path was computed but never actually wired in.
    long_pull  = (daily_adx_ok and pull_long_align and pull_touched_long
                  and pull_recover_long  and pull_bull_bar and vwap_long  and rsi_pull_long
                  and long_score  >= (MIN_SCORE - 1) and market_ok)
    short_pull = (daily_adx_ok
                  and pull_short_align
                  and pull_touched_short
                  and pull_recover_short and pull_bear_bar and vwap_short and rsi_pull_short
                  and short_score >= (MIN_SCORE - 1) and market_ok)

    long_sig  = long_break  or long_pull
    short_sig = short_break or short_pull

    def make_breakdown(is_long, oi_tag: str = "OI?", vol_ratio_val=None):
        bb_s  = "BB✓"  if (bb_long  if is_long else bb_short)  else "BB✗"
        d_s   = "4H✓"  if (h4_bull and h4_trend_held_bull if is_long else h4_bear and h4_trend_held_bear) else "4H✗"
        adx_s = "ADX✓" if adx_score_ok  else "ADX✗"
        obv_s = "OBV✓" if (obv_slope_long if is_long else obv_slope_short) else "OBV✗"
        # vol_score_ok is still used for the base long_score/short_score calculation.
        vol_s  = "VOL✓" if vol_score_ok_pull else "VOL✗"
        accel_s = "VA✓" if vol_accel_ok else "VA✗"
        return f"{bb_s} {d_s} {adx_s} {obv_s} {vol_s} {oi_tag} {accel_s}"

    def make_gates(is_long):
        h1 = h1_bull if is_long else h1_bear
        if is_long and exhaustion_long_align and not full_long_align:
            h4_tag = "4H:⚠"   # exhaustion mode — not fully confirmed
        elif not is_long and exhaustion_short_align and not full_short_align:
            h4_tag = "4H:⚠"   # exhaustion mode — not fully confirmed
        else:
            h4_tag = f"4H:{'✓' if (full_long_align if is_long else full_short_align) else '✗'}"
        return (f"DailyADX:{'✓' if daily_adx_ok else '✗'} "
                f"{h4_tag} "
                f"1H:{'✓' if h1 else '✗'}")

    if long_sig:
        res.fire_long    = True
        res.signal_type  = "BREAK" if long_break else "PULL"
        res.score        = long_score
        res.entry        = cur_c
        zone_half_width  = atr_val * PULL_ZONE_MULT
        res.entry_low    = cur_c - zone_half_width
        res.entry_high   = cur_c + zone_half_width
        res.atr_val      = atr_val
        res.atr_pct      = atr_pct
        res.v10_gates    = make_gates(True)
        res.vol_ratio    = vol_ratio
        _sl_mult  = SL_HIGH_ATR_MULT if atr_pct > HIGH_ATR_THRESHOLD else (
            SL_MULT_BREAK if res.signal_type == "BREAK" else SL_MULT_PULL
        )
        _tp2_mult = TP2_MULT_BREAK if res.signal_type == "BREAK" else TP2_MULT_PULL
        res.tp1 = cur_c + atr_val * (TP1_MULT_BREAK if res.signal_type == "BREAK" else TP1_MULT_PULL)
        res.tp2 = cur_c + atr_val * _tp2_mult
        res.sl  = cur_c - atr_val * _sl_mult
        if res.signal_type == "PULL":
            _limit_px  = safe(ema_f15[-1])   # EMA21 (fast EMA = period 21)
            _limit_dist = (cur_c - _limit_px) / atr_val if atr_val > 0 else 0.0
            if _limit_px > 0 and 0 < _limit_dist <= PULL_LIMIT_MAX_ATR_DIST:
                res.limit_entry      = _limit_px
                res.limit_entry_dist = _limit_dist

    elif short_sig:
        res.fire_short   = True
        res.signal_type  = "BREAK" if short_break else "PULL"
        res.score        = short_score
        res.entry        = cur_c
        zone_half_width  = atr_val * PULL_ZONE_MULT
        res.entry_low    = cur_c - zone_half_width
        res.entry_high   = cur_c + zone_half_width
        res.atr_val      = atr_val
        res.atr_pct      = atr_pct
        res.v10_gates    = make_gates(False)
        res.vol_ratio    = vol_ratio
        _sl_mult  = SL_HIGH_ATR_MULT if atr_pct > HIGH_ATR_THRESHOLD else (
            SL_MULT_BREAK if res.signal_type == "BREAK" else SL_MULT_PULL
        )
        _tp2_mult = TP2_MULT_BREAK if res.signal_type == "BREAK" else TP2_MULT_PULL
        res.tp1 = cur_c - atr_val * (TP1_MULT_BREAK if res.signal_type == "BREAK" else TP1_MULT_PULL)
        res.tp2 = cur_c - atr_val * _tp2_mult
        res.sl  = cur_c + atr_val * _sl_mult
        if res.signal_type == "PULL":
            _limit_px  = safe(ema_f15[-1])   # EMA21
            _limit_dist = (_limit_px - cur_c) / atr_val if atr_val > 0 else 0.0
            if _limit_px > 0 and 0 < _limit_dist <= PULL_LIMIT_MAX_ATR_DIST:
                res.limit_entry      = _limit_px
                res.limit_entry_dist = _limit_dist

    if not (res.fire_long or res.fire_short):
        return res

    direction = "long" if res.fire_long else "short"
    price_dir = "up" if cur_c > cur_o else "down"

    _breadth_snap_local = compute_market_breadth()
    _breadth_pct_local  = _breadth_snap_local["breadth_50_pct"]

    # is a signal-type-aware re-evaluation used only for the PULL vol-floor check and
    # breakdown display — it does NOT overwrite the vol_score_ok used in long/short_score.
    vol_score_ok_pull = True if vm15 == 0 else (
        cur_v >= vm15 * VOL_SCORE_MULT if res.signal_type == "BREAK"
        else cur_v <= vm15 * 1.3
    )

    # Breadth hard suppress: BREAK longs above 95% breadth
    breadth_snap = _breadth_snap_local
    breadth_pct  = _breadth_pct_local
    if res.signal_type == "BREAK" and direction == "long" and breadth_pct > BREADTH_BREAK_LONG_SUPPRESS:
        print(f"  [BREADTH SUPPRESS] {symbol} BREAK LONG suppressed — breadth {breadth_pct*100:.0f}% > {BREADTH_BREAK_LONG_SUPPRESS*100:.0f}%")
        res.fire_long = False
        return res

    # FINAL SCORING STACK
    # 1. OI  2. BTC+Breadth  3. RS  4. Win Rate
    # 5. Macro  6. Volume (BREAK)  6b. Signal-type quality floors
    # 7. D200  8. Session/EMA vel/RSI confluence/ATR pctile
    # 9. S/R Proximity + TP/SL override (sole TP1-obstruction protection) + 1H RSI
    adjusted_score = res.score
    adjs = res.score_adjustments

    oi_data = compute_oi_trend(state, symbol, cur_c, price_dir, direction)
    res.oi_trend_data = oi_data
    adjusted_score += oi_data["score_adj"]
    if oi_data["score_adj"] != 0:
        adjs.append((f"OI ({oi_data['breakdown_tag']})", oi_data["score_adj"]))

    oi_acceleration = oi_data.get("oi_acceleration")
    oi_trend        = oi_data.get("oi_trend", "unknown")
    oi_contribution = oi_data["score_adj"]

    if oi_acceleration is not None:
        if (oi_acceleration > OI_ACCEL_MIN_THRESHOLD
                and oi_trend == "rising"
                and oi_data["score_adj"] > 0
                and oi_contribution < OI_SCORE_CAP):
            adjusted_score += 1
            oi_contribution += 1
            adjs.append(("OI Acceleration (confirming↑)", 1))
        elif (oi_acceleration < -OI_ACCEL_MIN_THRESHOLD
                and oi_trend == "falling"
                and oi_data["score_adj"] < 0
                and oi_contribution > -OI_SCORE_CAP):
            adjusted_score -= 1
            oi_contribution -= 1
            adjs.append(("OI Acceleration (diverging↓)", -1))

    btc_adj, btc_label = check_btc_regime_filter(direction, symbol, res.signal_type)
    res.btc_regime_label = btc_label
    adjusted_score += btc_adj
    if btc_adj != 0:
        adjs.append((btc_label, btc_adj))

    # Compute RS early so breadth compound penalty can reference it
    rs_data      = compute_relative_strength(symbol)
    res.rs_data  = rs_data

    breadth_adj, breadth_label = apply_breadth_adjustment(direction, rs_pct=rs_data.get("rs_pct"))
    res.breadth_label = breadth_label
    adjusted_score += breadth_adj
    if breadth_adj != 0:
        adjs.append((breadth_label, breadth_adj))

    # Exhaustion short penalty — lower conviction than full bear alignment
    if (direction == "short"
            and USE_EXHAUSTION_SHORT
            and exhaustion_short_align
            and not full_short_align):
        if oi_data.get("oi_trend") == "falling":
            adjs.append(("Exhaustion + OI confirms (no penalty)", 0))
        else:
            adjusted_score += EXHAUSTION_SHORT_SCORE_ADJ
            adjs.append((f"Exhaustion short mode (4H spread narrowing, not yet confirmed)",
                         EXHAUSTION_SHORT_SCORE_ADJ))

    # Exhaustion long penalty — lower conviction than full bull alignment
    if (direction == "long"
            and USE_EXHAUSTION_LONG
            and exhaustion_long_align
            and not full_long_align):
        if oi_data.get("oi_trend") == "rising":
            adjs.append(("Exhaustion + OI confirms (no penalty)", 0))
        else:
            adjusted_score += EXHAUSTION_LONG_SCORE_ADJ
            adjs.append((f"Exhaustion long mode (4H spread narrowing, not yet confirmed)",
                         EXHAUSTION_LONG_SCORE_ADJ))

    res.alignment_mode = (
        "exhaustion" if (
            (direction == "short" and USE_EXHAUSTION_SHORT
             and exhaustion_short_align and not full_short_align)
            or (direction == "long" and USE_EXHAUSTION_LONG
                and exhaustion_long_align and not full_long_align)
        ) else "full"
    )

    _ref_ms       = reference_ms if reference_ms is not None else int(time.time() * 1000)
    iv_4h_ms      = 4 * 60 * 60 * 1000
    bar_open_4h_ms = (_ref_ms // iv_4h_ms) * iv_4h_ms
    bar_age_frac  = (_ref_ms - bar_open_4h_ms) / iv_4h_ms
    _h1_spread    = (ef1h - es1h) / atr_val if atr_val > 0 else 0.0
    h4_stale_bias = (
        bar_age_frac >= H4_STALE_AGE_FRACTION
        and ((direction == "long"  and _h1_spread < H4_STALE_SPREAD_MIN)
          or (direction == "short" and _h1_spread > -H4_STALE_SPREAD_MIN))
    )
    if h4_stale_bias:
        adjusted_score -= 1
        adjs.append((f"4H bias stale ({bar_age_frac*100:.0f}% into bar, 1H spread {_h1_spread:+.2f}x ATR)", -1))

    adjusted_score += rs_data["score_adj"]
    if rs_data["score_adj"] != 0:
        adjs.append((rs_data["label"], rs_data["score_adj"]))

    wr_data = compute_win_rate_analytics(
        state, symbol, direction, res.signal_type, res.score, res.alignment_mode
    )
    res.win_rate_data = wr_data
    adjusted_score += wr_data["score_adj"]
    if wr_data["score_adj"] != 0:
        adjs.append((wr_data["label"], wr_data["score_adj"]))

    macro_data = apply_macro_filter(state, atr_pct, reference_ms=reference_ms)
    res.macro_data = macro_data
    if macro_data["hard_suppress"]:
        print(f"  [MACRO FILTER] {symbol} hard suppressed — elevated ATR + macro risk window")
        res.fire_long  = False
        res.fire_short = False
        return res
    adjusted_score += macro_data["score_adj"]
    if macro_data["score_adj"] != 0:
        adjs.append(("Macro Risk Window", macro_data["score_adj"]))

    # in long_break/short_break, making this branch unreachable. Secondary
    # quality is handled by vol_accel_ok and the vol_ratio floor in Step 6b.

    res.supports, res.resistances = find_sr_levels(candles_15m, atr_val=atr_val)

    if res.signal_type == "PULL":
        # price_dir is "down" (selling into a falling market). When price_dir is "up"
        # and OI is falling, that is short-covering — already made neutral by FIX-C2
        # in compute_oi_trend(). Applying -1 here on top would undo FIX-C2 silently.
        # (price up + OI falling on a long, or price down + OI falling on a short).
        # Step 6b only adds the extra -1 when OI is falling but Step 1 scored it 0 (neutral),
        # e.g. when price_dir disagrees with the PULL recover bar direction.
        _oi_falling_bearish = (
            oi_data.get("oi_trend") == "falling"
            and oi_data.get("score_adj", 0) == 0
            and price_dir == "down"
        )
        if _oi_falling_bearish:
            adjusted_score -= 1
            adjs.append(("OI falling on PULL with price down (Step-1 neutral)", -1))

        # fix). An exact 0.40x reading is indistinguishable from 0.39x given raw data
        # rounding, so it must fail the floor too: 0.39x fail, 0.40x fail, 0.41x pass.
        if vol_ratio is not None:
            _breadth_pct = _breadth_pct_local
            _vol_floor   = (PULL_VOL_FLOOR_OVERBOUGHT
                            if _breadth_pct > PULL_VOL_OVERBOUGHT_BREADTH
                            else PULL_VOL_FLOOR)
            if vol_ratio <= _vol_floor:
                adjusted_score -= 1
                adjs.append((f"Vol floor (ratio {vol_ratio:.2f}x <= {_vol_floor:.2f}x"
                             f"{' overbought' if _breadth_pct > PULL_VOL_OVERBOUGHT_BREADTH else ''})", -1))

        candle_range = cur_h - cur_l
        body_size    = abs(cur_c - cur_o)
        body_ratio   = body_size / candle_range if candle_range > 0 else 1.0
        pull_body_ok = body_ratio >= PULL_BODY_MIN_RATIO
        if not pull_body_ok:
            adjusted_score -= 1
            adjs.append((f"Weak candle body (ratio {body_ratio:.2f} < {PULL_BODY_MIN_RATIO})", -1))

    if res.signal_type == "BREAK":
        #           High-volume BREAKs with flat OI are valid momentum moves (OI lags price).
        if oi_data.get("oi_trend") == "flat" and (vol_ratio is None or vol_ratio < BREAK_OI_FLAT_VOL_THRESHOLD):
            adjusted_score -= 1
            adjs.append(("OI flat + low vol on BREAK (low conviction)", -1))

        rs_pct = rs_data.get("rs_pct")
        if rs_pct is not None:
            if rs_pct < RS_BREAK_HARD_GATE_PCT:
                # Hard gate: symbol is clearly underperforming BTC — not a leading breakout.
                print(f"  [RS GATE] {symbol} BREAK suppressed — RS {rs_pct:+.1f}% < {RS_BREAK_HARD_GATE_PCT:.1f}%")
                res.fire_long  = False
                res.fire_short = False
                return res
            elif rs_pct < 0:
                rs_percentile = rs_data.get("percentile")
                if rs_percentile is not None and rs_percentile <= RS_BREAK_SOFT_PERCENTILE:
                    adjusted_score -= 1
                    adjs.append((f"RS negative on BREAK ({rs_pct:+.1f}%, "
                                 f"{rs_percentile*100:.0f}th pct)", -1))

        if direction == "long" and res.resistances:
            nearest_res = res.resistances[0]
            if res.entry < nearest_res < res.tp1:
                tp_range = res.tp1 - res.entry
                if tp_range > 0:
                    blocked_pct = (nearest_res - res.entry) / tp_range
                    if blocked_pct >= TP1_WALL_MIN_CLEARANCE:
                        adjusted_score -= 1
                        adjs.append((f"Resistance blocks TP1 ({blocked_pct*100:.0f}%)", -1))
        elif direction == "short" and res.supports:
            nearest_sup = res.supports[0]
            if res.tp1 < nearest_sup < res.entry:
                tp_range = res.entry - res.tp1
                if tp_range > 0:
                    blocked_pct = (res.entry - nearest_sup) / tp_range
                    if blocked_pct >= TP1_WALL_MIN_CLEARANCE:
                        adjusted_score -= 1
                        adjs.append((f"Support blocks TP1 ({blocked_pct*100:.0f}%)", -1))

    if USE_D200_FILTER:
        if direction == "long" and d200_above:
            adjusted_score += D200_SOFT_ADJ
            d200_label = f"D200: Price above (+{D200_SOFT_ADJ})"
            adjs.append((d200_label, D200_SOFT_ADJ))
        elif direction == "long" and d200_below:
            adjusted_score -= D200_SOFT_ADJ
            d200_label = f"D200: Price below (-{D200_SOFT_ADJ})"
            adjs.append((d200_label, -D200_SOFT_ADJ))
        elif direction == "short" and d200_below:
            adjusted_score += D200_SOFT_ADJ
            d200_label = f"D200: Price below (+{D200_SOFT_ADJ})"
            adjs.append((d200_label, D200_SOFT_ADJ))
        elif direction == "short" and d200_above:
            adjusted_score -= D200_SOFT_ADJ
            d200_label = f"D200: Price above (-{D200_SOFT_ADJ})"
            adjs.append((d200_label, -D200_SOFT_ADJ))
        else:
            d200_label = "D200: Neutral"
        res.d200_label = d200_label
    else:
        res.d200_label = "D200: Disabled"

    # so delayed GitHub Actions runs don't misapply the dead zone penalty.
    _ref_ts_s   = (reference_ms / 1000) if reference_ms is not None else time.time()
    _now_utc    = datetime.fromtimestamp(_ref_ts_s, tz=timezone.utc)
    _in_dead = SESSION_DEAD_ZONE_START_UTC <= _now_utc.hour < SESSION_DEAD_ZONE_END_UTC
    _atr_vals = [v for v in atr15[-(48 + ATR_LEN):] if not math.isnan(v)]
    if _atr_vals:
        _atr_pct_thresh = sorted(_atr_vals)[int(len(_atr_vals) * SESSION_LOW_ATR_PERCENTILE)]
    else:
        _atr_pct_thresh = atr_val
    _low_atr        = atr_val <= _atr_pct_thresh
    session_penalty = _in_dead and _low_atr
    if session_penalty:
        adjusted_score -= 1
        adjs.append((f"Low-liquidity session ({_now_utc.hour:02d}:xx UTC, thin ATR)", -1))

    if len(ema_f15) >= EMA_VELOCITY_LOOKBACK + 2:
        _ef_velocity = (safe(ema_f15[-1]) - safe(ema_f15[-(1 + EMA_VELOCITY_LOOKBACK)])) / atr_val
        if res.signal_type == "BREAK":
            if _ef_velocity > EMA_VELOCITY_STRONG_MIN and direction == "long":
                adjusted_score += 1
                adjs.append((f"EMA accelerating up ({_ef_velocity:+.3f}×ATR)", +1))
            elif _ef_velocity < -EMA_VELOCITY_STRONG_MIN and direction == "short":
                adjusted_score += 1
                adjs.append((f"EMA accelerating down ({_ef_velocity:+.3f}×ATR)", +1))
            elif abs(_ef_velocity) < EMA_VELOCITY_WEAK_MAX:
                adjusted_score -= 1
                adjs.append((f"EMA flattening ({_ef_velocity:+.3f}×ATR) on BREAK", -1))

    rsi4h_arr = rsi(c4h, RSI_LEN)
    _r4h_raw  = rsi4h_arr[-2] if len(rsi4h_arr) >= 2 else float("nan")
    r4h_valid = not math.isnan(_r4h_raw)
    r4h       = _r4h_raw if r4h_valid else 50.0
    
    if USE_1H_RSI_DIVERGENCE and rsi_divergence["type"] is not None:
        div_type = rsi_divergence["type"]
        if res.signal_type == "PULL":
            if direction == "short" and div_type == "bearish":
                adjusted_score += DIVERGENCE_BONUS
                adjs.append(("1H RSI bearish divergence (PULL short)", DIVERGENCE_BONUS))
            elif direction == "long" and div_type == "bullish":
                adjusted_score += DIVERGENCE_BONUS
                adjs.append(("1H RSI bullish divergence (PULL long)", DIVERGENCE_BONUS))
        elif res.alignment_mode == "exhaustion":
            if direction == "short" and div_type == "bearish":
                adjusted_score += DIVERGENCE_BONUS
                adjs.append(("1H RSI bearish divergence (exhaustion short)", DIVERGENCE_BONUS))
            elif direction == "long" and div_type == "bullish":
                adjusted_score += DIVERGENCE_BONUS
                adjs.append(("1H RSI bullish divergence (exhaustion long)", DIVERGENCE_BONUS))
    
    if USE_FALSE_BREAKOUT_DETECTION and res.signal_type == "PULL":
        # Get current bar index for pattern check
        _ref_ms_for_pattern = reference_ms if reference_ms is not None else int(time.time() * 1000)
        _bar_index_pattern = _ref_ms_for_pattern // (15 * 60 * 1000)
        is_false_breakout, fb_desc = check_false_breakout_pattern(state, symbol, direction, _bar_index_pattern)
        if is_false_breakout:
            adjusted_score += FALSE_BREAKOUT_BONUS
            adjs.append((fb_desc, FALSE_BREAKOUT_BONUS))

    if res.signal_type == "BREAK":
        rsi_15m_ok_long  = RSI_BREAK_LONG_MIN  <= r15 <= RSI_BREAK_LONG_MAX
        rsi_15m_ok_short = RSI_BREAK_SHORT_MIN <= r15 <= RSI_BREAK_SHORT_MAX
    else:
        rsi_15m_ok_long  = RSI_PULL_LONG_MIN   <= r15 <= RSI_PULL_LONG_MAX
        rsi_15m_ok_short = RSI_PULL_SHORT_MIN  <= r15 <= RSI_PULL_SHORT_MAX

    rsi_1h_ok_long   = r1h <= RSI_1H_PULL_LONG_MAX
    rsi_4h_ok_long   = r4h <= RSI_4H_PULL_LONG_MAX
    rsi_1h_ok_short  = r1h >= RSI_1H_PULL_SHORT_MIN
    rsi_4h_ok_short  = r4h >= RSI_4H_PULL_SHORT_MIN
    # Confluence bonus fires only when 1H AND 4H RSI are also aligned.
    rsi_confluence_long  = rsi_1h_ok_long  and rsi_4h_ok_long
    rsi_confluence_short = rsi_1h_ok_short and rsi_4h_ok_short

    if direction == "long":
        if rsi_confluence_long:
            adjusted_score += 1
            adjs.append(("RSI confluence 1H/4H", +1))
        elif r4h_valid and r4h > RSI_4H_PULL_LONG_MAX:
            adjusted_score -= 1
            adjs.append((f"4H RSI overbought ({r4h:.0f})", -1))

    if direction == "short":
        if rsi_confluence_short:
            adjusted_score += 1
            adjs.append(("RSI confluence 1H/4H", +1))
        elif r4h_valid and r4h < RSI_4H_PULL_SHORT_MIN:
            adjusted_score -= 1
            adjs.append((f"4H RSI oversold ({r4h:.0f})", -1))

    _atr_pctile = get_atr_percentile(state, symbol, atr_pct)
    if _atr_pctile is not None:
        if res.signal_type == "BREAK" and _atr_pctile < ATR_LOW_PERCENTILE:   # 0.10
            adjusted_score -= 1
            adjs.append((f"ATR low vs symbol history ({_atr_pctile*100:.0f}th pct)", -1))
        elif res.signal_type == "PULL" and _atr_pctile > ATR_HIGH_PERCENTILE:  # 0.90
            adjusted_score -= 1
            adjs.append((f"ATR high vs symbol history ({_atr_pctile*100:.0f}th pct)", -1))

    # SUPPORT_PROXIMITY_ATR and PROXIMITY_RS_MIN constants retained (referenced elsewhere / future use)

    # This is the SOLE protection against TP1 obstruction.  After this override,
    # res.tp1 == min(sr_tp1, atr_tp1) for longs / max(sr_tp1, atr_tp1) for shorts,
    # so the old "headroom penalty" block that checked nearest_res < res.tp1 was
    # unreachable (nearest_res IS res.tp1 after the cap) and has been removed.
    if res.signal_type == "PULL":
        if res.fire_long and res.resistances:
            sr_tp1  = res.resistances[0]
            atr_tp1 = res.entry + atr_val * TP1_MULT_PULL
            res.tp1 = min(sr_tp1, atr_tp1)
        elif res.fire_short and res.supports:
            sr_tp1  = res.supports[0]
            atr_tp1 = res.entry - atr_val * TP1_MULT_PULL
            res.tp1 = max(sr_tp1, atr_tp1)

    # 1H RSI soft gate for PULL
    # (i.e., don't stack -1 on top of a missed +1 for the same condition)
    _rsi_conf_awarded = (direction == "long" and rsi_confluence_long) or \
                        (direction == "short" and rsi_confluence_short)
    if res.signal_type == "PULL" and not _rsi_conf_awarded:
        if direction == "long" and r1h > RSI_1H_PULL_LONG_MAX:
            adjusted_score -= 1
            adjs.append((f"1H RSI extended ({r1h:.0f} > {RSI_1H_PULL_LONG_MAX:.0f})", -1))
        elif direction == "short" and r1h < RSI_1H_PULL_SHORT_MIN:
            adjusted_score -= 1
            adjs.append((f"1H RSI extended ({r1h:.0f} < {RSI_1H_PULL_SHORT_MIN:.0f})", -1))

    if funding_rate is not None:
        if USE_FUNDING_CARRY:
            rate = funding_rate
            tailwind = (rate < 0 and direction == "long") or (rate > 0 and direction == "short")
            if tailwind:
                if direction == "short" and rate >= FUNDING_CARRY_POSITIVE_THRESHOLD:
                    adjusted_score += FUNDING_CARRY_BONUS
                    adjs.append((f"Funding tailwind carry on {res.signal_type} ({rate*100:+.4f}%/8h)", FUNDING_CARRY_BONUS))
                    print(f"  [FUNDING CARRY] {symbol} {direction.upper()} {res.signal_type} bonus "
                          f"— positive funding {rate*100:+.4f}%/8h")
                elif direction == "long" and rate <= FUNDING_CARRY_NEGATIVE_THRESHOLD:
                    adjusted_score += FUNDING_CARRY_BONUS
                    adjs.append((f"Funding tailwind carry on {res.signal_type} ({rate*100:+.4f}%/8h)", FUNDING_CARRY_BONUS))
                    print(f"  [FUNDING CARRY] {symbol} {direction.upper()} {res.signal_type} bonus "
                          f"— negative funding {rate*100:+.4f}%/8h")

        if FUNDING_PULL_WARN_MIN is not None:
            rate = funding_rate
            headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
            if headwind and FUNDING_PULL_WARN_MIN <= abs(rate) < FUNDING_SUPPRESS_EXTREME:
                adjusted_score -= 1
                adjs.append((f"Funding headwind on {res.signal_type} ({rate*100:+.4f}%/8h)", -1))
                print(f"  [FUNDING] {symbol} {direction.upper()} {res.signal_type} soft penalty "
                      f"— funding {rate*100:+.4f}%/8h")

        rate = funding_rate
        headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
        if headwind and get_funding_trend(state, symbol) == "rising":
            adjusted_score -= 1
            adjs.append((f"Funding headwind rising on {res.signal_type} ({rate*100:+.4f}%/8h ↑)", -1))
            print(f"  [FUNDING TREND] {symbol} {direction.upper()} {res.signal_type} trend penalty "
                  f"— funding rising {rate*100:+.4f}%/8h")

    res.final_score = adjusted_score
    _breadth_pct   = _breadth_pct_local
    _effective_min = get_effective_min_score(get_btc_regime(), _breadth_pct)
    if adjusted_score < _effective_min:
        print(f"  [SCORE FILTER] {symbol} {direction.upper()} suppressed: "
              f"base={res.score} → final={adjusted_score} < {_effective_min}"
              f"{' (regime-adjusted)' if _effective_min > MIN_SCORE else ''}")
        res.fire_long  = False
        res.fire_short = False
        return res

    res.breakdown = make_breakdown(res.fire_long, oi_data["breakdown_tag"], vol_ratio)

    _tp1_dist = abs(res.tp1 - res.entry)
    _sl_dist  = abs(res.sl  - res.entry)
    if _sl_dist > 0 and (_tp1_dist / _sl_dist) < MIN_RR_RATIO:
        print(f"  [RR FILTER] {symbol} {direction.upper()} suppressed — "
              f"R:R {_tp1_dist/_sl_dist:.2f} < {MIN_RR_RATIO} "
              f"(TP1={res.tp1:.4f} entry={res.entry:.4f} SL={res.sl:.4f})")
        res.fire_long  = False
        res.fire_short = False
        return res

    if SR_CLEARANCE_ATR_MULT > 0 and (res.fire_long or res.fire_short):
        min_clearance = atr_val * SR_CLEARANCE_ATR_MULT
        if res.fire_long and res.resistances:
            nearest_res = res.resistances[0]
            if nearest_res - res.entry < min_clearance:
                print(f"  [SR FILTER] {symbol} LONG suppressed — resistance {nearest_res:.4f} "
                      f"too close to entry {res.entry:.4f}")
                res.fire_long = False
        if res.fire_short and res.supports:
            nearest_sup = res.supports[0]
            if res.entry - nearest_sup < min_clearance:
                print(f"  [SR FILTER] {symbol} SHORT suppressed — support {nearest_sup:.4f} "
                      f"too close to entry {res.entry:.4f}")
                res.fire_short = False

    return res

# COOLDOWN STATE

def load_state() -> dict:
    _fresh = {
        "oi_history": {}, "signal_history": [], "macro_calendar_cache": {},
        "post_loss_cooldown": {}, "atr_history": {}, "funding_history": {},
        "signal_cooldowns": {},
        "failed_breakouts": {},
    }
    for path in (STATE_FILE, STATE_FILE + ".bak"):
        if Path(path).exists():
            try:
                s = json.loads(Path(path).read_text())
                s.setdefault("oi_history", {})
                s.setdefault("signal_history", [])
                s.setdefault("macro_calendar_cache", {})
                s.setdefault("post_loss_cooldown", {})
                s.setdefault("atr_history", {})
                s.setdefault("funding_history", {})
                s.setdefault("signal_cooldowns", {})
                s.setdefault("failed_breakouts", {})
                if path != STATE_FILE:
                    print(f"[STATE] Loaded from backup {path}")
                return s
            except Exception as e:
                print(f"[STATE] Failed to load {path}: {e}")
    print("[STATE] Starting fresh — no valid state file found")
    return _fresh

def save_state(state: dict):
    import copy
    with _state_lock:
        state_copy = copy.deepcopy(state)
    tmp_path = STATE_FILE + ".tmp"
    Path(tmp_path).write_text(json.dumps(state_copy, indent=2))
    os.replace(tmp_path, STATE_FILE)
    try:
        import shutil
        shutil.copy2(STATE_FILE, STATE_FILE + ".bak")
    except Exception:
        pass

# Bar-index cooldown: how many 15m bars must pass before the same symbol+direction
# can fire again.  Prevents duplicate signals on consecutive scan runs.
SIGNAL_COOLDOWN_BARS:           int = 4   # Standard cooldown: 4 bars = 1 hour
SIGNAL_COOLDOWN_BARS_HIGHSCORE: int = 2   # High-conviction cooldown: 2 bars = 30 min
SIGNAL_HIGHSCORE_THRESHOLD:     int = 7   # Score at or above this uses shorter cooldown

def check_cooldown(state, coin, direction, bar_index, signal_type: str = "",
                   candidate_score: int = 0) -> bool:
    symbol = coin if coin.endswith("USDT") else coin + "USDT"

    with _state_lock:
        active = list(state.get("active_signals", []))
    for sig in active:
        if sig.get("symbol") == symbol and not sig.get("resolved", False):
            if sig.get("direction") == direction:
                return False

    cooldown_key = f"{symbol}_{direction}"
    with _state_lock:
        last_bar   = state.get("signal_cooldowns",  {}).get(cooldown_key)
        last_sl_ts = state.get("post_loss_cooldown", {}).get(cooldown_key)
    if last_bar is not None:
        bars_elapsed = bar_index - last_bar
        required_bars = (SIGNAL_COOLDOWN_BARS_HIGHSCORE
                         if candidate_score >= SIGNAL_HIGHSCORE_THRESHOLD
                         else SIGNAL_COOLDOWN_BARS)
        if bars_elapsed < required_bars:
            remaining_bars = required_bars - bars_elapsed
            print(f"  [BAR COOLDOWN] {hl_coin(symbol)} {direction.upper()} "
                  f"{signal_type} (score={candidate_score}) blocked — "
                  f"{remaining_bars} bar(s) remaining (need {required_bars})")
            return False

    if last_sl_ts is not None:
        elapsed = int(time.time()) - last_sl_ts
        if elapsed < PULL_REENTRY_COOLDOWN_S:
            remaining = PULL_REENTRY_COOLDOWN_S - elapsed
            print(f"  [POST-LOSS COOLDOWN] {hl_coin(symbol)} {direction.upper()} "
                  f"{signal_type} blocked — {remaining}s remaining after SL")
            return False

    return True

def update_cooldown(state, coin, direction, bar_index):
    # check_cooldown can enforce SIGNAL_COOLDOWN_BARS between same-direction signals.
    symbol = coin if coin.endswith("USDT") else coin + "USDT"
    cooldown_key = f"{symbol}_{direction}"
    with _state_lock:
        state.setdefault("signal_cooldowns", {})[cooldown_key] = bar_index

# TELEGRAM

def stars(score: int) -> str:
    capped = max(0, min(score, 8))
    return "★" * capped + "☆" * (8 - capped)

def send_telegram(text: str) -> int | None:
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
    direction = "long" if sig.fire_long else "short"
    rate      = sig.funding_rate
    tailwind  = False if rate is None else (
        (rate < 0 and direction == "long") or (rate > 0 and direction == "short")
    )
    sig_type_wr = sig.win_rate_data.get("win_rate") or 0.5
    oi_confirm  = sig.oi_trend_data.get("score_adj", 0) > 0
    return (sig.final_score, round(sig_type_wr, 2), int(tailwind), int(oi_confirm))

def format_signal(symbol: str, sig: SignalResult, engine_tag: str = "V5", rank: int = 0) -> str:
    direction = "▲ LONG" if sig.fire_long else "▼ SHORT"
    dir_str   = "long" if sig.fire_long else "short"
    emoji     = "🟢" if sig.fire_long else "🔴"
    ts        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def fmt(v):
        if v >= 1000: return f"{v:,.2f}"
        if v >= 1:    return f"{v:.4f}"
        return f"{v:.6f}"

    pull_entry_block = ""
    if sig.signal_type == "PULL" and sig.limit_entry is not None:
        pct_tgt = int(PULL_LIMIT_TRANCHE_PCT * 100)
        pct_now = 100 - pct_tgt
        pull_entry_block = (
            f"\n<b>📌 Split Entry Plan (PULL)</b>\n"
            f"  Tranche A ({pct_now}%):  Market @ <code>{fmt(sig.entry)}</code>\n"
            f"  Tranche B ({pct_tgt}%):  Limit  @ <code>{fmt(sig.limit_entry)}</code>  "
            f"(EMA21, {sig.limit_entry_dist:.2f}× ATR below)\n"
        )

    spread_line = ""
    if sig.spread_pct is not None:
        spread_tag = ("⚠️ elevated" if sig.spread_pct >= SPREAD_WARN_PCT else "✅ tight")
        spread_line = f"\nSpread/Liquidity: {sig.spread_pct:.3f}%  {spread_tag}"

    def recommended_leverage(atr_pct: float, score: int) -> str:
        if atr_pct <= 0.60:
            low, high = 4, 6
        elif atr_pct <= 1.20:
            low, high = 3, 5
        elif atr_pct <= 2.00:
            low, high = 2, 4
        else:
            low, high = 1, 3
        if score <= 4:
            high = max(low, high - 1)
        return f"{low}x - {high}x"

    lev_range    = recommended_leverage(sig.atr_pct, sig.final_score)
    funding_str  = format_funding(sig.funding_rate, dir_str)
    rate         = sig.funding_rate
    headwind     = rate is not None and (
        (rate > 0 and dir_str == "long") or (rate < 0 and dir_str == "short")
    )
    chk_funding  = "⚠️" if (headwind and rate is not None and abs(rate) >= FUNDING_SUPPRESS_EXTREME) else "✅"

    sr_lines = ""
    if sig.resistances:
        sr_lines += "🔴 Resistance: " + "  |  ".join(f"<code>{fmt(r)}</code>" for r in sig.resistances) + "\n"
    if sig.supports:
        sr_lines += "🟢 Support:    " + "  |  ".join(f"<code>{fmt(s)}</code>" for s in sig.supports) + "\n"

    oi_data      = sig.oi_trend_data
    oi_line      = oi_data.get("label", "OI Trend: Unknown")
    btc_line     = sig.btc_regime_label or "BTC Regime: Unknown"
    breadth_line = sig.breadth_label    or "Market Breadth: Unknown"
    rs_line      = sig.rs_data.get("label", "Relative Strength: N/A")
    wr_line      = sig.win_rate_data.get("label", "Win Rate: N/A")
    macro_line   = sig.macro_data.get("label", "Macro Risk: None") if sig.macro_data else "Macro Risk: None"
    d200_line    = sig.d200_label or "D200: Unknown"
    vol_ratio_line = (f"Volume Ratio: {sig.vol_ratio:.1f}x" if sig.vol_ratio is not None
                      else "Volume Ratio: N/A")

    score_trail = ""
    if sig.score_adjustments:
        parts = []
        for lbl, adj in sig.score_adjustments:
            sign = "+" if adj > 0 else ""
            safe_lbl = lbl.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            parts.append(f"{safe_lbl}: {sign}{adj}")
        score_trail = "\n<i>Adjustments: " + "  |  ".join(parts) + "</i>"

    sr_block     = f"\n{sr_lines}" if sr_lines else ""
    medal        = RANK_MEDALS.get(rank, "")
    rank_tag     = f"{medal} <b>Priority #{rank}</b>\n" if rank else ""
    counter_tag  = "⚠️ " if "counter-trend" in sig.btc_regime_label else ""

    return (
        f"{rank_tag}{counter_tag}{emoji} <b>{direction} [{sig.signal_type}]</b>  {stars(sig.final_score)}\n"
        f"<b>Pair:</b>  {symbol}\n\n"
        f"<b>Entry:</b> <code>{fmt(sig.entry)}</code>\n"
        f"<b>Entry Zone:</b> <code>{fmt(sig.entry_low)}</code> – <code>{fmt(sig.entry_high)}</code>\n"
        f"{pull_entry_block}"
        f"\n<b>TP1:</b>   <code>{fmt(sig.tp1)}</code>\n"
        f"<b>TP2:</b>   <code>{fmt(sig.tp2)}</code>\n"
        f"<b>SL:</b>    <code>{fmt(sig.sl)}</code>\n\n"
        f"<b>Leverage:</b> {lev_range}\n"
        f"<b>Score:</b> {sig.final_score}/{sig.score}+adj  |  {sig.breakdown}\n"
        f"<b>Gates:</b> {sig.v10_gates}\n"
        f"\n<b>Signal Context</b>\n"
        f"{oi_line}\n"
        f"{btc_line}\n"
        f"{breadth_line}\n"
        f"{rs_line}\n"
        f"{wr_line}\n"
        f"{macro_line}\n"
        f"{d200_line}\n"
        f"{vol_ratio_line}"
        f"{spread_line}"
        f"{score_trail}"
        f"{sr_block}\n"
        f"<b>Pre-Trade Checklist</b>\n"
        f"✅ Trend identified (4H/1H/15m aligned)\n"
        f"{'✅' if sig.supports or sig.resistances else '☐'} Key S/R marked\n"
        f"✅ Clear entry signal ({sig.signal_type}, score {sig.final_score}/{sig.score}+adj)\n"
        f"✅ Leverage appropriate ({lev_range})\n"
        f"{chk_funding} {funding_str}\n"
        f"📊 {format_oi(sig.open_interest)}\n\n"
        f"<i>Scalp Swing v12.8.2 [4H/15m] • Hyperliquid Perps • {ts}</i>"
    )

# ACTIVE SIGNAL TRACKING

def track_signal(state: dict, symbol: str, direction: str,
                 msg_id: int, sig: SignalResult, bar_index: int,
                 hist_id: str | None = None):
    active = state.setdefault("active_signals", [])
    active.append({
        "symbol":          symbol,
        "direction":       direction,
        "msg_id":          msg_id,
        "bar_index":       bar_index,
        "signal_bar_time": (int(time.time() * 1000) // 900_000) * 900_000,
        "tp1":             sig.tp1,
        "tp2":             sig.tp2,
        "sl":              sig.sl,
        "tp1_hit":         False,
        "resolved":        False,
        "hist_id":         hist_id,
        "signal_type":     sig.signal_type,
    })

def check_active_signals(state: dict, bar_index_now: int,
                         scan_reference_ms: int | None = None):
    signals = state.get("active_signals", [])
    if not signals:
        return
    ref_ms = scan_reference_ms if scan_reference_ms is not None else int(time.time() * 1000)
    still_active = []
    for sig in signals:
        age = bar_index_now - sig.get("bar_index", bar_index_now)
        if age > SIGNAL_MAX_AGE_BARS:
            print(f"  [TRACK] {sig['symbol']} expired after {age} bars — dropping")
            hist_id = sig.get("hist_id")
            if hist_id:
                update_signal_result(state, hist_id, "expired")
            state.setdefault("resolved_signals", []).append({
                "symbol":      sig["symbol"],
                "direction":   sig.get("direction", ""),
                "outcome":     "expired",
                "resolved_at": int(time.time()),
            })
            # is a sideways/neutral outcome, not a failed breakout. Only SL hits (in
            # resolve_signal("sl")) are recorded as failed breakouts for pattern detection.
            continue
        if sig.get("resolved", False):
            continue

        symbol    = sig["symbol"]
        direction = sig["direction"]
        msg_id    = sig["msg_id"]
        tp1       = sig["tp1"]
        tp2       = sig["tp2"]
        sl        = sig["sl"]
        tp1_hit   = sig.get("tp1_hit", False)
        signal_bar_time_ms = sig.get("signal_bar_time")
        last_processed_ts  = sig.get("last_processed_candle_ts", signal_bar_time_ms or 0)

        try:
            # timestamp and never accidentally includes the currently-forming bar.
            candles = get_candles(symbol, "15m", N_15M,
                                  start_time_ms=signal_bar_time_ms,
                                  reference_ms=ref_ms)
        except Exception as e:
            print(f"  [TRACK] candle fetch failed for {symbol}: {e}")
            still_active.append(sig)
            continue

        if not candles:
            still_active.append(sig)
            continue

        new_candles = [c for c in candles if c["t"] > last_processed_ts]
        if not new_candles:
            still_active.append(sig)
            continue

        hist_id = sig.get("hist_id")

        def resolve_signal(outcome: str):
            if hist_id:
                update_signal_result(state, hist_id, outcome)
            state.setdefault("resolved_signals", []).append({
                "symbol":      symbol,
                "direction":   direction,
                "outcome":     outcome,
                "resolved_at": int(time.time()),
            })
            sig["resolved"] = True
            if outcome == "sl":
                cooldown_key = f"{symbol}_{direction}"
                state.setdefault("post_loss_cooldown", {})[cooldown_key] = int(time.time())
                signal_type = sig.get("signal_type", "UNKNOWN")
                record_failed_breakout(state, symbol, direction, signal_type, bar_index_now)

        for candle in new_candles:
            c_high = candle["h"]
            c_low  = candle["l"]
            last_processed_ts = candle["t"]

            if direction == "long":
                if not tp1_hit and c_high >= tp1:
                    react_to_message(msg_id, REACT_TP1)
                    print(f"  [TRACK] {symbol} TP1 hit → {REACT_TP1}")
                    tp1_hit = True
                    sig["tp1_hit"] = True
                if not tp1_hit and c_low <= sl:
                    react_to_message(msg_id, REACT_SL)
                    print(f"  [TRACK] {symbol} SL hit → {REACT_SL}")
                    resolve_signal("sl")
                    break
                if tp1_hit and c_high >= tp2:
                    react_to_message(msg_id, REACT_TP2)
                    print(f"  [TRACK] {symbol} TP2 hit → {REACT_TP2}")
                    resolve_signal("tp2")
                    break
                if tp1_hit and not sig.get("resolved", False) and c_low <= sl:
                    print(f"  [TRACK] {symbol} SL hit after TP1 — resolving silently")
                    resolve_signal("tp1")
                    break
            else:
                if not tp1_hit and c_low <= tp1:
                    react_to_message(msg_id, REACT_TP1)
                    print(f"  [TRACK] {symbol} TP1 hit → {REACT_TP1}")
                    tp1_hit = True
                    sig["tp1_hit"] = True
                if not tp1_hit and c_high >= sl:
                    react_to_message(msg_id, REACT_SL)
                    print(f"  [TRACK] {symbol} SL hit → {REACT_SL}")
                    resolve_signal("sl")
                    break
                if tp1_hit and c_low <= tp2:
                    react_to_message(msg_id, REACT_TP2)
                    print(f"  [TRACK] {symbol} TP2 hit → {REACT_TP2}")
                    resolve_signal("tp2")
                    break
                if tp1_hit and not sig.get("resolved", False) and c_high >= sl:
                    print(f"  [TRACK] {symbol} SL hit after TP1 — resolving silently")
                    resolve_signal("tp1")
                    break

        if not sig.get("resolved", False):
            sig["last_processed_candle_ts"] = last_processed_ts
            still_active.append(sig)

    state["active_signals"] = still_active

# DAILY SUMMARY

def should_send_summary() -> bool:
    now = datetime.now(timezone.utc)
    return now.hour == 8 and now.minute < 15

def send_summary(state: dict):
    cutoff_24h = int(time.time()) - 86400
    cutoff_48h = int(time.time()) - 172800
    recent     = [e for e in state.get("resolved_signals", []) if e["resolved_at"] >= cutoff_24h]
    tp1_count  = sum(1 for e in recent if e["outcome"] == "tp1")
    tp2_count  = sum(1 for e in recent if e["outcome"] == "tp2")
    sl_count   = sum(1 for e in recent if e["outcome"] == "sl")
    winners    = tp1_count + tp2_count
    losers     = sl_count
    if winners == 0 and losers == 0:
        return

    all_history = [e for e in state.get("signal_history", [])
                    if e.get("result") in ("tp1", "tp2", "sl")]
    total       = len(all_history)
    overall_wr  = None
    if total >= WIN_RATE_MIN_SAMPLE:
        wins       = sum(1 for r in all_history if r.get("result") in ("tp1", "tp2"))
        overall_wr = wins / total

    lines = [
        "📊 Signal Summary (last 24h)",
        f"✅ Winners: {winners} (🔥×{tp1_count}  🏆×{tp2_count})",
        f"❌ Losers:  {losers} (😭×{sl_count})",
    ]
    if overall_wr is not None:
        lines.append(f"📈 Overall Win Rate: {overall_wr*100:.0f}% ({total} trades)")

    def r_pnl(entries):
        return sum(
            +2 if e["result"] == "tp2"
            else +1 if e["result"] == "tp1"
            else -1
            for e in entries if e.get("result") in ("tp1", "tp2", "sl")
        )

    hist_all = [e for e in state.get("signal_history", [])
                if e.get("result") in ("tp1", "tp2", "sl")]

    breakdown_lines = []
    for sig_type in ("BREAK", "PULL"):
        for dirn in ("long", "short"):
            subset = [e for e in hist_all
                      if e.get("signal_type") == sig_type and e.get("direction") == dirn]
            if len(subset) >= 5:
                wins = sum(1 for e in subset if e["result"] in ("tp1", "tp2"))
                wr   = wins / len(subset)
                pnl  = r_pnl(subset)
                breakdown_lines.append(
                    f"  {sig_type} {dirn}: {wr*100:.0f}% WR  {pnl:+d}R  (n={len(subset)})"
                )

    if breakdown_lines:
        lines.append("\n📋 By type / direction:\n" + "\n".join(breakdown_lines))

    send_telegram("\n".join(lines))
    state["resolved_signals"] = [
        e for e in state.get("resolved_signals", []) if e["resolved_at"] >= cutoff_48h
    ]

# SCAN

def collect_market_inputs(symbol: str, state: dict, reference_ms: int) -> tuple | None:
    data = fetch_all_candles(symbol, reference_ms=reference_ms)
    if data is None:
        return None
    candles_15m, candles_1h, candles_4h, candles_d = data
    record_market_inputs_from_candles(symbol, candles_15m, candles_4h)

    # when a symbol is about to return a live signal. This runs unconditionally
    # for every WATCHLIST symbol each scan (Phase 1), so get_funding_trend()
    # gets continuous history to compare against instead of relying on rare,
    # signal-gated writes that left most symbols stuck reporting "stable".
    ctx = get_market_context(symbol)
    if ctx and ctx.get("funding") is not None:
        update_funding_history(state, symbol, ctx.get("funding"))

    return data

def scan_symbol(symbol: str, state: dict, bar_index_now: int,
                candle_bundle: tuple | None = None,
                reference_ms: int | None = None) -> list[tuple]:
    coin = hl_coin(symbol)
    if candle_bundle is None:
        data = fetch_all_candles(symbol, reference_ms=reference_ms)
        if data is None:
            print(f"    Skipping {coin}: insufficient candles")
            return []
    else:
        data = candle_bundle

    candles_15m, candles_1h, candles_4h, candles_d = data

    ctx = get_market_context(symbol)
    if ctx:
        oi_usd_val = ctx.get("open_interest")
        update_oi_history(state, symbol, oi_usd_val)

    live_cache   = get_meta_and_asset_ctxs()
    live_funding = (live_cache.get(hl_coin(symbol), {}).get("funding")
                    if live_cache else None)
    _ctx_funding = ctx.get("funding") if ctx else None
    funding_for_suppress = live_funding if live_funding is not None else _ctx_funding
    if live_funding is not None and live_funding != _ctx_funding:
        pct = (live_funding * 100) if live_funding is not None else float("nan")
        print(f"    [FUNDING REFRESH] {coin}: updated → {pct:+.4f}%/8h")

    sig = compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d,
                          state, record_market_inputs=False, reference_ms=reference_ms,
                          funding_rate=funding_for_suppress)
    if not (sig.fire_long or sig.fire_short):
        print(f"    {coin}: no signal")
        return []

    direction = "long" if sig.fire_long else "short"
    if not check_cooldown(state, symbol, direction, bar_index_now,
                          signal_type=sig.signal_type,
                          candidate_score=sig.final_score):
        print(f"    {coin} signal suppressed by cooldown")
        return []

    print(f"    🚀 SIGNAL: {coin} {direction.upper()} [{sig.signal_type}] "
          f"base={sig.score} final={sig.final_score}")

    if ctx:
        sig.funding_rate  = live_funding if live_funding is not None else ctx.get("funding")
        sig.open_interest = ctx.get("open_interest")
        pct = (sig.funding_rate * 100) if sig.funding_rate is not None else float("nan")
        print(f"    [MARKET CTX] {coin}: funding={pct:+.4f}%/8h  {format_oi(sig.open_interest)}")

    if FUNDING_SUPPRESS_EXTREME is not None and funding_for_suppress is not None:
        rate     = funding_for_suppress
        headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
        if headwind and abs(rate) >= FUNDING_SUPPRESS_EXTREME:
            pct = rate * 100
            print(f"    [FUNDING FILTER] {coin} {direction.upper()} suppressed — "
                  f"funding {pct:+.4f}%/8h is extreme headwind")
            return []

    # this variable and would crash with NameError if triggered first.
    # suppression checks use the regime-adaptive threshold rather than raw MIN_SCORE.
    _breadth_pct_scan = compute_market_breadth()["breadth_50_pct"]
    _eff_min_scan = get_effective_min_score(get_btc_regime(), _breadth_pct_scan)

    # mark_px is the exchange's fair-value mid (order-book derived).  A large
    # divergence from the last 15m close signals wide spread / thin liquidity,
    # which inflates slippage and can make an otherwise-valid signal negative EV.
    if symbol not in SPREAD_EXEMPT:
        live_mark = (live_cache.get(hl_coin(symbol), {}).get("mark_px")
                     if live_cache else None)
        if live_mark is not None and live_mark > 0:
            _close_px = candles_15m[-1]["c"]
            _spread_pct = abs(live_mark - _close_px) / _close_px * 100.0
            sig.spread_pct = _spread_pct
            if _spread_pct >= SPREAD_SUPPRESS_PCT:
                print(f"    [SPREAD FILTER] {coin} {direction.upper()} hard suppressed — "
                      f"mark/close divergence {_spread_pct:.3f}% >= {SPREAD_SUPPRESS_PCT:.2f}%")
                return []
            elif _spread_pct >= SPREAD_WARN_PCT:
                sig.final_score -= 1
                sig.score_adjustments.append(
                    (f"Spread/liquidity warn ({_spread_pct:.3f}%)", -1)
                )
                print(f"    [SPREAD WARN] {coin} {direction.upper()} −1 score — "
                      f"mark/close divergence {_spread_pct:.3f}%")
                if sig.final_score < _eff_min_scan:
                    print(f"    [SPREAD FILTER] {coin} suppressed after spread penalty "
                          f"(final={sig.final_score} < eff_min={_eff_min_scan})")
                    return []

    # The final score gate inside compute_signals (using _effective_min) handles suppression automatically.

    return [(symbol, format_signal(symbol, sig, "CORE"), direction, sig)]

# MAIN

def deduplicate_correlated(signals: list[tuple]) -> list[tuple]:
    seen_groups: set[tuple] = set()
    result: list[tuple]     = []
    for sig_tuple in signals:
        symbol    = sig_tuple[0]
        direction = sig_tuple[2]
        group  = next(
            (g for g, members in CORR_GROUPS.items() if symbol in members),
            symbol
        )
        if (group, direction) not in seen_groups:
            seen_groups.add((group, direction))
            result.append(sig_tuple)
    return result

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Scanner starting…")
    print(f"Watchlist ({len(WATCHLIST)} pairs): {[hl_coin(s) for s in WATCHLIST]}")

    scan_reference_ms = int(time.time() * 1000)
    bar_index_now     = scan_reference_ms // (15 * 60 * 1000)
    state             = load_state()

    prune_state(state)

    reset_breadth_cache()
    reset_rs_cache()
    reset_win_rates_cache()

    print("[INIT] Fetching market context (metaAndAssetCtxs)…")
    get_meta_and_asset_ctxs()

    print("[INIT] Computing BTC regime…")
    try:
        btc_1h = get_candles("BTCUSDT", "1h", N_1H, reference_ms=scan_reference_ms)
        btc_4h = get_candles("BTCUSDT", "4h", N_4H, reference_ms=scan_reference_ms)
        regime = compute_btc_regime(btc_1h, btc_4h)
        set_btc_regime(regime)
        print(f"  {regime['label']}")
    except Exception as e:
        print(f"  [BTC REGIME] failed to compute: {e}")

    print("[TRACK] Checking active signals…")
    check_active_signals(state, bar_index_now, scan_reference_ms)
    save_state(state)

    if should_send_summary():
        send_summary(state)
        save_state(state)

    print("[PHASE 1] Collecting market breadth / RS inputs…")
    candle_bundles: dict[str, tuple] = {}
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {
            ex.submit(collect_market_inputs, sym, state, scan_reference_ms): sym
            for sym in WATCHLIST
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                bundle = fut.result()
                if bundle is not None:
                    candle_bundles[sym] = bundle
            except Exception as e:
                print(f"    ERROR collecting inputs for {sym}: {e}")

    finalize_breadth_cache()
    finalize_rs_cache()
    with _breadth_lock:
        breadth_n = len(_breadth_snapshot or {})
    with _rs_lock:
        rs_n = len(_rs_snapshot or {})
    print(f"  Breadth symbols: {breadth_n}  RS symbols: {rs_n}")

    print("[PHASE 2] Scanning symbols for signals…")
    print("[INIT] Refreshing market context before Phase 2…")
    get_meta_and_asset_ctxs()
    pending_signals: list[tuple] = []
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {
            ex.submit(scan_symbol, sym, state, bar_index_now, candle_bundles.get(sym),
                      scan_reference_ms): sym
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

    save_state(state)

    pending_signals.sort(key=lambda t: priority_score(t[3]), reverse=True)
    deduped_signals = deduplicate_correlated(pending_signals)
    
    _btc_regime_main = get_btc_regime()
    _breadth_pct_main = compute_market_breadth()["breadth_50_pct"]
    _max_signals = get_dynamic_max_signals(_btc_regime_main, _breadth_pct_main)
    print(f"  [DYNAMIC SIGNALS] Max signals this scan: {_max_signals} "
          f"(BTC regime: {_btc_regime_main['label'] if _btc_regime_main else 'Unknown'}, "
          f"breadth: {_breadth_pct_main*100:.0f}%)")
    
    top_signals     = deduped_signals[:_max_signals]
    top_set         = set(id(t) for t in top_signals)
    # Build dropped list from deduped_signals (not pending_signals) so that signals
    # eliminated by the correlation filter are not mislabelled as "lower-priority".
    dropped_signals = [t for t in deduped_signals if id(t) not in top_set]

    if dropped_signals:
        dropped_names = [f"{hl_coin(s)} {d.upper()}" for s, _, d, _ in dropped_signals]
        print(f"  [RANK] Dropped {len(dropped_signals)} lower-priority signal(s): {', '.join(dropped_names)}")
        # [FIX-C7 Part A] Record rank-suppressed signals with sent=False so the win rate
        # system has visibility into the full scored population, not just the top cut.
        # Without this, compute_win_rates() learns only from signals that happened to rank
        # highly, creating a self-reinforcing survivorship loop where early winners
        # permanently inflate score adjustments for their signal type/direction.
        # Note: sent=False entries are excluded from win rate computation by compute_win_rates()
        # (which filters e.get("sent", True)) — they are recorded purely as population data
        # so future audits and analytics can measure the full distribution.
        for symbol, _msg, direction, sig in dropped_signals:
            record_signal_history(
                state, symbol, direction, sig.signal_type, sig.final_score,
                sig.funding_rate, sig.atr_pct,
                sig.oi_trend_data.get("oi_change_pct"),
                sig.alignment_mode,
                sent=False,  # [FIX-C7 Part A] suppressed by rank — not sent via Telegram
            )

    signals_fired = 0
    for rank, (symbol, _msg, direction, sig) in enumerate(top_signals, start=1):
        msg    = format_signal(symbol, sig, "CORE", rank=rank)
        msg_id = send_telegram(msg)

        hist_id = record_signal_history(
            state, symbol, direction, sig.signal_type, sig.final_score,
            sig.funding_rate, sig.atr_pct,
            sig.oi_trend_data.get("oi_change_pct"),
            sig.alignment_mode,
            sent=True,
        )

        if msg_id:
            # send. It previously ran unconditionally right after send_telegram,
            # so a failed/undelivered alert (msg_id=None) still silently set the
            # bar-index cooldown — blocking that symbol+direction from re-firing
            # for SIGNAL_COOLDOWN_BARS even though the user never received it.
            update_cooldown(state, symbol, direction, bar_index_now)
            track_signal(state, symbol, direction, msg_id, sig, bar_index_now, hist_id=hist_id)
            print(f"  [TRACK] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"TP1={sig.tp1:.4f} TP2={sig.tp2:.4f} SL={sig.sl:.4f}")
        else:
            print(f"  [TG FAIL] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"— Telegram send failed, cooldown NOT set (eligible to re-fire next scan)")
        signals_fired += 1
        time.sleep(0.5)

    save_state(state)
    print(f"Scan complete. {signals_fired} signal(s) fired.")

if __name__ == "__main__":
    main()
