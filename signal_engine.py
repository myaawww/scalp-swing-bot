"""
Scalp Swing Bot v11.2.2 – Python Signal Engine
Hyperliquid Perpetuals edition
Timeframe map: 4H bias / 1H middle / 15m execution
Runs on GitHub Actions every 15 min, sends Telegram alerts.
No orders are placed — signal only.
"""

import os, json, time, math, random, threading, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_FF_TZ = ZoneInfo("America/New_York")
OI_EXPECTED_INTERVAL_S: float = 15 * 60

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
WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "TONUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
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
MIN_SCORE            = 4          # [v11.3] restored to 4 — 4-star signals had high observed win rate
TP1_MULT             = 1.2        # widened from 1.0 — more room to breathe
TP2_MULT             = 2.0        # widened from 1.5 — let full winners run
SL_MULT              = 0.85       # tightened from 1.0 — cut losses faster
COOLDOWN_BARS        = 32
GLOBAL_COOLDOWN      = 16
MAX_SIGNALS_PER_SCAN = 3

# ── FILTERS ──────────────────────────────────────────────────
ADX_BREAK_GATE  = 20.0
ADX_SCORE_MIN   = 20.0

# Per-signal-type RSI gates
# BREAK: momentum confirmation — tighter, requires directional conviction
RSI_BREAK_LONG_MIN  = 50.0;  RSI_BREAK_LONG_MAX  = 75.0
RSI_BREAK_SHORT_MIN = 25.0;  RSI_BREAK_SHORT_MAX = 50.0
# PULL: retracement tolerance — wider, allows dip/spike entries
RSI_PULL_LONG_MIN   = 38.0;  RSI_PULL_LONG_MAX   = 65.0
RSI_PULL_SHORT_MIN  = 35.0;  RSI_PULL_SHORT_MAX  = 62.0
# Legacy aliases kept for any code that references them directly (unused in signal logic)
RSI_LONG_MIN    = RSI_PULL_LONG_MIN;   RSI_LONG_MAX  = RSI_BREAK_LONG_MAX
RSI_SHORT_MIN   = RSI_BREAK_SHORT_MIN; RSI_SHORT_MAX = RSI_PULL_SHORT_MAX

VOL_SCORE_MULT  = 1.0
MAX_ATR_PCT     = 10.0
MIN_ATR_PCT     = 0.2
WICK_FILTER     = 0.45
RANGE_PCT_BREAK = 0.30
PULL_ZONE_MULT  = 0.25
TREND_HOLD_BARS = 2
USE_ROLLING_VWAP  = True
ROLLING_VWAP_LEN  = 16

# [SYM 2] D200 is now a soft score bonus (+1 aligned, -1 counter),
# not a hard gate. Set to False to disable entirely.
USE_D200_FILTER   = True
D200_SOFT_ADJ     = 1      # magnitude of bonus/penalty (symmetric)

USE_DAILY_ADX     = True
MIN_DAILY_ADX     = 20.0
PULL_REQUIRES_4H  = True

# ── ACCURACY FILTERS (v3) ─────────────────────────────────────
FUNDING_SUPPRESS_EXTREME: float = 0.0010
SR_CLEARANCE_ATR_MULT: float    = 0.3

# ── OI TREND (v3 / P1) ────────────────────────────────────────
OI_HISTORY_DEPTH: int        = 6
OI_CHANGE_THRESHOLD_PCT: float = 1.0

# ── BREAKOUT VOLUME UPGRADE (P6) ──────────────────────────────
BREAK_VOL_MULT: float = 1.5

# ── RELATIVE STRENGTH (P5) ────────────────────────────────────
RS_TOP_PERCENTILE: float    = 0.20
RS_BOTTOM_PERCENTILE: float = 0.20

# ── MARKET BREADTH (P7) ───────────────────────────────────────
# [SYM 4] Equidistant from 0.50 — was 0.15 / 0.85
BREADTH_WEAK_LONG_THRESHOLD:  float = 0.20
BREADTH_WEAK_SHORT_THRESHOLD: float = 0.80

# ── HISTORICAL WIN RATE (P2) ──────────────────────────────────
WIN_RATE_MIN_SAMPLE: int    = 20
WIN_RATE_HIGH_THRESH: float = 0.65
WIN_RATE_LOW_THRESH: float  = 0.45

# ── ECONOMIC CALENDAR (P4) ────────────────────────────────────
MACRO_WINDOW_BEFORE_MINS: int   = 60
MACRO_WINDOW_AFTER_MINS:  int   = 30
MACRO_HIGH_ATR_SUPPRESS_PCT: float = 3.0
MACRO_CACHE_TTL_S: int          = 3600

# ── CANDLE COUNTS PER TIMEFRAME ──────────────────────────────
N_15M = 300
N_1H  = 60
N_4H  = 80   # bumped from 60 — ADX needs 28+ bars to warm up; 80 gives comfortable margin
N_1D  = 210

# ── REACTION SETTINGS ────────────────────────────────────────
REACT_TP1           = "🔥"
REACT_TP2           = "🏆"
REACT_SL            = "😭"
SIGNAL_MAX_AGE_BARS = 48

# ── HYPERLIQUID ENDPOINT ──────────────────────────────────────
HL_INFO_URL = "https://api.hyperliquid.xyz/info"

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
    return symbol.replace("USDT", "")


def hl_post(payload: dict) -> any:
    max_attempts  = int(os.getenv("HL_MAX_ATTEMPTS", "6"))
    base_sleep_s  = float(os.getenv("HL_BASE_SLEEP_S", "0.75"))
    for attempt in range(max_attempts):
        try:
            global _hl_last_request_ts, _hl_min_interval_s
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
            results[tf] = fut.result()
    return candles_15m, results["1h"], results["4h"], results["1d"]


# ═══════════════════════════════════════════════════════════════
# FUNDING RATE + OPEN INTEREST
# ═══════════════════════════════════════════════════════════════

FUNDING_WARN_EXTREME = 0.0010
FUNDING_WARN_HIGH    = 0.0005

_meta_cache: dict | None = None
_meta_cache_lock = threading.Lock()


def get_meta_and_asset_ctxs() -> dict | None:
    global _meta_cache
    with _meta_cache_lock:
        if _meta_cache is not None:
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
        with _meta_cache_lock:
            _meta_cache = cache
        return cache
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


# ── P1: Real OI Trend ─────────────────────────────────────────

def update_oi_history(state: dict, symbol: str, oi_coins: float | None) -> None:
    if oi_coins is None:
        return
    oi_hist     = state.setdefault("oi_history", {})
    symbol_hist = oi_hist.setdefault(symbol, [])
    symbol_hist.append({"ts": int(time.time()), "oi": oi_coins})
    oi_hist[symbol] = symbol_hist[-OI_HISTORY_DEPTH:]


def compute_oi_trend(state: dict, symbol: str, current_price: float,
                     price_direction: str, trade_direction: str) -> dict:
    oi_hist = state.get("oi_history", {}).get(symbol, [])
    if len(oi_hist) < 2:
        return {
            "oi_trend": "unknown", "oi_change_pct": None,
            "oi_acceleration": None, "score_adj": 0,
            "label": "OI Trend: Unknown", "breakdown_tag": "OI?"
        }
    recent = oi_hist[-1]["oi"]
    prior  = oi_hist[-2]["oi"]
    if prior == 0:
        return {
            "oi_trend": "unknown", "oi_change_pct": None,
            "oi_acceleration": None, "score_adj": 0,
            "label": "OI Trend: Unknown", "breakdown_tag": "OI?"
        }
    raw_change_pct = (recent - prior) / prior * 100.0
    elapsed_s      = max(1.0, oi_hist[-1]["ts"] - oi_hist[-2]["ts"])
    scale          = OI_EXPECTED_INTERVAL_S / elapsed_s
    oi_change_pct  = raw_change_pct * scale

    oi_acceleration = None
    if len(oi_hist) >= 3:
        prior2 = oi_hist[-3]["oi"]
        if prior2 != 0:
            prev_elapsed = max(1.0, oi_hist[-2]["ts"] - oi_hist[-3]["ts"])
            prev_raw     = (prior - prior2) / prior2 * 100.0
            prev_change  = prev_raw * (OI_EXPECTED_INTERVAL_S / prev_elapsed)
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
            score_adj, condition, breakdown_tag = 1,  "Bullish Confirmation",          "OI↑"
        elif bearish_confirm or bullish_div:
            score_adj, condition, breakdown_tag = -1, "Counter/Divergence vs Long",    "OI Divergence"
        else:
            score_adj, condition, breakdown_tag = 0,  "Neutral",                       "OI→"
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


SR_PIVOT_LEFT_BARS  = 3   # widened from 2 — reduces noise from single wicks
SR_PIVOT_RIGHT_BARS = 3   # widened from 2
SR_CLUSTER_ATR_MULT = 0.3  # merge pivots within 0.3 ATR of each other


def _cluster_levels(pivots: list[float], atr_val: float, tolerance: float = 0.3) -> list[float]:
    """Merge nearby pivot levels into zones so each zone represents a real S/R area."""
    if not pivots or atr_val <= 0:
        return pivots
    zones: list[float] = []
    for p in sorted(pivots):
        if zones and abs(p - zones[-1]) < atr_val * tolerance:
            zones[-1] = (zones[-1] + p) / 2.0   # merge into midpoint
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
    window       = candles_15m[-101:-1]
    for i in range(lb, len(window) - rb):
        h = window[i]["h"]
        l = window[i]["l"]
        if all(h > window[i - k]["h"] for k in range(1, lb + 1)) and \
           all(h > window[i + k]["h"] for k in range(1, rb + 1)):
            pivots_high.append(h)
        if all(l < window[i - k]["l"] for k in range(1, lb + 1)) and \
           all(l < window[i + k]["l"] for k in range(1, rb + 1)):
            pivots_low.append(l)

    # cluster nearby pivots into zones
    eff_atr = atr_val if atr_val and atr_val > 0 else (cur * 0.005)
    pivots_high = _cluster_levels(pivots_high, eff_atr, SR_CLUSTER_ATR_MULT)
    pivots_low  = _cluster_levels(pivots_low,  eff_atr, SR_CLUSTER_ATR_MULT)

    resistances = sorted([p for p in pivots_high if p > cur], key=lambda x: x - cur)[:n_levels]
    supports    = sorted([p for p in pivots_low  if p < cur], key=lambda x: cur - x)[:n_levels]
    return supports, resistances


# ═══════════════════════════════════════════════════════════════
# P3: BTC MARKET REGIME FILTER  [SYM 3 applied here]
# ═══════════════════════════════════════════════════════════════

_btc_regime_cache: dict | None = None
_btc_regime_lock  = threading.Lock()


def compute_btc_regime(candles_1h: list[dict], candles_4h: list[dict]) -> dict:
    def _arr(candles):
        return [c["c"] for c in candles]

    c4h = _arr(candles_4h)
    c1h = _arr(candles_1h)

    ef4h = safe(ema(c4h, FAST_LEN)[-2])
    es4h = safe(ema(c4h, SLOW_LEN)[-2])
    ef1h = safe(ema(c1h, FAST_LEN)[-2])
    es1h = safe(ema(c1h, SLOW_LEN)[-2])

    btc_4h_momentum = len(c4h) >= 6 and c4h[-2] > c4h[-5]   # 4H close up over last 3 bars

    btc_bullish = (ef4h > es4h) and (ef1h > es1h) and btc_4h_momentum
    btc_bearish = (ef4h < es4h) and (ef1h < es1h) and not btc_4h_momentum

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


# Coins known to decorrelate from BTC — exempt from counter-trend penalty
LOW_BTC_CORR = {"TAOUSDT", "TRXUSDT", "PENDLEUSDT", "ONDOUSDT", "HYPEUSDT"}


def check_btc_regime_filter(direction: str, symbol: str) -> tuple[int, str]:
    # Returns score adj: +1 tailwind, -1 counter-trend, 0 mixed/exempt/BTC
    if hl_coin(symbol) == "BTC":
        return 0, "BTC Regime: N/A (BTC itself)"

    regime = get_btc_regime()
    if regime is None:
        return 0, "BTC Regime: Unknown"

    label = regime["label"]

    # Counter-trend penalty — equal for both directions
    if direction == "long" and regime["bearish"]:
        if symbol in LOW_BTC_CORR:
            return 0, f"{label} — counter-trend (exempt, decorrelated)"
        return -1, f"{label} — counter-trend (-1)"

    if direction == "short" and regime["bullish"]:
        if symbol in LOW_BTC_CORR:
            return 0, f"{label} — counter-trend (exempt, decorrelated)"
        return -1, f"{label} — counter-trend (-1)"

    # Tailwind reward
    if direction == "long" and regime["bullish"]:
        return +1, f"{label} — tailwind (+1)"
    if direction == "short" and regime["bearish"]:
        return +1, f"{label} — tailwind (+1)"

    return 0, f"{label} — Mixed (0)"


# ═══════════════════════════════════════════════════════════════
# P7: MARKET BREADTH FILTER  [SYM 4 thresholds applied]
# ═══════════════════════════════════════════════════════════════

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


def apply_breadth_adjustment(direction: str) -> tuple[int, str]:
    breadth = compute_market_breadth()
    pct     = breadth["breadth_50_pct"]
    label   = breadth["label"]
    adj     = 0
    if direction == "long" and pct < BREADTH_WEAK_LONG_THRESHOLD:
        adj    = -1
        label += " (-1)"
    elif direction == "short" and pct > BREADTH_WEAK_SHORT_THRESHOLD:
        adj    = -1
        label += " (-1)"
    return adj, label


# ═══════════════════════════════════════════════════════════════
# P5: RELATIVE STRENGTH RANKING
# ═══════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════
# P2: HISTORICAL PERFORMANCE ANALYTICS
# ═══════════════════════════════════════════════════════════════

def record_signal_history(state: dict, symbol: str, direction: str,
                           signal_type: str, score: int,
                           funding_rate: float | None, atr_pct: float,
                           oi_change_pct: float | None) -> str:
    hist     = state.setdefault("signal_history", [])
    entry_id = f"{symbol}_{int(time.time())}"
    hist.append({
        "id":            entry_id,
        "symbol":        symbol,
        "direction":     direction,
        "signal_type":   signal_type,
        "score":         score,
        "funding_rate":  funding_rate,
        "atr_pct":       atr_pct,
        "oi_change_pct": oi_change_pct,
        "result":        None,
        "timestamp":     int(time.time()),
    })
    if len(hist) > 500:
        state["signal_history"] = hist[-500:]
    return entry_id


def update_signal_result(state: dict, signal_id: str, result: str) -> None:
    for entry in state.get("signal_history", []):
        if entry.get("id") == signal_id:
            entry["result"] = result
            return


def compute_win_rates(state: dict) -> dict:
    hist = [e for e in state.get("signal_history", [])
            if e.get("result") in ("tp1", "tp2", "sl")]
    wrs: dict = {"by_symbol": {}, "by_type": {}, "by_score": {}, "by_direction": {}}

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
                                signal_type: str, score: int) -> dict:
    wrs      = get_cached_win_rates(state)
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
    return {"win_rate": wr, "sample_size": n, "score_adj": score_adj,
            "label": f"Win Rate: {wr*100:.0f}%  Sample: {n}"}


# ═══════════════════════════════════════════════════════════════
# P4: ECONOMIC CALENDAR FILTER
# ═══════════════════════════════════════════════════════════════

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
    try:
        resp = requests.get(MACRO_CALENDAR_URL, timeout=10)
        resp.raise_for_status()
        raw_events = resp.json()
    except Exception as e:
        print(f"  [MACRO CAL] fetch failed: {e} — using cached/empty")
        return cache_entry.get("events", [])
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


def apply_macro_filter(state: dict, atr_pct: float) -> dict:
    events      = fetch_macro_calendar(state)
    now_utc     = datetime.now(timezone.utc)
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


# ═══════════════════════════════════════════════════════════════
# INDICATOR MATH
# ═══════════════════════════════════════════════════════════════

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
        self.score_adjustments: list[tuple[str, int]] = []


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


def compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d,
                    state: dict, record_market_inputs: bool = True) -> SignalResult:
    res = SignalResult()

    def arrays(candles):
        return ([c["o"] for c in candles], [c["h"] for c in candles],
                [c["l"] for c in candles], [c["c"] for c in candles],
                [c["v"] for c in candles])

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

    cur_c = c15[-1]; cur_o = o15[-1]
    cur_h = h15[-1]; cur_l = l15[-1]
    cur_v = v15[-1]
    prev_l = l15[-2]; prev_h = h15[-2]
    ef15_prev = safe(ema_f15[-2])

    atr_val  = a15 if a15 > 0 else (cur_c * 0.03 / 100)
    atr_pct  = atr_val / cur_c * 100
    market_ok = MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT

    rv         = safe(rvwap15[-1])
    vwap_long  = cur_c > rv if USE_ROLLING_VWAP else True
    vwap_short = cur_c < rv if USE_ROLLING_VWAP else True

    # [v11] Per-signal-type RSI gates
    rsi_break_long  = RSI_BREAK_LONG_MIN  <= r15 <= RSI_BREAK_LONG_MAX
    rsi_break_short = RSI_BREAK_SHORT_MIN <= r15 <= RSI_BREAK_SHORT_MAX
    rsi_pull_long   = RSI_PULL_LONG_MIN   <= r15 <= RSI_PULL_LONG_MAX
    rsi_pull_short  = RSI_PULL_SHORT_MIN  <= r15 <= RSI_PULL_SHORT_MAX

    bb_long  = cur_c > bb_basis
    bb_short = cur_c < bb_basis

    obv_slope_long  = obv15[-1] > obv15[-(1 + OBV_LEN)]
    obv_slope_short = obv15[-1] < obv15[-(1 + OBV_LEN)]

    adx_score_ok = adx15 >= ADX_SCORE_MIN
    vol_score_ok = True if vm15 == 0 else (cur_v >= vm15 * VOL_SCORE_MULT)

    vol_break_ok = True if vm15 == 0 else (cur_v >= vm15 * BREAK_VOL_MULT)
    vol_ratio    = (cur_v / vm15) if vm15 > 0 else None

    # ── 1H ───────────────────────────────────────────────────
    o1h, h1h, l1h, c1h, v1h = arrays(candles_1h)
    ema_f1h = ema(c1h, FAST_LEN)
    ema_s1h = ema(c1h, SLOW_LEN)
    ef1h = safe(ema_f1h[-2]); es1h = safe(ema_s1h[-2])
    h1_bull = ef1h > es1h
    h1_bear = ef1h < es1h

    # ── 4H ───────────────────────────────────────────────────
    o4h, h4h, l4h, c4h, v4h = arrays(candles_4h)
    ema_f4h = ema(c4h, FAST_LEN)
    ema_s4h = ema(c4h, SLOW_LEN)
    ef4h = safe(ema_f4h[-2]); es4h = safe(ema_s4h[-2])
    _, _, adx4h_arr = adx_dmi(h4h, l4h, c4h, ADX_LEN)
    adx4h  = safe(adx4h_arr[-2], 25.0)
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

    # ── Daily 200 EMA + Daily ADX ────────────────────────────
    # [SYM 2] D200 computed for soft score adjustment — no longer a hard gate
    if candles_d and len(candles_d) >= TREND_LEN:
        o_d, h_d, l_d, c_d, v_d = arrays(candles_d)
        ema_d200 = ema(c_d, TREND_LEN)
        d200     = safe(ema_d200[-2])
        _, _, adx_d_arr = adx_dmi(h_d, l_d, c_d, ADX_LEN)
        adx_daily = safe(adx_d_arr[-2], 25.0)
    else:
        d200      = cur_c   # neutral fallback — no bonus either way
        adx_daily = 25.0

    # Soft D200 alignment flags (used in scoring, not as gates)
    d200_above = cur_c > d200   # favours longs
    d200_below = cur_c < d200   # favours shorts

    daily_adx_ok = adx_daily >= MIN_DAILY_ADX if USE_DAILY_ADX else True

    # ── Alignment gates ───────────────────────────────────────
    # D200 hard gate REMOVED. full_long_align / full_short_align now depend
    # only on 4H trend, trend hold, and 1H EMA alignment.
    full_long_align  = h4_bull and h4_trend_held_bull and h1_bull
    full_short_align = h4_bear and h4_trend_held_bear and h1_bear

    pull_long_align  = (h4_bull and h4_trend_held_bull and h1_bull) \
                        if PULL_REQUIRES_4H else (h4_trend_held_bull and h1_bull)
    pull_short_align = (h4_bear and h4_trend_held_bear and h1_bear) \
                        if PULL_REQUIRES_4H else (h4_trend_held_bear and h1_bear)

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

    # Signal conditions — D200 macro gate removed; D200 is now a soft score adj
    long_break  = (daily_adx_ok and full_long_align  and break_bull_bar
                   and adx_break_ok and vwap_long  and rsi_break_long
                   and long_score  >= (MIN_SCORE - 1) and market_ok and vol_break_ok)
    short_break = (daily_adx_ok and full_short_align and break_bear_bar
                   and adx_break_ok and vwap_short and rsi_break_short
                   and short_score >= (MIN_SCORE - 1) and market_ok and vol_break_ok)

    long_pull  = (daily_adx_ok and pull_long_align  and pull_touched_long
                  and pull_recover_long  and pull_bull_bar and vwap_long  and rsi_pull_long
                  and long_score  >= (MIN_SCORE - 1) and market_ok)
    short_pull = (daily_adx_ok and pull_short_align and pull_touched_short
                  and pull_recover_short and pull_bear_bar and vwap_short and rsi_pull_short
                  and short_score >= (MIN_SCORE - 1) and market_ok)

    long_sig  = long_break  or long_pull
    short_sig = short_break or short_pull

    def make_breakdown(is_long, oi_tag: str = "OI?", vol_ratio_val=None):
        bb_s  = "BB✓"  if (bb_long  if is_long else bb_short)  else "BB✗"
        d_s   = "4H✓"  if (h4_bull and h4_trend_held_bull if is_long else h4_bear and h4_trend_held_bear) else "4H✗"
        adx_s = "ADX✓" if adx_score_ok  else "ADX✗"
        obv_s = "OBV✓" if (obv_slope_long if is_long else obv_slope_short) else "OBV✗"
        vol_s = "VOL✓" if vol_score_ok   else "VOL✗"
        return f"{bb_s} {d_s} {adx_s} {obv_s} {vol_s} {oi_tag}"

    def make_gates(is_long):
        h1 = h1_bull if is_long else h1_bear
        return (f"DailyADX:{'✓' if daily_adx_ok else '✗'} "
                f"4H:{'✓' if (full_long_align if is_long else full_short_align) else '✗'} "
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
        res.v10_gates    = make_gates(True)
        res.vol_ratio    = vol_ratio
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
        res.v10_gates    = make_gates(False)
        res.vol_ratio    = vol_ratio

    if not (res.fire_long or res.fire_short):
        return res

    direction = "long" if res.fire_long else "short"
    price_dir = "up" if cur_c > cur_o else "down"

    # ════════════════════════════════════════════════════════
    # FINAL SCORING STACK
    # 1. OI  2. BTC+Breadth  3. RS  4. Win Rate
    # 5. Macro  6. Volume (BREAK)  7. D200 soft bonus
    # ════════════════════════════════════════════════════════
    adjusted_score = res.score
    adjs = res.score_adjustments

    # ── Step 1: OI Confirmation ───────────────────────────────
    oi_data = compute_oi_trend(state, symbol, cur_c, price_dir, direction)
    res.oi_trend_data = oi_data
    adjusted_score += oi_data["score_adj"]
    if oi_data["score_adj"] != 0:
        adjs.append((f"OI ({oi_data['breakdown_tag']})", oi_data["score_adj"]))

    # OI acceleration as independent score component (uses the 3-reading window)
    oi_acceleration = oi_data.get("oi_acceleration")
    oi_trend        = oi_data.get("oi_trend", "unknown")
    if oi_acceleration is not None:
        if oi_acceleration > 0 and oi_trend == "rising" and oi_data["score_adj"] > 0:
            adjusted_score += 1
            adjs.append(("OI Acceleration (confirming↑)", 1))
        elif oi_acceleration < 0 and oi_trend == "falling" and oi_data["score_adj"] < 0:
            adjusted_score -= 1
            adjs.append(("OI Acceleration (diverging↓)", -1))

    # ── Step 2: BTC Regime + Market Breadth ──────────────────
    btc_adj, btc_label = check_btc_regime_filter(direction, symbol)
    res.btc_regime_label = btc_label
    adjusted_score += btc_adj
    if btc_adj != 0:
        adjs.append((btc_label, btc_adj))

    breadth_adj, breadth_label = apply_breadth_adjustment(direction)
    res.breadth_label = breadth_label
    adjusted_score += breadth_adj
    if breadth_adj != 0:
        adjs.append((breadth_label, breadth_adj))

    # ── Step 3: Relative Strength ─────────────────────────────
    rs_data = compute_relative_strength(symbol)
    res.rs_data = rs_data
    adjusted_score += rs_data["score_adj"]
    if rs_data["score_adj"] != 0:
        adjs.append((rs_data["label"], rs_data["score_adj"]))

    # ── Step 4: Historical Win Rate ───────────────────────────
    wr_data = compute_win_rate_analytics(state, symbol, direction, res.signal_type, res.score)
    res.win_rate_data = wr_data
    adjusted_score += wr_data["score_adj"]
    if wr_data["score_adj"] != 0:
        adjs.append((wr_data["label"], wr_data["score_adj"]))

    # ── Step 5: Macro Filter ──────────────────────────────────
    macro_data = apply_macro_filter(state, atr_pct)
    res.macro_data = macro_data
    if macro_data["hard_suppress"]:
        print(f"  [MACRO FILTER] {symbol} hard suppressed — elevated ATR + macro risk window")
        res.fire_long  = False
        res.fire_short = False
        return res
    adjusted_score += macro_data["score_adj"]
    if macro_data["score_adj"] != 0:
        adjs.append(("Macro Risk Window", macro_data["score_adj"]))

    # ── Step 6: Volume Confirmation (BREAK only) ──────────────
    if res.signal_type == "BREAK" and not vol_break_ok:
        adjusted_score -= 1
        adjs.append(("Vol (BREAK threshold not met)", -1))

    # ── Step 7: D200 Soft Bonus/Penalty [SYM 2] ──────────────
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

    # ── Final suppression check ───────────────────────────────
    res.final_score = adjusted_score
    if adjusted_score < MIN_SCORE:
        print(f"  [SCORE FILTER] {symbol} {direction.upper()} suppressed: "
              f"base={res.score} → final={adjusted_score} < {MIN_SCORE}")
        res.fire_long  = False
        res.fire_short = False
        return res

    res.breakdown = make_breakdown(res.fire_long, oi_data["breakdown_tag"], vol_ratio)

    # ── Support / Resistance ──────────────────────────────────
    if res.fire_long or res.fire_short:
        res.supports, res.resistances = find_sr_levels(candles_15m, atr_val=atr_val)

    # ── S/R clearance filter ──────────────────────────────────
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


# ═══════════════════════════════════════════════════════════════
# COOLDOWN STATE
# ═══════════════════════════════════════════════════════════════

def load_state() -> dict:
    if Path(STATE_FILE).exists():
        try:
            s = json.loads(Path(STATE_FILE).read_text())
            s.setdefault("oi_history", {})
            s.setdefault("signal_history", [])
            s.setdefault("macro_calendar_cache", {})
            return s
        except Exception:
            pass
    return {"oi_history": {}, "signal_history": [], "macro_calendar_cache": {}}


def save_state(state: dict):
    import tempfile, os as _os
    tmp_path = STATE_FILE + ".tmp"
    Path(tmp_path).write_text(json.dumps(state, indent=2))
    _os.replace(tmp_path, STATE_FILE)


def check_cooldown(state, coin, direction, bar_index) -> bool:
    """
    [v11.3] Outcome-based cooldown — direction-aware.
    A coin is locked only while it has an unresolved active signal in the
    same direction. An unresolved LONG does NOT block a SHORT, and vice versa.
    The coin unlocks the moment TP2 or SL is hit (signal resolved by tracker).
    bar_index retained in signature for drop-in compatibility but unused.
    """
    symbol = coin if coin.endswith("USDT") else coin + "USDT"
    active = state.get("active_signals", [])
    for sig in active:
        if sig.get("symbol") == symbol and not sig.get("resolved", False):
            if sig.get("direction") == direction:
                return False   # same direction still open — block
    return True


def update_cooldown(state, coin, direction, bar_index):
    """[v11.3] No-op — cooldown is now driven purely by active signal resolution."""
    pass


# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

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
    is_break   = sig.signal_type == "BREAK"
    oi_confirm = sig.oi_trend_data.get("score_adj", 0) > 0
    return (sig.final_score, int(is_break), int(tailwind), int(oi_confirm))


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
            high = max(low, high - 2)
        return f"{low}x - {high}x"

    lev_range    = recommended_leverage(sig.atr_pct, sig.final_score)
    funding_str  = format_funding(sig.funding_rate, dir_str)
    rate         = sig.funding_rate
    headwind     = rate is not None and (
        (rate > 0 and dir_str == "long") or (rate < 0 and dir_str == "short")
    )
    chk_funding  = "⚠️" if (headwind and rate is not None and abs(rate) >= FUNDING_WARN_EXTREME) else "✅"

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
            parts.append(f"{lbl}: {sign}{adj}")
        score_trail = "\n<i>Adjustments: " + "  |  ".join(parts) + "</i>"

    sr_block     = f"\n{sr_lines}" if sr_lines else ""
    medal        = RANK_MEDALS.get(rank, "")
    rank_tag     = f"{medal} <b>Priority #{rank}</b>\n" if rank else ""
    counter_tag  = "⚠️ " if "counter-trend" in sig.btc_regime_label else ""

    return (
        f"{rank_tag}{counter_tag}{emoji} <b>{direction} [{sig.signal_type}]</b>  {stars(sig.final_score)}\n"
        f"<b>Pair:</b>  {symbol}\n\n"
        f"<b>Entry:</b> <code>{fmt(sig.entry)}</code>\n"
        f"<b>Entry Zone:</b> <code>{fmt(sig.entry_low)}</code> – <code>{fmt(sig.entry_high)}</code>\n\n"
        f"<b>TP1:</b>   <code>{fmt(sig.tp1)}</code>\n"
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
        f"{score_trail}"
        f"{sr_block}\n"
        f"<b>Pre-Trade Checklist</b>\n"
        f"✅ Trend identified (4H/1H/15m aligned)\n"
        f"{'✅' if sig.supports or sig.resistances else '☐'} Key S/R marked\n"
        f"✅ Clear entry signal ({sig.signal_type}, score {sig.final_score}/{sig.score}+adj)\n"
        f"✅ Leverage appropriate ({lev_range})\n"
        f"{chk_funding} {funding_str}\n"
        f"📊 {format_oi(sig.open_interest)}\n\n"
        f"<i>Scalp Swing v11.2.2 [4H/15m] • Hyperliquid Perps • {ts}</i>"
    )


# ═══════════════════════════════════════════════════════════════
# ACTIVE SIGNAL TRACKING
# ═══════════════════════════════════════════════════════════════

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
    })


def check_active_signals(state: dict, bar_index_now: int):
    signals = state.get("active_signals", [])
    if not signals:
        return
    still_active = []
    for sig in signals:
        age = bar_index_now - sig.get("bar_index", bar_index_now)
        if age > SIGNAL_MAX_AGE_BARS:
            print(f"  [TRACK] {sig['symbol']} expired after {age} bars — dropping")
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
            candles = get_candles(symbol, "15m", N_15M, start_time_ms=signal_bar_time_ms)
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

        for candle in new_candles:
            c_high = candle["h"]
            c_low  = candle["l"]
            last_processed_ts = candle["t"]

            if direction == "long":
                if not tp1_hit and c_low <= sl:
                    react_to_message(msg_id, REACT_SL)
                    print(f"  [TRACK] {symbol} SL hit → {REACT_SL}")
                    resolve_signal("sl")
                    break
                if not tp1_hit and c_high >= tp1:
                    react_to_message(msg_id, REACT_TP1)
                    print(f"  [TRACK] {symbol} TP1 hit → {REACT_TP1}")
                    tp1_hit = True
                    sig["tp1_hit"] = True
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
                if not tp1_hit and c_high >= sl:
                    react_to_message(msg_id, REACT_SL)
                    print(f"  [TRACK] {symbol} SL hit → {REACT_SL}")
                    resolve_signal("sl")
                    break
                if not tp1_hit and c_low <= tp1:
                    react_to_message(msg_id, REACT_TP1)
                    print(f"  [TRACK] {symbol} TP1 hit → {REACT_TP1}")
                    tp1_hit = True
                    sig["tp1_hit"] = True
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


# ═══════════════════════════════════════════════════════════════
# DAILY SUMMARY
# ═══════════════════════════════════════════════════════════════

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

    send_telegram("\n".join(lines))
    state["resolved_signals"] = [
        e for e in state.get("resolved_signals", []) if e["resolved_at"] >= cutoff_48h
    ]


# ═══════════════════════════════════════════════════════════════
# SCAN
# ═══════════════════════════════════════════════════════════════

def collect_market_inputs(symbol: str, _state: dict, reference_ms: int) -> tuple | None:
    data = fetch_all_candles(symbol, reference_ms=reference_ms)
    if data is None:
        return None
    candles_15m, candles_1h, candles_4h, candles_d = data
    record_market_inputs_from_candles(symbol, candles_15m, candles_4h)
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
        oi_coins = ctx.get("open_interest_coins")
        update_oi_history(state, symbol, oi_coins)

    sig = compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d,
                          state, record_market_inputs=False)
    if not (sig.fire_long or sig.fire_short):
        print(f"    {coin}: no signal")
        return []

    direction = "long" if sig.fire_long else "short"
    if not check_cooldown(state, symbol, direction, bar_index_now):
        print(f"    {coin} signal suppressed by cooldown")
        return []

    print(f"    🚀 SIGNAL: {coin} {direction.upper()} [{sig.signal_type}] "
          f"base={sig.score} final={sig.final_score}")

    if ctx:
        sig.funding_rate  = ctx.get("funding")
        sig.open_interest = ctx.get("open_interest")
        pct = (sig.funding_rate * 100) if sig.funding_rate is not None else float("nan")
        print(f"    [MARKET CTX] {coin}: funding={pct:+.4f}%/8h  {format_oi(sig.open_interest)}")

    if FUNDING_SUPPRESS_EXTREME is not None and sig.funding_rate is not None:
        rate     = sig.funding_rate
        headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
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

    scan_reference_ms = int(time.time() * 1000)
    bar_index_now     = scan_reference_ms // (15 * 60 * 1000)
    state             = load_state()

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
    check_active_signals(state, bar_index_now)
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

    print("[PHASE 3] Scanning symbols for signals…")
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

        hist_id = record_signal_history(
            state, symbol, direction, sig.signal_type, sig.final_score,
            sig.funding_rate, sig.atr_pct,
            sig.oi_trend_data.get("oi_change_pct")
        )

        if msg_id:
            track_signal(state, symbol, direction, msg_id, sig, bar_index_now, hist_id=hist_id)
            print(f"  [TRACK] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"TP1={sig.tp1:.4f} TP2={sig.tp2:.4f} SL={sig.sl:.4f}")
        signals_fired += 1
        time.sleep(0.5)

    save_state(state)
    print(f"Scan complete. {signals_fired} signal(s) fired.")


if __name__ == "__main__":
    main()
