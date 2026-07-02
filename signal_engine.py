"""
CASTELLAN PROTOCOL v1.0.0
--------------------------------------------------------------------
An institutional Smart Money Concept execution engine trading a single,
non-negotiable sequence on Hyperliquid perpetuals:

    H4 HTF Bias
      -> H4 Institutional Point of Interest (POI)
        -> H4 Swing Failure Pattern (liquidity sweep)
          -> M15 Market Structure Shift (confirmation)
            -> Breaker / Mitigation Block entry
              -> Structure-based Stop Loss
                -> Minimum 2R Target, dynamically extended to 3R / 4R

No EMA crossover systems, no RSI/MACD entries, no breakout or pullback
systems, no scoring/voting, no machine learning. One setup, executed
with discipline. Signal quality is prioritized over signal frequency.

Infrastructure (Hyperliquid REST API, hardcoded watchlist, Telegram
delivery + reaction system, cron-per-run scan architecture, state.json
persistence) mirrors the operator's existing engines. The trading model
itself is entirely original to Castellan Protocol.
--------------------------------------------------------------------
"""

import os, json, time, math, random, threading, requests, sys, copy
import signal as os_signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

__version__ = "1.0.0"
ENGINE_NAME = "Castellan Protocol"

# ── CONFIG ──────────────────────────────────────────────────────────
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

STATE_FILE     = "state.json"
SCAN_WORKERS   = int(os.getenv("SCAN_WORKERS", "2"))
HL_INFO_URL    = "https://api.hyperliquid.xyz/info"

HL_MIN_INTERVAL_S     = float(os.getenv("HL_MIN_INTERVAL_S", "0.18"))
HL_MIN_INTERVAL_MAX_S = float(os.getenv("HL_MIN_INTERVAL_MAX_S", "0.60"))
HL_TF_WORKERS         = int(os.getenv("HL_TF_WORKERS", "1"))

_hl_request_lock    = threading.Lock()
_hl_last_request_ts = 0.0
_hl_min_interval_s  = HL_MIN_INTERVAL_S
_hl_consecutive_successes = 0
_hl_session = requests.Session()
_state_lock = threading.Lock()

# ── HARDCODED WATCHLIST (shared with the operator's other engines) ──
WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]

INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}

N_M15 = 300     # ~3.1 days of M15 — plenty for post-sweep MSS confirmation
N_H4  = 220     # ~36 days of H4 — bias + POI lookback
N_D   = 400     # ~13 months of daily — PDH/PDL/PWH/PWL/PMH/PML

REACT_TP1 = "🔥"
REACT_TP2 = "🏆"
REACT_SL  = "😭"

STATE_VERSION = 1

# ── STRATEGY CONSTANTS ───────────────────────────────────────────────
PIVOT_LEFT_H4          = 3
PIVOT_RIGHT_H4         = 3
PIVOT_LEFT_M15         = 2
PIVOT_RIGHT_M15        = 2
H4_POI_LOOKBACK        = 120
H4_LIQUIDITY_TOLERANCE = 0.0012      # 0.12% price tolerance for equal highs/lows clustering
POI_CONFLUENCE_BUFFER_ATR_MULT = 0.30
OB_DISPLACEMENT_ATR_MULT = 1.30
OB_BOS_LOOKBACK         = 3
FVG_MIN_GAP_ATR_MULT    = 0.05

SFP_MAX_SWEEP_DEPTH_ATR_MULT = 1.60   # reject SFPs that sweep too far (runaway, not a rejection)
SFP_MIN_WICK_RATIO      = 0.33        # rejection wick must be a meaningful share of the candle range
SFP_LOOKBACK_H4_BARS    = 24          # only consider SFPs from the most recent ~4 days of H4

MSS_MAX_WAIT_HOURS      = 30          # M15 confirmation must arrive within this window of the H4 SFP
MSS_DISPLACEMENT_ATR_MULT = 1.15
MSS_MIN_CLOSE_MARGIN_ATR_MULT = 0.08  # confirmation close must clear the swing point by this margin
BREAKER_SEARCH_BARS     = 6

SL_BUFFER_ATR_MULT      = 0.20
MIN_RR                  = 2.0
EXT_RR_LEVELS           = (2.0, 3.0, 4.0)
LIQUIDITY_ROOM_BUFFER_ATR_MULT = 0.50

SIGNAL_MAX_AGE_BARS_M15 = 96          # 24h max life for a tracked, unresolved signal
PENDING_SETUP_MAX_AGE_HOURS = 48      # drop an unconfirmed H4 SFP after this long

MAX_CONCURRENT_ACTIVE_SIGNALS = 10
MAX_SIGNALS_PER_SCAN = 2
SIGNAL_COOLDOWN_H4_BARS = 2

_shutdown = False
def _handle_sigterm(signum, frame):
    global _shutdown
    _shutdown = True
os_signal.signal(os_signal.SIGTERM, _handle_sigterm)
os_signal.signal(os_signal.SIGINT, _handle_sigterm)


# ══════════════════════════════════════════════════════════════════
# HYPERLIQUID API LAYER
# ══════════════════════════════════════════════════════════════════

def hl_coin(symbol: str) -> str:
    return symbol.replace("USDT", "")

def hl_post(payload: dict):
    global _hl_last_request_ts, _hl_min_interval_s, _hl_consecutive_successes
    max_attempts = int(os.getenv("HL_MAX_ATTEMPTS", "6"))
    base_sleep_s = float(os.getenv("HL_BASE_SLEEP_S", "0.75"))
    for attempt in range(max_attempts):
        try:
            with _hl_request_lock:
                now = time.time()
                wait_s = _hl_min_interval_s - (now - _hl_last_request_ts)
                if wait_s > 0:
                    time.sleep(wait_s)
                _hl_last_request_ts = time.time()

            r = _hl_session.post(HL_INFO_URL, json=payload,
                                  headers={"Content-Type": "application/json"}, timeout=15)
            if r.status_code == 429:
                with _hl_request_lock:
                    _hl_min_interval_s = min(HL_MIN_INTERVAL_MAX_S, _hl_min_interval_s * 1.25 + 0.02)
                    _hl_consecutive_successes = 0
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
                _hl_consecutive_successes += 1
                if _hl_consecutive_successes >= 10:
                    _hl_min_interval_s = HL_MIN_INTERVAL_S
                    _hl_consecutive_successes = 0
                else:
                    _hl_min_interval_s = max(HL_MIN_INTERVAL_S, _hl_min_interval_s - 0.0025)
            return r.json()
        except Exception:
            if attempt == max_attempts - 1:
                raise
            time.sleep(min(20.0, base_sleep_s * (2 ** attempt)) + random.uniform(0.0, 0.25))
    raise RuntimeError("hl_post exhausted all retries (likely persistent 429)")

def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    iv_ms = INTERVAL_MS.get(interval, 3_600_000)
    return (reference_ms // iv_ms) * iv_ms

def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]

def get_candles(symbol: str, interval: str, n: int,
                 start_time_ms: int | None = None,
                 reference_ms: int | None = None) -> list[dict]:
    coin = hl_coin(symbol)
    iv_ms = INTERVAL_MS.get(interval, 3_600_000)
    ref_ms = int(time.time() * 1000) if reference_ms is None else reference_ms
    end_ms = current_bar_open_ms(ref_ms, interval)
    computed_start_ms = start_time_ms if start_time_ms is not None else end_ms - iv_ms * (n + 10)

    payload = {"type": "candleSnapshot", "req": {
        "coin": coin, "interval": interval,
        "startTime": computed_start_ms, "endTime": end_ms,
    }}
    raw = hl_post(payload)
    if raw is None:
        return []
    candles = []
    for c in raw:
        base_v = float(c["v"])
        quote_v = float(c["q"]) if c.get("q") is not None else base_v
        candles.append({"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                         "l": float(c["l"]), "c": float(c["c"]), "v": base_v, "qv": quote_v})
    candles = filter_closed_candles(candles, interval, ref_ms)
    return candles[-n:]

def fetch_all_candles(symbol: str, reference_ms: int | None = None):
    candles_m15 = get_candles(symbol, "15m", N_M15, reference_ms=reference_ms)
    if len(candles_m15) < 60:
        return None
    results = {}
    with ThreadPoolExecutor(max_workers=max(1, HL_TF_WORKERS)) as ex:
        futures = {
            ex.submit(get_candles, symbol, "4h", N_H4, None, reference_ms): "4h",
            ex.submit(get_candles, symbol, "1d", N_D, None, reference_ms): "1d",
        }
        for fut in as_completed(futures):
            tf = futures[fut]
            try:
                results[tf] = fut.result()
            except Exception as e:
                print(f"  [CANDLES] {symbol} {tf} fetch failed: {e} — skipping symbol")
                return None
    if "4h" not in results or "1d" not in results:
        return None
    if len(results["4h"]) < 60 or len(results["1d"]) < 30:
        return None
    return candles_m15, results["4h"], results["1d"]

_meta_cache: dict | None = None
_meta_cache_lock = threading.Lock()
_meta_cache_fetched_at = 0.0
META_CACHE_TTL_S = 55.0

def get_meta_and_asset_ctxs() -> dict | None:
    global _meta_cache, _meta_cache_fetched_at
    with _meta_cache_lock:
        if _meta_cache is not None and (time.time() - _meta_cache_fetched_at) < META_CACHE_TTL_S:
            return _meta_cache
    try:
        data = hl_post({"type": "metaAndAssetCtxs"})
        if data is None:
            return _meta_cache
        universe, asset_ctxs = data[0].get("universe", []), data[1]
        cache = {}
        for i, asset in enumerate(universe):
            name = asset.get("name", "")
            if not name:
                continue
            ctx = asset_ctxs[i]
            cache[name] = {
                "funding": float(ctx["funding"]) if ctx.get("funding") is not None else None,
                "open_interest_coins": float(ctx["openInterest"]) if ctx.get("openInterest") is not None else None,
                "mark_px": float(ctx["markPx"]) if ctx.get("markPx") is not None else None,
            }
        with _meta_cache_lock:
            _meta_cache = cache
            _meta_cache_fetched_at = time.time()
        return _meta_cache
    except Exception as e:
        print(f"  [META CACHE] fetch failed: {e}")
        with _meta_cache_lock:
            return _meta_cache

def get_open_interest_usd(symbol: str) -> float | None:
    cache = get_meta_and_asset_ctxs()
    if not cache:
        return None
    row = cache.get(hl_coin(symbol))
    if not row or row.get("open_interest_coins") is None or row.get("mark_px") is None:
        return None
    return row["open_interest_coins"] * row["mark_px"]


# ══════════════════════════════════════════════════════════════════
# MATH HELPERS
# ══════════════════════════════════════════════════════════════════

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        return a / b if b else default
    except Exception:
        return default

def atr_series(candles: list[dict], period: int = 14) -> list[float]:
    if len(candles) < 2:
        return [0.0] * len(candles)
    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    out, run = [], None
    for i, tr in enumerate(trs):
        if i < period:
            run = sum(trs[:i + 1]) / (i + 1)
        else:
            run = (run * (period - 1) + tr) / period
        out.append(run)
    return out


# ══════════════════════════════════════════════════════════════════
# STEP 1 — HTF BIAS (H4)
# ══════════════════════════════════════════════════════════════════

class Pivot:
    __slots__ = ("index", "price", "kind")
    def __init__(self, index, price, kind):
        self.index, self.price, self.kind = index, price, kind

def detect_pivots(candles: list[dict], left: int, right: int) -> list[Pivot]:
    pivots = []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        h, l = candles[i]["h"], candles[i]["l"]
        if h == max(window_h) and window_h.count(h) == 1:
            pivots.append(Pivot(i, h, "high"))
        if l == min(window_l) and window_l.count(l) == 1:
            pivots.append(Pivot(i, l, "low"))
    return pivots

class HTFBias:
    def __init__(self, bias, range_low, range_high, eq, atr_h4):
        self.bias = bias
        self.range_low = range_low
        self.range_high = range_high
        self.eq = eq
        self.atr_h4 = atr_h4

    @property
    def zone(self) -> str:
        return None

def compute_htf_bias(candles_h4: list[dict]) -> HTFBias | None:
    if len(candles_h4) < (PIVOT_LEFT_H4 + PIVOT_RIGHT_H4 + 10):
        return None
    pivots = detect_pivots(candles_h4, PIVOT_LEFT_H4, PIVOT_RIGHT_H4)
    highs = sorted([p for p in pivots if p.kind == "high"], key=lambda p: p.index)
    lows  = sorted([p for p in pivots if p.kind == "low"],  key=lambda p: p.index)
    if len(highs) < 2 or len(lows) < 2:
        return None

    last_high, prev_high = highs[-1], highs[-2]
    last_low,  prev_low  = lows[-1],  lows[-2]

    if last_high.price > prev_high.price and last_low.price > prev_low.price:
        bias = "bullish"
    elif last_high.price < prev_high.price and last_low.price < prev_low.price:
        bias = "bearish"
    else:
        bias = "neutral"

    close = candles_h4[-1]["c"]
    # CHOCH: a decisive close through the most recent opposing swing flips bias.
    if bias in ("bullish", "neutral") and close < last_low.price:
        bias = "bearish"
    elif bias in ("bearish", "neutral") and close > last_high.price:
        bias = "bullish"

    range_high = max(last_high.price, prev_high.price)
    range_low  = min(last_low.price, prev_low.price)
    if range_high <= range_low:
        return None
    eq = (range_high + range_low) / 2.0
    atr_h4 = atr_series(candles_h4, 14)[-1]
    return HTFBias(bias, range_low, range_high, eq, atr_h4)

def price_zone(price: float, htf: HTFBias) -> str:
    return "premium" if price >= htf.eq else "discount"


# ══════════════════════════════════════════════════════════════════
# STEP 2 — INSTITUTIONAL H4 POIs
# ══════════════════════════════════════════════════════════════════

class POIZone:
    def __init__(self, low, high, kind, index, quality=1):
        self.low, self.high = min(low, high), max(low, high)
        self.kind = kind          # "demand" | "supply"
        self.index = index        # index of formation on candles_h4
        self.quality = quality
        self.mid = (self.low + self.high) / 2.0

    def contains(self, price: float, buf: float = 0.0) -> bool:
        return (self.low - buf) <= price <= (self.high + buf)

def _order_blocks(candles: list[dict], atr_vals: list[float]) -> list[POIZone]:
    zones = []
    n = len(candles)
    for i in range(OB_BOS_LOOKBACK + 1, n):
        c = candles[i]
        body = c["c"] - c["o"]
        a = atr_vals[i] or 1e-9
        displacement = abs(body) >= OB_DISPLACEMENT_ATR_MULT * a
        if not displacement:
            continue
        if body > 0:
            prior_high = max(candles[j]["h"] for j in range(max(0, i - OB_BOS_LOOKBACK), i))
            if c["c"] <= prior_high:
                continue
            for j in range(i - 1, max(-1, i - OB_BOS_LOOKBACK - 1), -1):
                ob = candles[j]
                if ob["c"] < ob["o"]:
                    zones.append(POIZone(ob["l"], ob["h"], "demand", j))
                    break
        else:
            prior_low = min(candles[j]["l"] for j in range(max(0, i - OB_BOS_LOOKBACK), i))
            if c["c"] >= prior_low:
                continue
            for j in range(i - 1, max(-1, i - OB_BOS_LOOKBACK - 1), -1):
                ob = candles[j]
                if ob["c"] > ob["o"]:
                    zones.append(POIZone(ob["l"], ob["h"], "supply", j))
                    break
    return zones

def _fair_value_gaps(candles: list[dict], atr_vals: list[float]) -> list[POIZone]:
    zones = []
    for i in range(1, len(candles) - 1):
        left, right = candles[i - 1], candles[i + 1]
        a = atr_vals[i] or 1e-9
        if left["h"] < right["l"] and (right["l"] - left["h"]) >= FVG_MIN_GAP_ATR_MULT * a:
            zones.append(POIZone(left["h"], right["l"], "demand", i))
        elif left["l"] > right["h"] and (left["l"] - right["h"]) >= FVG_MIN_GAP_ATR_MULT * a:
            zones.append(POIZone(right["h"], left["l"], "supply", i))
    return zones

def _equal_liquidity_levels(pivots: list[Pivot], tolerance_pct: float) -> list[tuple]:
    """Clusters equal highs / equal lows into external liquidity pool levels."""
    levels = []
    for kind in ("high", "low"):
        pts = sorted([p for p in pivots if p.kind == kind], key=lambda p: p.price)
        used = [False] * len(pts)
        for i, p in enumerate(pts):
            if used[i]:
                continue
            cluster = [p]
            for j in range(i + 1, len(pts)):
                if used[j]:
                    continue
                if abs(pts[j].price - p.price) / max(p.price, 1e-9) <= tolerance_pct:
                    cluster.append(pts[j])
                    used[j] = True
            if len(cluster) >= 2:
                avg = sum(c.price for c in cluster) / len(cluster)
                levels.append(("EQH" if kind == "high" else "EQL", avg))
    return levels

def _period_hl(candles_d: list[dict], days: int) -> tuple:
    if len(candles_d) < days + 1:
        return None, None
    window = candles_d[-(days + 1):-1]  # exclude the still-forming/last day
    if not window:
        return None, None
    return max(c["h"] for c in window), min(c["l"] for c in window)

def build_liquidity_levels(candles_h4: list[dict], candles_d: list[dict], pivots_h4) -> list[tuple]:
    levels = list(_equal_liquidity_levels(pivots_h4, H4_LIQUIDITY_TOLERANCE))
    pdh, pdl = _period_hl(candles_d, 1)
    pwh, pwl = _period_hl(candles_d, 7)
    pmh, pml = _period_hl(candles_d, 30)
    for label, val in (("PDH", pdh), ("PDL", pdl), ("PWH", pwh), ("PWL", pwl),
                        ("PMH", pmh), ("PML", pml)):
        if val is not None:
            levels.append((label, val))
    return levels

def is_zone_untested(zone: POIZone, candles: list[dict]) -> bool:
    """A POI is fresh only if no candle after its formation has already
    traded back through it and closed beyond, i.e. it hasn't been mitigated."""
    for c in candles[zone.index + 1: -1]:
        if c["l"] <= zone.mid <= c["h"]:
            return False
    return True

def build_poi_zones(candles_h4: list[dict], candles_d: list[dict], htf: HTFBias) -> list[POIZone]:
    window = candles_h4[-H4_POI_LOOKBACK:]
    offset = len(candles_h4) - len(window)
    atr_vals = atr_series(candles_h4, 14)
    pivots_h4 = detect_pivots(window, PIVOT_LEFT_H4, PIVOT_RIGHT_H4)
    for p in pivots_h4:
        p.index += offset

    raw = _order_blocks(window, atr_vals[offset:]) + _fair_value_gaps(window, atr_vals[offset:])
    for z in raw:
        z.index += offset

    liquidity_levels = build_liquidity_levels(candles_h4, candles_d, pivots_h4)
    a = htf.atr_h4 or 1e-9
    buf = POI_CONFLUENCE_BUFFER_ATR_MULT * a

    kept = []
    for z in raw:
        if not is_zone_untested(z, candles_h4):
            continue
        if z.kind == "demand" and price_zone(z.mid, htf) != "discount":
            continue
        if z.kind == "supply" and price_zone(z.mid, htf) != "premium":
            continue
        confluence = sum(1 for _, lvl in liquidity_levels if (z.low - buf) <= lvl <= (z.high + buf))
        z.quality = 1 + confluence
        kept.append(z)

    kept.sort(key=lambda z: (z.quality, z.index), reverse=True)
    return kept[:5]


# ══════════════════════════════════════════════════════════════════
# STEP 3 — H4 SWING FAILURE PATTERN (LIQUIDITY SWEEP)
# ══════════════════════════════════════════════════════════════════

class SFPEvent:
    def __init__(self, direction, zone: POIZone, sweep_extreme, candle_index, candle_time):
        self.direction = direction         # "long" | "short"
        self.zone = zone
        self.sweep_extreme = sweep_extreme
        self.candle_index = candle_index
        self.candle_time = candle_time

def detect_sfp(candles_h4: list[dict], zones: list[POIZone], htf: HTFBias) -> SFPEvent | None:
    if htf.bias == "neutral" or not zones:
        return None
    atr_vals = atr_series(candles_h4, 14)
    n = len(candles_h4)
    start = max(0, n - SFP_LOOKBACK_H4_BARS)

    best = None
    for i in range(start, n):
        c = candles_h4[i]
        a = atr_vals[i] or 1e-9
        rng = c["h"] - c["l"]
        if rng <= 0:
            continue
        for z in zones:
            if htf.bias == "bullish" and z.kind == "demand":
                swept = c["l"] < z.low and (z.low - c["l"]) <= SFP_MAX_SWEEP_DEPTH_ATR_MULT * a
                rejected = c["c"] > z.low and c["c"] > c["o"]
                wick_ratio = safe_div(min(c["o"], c["c"]) - c["l"], rng)
                if swept and rejected and wick_ratio >= SFP_MIN_WICK_RATIO:
                    cand = SFPEvent("long", z, c["l"], i, c["t"])
                    if best is None or cand.candle_index > best.candle_index:
                        best = cand
            elif htf.bias == "bearish" and z.kind == "supply":
                swept = c["h"] > z.high and (c["h"] - z.high) <= SFP_MAX_SWEEP_DEPTH_ATR_MULT * a
                rejected = c["c"] < z.high and c["c"] < c["o"]
                wick_ratio = safe_div(c["h"] - max(c["o"], c["c"]), rng)
                if swept and rejected and wick_ratio >= SFP_MIN_WICK_RATIO:
                    cand = SFPEvent("short", z, c["h"], i, c["t"])
                    if best is None or cand.candle_index > best.candle_index:
                        best = cand
    return best


# ══════════════════════════════════════════════════════════════════
# STEP 4 — M15 MARKET STRUCTURE SHIFT (CONFIRMATION)
# ══════════════════════════════════════════════════════════════════

class MSSEvent:
    def __init__(self, direction, impulse_index, swing_price, confirm_time):
        self.direction = direction
        self.impulse_index = impulse_index
        self.swing_price = swing_price
        self.confirm_time = confirm_time

def detect_mss(candles_m15: list[dict], direction: str, sweep_time_ms: int) -> MSSEvent | None:
    post = [(i, c) for i, c in enumerate(candles_m15) if c["t"] > sweep_time_ms]
    if len(post) < (PIVOT_LEFT_M15 + PIVOT_RIGHT_M15 + 3):
        return None
    post_candles = [c for _, c in post]
    offset = post[0][0]
    atr_vals = atr_series(candles_m15, 14)
    pivots = detect_pivots(post_candles, PIVOT_LEFT_M15, PIVOT_RIGHT_M15)

    if direction == "long":
        swing_highs = sorted([p for p in pivots if p.kind == "high"], key=lambda p: p.index)
        if not swing_highs:
            return None
        for idx in range(swing_highs[0].index + 1, len(post_candles)):
            relevant_highs = [p for p in swing_highs if p.index < idx]
            if not relevant_highs:
                continue
            swing_price = relevant_highs[-1].price
            c = post_candles[idx]
            a = atr_vals[offset + idx] or 1e-9
            displacement = (c["c"] - c["o"]) >= MSS_DISPLACEMENT_ATR_MULT * a
            margin = c["c"] - swing_price >= MSS_MIN_CLOSE_MARGIN_ATR_MULT * a
            if c["c"] > swing_price and displacement and margin:
                return MSSEvent("long", offset + idx, swing_price, c["t"])
        return None
    else:
        swing_lows = sorted([p for p in pivots if p.kind == "low"], key=lambda p: p.index)
        if not swing_lows:
            return None
        for idx in range(swing_lows[0].index + 1, len(post_candles)):
            relevant_lows = [p for p in swing_lows if p.index < idx]
            if not relevant_lows:
                continue
            swing_price = relevant_lows[-1].price
            c = post_candles[idx]
            a = atr_vals[offset + idx] or 1e-9
            displacement = (c["o"] - c["c"]) >= MSS_DISPLACEMENT_ATR_MULT * a
            margin = swing_price - c["c"] >= MSS_MIN_CLOSE_MARGIN_ATR_MULT * a
            if c["c"] < swing_price and displacement and margin:
                return MSSEvent("short", offset + idx, swing_price, c["t"])
        return None


# ══════════════════════════════════════════════════════════════════
# STEP 5 — BREAKER ENTRY, STRUCTURE STOP, DYNAMIC TARGETS
# ══════════════════════════════════════════════════════════════════

class TradePlan:
    def __init__(self, entry, sl, tp1, tp2, r_multiple_tp2, breaker_low, breaker_high):
        self.entry, self.sl, self.tp1, self.tp2 = entry, sl, tp1, tp2
        self.r_multiple_tp2 = r_multiple_tp2
        self.breaker_low, self.breaker_high = breaker_low, breaker_high

def find_breaker_block(candles_m15: list[dict], mss: MSSEvent) -> POIZone | None:
    lo = max(0, mss.impulse_index - BREAKER_SEARCH_BARS)
    if mss.direction == "long":
        for j in range(mss.impulse_index - 1, lo - 1, -1):
            c = candles_m15[j]
            if c["c"] < c["o"]:
                return POIZone(c["l"], c["h"], "demand", j)
    else:
        for j in range(mss.impulse_index - 1, lo - 1, -1):
            c = candles_m15[j]
            if c["c"] > c["o"]:
                return POIZone(c["l"], c["h"], "supply", j)
    return None

def room_to_next_opposing_level(entry: float, direction: str, sfp: SFPEvent,
                                 zones: list[POIZone], liquidity_levels: list[tuple]) -> float | None:
    candidates = []
    for z in zones:
        if direction == "long" and z.kind == "supply" and z.low > entry:
            candidates.append(z.low)
        if direction == "short" and z.kind == "demand" and z.high < entry:
            candidates.append(z.high)
    for _, lvl in liquidity_levels:
        if direction == "long" and lvl > entry:
            candidates.append(lvl)
        if direction == "short" and lvl < entry:
            candidates.append(lvl)
    if not candidates:
        return None
    return min(candidates) - entry if direction == "long" else entry - max(candidates)

def build_trade_plan(direction: str, sfp: SFPEvent, breaker: POIZone,
                      candles_h4: list[dict], atr_h4: float,
                      zones: list[POIZone], liquidity_levels: list[tuple]) -> TradePlan | None:
    buf = SL_BUFFER_ATR_MULT * (atr_h4 or 1e-9)
    if direction == "long":
        entry = breaker.high
        sl = sfp.sweep_extreme - buf
    else:
        entry = breaker.low
        sl = sfp.sweep_extreme + buf

    risk = abs(entry - sl)
    if risk <= 0:
        return None

    tp1 = entry + MIN_RR * risk if direction == "long" else entry - MIN_RR * risk

    room = room_to_next_opposing_level(entry, direction, sfp, zones, liquidity_levels)
    best_rr = MIN_RR
    if room is not None:
        usable_room = room - LIQUIDITY_ROOM_BUFFER_ATR_MULT * (atr_h4 or 1e-9)
        for rr in EXT_RR_LEVELS:
            if usable_room >= rr * risk:
                best_rr = rr
    tp2 = entry + best_rr * risk if direction == "long" else entry - best_rr * risk

    if direction == "long" and not (sl < entry < tp1 <= tp2):
        return None
    if direction == "short" and not (tp2 <= tp1 < entry < sl):
        return None

    return TradePlan(entry, sl, tp1, tp2, best_rr, breaker.low, breaker.high)


# ══════════════════════════════════════════════════════════════════
# STATE PERSISTENCE
# ══════════════════════════════════════════════════════════════════

def load_state() -> dict:
    fresh = {"_version": STATE_VERSION, "pending_setups": {}, "active_signals": [],
             "resolved_signals": [], "signal_cooldowns": {}, "signal_history": []}
    for path in (STATE_FILE, STATE_FILE + ".bak"):
        if Path(path).exists():
            try:
                s = json.loads(Path(path).read_text())
                if s.get("_version", 1) != STATE_VERSION:
                    print(f"[STATE] Schema version mismatch in {path}. Starting fresh.")
                    continue
                for k, v in fresh.items():
                    s.setdefault(k, v if not isinstance(v, (dict, list)) else type(v)())
                if path != STATE_FILE:
                    print(f"[STATE] Loaded from backup {path}")
                return s
            except Exception as e:
                print(f"[STATE] Failed to load {path}: {e}")
    print("[STATE] Starting fresh — no valid state file found")
    return fresh

def save_state(state: dict):
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


# ══════════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════════

def _sanitize_error(e: Exception) -> str:
    import re
    msg = str(e)
    if "bot" in msg and "/" in msg:
        msg = re.sub(r'https?://[^\s]+', '[URL]', msg)
    return f"{e.__class__.__name__}: {msg[:200]}"

def send_telegram(text: str) -> int | None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
            r.raise_for_status()
            return r.json()["result"]["message_id"]
        except Exception as e:
            if attempt == 2:
                print(f"[TG ERROR] {_sanitize_error(e)}")
            time.sleep(2)
    return None

def react_to_message(message_id: int, emoji: str):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    try:
        r = requests.post(url, json={"chat_id": TG_CHAT_ID, "message_id": message_id,
                                      "reaction": [{"type": "emoji", "emoji": emoji}], "is_big": False}, timeout=10)
        r.raise_for_status()
        print(f"  [REACT] {emoji} -> msg_id {message_id}")
    except Exception as e:
        print(f"  [REACT ERROR] msg_id {message_id}: {_sanitize_error(e)}")

def fmt_px(v: float) -> str:
    if v >= 1000: return f"{v:,.2f}"
    if v >= 1:    return f"{v:,.4f}"
    return f"{v:.6f}"

def format_signal(symbol: str, direction: str, plan: TradePlan, htf: HTFBias, zone_kind: str) -> str:
    coin = hl_coin(symbol)
    arrow = "▲ LONG" if direction == "long" else "▼ SHORT"
    emoji = "🟢" if direction == "long" else "🔴"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    risk = abs(plan.entry - plan.sl)
    return (
        f"{emoji} <b>{ENGINE_NAME} v{__version__}</b>\n"
        f"<b>{coin} — {arrow}</b>\n\n"
        f"HTF Bias (H4): {htf.bias.upper()} | Zone swept: {zone_kind.upper()}\n"
        f"Setup: H4 SFP -> M15 MSS -> Breaker Entry\n\n"
        f"Entry (breaker zone): {fmt_px(plan.entry)}\n"
        f"Stop Loss (structure): {fmt_px(plan.sl)}\n"
        f"TP1 (2R): {fmt_px(plan.tp1)}\n"
        f"TP2 ({plan.r_multiple_tp2:.0f}R): {fmt_px(plan.tp2)}\n"
        f"Risk distance: {fmt_px(risk)}\n\n"
        f"{ts}"
    )


# ══════════════════════════════════════════════════════════════════
# SIGNAL LIFECYCLE: PENDING SETUPS -> ACTIVE SIGNALS -> RESOLUTION
# ══════════════════════════════════════════════════════════════════

def check_cooldown(state: dict, symbol: str, direction: str, bar_index_h4: int) -> bool:
    active = state.get("active_signals", [])
    active_count = sum(1 for s in active if not s.get("resolved", False))
    if active_count >= MAX_CONCURRENT_ACTIVE_SIGNALS:
        return False
    for s in active:
        if s.get("symbol") == symbol and not s.get("resolved", False):
            return False
    key = f"{symbol}_{direction}"
    last_bar = state.get("signal_cooldowns", {}).get(key)
    if last_bar is not None and (bar_index_h4 - last_bar) < SIGNAL_COOLDOWN_H4_BARS:
        return False
    return True

def update_cooldown(state: dict, symbol: str, direction: str, bar_index_h4: int):
    state.setdefault("signal_cooldowns", {})[f"{symbol}_{direction}"] = bar_index_h4

def process_symbol(symbol: str, state: dict, bundle: tuple, reference_ms: int,
                    bar_index_h4: int, bar_index_m15: int) -> dict | None:
    candles_m15, candles_h4, candles_d = bundle
    coin = hl_coin(symbol)

    pending = state.setdefault("pending_setups", {})
    setup = pending.get(symbol)

    # -- Advance an existing pending H4 SFP toward M15 confirmation --
    if setup is not None:
        age_hours = (reference_ms - setup["sfp_time"]) / 3_600_000.0
        if age_hours > PENDING_SETUP_MAX_AGE_HOURS:
            print(f"    {coin}: pending setup expired ({age_hours:.1f}h) — dropping")
            del pending[symbol]
            setup = None

    if setup is not None:
        mss = detect_mss(candles_m15, setup["direction"], setup["sfp_time"])
        if mss is None:
            print(f"    {coin}: SFP pending, no M15 MSS yet")
            return None

        zone = POIZone(setup["zone_low"], setup["zone_high"],
                        "demand" if setup["direction"] == "long" else "supply", 0)
        sfp = SFPEvent(setup["direction"], zone, setup["sweep_extreme"], 0, setup["sfp_time"])
        breaker = find_breaker_block(candles_m15, mss)
        if breaker is None:
            print(f"    {coin}: MSS confirmed but no breaker candle found — dropping setup")
            del pending[symbol]
            return None

        htf = HTFBias(setup["direction"] == "long" and "bullish" or "bearish",
                      setup["range_low"], setup["range_high"], setup["eq"], setup["atr_h4"])
        zones = build_poi_zones(candles_h4, candles_d, htf)
        liquidity_levels = build_liquidity_levels(
            candles_h4, candles_d, detect_pivots(candles_h4[-H4_POI_LOOKBACK:], PIVOT_LEFT_H4, PIVOT_RIGHT_H4))

        plan = build_trade_plan(setup["direction"], sfp, breaker, candles_h4, setup["atr_h4"],
                                 zones, liquidity_levels)
        del pending[symbol]
        if plan is None:
            print(f"    {coin}: MSS confirmed but trade plan failed validation")
            return None

        if not check_cooldown(state, symbol, setup["direction"], bar_index_h4):
            print(f"    {coin}: setup confirmed but suppressed by cooldown / concurrency limit")
            return None

        print(f"    CASTELLAN SIGNAL: {coin} {setup['direction'].upper()} "
              f"entry={fmt_px(plan.entry)} sl={fmt_px(plan.sl)} "
              f"tp1={fmt_px(plan.tp1)} tp2={fmt_px(plan.tp2)} ({plan.r_multiple_tp2:.0f}R)")
        return {"symbol": symbol, "direction": setup["direction"], "plan": plan,
                "zone_kind": zone.kind, "bar_index_m15": bar_index_m15}

    # -- No pending setup: look for a fresh H4 bias -> POI -> SFP sequence --
    htf = compute_htf_bias(candles_h4)
    if htf is None or htf.bias == "neutral":
        print(f"    {coin}: no H4 bias")
        return None

    zones = build_poi_zones(candles_h4, candles_d, htf)
    if not zones:
        print(f"    {coin}: no qualified H4 POI in {price_zone(candles_h4[-1]['c'], htf)}")
        return None

    sfp = detect_sfp(candles_h4, zones, htf)
    if sfp is None:
        print(f"    {coin}: {htf.bias} bias, no valid H4 SFP yet")
        return None

    already_active = any(s.get("symbol") == symbol and not s.get("resolved", False)
                          for s in state.get("active_signals", []))
    if already_active:
        return None

    pending[symbol] = {
        "direction": sfp.direction, "zone_low": sfp.zone.low, "zone_high": sfp.zone.high,
        "sweep_extreme": sfp.sweep_extreme, "sfp_time": sfp.candle_time,
        "range_low": htf.range_low, "range_high": htf.range_high, "eq": htf.eq,
        "atr_h4": htf.atr_h4,
    }
    print(f"    {coin}: H4 SFP detected ({sfp.direction.upper()}) — awaiting M15 MSS confirmation")
    return None

def track_signal(state: dict, symbol: str, direction: str, msg_id: int,
                  plan: TradePlan, bar_index_m15: int):
    state.setdefault("active_signals", []).append({
        "symbol": symbol, "direction": direction, "msg_id": msg_id,
        "bar_index": bar_index_m15, "signal_bar_time": bar_index_m15 * 900_000,
        "entry": plan.entry, "tp1": plan.tp1, "tp2": plan.tp2, "sl": plan.sl,
        "tp1_hit": False, "resolved": False,
    })

def check_active_signals(state: dict, bar_index_m15_now: int, reference_ms: int):
    signals = list(state.get("active_signals", []))
    if not signals:
        return
    still_active = []
    for sig in signals:
        if sig.get("resolved", False):
            continue
        age = bar_index_m15_now - sig.get("bar_index", bar_index_m15_now)
        if age > SIGNAL_MAX_AGE_BARS_M15:
            print(f"  [TRACK] {sig['symbol']} expired after {age} M15 bars — dropping")
            state.setdefault("resolved_signals", []).append(
                {"symbol": sig["symbol"], "direction": sig.get("direction", ""),
                 "outcome": "expired", "resolved_at": int(time.time())})
            continue

        symbol, direction, msg_id = sig["symbol"], sig["direction"], sig["msg_id"]
        tp1, tp2, sl = sig["tp1"], sig["tp2"], sig["sl"]
        tp1_hit = sig.get("tp1_hit", False)
        signal_bar_time_ms = sig.get("signal_bar_time")
        last_ts = sig.get("last_processed_candle_ts", signal_bar_time_ms or 0)

        try:
            candles = get_candles(symbol, "15m", N_M15, start_time_ms=signal_bar_time_ms, reference_ms=reference_ms)
        except Exception as e:
            print(f"  [TRACK] candle fetch failed for {symbol}: {e}")
            still_active.append(sig)
            continue
        new_candles = [c for c in candles if c["t"] > last_ts]
        if not new_candles:
            still_active.append(sig)
            continue

        def resolve(outcome):
            state.setdefault("resolved_signals", []).append(
                {"symbol": symbol, "direction": direction, "outcome": outcome, "resolved_at": int(time.time())})
            sig["resolved"] = True

        for c in new_candles:
            last_ts = c["t"]
            if direction == "long":
                if not tp1_hit:
                    if c["h"] >= tp1 and c["l"] <= sl:
                        if abs(sl - c["o"]) < abs(tp1 - c["o"]):
                            react_to_message(msg_id, REACT_SL); resolve("sl"); break
                        react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif c["h"] >= tp1:
                        react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif c["l"] <= sl:
                        react_to_message(msg_id, REACT_SL); resolve("sl"); break
                if tp1_hit and c["h"] >= tp2:
                    react_to_message(msg_id, REACT_TP2); resolve("tp2"); break
                if tp1_hit and not sig.get("resolved", False) and c["l"] <= sl:
                    resolve("tp1"); break
            else:
                if not tp1_hit:
                    if c["l"] <= tp1 and c["h"] >= sl:
                        if abs(sl - c["o"]) < abs(tp1 - c["o"]):
                            react_to_message(msg_id, REACT_SL); resolve("sl"); break
                        react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif c["l"] <= tp1:
                        react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif c["h"] >= sl:
                        react_to_message(msg_id, REACT_SL); resolve("sl"); break
                if tp1_hit and c["l"] <= tp2:
                    react_to_message(msg_id, REACT_TP2); resolve("tp2"); break
                if tp1_hit and not sig.get("resolved", False) and c["h"] >= sl:
                    resolve("tp1"); break

        if not sig.get("resolved", False):
            sig["last_processed_candle_ts"] = last_ts
            still_active.append(sig)

    state["active_signals"] = still_active


# ══════════════════════════════════════════════════════════════════
# MAIN SCAN LOOP
# ══════════════════════════════════════════════════════════════════

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] {ENGINE_NAME} v{__version__} starting…")
    print(f"Watchlist ({len(WATCHLIST)} pairs): {[hl_coin(s) for s in WATCHLIST]}")

    reference_ms = int(time.time() * 1000)
    bar_index_h4 = reference_ms // INTERVAL_MS["4h"]
    bar_index_m15 = reference_ms // INTERVAL_MS["15m"]
    state = load_state()

    print("[TRACK] Checking active signals…")
    check_active_signals(state, bar_index_m15, reference_ms)
    save_state(state)

    print("[INIT] Fetching market context…")
    get_meta_and_asset_ctxs()

    if _shutdown:
        save_state(state); sys.exit(0)

    print("[PHASE 1] Fetching candles…")
    bundles: dict[str, tuple] = {}
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {ex.submit(fetch_all_candles, sym, reference_ms): sym for sym in WATCHLIST}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                data = fut.result()
                if data is not None:
                    bundles[sym] = data
            except Exception as e:
                print(f"    ERROR fetching {sym}: {e}")

    if _shutdown:
        save_state(state); sys.exit(0)

    print("[PHASE 2] Running SMC sequence per symbol…")
    results = []
    for sym in WATCHLIST:
        bundle = bundles.get(sym)
        if bundle is None:
            print(f"    Skipping {hl_coin(sym)}: insufficient candles")
            continue
        try:
            oi_usd = get_open_interest_usd(sym)
            if oi_usd is not None and oi_usd < 500_000:
                print(f"    Skipping {hl_coin(sym)}: OI too low (${oi_usd:,.0f})")
                continue
            res = process_symbol(sym, state, bundle, reference_ms, bar_index_h4, bar_index_m15)
            if res:
                results.append(res)
        except Exception as e:
            print(f"    ERROR processing {sym}: {e}")

    results = results[:MAX_SIGNALS_PER_SCAN]

    signals_fired = 0
    for res in results:
        symbol, direction, plan = res["symbol"], res["direction"], res["plan"]
        msg = format_signal(symbol, direction, plan,
                             HTFBias(direction == "long" and "bullish" or "bearish", 0, 0, 0, 0),
                             res["zone_kind"])
        msg_id = send_telegram(msg)
        if msg_id:
            update_cooldown(state, symbol, direction, bar_index_h4)
            track_signal(state, symbol, direction, msg_id, plan, res["bar_index_m15"])
            print(f"  [FIRED] {hl_coin(symbol)} {direction.upper()} "
                  f"TP1={fmt_px(plan.tp1)} TP2={fmt_px(plan.tp2)} SL={fmt_px(plan.sl)}")
            signals_fired += 1
        else:
            print(f"  [TG FAIL] {hl_coin(symbol)} {direction.upper()} — Telegram send failed")
        time.sleep(0.5)

    save_state(state)
    print(f"Scan complete. {signals_fired} signal(s) fired.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            send_telegram(f"🚨 {ENGINE_NAME} crashed: {e}")
        except Exception:
            pass
        raise
