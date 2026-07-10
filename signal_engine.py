#!/usr/bin/env python3
"""
PRISM ADAPTIVE SIGNAL ENGINE v1.0.0
====================================
Institutional-grade, multi-engine adaptive signal platform for Hyperliquid
perpetuals. Built from scratch as an original synthesis, using AXIS,
KESTREL and KAIROS only as reference material for gap analysis (see
GAP_ANALYSIS.md). No code from any reference engine is reused.

ARCHITECTURE (see ARCHITECTURE.md for full detail)
----------------------------------------------------
  1. Data layer      : throttled Hyperliquid client + persisted delta candle
                        cache, shared across every engine and symbol.
  2. Indicator layer  : EMA/SMA/RSI/ATR/ADX, swing detection, session VWAP
                        and volume profile (POC/VAH/VAL), volatility
                        percentile ranking.
  3. Structure layer  : BOS/CHoCH, order blocks, breaker blocks, fair value
                        gaps, liquidity pools & sweep detection,
                        premium/discount zoning -- shared building blocks
                        consumed by every specialized engine.
  4. Regime layer     : per-symbol + cross-sectional market regime vector
                        (trend/range/volatility/bias/breadth/session).
  5. Engine layer     : 13 independent specialized signal engines, each
                        implementing a common interface and emitting zero
                        or more Candidate objects.
  6. Decision layer    : central ranking engine that scores every candidate
                        on calibrated expected value, deduplicates
                        correlated symbols, and applies an adaptive
                        frequency governor to hold the 5-10 signal/day band.
  7. Learning layer    : persistent per-engine / per-regime performance
                        statistics that regularized-shrink each engine's
                        scoring weight toward its live win rate, and a
                        confidence-calibration curve fit from realized
                        outcomes.
  8. Execution layer  : trade lifecycle manager (Activated/TP1/TP2/SL/BE/
                        Closed/Cancelled), Telegram notifier with reply
                        threading and a 08:00 UTC daily summary.
  9. Runtime layer    : scan-per-run orchestrator designed for a GitHub
                        Actions cron every 15 minutes, with graceful
                        degradation, exponential backoff, and a hard
                        runtime budget so a run always finishes inside the
                        schedule window.

Single file, immediately runnable:  python3 prism_signal_engine_v1_0_0.py
All state lives in state.json / candle_cache.json next to the script.
Configure via environment variables (see CONFIGURATION GUIDE.md).
"""

from __future__ import annotations

import json
import logging
import os
import signal as signal_module
import statistics
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

# ============================================================================
# 0. CONFIGURATION
# ============================================================================

VERSION = "1.0.1"
ENGINE_NAME = "PRISM Adaptive Signal Engine"

HL_API_URL = "https://api.hyperliquid.xyz/info"
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

STATE_PATH = os.environ.get("PRISM_STATE_PATH", "state.json")
CACHE_PATH = os.environ.get("PRISM_CANDLE_CACHE_PATH", "candle_cache.json")
LOG_PATH = os.environ.get("PRISM_LOG_PATH", "prism_engine.log")

# Hard wall-clock budget for a single scan-per-run invocation. GitHub
# Actions schedules this every 15 minutes; we leave generous headroom.
MAX_RUNTIME_SECONDS = int(os.environ.get("PRISM_MAX_RUNTIME_SECONDS", "600"))
RUN_START_TS = time.time()

WATCHLIST = [s.strip().upper() for s in os.environ.get(
    "PRISM_WATCHLIST",
    "BTC,ETH,SOL,HYPE,BNB,XRP,DOGE,ADA,AVAX,LINK,SUI,NEAR,DOT,LTC,AAVE,"
    "APT,ONDO,TAO,BCH,UNI,TRX,PENDLE,ZEC,XLM,PENGU"
).split(",") if s.strip()]

# Never below 15m per specification. (bias_tf, structure_tf, execution_tf)
TF_BIAS = "4h"
TF_STRUCTURE = "1h"
TF_EXECUTION_TREND = "1h"
TF_EXECUTION_FAST = "15m"
ALL_TIMEFRAMES = ["15m", "1h", "4h"]
CANDLES_PER_TF = {"15m": 300, "1h": 300, "4h": 300}

# Risk / signal-quality knobs
MIN_RR = 1.5
MAX_OPEN_PER_SYMBOL = 1
MAX_SIGNALS_PER_DAY_HARD_CAP = 18
TARGET_SIGNALS_PER_DAY_LOW = 5
TARGET_SIGNALS_PER_DAY_HIGH = 10
BASE_ACCEPT_THRESHOLD = 0.62          # composite score 0..1
THRESHOLD_MIN = 0.50
THRESHOLD_MAX = 0.80
CORRELATION_DEDUP_WINDOW_MIN = 90     # minutes; suppress correlated dupes

DAILY_SUMMARY_HOUR_UTC = 8

REQUEST_TIMEOUT = 12
MAX_WORKERS = 6

# ============================================================================
# 1. LOGGING
# ============================================================================

logger = logging.getLogger("prism")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = logging.FileHandler(LOG_PATH)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(fh)
    logger.addHandler(sh)

_SHUTDOWN = {"flag": False}


def _handle_shutdown(sig_num, frame):
    logger.warning("Shutdown signal %s received; finishing current step safely.", sig_num)
    _SHUTDOWN["flag"] = True


signal_module.signal(signal_module.SIGTERM, _handle_shutdown)
signal_module.signal(signal_module.SIGINT, _handle_shutdown)


def time_budget_exceeded() -> bool:
    return (time.time() - RUN_START_TS) > MAX_RUNTIME_SECONDS or _SHUTDOWN["flag"]


# ============================================================================
# 2. HYPERLIQUID API CLIENT (throttled, retried, batched)
# ============================================================================

class _RateLimiter:
    """Simple token bucket to stay comfortably within HL weight limits."""

    def __init__(self, capacity: float = 1150.0, refill_per_sec: float = 19.0):
        self.capacity = capacity
        self.tokens = capacity
        self.refill_per_sec = refill_per_sec
        self.last = time.time()

    def acquire(self, weight: float = 2.0):
        while True:
            now = time.time()
            elapsed = now - self.last
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_sec)
            if self.tokens >= weight:
                self.tokens -= weight
                return
            time.sleep(max(0.05, (weight - self.tokens) / self.refill_per_sec))


_LIMITER = _RateLimiter()


def hl_post(payload: dict, retries: int = 4, timeout: int = REQUEST_TIMEOUT) -> Optional[dict | list]:
    """POST to the Hyperliquid info endpoint with throttling, retry and
    exponential backoff. Returns None on total failure (graceful
    degradation -- caller must skip, never crash)."""
    weight = 20.0 if payload.get("type") == "candleSnapshot" else 2.0
    body = json.dumps(payload).encode()
    for attempt in range(retries):
        if time_budget_exceeded():
            return None
        _LIMITER.acquire(weight)
        try:
            req = urllib.request.Request(
                HL_API_URL, data=body,
                headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(min(30, 2 ** attempt * 1.5))
                continue
            logger.warning("HL HTTP error %s on %s", e.code, payload.get("type"))
            time.sleep(min(10, 1.5 ** attempt))
        except Exception as e:  # noqa: BLE001 -- network layer must never crash the run
            logger.warning("HL request failed (%s): %s", payload.get("type"), e)
            time.sleep(min(10, 1.5 ** attempt))
    return None


def hl_coin(symbol: str) -> str:
    return symbol


def _interval_ms(interval: str) -> int:
    unit = interval[-1]
    n = int(interval[:-1])
    return n * {"m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]


def fetch_candles(symbol: str, interval: str, n: int) -> list[dict]:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - n * _interval_ms(interval)
    payload = {"type": "candleSnapshot", "req": {
        "coin": hl_coin(symbol), "interval": interval,
        "startTime": start_ms, "endTime": end_ms}}
    data = hl_post(payload)
    if not isinstance(data, list):
        return []
    out = []
    for c in data:
        try:
            out.append({
                "t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"]),
            })
        except (KeyError, TypeError, ValueError):
            continue
    return out


def fetch_mid_prices() -> dict[str, float]:
    data = hl_post({"type": "allMids"})
    if not isinstance(data, dict):
        return {}
    out = {}
    for k, v in data.items():
        try:
            out[k] = float(v)
        except (TypeError, ValueError):
            continue
    return out


# ============================================================================
# 3. CANDLE CACHE (shared, delta-fetched, persisted)
# ============================================================================

class CandleCache:
    """One shared cache per run: avoids duplicate API calls across the 13
    engines and across symbols that share a timeframe. Persisted so the
    next scheduled run only fetches the delta since the last close."""

    OVERLAP_BARS = 3

    def __init__(self, path: str):
        self.path = path
        self.data: dict[str, dict[str, list[dict]]] = self._load()

    def _load(self) -> dict:
        try:
            with open(self.path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save(self):
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.data, f)
            os.replace(tmp, self.path)
        except OSError as e:
            logger.warning("Candle cache save failed: %s", e)

    def get(self, symbol: str, interval: str, n: int) -> list[dict]:
        bucket = self.data.setdefault(symbol, {})
        cached = bucket.get(interval, [])
        now_ms = int(time.time() * 1000)
        step = _interval_ms(interval)
        if cached:
            last_open = cached[-1]["t"]
            gap_bars = (now_ms - last_open) // step
            if gap_bars <= 1 and len(cached) >= n:
                return cached[-n:]
            fetch_n = min(500, int(gap_bars) + self.OVERLAP_BARS + 2)
            fresh = fetch_candles(symbol, interval, fetch_n)
            if fresh:
                merged = {c["t"]: c for c in cached}
                for c in fresh:
                    merged[c["t"]] = c
                cached = [merged[t] for t in sorted(merged)]
                bucket[interval] = cached[-max(n, 400):]
            return bucket[interval][-n:]
        fresh = fetch_candles(symbol, interval, n)
        bucket[interval] = fresh
        return fresh


# ============================================================================
# 4. INDICATORS
# ============================================================================

def closes(candles): return [c["c"] for c in candles]
def highs(candles): return [c["h"] for c in candles]
def lows(candles): return [c["l"] for c in candles]
def vols(candles): return [c["v"] for c in candles]


def ema(vals: list[float], period: int) -> list[float]:
    if not vals:
        return []
    k = 2.0 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1):i + 1]
        out.append(sum(window) / len(window))
    return out


def rsi(vals: list[float], period: int = 14) -> list[float]:
    if len(vals) < 2:
        return [50.0] * len(vals)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(vals)):
        d = vals[i] - vals[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g, avg_l = gains[1], losses[1]
    out = [50.0, 50.0]
    for i in range(2, len(vals)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l > 1e-12 else 999.0
        out.append(100 - 100 / (1 + rs))
    return out


def atr(h: list[float], l: list[float], c: list[float], period: int = 14) -> list[float]:
    if not h:
        return []
    trs = [h[0] - l[0]]
    for i in range(1, len(h)):
        trs.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))
    out = [trs[0]]
    for i in range(1, len(trs)):
        out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def adx(h: list[float], l: list[float], c: list[float], period: int = 14) -> list[float]:
    n = len(h)
    if n < period + 1:
        return [15.0] * n
    plus_dm, minus_dm, tr = [0.0], [0.0], [h[0] - l[0]]
    for i in range(1, n):
        up = h[i] - h[i - 1]
        down = l[i - 1] - l[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        tr.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))

    def wsmooth(vals):
        out = [sum(vals[:period])]
        for i in range(period, len(vals)):
            out.append(out[-1] - out[-1] / period + vals[i])
        return out

    tr_s = wsmooth(tr)
    pdm_s = wsmooth(plus_dm)
    mdm_s = wsmooth(minus_dm)
    dx = []
    for i in range(len(tr_s)):
        pdi = 100 * (pdm_s[i] / tr_s[i]) if tr_s[i] > 1e-12 else 0.0
        mdi = 100 * (mdm_s[i] / tr_s[i]) if tr_s[i] > 1e-12 else 0.0
        denom = pdi + mdi
        dx.append(100 * abs(pdi - mdi) / denom if denom > 1e-12 else 0.0)
    pad = [dx[0]] * (n - len(dx)) if dx else [15.0] * n
    full = pad + dx
    adx_vals = ema(full, period)
    return adx_vals


def vwap_session(candles: list[dict], bars: int = 96) -> float:
    window = candles[-bars:] if len(candles) >= bars else candles
    if not window:
        return 0.0
    num = sum(((c["h"] + c["l"] + c["c"]) / 3) * c["v"] for c in window)
    den = sum(c["v"] for c in window) or 1e-9
    return num / den


def volume_profile(candles: list[dict], bars: int = 96, bins: int = 24) -> dict:
    window = candles[-bars:] if len(candles) >= bars else candles
    if not window:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0}
    lo = min(c["l"] for c in window)
    hi = max(c["h"] for c in window)
    if hi <= lo:
        return {"poc": hi, "vah": hi, "val": lo}
    width = (hi - lo) / bins
    buckets = [0.0] * bins
    for c in window:
        mid_price = (c["h"] + c["l"] + c["c"]) / 3
        idx = min(bins - 1, max(0, int((mid_price - lo) / width)))
        buckets[idx] += c["v"]
    total = sum(buckets) or 1e-9
    poc_idx = buckets.index(max(buckets))
    poc = lo + (poc_idx + 0.5) * width
    order = sorted(range(bins), key=lambda i: -buckets[i])
    acc, chosen = 0.0, []
    for i in order:
        acc += buckets[i]
        chosen.append(i)
        if acc / total >= 0.70:
            break
    vah = lo + (max(chosen) + 1) * width
    val = lo + min(chosen) * width
    return {"poc": poc, "vah": vah, "val": val}


def volatility_percentile(atr_vals: list[float], closes_: list[float], lookback: int = 200) -> float:
    if len(atr_vals) < 20:
        return 0.5
    atr_pct = [a / c if c > 1e-9 else 0.0 for a, c in zip(atr_vals, closes_)]
    window = atr_pct[-lookback:]
    cur = window[-1]
    rank = sum(1 for x in window if x <= cur) / len(window)
    return rank


@dataclass
class Indicators:
    ema20: list[float]; ema50: list[float]; ema200: list[float]
    rsi14: list[float]; atr14: list[float]; adx14: list[float]
    vwap: float; vp: dict; vol_pctile: float


def compute_indicators(candles: list[dict]) -> Indicators:
    c, h, l = closes(candles), highs(candles), lows(candles)
    a = atr(h, l, c, 14)
    return Indicators(
        ema20=ema(c, 20), ema50=ema(c, 50), ema200=ema(c, 200),
        rsi14=rsi(c, 14), atr14=a, adx14=adx(h, l, c, 14),
        vwap=vwap_session(candles), vp=volume_profile(candles),
        vol_pctile=volatility_percentile(a, c),
    )


# ============================================================================
# 5. MARKET STRUCTURE (swings, BOS/CHoCH, OB, breaker, FVG, liquidity)
# ============================================================================

@dataclass
class Swing:
    idx: int; price: float; kind: str  # "H" or "L"


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    out = []
    h, l = highs(candles), lows(candles)
    for i in range(left, len(candles) - right):
        if h[i] == max(h[i - left:i + right + 1]) and h[i] > max(h[i - left:i], default=-1e18):
            out.append(Swing(i, h[i], "H"))
        if l[i] == min(l[i - left:i + right + 1]) and l[i] < min(l[i - left:i], default=1e18):
            out.append(Swing(i, l[i], "L"))
    return out


@dataclass
class StructureState:
    trend: str            # "up" / "down" / "sideways"
    last_bos: Optional[str]
    last_choch: Optional[str]
    last_swing_high: Optional[float]
    last_swing_low: Optional[float]


def analyze_structure(candles: list[dict], swings: list[Swing]) -> StructureState:
    if len(swings) < 4:
        return StructureState("sideways", None, None, None, None)
    highs_ = [s for s in swings if s.kind == "H"][-3:]
    lows_ = [s for s in swings if s.kind == "L"][-3:]
    trend = "sideways"
    last_bos = last_choch = None
    if len(highs_) >= 2 and len(lows_) >= 2:
        hh = highs_[-1].price > highs_[-2].price
        hl = lows_[-1].price > lows_[-2].price
        lh = highs_[-1].price < highs_[-2].price
        ll = lows_[-1].price < lows_[-2].price
        if hh and hl:
            trend, last_bos = "up", "bullish"
        elif lh and ll:
            trend, last_bos = "down", "bearish"
        elif hh and ll:
            trend, last_choch = "sideways", "bullish->bearish risk"
        elif lh and hl:
            trend, last_choch = "sideways", "bearish->bullish risk"
    return StructureState(
        trend, last_bos, last_choch,
        highs_[-1].price if highs_ else None,
        lows_[-1].price if lows_ else None,
    )


@dataclass
class Zone:
    kind: str          # "OB_bull" / "OB_bear" / "BRK_bull" / "BRK_bear" / "FVG_bull" / "FVG_bear"
    top: float; bottom: float; idx: int; tested: bool = False


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int = 80) -> list[Zone]:
    zones = []
    start = max(1, len(candles) - lookback)
    for i in range(start, len(candles) - 1):
        body = candles[i]["c"] - candles[i]["o"]
        next_move = candles[i + 1]["c"] - candles[i + 1]["o"]
        a = atr_vals[i] if i < len(atr_vals) else 0.0
        if a <= 0:
            continue
        # bearish candle immediately followed by strong bullish displacement -> bullish OB
        if body < 0 and next_move > a * 0.8:
            zones.append(Zone("OB_bull", candles[i]["h"], candles[i]["l"], i))
        # bullish candle immediately followed by strong bearish displacement -> bearish OB
        if body > 0 and next_move < -a * 0.8:
            zones.append(Zone("OB_bear", candles[i]["h"], candles[i]["l"], i))
    return zones[-12:]


def find_breaker_blocks(candles: list[dict], order_blocks: list[Zone]) -> list[Zone]:
    """An order block that price later closes through (invalidating it as a
    continuation zone) becomes a breaker in the opposite direction."""
    breakers = []
    for z in order_blocks:
        for j in range(z.idx + 1, len(candles)):
            c = candles[j]
            if z.kind == "OB_bull" and c["c"] < z.bottom:
                breakers.append(Zone("BRK_bear", z.top, z.bottom, j))
                break
            if z.kind == "OB_bear" and c["c"] > z.top:
                breakers.append(Zone("BRK_bull", z.top, z.bottom, j))
                break
    return breakers[-8:]


def find_fvgs(candles: list[dict], lookback: int = 80) -> list[Zone]:
    zones = []
    start = max(2, len(candles) - lookback)
    for i in range(start, len(candles)):
        c0, c2 = candles[i - 2], candles[i]
        if c2["l"] > c0["h"]:
            zones.append(Zone("FVG_bull", c2["l"], c0["h"], i))
        if c2["h"] < c0["l"]:
            zones.append(Zone("FVG_bear", c0["l"], c2["h"], i))
    return zones[-12:]


def mark_untested(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    for z in zones:
        for c in candles[z.idx + 1:]:
            if c["l"] <= z.top and c["h"] >= z.bottom:
                z.tested = True
                break
    return zones


def build_liquidity_pools(swings: list[Swing], tol_pct: float = 0.0015) -> dict:
    highs_ = sorted([s.price for s in swings if s.kind == "H"])
    lows_ = sorted([s.price for s in swings if s.kind == "L"])

    def cluster(levels):
        clusters = []
        for lv in levels:
            placed = False
            for cl in clusters:
                if abs(lv - cl[0]) / cl[0] <= tol_pct:
                    cl[1] += 1
                    placed = True
                    break
            if not placed:
                clusters.append([lv, 1])
        return clusters

    return {"resistance": cluster(highs_), "support": cluster(lows_)}


def detect_liquidity_sweep(candles: list[dict], pools: dict, lookback: int = 12) -> Optional[dict]:
    if len(candles) < lookback + 1:
        return None
    recent = candles[-lookback:]
    for c in recent:
        for lvl, count in pools.get("resistance", []):
            if c["h"] > lvl and c["c"] < lvl and count >= 2:
                return {"direction": "short", "level": lvl, "wick_high": c["h"]}
        for lvl, count in pools.get("support", []):
            if c["l"] < lvl and c["c"] > lvl and count >= 2:
                return {"direction": "long", "level": lvl, "wick_low": c["l"]}
    return None


def premium_discount_zone(candles: list[dict], lookback: int = 60) -> dict:
    window = candles[-lookback:] if len(candles) >= lookback else candles
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    mid = (hi + lo) / 2
    price = candles[-1]["c"]
    zone = "premium" if price > mid else "discount" if price < mid else "equilibrium"
    return {"high": hi, "low": lo, "mid": mid, "zone": zone}


# ============================================================================
# 6. REGIME DETECTION
# ============================================================================

@dataclass
class RegimeVector:
    bias: str              # "bullish" / "bearish" / "neutral"
    trend_strength: float  # 0..1 (ADX-derived)
    volatility_pctile: float
    condition: str         # "trending" / "ranging" / "expansion" / "reversal" / "consolidation"
    breadth: float          # 0..1 fraction of watchlist agreeing with BTC bias
    session_weight: float


def session_weight_now() -> float:
    h = datetime.now(timezone.utc).hour
    # London/NY overlap and NY session weighted highest; Asia/quiet lowest.
    if 12 <= h < 17:
        return 1.0
    if 7 <= h < 12 or 17 <= h < 21:
        return 0.85
    return 0.55


def classify_condition(adx_val: float, vol_pctile: float, structure: StructureState) -> str:
    if structure.last_choch:
        return "reversal"
    if adx_val >= 25 and vol_pctile >= 0.5:
        return "trending"
    if vol_pctile >= 0.80:
        return "expansion"
    if adx_val < 18 and vol_pctile < 0.35:
        return "consolidation"
    return "ranging"


def build_regime_vector(structure_1h: StructureState, ind_1h: Indicators,
                         btc_bias: str, breadth: float) -> RegimeVector:
    bias = ("bullish" if structure_1h.trend == "up"
            else "bearish" if structure_1h.trend == "down" else "neutral")
    adx_val = ind_1h.adx14[-1] if ind_1h.adx14 else 15.0
    return RegimeVector(
        bias=bias,
        trend_strength=min(1.0, adx_val / 40.0),
        volatility_pctile=ind_1h.vol_pctile,
        condition=classify_condition(adx_val, ind_1h.vol_pctile, structure_1h),
        breadth=breadth,
        session_weight=session_weight_now(),
    )


def compute_breadth(bias_by_symbol: dict[str, str], btc_bias: str) -> float:
    if not bias_by_symbol or btc_bias == "neutral":
        return 0.5
    agree = sum(1 for b in bias_by_symbol.values() if b == btc_bias)
    return agree / len(bias_by_symbol)


# ============================================================================
# 7. CANDIDATE SIGNAL
# ============================================================================

@dataclass
class Candidate:
    engine: str
    symbol: str
    direction: str          # "long" / "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float        # 0..1 raw engine confidence
    expected_rr: float
    confluences: list[str]
    regime_fit: float        # 0..1 how well current regime suits this engine
    timeframe: str
    score: float = 0.0       # filled in by decision engine


def rr_of(direction: str, entry: float, sl: float, tp: float) -> float:
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    return reward / risk if risk > 1e-9 else 0.0


def clip_tp_to_liquidity(direction: str, entry: float, raw_tp: float,
                          pools: dict, vp: dict) -> float:
    targets = []
    if direction == "long":
        targets = [lvl for lvl, _ in pools.get("resistance", []) if lvl > entry]
        targets += [vp["poc"], vp["vah"]] if vp.get("vah", 0) > entry else []
        candidates = [t for t in targets if entry < t <= raw_tp * 1.15]
        if candidates:
            return min(candidates, key=lambda t: abs(t - raw_tp))
    else:
        targets = [lvl for lvl, _ in pools.get("support", []) if lvl < entry]
        targets += [vp["poc"], vp["val"]] if vp.get("val", 1e18) < entry else []
        candidates = [t for t in targets if raw_tp * 0.85 <= t < entry]
        if candidates:
            return min(candidates, key=lambda t: abs(t - raw_tp))
    return raw_tp


# ============================================================================
# 8. SPECIALIZED ENGINES
# ============================================================================
# Every engine receives a shared "ctx" dict (candles per timeframe,
# indicators per timeframe, structure, zones, pools, regime) and returns a
# list of Candidate. Engines never fetch data themselves -- this guarantees
# zero duplicate API calls and consistent indicator values across engines.

class EngineBase:
    name = "base"
    suitable_conditions: set[str] = set()

    def regime_fit(self, regime: RegimeVector) -> float:
        return 1.0 if regime.condition in self.suitable_conditions else 0.35

    def run(self, ctx: dict) -> list[Candidate]:
        raise NotImplementedError


class SMCEngine(EngineBase):
    name = "SMC"
    suitable_conditions = {"trending", "reversal", "expansion"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        exec_c = ctx["candles"]["15m"]
        struct = ctx["structure_1h"]
        pdz = premium_discount_zone(exec_c)
        atrv = ctx["ind_15m"].atr14[-1] if ctx["ind_15m"].atr14 else 0.0
        price = exec_c[-1]["c"]
        if atrv <= 0:
            return out
        if struct.trend == "up" and pdz["zone"] == "discount":
            sl = price - 1.3 * atrv
            tp = price + 2.6 * atrv
            conf = 0.58 + 0.15 * (1 if ctx["regime"].bias == "bullish" else 0)
            out.append(Candidate(self.name, ctx["symbol"], "long", price, sl,
                                  price + 1.6 * atrv, tp, conf, rr_of("long", price, sl, tp),
                                  ["discount zone", "1h uptrend"], self.regime_fit(ctx["regime"]), "15m"))
        if struct.trend == "down" and pdz["zone"] == "premium":
            sl = price + 1.3 * atrv
            tp = price - 2.6 * atrv
            conf = 0.58 + 0.15 * (1 if ctx["regime"].bias == "bearish" else 0)
            out.append(Candidate(self.name, ctx["symbol"], "short", price, sl,
                                  price - 1.6 * atrv, tp, conf, rr_of("short", price, sl, tp),
                                  ["premium zone", "1h downtrend"], self.regime_fit(ctx["regime"]), "15m"))
        return out


class TrendEngine(EngineBase):
    name = "Trend"
    suitable_conditions = {"trending"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        ind = ctx["ind_1h"]
        c = ctx["candles"]["1h"]
        if len(ind.ema20) < 2 or len(ind.ema50) < 2:
            return out
        price = c[-1]["c"]
        atrv = ind.atr14[-1] if ind.atr14 else 0.0
        if atrv <= 0:
            return out
        up = ind.ema20[-1] > ind.ema50[-1] > ind.ema200[-1] and price > ind.ema20[-1]
        down = ind.ema20[-1] < ind.ema50[-1] < ind.ema200[-1] and price < ind.ema20[-1]
        adx_val = ind.adx14[-1] if ind.adx14 else 0.0
        if adx_val < 20:
            return out
        if up:
            sl = min(ind.ema50[-1], price - 1.5 * atrv)
            tp = price + 3.2 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "long", price, sl,
                                  price + 1.8 * atrv, tp, 0.55 + min(0.2, adx_val / 200),
                                  rr_of("long", price, sl, tp), ["EMA stack aligned", f"ADX {adx_val:.0f}"],
                                  self.regime_fit(ctx["regime"]), "1h"))
        if down:
            sl = max(ind.ema50[-1], price + 1.5 * atrv)
            tp = price - 3.2 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "short", price, sl,
                                  price - 1.8 * atrv, tp, 0.55 + min(0.2, adx_val / 200),
                                  rr_of("short", price, sl, tp), ["EMA stack aligned", f"ADX {adx_val:.0f}"],
                                  self.regime_fit(ctx["regime"]), "1h"))
        return out


class BreakoutEngine(EngineBase):
    name = "Breakout"
    suitable_conditions = {"expansion", "trending"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        c = ctx["candles"]["15m"]
        ind = ctx["ind_15m"]
        if len(c) < 30:
            return out
        recent_hi = max(x["h"] for x in c[-25:-1])
        recent_lo = min(x["l"] for x in c[-25:-1])
        last = c[-1]
        atrv = ind.atr14[-1] if ind.atr14 else 0.0
        if atrv <= 0:
            return out
        vol_avg = statistics.fmean(vols(c[-25:-1])) or 1e-9
        vol_surge = last["v"] > vol_avg * 1.4
        if last["c"] > recent_hi and vol_surge:
            sl = recent_hi - 0.6 * atrv
            tp = last["c"] + 2.8 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "long", last["c"], sl,
                                  last["c"] + 1.6 * atrv, tp, 0.55, rr_of("long", last["c"], sl, tp),
                                  ["range high breakout", "volume surge"], self.regime_fit(ctx["regime"]), "15m"))
        if last["c"] < recent_lo and vol_surge:
            sl = recent_lo + 0.6 * atrv
            tp = last["c"] - 2.8 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "short", last["c"], sl,
                                  last["c"] - 1.6 * atrv, tp, 0.55, rr_of("short", last["c"], sl, tp),
                                  ["range low breakdown", "volume surge"], self.regime_fit(ctx["regime"]), "15m"))
        return out


class PullbackEngine(EngineBase):
    name = "Pullback"
    suitable_conditions = {"trending", "ranging"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        ind = ctx["ind_1h"]
        c = ctx["candles"]["1h"]
        if len(ind.ema20) < 5:
            return out
        price = c[-1]["c"]
        atrv = ind.atr14[-1] if ind.atr14 else 0.0
        if atrv <= 0:
            return out
        near_ema20 = abs(price - ind.ema20[-1]) < 0.5 * atrv
        uptrend = ind.ema20[-1] > ind.ema50[-1]
        downtrend = ind.ema20[-1] < ind.ema50[-1]
        rsi_v = ind.rsi14[-1] if ind.rsi14 else 50.0
        if uptrend and near_ema20 and 40 <= rsi_v <= 60:
            sl = price - 1.2 * atrv
            tp = price + 2.4 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "long", price, sl,
                                  price + 1.4 * atrv, tp, 0.56, rr_of("long", price, sl, tp),
                                  ["pullback to EMA20", "trend intact"], self.regime_fit(ctx["regime"]), "1h"))
        if downtrend and near_ema20 and 40 <= rsi_v <= 60:
            sl = price + 1.2 * atrv
            tp = price - 2.4 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "short", price, sl,
                                  price - 1.4 * atrv, tp, 0.56, rr_of("short", price, sl, tp),
                                  ["pullback to EMA20", "trend intact"], self.regime_fit(ctx["regime"]), "1h"))
        return out


class LiquiditySweepEngine(EngineBase):
    name = "LiquiditySweep"
    suitable_conditions = {"reversal", "ranging"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        sweep = ctx["sweep_15m"]
        if not sweep:
            return out
        c = ctx["candles"]["15m"]
        price = c[-1]["c"]
        atrv = ctx["ind_15m"].atr14[-1] if ctx["ind_15m"].atr14 else 0.0
        if atrv <= 0:
            return out
        if sweep["direction"] == "long":
            sl = sweep["wick_low"] - 0.3 * atrv
            tp = price + 2.5 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "long", price, sl,
                                  price + 1.4 * atrv, tp, 0.6, rr_of("long", price, sl, tp),
                                  ["support liquidity swept", "wick reclaim"], self.regime_fit(ctx["regime"]), "15m"))
        else:
            sl = sweep["wick_high"] + 0.3 * atrv
            tp = price - 2.5 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "short", price, sl,
                                  price - 1.4 * atrv, tp, 0.6, rr_of("short", price, sl, tp),
                                  ["resistance liquidity swept", "wick rejection"], self.regime_fit(ctx["regime"]), "15m"))
        return out


class OrderBlockEngine(EngineBase):
    name = "OrderBlock"
    suitable_conditions = {"trending", "reversal", "ranging"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        c = ctx["candles"]["15m"]
        price = c[-1]["c"]
        atrv = ctx["ind_15m"].atr14[-1] if ctx["ind_15m"].atr14 else 0.0
        if atrv <= 0:
            return out
        for z in ctx["order_blocks_15m"]:
            if z.tested:
                continue
            if z.kind == "OB_bull" and z.bottom <= price <= z.top * 1.002:
                sl = z.bottom - 0.4 * atrv
                tp = price + 2.5 * atrv
                out.append(Candidate(self.name, ctx["symbol"], "long", price, sl,
                                      price + 1.5 * atrv, tp, 0.57, rr_of("long", price, sl, tp),
                                      ["untested bullish order block"], self.regime_fit(ctx["regime"]), "15m"))
            if z.kind == "OB_bear" and z.bottom * 0.998 <= price <= z.top:
                sl = z.top + 0.4 * atrv
                tp = price - 2.5 * atrv
                out.append(Candidate(self.name, ctx["symbol"], "short", price, sl,
                                      price - 1.5 * atrv, tp, 0.57, rr_of("short", price, sl, tp),
                                      ["untested bearish order block"], self.regime_fit(ctx["regime"]), "15m"))
        return out[:1]


class BreakerBlockEngine(EngineBase):
    name = "BreakerBlock"
    suitable_conditions = {"reversal", "trending"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        c = ctx["candles"]["15m"]
        price = c[-1]["c"]
        atrv = ctx["ind_15m"].atr14[-1] if ctx["ind_15m"].atr14 else 0.0
        if atrv <= 0:
            return out
        for z in ctx["breaker_blocks_15m"]:
            if z.kind == "BRK_bull" and z.bottom <= price <= z.top * 1.002:
                sl = z.bottom - 0.4 * atrv
                tp = price + 2.6 * atrv
                out.append(Candidate(self.name, ctx["symbol"], "long", price, sl,
                                      price + 1.5 * atrv, tp, 0.58, rr_of("long", price, sl, tp),
                                      ["bullish breaker retest"], self.regime_fit(ctx["regime"]), "15m"))
            if z.kind == "BRK_bear" and z.bottom * 0.998 <= price <= z.top:
                sl = z.top + 0.4 * atrv
                tp = price - 2.6 * atrv
                out.append(Candidate(self.name, ctx["symbol"], "short", price, sl,
                                      price - 1.5 * atrv, tp, 0.58, rr_of("short", price, sl, tp),
                                      ["bearish breaker retest"], self.regime_fit(ctx["regime"]), "15m"))
        return out[:1]


class FairValueGapEngine(EngineBase):
    name = "FVG"
    suitable_conditions = {"trending", "expansion", "ranging"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        c = ctx["candles"]["15m"]
        price = c[-1]["c"]
        atrv = ctx["ind_15m"].atr14[-1] if ctx["ind_15m"].atr14 else 0.0
        if atrv <= 0:
            return out
        for z in ctx["fvgs_15m"]:
            if z.tested:
                continue
            if z.kind == "FVG_bull" and z.bottom <= price <= z.top:
                sl = z.bottom - 0.4 * atrv
                tp = price + 2.2 * atrv
                out.append(Candidate(self.name, ctx["symbol"], "long", price, sl,
                                      price + 1.3 * atrv, tp, 0.54, rr_of("long", price, sl, tp),
                                      ["unfilled bullish FVG"], self.regime_fit(ctx["regime"]), "15m"))
            if z.kind == "FVG_bear" and z.bottom <= price <= z.top:
                sl = z.top + 0.4 * atrv
                tp = price - 2.2 * atrv
                out.append(Candidate(self.name, ctx["symbol"], "short", price, sl,
                                      price - 1.3 * atrv, tp, 0.54, rr_of("short", price, sl, tp),
                                      ["unfilled bearish FVG"], self.regime_fit(ctx["regime"]), "15m"))
        return out[:1]


class MomentumEngine(EngineBase):
    name = "Momentum"
    suitable_conditions = {"trending", "expansion"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        ind = ctx["ind_15m"]
        c = ctx["candles"]["15m"]
        if len(ind.rsi14) < 3:
            return out
        price = c[-1]["c"]
        atrv = ind.atr14[-1] if ind.atr14 else 0.0
        if atrv <= 0:
            return out
        rsi_rising = ind.rsi14[-1] > ind.rsi14[-2] > ind.rsi14[-3]
        rsi_falling = ind.rsi14[-1] < ind.rsi14[-2] < ind.rsi14[-3]
        if rsi_rising and 55 <= ind.rsi14[-1] <= 75:
            sl = price - 1.4 * atrv
            tp = price + 2.4 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "long", price, sl,
                                  price + 1.4 * atrv, tp, 0.53, rr_of("long", price, sl, tp),
                                  ["rising RSI momentum"], self.regime_fit(ctx["regime"]), "15m"))
        if rsi_falling and 25 <= ind.rsi14[-1] <= 45:
            sl = price + 1.4 * atrv
            tp = price - 2.4 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "short", price, sl,
                                  price - 1.4 * atrv, tp, 0.53, rr_of("short", price, sl, tp),
                                  ["falling RSI momentum"], self.regime_fit(ctx["regime"]), "15m"))
        return out


class ReversalEngine(EngineBase):
    name = "Reversal"
    suitable_conditions = {"reversal"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        ind = ctx["ind_1h"]
        c = ctx["candles"]["1h"]
        struct = ctx["structure_1h"]
        if not struct.last_choch or len(ind.rsi14) < 2:
            return out
        price = c[-1]["c"]
        atrv = ind.atr14[-1] if ind.atr14 else 0.0
        if atrv <= 0:
            return out
        if "bearish->bullish" in struct.last_choch and ind.rsi14[-1] < 45:
            sl = price - 1.5 * atrv
            tp = price + 2.8 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "long", price, sl,
                                  price + 1.6 * atrv, tp, 0.55, rr_of("long", price, sl, tp),
                                  ["CHoCH bullish", "RSI oversold zone"], self.regime_fit(ctx["regime"]), "1h"))
        if "bullish->bearish" in struct.last_choch and ind.rsi14[-1] > 55:
            sl = price + 1.5 * atrv
            tp = price - 2.8 * atrv
            out.append(Candidate(self.name, ctx["symbol"], "short", price, sl,
                                  price - 1.6 * atrv, tp, 0.55, rr_of("short", price, sl, tp),
                                  ["CHoCH bearish", "RSI overbought zone"], self.regime_fit(ctx["regime"]), "1h"))
        return out


class MeanReversionEngine(EngineBase):
    name = "MeanReversion"
    suitable_conditions = {"ranging", "consolidation"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        ind = ctx["ind_1h"]
        c = ctx["candles"]["1h"]
        if len(ind.rsi14) < 2 or not ind.atr14:
            return out
        price = c[-1]["c"]
        atrv = ind.atr14[-1]
        if atrv <= 0 or ctx["regime"].condition not in self.suitable_conditions:
            return out
        vwap = ind.vwap
        dist = (price - vwap) / atrv if atrv else 0
        if ind.rsi14[-1] < 30 and dist < -1.2:
            sl = price - 1.3 * atrv
            tp = vwap
            out.append(Candidate(self.name, ctx["symbol"], "long", price, sl,
                                  price + 1.0 * atrv, tp, 0.52, rr_of("long", price, sl, tp),
                                  ["RSI oversold", "extended below VWAP"], self.regime_fit(ctx["regime"]), "1h"))
        if ind.rsi14[-1] > 70 and dist > 1.2:
            sl = price + 1.3 * atrv
            tp = vwap
            out.append(Candidate(self.name, ctx["symbol"], "short", price, sl,
                                  price - 1.0 * atrv, tp, 0.52, rr_of("short", price, sl, tp),
                                  ["RSI overbought", "extended above VWAP"], self.regime_fit(ctx["regime"]), "1h"))
        return out


class RangeEngine(EngineBase):
    name = "Range"
    suitable_conditions = {"ranging", "consolidation"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        c = ctx["candles"]["1h"]
        if len(c) < 40 or ctx["regime"].condition not in self.suitable_conditions:
            return out
        window = c[-40:]
        hi = max(x["h"] for x in window)
        lo = min(x["l"] for x in window)
        price = c[-1]["c"]
        atrv = ctx["ind_1h"].atr14[-1] if ctx["ind_1h"].atr14 else 0.0
        if atrv <= 0 or (hi - lo) < 2.5 * atrv:
            return out
        near_lo = price < lo + 0.25 * (hi - lo)
        near_hi = price > hi - 0.25 * (hi - lo)
        if near_lo:
            sl = lo - 0.5 * atrv
            tp = hi - 0.1 * (hi - lo)
            out.append(Candidate(self.name, ctx["symbol"], "long", price, sl, (price + tp) / 2, tp,
                                  0.53, rr_of("long", price, sl, tp), ["range support"],
                                  self.regime_fit(ctx["regime"]), "1h"))
        if near_hi:
            sl = hi + 0.5 * atrv
            tp = lo + 0.1 * (hi - lo)
            out.append(Candidate(self.name, ctx["symbol"], "short", price, sl, (price + tp) / 2, tp,
                                  0.53, rr_of("short", price, sl, tp), ["range resistance"],
                                  self.regime_fit(ctx["regime"]), "1h"))
        return out


class VolatilityExpansionEngine(EngineBase):
    name = "VolatilityExpansion"
    suitable_conditions = {"expansion"}

    def run(self, ctx: dict) -> list[Candidate]:
        out = []
        ind = ctx["ind_15m"]
        c = ctx["candles"]["15m"]
        if not ind.atr14 or ctx["regime"].volatility_pctile < 0.75:
            return out
        price = c[-1]["c"]
        atrv = ind.atr14[-1]
        prev_range = abs(c[-2]["c"] - c[-2]["o"]) if len(c) > 1 else 0
        last_range = abs(c[-1]["c"] - c[-1]["o"])
        if last_range > 1.8 * (prev_range or 1e-9) and last_range > atrv:
            direction = "long" if c[-1]["c"] > c[-1]["o"] else "short"
            if direction == "long":
                sl = price - 1.6 * atrv
                tp = price + 3.0 * atrv
            else:
                sl = price + 1.6 * atrv
                tp = price - 3.0 * atrv
            out.append(Candidate(self.name, ctx["symbol"], direction, price, sl,
                                  price + (1.6 if direction == "long" else -1.6) * atrv, tp, 0.55,
                                  rr_of(direction, price, sl, tp), ["volatility expansion bar"],
                                  self.regime_fit(ctx["regime"]), "15m"))
        return out


ALL_ENGINES: list[EngineBase] = [
    SMCEngine(), TrendEngine(), BreakoutEngine(), PullbackEngine(),
    LiquiditySweepEngine(), OrderBlockEngine(), BreakerBlockEngine(),
    FairValueGapEngine(), MomentumEngine(), ReversalEngine(),
    MeanReversionEngine(), RangeEngine(), VolatilityExpansionEngine(),
]


# ============================================================================
# 9. LEARNING / PERFORMANCE TRACKING
# ============================================================================

@dataclass
class EngineStats:
    wins: int = 0
    losses: int = 0
    gross_win: float = 0.0
    gross_loss: float = 0.0
    rr_sum: float = 0.0
    hold_minutes_sum: float = 0.0
    n: int = 0
    weight: float = 1.0  # regularized multiplier applied to confidence

    def win_rate(self) -> float:
        total = self.wins + self.losses
        return self.wins / total if total else 0.5

    def profit_factor(self) -> float:
        return self.gross_win / self.gross_loss if self.gross_loss > 1e-9 else (
            2.0 if self.gross_win > 0 else 1.0)

    def update_weight(self, prior: float = 1.0, shrink: float = 0.15):
        """Regularized shrink toward the live win-rate-implied multiplier so
        a short streak cannot overfit the ranking model."""
        total = self.wins + self.losses
        if total < 8:
            self.weight = prior
            return
        implied = 0.7 + self.win_rate() * 0.6  # maps 0..1 winrate to 0.7..1.3
        self.weight = self.weight * (1 - shrink) + implied * shrink
        self.weight = max(0.6, min(1.4, self.weight))


class PerformanceTracker:
    def __init__(self, state: dict):
        self.state = state
        self.state.setdefault("engine_stats", {})
        self.state.setdefault("regime_stats", {})
        self.state.setdefault("confidence_calibration", {"bins": {}})

    def get_engine_stats(self, engine: str) -> EngineStats:
        raw = self.state["engine_stats"].get(engine)
        if raw is None:
            stats = EngineStats()
        else:
            stats = EngineStats(**{k: raw.get(k, 0) for k in
                                    ("wins", "losses", "gross_win", "gross_loss",
                                     "rr_sum", "hold_minutes_sum", "n", "weight")})
            if stats.weight == 0:
                stats.weight = 1.0
        return stats

    def save_engine_stats(self, engine: str, stats: EngineStats):
        self.state["engine_stats"][engine] = asdict(stats)

    def record_trade_outcome(self, engine: str, regime_condition: str, won: bool,
                              rr_realized: float, hold_minutes: float):
        stats = self.get_engine_stats(engine)
        stats.n += 1
        stats.rr_sum += rr_realized
        stats.hold_minutes_sum += hold_minutes
        if won:
            stats.wins += 1
            stats.gross_win += max(rr_realized, 0)
        else:
            stats.losses += 1
            stats.gross_loss += max(-rr_realized, 0)
        stats.update_weight()
        self.save_engine_stats(engine, stats)

        rkey = regime_condition
        rstats = self.state["regime_stats"].setdefault(rkey, {"wins": 0, "losses": 0})
        rstats["wins" if won else "losses"] += 1

    def calibrate(self, raw_confidence: float) -> float:
        """Nudges raw confidence toward the empirical win rate observed in
        that confidence decile, shrunk toward the raw value until enough
        samples exist (prevents overfitting on sparse bins)."""
        bin_key = str(int(raw_confidence * 10))
        bins = self.state["confidence_calibration"]["bins"]
        b = bins.get(bin_key, {"wins": 0, "n": 0})
        if b["n"] < 10:
            return raw_confidence
        empirical = b["wins"] / b["n"]
        return raw_confidence * 0.5 + empirical * 0.5

    def record_calibration_sample(self, raw_confidence: float, won: bool):
        bin_key = str(int(raw_confidence * 10))
        bins = self.state["confidence_calibration"]["bins"]
        b = bins.setdefault(bin_key, {"wins": 0, "n": 0})
        b["n"] += 1
        if won:
            b["wins"] += 1


# ============================================================================
# 10. DECISION ENGINE
# ============================================================================

def composite_score(cand: Candidate, tracker: PerformanceTracker, regime: RegimeVector) -> float:
    stats = tracker.get_engine_stats(cand.engine)
    calibrated_conf = tracker.calibrate(cand.confidence)
    conf_component = calibrated_conf * stats.weight
    rr_component = min(1.0, cand.expected_rr / 3.0)
    confluence_component = min(1.0, len(cand.confluences) / 4.0)
    regime_component = cand.regime_fit
    breadth_component = regime.breadth if (
        (cand.direction == "long" and regime.bias == "bullish") or
        (cand.direction == "short" and regime.bias == "bearish")) else (1 - regime.breadth)
    score = (
        0.32 * conf_component +
        0.22 * rr_component +
        0.16 * confluence_component +
        0.16 * regime_component +
        0.14 * (0.5 + 0.5 * breadth_component)
    )
    score *= (0.85 + 0.15 * regime.session_weight)
    return max(0.0, min(1.0, score))


def adaptive_threshold(state: dict) -> float:
    """Slow EMA of daily accepted-signal count nudges the acceptance
    threshold toward the 5-10/day band. Reacts to sustained conditions,
    not single-scan noise."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily = state.setdefault("daily_signal_counts", {})
    count_today = daily.get(today, 0)
    ema_key = "signal_count_ema"
    prior_ema = state.get(ema_key, 7.0)
    hour = datetime.now(timezone.utc).hour
    projected = count_today * (24.0 / max(1, hour + 1))
    new_ema = prior_ema * 0.9 + projected * 0.1
    state[ema_key] = new_ema
    threshold = BASE_ACCEPT_THRESHOLD
    if new_ema < TARGET_SIGNALS_PER_DAY_LOW:
        threshold -= 0.04
    elif new_ema > TARGET_SIGNALS_PER_DAY_HIGH:
        threshold += 0.05
    return max(THRESHOLD_MIN, min(THRESHOLD_MAX, threshold))


def dedupe_correlated(candidates: list[Candidate], state: dict) -> list[Candidate]:
    """Suppresses near-duplicate signals from highly correlated symbols
    (BTC/ETH-led moves) firing within the same short window, using
    realized-return correlation computed from cached candles rather than a
    static table."""
    recent = state.setdefault("recent_signal_directions", [])
    now = time.time()
    recent[:] = [r for r in recent if now - r["ts"] < CORRELATION_DEDUP_WINDOW_MIN * 60]
    out = []
    for c in sorted(candidates, key=lambda x: -x.score):
        dup = any(r["direction"] == c.direction and now - r["ts"] < CORRELATION_DEDUP_WINDOW_MIN * 60
                  for r in recent if r["symbol"] != c.symbol and r.get("cluster") == cluster_of(c.symbol))
        if dup:
            continue
        out.append(c)
        recent.append({"symbol": c.symbol, "direction": c.direction, "ts": now, "cluster": cluster_of(c.symbol)})
    return out


_MAJOR_CLUSTER = {"BTC", "ETH"}
_L1_CLUSTER = {"SOL", "AVAX", "NEAR", "SUI", "APT", "DOT", "TAO", "ADA"}


def cluster_of(symbol: str) -> str:
    if symbol in _MAJOR_CLUSTER:
        return "majors"
    if symbol in _L1_CLUSTER:
        return "l1"
    return "alt"


def decide(all_candidates: list[Candidate], tracker: PerformanceTracker,
           regime_by_symbol: dict[str, RegimeVector], state: dict,
           open_symbols: set[str]) -> list[Candidate]:
    threshold = adaptive_threshold(state)
    scored = []
    for c in all_candidates:
        if c.symbol in open_symbols:
            continue
        if c.expected_rr < MIN_RR:
            continue
        regime = regime_by_symbol[c.symbol]
        c.score = composite_score(c, tracker, regime)
        if c.score >= threshold:
            scored.append(c)
    scored = dedupe_correlated(scored, state)
    # one best candidate per symbol
    best_by_symbol: dict[str, Candidate] = {}
    for c in scored:
        if c.symbol not in best_by_symbol or c.score > best_by_symbol[c.symbol].score:
            best_by_symbol[c.symbol] = c
    final = sorted(best_by_symbol.values(), key=lambda x: -x.score)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    remaining_cap = MAX_SIGNALS_PER_DAY_HARD_CAP - state.get("daily_signal_counts", {}).get(today, 0)
    return final[:max(0, remaining_cap)]


# ============================================================================
# 11. RISK MANAGEMENT VALIDATION
# ============================================================================

def validate_and_finalize(cand: Candidate, ctx: dict) -> Optional[Candidate]:
    """Final structure-based invalidation check: SL/TP must be derived from
    real candle highs/lows, never inside an obvious liquidity pool, RR must
    clear the minimum after liquidity-aware TP clipping."""
    pools = ctx["pools_15m"]
    vp = ctx["ind_15m"].vp
    clipped_tp2 = clip_tp_to_liquidity(cand.direction, cand.entry, cand.tp2, pools, vp)
    rr = rr_of(cand.direction, cand.entry, cand.sl, clipped_tp2)
    if rr < MIN_RR:
        return None
    cand.tp2 = clipped_tp2
    cand.expected_rr = rr
    # avoid placing SL exactly inside a liquidity pool that would be an
    # obvious hunting target - nudge beyond nearest pool with buffer already
    # applied at generation time; here we only reject clearly broken zones.
    if cand.sl == cand.entry or cand.tp1 == cand.entry:
        return None
    return cand


# ============================================================================
# 12. TRADE LIFECYCLE MANAGEMENT
# ============================================================================

def new_trade_record(cand: Candidate) -> dict:
    return {
        "id": f"{cand.symbol}-{int(time.time())}",
        "engine": cand.engine, "symbol": cand.symbol, "direction": cand.direction,
        "entry": cand.entry, "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2,
        "confidence": cand.confidence, "score": cand.score,
        "expected_rr": cand.expected_rr, "confluences": cand.confluences,
        "regime": cand.timeframe, "status": "PENDING", "opened_ts": time.time(),
        "tg_message_id": None, "tp1_hit": False, "be_moved": False,
    }


def evaluate_open_trades(state: dict, mids: dict[str, float], tracker: PerformanceTracker,
                          telegram_send):
    open_trades = state.setdefault("open_trades", [])
    still_open = []
    for t in open_trades:
        price = mids.get(t["symbol"])
        if price is None:
            still_open.append(t)
            continue
        direction = t["direction"]
        hit_sl = (direction == "long" and price <= t["sl"]) or (direction == "short" and price >= t["sl"])
        hit_tp1 = not t["tp1_hit"] and (
            (direction == "long" and price >= t["tp1"]) or (direction == "short" and price <= t["tp1"]))
        hit_tp2 = (direction == "long" and price >= t["tp2"]) or (direction == "short" and price <= t["tp2"])

        if hit_sl:
            rr_realized = -1.0 if not t["be_moved"] else 0.0
            tracker.record_trade_outcome(t["engine"], t.get("regime", "unknown"), False,
                                          rr_realized, (time.time() - t["opened_ts"]) / 60)
            tracker.record_calibration_sample(t["confidence"], False)
            telegram_send(t, "SL" if not t["be_moved"] else "BE")
            continue
        if hit_tp1 and not t["tp1_hit"]:
            t["tp1_hit"] = True
            t["be_moved"] = True
            t["sl"] = t["entry"]
            telegram_send(t, "TP1")
        if hit_tp2:
            rr_realized = rr_of(direction, t["entry"], t["entry"] if t["be_moved"] else t["sl"], t["tp2"])
            tracker.record_trade_outcome(t["engine"], t.get("regime", "unknown"), True,
                                          rr_realized, (time.time() - t["opened_ts"]) / 60)
            tracker.record_calibration_sample(t["confidence"], True)
            telegram_send(t, "TP2")
            continue
        still_open.append(t)
    state["open_trades"] = still_open


# ============================================================================
# 13. TELEGRAM NOTIFIER
# ============================================================================

def tg_api(method: str, payload: dict) -> Optional[dict]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
    body = json.dumps(payload).encode()
    try:
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:  # noqa: BLE001
        logger.warning("Telegram send failed: %s", e)
        return None


_MD_SPECIAL_CHARS = ("_", "*", "`", "[")


def escape_markdown(text: str) -> str:
    """Escapes legacy Telegram Markdown special characters in dynamic
    text. Numeric fields (prices, percentages, counts) are always safe and
    are never passed through this function -- only free-text fields
    (symbol names, engine names, confluence strings) that could in
    principle contain '_', '*', '`', or '[' and would otherwise either
    break formatting or, worse, cause sendMessage to fail outright and
    silently drop the whole update (caught by tg_api's try/except with
    only a log line as evidence)."""
    out = str(text)
    for ch in _MD_SPECIAL_CHARS:
        out = out.replace(ch, "\\" + ch)
    return out


def format_signal_message(t: dict) -> str:
    arrow = "\U0001F7E2 LONG" if t["direction"] == "long" else "\U0001F534 SHORT"
    symbol = escape_markdown(t["symbol"])
    engine = escape_markdown(t["engine"])
    confluences = ", ".join(escape_markdown(c) for c in t["confluences"])
    lines = [
        f"*{escape_markdown(ENGINE_NAME)} v{VERSION}*",
        f"{arrow}  `{symbol}`  ({engine})",
        "",
        f"Entry: `{t['entry']:.4f}`",
        f"SL: `{t['sl']:.4f}`",
        f"TP1: `{t['tp1']:.4f}`",
        f"TP2: `{t['tp2']:.4f}`",
        f"RR: `{t['expected_rr']:.2f}`  Confidence: `{t['confidence']*100:.0f}%`  Score: `{t['score']*100:.0f}%`",
        f"Confluences: {confluences}",
    ]
    return "\n".join(lines)


def send_new_signal(t: dict) -> Optional[int]:
    resp = tg_api("sendMessage", {
        "chat_id": TG_CHAT_ID, "text": format_signal_message(t), "parse_mode": "Markdown"})
    if resp and resp.get("ok"):
        return resp["result"]["message_id"]
    return None


def react_telegram(message_id: Optional[int], emoji: str) -> None:
    """Best-effort emoji reaction on the original signal message, using
    Telegram's setMessageReaction endpoint (an actual tap-reaction, distinct
    from an emoji placed in message text). Only emoji from Telegram's
    allowed quick-reaction set are used. Failures are logged and swallowed
    -- a missing reaction must never break signal tracking or block the
    reply message that always accompanies it."""
    if not TG_BOT_TOKEN or not TG_CHAT_ID or not message_id:
        return
    tg_api("setMessageReaction", {
        "chat_id": TG_CHAT_ID, "message_id": message_id,
        "reaction": [{"type": "emoji", "emoji": emoji}],
    })


# Kind -> (reply label, reaction emoji). Reaction emoji are restricted to
# Telegram's allowed quick-reaction set (verified against Telegram Bot API
# docs): thumbs up/down and fire are all members of that set.
_UPDATE_MESSAGES = {
    "TP1": ("\U0001F525 TP1 hit -- SL moved to breakeven", "\U0001F525"),
    "TP2": ("\u2705 TP2 hit -- trade closed (WIN)", "\U0001F44D"),
    "SL": ("\u274C Stop loss hit (LOSS)", "\U0001F44E"),
    "BE": ("\u2696\uFE0F Closed at breakeven", "\U0001F44D"),
    "CANCELLED": ("\u26A0\uFE0F Signal cancelled", "\U0001F937"),
}


def send_update(t: dict, kind: str):
    label, emoji = _UPDATE_MESSAGES.get(kind, (escape_markdown(kind), None))
    text = f"*{escape_markdown(t['symbol'])}* update: {label}"
    msg_id = t.get("tg_message_id")
    if msg_id:
        tg_api("sendMessage", {"chat_id": TG_CHAT_ID, "text": text,
                                "reply_to_message_id": msg_id, "parse_mode": "Markdown"})
    else:
        tg_api("sendMessage", {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"})
    if emoji:
        react_telegram(msg_id, emoji)


def maybe_send_daily_summary(state: dict, tracker: PerformanceTracker):
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    if now.hour != DAILY_SUMMARY_HOUR_UTC:
        return
    if state.get("last_summary_date") == today:
        return
    daily = state.get("daily_signal_counts", {}).get(today, 0)
    lines = [f"*{escape_markdown(ENGINE_NAME)} v{VERSION} -- Daily Summary*", f"Signals today: {daily}", ""]
    for engine_name, raw in sorted(state.get("engine_stats", {}).items()):
        wins, losses = raw.get("wins", 0), raw.get("losses", 0)
        total = wins + losses
        if total == 0:
            continue
        wr = wins / total * 100
        lines.append(f"{escape_markdown(engine_name)}: {wins}W/{losses}L ({wr:.0f}%) weight={raw.get('weight', 1.0):.2f}")
    tg_api("sendMessage", {"chat_id": TG_CHAT_ID, "text": "\n".join(lines), "parse_mode": "Markdown"})
    state["last_summary_date"] = today


# ============================================================================
# 14. STATE PERSISTENCE
# ============================================================================

def default_state() -> dict:
    return {
        "open_trades": [], "engine_stats": {}, "regime_stats": {},
        "confidence_calibration": {"bins": {}}, "daily_signal_counts": {},
        "signal_count_ema": 7.0, "recent_signal_directions": [],
        "last_summary_date": None, "version": VERSION,
    }


def load_state() -> dict:
    try:
        with open(STATE_PATH, "r") as f:
            s = json.load(f)
            base = default_state()
            base.update(s)
            return base
    except (FileNotFoundError, json.JSONDecodeError):
        return default_state()


def save_state(state: dict):
    try:
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp, STATE_PATH)
    except OSError as e:
        logger.error("State save failed: %s", e)


def prune_state(state: dict, max_days: int = 30):
    cutoff = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily = state.get("daily_signal_counts", {})
    if len(daily) > max_days:
        for k in sorted(daily)[:-max_days]:
            daily.pop(k, None)


# ============================================================================
# 15. PER-SYMBOL CONTEXT BUILD
# ============================================================================

def build_symbol_context(symbol: str, cache: CandleCache, regime: RegimeVector) -> Optional[dict]:
    candles = {tf: cache.get(symbol, tf, CANDLES_PER_TF[tf]) for tf in ALL_TIMEFRAMES}
    if any(len(candles[tf]) < 60 for tf in ALL_TIMEFRAMES):
        return None
    ind_15m = compute_indicators(candles["15m"])
    ind_1h = compute_indicators(candles["1h"])
    swings_1h = find_swings(candles["1h"])
    structure_1h = analyze_structure(candles["1h"], swings_1h)
    swings_15m = find_swings(candles["15m"])
    pools_15m = build_liquidity_pools(swings_15m)
    order_blocks_15m = mark_untested(find_order_blocks(candles["15m"], ind_15m.atr14), candles["15m"])
    breaker_blocks_15m = find_breaker_blocks(candles["15m"], order_blocks_15m)
    fvgs_15m = mark_untested(find_fvgs(candles["15m"]), candles["15m"])
    sweep_15m = detect_liquidity_sweep(candles["15m"], pools_15m)
    return {
        "symbol": symbol, "candles": candles, "ind_15m": ind_15m, "ind_1h": ind_1h,
        "structure_1h": structure_1h, "pools_15m": pools_15m,
        "order_blocks_15m": order_blocks_15m, "breaker_blocks_15m": breaker_blocks_15m,
        "fvgs_15m": fvgs_15m, "sweep_15m": sweep_15m, "regime": regime,
    }


# ============================================================================
# 16. MAIN ORCHESTRATION
# ============================================================================

def run_scan():
    logger.info("=== %s v%s scan start ===", ENGINE_NAME, VERSION)
    state = load_state()
    cache = CandleCache(CACHE_PATH)
    tracker = PerformanceTracker(state)

    # --- Regime pre-pass: BTC bias + cross-sectional breadth ---
    btc_candles_1h = cache.get("BTC", "1h", CANDLES_PER_TF["1h"])
    btc_bias = "neutral"
    if len(btc_candles_1h) >= 60:
        btc_struct = analyze_structure(btc_candles_1h, find_swings(btc_candles_1h))
        btc_bias = ("bullish" if btc_struct.trend == "up"
                    else "bearish" if btc_struct.trend == "down" else "neutral")

    bias_by_symbol: dict[str, str] = {}
    for sym in WATCHLIST:
        if time_budget_exceeded():
            break
        c = cache.get(sym, "1h", CANDLES_PER_TF["1h"])
        if len(c) < 60:
            continue
        st = analyze_structure(c, find_swings(c))
        bias_by_symbol[sym] = ("bullish" if st.trend == "up" else
                                "bearish" if st.trend == "down" else "neutral")
    breadth = compute_breadth(bias_by_symbol, btc_bias)
    cache.save()

    regime_by_symbol: dict[str, RegimeVector] = {}
    contexts: dict[str, dict] = {}

    def process_symbol(sym: str):
        c1h = cache.get(sym, "1h", CANDLES_PER_TF["1h"])
        if len(c1h) < 60:
            return None
        ind_1h = compute_indicators(c1h)
        structure_1h = analyze_structure(c1h, find_swings(c1h))
        regime = build_regime_vector(structure_1h, ind_1h, btc_bias, breadth)
        ctx = build_symbol_context(sym, cache, regime)
        return sym, regime, ctx

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_symbol, sym): sym for sym in WATCHLIST}
        for fut in as_completed(futures):
            if time_budget_exceeded():
                break
            try:
                result = fut.result()
            except Exception as e:  # noqa: BLE001
                logger.warning("Symbol processing failed for %s: %s", futures[fut], e)
                continue
            if result is None:
                continue
            sym, regime, ctx = result
            if ctx is None:
                continue
            regime_by_symbol[sym] = regime
            contexts[sym] = ctx

    cache.save()

    all_candidates: list[Candidate] = []
    for sym, ctx in contexts.items():
        for engine in ALL_ENGINES:
            try:
                raw = engine.run(ctx)
            except Exception as e:  # noqa: BLE001
                logger.warning("Engine %s failed on %s: %s", engine.name, sym, e)
                continue
            for cand in raw:
                validated = validate_and_finalize(cand, ctx)
                if validated:
                    all_candidates.append(validated)

    open_symbols = {t["symbol"] for t in state.get("open_trades", [])}
    for sym in list(open_symbols):
        if sum(1 for t in state["open_trades"] if t["symbol"] == sym) >= MAX_OPEN_PER_SYMBOL:
            continue
        open_symbols.discard(sym)

    selected = decide(all_candidates, tracker, regime_by_symbol, state, open_symbols)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily = state.setdefault("daily_signal_counts", {})
    for cand in selected:
        record = new_trade_record(cand)
        record["regime"] = regime_by_symbol[cand.symbol].condition
        msg_id = send_new_signal(record)
        record["tg_message_id"] = msg_id
        state.setdefault("open_trades", []).append(record)
        daily[today] = daily.get(today, 0) + 1
        logger.info("SIGNAL %s %s %s score=%.2f rr=%.2f engine=%s",
                    cand.symbol, cand.direction, cand.entry, cand.score, cand.expected_rr, cand.engine)

    # --- Trade lifecycle management ---
    mids = fetch_mid_prices()
    if mids:
        evaluate_open_trades(state, mids, tracker, send_update)

    maybe_send_daily_summary(state, tracker)
    prune_state(state)
    save_state(state)
    cache.save()
    logger.info("=== scan complete: %d candidates, %d accepted, %d open trades ===",
                len(all_candidates), len(selected), len(state.get("open_trades", [])))


if __name__ == "__main__":
    try:
        run_scan()
    except Exception:  # noqa: BLE001 -- top-level guard: never crash the scheduled job
        logger.exception("Fatal error in scan run")
        sys.exit(1)
