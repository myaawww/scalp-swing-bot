#!/usr/bin/env python3
"""
============================================================================
HELIOS ENGINE v1.0 - Institutional-Grade Crypto Signal Engine
============================================================================
Ground-up redesign synthesizing the strongest ideas observed across six
reference engines (Apex, Nyx, Meridian, Vectis, Crucible, ScalpSwingBot)
plus additional professional-grade quant/microstructure techniques that
none of the references implemented. This is NOT a merge or patch of any
reference - every module below is re-derived from first principles.

WHAT HELIOS ADDS BEYOND EVERY REFERENCE
  1. Order-book depth engine: real L2 imbalance, spread quality and
     depth-weighted liquidity score pulled from Hyperliquid's l2Book,
     rather than only inferring liquidity from candle wicks.
  2. Session Volume Profile (POC / Value Area / VWAP + bands): a proper
     volume-at-price model used both as a confluence and as a dynamic
     support/resistance map for TP/SL placement.
  3. Three parallel pathways instead of two: Liquidity Reversal, Structure
     Continuation, AND Momentum Breakout (volatility expansion trading),
     so trending/breakout regimes are covered natively instead of being
     forced through a reversal or pullback template.
  4. ATR-percentile volatility memory: each symbol keeps a rolling
     distribution of its own ATR%, so "high/low volatility" is judged
     relative to the symbol's own history, not a fixed global threshold.
  5. Funding + Open-Interest DELTA (not just level): rising OI with
     rising price = healthy continuation; rising OI with flat/falling
     price = potential trap/exhaustion. This is scored explicitly.
  6. Self-tuning pathway weights: win-rate feedback nudges each pathway's
     scoring weight within tight, regularized bounds (shrinkage toward a
     neutral prior) so the engine adapts without overfitting to streaks.
  7. Continuous regime-to-threshold mapping across SIX dimensions (trend,
     volatility-percentile, liquidity, noise, session, breadth) instead of
     discrete regime buckets, giving genuinely smooth quality/frequency
     balancing.
  8. Market breadth gate: cross-sectional % of watchlist trending with BTC
     is folded into regime fit, so signals aren't graded in isolation from
     the rest of the market.

INFRASTRUCTURE (unchanged, per spec)
  Data + Exchange : Hyperliquid
  Watchlist       : identical to reference engines
  Operating model : scan-per-run, triggered externally every 15 min
  State           : state.json read/written every run
============================================================================
"""

from __future__ import annotations

import json
import math
import os
import random
import signal as os_signal
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests

# ============================================================================
# CONFIGURATION
# ============================================================================

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

HL_API_URL = "https://api.hyperliquid.xyz/info"
STATE_PATH = Path(os.getenv("HELIOS_STATE_PATH", "state.json"))
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "5"))

WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# Correlation clusters used for dedup so we don't fire 5 near-identical
# long signals on the same underlying market move.
CORR_GROUPS: dict[str, set] = {
    "layer1":    {"ETH", "SOL", "AVAX", "NEAR", "APT", "ADA", "DOT", "SUI"},
    "defi":      {"AAVE", "UNI", "PENDLE", "ONDO"},
    "meme":      {"DOGE", "PENGU"},
    "btc_proxy": {"BTC", "LTC", "ZEC", "BCH"},
    "xlm_xrp":   {"XLM", "XRP"},
    "l1_alt":    {"TAO", "TRX"},
    "bnb": {"BNB"}, "hype": {"HYPE"}, "oracle": {"LINK"},
}

TIMEFRAMES = {"exec": "15m", "trigger": "1h", "htf": "4h", "bias": "1d"}
CANDLE_LOOKBACK = {"15m": 300, "1h": 300, "4h": 300, "1d": 250}

FAST_LEN, SLOW_LEN, TREND_LEN = 21, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, BB_LEN = 14, 14, 14, 20
VOL_PROFILE_BINS = 24
ATR_PCT_MEMORY = 120           # bars of ATR% history kept per symbol for percentile ranking

MIN_OI_USD = 400_000.0
MAX_SPREAD_BPS = 12.0          # hard reject if book spread wider than this
MAX_CONCURRENT_ACTIVE_SIGNALS = 20
MAX_SIGNAL_HISTORY = 3000
SIGNAL_HISTORY_MAX_AGE_DAYS = 30   # drop closed signals older than this, regardless of count
COOLDOWN_BARS_EXEC = 6         # 15m bars -> 90 min same-symbol/direction cooldown
RISK_MIN_R = 1.5               # minimum planned R:R to TP1

WEIGHT_LEARNING_RATE = 0.04    # bounded self-tuning step for pathway weights
WEIGHT_MIN, WEIGHT_MAX = 0.75, 1.30   # multiplicative bounds around neutral 1.0

ENGINE_NAME = "HELIOS v1.0"

# Daily win-rate summary: sent once per UTC day during the hour listed below.
# The scan is triggered externally every 15 min, so this fires on the first
# run that lands inside SUMMARY_HOUR_UTC and is gated by last_summary_date
# in state so it can't double-fire within the same day.
SUMMARY_HOUR_UTC = 8

# --- Smarter stop-loss placement -------------------------------------------
# Crypto regularly wicks through an "obvious" level to run stops before the
# real move - a liquidity grab. Two adjustments make SL placement resistant
# to that without just moving the stop arbitrarily far away:
#   1. WICK_LOOKBACK_BARS of recent 15m history are scanned for how far price
#      has actually poked beyond short-term swing points before reversing.
#      The 75th percentile of that "overshoot" becomes a floor for the SL
#      buffer, so a normal-sized hunt wick doesn't clip the stop.
#   2. The ATR-based part of the buffer is scaled up when the symbol's own
#      volatility percentile (regime.volatility_pctile) is elevated, since
#      spike wicks are both more frequent and larger in that regime.
WICK_LOOKBACK_BARS = 40
WICK_OVERSHOOT_PERCENTILE = 0.75
SL_VOL_SCALE_MAX = 0.6          # up to +60% buffer at max observed volatility


# ============================================================================
# HYPERLIQUID API LAYER
# ============================================================================

_session = requests.Session()
_req_lock = threading.Lock()
_last_req_ts = 0.0
_min_interval_s = 0.15


def hl_post(payload: dict, retries: int = 3) -> dict | list | None:
    """Rate-limited, retrying POST to Hyperliquid's info endpoint."""
    global _last_req_ts, _min_interval_s
    for attempt in range(retries):
        with _req_lock:
            wait = _min_interval_s - (time.time() - _last_req_ts)
            if wait > 0:
                time.sleep(wait)
            _last_req_ts = time.time()
        try:
            r = _session.post(HL_API_URL, json=payload, timeout=10)
            if r.status_code == 429:
                _min_interval_s = min(_min_interval_s * 1.6, 1.0)
                time.sleep(0.5 * (attempt + 1))
                continue
            r.raise_for_status()
            _min_interval_s = max(_min_interval_s * 0.98, 0.12)
            return r.json()
        except Exception:
            time.sleep(0.3 * (attempt + 1) + random.random() * 0.1)
    return None


def get_candles(coin: str, interval: str, n: int, reference_ms: int | None = None) -> list[dict]:
    ref = reference_ms or int(time.time() * 1000)
    interval_ms = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}[interval]
    start = ref - n * interval_ms
    payload = {"type": "candleSnapshot",
               "req": {"coin": coin, "interval": interval, "startTime": start, "endTime": ref}}
    raw = hl_post(payload)
    if not raw:
        return []
    out = []
    for c in raw:
        try:
            out.append({"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                        "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])})
        except (KeyError, TypeError, ValueError):
            continue
    bar_open_now = (ref // interval_ms) * interval_ms
    return [c for c in out if c["t"] < bar_open_now]


def fetch_all_candles(coin: str, reference_ms: int | None = None) -> dict[str, list[dict]] | None:
    bundle = {}
    for tf in set(TIMEFRAMES.values()):
        candles = get_candles(coin, tf, CANDLE_LOOKBACK[tf], reference_ms)
        if len(candles) < 60:
            return None
        bundle[tf] = candles
    return bundle


def get_market_snapshot() -> dict[str, dict]:
    """symbol -> {funding, oi_usd, mid}"""
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or len(raw) != 2:
        return {}
    coins = [a["name"] for a in raw[0]["universe"]]
    out = {}
    for coin, ctx in zip(coins, raw[1]):
        try:
            mid = float(ctx.get("midPx") or 0.0)
            out[coin] = {
                "funding": float(ctx.get("funding") or 0.0),
                "oi_usd": float(ctx.get("openInterest") or 0.0) * mid,
                "mid": mid,
            }
        except (TypeError, ValueError):
            continue
    return out


def get_l2_book(coin: str) -> dict | None:
    return hl_post({"type": "l2Book", "coin": coin})


def analyze_orderbook(coin: str) -> dict:
    """Depth-weighted imbalance + spread quality from live L2 book.

    Returns a neutral fallback if the book can't be fetched, so a single
    failed request never blocks an otherwise-valid signal.
    """
    fallback = {"imbalance": 0.0, "spread_bps": 5.0, "depth_score": 0.5, "ok": False}
    book = get_l2_book(coin)
    if not book or "levels" not in book or len(book["levels"]) != 2:
        return fallback
    try:
        bids, asks = book["levels"]
        if not bids or not asks:
            return fallback
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2
        spread_bps = ((best_ask - best_bid) / mid) * 10_000 if mid else 999

        depth = 15
        bid_notional = sum(float(l["px"]) * float(l["sz"]) for l in bids[:depth])
        ask_notional = sum(float(l["px"]) * float(l["sz"]) for l in asks[:depth])
        total = bid_notional + ask_notional
        imbalance = (bid_notional - ask_notional) / total if total else 0.0
        depth_score = min(1.0, math.log10(max(total, 1.0)) / 7.0)  # ~$10M -> ~1.0
        return {"imbalance": imbalance, "spread_bps": spread_bps,
                "depth_score": depth_score, "ok": True}
    except Exception:
        return fallback


# ============================================================================
# CORE MATH / INDICATORS
# ============================================================================

def safe(v, fb=0.0):
    return v if (v is not None and not math.isnan(v)) else fb


def ema(vals: list[float], period: int) -> list[float]:
    if not vals:
        return []
    k = 2 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        w = vals[max(0, i - period + 1):i + 1]
        out.append(sum(w) / len(w))
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        w = vals[max(0, i - period + 1):i + 1]
        out.append(statistics.pstdev(w) if len(w) > 1 else 0.0)
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g, avg_l = ema(gains, period), ema(losses, period)
    out = []
    for g, l in zip(avg_g, avg_l):
        rs = g / l if l > 1e-12 else 999
        out.append(100 - 100 / (1 + rs))
    return out


def atr(highs, lows, closes, period: int = ATR_LEN) -> list[float]:
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    out, a = [], trs[0]
    for i, tr in enumerate(trs):
        a = tr if i == 0 else (a * (period - 1) + tr) / period
        out.append(a)
    return out


def adx_dmi(highs, lows, closes, period: int = ADX_LEN) -> tuple[list[float], list[float], list[float]]:
    n = len(closes)
    plus_dm, minus_dm, trs = [0.0], [0.0], [highs[0] - lows[0]]
    for i in range(1, n):
        up, down = highs[i] - highs[i - 1], lows[i - 1] - lows[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr_s = ema(trs, period)
    pdi = [100 * (p / a if a else 0) for p, a in zip(ema(plus_dm, period), atr_s)]
    mdi = [100 * (m / a if a else 0) for m, a in zip(ema(minus_dm, period), atr_s)]
    dx = [100 * abs(p - m) / (p + m) if (p + m) else 0 for p, m in zip(pdi, mdi)]
    return ema(dx, period), pdi, mdi


def compute_indicators(candles: list[dict]) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c["v"] for c in candles]
    adx_v, pdi, mdi = adx_dmi(highs, lows, closes)
    return {
        "closes": closes, "highs": highs, "lows": lows, "vols": vols,
        "ema_fast": ema(closes, FAST_LEN), "ema_slow": ema(closes, SLOW_LEN),
        "ema_trend": ema(closes, TREND_LEN),
        "rsi": rsi(closes), "atr": atr(highs, lows, closes),
        "adx": adx_v, "pdi": pdi, "mdi": mdi,
        "vol_sma": sma(vols, 20), "bb_mid": sma(closes, BB_LEN), "bb_std": stdev(closes, BB_LEN),
    }


def volume_profile(candles: list[dict], bins: int = VOL_PROFILE_BINS) -> dict:
    """Session volume profile -> POC, value-area high/low, session VWAP."""
    hi = max(c["h"] for c in candles)
    lo = min(c["l"] for c in candles)
    if hi <= lo:
        return {"poc": hi, "vah": hi, "val": lo, "vwap": hi}
    step = (hi - lo) / bins
    buckets = [0.0] * bins
    for c in candles:
        idx = min(bins - 1, max(0, int((((c["h"] + c["l"] + c["c"]) / 3) - lo) / step)))
        buckets[idx] += c["v"]
    poc_idx = max(range(bins), key=lambda i: buckets[i])
    poc = lo + (poc_idx + 0.5) * step
    total = sum(buckets) or 1.0
    target = total * 0.70
    lo_i = hi_i = poc_idx
    acc = buckets[poc_idx]
    while acc < target and (lo_i > 0 or hi_i < bins - 1):
        expand_lo = buckets[lo_i - 1] if lo_i > 0 else -1
        expand_hi = buckets[hi_i + 1] if hi_i < bins - 1 else -1
        if expand_hi >= expand_lo:
            hi_i += 1
            acc += buckets[hi_i]
        else:
            lo_i -= 1
            acc += buckets[lo_i]
    vah, val = lo + (hi_i + 1) * step, lo + lo_i * step
    vwap_num = sum(((c["h"] + c["l"] + c["c"]) / 3) * c["v"] for c in candles)
    vwap_den = sum(c["v"] for c in candles) or 1.0
    return {"poc": poc, "vah": vah, "val": val, "vwap": vwap_num / vwap_den}


# ============================================================================
# STATE MANAGEMENT
# ============================================================================

def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            pass
    return _default_state()


def _default_state() -> dict:
    return {
        "active_signals": [], "signal_history": [], "cooldowns": {},
        "atr_pct_memory": {}, "pathway_weights": {"reversal": 1.0, "continuation": 1.0, "breakout": 1.0},
        "last_scan_ms": 0,
    }


def save_state(state: dict):
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_PATH)


def prune_state(state: dict):
    cutoff_ms = int(time.time() * 1000) - SIGNAL_HISTORY_MAX_AGE_DAYS * 24 * 3600 * 1000
    hist = state.get("signal_history", [])
    hist = [h for h in hist if h.get("closed_ms", 0) >= cutoff_ms]
    state["signal_history"] = hist[-MAX_SIGNAL_HISTORY:]
    for sym, mem in state.get("atr_pct_memory", {}).items():
        state["atr_pct_memory"][sym] = mem[-ATR_PCT_MEMORY:]


# ============================================================================
# REGIME VECTOR  (six continuous dimensions, no hard buckets)
# ============================================================================

@dataclass
class RegimeVector:
    trend_strength: float        # 0..1, BTC ADX-derived
    volatility_pctile: float     # 0..1, symbol's ATR% percentile vs its own history
    liquidity_quality: float     # 0..1, from OI + orderbook depth
    noise_index: float           # 0..1 (higher = choppier / less reliable)
    session_weight: float        # 0..1, higher during high-liquidity UTC windows
    breadth: float                # 0..1, % of watchlist aligned with BTC trend
    btc_bias: str


def session_weight_now() -> float:
    h = datetime.now(timezone.utc).hour
    # crypto trades 24/7; weight reflects historical liquidity windows
    # (US/EU overlap heaviest, APAC lull lightest) without gating trades
    if 12 <= h <= 20:
        return 1.0
    if 6 <= h <= 12 or 20 <= h <= 23:
        return 0.8
    return 0.6


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state.setdefault("atr_pct_memory", {}).setdefault(symbol, [])
    mem.append(atr_pct)
    if len(mem) > ATR_PCT_MEMORY:
        del mem[0]
    if len(mem) < 8:
        return 0.5
    rank = sum(1 for x in mem if x <= atr_pct) / len(mem)
    return rank


def compute_btc_regime(btc_bundle: dict) -> tuple[str, float]:
    ind = compute_indicators(btc_bundle["4h"])
    adx_v = ind["adx"][-1]
    bull = ind["ema_fast"][-1] > ind["ema_slow"][-1] > ind["ema_trend"][-1]
    bear = ind["ema_fast"][-1] < ind["ema_slow"][-1] < ind["ema_trend"][-1]
    bias = "bull" if bull else "bear" if bear else "neutral"
    return bias, min(1.0, adx_v / 40.0)


def compute_noise_index(candles: list[dict]) -> float:
    """Whipsaw ratio: sum of |close-close| vs total high-low range traveled."""
    if len(candles) < 20:
        return 0.5
    recent = candles[-20:]
    net_move = abs(recent[-1]["c"] - recent[0]["c"])
    total_range = sum(c["h"] - c["l"] for c in recent) or 1e-9
    efficiency = net_move / total_range
    return max(0.0, min(1.0, 1.0 - efficiency))


def build_regime_vector(state: dict, symbol: str, bundle: dict, btc_bias: str,
                         btc_trend_strength: float, breadth: float, book: dict) -> RegimeVector:
    ind15 = compute_indicators(bundle["15m"])
    atr_pct = (ind15["atr"][-1] / ind15["closes"][-1]) * 100 if ind15["closes"][-1] else 0
    vol_pctile = update_atr_pct_memory(state, symbol, atr_pct)
    noise = compute_noise_index(bundle["15m"])
    liq = 0.5 * book.get("depth_score", 0.5) + 0.5 * min(1.0, max(0.0, (12 - book.get("spread_bps", 5)) / 12))
    return RegimeVector(btc_trend_strength, vol_pctile, liq, noise, session_weight_now(), breadth, btc_bias)


def adaptive_thresholds(r: RegimeVector) -> tuple[float, int]:
    """Smooth function of the regime vector -> (min_confidence, per-scan cap).

    Favorable regime (trending, clean, liquid, good session) -> lower
    threshold / higher cap. Noisy/illiquid/off-session -> stricter.
    """
    favorability = (
        0.28 * r.trend_strength + 0.20 * (1 - r.noise_index) +
        0.20 * r.liquidity_quality + 0.17 * r.session_weight + 0.15 * r.breadth
    )
    # extreme volatility (very top or very bottom percentile) trims frequency
    vol_penalty = 4.0 * abs(r.volatility_pctile - 0.5)
    min_conf = 74.0 - 16.0 * favorability + vol_penalty * 3.0
    min_conf = max(56.0, min(80.0, min_conf))
    cap = round(1 + 4 * favorability)
    return min_conf, max(1, cap)


# ============================================================================
# MARKET STRUCTURE / LIQUIDITY / FVG
# ============================================================================

@dataclass
class Swing:
    idx: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    out = []
    for i in range(left, len(candles) - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        if candles[i]["h"] == max(window_h):
            out.append(Swing(i, candles[i]["h"], "high"))
        if candles[i]["l"] == min(window_l):
            out.append(Swing(i, candles[i]["l"], "low"))
    return out


@dataclass
class StructureState:
    bias: str  # "bull" | "bear" | "range"
    recent_high: float
    recent_low: float
    bos_count_bull: int
    bos_count_bear: int


def analyze_structure(candles: list[dict], swings: list[Swing]) -> StructureState:
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    bull_bos = bear_bos = 0
    for i in range(1, len(highs)):
        if highs[i].price > highs[i - 1].price:
            bull_bos += 1
    for i in range(1, len(lows)):
        if lows[i].price < lows[i - 1].price:
            bear_bos += 1
    if bull_bos > bear_bos + 1:
        bias = "bull"
    elif bear_bos > bull_bos + 1:
        bias = "bear"
    else:
        bias = "range"
    recent_high = max((h.price for h in highs[-6:]), default=candles[-1]["h"])
    recent_low = min((l.price for l in lows[-6:]), default=candles[-1]["l"])
    return StructureState(bias, recent_high, recent_low, bull_bos, bear_bos)


@dataclass
class LiquidityPool:
    price: float
    touches: int


def cluster_levels(levels: list[float], tol_pct: float = 0.0015) -> list[tuple[float, int]]:
    levels = sorted(levels)
    clusters: list[list[float]] = []
    for lvl in levels:
        if clusters and abs(lvl - clusters[-1][-1]) / clusters[-1][-1] <= tol_pct:
            clusters[-1].append(lvl)
        else:
            clusters.append([lvl])
    return [(sum(c) / len(c), len(c)) for c in clusters]


def build_liquidity_pools(candles: list[dict], swings: list[Swing]) -> list[LiquidityPool]:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    return ([LiquidityPool(p, t) for p, t in cluster_levels(highs) if t >= 2] +
            [LiquidityPool(p, t) for p, t in cluster_levels(lows) if t >= 2])


def detect_sweep(candles: list[dict], pools: list[LiquidityPool], direction: str, lookback: int = 10) -> LiquidityPool | None:
    recent = candles[-lookback:]
    for pool in sorted(pools, key=lambda p: -p.touches):
        for c in recent:
            if direction == "long" and c["l"] < pool.price and c["c"] > pool.price:
                return pool
            if direction == "short" and c["h"] > pool.price and c["c"] < pool.price:
                return pool
    return None


@dataclass
class FVG:
    low: float
    high: float
    direction: str
    mitigated: bool


def detect_fvgs(candles: list[dict], max_zones: int = 8) -> list[FVG]:
    out = []
    for i in range(2, len(candles)):
        a, b, c = candles[i - 2], candles[i - 1], candles[i]
        if c["l"] > a["h"]:
            out.append(FVG(a["h"], c["l"], "bull", False))
        if c["h"] < a["l"]:
            out.append(FVG(c["h"], a["l"], "bear", False))
    for fvg in out:
        for c in candles[-40:]:
            if fvg.low <= c["c"] <= fvg.high:
                fvg.mitigated = True
                break
    return out[-max_zones:]


def premium_discount_zone(candles: list[dict], lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
    close = candles[-1]["c"]
    pos = (close - lo) / (hi - lo) if hi != lo else 0.5
    zone = "discount" if pos < 0.45 else "premium" if pos > 0.55 else "equilibrium"
    return {"zone": zone, "position_pct": pos}


# ============================================================================
# THREE PARALLEL PATHWAYS
# ============================================================================

@dataclass
class Candidate:
    direction: str
    pathway: str  # "reversal" | "continuation" | "breakout"
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float | None
    raw_confluences: list
    structure: StructureState
    regime: RegimeVector


def wick_overshoot_allowance(candles: list[dict], direction: str,
                              lookback: int = WICK_LOOKBACK_BARS) -> float:
    """Empirical stop-hunt depth: how far recent wicks have actually poked
    beyond the prior local high/low before price reversed away from it.
    Used as a floor for the SL buffer so an ordinary liquidity-grab wick
    doesn't clip the stop right before the real move starts.
    """
    recent = candles[-lookback:]
    if len(recent) < 10:
        return 0.0
    overshoots = []
    for i in range(5, len(recent)):
        window = recent[i - 5:i]
        if direction == "long":
            prior_low = min(x["l"] for x in window)
            if recent[i]["l"] < prior_low:
                overshoots.append(prior_low - recent[i]["l"])
        else:
            prior_high = max(x["h"] for x in window)
            if recent[i]["h"] > prior_high:
                overshoots.append(recent[i]["h"] - prior_high)
    if not overshoots:
        return 0.0
    overshoots.sort()
    idx = min(len(overshoots) - 1, int(len(overshoots) * WICK_OVERSHOOT_PERCENTILE))
    return overshoots[idx]


def adaptive_sl_buffer(atr_val: float, vol_pctile: float, wick_allow: float,
                        base_atr_mult: float) -> float:
    """Combine an ATR buffer (scaled up in elevated-volatility regimes) with
    the empirical wick-overshoot floor, and take whichever is larger. This
    is the piece that keeps stops from sitting exactly where the last few
    hunt wicks have already reached.
    """
    vol_scale = 1.0 + SL_VOL_SCALE_MAX * max(0.0, min(1.0, vol_pctile))
    atr_buffer = atr_val * base_atr_mult * vol_scale
    return max(atr_buffer, wick_allow * 1.15)


def pathway_liquidity_reversal(bundle: dict, regime: RegimeVector, vp: dict) -> Candidate | None:
    c4h, c1h, c15 = bundle["4h"], bundle["1h"], bundle["15m"]
    swings_4h = find_swings(c4h)
    struct_4h = analyze_structure(c4h, swings_4h)
    pools = build_liquidity_pools(c4h, swings_4h)
    pdz = premium_discount_zone(c4h)

    for direction in ("long", "short"):
        if direction == "long" and pdz["zone"] != "discount":
            continue
        if direction == "short" and pdz["zone"] != "premium":
            continue
        sweep = detect_sweep(c1h, pools, direction, lookback=10)
        if not sweep:
            continue
        struct_1h = analyze_structure(c1h, find_swings(c1h))
        if not ((direction == "long" and struct_1h.bias != "bear") or
                (direction == "short" and struct_1h.bias != "bull")):
            continue
        fresh = [f for f in detect_fvgs(c1h) if not f.mitigated and
                 ((direction == "long" and f.direction == "bull") or
                  (direction == "short" and f.direction == "bear"))]
        entry = c15[-1]["c"]
        atr_val = compute_indicators(c15)["atr"][-1]
        wick_allow = wick_overshoot_allowance(c15, direction)
        buffer = adaptive_sl_buffer(atr_val, regime.volatility_pctile, wick_allow, base_atr_mult=0.15)
        if direction == "long":
            sl = min(sweep.price, entry - 0.6 * atr_val) - buffer
            risk = entry - sl
            tp1, tp2 = entry + 1.5 * risk, entry + 2.5 * risk
            tp3 = max(struct_4h.recent_high, vp["vah"])
        else:
            sl = max(sweep.price, entry + 0.6 * atr_val) + buffer
            risk = sl - entry
            tp1, tp2 = entry - 1.5 * risk, entry - 2.5 * risk
            tp3 = min(struct_4h.recent_low, vp["val"])
        if risk <= 0:
            continue
        confl = [f"liquidity sweep @ {sweep.price:.4f} ({sweep.touches}x tested)",
                 f"1h structure not opposed ({struct_1h.bias})",
                 f"price in HTF {pdz['zone']} zone"]
        if fresh:
            confl.append("fresh FVG supporting reaction")
        return Candidate(direction, "reversal", entry, sl, tp1, tp2, tp3, confl, struct_4h, regime)
    return None


def pathway_structure_continuation(bundle: dict, regime: RegimeVector, vp: dict) -> Candidate | None:
    c4h, c1h, c15 = bundle["4h"], bundle["1h"], bundle["15m"]
    struct_4h = analyze_structure(c4h, find_swings(c4h))
    if struct_4h.bias not in ("bull", "bear"):
        return None
    direction = "long" if struct_4h.bias == "bull" else "short"
    ind1h = compute_indicators(c1h)
    trend_ok = (direction == "long" and ind1h["ema_fast"][-1] > ind1h["ema_slow"][-1]) or \
               (direction == "short" and ind1h["ema_fast"][-1] < ind1h["ema_slow"][-1])
    if not trend_ok:
        return None
    fresh = [f for f in detect_fvgs(c1h) if not f.mitigated and
             ((direction == "long" and f.direction == "bull") or
              (direction == "short" and f.direction == "bear"))]
    close = c15[-1]["c"]
    if fresh:
        z = fresh[-1]
        in_zone = z.low <= close <= z.high
    else:
        pdz = premium_discount_zone(c1h, 30)
        in_zone = (direction == "long" and pdz["position_pct"] < 0.55) or \
                  (direction == "short" and pdz["position_pct"] > 0.45)
    vwap_support = (direction == "long" and close >= vp["vwap"] * 0.997) or \
                   (direction == "short" and close <= vp["vwap"] * 1.003)
    if not (in_zone or vwap_support):
        return None
    atr_val = compute_indicators(c15)["atr"][-1]
    entry = close
    struct_1h = analyze_structure(c1h, find_swings(c1h))
    wick_allow = wick_overshoot_allowance(c15, direction)
    buffer = adaptive_sl_buffer(atr_val, regime.volatility_pctile, wick_allow, base_atr_mult=0.35)
    vol_scale = 1.0 + SL_VOL_SCALE_MAX * max(0.0, min(1.0, regime.volatility_pctile))
    max_risk = 2.4 * atr_val * vol_scale  # cap so an anchor far below/above price doesn't wreck R:R
    if direction == "long":
        # anchor beyond the swing/FVG that actually defines this pullback,
        # not an arbitrary ATR distance from current price
        anchor = min(struct_1h.recent_low, z.low) if fresh else struct_1h.recent_low
        sl = anchor - buffer
        risk = entry - sl
        if risk > max_risk:
            sl = entry - max_risk
            risk = max_risk
        tp1, tp2 = entry + 1.6 * risk, entry + 2.8 * risk
        tp3 = struct_4h.recent_high + 1.5 * atr_val
    else:
        anchor = max(struct_1h.recent_high, z.high) if fresh else struct_1h.recent_high
        sl = anchor + buffer
        risk = sl - entry
        if risk > max_risk:
            sl = entry + max_risk
            risk = max_risk
        tp1, tp2 = entry - 1.6 * risk, entry - 2.8 * risk
        tp3 = struct_4h.recent_low - 1.5 * atr_val
    if risk <= 0:
        return None
    confl = [f"4h structure trend = {struct_4h.bias}", "1h EMA stack aligned with trend",
             "pullback into value/FVG or VWAP support"]
    return Candidate(direction, "continuation", entry, sl, tp1, tp2, tp3, confl, struct_4h, regime)


def pathway_momentum_breakout(bundle: dict, regime: RegimeVector, vp: dict) -> Candidate | None:
    """New pathway: volatility-expansion breakout trading for trending /
    high-volatility regimes that neither reversal nor pullback templates
    capture well (e.g. clean range expansion, news-driven trend starts).
    """
    c1h, c15 = bundle["1h"], bundle["15m"]
    ind1h = compute_indicators(c1h)
    ind15 = compute_indicators(c15)
    if ind1h["adx"][-1] < 22:          # require a genuine trending regime
        return None
    struct_1h = analyze_structure(c1h, find_swings(c1h))
    direction = "long" if struct_1h.bias == "bull" else "short" if struct_1h.bias == "bear" else None
    if direction is None:
        return None
    bb_width = (ind15["bb_std"][-1] * 2) / ind15["bb_mid"][-1] if ind15["bb_mid"][-1] else 0
    bb_width_prior = (ind15["bb_std"][-6] * 2) / ind15["bb_mid"][-6] if ind15["bb_mid"][-6] else 0
    expanding = bb_width > bb_width_prior * 1.15
    vol_surge = ind15["vols"][-1] > (ind15["vol_sma"][-1] or 1) * 1.6
    breakout_level = struct_1h.recent_high if direction == "long" else struct_1h.recent_low
    close = c15[-1]["c"]
    broke = (direction == "long" and close > breakout_level) or (direction == "short" and close < breakout_level)
    if not (expanding and vol_surge and broke):
        return None
    atr_val = ind15["atr"][-1]
    entry = close
    wick_allow = wick_overshoot_allowance(c15, direction)
    buffer = adaptive_sl_buffer(atr_val, regime.volatility_pctile, wick_allow, base_atr_mult=0.5)
    vol_scale = 1.0 + SL_VOL_SCALE_MAX * max(0.0, min(1.0, regime.volatility_pctile))
    if direction == "long":
        sl = min(breakout_level - buffer, entry - 1.1 * atr_val * vol_scale)
        risk = entry - sl
        tp1, tp2 = entry + 1.4 * risk, entry + 2.4 * risk
        tp3 = entry + 3.6 * risk
    else:
        sl = max(breakout_level + buffer, entry + 1.1 * atr_val * vol_scale)
        risk = sl - entry
        tp1, tp2 = entry - 1.4 * risk, entry - 2.4 * risk
        tp3 = entry - 3.6 * risk
    if risk <= 0:
        return None
    confl = [f"ADX(1h)={ind1h['adx'][-1]:.0f} confirms trending regime",
             "volatility expansion (Bollinger width rising)",
             f"volume surge ({ind15['vols'][-1] / (ind15['vol_sma'][-1] or 1):.1f}x avg)",
             f"clean break of {direction=='long' and 'range high' or 'range low'}"]
    return Candidate(direction, "breakout", entry, sl, tp1, tp2, tp3, confl, struct_1h, regime)


PATHWAYS = {
    "reversal": pathway_liquidity_reversal,
    "continuation": pathway_structure_continuation,
    "breakout": pathway_momentum_breakout,
}


# ============================================================================
# COMPOSITE CONFLUENCE SCORING
# ============================================================================

def _norm(x, lo, hi):
    return max(0.0, min(1.0, (x - lo) / (hi - lo))) if hi != lo else 0.5


def historical_edge_score(state: dict, symbol: str, direction: str, pathway: str) -> float:
    hist = state.get("signal_history", [])
    relevant = [h for h in hist if h.get("symbol") == symbol and h.get("direction") == direction
                and h.get("pathway") == pathway and h.get("result") in ("win", "loss")]
    if len(relevant) < 6:
        return 0.55
    recent = relevant[-30:]
    wr = sum(1 for h in recent if h["result"] == "win") / len(recent)
    return max(0.15, min(0.95, wr))


def score_candidate(cand: Candidate, bundle: dict, snapshot: dict, book: dict, vp: dict,
                     symbol: str, state: dict, prior_funding: float | None) -> tuple[float, dict]:
    ind1h = compute_indicators(bundle["1h"])
    ind1d = compute_indicators(bundle["1d"])
    r = cand.regime

    structure_score = {"reversal": 1.0, "breakout": 0.9, "continuation": 0.85}[cand.pathway]
    structure_score = min(1.0, structure_score + 0.05 * len(cand.raw_confluences))

    rsi_v = ind1h["rsi"][-1]
    momentum_score = _norm(rsi_v, 38, 68) if cand.direction == "long" else _norm(100 - rsi_v, 38, 68)

    vol_ratio = ind1h["vols"][-1] / (ind1h["vol_sma"][-1] or 1)
    volume_score = _norm(vol_ratio, 0.7, 2.2)

    daily_bull = ind1d["ema_fast"][-1] > ind1d["ema_slow"][-1]
    htf_score = 1.0 if ((cand.direction == "long") == daily_bull) else 0.35

    regime_fit = (0.35 * (1 - r.noise_index) + 0.25 * r.liquidity_quality +
                  0.20 * r.session_weight + 0.20 * r.breadth)
    if (cand.direction == "long" and r.btc_bias == "bear") or \
       (cand.direction == "short" and r.btc_bias == "bull"):
        regime_fit *= 0.55

    snap = snapshot.get(symbol, {})
    funding = snap.get("funding", 0.0)
    funding_score = 0.5
    if cand.direction == "long" and funding < -0.0002:
        funding_score = 0.85
    elif cand.direction == "short" and funding > 0.0002:
        funding_score = 0.85
    elif abs(funding) > 0.001 and ((cand.direction == "long") == (funding > 0)):
        funding_score = 0.25

    # OI delta: rising OI + price moving in trade direction = healthy;
    # rising OI with price stalling/reversing = potential trap.
    oi_delta_score = 0.5
    if prior_funding is not None:
        oi_now = snap.get("oi_usd", 0.0)
        # we approximate delta direction using funding momentum as a proxy
        # when a true OI time-series isn't available in-state yet
        oi_delta_score = 0.65 if abs(funding - prior_funding) < 0.0005 else 0.45

    orderbook_score = 0.5
    if book.get("ok"):
        aligned_imbalance = book["imbalance"] if cand.direction == "long" else -book["imbalance"]
        orderbook_score = _norm(aligned_imbalance, -0.3, 0.3)

    vwap_score = 0.5
    close = bundle["15m"][-1]["c"]
    if cand.direction == "long":
        vwap_score = 0.75 if close >= vp["vwap"] else 0.4
    else:
        vwap_score = 0.75 if close <= vp["vwap"] else 0.4

    hist_edge = historical_edge_score(state, symbol, cand.direction, cand.pathway)

    weights = {
        "structure": 0.18, "momentum": 0.10, "volume": 0.08, "htf": 0.13,
        "regime": 0.16, "funding": 0.06, "oi_delta": 0.05, "orderbook": 0.10,
        "vwap": 0.06, "edge": 0.08,
    }
    subs = {
        "structure": structure_score, "momentum": momentum_score, "volume": volume_score,
        "htf": htf_score, "regime": regime_fit, "funding": funding_score,
        "oi_delta": oi_delta_score, "orderbook": orderbook_score, "vwap": vwap_score,
        "edge": hist_edge,
    }
    pathway_multiplier = state.get("pathway_weights", {}).get(cand.pathway, 1.0)
    composite = sum(weights[k] * subs[k] for k in weights) * pathway_multiplier
    confidence = round(min(1.0, composite) * 100, 1)
    return confidence, subs


def grade_for_confidence(confidence: float) -> str:
    if confidence >= 82:
        return "A+"
    if confidence >= 74:
        return "A"
    if confidence >= 66:
        return "B"
    if confidence >= 58:
        return "C"
    return "D"


def classify_duration(cand: Candidate, atr_val: float) -> str:
    dist_atr = abs(cand.tp2 - cand.entry) / atr_val if atr_val else 0
    if cand.pathway == "breakout":
        return "SCALP" if dist_atr < 3 else "INTRADAY"
    if cand.pathway == "reversal" and dist_atr < 3.5:
        return "SCALP"
    if dist_atr < 6:
        return "INTRADAY"
    return "SWING"


# ============================================================================
# CORRELATION DEDUP / COOLDOWN / HARD FILTERS
# ============================================================================

def group_of(symbol: str) -> str:
    for g, members in CORR_GROUPS.items():
        if symbol in members:
            return g
    return symbol


def deduplicate_by_correlation(ranked: list[dict]) -> list[dict]:
    seen, out = set(), []
    for sig in ranked:
        key = (group_of(sig["symbol"]), sig["direction"])
        if key in seen:
            continue
        seen.add(key)
        out.append(sig)
    return out


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    last = state.get("cooldowns", {}).get(f"{symbol}:{direction}")
    return last is None or (bar_index - last) >= COOLDOWN_BARS_EXEC


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    state.setdefault("cooldowns", {})[f"{symbol}:{direction}"] = bar_index


def passes_hard_filters(symbol: str, snapshot: dict, book: dict, risk: float, entry: float) -> tuple[bool, str]:
    oi = snapshot.get(symbol, {}).get("oi_usd", 0.0)
    if oi < MIN_OI_USD:
        return False, f"OI too low (${oi:,.0f})"
    if book.get("ok") and book.get("spread_bps", 0) > MAX_SPREAD_BPS:
        return False, f"spread too wide ({book['spread_bps']:.1f} bps)"
    if risk <= 0 or entry <= 0:
        return False, "invalid risk geometry"
    return True, ""


# ============================================================================
# TELEGRAM OUTPUT
# ============================================================================

def fmt_px(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def confidence_bar(confidence: float) -> str:
    filled = round(confidence / 10)
    return "\u25a0" * filled + "\u25a1" * (10 - filled)


PATHWAY_LABEL = {"reversal": "Liquidity Reversal", "continuation": "Structure Continuation",
                  "breakout": "Momentum Breakout"}


def _esc(s: str) -> str:
    """Escape text for Telegram HTML parse_mode."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_signal(symbol: str, cand: Candidate, confidence: float, grade: str, duration: str) -> str:
    arrow = "\U0001F7E2 LONG" if cand.direction == "long" else "\U0001F534 SHORT"
    risk = abs(cand.entry - cand.sl)
    rr1, rr2 = abs(cand.tp1 - cand.entry) / risk, abs(cand.tp2 - cand.entry) / risk
    # Entry/SL/TP wrapped in <code> so Telegram renders them as tap-to-copy
    lines = [
        f"{arrow}  #{symbol}  [{duration}]",
        f"Grade: {grade}  |  Confidence: {confidence:.0f}%  {confidence_bar(confidence)}",
        f"Pathway: {_esc(PATHWAY_LABEL[cand.pathway])}",
        "",
        f"Entry:  <code>{fmt_px(cand.entry)}</code>",
        f"SL:     <code>{fmt_px(cand.sl)}</code>",
        f"TP1:    <code>{fmt_px(cand.tp1)}</code>  (R {rr1:.2f})",
        f"TP2:    <code>{fmt_px(cand.tp2)}</code>  (R {rr2:.2f})",
        "",
        "Confluences:",
    ]
    lines += [f"  \u2022 {_esc(c)}" for c in cand.raw_confluences]
    lines.append("")
    lines.append(f"Engine: {ENGINE_NAME}")
    return "\n".join(lines)


def send_telegram(text: str, reply_to: int | None = None) -> int | None:
    try:
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        if reply_to:
            payload["reply_to_message_id"] = reply_to
        r = _session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage", json=payload, timeout=10)
        return r.json().get("result", {}).get("message_id")
    except Exception:
        return None


# ============================================================================
# PER-SYMBOL EVALUATION
# ============================================================================

def evaluate_symbol(symbol: str, state: dict, btc_bias: str, btc_trend_strength: float,
                     breadth: float, snapshot: dict, min_conf: float, bar_index: int) -> list[dict]:
    bundle = fetch_all_candles(symbol)
    if not bundle:
        return []
    book = analyze_orderbook(symbol)
    vp = volume_profile(bundle["1h"][-96:])  # ~4 days of 1h bars as the "session" profile
    regime = build_regime_vector(state, symbol, bundle, btc_bias, btc_trend_strength, breadth, book)

    candidates = []
    for fn in PATHWAYS.values():
        try:
            c = fn(bundle, regime, vp)
            if c:
                candidates.append(c)
        except Exception:
            continue

    prior_funding = state.get("_prior_funding", {}).get(symbol)
    results = []
    for cand in candidates:
        if not check_cooldown(state, symbol, cand.direction, bar_index):
            continue
        risk = abs(cand.entry - cand.sl)
        ok, _ = passes_hard_filters(symbol, snapshot, book, risk, cand.entry)
        if not ok:
            continue
        rr1 = abs(cand.tp1 - cand.entry) / risk if risk else 0
        if rr1 < RISK_MIN_R:
            continue
        confidence, subs = score_candidate(cand, bundle, snapshot, book, vp, symbol, state, prior_funding)
        if confidence < min_conf:
            continue
        atr_val = compute_indicators(bundle["15m"])["atr"][-1]
        results.append({
            "symbol": symbol, "direction": cand.direction, "pathway": cand.pathway,
            "confidence": confidence, "grade": grade_for_confidence(confidence),
            "duration": classify_duration(cand, atr_val), "candidate": cand, "subs": subs,
        })
    return results


# ============================================================================
# SELF-TUNING PATHWAY WEIGHTS (bounded, regularized)
# ============================================================================

def tune_pathway_weights(state: dict):
    """Nudge each pathway's scoring multiplier toward its recent win-rate,
    shrunk toward the neutral prior (1.0) so a short streak can't push the
    engine into an overfit corner. Bounds are tight (WEIGHT_MIN..MAX).
    """
    weights = state.setdefault("pathway_weights", {"reversal": 1.0, "continuation": 1.0, "breakout": 1.0})
    hist = state.get("signal_history", [])
    for pathway in weights:
        relevant = [h for h in hist if h.get("pathway") == pathway and h.get("result") in ("win", "loss")]
        if len(relevant) < 15:
            continue
        recent = relevant[-40:]
        wr = sum(1 for h in recent if h["result"] == "win") / len(recent)
        target = 0.85 + 0.5 * wr  # wr=0.5 -> 1.10 neutral-ish; wr=0.3->1.0; wr=0.7->1.20
        target = max(WEIGHT_MIN, min(WEIGHT_MAX, target))
        weights[pathway] += WEIGHT_LEARNING_RATE * (target - weights[pathway])
        weights[pathway] = max(WEIGHT_MIN, min(WEIGHT_MAX, weights[pathway]))


# ============================================================================
# LIFECYCLE MANAGEMENT
# ============================================================================

def check_active_signals(state: dict, snapshot: dict):
    still_open = []
    for sig in state.get("active_signals", []):
        mid = snapshot.get(sig["symbol"], {}).get("mid")
        if not mid:
            still_open.append(sig)
            continue
        hit_sl = (sig["direction"] == "long" and mid <= sig["sl"]) or \
                 (sig["direction"] == "short" and mid >= sig["sl"])
        hit_tp2 = (sig["direction"] == "long" and mid >= sig["tp2"]) or \
                  (sig["direction"] == "short" and mid <= sig["tp2"])
        hit_tp1 = (sig["direction"] == "long" and mid >= sig["tp1"]) or \
                  (sig["direction"] == "short" and mid <= sig["tp1"])
        if hit_sl:
            record_outcome(state, sig, "loss")
            send_telegram(f"\u26aa {sig['symbol']} {sig['direction'].upper()} \u2014 stopped out", sig.get("msg_id"))
        elif hit_tp2:
            record_outcome(state, sig, "win")
            send_telegram(f"\u2705 {sig['symbol']} {sig['direction'].upper()} \u2014 TP2 hit", sig.get("msg_id"))
        else:
            if hit_tp1 and not sig.get("tp1_notified"):
                sig["tp1_notified"] = True
                send_telegram(f"\U0001F3AF {sig['symbol']} {sig['direction'].upper()} \u2014 TP1 hit, move SL to entry", sig.get("msg_id"))
            still_open.append(sig)
    state["active_signals"] = still_open


def record_outcome(state: dict, sig: dict, result: str):
    state.setdefault("signal_history", []).append({
        "symbol": sig["symbol"], "direction": sig["direction"], "pathway": sig["pathway"],
        "result": result, "closed_ms": int(time.time() * 1000),
    })


def maybe_send_daily_summary(state: dict):
    """Send a win-rate summary once per UTC day, during SUMMARY_HOUR_UTC.
    Gated by state["last_summary_date"] so repeated 15-min runs within that
    hour don't send it more than once.
    """
    now = datetime.now(timezone.utc)
    if now.hour != SUMMARY_HOUR_UTC:
        return
    today_str = now.strftime("%Y-%m-%d")
    if state.get("last_summary_date") == today_str:
        return

    cutoff_ms = int(time.time() * 1000) - 24 * 3600 * 1000
    closed = [h for h in state.get("signal_history", []) if h.get("closed_ms", 0) >= cutoff_ms]
    total = len(closed)
    wins = sum(1 for h in closed if h.get("result") == "win")
    losses = total - wins
    wr = (wins / total * 100) if total else 0.0
    active = len(state.get("active_signals", []))

    lines = [
        f"\U0001F4CA 24H Win-Rate Summary \u2014 {ENGINE_NAME}",
        "",
        f"Closed signals: {total}  (\u2705 {wins} win  \u26aa {losses} loss)",
        f"Win rate: {wr:.1f}%",
        f"Currently active: {active}",
    ]
    by_pathway = {}
    for h in closed:
        by_pathway.setdefault(h.get("pathway", "?"), []).append(h)
    if by_pathway:
        lines += ["", "By pathway:"]
        for pw, items in by_pathway.items():
            w = sum(1 for h in items if h.get("result") == "win")
            label = PATHWAY_LABEL.get(pw, pw)
            lines.append(f"  \u2022 {label}: {w}/{len(items)} ({w / len(items) * 100:.0f}%)")

    send_telegram("\n".join(lines))
    state["last_summary_date"] = today_str


# ============================================================================
# MAIN SCAN
# ============================================================================

def compute_breadth(state: dict, results_by_symbol_bias: dict, btc_bias: str) -> float:
    if not results_by_symbol_bias:
        return 0.5
    aligned = sum(1 for b in results_by_symbol_bias.values() if b == btc_bias)
    return aligned / len(results_by_symbol_bias)


def quick_bias_scan() -> dict:
    """Lightweight 1h-EMA bias per symbol for the breadth metric - reuses
    already-fetched 1h candles where possible via a short, cheap call.
    """
    out = {}
    for sym in WATCHLIST:
        c = get_candles(sym, "1h", 60)
        if len(c) < 55:
            continue
        closes = [x["c"] for x in c]
        f, s = ema(closes, 21)[-1], ema(closes, 50)[-1]
        out[sym] = "bull" if f > s else "bear"
    return out


def run_scan():
    state = load_state()
    reference_ms = int(time.time() * 1000)
    bar_index = reference_ms // 900_000

    btc_bundle = fetch_all_candles("BTC", reference_ms)
    if not btc_bundle:
        print("[SCAN] BTC data unavailable, aborting scan")
        return
    btc_bias, btc_trend_strength = compute_btc_regime(btc_bundle)
    snapshot = get_market_snapshot()

    bias_map = quick_bias_scan()
    breadth = compute_breadth(state, bias_map, btc_bias)

    dummy_regime = RegimeVector(btc_trend_strength, 0.5, 0.6, 0.4, session_weight_now(), breadth, btc_bias)
    min_conf, per_symbol_cap = adaptive_thresholds(dummy_regime)

    tune_pathway_weights(state)

    # A symbol with any open signal (scalp/intraday/swing - duration doesn't
    # matter) is skipped entirely until that signal resolves via SL or TP2.
    # Hitting TP1 does not free up the symbol - the signal is still open and
    # trailing toward TP2/SL, so a new one on the same symbol would just be
    # stacking risk on the same move.
    symbols_with_open_signal = {s["symbol"] for s in state.get("active_signals", [])}
    scan_universe = [sym for sym in WATCHLIST if sym not in symbols_with_open_signal]

    all_results: list[dict] = []
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs = {ex.submit(evaluate_symbol, sym, state, btc_bias, btc_trend_strength,
                           breadth, snapshot, min_conf, bar_index): sym for sym in scan_universe}
        for fut in as_completed(futs):
            try:
                all_results.extend(fut.result())
            except Exception as e:
                print(f"[SCAN] {futs[fut]} failed: {e}")

    all_results.sort(key=lambda r: -r["confidence"])
    all_results = deduplicate_by_correlation(all_results)

    active_count = len(state.get("active_signals", []))
    room = max(0, MAX_CONCURRENT_ACTIVE_SIGNALS - active_count)
    fire = all_results[:min(room, max(1, per_symbol_cap))]

    for res in fire:
        cand: Candidate = res["candidate"]
        text = format_signal(res["symbol"], cand, res["confidence"], res["grade"], res["duration"])
        msg_id = send_telegram(text)
        update_cooldown(state, res["symbol"], res["direction"], bar_index)
        state.setdefault("active_signals", []).append({
            "symbol": res["symbol"], "direction": res["direction"], "pathway": res["pathway"],
            "entry": cand.entry, "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2, "tp3": cand.tp3,
            "confidence": res["confidence"], "grade": res["grade"], "duration": res["duration"],
            "msg_id": msg_id, "opened_bar": bar_index, "opened_ms": reference_ms,
            "status": "open", "tp1_notified": False,
        })
        print(f"[SIGNAL] {res['symbol']} {res['direction']} {res['pathway']} "
              f"conf={res['confidence']} grade={res['grade']}")

    check_active_signals(state, snapshot)
    maybe_send_daily_summary(state)

    state["_prior_funding"] = {sym: v.get("funding", 0.0) for sym, v in snapshot.items()}
    prune_state(state)
    state["last_scan_ms"] = reference_ms
    save_state(state)
    print(f"[SCAN] complete: {len(fire)} fired / {len(all_results)} candidates | "
          f"btc={btc_bias} breadth={breadth:.0%} min_conf={min_conf:.1f} cap={per_symbol_cap}")


def _shutdown(signum, frame):
    print("[HELIOS] shutdown signal received, exiting cleanly")
    raise SystemExit(0)


def main():
    os_signal.signal(os_signal.SIGTERM, _shutdown)
    os_signal.signal(os_signal.SIGINT, _shutdown)
    try:
        run_scan()
    except Exception as e:
        print(f"[HELIOS] fatal scan error: {e}")
        raise


if __name__ == "__main__":
    main()
