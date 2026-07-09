#!/usr/bin/env python3
"""
AETHER ENGINE v1.0.0
====================
Adaptive Ensemble Trading & Heuristic Engine for Risk Optimization

Institutional-grade multi-engine ensemble for Hyperliquid perpetuals.
Completely original architecture inspired by (not merged from) Axis, Kairos,
and Meridian design lessons.

Architecture
------------
  1. Market Intelligence Layer  — regime, breadth, volatility, session, BTC bias
  2. Specialized Engines (6)    — each emits independent trade candidates
       • SMC Liquidity Sweep
       • Order Block Continuity
       • Fair Value Gap Rebalance
       • Trend Continuation
       • Volatility Breakout
       • Mean Reversion Range
  3. Central Decision Engine    — regime-weighted scoring, EV, correlation dedup,
                                  adaptive frequency governor, multi-engine confluence
  4. Continuous Learning        — per-engine stats, confidence calibration,
                                  gradual weight drift (no overfitting)
  5. Execution & Lifecycle      — structure SL/TP, Telegram, TP1→BE, daily summary

Workflow: scan-per-run · state.json · cron every 15m · Hyperliquid info API
"""

from __future__ import annotations

import collections
import json
import math
import os
import signal
import statistics
import threading
import time
import logging
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

# ============================================================================
# IDENTITY
# ============================================================================

ENGINE_NAME = "AETHER"
__version__ = "1.0.0"

# ============================================================================
# CONFIGURATION
# ============================================================================

HL_API_URL = "https://api.hyperliquid.xyz/info"
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

STATE_PATH = os.environ.get("AETHER_STATE_PATH", "state.json")
LOG_PATH = os.environ.get("AETHER_LOG_PATH", "aether_engine.log")
CANDLE_CACHE_PATH = os.environ.get("AETHER_CANDLE_CACHE_PATH", "candle_cache.json")
CANDLE_DELTA_OVERLAP_BARS = 3

WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# Adaptive timeframe combos selected per-symbol from regime
COMBOS = {
    "scalp":    {"bias": "1h", "struct": "15m", "exec": "5m",  "hold_hint": "0.5-4h"},
    "intraday": {"bias": "4h", "struct": "1h",  "exec": "15m", "hold_hint": "4-24h"},
    "swing":    {"bias": "1d", "struct": "4h",  "exec": "1h",  "hold_hint": "1-5d"},
}

TF_MS = {
    "5m": 5 * 60_000, "15m": 15 * 60_000, "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000, "1d": 24 * 60 * 60_000,
}
CANDLE_COUNT = {"5m": 300, "15m": 300, "1h": 300, "4h": 240, "1d": 180}

# Indicator lengths
ATR_LEN = 14
RSI_LEN = 14
ADX_LEN = 14
EMA_FAST, EMA_SLOW, EMA_TREND = 20, 50, 200
BB_LEN, BB_MULT = 20, 2.0
VOL_PROFILE_BINS = 24
VOL_PROFILE_LOOKBACK = 96

# Specialized engines
ENGINES = (
    "smc_liquidity",
    "order_block",
    "fvg_rebalance",
    "trend_continuation",
    "volatility_breakout",
    "mean_reversion",
)

# Adaptive frequency governor → target 5–10 quality signals / day
TARGET_SIGNALS_MIN = 5
TARGET_SIGNALS_MAX = 10
GOVERNOR_STEP = 1.5
GOVERNOR_FLOOR = 52.0
GOVERNOR_CEIL = 86.0
GOVERNOR_MIN_INTERVAL_S = 3600

# Learning
ENGINE_WEIGHT_LR = 0.035
ENGINE_WEIGHT_MIN, ENGINE_WEIGHT_MAX = 0.70, 1.35
MIN_SAMPLES_FOR_LEARN = 12
CALIBRATION_LR = 0.02

# Risk / quality floors
MIN_OI_USD = 2_500_000
MIN_ATR_PCT = 0.0012
MAX_ATR_PCT = 0.10
MIN_RR = 1.5
MAX_CONCURRENT_PER_SYMBOL = 1
MAX_CONCURRENT_SAME_DIRECTION = 7
MAX_SIGNALS_PER_SCAN = 3
COOLDOWN_BARS = 5
DEDUP_PRICE_TOL_PCT = 0.0025
DEDUP_TIME_WINDOW_HOURS = 36
POI_ATR_MULT = {"scalp": 0.55, "intraday": 0.70, "swing": 0.90}
POI_MAX_PCT_OF_PRICE = 0.009
MAX_ENTRY_DRIFT_R = 0.55

# Correlation
CORR_LOOKBACK = 60
CORR_CLUSTER_THRESHOLD = 0.72

# Network
HL_WEIGHT_BUDGET_PER_MINUTE = 1000.0
HL_DEFAULT_INFO_WEIGHT = 20
HL_ENDPOINT_BASE_WEIGHT = {
    "l2Book": 2, "allMids": 2, "clearinghouseState": 2, "orderStatus": 2,
    "spotClearinghouseState": 2, "exchangeStatus": 2, "userRole": 60,
}
FETCH_THREAD_WORKERS = 6

# Telegram reactions (official set only)
REACT_TP1 = "🔥"
REACT_TP2 = "👍"
REACT_SL = "👎"
REACT_BE = "🤝"
REACT_CANCEL = "🙈"

DAILY_SUMMARY_UTC_HOUR = 8

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger("aether")


def _handle_shutdown(sig_num, frame):
    log.warning("Shutdown signal %s", sig_num)
    raise SystemExit(0)


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)

# ============================================================================
# HYPERLIQUID API
# ============================================================================


def hl_coin(symbol: str) -> str:
    return symbol.upper().replace("USDT", "")


class _WeightRateLimiter:
    def __init__(self, budget_per_minute: float):
        self.budget = budget_per_minute
        self.window_s = 60.0
        self.lock = threading.Lock()
        self.events: collections.deque[tuple[float, float]] = collections.deque()

    def wait(self, weight: float):
        while True:
            with self.lock:
                now = time.monotonic()
                cutoff = now - self.window_s
                while self.events and self.events[0][0] < cutoff:
                    self.events.popleft()
                used = sum(w for _, w in self.events)
                if used + weight <= self.budget:
                    self.events.append((now, weight))
                    return
                sleep_for = max(0.05, self.events[0][0] + self.window_s - now)
            time.sleep(min(sleep_for, 2.0))


_rate_limiter = _WeightRateLimiter(HL_WEIGHT_BUDGET_PER_MINUTE)


def _request_weight(payload: dict) -> float:
    req_type = payload.get("type", "")
    if req_type == "candleSnapshot":
        req = payload.get("req", {})
        interval = req.get("interval")
        start_ms, end_ms = req.get("startTime"), req.get("endTime")
        n_bars = 60
        if interval in TF_MS and start_ms is not None and end_ms is not None:
            step = TF_MS[interval]
            n_bars = max(1, math.ceil((end_ms - start_ms) / step))
        return HL_DEFAULT_INFO_WEIGHT * math.ceil(n_bars / 60)
    return HL_ENDPOINT_BASE_WEIGHT.get(req_type, HL_DEFAULT_INFO_WEIGHT)


def hl_post(payload: dict, retries: int = 4, timeout: int = 12):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        HL_API_URL, data=body, headers={"Content-Type": "application/json"}
    )
    weight = _request_weight(payload)
    for attempt in range(retries):
        _rate_limiter.wait(weight)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait_s = float(e.headers.get("Retry-After") or 10.0)
                log.warning("429 type=%s backoff %.1fs", payload.get("type"), wait_s)
                time.sleep(wait_s)
            else:
                log.warning("HTTP %s type=%s: %s", e.code, payload.get("type"), e)
                time.sleep(0.8 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            log.warning("hl_post fail type=%s: %s", payload.get("type"), e)
            time.sleep(0.8 * (attempt + 1))
    log.error("hl_post exhausted type=%s", payload.get("type"))
    return None


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = TF_MS[interval]
    return (reference_ms // step) * step


def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]


def _request_candles(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[dict]:
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": hl_coin(symbol),
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }
    raw = hl_post(payload)
    if not raw:
        return []
    return [
        {"t": c["t"], "o": float(c["o"]), "h": float(c["h"]),
         "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])}
        for c in raw
    ]


def get_candles(symbol: str, interval: str, n: int, reference_ms: int | None = None,
                cache_entry: list[dict] | None = None) -> list[dict]:
    reference_ms = reference_ms or int(time.time() * 1000)
    if cache_entry:
        step = TF_MS[interval]
        last_cached_t = cache_entry[-1]["t"]
        start_ms = last_cached_t - step * CANDLE_DELTA_OVERLAP_BARS
        new_raw = _request_candles(symbol, interval, start_ms, reference_ms)
        if new_raw:
            merged = {c["t"]: c for c in cache_entry}
            for c in new_raw:
                merged[c["t"]] = c
            candles = [merged[t] for t in sorted(merged.keys())]
        else:
            candles = cache_entry
        candles = filter_closed_candles(candles, interval, reference_ms)
        return candles[-n:]
    lookback_ms = n * TF_MS[interval] * 2 + TF_MS[interval] * 5
    raw = _request_candles(symbol, interval, reference_ms - lookback_ms, reference_ms)
    candles = filter_closed_candles(raw, interval, reference_ms)
    return candles[-n:]


def fetch_all_candles(symbol: str, candle_cache: dict | None = None,
                      reference_ms: int | None = None) -> dict[str, list[dict]] | None:
    bundle = {}
    sym_cache = (candle_cache or {}).get(symbol, {})
    for tf in ("5m", "15m", "1h", "4h", "1d"):
        cache_entry = sym_cache.get(tf)
        candles = get_candles(symbol, tf, CANDLE_COUNT[tf], reference_ms, cache_entry)
        if len(candles) < 60:
            log.info("Insufficient %s candles for %s (%d)", tf, symbol, len(candles))
            return None
        bundle[tf] = candles
        if candle_cache is not None:
            candle_cache.setdefault(symbol, {})[tf] = candles
    return bundle


def get_meta_and_ctx() -> tuple[list[str], list[dict]] | None:
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or len(raw) < 2:
        return None
    universe = [a["name"] for a in raw[0]["universe"]]
    return universe, raw[1]


def get_market_snapshot() -> dict[str, dict]:
    out = {}
    got = get_meta_and_ctx()
    if not got:
        return out
    universe, ctxs = got
    for i, name in enumerate(universe):
        if name not in WATCHLIST and name != "BTC":
            continue
        try:
            ctx = ctxs[i]
            mark = float(ctx.get("markPx", 0) or 0)
            funding = float(ctx.get("funding", 0) or 0)
            oi_coins = float(ctx.get("openInterest", 0) or 0)
            out[name] = {"mark": mark, "funding": funding, "oi_usd": oi_coins * mark}
        except (KeyError, ValueError, TypeError, IndexError):
            continue
    return out


def get_l2_book(coin: str) -> dict | None:
    return hl_post({"type": "l2Book", "coin": hl_coin(coin)})


def analyze_orderbook(coin: str) -> dict:
    book = get_l2_book(coin)
    if not book or "levels" not in book:
        return {"imbalance": 0.0, "spread_bps": None}
    try:
        bids, asks = book["levels"][0], book["levels"][1]
        depth = 15
        bid_sz = sum(float(x["sz"]) for x in bids[:depth])
        ask_sz = sum(float(x["sz"]) for x in asks[:depth])
        total = bid_sz + ask_sz
        imbalance = (bid_sz - ask_sz) / total if total > 0 else 0.0
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2
        spread_bps = ((best_ask - best_bid) / mid) * 10_000 if mid else None
        return {"imbalance": imbalance, "spread_bps": spread_bps}
    except (KeyError, IndexError, ValueError, TypeError):
        return {"imbalance": 0.0, "spread_bps": None}

# ============================================================================
# MATH / INDICATORS
# ============================================================================


def safe(v, fb=0.0):
    try:
        if v is None or math.isnan(v) or math.isinf(v):
            return fb
        return v
    except TypeError:
        return fb


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
        lo = max(0, i - period + 1)
        window = vals[lo:i + 1]
        out.append(sum(window) / len(window))
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        lo = max(0, i - period + 1)
        window = vals[lo:i + 1]
        out.append(statistics.pstdev(window) if len(window) > 1 else 0.0)
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    out = [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period
    if avg_loss < 1e-12:
        out[period] = 100.0
    else:
        out[period] = 100 - (100 / (1 + avg_gain / avg_loss))
    for i in range(period + 1, len(closes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss > 1e-12 else 999.0
        out[i] = 100 - (100 / (1 + rs))
    return out


def atr(highs, lows, closes, period: int = ATR_LEN) -> list[float]:
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
    out, a = [], trs[0]
    for i, tr in enumerate(trs):
        a = tr if i == 0 else (a * (period - 1) + tr) / period
        out.append(a)
    return out


def adx_dmi(highs, lows, closes, period: int = ADX_LEN):
    n = len(closes)
    plus_dm, minus_dm, trs = [0.0], [0.0], [highs[0] - lows[0]]
    for i in range(1, n):
        up, down = highs[i] - highs[i - 1], lows[i - 1] - lows[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))

    def wilder(series):
        out, s = [], series[0]
        for i, v in enumerate(series):
            s = v if i == 0 else s - (s / period) + v
            out.append(s)
        return out

    atr_w = wilder(trs)
    pdm_w = wilder(plus_dm)
    mdm_w = wilder(minus_dm)
    plus_di = [100 * safe(pdm_w[i] / atr_w[i], 0) if atr_w[i] else 0.0 for i in range(n)]
    minus_di = [100 * safe(mdm_w[i] / atr_w[i], 0) if atr_w[i] else 0.0 for i in range(n)]
    dx = [100 * safe(abs(plus_di[i] - minus_di[i]) / (plus_di[i] + minus_di[i]), 0)
          if (plus_di[i] + minus_di[i]) else 0.0 for i in range(n)]
    return ema(dx, period), plus_di, minus_di


def bollinger_width_pct(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT) -> list[float]:
    mids = sma(closes, period)
    sds = stdev(closes, period)
    return [safe((2 * mult * sds[i]) / mids[i], 0) if mids[i] else 0.0 for i in range(len(closes))]


def detect_rsi_divergence(closes, rsi_values, highs, lows, lookback: int = 20) -> dict:
    min_len = lookback + 2
    if min(len(closes), len(rsi_values), len(highs), len(lows)) < min_len:
        return {"type": None, "strength": 0}
    rh = highs[-min_len:]
    rl = lows[-min_len:]
    rr = rsi_values[-min_len:]
    ph, pl, rsh, rsl = [], [], [], []
    for i in range(1, len(rh) - 1):
        if rh[i] > rh[i - 1] and rh[i] > rh[i + 1]:
            ph.append(rh[i]); rsh.append(rr[i])
        if rl[i] < rl[i - 1] and rl[i] < rl[i + 1]:
            pl.append(rl[i]); rsl.append(rr[i])
    if len(ph) >= 2 and ph[-1] > ph[-2] and rsh[-1] < rsh[-2]:
        return {"type": "bearish", "strength": 1}
    if len(pl) >= 2 and pl[-1] < pl[-2] and rsl[-1] > rsl[-2]:
        return {"type": "bullish", "strength": 1}
    return {"type": None, "strength": 0}


def compute_indicators(candles: list[dict]) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c["v"] for c in candles]
    adx_v, plus_di, minus_di = adx_dmi(highs, lows, closes)
    rsi_vals = rsi(closes)
    return {
        "closes": closes, "highs": highs, "lows": lows, "vols": vols,
        "ema_fast": ema(closes, EMA_FAST), "ema_slow": ema(closes, EMA_SLOW),
        "ema_trend": ema(closes, EMA_TREND),
        "rsi": rsi_vals, "atr": atr(highs, lows, closes),
        "adx": adx_v, "plus_di": plus_di, "minus_di": minus_di,
        "bb_width": bollinger_width_pct(closes),
        "vol_sma20": sma(vols, 20),
        "rsi_divergence": detect_rsi_divergence(closes, rsi_vals, highs, lows),
    }


def volume_profile(candles: list[dict], bins: int = VOL_PROFILE_BINS) -> dict:
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
# STATE
# ============================================================================


def _default_state() -> dict:
    return {
        "active_signals": [],
        "signal_history": [],
        "cooldowns": {},
        "atr_pct_memory": {},
        "governor": {"threshold": 64.0, "last_adjust_ts": 0, "daily_count_ema": 6.5},
        "engine_weights": {e: 1.0 for e in ENGINES},
        "engine_stats": {e: {"wins": 0, "losses": 0, "r_sum": 0.0, "n": 0,
                             "mfe_sum": 0.0, "mae_sum": 0.0, "hold_sum": 0.0,
                             "conf_sum": 0.0, "conf_correct": 0} for e in ENGINES},
        "regime_stats": {},
        "confidence_calibration": 1.0,
        "learning_log": [],
        "last_summary_date": "",
        "last_summary_ts": 0,
        "meta": {"version": __version__, "created": int(time.time()), "engine": ENGINE_NAME},
    }


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return _default_state()
    try:
        with open(STATE_PATH, "r") as f:
            state = json.load(f)
        base = _default_state()
        for k, v in base.items():
            state.setdefault(k, v if not isinstance(v, (dict, list)) else type(v)())
        for e in ENGINES:
            state["engine_weights"].setdefault(e, 1.0)
            state["engine_stats"].setdefault(e, base["engine_stats"][e])
        return state
    except (json.JSONDecodeError, OSError) as e:
        log.error("State load failed (%s), fresh start", e)
        return _default_state()


def save_state(state: dict):
    tmp = STATE_PATH + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_PATH)
    except OSError as e:
        log.error("State save failed: %s", e)


def load_candle_cache() -> dict:
    if not os.path.exists(CANDLE_CACHE_PATH):
        return {}
    try:
        with open(CANDLE_CACHE_PATH, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_candle_cache(candle_cache: dict):
    tmp = CANDLE_CACHE_PATH + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(candle_cache, f)
        os.replace(tmp, CANDLE_CACHE_PATH)
    except OSError as e:
        log.error("Candle cache save failed: %s", e)


def prune_state(state: dict, max_signals: int = 900, max_days: int = 28):
    cutoff = int(time.time()) - max_days * 86400
    state["signal_history"] = [
        s for s in state["signal_history"] if s.get("ts", 0) >= cutoff
    ][-max_signals:]
    for sym in list(state["atr_pct_memory"].keys()):
        state["atr_pct_memory"][sym] = state["atr_pct_memory"][sym][-220:]
    state["learning_log"] = state.get("learning_log", [])[-200:]

# ============================================================================
# REGIME INTELLIGENCE
# ============================================================================


@dataclass
class RegimeVector:
    btc_bias: str
    btc_strength: float
    vol_pctile: float
    adx: float
    session_weight: float
    noise_index: float
    breadth: float
    regime_label: str  # trend_bull | trend_bear | range | expansion | reversal | chop

    def favorability(self) -> float:
        trend = min(self.adx / 35.0, 1.0)
        noise_pen = max(0.0, 1.0 - self.noise_index)
        return round(
            0.36 * trend + 0.26 * noise_pen + 0.22 * self.session_weight + 0.16 * self.breadth, 4
        )


def session_weight_now() -> float:
    hour = time.gmtime().tm_hour
    if 13 <= hour <= 21:
        return 1.0
    if 0 <= hour <= 5:
        return 0.72
    return 0.88


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state["atr_pct_memory"].setdefault(symbol, [])
    mem.append(atr_pct)
    state["atr_pct_memory"][symbol] = mem[-220:]
    if len(mem) < 10:
        return 0.5
    sorted_mem = sorted(mem)
    rank = sum(1 for x in sorted_mem if x <= atr_pct)
    return rank / len(sorted_mem)


def compute_noise_index(candles: list[dict], lookback: int = 30) -> float:
    window = candles[-lookback:]
    if len(window) < 5:
        return 0.5
    net = abs(window[-1]["c"] - window[0]["c"])
    path = sum(abs(window[i]["c"] - window[i - 1]["c"]) for i in range(1, len(window)))
    efficiency = safe(net / path, 0.5) if path else 0.5
    return round(1.0 - min(efficiency, 1.0), 4)


def compute_btc_regime(btc_bundle: dict) -> tuple[str, float]:
    ind = compute_indicators(btc_bundle["4h"])
    price = ind["closes"][-1]
    ef, es, et = ind["ema_fast"][-1], ind["ema_slow"][-1], ind["ema_trend"][-1]
    adx_v = ind["adx"][-1]
    if price > ef > es > et:
        return "bullish", safe(adx_v, 0.0)
    if price < ef < es < et:
        return "bearish", safe(adx_v, 0.0)
    return "neutral", safe(adx_v, 0.0)


def symbol_bias_from_bundle(bundle: dict) -> str | None:
    candles = bundle.get("1h")
    if not candles or len(candles) < EMA_SLOW + 5:
        return None
    closes = [c["c"] for c in candles]
    fast, slow = ema(closes, EMA_FAST)[-1], ema(closes, EMA_SLOW)[-1]
    return "bullish" if fast > slow else "bearish"


def compute_breadth(bundles: dict[str, dict], btc_bias: str) -> float:
    if btc_bias not in ("bullish", "bearish") or not bundles:
        return 0.5
    biases = [b for b in (symbol_bias_from_bundle(b) for b in bundles.values()) if b]
    if not biases:
        return 0.5
    return sum(1 for b in biases if b == btc_bias) / len(biases)


def classify_regime(btc_bias: str, adx: float, vol_pctile: float, noise: float) -> str:
    if noise > 0.72 and adx < 18:
        return "chop"
    if vol_pctile > 0.75 and adx > 22:
        return "expansion"
    if adx >= 25:
        if btc_bias == "bullish":
            return "trend_bull"
        if btc_bias == "bearish":
            return "trend_bear"
    if adx < 18 and vol_pctile < 0.45:
        return "range"
    if noise > 0.55 and adx < 22:
        return "reversal"
    return "range" if adx < 20 else ("trend_bull" if btc_bias == "bullish" else
                                     "trend_bear" if btc_bias == "bearish" else "range")


def build_regime_vector(state, symbol, bundle, btc_bias, btc_strength, breadth, combo_key) -> RegimeVector:
    combo = COMBOS[combo_key]
    ind = compute_indicators(bundle[combo["bias"]])
    atr_pct = safe(ind["atr"][-1] / ind["closes"][-1], 0.01)
    vol_pctile = update_atr_pct_memory(state, symbol, atr_pct)
    noise = compute_noise_index(bundle[combo["struct"]])
    adx_v = safe(ind["adx"][-1], 0.0)
    label = classify_regime(btc_bias, adx_v, vol_pctile, noise)
    return RegimeVector(
        btc_bias=btc_bias, btc_strength=btc_strength, vol_pctile=vol_pctile,
        adx=adx_v, session_weight=session_weight_now(), noise_index=noise,
        breadth=breadth, regime_label=label,
    )


def select_combo(regime: RegimeVector) -> str:
    if regime.vol_pctile > 0.72 and regime.adx > 26:
        return "scalp"
    if regime.vol_pctile < 0.28 and regime.adx < 17:
        return "swing"
    return "intraday"


# Regime → engine priority multipliers (dynamic base; history refines further)
REGIME_ENGINE_PRIOR = {
    "trend_bull": {
        "trend_continuation": 1.25, "order_block": 1.15, "fvg_rebalance": 1.05,
        "smc_liquidity": 0.95, "volatility_breakout": 1.10, "mean_reversion": 0.70,
    },
    "trend_bear": {
        "trend_continuation": 1.25, "order_block": 1.15, "fvg_rebalance": 1.05,
        "smc_liquidity": 0.95, "volatility_breakout": 1.10, "mean_reversion": 0.70,
    },
    "range": {
        "mean_reversion": 1.30, "smc_liquidity": 1.20, "fvg_rebalance": 1.10,
        "order_block": 0.95, "trend_continuation": 0.65, "volatility_breakout": 0.70,
    },
    "expansion": {
        "volatility_breakout": 1.35, "trend_continuation": 1.15, "order_block": 1.05,
        "fvg_rebalance": 0.90, "smc_liquidity": 0.85, "mean_reversion": 0.55,
    },
    "reversal": {
        "smc_liquidity": 1.30, "fvg_rebalance": 1.20, "order_block": 1.15,
        "mean_reversion": 1.05, "trend_continuation": 0.70, "volatility_breakout": 0.75,
    },
    "chop": {
        "mean_reversion": 1.10, "smc_liquidity": 1.05, "fvg_rebalance": 0.95,
        "order_block": 0.80, "trend_continuation": 0.55, "volatility_breakout": 0.50,
    },
}

# ============================================================================
# MARKET STRUCTURE (SMC primitives)
# ============================================================================


@dataclass
class Swing:
    index: int
    price: float
    kind: str  # high | low


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    out = []
    for i in range(left, len(candles) - right):
        wh = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        wl = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        if candles[i]["h"] == max(wh):
            out.append(Swing(i, candles[i]["h"], "high"))
        if candles[i]["l"] == min(wl):
            out.append(Swing(i, candles[i]["l"], "low"))
    return out


@dataclass
class StructureState:
    bias: str
    last_bos_index: int
    last_choch_index: int
    swings: list[Swing]


def analyze_structure(candles: list[dict], swings: list[Swing]) -> StructureState:
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    bias, last_bos, last_choch = "neutral", -1, -1
    if len(highs) >= 2 and len(lows) >= 2:
        hh = highs[-1].price > highs[-2].price
        hl = lows[-1].price > lows[-2].price
        lh = highs[-1].price < highs[-2].price
        ll = lows[-1].price < lows[-2].price
        if hh and hl:
            bias, last_bos = "bullish", highs[-1].index
        elif lh and ll:
            bias, last_bos = "bearish", lows[-1].index
        elif hh and ll:
            bias, last_choch = "bullish", lows[-1].index
        elif lh and hl:
            bias, last_choch = "bearish", highs[-1].index
    return StructureState(bias, last_bos, last_choch, swings)


@dataclass
class Zone:
    low: float
    high: float
    kind: str
    index: int
    untested: bool = True
    quality: int = 1

    def mid(self) -> float:
        return (self.low + self.high) / 2

    def contains(self, price: float, buf: float = 0.0) -> bool:
        return (self.low - buf) <= price <= (self.high + buf)


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int = 60) -> list[Zone]:
    zones = []
    start = max(1, len(candles) - lookback)
    for i in range(start, len(candles) - 1):
        c, nxt = candles[i], candles[i + 1]
        move = abs(nxt["c"] - nxt["o"])
        if move < 1.15 * atr_vals[i]:
            continue
        if c["c"] < c["o"] and nxt["c"] > nxt["o"] and nxt["c"] > c["h"]:
            zones.append(Zone(c["l"], c["h"], "bullish_ob", i))
        elif c["c"] > c["o"] and nxt["c"] < nxt["o"] and nxt["c"] < c["l"]:
            zones.append(Zone(c["l"], c["h"], "bearish_ob", i))
    return zones[-10:]


def find_fvgs(candles: list[dict], lookback: int = 60) -> list[Zone]:
    zones = []
    start = max(2, len(candles) - lookback)
    for i in range(start, len(candles)):
        a, b, c = candles[i - 2], candles[i - 1], candles[i]
        if a["h"] < c["l"]:
            zones.append(Zone(a["h"], c["l"], "bullish_fvg", i))
        elif a["l"] > c["h"]:
            zones.append(Zone(c["h"], a["l"], "bearish_fvg", i))
    return zones[-12:]


def mark_untested(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    for z in zones:
        for c in candles[z.index + 1:]:
            if z.contains(c["c"]) or (c["l"] <= z.high and c["h"] >= z.low):
                z.untested = False
                break
    return zones


def cluster_levels(levels: list[float], tol_pct: float = 0.0015) -> list[tuple[float, int]]:
    if not levels:
        return []
    levels = sorted(levels)
    clusters, current = [], [levels[0]]
    for lv in levels[1:]:
        if abs(lv - current[-1]) / max(current[-1], 1e-12) <= tol_pct:
            current.append(lv)
        else:
            clusters.append((sum(current) / len(current), len(current)))
            current = [lv]
    clusters.append((sum(current) / len(current), len(current)))
    return clusters


def build_liquidity_pools(swings: list[Swing]) -> dict:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    return {"resistance": cluster_levels(highs), "support": cluster_levels(lows)}


def detect_sweep(candles: list[dict], pools: dict, direction: str, lookback: int = 12) -> Optional[dict]:
    window = candles[-lookback:]
    targets = pools["support"] if direction == "long" else pools["resistance"]
    for level, touches in targets:
        for c in window:
            if direction == "long" and c["l"] < level and c["c"] > level:
                return {"level": level, "touches": touches, "candle": c, "extreme": c["l"]}
            if direction == "short" and c["h"] > level and c["c"] < level:
                return {"level": level, "touches": touches, "candle": c, "extreme": c["h"]}
    return None


def premium_discount(candles: list[dict], lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    eq = (hi + lo) / 2
    price = candles[-1]["c"]
    return {"high": hi, "low": lo, "eq": eq, "zone": "premium" if price > eq else "discount"}


def detect_mss(candles_exec: list[dict], direction: str, lookback: int = 30) -> Optional[dict]:
    swings = find_swings(candles_exec[-lookback:], left=2, right=2)
    if not swings:
        return None
    last_close = candles_exec[-1]["c"]
    if direction == "long":
        highs = [s for s in swings if s.kind == "high"]
        if highs and last_close > highs[-1].price:
            return {"confirm_price": highs[-1].price, "index": highs[-1].index}
    else:
        lows = [s for s in swings if s.kind == "low"]
        if lows and last_close < lows[-1].price:
            return {"confirm_price": lows[-1].price, "index": lows[-1].index}
    return None


def adaptive_sl_buffer(candles: list[dict], atr_val: float, vol_pctile: float, lookback: int = 20) -> float:
    window = candles[-lookback:]
    if len(window) < 5:
        base = atr_val * 0.35
    else:
        wicks = []
        for c in window:
            body_top, body_bot = max(c["o"], c["c"]), min(c["o"], c["c"])
            wicks.append(max(c["h"] - body_top, body_bot - c["l"]))
        avg_wick = sum(wicks) / len(wicks)
        base = max(atr_val * 0.28, avg_wick * 1.25)
    vol_scale = 1.0 + 0.45 * max(0.0, vol_pctile - 0.5)
    return min(base * vol_scale, atr_val * 1.05)


def clip_tp_to_liquidity(entry: float, tp: float, direction: str, pools: dict,
                         vp: dict | None = None) -> float:
    targets = pools["resistance"] if direction == "long" else pools["support"]
    candidates = [lv for lv, _ in targets if (lv > entry if direction == "long" else lv < entry)]
    if vp:
        candidates += [lv for lv in (vp["poc"], vp["vah"], vp["val"])
                       if (lv > entry if direction == "long" else lv < entry)]
    if not candidates:
        return tp
    nearest = min(candidates, key=lambda lv: abs(lv - tp))
    if abs(tp - entry) > 0 and abs(nearest - tp) / abs(tp - entry) < 0.42:
        return nearest
    return tp

# ============================================================================
# CANDIDATE MODEL
# ============================================================================


@dataclass
class Candidate:
    symbol: str
    direction: str
    engine: str
    combo_name: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    confluences: list[str] = field(default_factory=list)
    atr_val: float = 0.0
    regime_label: str = "range"
    expected_rr: float = 0.0

    def rr(self) -> float:
        risk = abs(self.entry - self.sl)
        reward = abs(self.tp2 - self.entry)
        return safe(reward / risk, 0.0) if risk else 0.0

    def rr1(self) -> float:
        risk = abs(self.entry - self.sl)
        reward = abs(self.tp1 - self.entry)
        return safe(reward / risk, 0.0) if risk else 0.0


def clamp_to_market(cand: Candidate, market_price: float) -> Candidate:
    if market_price <= 0:
        return cand
    max_dist = min(cand.atr_val * POI_ATR_MULT.get(cand.combo_name, 0.7),
                   market_price * POI_MAX_PCT_OF_PRICE)
    dist = cand.entry - market_price
    if abs(dist) <= max_dist:
        return cand
    target_dist = max_dist if dist > 0 else -max_dist
    shift = target_dist - dist
    cand.entry += shift
    cand.sl += shift
    cand.tp1 += shift
    cand.tp2 += shift
    return cand


def finalize_levels(entry, sl, direction, atr_val, pools, vp, rr1=1.6, rr2=2.8):
    risk = abs(entry - sl)
    if risk <= 0:
        return None
    if direction == "long":
        tp1 = entry + risk * rr1
        tp2 = entry + risk * rr2
    else:
        tp1 = entry - risk * rr1
        tp2 = entry - risk * rr2
    tp1 = clip_tp_to_liquidity(entry, tp1, direction, pools, vp)
    tp2 = clip_tp_to_liquidity(entry, tp2, direction, pools, vp)
    # Ensure geometric validity using actual structure extremes
    if direction == "long" and not (sl < entry < tp1 <= tp2):
        return None
    if direction == "short" and not (tp2 <= tp1 < entry < sl):
        return None
    return tp1, tp2

# ============================================================================
# SPECIALIZED ENGINES
# ============================================================================


def eng_smc_liquidity(symbol, bundle, combo_name, regime, vp) -> Optional[Candidate]:
    """Liquidity sweep → MSS → breaker/OB entry (institutional trap fade)."""
    combo = COMBOS[combo_name]
    struct = bundle[combo["struct"]]
    exec_c = bundle[combo["exec"]]
    ind_s = compute_indicators(struct)
    atr_val = ind_s["atr"][-1]
    swings = find_swings(struct)
    pools = build_liquidity_pools(swings)
    pd = premium_discount(struct)

    for direction in ("long", "short"):
        if direction == "long" and pd["zone"] != "discount":
            continue
        if direction == "short" and pd["zone"] != "premium":
            continue
        sweep = detect_sweep(struct, pools, direction)
        if not sweep:
            continue
        mss = detect_mss(exec_c, direction)
        if not mss:
            continue
        obs = mark_untested(find_order_blocks(exec_c, compute_indicators(exec_c)["atr"]), exec_c)
        kind = "bullish_ob" if direction == "long" else "bearish_ob"
        breaker = next((z for z in reversed(obs) if z.kind == kind and z.untested), None)
        entry = breaker.mid() if breaker else exec_c[-1]["c"]
        buf = adaptive_sl_buffer(struct, atr_val, regime.vol_pctile)
        if direction == "long":
            sl = min(sweep["extreme"], breaker.low if breaker else entry) - buf
        else:
            sl = max(sweep["extreme"], breaker.high if breaker else entry) + buf
        levels = finalize_levels(entry, sl, direction, atr_val, pools, vp, 1.55, 2.9)
        if not levels:
            continue
        tp1, tp2 = levels
        conf = ["liquidity sweep", f"MSS ({combo['exec']})", f"{pd['zone']} zone"]
        if breaker:
            conf.append("untested breaker/OB")
        if sweep["touches"] > 1:
            conf.append(f"pool tapped {sweep['touches']}x")
        div = ind_s.get("rsi_divergence", {})
        if (direction == "long" and div.get("type") == "bullish") or \
           (direction == "short" and div.get("type") == "bearish"):
            conf.append(f"RSI {div['type']} divergence")
        cand = Candidate(symbol, direction, "smc_liquidity", combo_name,
                         entry, sl, tp1, tp2, conf, atr_val, regime.regime_label)
        if cand.rr() >= MIN_RR:
            return cand
    return None


def eng_order_block(symbol, bundle, combo_name, regime, vp) -> Optional[Candidate]:
    """Fresh HTF order block with structure bias + exec reaction."""
    combo = COMBOS[combo_name]
    bias_c = bundle[combo["bias"]]
    exec_c = bundle[combo["exec"]]
    ind_b = compute_indicators(bias_c)
    ind_e = compute_indicators(exec_c)
    atr_val = ind_e["atr"][-1]
    swings_b = find_swings(bias_c)
    struct = analyze_structure(bias_c, swings_b)
    if struct.bias == "neutral":
        return None
    direction = "long" if struct.bias == "bullish" else "short"
    obs = mark_untested(find_order_blocks(bias_c, ind_b["atr"], lookback=80), bias_c)
    kind = "bullish_ob" if direction == "long" else "bearish_ob"
    zone = next((z for z in reversed(obs) if z.kind == kind and z.untested), None)
    if not zone:
        return None
    price = exec_c[-1]["c"]
    # Price must be interacting with or just leaving the OB
    buf = atr_val * 0.35
    if not zone.contains(price, buf) and abs(price - zone.mid()) > atr_val * 1.2:
        return None
    # Exec confirmation: candle closes back through OB mid in trade direction
    last = exec_c[-1]
    if direction == "long" and not (last["c"] > zone.mid() and last["c"] > last["o"]):
        return None
    if direction == "short" and not (last["c"] < zone.mid() and last["c"] < last["o"]):
        return None
    entry = zone.mid() if zone.contains(price, buf) else price
    sl_buf = adaptive_sl_buffer(exec_c, atr_val, regime.vol_pctile)
    sl = (zone.low - sl_buf) if direction == "long" else (zone.high + sl_buf)
    pools = build_liquidity_pools(find_swings(exec_c))
    levels = finalize_levels(entry, sl, direction, atr_val, pools, vp, 1.6, 2.7)
    if not levels:
        return None
    tp1, tp2 = levels
    conf = [f"HTF {struct.bias} structure", "untested order block", f"{combo['bias']} bias"]
    if ind_b["adx"][-1] >= 20:
        conf.append(f"ADX {ind_b['adx'][-1]:.0f}")
    cand = Candidate(symbol, direction, "order_block", combo_name,
                     entry, sl, tp1, tp2, conf, atr_val, regime.regime_label)
    return cand if cand.rr() >= MIN_RR else None


def eng_fvg_rebalance(symbol, bundle, combo_name, regime, vp) -> Optional[Candidate]:
    """Untested FVG fill with directional structure alignment."""
    combo = COMBOS[combo_name]
    struct = bundle[combo["struct"]]
    exec_c = bundle[combo["exec"]]
    ind_s = compute_indicators(struct)
    atr_val = ind_s["atr"][-1]
    swings = find_swings(struct)
    st = analyze_structure(struct, swings)
    fvgs = mark_untested(find_fvgs(struct), struct)
    price = exec_c[-1]["c"]
    for direction, kind in (("long", "bullish_fvg"), ("short", "bearish_fvg")):
        if st.bias not in ("neutral", "bullish" if direction == "long" else "bearish"):
            if st.bias != ("bullish" if direction == "long" else "bearish"):
                # Allow neutral; require aligned bias when present
                if st.bias != "neutral":
                    continue
        zone = next((z for z in reversed(fvgs) if z.kind == kind and z.untested), None)
        if not zone:
            continue
        if abs(price - zone.mid()) > atr_val * 1.4:
            continue
        # Reaction: wick into FVG, close back out
        last = exec_c[-1]
        if direction == "long":
            if not (last["l"] <= zone.high and last["c"] >= zone.mid()):
                continue
        else:
            if not (last["h"] >= zone.low and last["c"] <= zone.mid()):
                continue
        entry = zone.mid()
        buf = adaptive_sl_buffer(exec_c, atr_val, regime.vol_pctile)
        sl = (zone.low - buf) if direction == "long" else (zone.high + buf)
        pools = build_liquidity_pools(swings)
        levels = finalize_levels(entry, sl, direction, atr_val, pools, vp, 1.5, 2.6)
        if not levels:
            continue
        tp1, tp2 = levels
        conf = ["untested FVG", "rebalance reaction", f"structure {st.bias}"]
        pd = premium_discount(struct)
        if (direction == "long" and pd["zone"] == "discount") or \
           (direction == "short" and pd["zone"] == "premium"):
            conf.append(f"{pd['zone']} alignment")
        cand = Candidate(symbol, direction, "fvg_rebalance", combo_name,
                         entry, sl, tp1, tp2, conf, atr_val, regime.regime_label)
        if cand.rr() >= MIN_RR:
            return cand
    return None


def eng_trend_continuation(symbol, bundle, combo_name, regime, vp) -> Optional[Candidate]:
    """Full EMA stack + ADX trend with RSI pullback reset on exec TF."""
    combo = COMBOS[combo_name]
    bias_c = bundle[combo["bias"]]
    exec_c = bundle[combo["exec"]]
    ind_b = compute_indicators(bias_c)
    ind_e = compute_indicators(exec_c)
    if ind_b["adx"][-1] < 20:
        return None
    price = ind_b["closes"][-1]
    ef, es, et = ind_b["ema_fast"][-1], ind_b["ema_slow"][-1], ind_b["ema_trend"][-1]
    if price > ef > es > et:
        direction = "long"
    elif price < ef < es < et:
        direction = "short"
    else:
        return None
    # RSI pullback reset
    window = ind_e["rsi"][-8:]
    if direction == "long":
        if not (min(window[:-1], default=100) <= 42 and window[-1] > 46):
            return None
    else:
        if not (max(window[:-1], default=0) >= 58 and window[-1] < 54):
            return None
    atr_val = ind_e["atr"][-1]
    entry = ind_e["closes"][-1]
    buf = adaptive_sl_buffer(exec_c, atr_val, regime.vol_pctile)
    if direction == "long":
        sl = min(c["l"] for c in exec_c[-8:]) - buf
    else:
        sl = max(c["h"] for c in exec_c[-8:]) + buf
    pools = build_liquidity_pools(find_swings(exec_c))
    levels = finalize_levels(entry, sl, direction, atr_val, pools, vp, 1.5, 2.5)
    if not levels:
        return None
    tp1, tp2 = levels
    conf = [f"{combo['bias']} EMA stack", f"ADX {ind_b['adx'][-1]:.0f}", "RSI pullback reset"]
    div = ind_e.get("rsi_divergence", {})
    if (direction == "long" and div.get("type") == "bullish") or \
       (direction == "short" and div.get("type") == "bearish"):
        conf.append(f"RSI {div['type']} divergence")
    cand = Candidate(symbol, direction, "trend_continuation", combo_name,
                     entry, sl, tp1, tp2, conf, atr_val, regime.regime_label)
    return cand if cand.rr() >= MIN_RR else None


def eng_volatility_breakout(symbol, bundle, combo_name, regime, vp) -> Optional[Candidate]:
    """BB squeeze + volume expansion + range break."""
    combo = COMBOS[combo_name]
    struct = bundle[combo["struct"]]
    ind = compute_indicators(struct)
    bb_hist = ind["bb_width"][-60:]
    if len(bb_hist) < 20:
        return None
    current_bw = bb_hist[-1]
    sorted_bw = sorted(bb_hist)
    pctile = sum(1 for x in sorted_bw if x <= current_bw) / len(sorted_bw)
    if pctile > 0.38:
        return None
    last = struct[-1]
    avg_vol = ind["vol_sma20"][-2] if len(ind["vol_sma20"]) > 1 else ind["vol_sma20"][-1]
    if avg_vol <= 0 or last["v"] < 1.55 * avg_vol:
        return None
    window = struct[-21:-1]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    if last["c"] > hi:
        direction = "long"
    elif last["c"] < lo:
        direction = "short"
    else:
        return None
    # Reject breakouts against strong opposing RSI divergence
    div = ind.get("rsi_divergence", {})
    if (direction == "long" and div.get("type") == "bearish") or \
       (direction == "short" and div.get("type") == "bullish"):
        # Soft: still allow but will score lower via confluence caution
        pass
    atr_val = ind["atr"][-1]
    entry = last["c"]
    buf = adaptive_sl_buffer(struct, atr_val, regime.vol_pctile)
    sl = (lo - buf) if direction == "long" else (hi + buf)
    pools = build_liquidity_pools(find_swings(struct))
    levels = finalize_levels(entry, sl, direction, atr_val, pools, vp, 1.4, 2.4)
    if not levels:
        return None
    tp1, tp2 = levels
    conf = [f"BB squeeze pctile {pctile:.2f}", f"vol {last['v']/avg_vol:.1f}x",
            f"range break ({combo['struct']})"]
    if (direction == "long" and div.get("type") == "bearish") or \
       (direction == "short" and div.get("type") == "bullish"):
        conf.append(f"caution: opposing RSI {div.get('type')} div")
    cand = Candidate(symbol, direction, "volatility_breakout", combo_name,
                     entry, sl, tp1, tp2, conf, atr_val, regime.regime_label)
    return cand if cand.rr() >= MIN_RR else None


def eng_mean_reversion(symbol, bundle, combo_name, regime, vp) -> Optional[Candidate]:
    """Range mean reversion at extremes when ADX is low / chop regime."""
    if regime.adx > 24 and regime.regime_label not in ("range", "chop"):
        return None
    combo = COMBOS[combo_name]
    struct = bundle[combo["struct"]]
    ind = compute_indicators(struct)
    atr_val = ind["atr"][-1]
    price = ind["closes"][-1]
    pd = premium_discount(struct, lookback=40)
    r = ind["rsi"][-1]
    # Extremes only
    direction = None
    if pd["zone"] == "discount" and r <= 32:
        direction = "long"
    elif pd["zone"] == "premium" and r >= 68:
        direction = "short"
    if not direction:
        return None
    # Rejection candle
    last = struct[-1]
    body = abs(last["c"] - last["o"])
    rng = last["h"] - last["l"] or 1e-12
    if direction == "long":
        if not (last["c"] > last["o"] and (min(last["o"], last["c"]) - last["l"]) / rng >= 0.35):
            return None
    else:
        if not (last["c"] < last["o"] and (last["h"] - max(last["o"], last["c"])) / rng >= 0.35):
            return None
    entry = price
    buf = adaptive_sl_buffer(struct, atr_val, regime.vol_pctile)
    if direction == "long":
        sl = min(last["l"], pd["low"]) - buf
        # Target equilibrium / mid-range
        raw_tp1 = min(pd["eq"], entry + (entry - sl) * 1.4)
        raw_tp2 = entry + (entry - sl) * 2.2
    else:
        sl = max(last["h"], pd["high"]) + buf
        raw_tp1 = max(pd["eq"], entry - (sl - entry) * 1.4)
        raw_tp2 = entry - (sl - entry) * 2.2
    pools = build_liquidity_pools(find_swings(struct))
    tp1 = clip_tp_to_liquidity(entry, raw_tp1, direction, pools, vp)
    tp2 = clip_tp_to_liquidity(entry, raw_tp2, direction, pools, vp)
    if direction == "long" and not (sl < entry < tp1):
        return None
    if direction == "short" and not (tp1 < entry < sl):
        return None
    conf = [f"range extreme ({pd['zone']})", f"RSI {r:.0f}", "rejection wick", f"ADX {regime.adx:.0f}"]
    cand = Candidate(symbol, direction, "mean_reversion", combo_name,
                     entry, sl, tp1, tp2, conf, atr_val, regime.regime_label)
    return cand if cand.rr() >= MIN_RR else None


ENGINE_BUILDERS = [
    eng_smc_liquidity,
    eng_order_block,
    eng_fvg_rebalance,
    eng_trend_continuation,
    eng_volatility_breakout,
    eng_mean_reversion,
]

# ============================================================================
# CENTRAL DECISION ENGINE
# ============================================================================


def logistic(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def score_candidate(cand: Candidate, regime: RegimeVector, state: dict,
                    btc_bias: str, book: dict, vp: dict) -> float:
    """Unified logistic confidence 0–100 with regime + learning weights."""
    z = 0.0
    caution = sum(1 for c in cand.confluences if c.startswith("caution:"))
    positive = len(cand.confluences) - caution
    z += 0.85 * (positive - 1.4)
    z -= 0.85 * caution
    z += 1.15 * (cand.rr() - MIN_RR)
    z += 0.55 * (cand.rr1() - 1.2)
    z += 1.25 * (regime.favorability() - 0.5)

    # BTC alignment
    if cand.symbol != "BTC":
        if (cand.direction == "long" and btc_bias == "bullish") or \
           (cand.direction == "short" and btc_bias == "bearish"):
            z += 0.55
        elif (cand.direction == "long" and btc_bias == "bearish") or \
             (cand.direction == "short" and btc_bias == "bullish"):
            z -= 0.75

    # Orderbook micro
    imb = book.get("imbalance", 0.0) or 0.0
    z += 0.55 * imb if cand.direction == "long" else -0.55 * imb

    # VWAP
    if vp:
        aligned = (cand.entry >= vp["vwap"]) if cand.direction == "long" else (cand.entry <= vp["vwap"])
        z += 0.38 if aligned else -0.22

    # Regime-engine prior
    prior = REGIME_ENGINE_PRIOR.get(regime.regime_label, {}).get(cand.engine, 1.0)
    z += 1.6 * (prior - 1.0)

    # Learned engine weight (shrunk toward 1.0)
    ew = state["engine_weights"].get(cand.engine, 1.0)
    z += 2.6 * (ew - 1.0)

    # Confidence calibration scalar
    cal = state.get("confidence_calibration", 1.0)
    conf = 100 * logistic(z) * cal
    conf = max(0.0, min(99.5, conf))
    return round(conf, 2)


def expected_value(cand: Candidate, confidence: float, state: dict) -> float:
    """EV in R-units using engine historical WR when available, else conf proxy."""
    stats = state["engine_stats"].get(cand.engine, {})
    n = stats.get("n", 0)
    if n >= MIN_SAMPLES_FOR_LEARN:
        wr = stats["wins"] / max(stats["wins"] + stats["losses"], 1)
    else:
        wr = confidence / 100.0
    # Assume average win ≈ rr of TP1.5 blended, loss = -1R
    avg_win_r = 0.55 * cand.rr1() + 0.45 * cand.rr()
    return wr * avg_win_r - (1 - wr) * 1.0


def adaptive_threshold(regime: RegimeVector, base: float) -> float:
    fav = regime.favorability()
    adj = (0.5 - fav) * 9.0
    # Tighten hard in chop
    if regime.regime_label == "chop":
        adj += 6.0
    elif regime.regime_label == "expansion":
        adj -= 2.0
    return max(GOVERNOR_FLOOR, min(GOVERNOR_CEIL, base + adj))


def governor_adjust(state: dict, signals_24h: float):
    gov = state["governor"]
    now = time.time()
    gov["daily_count_ema"] = 0.9 * gov["daily_count_ema"] + 0.1 * signals_24h
    if now - gov.get("last_adjust_ts", 0) < GOVERNOR_MIN_INTERVAL_S:
        return
    ema_c = gov["daily_count_ema"]
    if ema_c < TARGET_SIGNALS_MIN:
        gov["threshold"] = max(GOVERNOR_FLOOR, gov["threshold"] - GOVERNOR_STEP)
        gov["last_adjust_ts"] = now
    elif ema_c > TARGET_SIGNALS_MAX:
        gov["threshold"] = min(GOVERNOR_CEIL, gov["threshold"] + GOVERNOR_STEP)
        gov["last_adjust_ts"] = now


def estimate_signals_24h(state: dict) -> int:
    cutoff = time.time() - 86400
    return sum(1 for h in state["signal_history"] if h.get("ts", 0) >= cutoff and h.get("sent", True))


def grade_for_confidence(c: float) -> str:
    if c >= 82:
        return "A+"
    if c >= 72:
        return "A"
    if c >= 62:
        return "B"
    return "C"

# ============================================================================
# CONTINUOUS LEARNING
# ============================================================================


def tune_engine_weights(state: dict):
    """Gradual regularized weight drift from historical outcomes."""
    weights = state["engine_weights"]
    history = [h for h in state["signal_history"] if h.get("result") in ("win", "loss")]
    for eng in ENGINES:
        relevant = [h for h in history if h.get("engine") == eng]
        if len(relevant) < MIN_SAMPLES_FOR_LEARN:
            continue
        recent = relevant[-50:]
        wr = sum(1 for h in recent if h["result"] == "win") / len(recent)
        # Shrink target toward 1.0; WR 0.5 → ~1.05, WR 0.65 → ~1.18
        target = 0.80 + 0.55 * wr
        target = max(ENGINE_WEIGHT_MIN, min(ENGINE_WEIGHT_MAX, target))
        weights[eng] += ENGINE_WEIGHT_LR * (target - weights[eng])
        weights[eng] = max(ENGINE_WEIGHT_MIN, min(ENGINE_WEIGHT_MAX, weights[eng]))


def update_engine_stats(state: dict, sig: dict, result: str, r_realized: float,
                        mfe: float = 0.0, mae: float = 0.0):
    eng = sig.get("engine")
    if eng not in state["engine_stats"]:
        return
    st = state["engine_stats"][eng]
    st["n"] = st.get("n", 0) + 1
    st["r_sum"] = st.get("r_sum", 0.0) + r_realized
    st["mfe_sum"] = st.get("mfe_sum", 0.0) + mfe
    st["mae_sum"] = st.get("mae_sum", 0.0) + mae
    hold = max(0, int(time.time()) - sig.get("ts", int(time.time())))
    st["hold_sum"] = st.get("hold_sum", 0.0) + hold
    conf = sig.get("confidence", 50)
    st["conf_sum"] = st.get("conf_sum", 0.0) + conf
    if result == "win":
        st["wins"] = st.get("wins", 0) + 1
        if conf >= 65:
            st["conf_correct"] = st.get("conf_correct", 0) + 1
    else:
        st["losses"] = st.get("losses", 0) + 1
        if conf < 65:
            st["conf_correct"] = st.get("conf_correct", 0) + 1

    # Regime bucket
    rl = sig.get("regime_label", "unknown")
    rs = state["regime_stats"].setdefault(rl, {"wins": 0, "losses": 0, "n": 0})
    rs["n"] += 1
    if result == "win":
        rs["wins"] += 1
    else:
        rs["losses"] += 1

    # Confidence calibration drift (slow)
    predicted = conf / 100.0
    actual = 1.0 if result == "win" else 0.0
    # If overconfident on losses or underconfident on wins, nudge
    err = actual - predicted
    state["confidence_calibration"] = max(0.85, min(1.15,
        state.get("confidence_calibration", 1.0) + CALIBRATION_LR * err * 0.15))

    insight = (
        f"{eng}|{result}|R={r_realized:+.2f}|regime={rl}|conf={conf:.0f}|"
        f"entry_q={'good' if abs(r_realized) > 0 else 'flat'}"
    )
    state.setdefault("learning_log", []).append({"ts": int(time.time()), "note": insight})


def analyze_trade_postmortem(sig: dict, result: str, exit_price: float) -> dict:
    risk = sig.get("risk") or abs(sig["entry"] - sig["sl"])
    if not risk:
        r = 0.0
    else:
        raw = (exit_price - sig["entry"]) if sig["direction"] == "long" else (sig["entry"] - exit_price)
        r = raw / risk
    return {
        "why": "target reached" if result == "win" else "invalidated at structure SL",
        "r_realized": round(r, 2),
        "engine": sig.get("engine"),
        "regime": sig.get("regime_label"),
        "confidence": sig.get("confidence"),
        "rr_planned": safe(abs(sig.get("tp2", 0) - sig["entry"]) / risk, 0),
    }

# ============================================================================
# CORRELATION DEDUP
# ============================================================================


def compute_returns(candles: list[dict], lookback: int) -> list[float]:
    closes = [c["c"] for c in candles[-lookback - 1:]]
    return [safe((closes[i] - closes[i - 1]) / closes[i - 1], 0.0) for i in range(1, len(closes))]


def pearson(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 8:
        return 0.0
    a, b = a[-n:], b[-n:]
    try:
        return statistics.correlation(a, b)
    except (statistics.StatisticsError, ValueError):
        return 0.0


def build_correlation_clusters(bundles: dict[str, dict]) -> list[set[str]]:
    returns = {sym: compute_returns(b["1h"], CORR_LOOKBACK) for sym, b in bundles.items()}
    symbols = list(returns.keys())
    parent = {s: s for s in symbols}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for i in range(len(symbols)):
        for j in range(i + 1, len(symbols)):
            if pearson(returns[symbols[i]], returns[symbols[j]]) >= CORR_CLUSTER_THRESHOLD:
                union(symbols[i], symbols[j])

    clusters: dict[str, set] = {}
    for s in symbols:
        clusters.setdefault(find(s), set()).add(s)
    return list(clusters.values())


def dedup_correlated(ranked: list[dict], clusters: list[set[str]]) -> list[dict]:
    def cluster_of(sym: str) -> frozenset:
        for c in clusters:
            if sym in c:
                return frozenset(c)
        return frozenset({sym})

    seen: dict[tuple, dict] = {}
    for r in ranked:
        key = (cluster_of(r["symbol"]), r["direction"])
        if key not in seen or r["score"] > seen[key]["score"]:
            seen[key] = r
    return list(seen.values())

# ============================================================================
# FILTERS / COOLDOWN
# ============================================================================


def passes_hard_filters(symbol: str, snapshot: dict, atr_pct: float, cand: Candidate) -> tuple[bool, str]:
    info = snapshot.get(symbol)
    if not info:
        return False, "no snapshot"
    if info["oi_usd"] < MIN_OI_USD:
        return False, f"OI low (${info['oi_usd']:,.0f})"
    if not (MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT):
        return False, f"ATR% band ({atr_pct:.4f})"
    if cand.rr() < MIN_RR:
        return False, f"RR {cand.rr():.2f}"
    max_dist = min(cand.atr_val * POI_ATR_MULT.get(cand.combo_name, 0.7),
                   cand.entry * POI_MAX_PCT_OF_PRICE)
    if abs(cand.entry - info["mark"]) > max_dist:
        return False, "entry far from mark"
    risk = abs(cand.entry - cand.sl)
    if risk > 0 and abs(info["mark"] - cand.entry) / risk > MAX_ENTRY_DRIFT_R:
        return False, "entry drift > max R"
    # Funding extreme headwind soft-block only if very extreme
    funding = info.get("funding") or 0.0
    if cand.direction == "long" and funding > 0.0012:
        return False, "extreme long funding headwind"
    if cand.direction == "short" and funding < -0.0012:
        return False, "extreme short funding headwind"
    return True, "ok"


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    last = state["cooldowns"].get(f"{symbol}:{direction}", -9999)
    return (bar_index - last) >= COOLDOWN_BARS


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    state["cooldowns"][f"{symbol}:{direction}"] = bar_index


def is_recent_duplicate(state: dict, symbol: str, direction: str, entry: float) -> bool:
    cutoff = time.time() - DEDUP_TIME_WINDOW_HOURS * 3600
    for h in state["signal_history"]:
        if h.get("symbol") != symbol or h.get("direction") != direction:
            continue
        if h.get("ts", 0) < cutoff:
            continue
        if abs(h.get("entry", 0) - entry) / max(entry, 1e-12) <= DEDUP_PRICE_TOL_PCT:
            return True
    return False

# ============================================================================
# TELEGRAM
# ============================================================================


def fmt_px(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def confidence_bar(c: float) -> str:
    filled = round(c / 10)
    return "█" * filled + "░" * (10 - filled)


def format_signal(cand: Candidate, confidence: float, grade: str, ev: float) -> str:
    arrow = "🟢 LONG" if cand.direction == "long" else "🔴 SHORT"
    hold = COMBOS[cand.combo_name]["hold_hint"]
    lines = [
        f"<b>{ENGINE_NAME} v{__version__}</b>",
        f"<b>{cand.symbol}/USD</b> — {arrow}",
        f"Engine: <code>{cand.engine}</code> | Grade <b>{grade}</b> | Regime: {cand.regime_label}",
        "",
        f"Entry: <code>{fmt_px(cand.entry)}</code>",
        f"SL:    <code>{fmt_px(cand.sl)}</code>",
        f"TP1:   <code>{fmt_px(cand.tp1)}</code>",
        f"TP2:   <code>{fmt_px(cand.tp2)}</code>",
        "",
        f"R:R (TP2): <code>{cand.rr():.2f}</code> | EV: <code>{ev:+.2f}R</code>",
        f"Confidence: {confidence:.1f}% {confidence_bar(confidence)}",
        f"Est. hold: {hold} | Combo: {cand.combo_name}",
        "",
        "Confluences:",
    ]
    for c in cand.confluences:
        lines.append(f"  • {c}")
    lines.append("")
    lines.append(time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()))
    return "\n".join(lines)


def send_telegram(text: str, parse_mode: str = "HTML") -> int | None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info("TG not configured:\n%s", text)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TG_CHAT_ID, "text": text, "parse_mode": parse_mode,
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("result", {}).get("message_id")
    except Exception as e:
        log.error("TG send failed: %s", e)
        return None


def reply_telegram(text: str, reply_to: int | None) -> int | None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info("TG update:\n%s", text)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("result", {}).get("message_id")
    except Exception as e:
        log.error("TG reply failed: %s", e)
        return None


def react_telegram(message_id: int | None, emoji: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID or not message_id:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    payload = json.dumps({
        "chat_id": TG_CHAT_ID, "message_id": message_id,
        "reaction": [{"type": "emoji", "emoji": emoji}],
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as e:
        log.debug("TG react non-fatal: %s", e)

# ============================================================================
# TRADE LIFECYCLE
# ============================================================================


def record_signal(state, cand, confidence, grade, bar_index, message_id, ev) -> dict:
    entry = {
        "symbol": cand.symbol, "direction": cand.direction, "engine": cand.engine,
        "combo": cand.combo_name, "entry": cand.entry, "sl": cand.sl,
        "risk": abs(cand.entry - cand.sl), "tp1": cand.tp1, "tp2": cand.tp2,
        "confidence": confidence, "grade": grade, "ev": ev,
        "regime_label": cand.regime_label, "confluences": cand.confluences,
        "ts": int(time.time()), "bar_index": bar_index, "result": "open",
        "tp1_hit": False, "message_id": message_id, "sent": True,
        "mfe": 0.0, "mae": 0.0,
    }
    state["active_signals"].append(entry)
    state["signal_history"].append(dict(entry))
    update_cooldown(state, cand.symbol, cand.direction, bar_index)
    return entry


def _r_multiple(sig: dict, price: float) -> float:
    risk = sig.get("risk") or abs(sig["entry"] - sig["sl"])
    if not risk:
        return 0.0
    raw = (price - sig["entry"]) if sig["direction"] == "long" else (sig["entry"] - price)
    return round(raw / risk, 2)


def _sync_history(state: dict, sig: dict):
    for h in state["signal_history"]:
        if h.get("ts") == sig.get("ts") and h.get("symbol") == sig.get("symbol") \
           and h.get("direction") == sig.get("direction"):
            h.update(sig)
            return


def _notify_tp1(sig: dict, price: float):
    r = _r_multiple(sig, price)
    text = (f"🔥 <b>TP1 Hit</b> — {sig['symbol']} {sig['direction'].upper()}\n"
            f"Price: <code>{fmt_px(price)}</code> | +{r:.2f}R\n"
            f"SL → breakeven <code>{fmt_px(sig['entry'])}</code>")
    reply_telegram(text, sig.get("message_id"))
    react_telegram(sig.get("message_id"), REACT_TP1)


def _close_out(state: dict, sig: dict, result: str, price: float):
    r = _r_multiple(sig, price)
    pm = analyze_trade_postmortem(sig, result, price)
    sig["result"] = result
    sig["exit_price"] = price
    sig["r_realized"] = r
    sig["closed_ts"] = int(time.time())
    sig["postmortem"] = pm
    _sync_history(state, sig)
    update_engine_stats(state, sig, result, r, sig.get("mfe", 0), sig.get("mae", 0))

    if result == "win":
        headline = "✅ <b>TP2 Hit — WIN</b>"
        emoji = REACT_TP2
    elif sig.get("tp1_hit"):
        headline = "🤝 <b>Closed at breakeven</b>"
        emoji = REACT_BE
        result = "be"
        sig["result"] = "be"
    else:
        headline = "❌ <b>SL Hit — LOSS</b>"
        emoji = REACT_SL

    text = (f"{headline} — {sig['symbol']} {sig['direction'].upper()}\n"
            f"Engine: <code>{sig.get('engine')}</code>\n"
            f"Exit: <code>{fmt_px(price)}</code> | Result: {r:+.2f}R\n"
            f"Regime: {sig.get('regime_label', '?')}")
    reply_telegram(text, sig.get("message_id"))
    react_telegram(sig.get("message_id"), emoji)
    log.info("Resolved %s %s %s → %s (%.2fR)", sig["symbol"], sig["direction"],
             sig.get("engine"), result, r)


def check_active_signals(state: dict, snapshot: dict):
    """Resolve using mark as proxy each scan; prefer high/low when candles available."""
    still = []
    for sig in state["active_signals"]:
        info = snapshot.get(sig["symbol"])
        if not info or not info.get("mark"):
            still.append(sig)
            continue
        price = info["mark"]
        direction = sig["direction"]

        # Track MFE / MAE in R
        r_now = _r_multiple(sig, price)
        sig["mfe"] = max(sig.get("mfe", 0.0), r_now)
        sig["mae"] = min(sig.get("mae", 0.0), r_now)

        hit_sl = (price <= sig["sl"]) if direction == "long" else (price >= sig["sl"])
        hit_tp2 = (price >= sig["tp2"]) if direction == "long" else (price <= sig["tp2"])
        hit_tp1 = (not sig.get("tp1_hit")) and (
            (price >= sig["tp1"]) if direction == "long" else (price <= sig["tp1"])
        )

        if hit_sl:
            _close_out(state, sig, "win" if sig.get("tp1_hit") else "loss", price)
            continue
        if hit_tp2:
            _close_out(state, sig, "win", price)
            continue
        if hit_tp1:
            sig["tp1_hit"] = True
            sig["sl"] = sig["entry"]
            _notify_tp1(sig, price)
            _sync_history(state, sig)
        still.append(sig)
    state["active_signals"] = still


def refine_resolution_with_candles(state: dict, reference_ms: int):
    """Second-pass: for active signals, use actual candle highs/lows on exec TF."""
    for sig in list(state["active_signals"]):
        if sig.get("resolved"):
            continue
        combo = COMBOS.get(sig.get("combo", "intraday"), COMBOS["intraday"])
        exec_tf = combo["exec"]
        try:
            candles = get_candles(sig["symbol"], exec_tf, 80, reference_ms)
        except Exception:
            continue
        if not candles:
            continue
        last_ts = sig.get("last_candle_ts", sig.get("ts", 0) * 1000)
        new_cs = [c for c in candles if c["t"] > last_ts]
        if not new_cs:
            continue
        direction = sig["direction"]
        for c in new_cs:
            sig["last_candle_ts"] = c["t"]
            # MFE/MAE from actual extremes
            if direction == "long":
                sig["mfe"] = max(sig.get("mfe", 0), _r_multiple(sig, c["h"]))
                sig["mae"] = min(sig.get("mae", 0), _r_multiple(sig, c["l"]))
                hit_tp2 = c["h"] >= sig["tp2"]
                hit_tp1 = (not sig.get("tp1_hit")) and c["h"] >= sig["tp1"]
                hit_sl = c["l"] <= sig["sl"]
            else:
                sig["mfe"] = max(sig.get("mfe", 0), _r_multiple(sig, c["l"]))
                sig["mae"] = min(sig.get("mae", 0), _r_multiple(sig, c["h"]))
                hit_tp2 = c["l"] <= sig["tp2"]
                hit_tp1 = (not sig.get("tp1_hit")) and c["l"] <= sig["tp1"]
                hit_sl = c["h"] >= sig["sl"]

            if hit_tp2 and hit_sl:
                # Same bar both — closer to open wins
                outcome = "loss" if abs(sig["sl"] - c["o"]) < abs(sig["tp2"] - c["o"]) else "win"
                px = sig["sl"] if outcome == "loss" else sig["tp2"]
                _close_out(state, sig, outcome, px)
                break
            if hit_sl:
                _close_out(state, sig, "win" if sig.get("tp1_hit") else "loss", sig["sl"])
                break
            if hit_tp2:
                _close_out(state, sig, "win", sig["tp2"])
                break
            if hit_tp1:
                sig["tp1_hit"] = True
                sig["sl"] = sig["entry"]
                _notify_tp1(sig, sig["tp1"])
                _sync_history(state, sig)
        if sig.get("result") not in ("open", None) and sig in state["active_signals"]:
            pass  # removed in _close_out path via check; ensure cleanup
    state["active_signals"] = [s for s in state["active_signals"] if s.get("result") == "open"]

# ============================================================================
# DAILY SUMMARY
# ============================================================================


def generate_daily_summary(state: dict) -> str:
    cutoff = time.time() - 86400
    recent = [h for h in state["signal_history"] if h.get("ts", 0) >= cutoff]
    resolved = [h for h in recent if h.get("result") in ("win", "loss", "be")]
    wins = [h for h in resolved if h["result"] == "win"]
    losses = [h for h in resolved if h["result"] == "loss"]
    be = [h for h in resolved if h["result"] == "be"]
    open_now = [h for h in recent if h.get("result") == "open"]
    total_r = sum(h.get("r_realized", 0.0) for h in resolved)
    decided = len(wins) + len(losses)
    wr = (len(wins) / decided * 100) if decided else 0.0
    gross_win = sum(h.get("r_realized", 0) for h in wins) or 0.0
    gross_loss = abs(sum(h.get("r_realized", 0) for h in losses)) or 1e-9
    pf = gross_win / gross_loss if losses else (gross_win if wins else 0.0)
    avg_rr = (sum(h.get("r_realized", 0) for h in resolved) / len(resolved)) if resolved else 0.0
    holds = [h.get("closed_ts", h.get("ts", 0)) - h.get("ts", 0) for h in resolved if h.get("closed_ts")]
    avg_hold_h = (sum(holds) / len(holds) / 3600) if holds else 0.0

    lines = [
        f"📊 <b>{ENGINE_NAME} v{__version__} — 24h Summary</b>",
        "",
        f"Signals: {len(recent)} | Open: {len(open_now)}",
        f"W/L/BE: {len(wins)}/{len(losses)}/{len(be)}",
        f"Win rate: {wr:.1f}% | PF: {pf:.2f}",
        f"Net R: {total_r:+.2f} | Avg R: {avg_rr:+.2f}",
        f"Avg hold: {avg_hold_h:.1f}h",
        "",
        "<b>By specialized engine:</b>",
    ]
    by_eng: dict[str, list] = {}
    for h in resolved:
        by_eng.setdefault(h.get("engine", "?"), []).append(h)
    for eng, items in sorted(by_eng.items()):
        w = sum(1 for i in items if i["result"] == "win")
        lines.append(f"  • {eng}: {w}/{len(items)} ({100*w/len(items):.0f}%) "
                     f"w={state['engine_weights'].get(eng, 1):.2f}")

    lines.append("")
    lines.append("<b>By market regime:</b>")
    by_rg: dict[str, list] = {}
    for h in resolved:
        by_rg.setdefault(h.get("regime_label", "?"), []).append(h)
    for rg, items in sorted(by_rg.items()):
        w = sum(1 for i in items if i["result"] == "win")
        lines.append(f"  • {rg}: {w}/{len(items)}")

    # Confidence accuracy
    conf_ok = 0
    conf_n = 0
    for h in resolved:
        if "confidence" not in h:
            continue
        conf_n += 1
        if (h["result"] == "win" and h["confidence"] >= 65) or \
           (h["result"] == "loss" and h["confidence"] < 65):
            conf_ok += 1
    if conf_n:
        lines.append("")
        lines.append(f"Confidence accuracy: {100*conf_ok/conf_n:.0f}% (n={conf_n})")
        lines.append(f"Calibration scalar: {state.get('confidence_calibration', 1):.3f}")

    # Learning insights
    lines.append("")
    lines.append("<b>Adaptive learning:</b>")
    best_eng = max(ENGINES, key=lambda e: state["engine_weights"].get(e, 1.0))
    worst_eng = min(ENGINES, key=lambda e: state["engine_weights"].get(e, 1.0))
    lines.append(f"  Best weight: {best_eng} ({state['engine_weights'][best_eng]:.2f})")
    lines.append(f"  Softest weight: {worst_eng} ({state['engine_weights'][worst_eng]:.2f})")
    gov = state["governor"]
    lines.append(f"  Governor threshold: {gov['threshold']:.1f} | "
                 f"daily EMA: {gov['daily_count_ema']:.1f}")

    # Recommendations
    lines.append("")
    lines.append("<b>Recommendations:</b>")
    if wr < 45 and decided >= 5:
        lines.append("  • Raise quality bar — WR soft; governor will tighten")
    if gov["daily_count_ema"] < TARGET_SIGNALS_MIN:
        lines.append("  • Frequency below band — threshold easing gradually")
    if gov["daily_count_ema"] > TARGET_SIGNALS_MAX:
        lines.append("  • Frequency above band — threshold tightening")
    if not resolved:
        lines.append("  • No resolved trades in window — collect more samples")
    if decided >= 8 and pf >= 1.4:
        lines.append("  • Edge intact — maintain current ensemble balance")

    lines.append("")
    lines.append(time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()))
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict):
    now = time.gmtime()
    today = time.strftime("%Y-%m-%d", now)
    if now.tm_hour != DAILY_SUMMARY_UTC_HOUR:
        return
    if state.get("last_summary_date") == today:
        return
    summary = generate_daily_summary(state)
    if send_telegram(summary):
        state["last_summary_date"] = today
        state["last_summary_ts"] = int(time.time())

# ============================================================================
# SCAN / MAIN
# ============================================================================


def evaluate_symbol(symbol, bundle, state, btc_bias, btc_strength, breadth,
                    snapshot, threshold) -> Optional[dict]:
    regime = build_regime_vector(state, symbol, bundle, btc_bias, btc_strength, breadth, "intraday")
    combo_name = select_combo(regime)
    # Rebuild regime with selected combo for accurate ADX/noise
    regime = build_regime_vector(state, symbol, bundle, btc_bias, btc_strength, breadth, combo_name)
    local_thr = adaptive_threshold(regime, threshold)
    combo = COMBOS[combo_name]
    bar_index = bundle[combo["exec"]][-1]["t"] // TF_MS[combo["exec"]]
    market_price = snapshot.get(symbol, {}).get("mark") or bundle[combo["exec"]][-1]["c"]
    vp = volume_profile(bundle["1h"][-VOL_PROFILE_LOOKBACK:])
    book = None

    best = None  # (cand, conf, grade, ev)
    multi_engine_hits = []  # for confluence boost

    for builder in ENGINE_BUILDERS:
        try:
            cand = builder(symbol, bundle, combo_name, regime, vp)
        except Exception as e:
            log.debug("%s %s error: %s", symbol, builder.__name__, e)
            continue
        if cand is None:
            continue
        cand = clamp_to_market(cand, market_price)
        if not check_cooldown(state, symbol, cand.direction, bar_index):
            continue
        if is_recent_duplicate(state, symbol, cand.direction, cand.entry):
            continue
        atr_pct = safe(cand.atr_val / cand.entry, 0.0)
        ok, reason = passes_hard_filters(symbol, snapshot, atr_pct, cand)
        if not ok:
            log.debug("%s %s filtered: %s", symbol, cand.engine, reason)
            continue
        multi_engine_hits.append(cand)
        if book is None:
            book = analyze_orderbook(symbol)
        conf = score_candidate(cand, regime, state, btc_bias, book, vp)
        # Multi-engine confluence boost: same direction agreement
        same_dir = sum(1 for c in multi_engine_hits if c.direction == cand.direction)
        if same_dir >= 2:
            conf = min(99.0, conf + 3.5 * (same_dir - 1))
            cand.confluences.append(f"ensemble confluence x{same_dir}")
        if conf < local_thr:
            continue
        ev = expected_value(cand, conf, state)
        if ev < 0.05 and conf < local_thr + 8:
            continue  # reject negative-EV marginal setups
        grade = grade_for_confidence(conf)
        if best is None or conf > best[1] or (conf == best[1] and ev > best[3]):
            best = (cand, conf, grade, ev)

    if best is None:
        return None
    cand, conf, grade, ev = best
    return {
        "cand": cand, "confidence": conf, "grade": grade, "ev": ev,
        "bar_index": bar_index, "regime": regime,
    }


def count_open_same_direction(state, direction) -> int:
    return sum(1 for s in state["active_signals"] if s["direction"] == direction)


def count_open_for_symbol(state, symbol) -> int:
    return sum(1 for s in state["active_signals"] if s["symbol"] == symbol)


def _prefetch(symbol, candle_cache):
    return symbol, fetch_all_candles(symbol, candle_cache)


def run_scan():
    log.info("=== %s v%s scan starting ===", ENGINE_NAME, __version__)
    t0 = time.monotonic()
    state = load_state()
    candle_cache = load_candle_cache()
    snapshot = get_market_snapshot()
    reference_ms = int(time.time() * 1000)

    # Lifecycle first
    check_active_signals(state, snapshot)
    try:
        refine_resolution_with_candles(state, reference_ms)
    except Exception as e:
        log.warning("Candle refinement skipped: %s", e)

    symbols = ["BTC"] + [s for s in WATCHLIST if s != "BTC"]
    bundles: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=FETCH_THREAD_WORKERS) as pool:
        futs = {pool.submit(_prefetch, sym, candle_cache): sym for sym in symbols}
        for fut in as_completed(futs):
            sym, bundle = fut.result()
            if bundle:
                bundles[sym] = bundle
            else:
                log.info("No bundle for %s", sym)
    save_candle_cache(candle_cache)
    log.info("Prefetched %d/%d in %.1fs", len(bundles), len(symbols), time.monotonic() - t0)

    btc_bundle = bundles.get("BTC")
    if not btc_bundle:
        log.error("BTC bundle missing — abort")
        save_state(state)
        return
    btc_bias, btc_strength = compute_btc_regime(btc_bundle)
    breadth = compute_breadth(bundles, btc_bias)
    log.info("BTC %s ADX=%.1f | breadth=%.0f%% | thr=%.1f",
             btc_bias, btc_strength, breadth * 100, state["governor"]["threshold"])

    tune_engine_weights(state)

    fired = []
    threshold = state["governor"]["threshold"]
    for symbol in WATCHLIST:
        if symbol not in bundles:
            continue
        if count_open_for_symbol(state, symbol) >= MAX_CONCURRENT_PER_SYMBOL:
            continue
        try:
            result = evaluate_symbol(symbol, bundles[symbol], state, btc_bias,
                                     btc_strength, breadth, snapshot, threshold)
        except Exception as e:
            log.exception("Eval error %s: %s", symbol, e)
            continue
        if result:
            fired.append(result)

    # Rank by confidence then EV
    fired.sort(key=lambda r: (r["confidence"], r["ev"]), reverse=True)

    # Correlation dedup
    if len(fired) > 1:
        corr_b = {r["cand"].symbol: bundles[r["cand"].symbol] for r in fired if r["cand"].symbol in bundles}
        clusters = build_correlation_clusters(corr_b)
        ranked = [{"symbol": r["cand"].symbol, "direction": r["cand"].direction,
                   "score": r["confidence"] + r["ev"] * 5, "ref": r} for r in fired]
        kept = dedup_correlated(ranked, clusters)
        kept_ids = {id(k["ref"]) for k in kept}
        fired = [r for r in fired if id(r) in kept_ids]

    # Cap per scan, quality first
    sent = 0
    for r in fired:
        if sent >= MAX_SIGNALS_PER_SCAN:
            break
        if count_open_same_direction(state, r["cand"].direction) >= MAX_CONCURRENT_SAME_DIRECTION:
            log.info("Skip %s: direction cap", r["cand"].symbol)
            continue
        text = format_signal(r["cand"], r["confidence"], r["grade"], r["ev"])
        msg_id = send_telegram(text)
        record_signal(state, r["cand"], r["confidence"], r["grade"],
                      r["bar_index"], msg_id, r["ev"])
        sent += 1
        log.info("FIRE %s %s eng=%s conf=%.1f grade=%s EV=%+.2f",
                 r["cand"].symbol, r["cand"].direction, r["cand"].engine,
                 r["confidence"], r["grade"], r["ev"])

    maybe_send_daily_summary(state)
    governor_adjust(state, estimate_signals_24h(state))
    prune_state(state)
    save_state(state)
    log.info("=== Scan done: %d fired, thr=%.1f, %.1fs ===",
             sent, state["governor"]["threshold"], time.monotonic() - t0)


def main():
    try:
        run_scan()
    except Exception as e:
        log.exception("Fatal: %s", e)
        try:
            send_telegram(f"🚨 {ENGINE_NAME} crashed: {e}")
        except Exception:
            pass
        raise SystemExit(1)


if __name__ == "__main__":
    main()
