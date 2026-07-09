#!/usr/bin/env python3
"""
APEX ENGINE v1.0.0
==================

WHAT THIS IS
------------
A scan-per-run signal engine: an external scheduler (cron-job.org, GitHub
Actions cron, systemd timer) invokes this script on a fixed cadence
(15 min is the reference cadence). Each run:

  1. Loads state.json (positions, engine stats, cooldowns, regime memory).
  2. Pulls fresh candles from the Hyperliquid public API for the watchlist.
  3. Runs a panel of specialized "sub-engines" (trend continuation, SMC
     liquidity reversal, momentum breakout, mean reversion / range,
     volatility expansion) that each independently propose candidate
     trades.
  4. Feeds every candidate through one centralized Decision Engine that
     scores confluence, regime fit, and expected value, then decides what
     gets published.
  5. Publishes accepted signals to Telegram, tracks open signals against
     fresh price action every run (TP1/TP2/SL/BE/close), and posts a daily
     performance summary at 08:00 UTC.
  6. Writes updated state.json (which includes a slowly-adapting weight
     per sub-engine, derived from that engine's own realized win rate /
     expected value -- this is the "continuous learning" loop).

HONEST SCOPE NOTE
------------------
This file implements a complete, coherent architecture end to end: real
Hyperliquid market data, real SMC structure detection (BOS/CHoCH, order
blocks, breaker blocks, FVGs, liquidity sweeps, premium/discount), a real
multi-engine ensemble with adaptive weighting, real structure-based
risk management, and a full Telegram trade-lifecycle + daily-summary
integration. What it does NOT and cannot do is *guarantee* any particular
win rate, signal count, or profitability -- those depend on the specific
market, the watchlist, execution costs, and how the adaptive parameters
settle over live/paper-traded history. Treat the numeric targets in
comments (e.g. "5-10 signals/day") as *design targets* the governor steers
toward, not promised outcomes. Paper-trade and inspect signal_history in
state.json before risking capital on it.

Single file, immediately runnable:

    python3 apex_engine_v1_0_0.py

Configure via environment variables (see CONFIGURATION below).
"""

from __future__ import annotations

import collections
import json
import logging
import math
import os
import signal
import statistics
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

# ============================================================================
# CONFIGURATION
# ============================================================================

ENGINE_NAME = "APEX ENGINE"
ENGINE_VERSION = "v1.0.0"

HL_API_URL = "https://api.hyperliquid.xyz/info"
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

STATE_PATH = os.environ.get("APEX_STATE_PATH", "state.json")
LOG_PATH = os.environ.get("APEX_LOG_PATH", "apex_engine.log")
CANDLE_CACHE_PATH = os.environ.get("APEX_CANDLE_CACHE_PATH", "candle_cache.json")
CANDLE_DELTA_OVERLAP_BARS = 3

WATCHLIST = [
    "BTC", "ETH", "HYPE", "SOL", "BNB", "XRP", "DOGE", "ADA", "SUI", "NEAR",
    "AVAX", "LINK", "DOT", "TRX", "BCH", "LTC", "AAVE", "UNI", "ONDO", "TAO",
    "APT", "PENDLE", "XLM", "ZEC", "PENGU",
]

TF_MS = {
    "5m": 5 * 60_000, "15m": 15 * 60_000, "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000, "1d": 24 * 60 * 60_000,
}
CANDLE_COUNT = {"5m": 300, "15m": 300, "1h": 300, "4h": 260, "1d": 220}

RSI_LEN = 14
ATR_LEN = 14
ADX_LEN = 14
BB_LEN = 20
BB_MULT = 2.0

# Frequency governor target band (design target, not a guarantee -- see
# HONEST SCOPE NOTE above).
TARGET_SIGNALS_PER_DAY_LOW = 5.0
TARGET_SIGNALS_PER_DAY_HIGH = 10.0
MAX_OPEN_SIGNALS = 12
MAX_OPEN_PER_SYMBOL = 1
MAX_OPEN_SAME_DIRECTION = 8

HL_WEIGHT_BUDGET_PER_MINUTE = float(os.environ.get("APEX_HL_WEIGHT_BUDGET", "1100"))
HL_DEFAULT_INFO_WEIGHT = 20
HL_ENDPOINT_BASE_WEIGHT = {"metaAndAssetCtxs": 20, "l2Book": 2}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger(ENGINE_NAME)


def _handle_shutdown(sig_num, frame):
    log.warning("Received shutdown signal %s, exiting cleanly.", sig_num)
    raise SystemExit(0)


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


# ============================================================================
# SECTION 1 -- HYPERLIQUID API
# ============================================================================

def hl_coin(symbol: str) -> str:
    return symbol.upper()


class _WeightRateLimiter:
    """Sliding-60s-window pacer for aggregate Hyperliquid request weight,
    shared across the thread pool used to fetch the watchlist concurrently."""

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


def hl_post(payload: dict, retries: int = 4, timeout: int = 12) -> dict | list | None:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(HL_API_URL, data=body, headers={"Content-Type": "application/json"})
    weight = _request_weight(payload)
    for attempt in range(retries):
        _rate_limiter.wait(weight)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = e.headers.get("Retry-After")
                wait_s = float(retry_after) if retry_after else 10.0
                log.warning("hl_post 429 (attempt %d, type=%s), backing off %.1fs",
                            attempt + 1, payload.get("type"), wait_s)
                time.sleep(wait_s)
            else:
                log.warning("hl_post HTTP error attempt %d (%s): %s", attempt + 1, payload.get("type"), e)
                time.sleep(0.8 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            log.warning("hl_post attempt %d failed (%s): %s", attempt + 1, payload.get("type"), e)
            time.sleep(0.8 * (attempt + 1))
    log.error("hl_post exhausted retries for type=%s", payload.get("type"))
    return None


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = TF_MS[interval]
    return (reference_ms // step) * step


def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]


def _request_candles(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[dict]:
    payload = {"type": "candleSnapshot", "req": {"coin": hl_coin(symbol), "interval": interval,
                                                   "startTime": start_ms, "endTime": end_ms}}
    raw = hl_post(payload)
    if not raw:
        return []
    return [{"t": c["t"], "o": float(c["o"]), "h": float(c["h"]),
              "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])} for c in raw]


def get_candles(symbol: str, interval: str, n: int, reference_ms: int | None = None,
                 cache_entry: list[dict] | None = None) -> list[dict]:
    reference_ms = reference_ms or int(time.time() * 1000)
    if cache_entry:
        step = TF_MS[interval]
        start_ms = cache_entry[-1]["t"] - step * CANDLE_DELTA_OVERLAP_BARS
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


def fetch_all_candles(symbol: str, candle_cache: dict[str, dict] | None = None,
                       reference_ms: int | None = None) -> dict[str, list[dict]] | None:
    bundle = {}
    sym_cache = (candle_cache or {}).get(symbol, {})
    for tf in ("15m", "1h", "4h", "1d"):
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
            day_vol = float(ctx.get("dayNtlVlm", 0) or 0)
            out[name] = {"mark": mark, "funding": funding, "oi_usd": oi_coins * mark, "day_vol_usd": day_vol}
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
# SECTION 2 -- INDICATORS
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
        out.append(out[-1] + k * (v - out[-1]))
    return out


def sma(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1):i + 1]
        out.append(sum(window) / len(window))
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1):i + 1]
        out.append(statistics.pstdev(window) if len(window) > 1 else 0.0)
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]
    avg_gain, avg_loss = sum(gains[:period]) / period, sum(losses[:period]) / period
    out = [50.0] * (period + 1)
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss > 1e-12 else 999.0
        out.append(100 - 100 / (1 + rs))
    while len(out) < len(closes):
        out.append(out[-1])
    return out[:len(closes)]


def atr(highs, lows, closes, period: int = ATR_LEN) -> list[float]:
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    out, run = [], trs[0]
    for i, tr in enumerate(trs):
        run = tr if i == 0 else (run * (period - 1) + tr) / period
        out.append(run)
    return out


def adx_dmi(highs, lows, closes, period: int = ADX_LEN) -> tuple[list[float], list[float], list[float]]:
    n = len(closes)
    plus_dm, minus_dm, trs = [0.0], [0.0], [highs[0] - lows[0]]
    for i in range(1, n):
        up, down = highs[i] - highs[i - 1], lows[i - 1] - lows[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))

    def wsmooth(vals):
        out, run = [], vals[0]
        for i, v in enumerate(vals):
            run = v if i == 0 else run - run / period + v
            out.append(run)
        return out

    tr_s, pdm_s, mdm_s = wsmooth(trs), wsmooth(plus_dm), wsmooth(minus_dm)
    plus_di = [100 * safe(p / t) for p, t in zip(pdm_s, tr_s)]
    minus_di = [100 * safe(m / t) for m, t in zip(mdm_s, tr_s)]
    dx = [100 * safe(abs(p - m) / (p + m)) for p, m in zip(plus_di, minus_di)]
    adx = ema(dx, period)
    return adx, plus_di, minus_di


def bollinger_width_pct(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT) -> list[float]:
    mid, sd = sma(closes, period), stdev(closes, period)
    return [safe((2 * mult * s) / m) for m, s in zip(mid, sd)]


def obv(closes, volumes) -> list[float]:
    out = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            out.append(out[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            out.append(out[-1] - volumes[i])
        else:
            out.append(out[-1])
    return out


def detect_rsi_divergence(closes: list[float], rsi_vals: list[float], lookback: int = 25) -> Optional[str]:
    if len(closes) < lookback + 5:
        return None
    window_c, window_r = closes[-lookback:], rsi_vals[-lookback:]

    def pivots_low(arr):
        return [i for i in range(2, len(arr) - 2) if arr[i] < arr[i - 1] and arr[i] < arr[i - 2]
                and arr[i] < arr[i + 1] and arr[i] < arr[i + 2]]

    def pivots_high(arr):
        return [i for i in range(2, len(arr) - 2) if arr[i] > arr[i - 1] and arr[i] > arr[i - 2]
                and arr[i] > arr[i + 1] and arr[i] > arr[i + 2]]

    lows = pivots_low(window_c)
    if len(lows) >= 2:
        i1, i2 = lows[-2], lows[-1]
        if window_c[i2] < window_c[i1] and window_r[i2] > window_r[i1]:
            return "bullish"
    highs = pivots_high(window_c)
    if len(highs) >= 2:
        i1, i2 = highs[-2], highs[-1]
        if window_c[i2] > window_c[i1] and window_r[i2] < window_r[i1]:
            return "bearish"
    return None


def daily_vwap(candles_15m: list[dict], reference_ms: int | None = None) -> Optional[float]:
    reference_ms = reference_ms or int(time.time() * 1000)
    day_start = (reference_ms // 86_400_000) * 86_400_000
    todays = [c for c in candles_15m if c["t"] >= day_start]
    if not todays:
        return None
    pv, vv = 0.0, 0.0
    for c in todays:
        typ = (c["h"] + c["l"] + c["c"]) / 3
        pv += typ * c["v"]
        vv += c["v"]
    return pv / vv if vv > 0 else None


def compute_indicators(candles: list[dict]) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c["v"] for c in candles]
    r = rsi(closes)
    a = atr(highs, lows, closes)
    adx, pdi, mdi = adx_dmi(highs, lows, closes)
    bw = bollinger_width_pct(closes)
    e20, e50, e200 = ema(closes, 20), ema(closes, 50), ema(closes, min(200, max(20, len(closes) - 1)))
    return {
        "closes": closes, "highs": highs, "lows": lows, "vols": vols,
        "rsi": r, "atr": a, "adx": adx, "plus_di": pdi, "minus_di": mdi,
        "bb_width": bw, "ema20": e20, "ema50": e50, "ema200": e200,
        "obv": obv(closes, vols), "rsi_div": detect_rsi_divergence(closes, r),
    }


# ============================================================================
# SECTION 3 -- STATE PERSISTENCE
# ============================================================================

def _default_state() -> dict:
    return {
        "version": ENGINE_VERSION,
        "open_signals": [],
        "signal_history": [],
        "cooldowns": {},
        "engine_stats": {},           # per sub-engine: {"wins":0,"losses":0,"total_r":0.0,"weight":1.0}
        "atr_pct_memory": {},         # per symbol: rolling ATR% samples for volatility percentile
        "last_summary_date": None,
        "governor": {"threshold_adj": 0.0, "daily_count_ema": 0.0},
        "bar_index": 0,
    }


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return _default_state()
    try:
        with open(STATE_PATH, "r") as f:
            state = json.load(f)
        base = _default_state()
        for k, v in base.items():
            state.setdefault(k, v)
        return state
    except (json.JSONDecodeError, OSError) as e:
        log.error("Failed to load state.json (%s); starting fresh.", e)
        return _default_state()


def save_state(state: dict):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_PATH)


def load_candle_cache() -> dict[str, dict]:
    if not os.path.exists(CANDLE_CACHE_PATH):
        return {}
    try:
        with open(CANDLE_CACHE_PATH, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_candle_cache(cache: dict[str, dict]):
    tmp = CANDLE_CACHE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f)
    os.replace(tmp, CANDLE_CACHE_PATH)


def prune_state(state: dict, max_signals: int = 1000, max_days: int = 30):
    cutoff = time.time() - max_days * 86400
    state["signal_history"] = [h for h in state["signal_history"] if h.get("ts", 0) >= cutoff][-max_signals:]
    for sym in list(state["atr_pct_memory"].keys()):
        state["atr_pct_memory"][sym] = state["atr_pct_memory"][sym][-300:]


# ============================================================================
# SECTION 4 -- MARKET REGIME
# ============================================================================

@dataclass
class RegimeVector:
    btc_bias: str            # "bull" | "bear" | "neutral"
    btc_strength: float      # 0..1
    vol_pctile: float        # symbol's own ATR% percentile vs its history, 0..1
    adx_strength: float      # normalized 0..1
    session_weight: float    # 0..1 liquidity-session multiplier
    noise_index: float       # 0..1, higher = choppier / more whipsaw-prone
    breadth: float           # fraction of watchlist agreeing with btc_bias, 0..1
    label: str                # "trend" | "range" | "volatile" | "quiet"


def session_weight_now() -> float:
    hour = time.gmtime().tm_hour
    # London/NY overlap and NY session carry the most genuine liquidity;
    # the low-liquidity Asia-only window (roughly 00:00-06:00 UTC) is
    # discounted since spreads widen and stop hunts get cheaper there.
    if 12 <= hour < 20:
        return 1.0
    if 6 <= hour < 12 or 20 <= hour < 24:
        return 0.85
    return 0.6


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state["atr_pct_memory"].setdefault(symbol, [])
    mem.append(atr_pct)
    if len(mem) > 300:
        del mem[0]
    if len(mem) < 20:
        return 0.5
    rank = sum(1 for x in mem if x <= atr_pct) / len(mem)
    return rank


def compute_btc_regime(btc_bundle: dict) -> tuple[str, float]:
    ind_1h = compute_indicators(btc_bundle["1h"])
    ind_4h = compute_indicators(btc_bundle["4h"])
    c = ind_1h["closes"][-1]
    e50_1h, e200_1h = ind_1h["ema50"][-1], ind_1h["ema200"][-1]
    e50_4h = ind_4h["ema50"][-1]
    adx_1h = ind_1h["adx"][-1]
    score = 0.0
    score += 1.0 if c > e50_1h else -1.0
    score += 1.0 if c > e200_1h else -1.0
    score += 1.0 if ind_4h["closes"][-1] > e50_4h else -1.0
    strength = min(1.0, adx_1h / 40.0)
    if score >= 2:
        return "bull", strength
    if score <= -2:
        return "bear", strength
    return "neutral", strength * 0.5


def compute_noise_index(candles: list[dict], lookback: int = 30) -> float:
    recent = candles[-lookback:]
    if len(recent) < 5:
        return 0.5
    net_move = abs(recent[-1]["c"] - recent[0]["c"])
    path_len = sum(abs(recent[i]["c"] - recent[i - 1]["c"]) for i in range(1, len(recent)))
    if path_len < 1e-9:
        return 0.5
    efficiency = net_move / path_len   # 1.0 = pure trend, ->0 = pure chop
    return max(0.0, min(1.0, 1.0 - efficiency))


def symbol_bias_from_bundle(bundle: dict) -> Optional[str]:
    ind = compute_indicators(bundle["1h"])
    c, e50 = ind["closes"][-1], ind["ema50"][-1]
    if e50 <= 0:
        return None
    return "bull" if c > e50 else "bear"


def compute_breadth(bundles: dict[str, dict], btc_bias: str) -> float:
    if btc_bias == "neutral" or not bundles:
        return 0.5
    agree = 0
    total = 0
    for sym, b in bundles.items():
        bias = symbol_bias_from_bundle(b)
        if bias is None:
            continue
        total += 1
        if bias == btc_bias:
            agree += 1
    return agree / total if total else 0.5


def build_regime_vector(state: dict, symbol: str, bundle: dict, btc_bias: str,
                         btc_strength: float, breadth: float) -> RegimeVector:
    ind_1h = compute_indicators(bundle["1h"])
    atr_pct = safe(ind_1h["atr"][-1] / ind_1h["closes"][-1]) if ind_1h["closes"][-1] else 0.0
    vol_pctile = update_atr_pct_memory(state, symbol, atr_pct)
    adx_strength = min(1.0, ind_1h["adx"][-1] / 40.0)
    noise = compute_noise_index(bundle["15m"])
    sess = session_weight_now()

    if adx_strength > 0.5 and noise < 0.55:
        label = "trend"
    elif vol_pctile > 0.8:
        label = "volatile"
    elif adx_strength < 0.3 and noise > 0.55:
        label = "range"
    else:
        label = "quiet"

    return RegimeVector(btc_bias, btc_strength, vol_pctile, adx_strength, sess, noise, breadth, label)


# ============================================================================
# SECTION 5 -- SMART MONEY CONCEPTS / STRUCTURE
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
    bias: str          # "bull" | "bear" | "neutral"
    last_bos: Optional[str]
    last_choch: Optional[str]
    last_high: Optional[float]
    last_low: Optional[float]


def analyze_structure(candles: list[dict], swings: list[Swing]) -> StructureState:
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return StructureState("neutral", None, None, None, None)
    hh = highs[-1].price > highs[-2].price
    hl = lows[-1].price > lows[-2].price
    lh = highs[-1].price < highs[-2].price
    ll = lows[-1].price < lows[-2].price
    close = candles[-1]["c"]
    bias, bos, choch = "neutral", None, None
    if hh and hl:
        bias = "bull"
    elif lh and ll:
        bias = "bear"
    if close > highs[-1].price:
        bos = "bull"
    elif close < lows[-1].price:
        bos = "bear"
    if bias == "bear" and close > highs[-1].price:
        choch = "bull"
    elif bias == "bull" and close < lows[-1].price:
        choch = "bear"
    return StructureState(bias, bos, choch, highs[-1].price, lows[-1].price)


@dataclass
class Zone:
    kind: str      # "order_block" | "breaker" | "fvg"
    direction: str  # "bull" | "bear"
    top: float
    bottom: float
    idx: int
    tested: bool = False


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int = 60) -> list[Zone]:
    zones = []
    n = len(candles)
    start = max(1, n - lookback)
    for i in range(start, n - 1):
        c = candles[i]
        nxt = candles[i + 1]
        body = abs(c["c"] - c["o"])
        if body < 0.3 * atr_vals[i]:
            continue
        # Bullish OB: last down-candle before a strong up-move that clears its high.
        if c["c"] < c["o"] and nxt["c"] > c["h"]:
            zones.append(Zone("order_block", "bull", c["h"], c["l"], i))
        # Bearish OB: last up-candle before a strong down-move that clears its low.
        if c["c"] > c["o"] and nxt["c"] < c["l"]:
            zones.append(Zone("order_block", "bear", c["h"], c["l"], i))
    return zones[-12:]


def find_fvgs(candles: list[dict], lookback: int = 60) -> list[Zone]:
    zones = []
    n = len(candles)
    start = max(2, n - lookback)
    for i in range(start, n):
        a, c = candles[i - 2], candles[i]
        if c["l"] > a["h"]:
            zones.append(Zone("fvg", "bull", c["l"], a["h"], i))
        if c["h"] < a["l"]:
            zones.append(Zone("fvg", "bear", a["l"], c["h"], i))
    return zones[-12:]


def find_breaker_blocks(candles: list[dict], swings: list[Swing], structure: StructureState) -> list[Zone]:
    """A breaker is a failed order block: the swing that got swept then
    reversed, leaving the sweeping candle's range as a fresh S/R zone."""
    zones = []
    lows = [s for s in swings if s.kind == "low"]
    highs = [s for s in swings if s.kind == "high"]
    if structure.last_choch == "bull" and lows:
        piv = lows[-1]
        seg = candles[max(0, piv.idx - 1):piv.idx + 2]
        if seg:
            zones.append(Zone("breaker", "bull", max(c["h"] for c in seg), piv.price, piv.idx))
    if structure.last_choch == "bear" and highs:
        piv = highs[-1]
        seg = candles[max(0, piv.idx - 1):piv.idx + 2]
        if seg:
            zones.append(Zone("breaker", "bear", piv.price, min(c["l"] for c in seg), piv.idx))
    return zones


def mark_untested(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    for z in zones:
        for c in candles[z.idx + 1:]:
            if c["l"] <= z.top and c["h"] >= z.bottom:
                z.tested = True
                break
    return zones


def cluster_levels(levels: list[float], tol_pct: float = 0.0015) -> list[tuple[float, int]]:
    if not levels:
        return []
    levels = sorted(levels)
    clusters = [[levels[0]]]
    for lv in levels[1:]:
        if abs(lv - clusters[-1][-1]) / clusters[-1][-1] <= tol_pct:
            clusters[-1].append(lv)
        else:
            clusters.append([lv])
    return [(sum(c) / len(c), len(c)) for c in clusters]


def build_liquidity_pools(swings: list[Swing]) -> dict:
    highs = cluster_levels([s.price for s in swings if s.kind == "high"])
    lows = cluster_levels([s.price for s in swings if s.kind == "low"])
    return {"buy_side": sorted(highs, key=lambda x: -x[1]), "sell_side": sorted(lows, key=lambda x: -x[1])}


def detect_sweep(candles: list[dict], pools: dict, direction: str, lookback: int = 10) -> Optional[dict]:
    """direction 'bull' looks for a sell-side liquidity sweep (wick below a
    pool that closes back above it) as the trigger for a long; 'bear' the
    mirror on buy-side pools."""
    recent = candles[-lookback:]
    targets = pools["sell_side"] if direction == "bull" else pools["buy_side"]
    for level, weight in targets:
        for c in recent:
            swept = (c["l"] < level and c["c"] > level) if direction == "bull" else (c["h"] > level and c["c"] < level)
            if swept:
                return {"level": level, "weight": weight, "wick_low": c["l"], "wick_high": c["h"]}
    return None


def premium_discount_zone(candles: list[dict], lookback: int = 50) -> dict:
    recent = candles[-lookback:]
    hi, lo = max(c["h"] for c in recent), min(c["l"] for c in recent)
    mid = (hi + lo) / 2
    close = candles[-1]["c"]
    zone = "premium" if close > mid else "discount"
    depth = safe((close - mid) / (hi - mid)) if close >= mid and hi > mid else safe((mid - close) / (mid - lo)) if mid > lo else 0.0
    return {"zone": zone, "high": hi, "low": lo, "mid": mid, "depth": max(0.0, min(1.0, depth))}


def volume_profile(candles: list[dict], bins: int = 24) -> dict:
    recent = candles[-120:] if len(candles) > 120 else candles
    if not recent:
        return {"poc": None, "vah": None, "val": None}
    hi, lo = max(c["h"] for c in recent), min(c["l"] for c in recent)
    if hi <= lo:
        return {"poc": None, "vah": None, "val": None}
    width = (hi - lo) / bins
    vol_bins = [0.0] * bins
    for c in recent:
        typ = (c["h"] + c["l"] + c["c"]) / 3
        b = min(bins - 1, max(0, int((typ - lo) / width)))
        vol_bins[b] += c["v"]
    total = sum(vol_bins)
    poc_i = vol_bins.index(max(vol_bins))
    poc = lo + width * (poc_i + 0.5)
    target = total * 0.7
    lo_i = hi_i = poc_i
    acc = vol_bins[poc_i]
    while acc < target and (lo_i > 0 or hi_i < bins - 1):
        expand_lo = vol_bins[lo_i - 1] if lo_i > 0 else -1
        expand_hi = vol_bins[hi_i + 1] if hi_i < bins - 1 else -1
        if expand_hi >= expand_lo:
            hi_i = min(bins - 1, hi_i + 1)
            acc += max(expand_hi, 0)
        else:
            lo_i = max(0, lo_i - 1)
            acc += max(expand_lo, 0)
        if lo_i == 0 and hi_i == bins - 1:
            break
    return {"poc": poc, "vah": lo + width * (hi_i + 1), "val": lo + width * lo_i}


# ============================================================================
# SECTION 6 -- CANDIDATE / TRADE PLAN
# ============================================================================

@dataclass
class Candidate:
    symbol: str
    engine: str            # which sub-engine proposed it
    direction: str          # "long" | "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    reasons: list[str] = field(default_factory=list)
    raw_score: float = 0.0
    timeframe: str = "15m"
    combo: str = ""


def _rr(entry: float, sl: float, tp: float) -> float:
    risk = abs(entry - sl)
    return safe((abs(tp - entry)) / risk) if risk > 1e-12 else 0.0


def adaptive_sl_buffer(candles: list[dict], atr_val: float, vol_pctile: float) -> float:
    """SL buffer beyond structure, wider when volatility percentile is high
    so stops survive normal wick-hunt noise instead of getting clipped."""
    base = 0.35 * atr_val
    return base * (1.0 + 0.6 * vol_pctile)


def clamp_candidate_to_market(cand: Candidate, market_price: float) -> Candidate:
    """If price has already run past the intended entry since the bar
    closed, snap entry to current market rather than publishing a stale
    limit that will never fill favorably."""
    if cand.direction == "long" and market_price > cand.entry:
        cand.entry = market_price
    elif cand.direction == "short" and market_price < cand.entry:
        cand.entry = market_price
    return cand


def _clip_tp_to_liquidity(entry: float, tp: float, direction: str, pools: dict, vprof: dict) -> float:
    candidates = [tp]
    targets = pools["buy_side"] if direction == "long" else pools["sell_side"]
    for level, _w in targets:
        if (direction == "long" and entry < level < tp) or (direction == "short" and tp < level < entry):
            candidates.append(level * (0.999 if direction == "long" else 1.001))
    for key in ("vah", "val", "poc"):
        lv = vprof.get(key)
        if lv and ((direction == "long" and entry < lv < tp) or (direction == "short" and tp < lv < entry)):
            candidates.append(lv)
    return min(candidates) if direction == "long" else max(candidates)


# ---- Sub-engine 1: SMC Liquidity Reversal (sweep + MSS + OB/breaker entry) -

def engine_liquidity_reversal(symbol: str, bundle: dict, regime: RegimeVector) -> Optional[Candidate]:
    htf = bundle["4h"]
    exe = bundle["15m"]
    ind_htf = compute_indicators(htf)
    swings_htf = find_swings(htf)
    structure_htf = analyze_structure(htf, swings_htf)
    pools = build_liquidity_pools(swings_htf)
    pd = premium_discount_zone(htf)

    direction = None
    if structure_htf.bias == "bull" and pd["zone"] == "discount":
        direction = "bull"
    elif structure_htf.bias == "bear" and pd["zone"] == "premium":
        direction = "bear"
    if direction is None:
        return None

    sweep = detect_sweep(exe, pools, direction, lookback=12)
    if not sweep:
        return None

    swings_exe = find_swings(exe)
    structure_exe = analyze_structure(exe, swings_exe)
    if direction == "bull" and structure_exe.last_choch != "bull" and structure_exe.last_bos != "bull":
        return None
    if direction == "bear" and structure_exe.last_choch != "bear" and structure_exe.last_bos != "bear":
        return None

    atr_vals = atr([c["h"] for c in exe], [c["l"] for c in exe], [c["c"] for c in exe])
    obs = mark_untested(find_order_blocks(exe, atr_vals), exe)
    breakers = find_breaker_blocks(exe, swings_exe, structure_exe)
    zone_candidates = [z for z in obs + breakers if z.direction == ("bull" if direction == "bull" else "bear") and not z.tested]

    entry_price = exe[-1]["c"]
    poi = zone_candidates[-1] if zone_candidates else None
    if poi:
        entry_price = (poi.top + poi.bottom) / 2

    a = atr_vals[-1]
    buf = adaptive_sl_buffer(exe, a, regime.vol_pctile)
    if direction == "bull":
        sl = min(sweep["wick_low"], poi.bottom if poi else sweep["wick_low"]) - buf
        risk = entry_price - sl
        tp1, tp2 = entry_price + 1.5 * risk, entry_price + 2.8 * risk
    else:
        sl = max(sweep["wick_high"], poi.top if poi else sweep["wick_high"]) + buf
        risk = sl - entry_price
        tp1, tp2 = entry_price - 1.5 * risk, entry_price - 2.8 * risk

    vprof = volume_profile(htf)
    tp2 = _clip_tp_to_liquidity(entry_price, tp2, "long" if direction == "bull" else "short", pools, vprof)
    if risk <= 0 or _rr(entry_price, sl, tp2) < 1.4:
        return None

    reasons = [f"HTF bias {structure_htf.bias}, price in {pd['zone']} zone",
               f"Liquidity sweep at {sweep['level']:.5g} with reversal close",
               f"Execution-TF {structure_exe.last_choch or structure_exe.last_bos} confirms"]
    if poi:
        reasons.append(f"Entry at untested {poi.kind.replace('_', ' ')}")

    return Candidate(symbol, "liquidity_reversal", "long" if direction == "bull" else "short",
                      entry_price, sl, tp1, tp2, reasons, timeframe="15m", combo="4h/15m")


# ---- Sub-engine 2: Trend Continuation (pullback into HTF-aligned zone) ----

def engine_trend_continuation(symbol: str, bundle: dict, regime: RegimeVector) -> Optional[Candidate]:
    if regime.label not in ("trend", "quiet"):
        return None
    htf, exe = bundle["1h"], bundle["15m"]
    ind_htf = compute_indicators(htf)
    ind_exe = compute_indicators(exe)
    if ind_htf["adx"][-1] < 18:
        return None
    trend_up = ind_htf["closes"][-1] > ind_htf["ema50"][-1] > ind_htf["ema200"][-1]
    trend_dn = ind_htf["closes"][-1] < ind_htf["ema50"][-1] < ind_htf["ema200"][-1]
    if not (trend_up or trend_dn):
        return None
    direction = "long" if trend_up else "short"

    price = exe["closes"][-1]
    e20, e50 = ind_exe["ema20"][-1], ind_exe["ema50"][-1]
    near_pullback = abs(price - e20) / price < 0.006 or abs(price - e50) / price < 0.01
    rsi_ok = (35 < ind_exe["rsi"][-1] < 60) if direction == "long" else (40 < ind_exe["rsi"][-1] < 65)
    if not (near_pullback and rsi_ok):
        return None

    a = ind_exe["atr"][-1]
    buf = adaptive_sl_buffer(bundle["15m"], a, regime.vol_pctile)
    swings = find_swings(bundle["15m"])
    recent_low = min((s.price for s in swings if s.kind == "low"), default=price - 2 * a)
    recent_high = max((s.price for s in swings if s.kind == "high"), default=price + 2 * a)

    if direction == "long":
        sl = min(recent_low, price - 1.2 * a) - buf
        risk = price - sl
        tp1, tp2 = price + 1.3 * risk, price + 2.5 * risk
    else:
        sl = max(recent_high, price + 1.2 * a) + buf
        risk = sl - price
        tp1, tp2 = price - 1.3 * risk, price - 2.5 * risk

    if risk <= 0 or _rr(price, sl, tp2) < 1.3:
        return None

    reasons = [f"1H trend {'up' if trend_up else 'down'} (ADX {ind_htf['adx'][-1]:.0f}), price pulled back to EMA20/50",
               f"15m RSI {ind_exe['rsi'][-1]:.0f} resetting without trend loss"]
    return Candidate(symbol, "trend_continuation", direction, price, sl, tp1, tp2, reasons,
                      timeframe="15m", combo="1h/15m")


# ---- Sub-engine 3: Momentum Breakout (range/consolidation expansion) ----

def engine_momentum_breakout(symbol: str, bundle: dict, regime: RegimeVector) -> Optional[Candidate]:
    exe = bundle["15m"]
    ind = compute_indicators(exe)
    bw = ind["bb_width"][-6:]
    if len(bw) < 6 or bw[-1] <= min(bw[:-1]):
        squeeze = False
    else:
        squeeze = bw[-2] <= sorted(bw[:-1])[0] * 1.15
    if not squeeze:
        return None

    price = exe[-1]["c"]
    highs20 = [c["h"] for c in exe[-21:-1]]
    lows20 = [c["l"] for c in exe[-21:-1]]
    range_hi, range_lo = max(highs20), min(lows20)
    vol_avg = sum(ind["vols"][-21:-1]) / 20
    vol_now = ind["vols"][-1]
    direction = None
    if price > range_hi and vol_now > 1.4 * vol_avg:
        direction = "long"
    elif price < range_lo and vol_now > 1.4 * vol_avg:
        direction = "short"
    if direction is None:
        return None

    a = ind["atr"][-1]
    buf = adaptive_sl_buffer(exe, a, regime.vol_pctile)
    if direction == "long":
        sl = range_hi - buf - 0.3 * a
        risk = price - sl
        tp1, tp2 = price + 1.4 * risk, price + 2.6 * risk
    else:
        sl = range_lo + buf + 0.3 * a
        risk = sl - price
        tp1, tp2 = price - 1.4 * risk, price - 2.6 * risk

    if risk <= 0 or _rr(price, sl, tp2) < 1.3:
        return None

    reasons = [f"Bollinger-width squeeze released with breakout close beyond {range_hi if direction=='long' else range_lo:.5g}",
               f"Volume {vol_now/vol_avg:.1f}x 20-bar average confirms expansion"]
    return Candidate(symbol, "momentum_breakout", direction, price, sl, tp1, tp2, reasons,
                      timeframe="15m", combo="15m")


# ---- Sub-engine 4: Mean Reversion / Range Fade -----------------------------

def engine_mean_reversion(symbol: str, bundle: dict, regime: RegimeVector) -> Optional[Candidate]:
    if regime.label != "range":
        return None
    exe = bundle["1h"]
    ind = compute_indicators(exe)
    price = exe[-1]["c"]
    sd = stdev(ind["closes"], BB_LEN)[-1]
    mid = sma(ind["closes"], BB_LEN)[-1]
    upper, lower = mid + BB_MULT * sd, mid - BB_MULT * sd
    if sd <= 0:
        return None

    direction = None
    if price <= lower and ind["rsi"][-1] < 32:
        direction = "long"
    elif price >= upper and ind["rsi"][-1] > 68:
        direction = "short"
    if direction is None:
        return None

    a = ind["atr"][-1]
    buf = adaptive_sl_buffer(exe, a, regime.vol_pctile)
    if direction == "long":
        sl = min(price - 1.1 * a, lower - 0.5 * a) - buf * 0.5
        risk = price - sl
        tp1, tp2 = mid, upper
    else:
        sl = max(price + 1.1 * a, upper + 0.5 * a) + buf * 0.5
        risk = sl - price
        tp1, tp2 = mid, lower

    if risk <= 0 or _rr(price, sl, tp2) < 1.1:
        return None

    reasons = [f"Price at Bollinger extreme (RSI {ind['rsi'][-1]:.0f}) inside a ranging 1H regime",
               "Target the mean before value-area edge, not a fixed R-multiple"]
    return Candidate(symbol, "mean_reversion", direction, price, sl, tp1, tp2, reasons,
                      timeframe="1h", combo="1h")


# ---- Sub-engine 5: Volatility Expansion (post-compression directional) ----

def engine_volatility_expansion(symbol: str, bundle: dict, regime: RegimeVector) -> Optional[Candidate]:
    if regime.vol_pctile > 0.35:
        return None  # only fires out of unusually quiet conditions
    exe = bundle["1h"]
    ind = compute_indicators(exe)
    adx_now, adx_prev = ind["adx"][-1], ind["adx"][-6]
    if adx_now < 20 or adx_now <= adx_prev:
        return None
    direction = "long" if ind["plus_di"][-1] > ind["minus_di"][-1] else "short"
    price = exe[-1]["c"]
    a = ind["atr"][-1]
    buf = adaptive_sl_buffer(exe, a, regime.vol_pctile)
    swings = find_swings(exe)
    if direction == "long":
        recent_low = min((s.price for s in swings[-6:] if s.kind == "low"), default=price - 2 * a)
        sl = recent_low - buf
        risk = price - sl
        tp1, tp2 = price + 1.6 * risk, price + 3.0 * risk
    else:
        recent_high = max((s.price for s in swings[-6:] if s.kind == "high"), default=price + 2 * a)
        sl = recent_high + buf
        risk = sl - price
        tp1, tp2 = price - 1.6 * risk, price - 3.0 * risk
    if risk <= 0 or _rr(price, sl, tp2) < 1.4:
        return None
    reasons = [f"ATR percentile {regime.vol_pctile:.0%} (compressed) with rising ADX ({adx_prev:.0f}->{adx_now:.0f})",
               "Directional DI cross signals expansion out of compression"]
    return Candidate(symbol, "volatility_expansion", direction, price, sl, tp1, tp2, reasons,
                      timeframe="1h", combo="1h")


SUB_ENGINES = [
    engine_liquidity_reversal,
    engine_trend_continuation,
    engine_momentum_breakout,
    engine_mean_reversion,
    engine_volatility_expansion,
]


# ============================================================================
# SECTION 7 -- DECISION ENGINE (ensemble scoring + adaptive weighting)
# ============================================================================

def logistic(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def get_engine_weight(state: dict, engine_name: str) -> float:
    stats = state["engine_stats"].get(engine_name)
    if not stats or (stats["wins"] + stats["losses"]) < 15:
        return 1.0  # neutral prior until statistically meaningful
    n = stats["wins"] + stats["losses"]
    wr = stats["wins"] / n
    avg_r = stats["total_r"] / n
    # Shrink toward neutral (1.0) proportional to sample size, so a short
    # hot/cold streak can't dominate; converges toward realized edge as
    # n grows, capped to a sane band to avoid the ensemble collapsing
    # onto a single engine.
    shrink = min(1.0, n / 80.0)
    raw = 0.85 + 0.5 * logistic(3 * avg_r) + 0.3 * logistic(6 * (wr - 0.5))
    weight = 1.0 * (1 - shrink) + raw * shrink
    return max(0.5, min(1.6, weight))


def score_candidate(cand: Candidate, regime: RegimeVector, state: dict, breadth: float) -> float:
    score = 0.0
    rr = _rr(cand.entry, cand.sl, cand.tp2)
    score += min(2.0, rr / 1.5)

    dir_bias = "bull" if cand.direction == "long" else "bear"
    if regime.btc_bias == dir_bias:
        score += 1.2 * regime.btc_strength
    elif regime.btc_bias != "neutral":
        score -= 0.8 * regime.btc_strength

    score += 0.8 * (breadth if dir_bias == regime.btc_bias else (1 - breadth)) - 0.4

    score += 0.6 * (1 - regime.noise_index)
    score += 0.3 * regime.session_weight
    score += min(1.0, 0.25 * len(cand.reasons))

    engine_weight = get_engine_weight(state, cand.engine)
    score *= engine_weight

    if regime.label == "volatile" and cand.engine not in ("liquidity_reversal", "volatility_expansion"):
        score -= 0.5
    if regime.label == "range" and cand.engine == "momentum_breakout":
        score -= 0.6

    return score


def adaptive_threshold(regime: RegimeVector, state: dict, base: float = 2.2) -> float:
    adj = state["governor"]["threshold_adj"]
    t = base + adj
    if regime.label == "volatile":
        t += 0.4
    if regime.label == "trend":
        t -= 0.2
    return t


def estimate_signals_last_24h(state: dict) -> int:
    cutoff = time.time() - 86400
    return sum(1 for h in state["signal_history"] if h.get("ts", 0) >= cutoff)


def governor_adjust_threshold(state: dict):
    """Slow EMA-based frequency governor: nudges the acceptance threshold
    toward the target signals/day band using sustained rate, not
    scan-to-scan noise, to avoid whipsawing the filter bar."""
    count_24h = estimate_signals_last_24h(state)
    gov = state["governor"]
    gov["daily_count_ema"] = 0.85 * gov.get("daily_count_ema", count_24h) + 0.15 * count_24h
    ema_count = gov["daily_count_ema"]
    step = 0.03
    if ema_count < TARGET_SIGNALS_PER_DAY_LOW:
        gov["threshold_adj"] = max(-1.0, gov["threshold_adj"] - step)
    elif ema_count > TARGET_SIGNALS_PER_DAY_HIGH:
        gov["threshold_adj"] = min(1.0, gov["threshold_adj"] + step)


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    key = f"{symbol}:{direction}"
    last = state["cooldowns"].get(key)
    return last is None or (bar_index - last) >= 3


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    state["cooldowns"][f"{symbol}:{direction}"] = bar_index


def count_open_same_direction(state: dict, direction: str) -> int:
    return sum(1 for s in state["open_signals"] if s["direction"] == direction)


def count_open_for_symbol(state: dict, symbol: str) -> int:
    return sum(1 for s in state["open_signals"] if s["symbol"] == symbol)


def compute_returns(candles: list[dict], lookback: int) -> list[float]:
    closes = [c["c"] for c in candles[-lookback:]]
    return [safe((closes[i] - closes[i - 1]) / closes[i - 1]) for i in range(1, len(closes))]


def pearson(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 10:
        return 0.0
    a, b = a[-n:], b[-n:]
    try:
        return statistics.correlation(a, b)
    except statistics.StatisticsError:
        return 0.0


def build_correlation_clusters(bundles: dict[str, dict]) -> list[set[str]]:
    returns = {sym: compute_returns(b["1h"], 60) for sym, b in bundles.items()}
    symbols = list(returns.keys())
    parent = {s: s for s in symbols}

    def find(x):
        while parent[x] != x:
            x = parent[x]
        return x

    def union(x, y):
        parent[find(x)] = find(y)

    for i in range(len(symbols)):
        for j in range(i + 1, len(symbols)):
            if pearson(returns[symbols[i]], returns[symbols[j]]) > 0.75:
                union(symbols[i], symbols[j])
    clusters: dict[str, set] = {}
    for s in symbols:
        clusters.setdefault(find(s), set()).add(s)
    return list(clusters.values())


def dedup_correlated(ranked: list[dict], clusters: list[set[str]]) -> list[dict]:
    seen_clusters = set()
    out = []
    for r in ranked:
        cluster_id = next((i for i, c in enumerate(clusters) if r["cand"].symbol in c), None)
        key = (cluster_id, r["cand"].direction)
        if cluster_id is not None and key in seen_clusters:
            continue
        seen_clusters.add(key)
        out.append(r)
    return out


def passes_hard_filters(symbol: str, snapshot: dict, cand: Candidate) -> tuple[bool, str]:
    snap = snapshot.get(symbol)
    if not snap or snap.get("mark", 0) <= 0:
        return False, "no market snapshot"
    if snap.get("day_vol_usd", 0) and snap["day_vol_usd"] < 2_000_000:
        return False, "24h volume too thin"
    rr = _rr(cand.entry, cand.sl, cand.tp2)
    if rr < 1.1:
        return False, "RR below floor"
    return True, ""


def decision_engine_select(candidates: list[Candidate], regimes: dict[str, RegimeVector],
                            state: dict, snapshot: dict, bundles: dict[str, dict],
                            bar_index: int) -> list[dict]:
    scored = []
    for cand in candidates:
        regime = regimes[cand.symbol]
        ok, why = passes_hard_filters(cand.symbol, snapshot, cand)
        if not ok:
            continue
        if not check_cooldown(state, cand.symbol, cand.direction, bar_index):
            continue
        breadth = regime.breadth
        score = score_candidate(cand, regime, state, breadth)
        threshold = adaptive_threshold(regime, state)
        confidence = max(0.0, min(0.98, logistic(score - threshold) * 0.9 + 0.05))
        scored.append({"cand": cand, "score": score, "threshold": threshold, "confidence": confidence, "regime": regime})

    scored = [s for s in scored if s["score"] >= s["threshold"]]
    scored.sort(key=lambda s: s["score"], reverse=True)

    clusters = build_correlation_clusters(bundles)
    scored = dedup_correlated(scored, clusters)

    accepted = []
    for s in scored:
        cand = s["cand"]
        if count_open_for_symbol(state, cand.symbol) >= MAX_OPEN_PER_SYMBOL:
            continue
        if len(state["open_signals"]) + len(accepted) >= MAX_OPEN_SIGNALS:
            break
        if count_open_same_direction(state, cand.direction) + sum(1 for a in accepted if a["cand"].direction == cand.direction) >= MAX_OPEN_SAME_DIRECTION:
            continue
        accepted.append(s)
    return accepted


# ============================================================================
# SECTION 8 -- TELEGRAM
# ============================================================================

def fmt_px(v: float) -> str:
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def confidence_bar(confidence: float) -> str:
    filled = round(confidence * 5)
    return "\u2B50" * filled + "\u2606" * (5 - filled)


ENGINE_LABELS = {
    "liquidity_reversal": "SMC Liquidity Reversal",
    "trend_continuation": "Trend Continuation",
    "momentum_breakout": "Momentum Breakout",
    "mean_reversion": "Mean Reversion",
    "volatility_expansion": "Volatility Expansion",
}


def format_signal(symbol: str, cand: Candidate, confidence: float, grade: str) -> str:
    arrow = "\U0001F7E2 LONG" if cand.direction == "long" else "\U0001F534 SHORT"
    rr1 = _rr(cand.entry, cand.sl, cand.tp1)
    rr2 = _rr(cand.entry, cand.sl, cand.tp2)
    lines = [
        f"\U0001F4E1 *{ENGINE_NAME} {ENGINE_VERSION}*",
        f"*{symbol}-PERP*  {arrow}   {grade}",
        f"Engine: {ENGINE_LABELS.get(cand.engine, cand.engine)}",
        "",
        f"Entry: `{fmt_px(cand.entry)}`",
        f"Stop Loss: `{fmt_px(cand.sl)}`",
        f"TP1: `{fmt_px(cand.tp1)}`  (RR {rr1:.2f})",
        f"TP2: `{fmt_px(cand.tp2)}`  (RR {rr2:.2f})",
        "",
        f"Confidence: {confidence_bar(confidence)} ({confidence*100:.0f}%)",
        "",
        "Confluence:",
    ] + [f"  \u2022 {r}" for r in cand.reasons]
    return "\n".join(lines)


def send_telegram(text: str) -> Optional[int]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info("Telegram not configured; message:\n%s", text)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = json.dumps({"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode()).get("result", {}).get("message_id")
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as e:
        log.error("Telegram send failed: %s", e)
        return None


def reply_telegram(text: str, reply_to_message_id: Optional[int]) -> Optional[int]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info("Telegram not configured; update:\n%s", text)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode()).get("result", {}).get("message_id")
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as e:
        log.error("Telegram reply failed: %s", e)
        return None


def react_telegram(message_id: Optional[int], emoji: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID or not message_id:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    payload = json.dumps({"chat_id": TG_CHAT_ID, "message_id": message_id,
                           "reaction": [{"type": "emoji", "emoji": emoji}]}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as e:
        log.debug("Telegram reaction failed (non-fatal): %s", e)


# ============================================================================
# SECTION 9 -- TRADE LIFECYCLE / TRACKING / LEARNING
# ============================================================================

def grade_for_confidence(confidence: float) -> str:
    if confidence >= 0.80:
        return "\U0001F451 A+"
    if confidence >= 0.68:
        return "\u2705 A"
    if confidence >= 0.55:
        return "\U0001F44D B"
    return "\u26A0\uFE0F C"


def record_signal(state: dict, cand: Candidate, confidence: float, grade: str, bar_index: int,
                   message_id: Optional[int]) -> dict:
    sig = {
        "id": f"{cand.symbol}-{int(time.time())}",
        "symbol": cand.symbol, "engine": cand.engine, "direction": cand.direction,
        "entry": cand.entry, "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2,
        "confidence": confidence, "grade": grade, "ts": time.time(), "bar_index": bar_index,
        "message_id": message_id, "status": "open", "tp1_hit": False, "be_active": False,
        "result": "open", "r_realized": 0.0, "combo": cand.combo,
    }
    state["open_signals"].append(sig)
    state["signal_history"].append(dict(sig))
    update_cooldown(state, cand.symbol, cand.direction, bar_index)
    return sig


def _r_multiple(sig: dict, price: float) -> float:
    risk = abs(sig["entry"] - sig["sl"])
    if risk <= 1e-12:
        return 0.0
    move = (price - sig["entry"]) if sig["direction"] == "long" else (sig["entry"] - price)
    return move / risk


def _update_engine_stats(state: dict, sig: dict, result: str, r_realized: float):
    stats = state["engine_stats"].setdefault(sig["engine"], {"wins": 0, "losses": 0, "total_r": 0.0})
    if result == "win":
        stats["wins"] += 1
    elif result == "loss":
        stats["losses"] += 1
    stats["total_r"] += r_realized


def _sync_history(state: dict, sig: dict):
    for h in state["signal_history"]:
        if h["id"] == sig["id"]:
            h.update(sig)
            break


def _close_out(state: dict, sig: dict, result: str, price: float):
    sig["status"] = "closed"
    sig["result"] = result
    sig["r_realized"] = _r_multiple(sig, price)
    _update_engine_stats(state, sig, result, sig["r_realized"])
    _sync_history(state, sig)
    label = {"win": "Take Profit hit", "loss": "Stop Loss hit", "cancelled": "Cancelled"}.get(result, result)
    emoji = {"win": "\u2705", "loss": "\u274C"}.get(result, "\u2757")
    reply_telegram(f"{emoji} *{sig['symbol']}* -- {label} ({sig['r_realized']:+.2f}R)", sig.get("message_id"))
    react_telegram(sig.get("message_id"), emoji)


def check_active_signals(state: dict, snapshot: dict, candles_by_symbol: dict[str, list[dict]]):
    still_open = []
    for sig in state["open_signals"]:
        symbol = sig["symbol"]
        candles = candles_by_symbol.get(symbol)
        if not candles:
            still_open.append(sig)
            continue
        for c in candles:
            if c["t"] <= sig["bar_index"]:
                continue
            hi, lo = c["h"], c["l"]
            long_dir = sig["direction"] == "long"
            hit_sl = (lo <= sig["sl"]) if long_dir else (hi >= sig["sl"])
            hit_tp1 = (not sig["tp1_hit"]) and ((hi >= sig["tp1"]) if long_dir else (lo <= sig["tp1"]))
            hit_tp2 = (hi >= sig["tp2"]) if long_dir else (lo <= sig["tp2"])

            if hit_sl and not sig["tp1_hit"]:
                _close_out(state, sig, "loss", sig["sl"])
                sig = None
                break
            if hit_tp1 and not sig["tp1_hit"]:
                sig["tp1_hit"] = True
                sig["be_active"] = True
                sig["sl"] = sig["entry"]
                reply_telegram(f"\U0001F3AF *{symbol}* -- TP1 hit. Stop moved to break-even.", sig.get("message_id"))
                react_telegram(sig.get("message_id"), "\U0001F44D")
            if sig["tp1_hit"] and hit_sl:
                _close_out(state, sig, "win", sig["sl"])
                sig = None
                break
            if hit_tp2:
                _close_out(state, sig, "win", sig["tp2"])
                sig = None
                break
        if sig is not None:
            _sync_history(state, sig)
            still_open.append(sig)
    state["open_signals"] = still_open


# ---- Post-trade learning: pattern notes appended to signal_history --------

def analyze_closed_trade(sig: dict, regime_label: str) -> dict:
    """Lightweight post-mortem tag set. Aggregated later for the daily
    summary's 'Learning Insights' section; also what future scoring reads
    back via engine_stats (win rate / avg R), which is the actual
    self-tuning mechanism -- this function only produces human-readable
    notes, it doesn't change scoring directly."""
    notes = []
    if sig["result"] == "win" and sig.get("tp1_hit"):
        notes.append("ran cleanly through TP1 to target -- good trend read")
    elif sig["result"] == "win":
        notes.append("reached TP2 without a TP1 partial pass -- consider tighter TP1")
    elif sig["result"] == "loss" and not sig.get("tp1_hit"):
        notes.append("stopped before any partial -- entry timing or SL placement may be too tight for regime")
    hold_bars = None
    return {"notes": notes, "regime": regime_label}


# ============================================================================
# SECTION 10 -- DAILY SUMMARY
# ============================================================================

DAILY_SUMMARY_UTC_HOUR = 8


def generate_daily_summary(state: dict) -> str:
    cutoff = time.time() - 86400
    recent = [h for h in state["signal_history"] if h.get("ts", 0) >= cutoff]
    resolved = [h for h in recent if h.get("result") in ("win", "loss")]
    wins = [h for h in resolved if h["result"] == "win"]
    losses = [h for h in resolved if h["result"] == "loss"]
    open_now = [h for h in recent if h.get("result") == "open"]
    total_r = sum(h.get("r_realized", 0.0) for h in resolved)
    win_rate = (len(wins) / len(resolved) * 100) if resolved else 0.0
    gross_win_r = sum(h["r_realized"] for h in wins)
    gross_loss_r = abs(sum(h["r_realized"] for h in losses))
    profit_factor = safe(gross_win_r / gross_loss_r) if gross_loss_r > 0 else (gross_win_r if gross_win_r > 0 else 0.0)
    avg_rr = statistics.mean([_rr(h["entry"], h["sl"], h["tp2"]) for h in recent]) if recent else 0.0
    hold_times = [ (h.get("ts",0)) for h in resolved ]

    lines = [
        f"\U0001F4CA *{ENGINE_NAME} {ENGINE_VERSION} -- 24h Summary*",
        "",
        f"Total Signals: {len(recent)}",
        f"Wins: {len(wins)}   Losses: {len(losses)}   Open: {len(open_now)}",
        f"Win Rate: {win_rate:.1f}%",
        f"Profit Factor: {profit_factor:.2f}",
        f"Average RR (planned): {avg_rr:.2f}",
        f"Net R: {total_r:+.2f}",
    ]
    if resolved:
        by_engine: dict[str, list] = {}
        for h in resolved:
            by_engine.setdefault(h["engine"], []).append(h)
        lines.append("")
        lines.append("Performance by Engine:")
        for eng, items in by_engine.items():
            w = sum(1 for i in items if i["result"] == "win")
            r = sum(i["r_realized"] for i in items)
            lines.append(f"  \u2022 {ENGINE_LABELS.get(eng, eng)}: {w}/{len(items)} ({100*w/len(items):.0f}%), {r:+.2f}R")

        best = max(resolved, key=lambda h: h["r_realized"])
        worst = min(resolved, key=lambda h: h["r_realized"])
        lines.append("")
        lines.append(f"Best Setup: {best['symbol']} {best['engine']} ({best['r_realized']:+.2f}R)")
        lines.append(f"Worst Setup: {worst['symbol']} {worst['engine']} ({worst['r_realized']:+.2f}R)")

        conf_buckets = sorted(resolved, key=lambda h: h["confidence"])
        hi_conf = [h for h in resolved if h["confidence"] >= 0.68]
        hi_conf_wr = (sum(1 for h in hi_conf if h["result"] == "win") / len(hi_conf) * 100) if hi_conf else None
        lines.append("")
        if hi_conf_wr is not None:
            lines.append(f"Confidence Accuracy (>=68% conf bucket): {hi_conf_wr:.0f}% actual win rate")

        insights = []
        if win_rate < 40 and len(resolved) >= 8:
            insights.append("Win rate trailing target -- governor will tighten the acceptance threshold.")
        if profit_factor > 1.5:
            insights.append("Profit factor healthy -- current regime weighting is working, no action needed.")
        worst_engine = min(by_engine.items(), key=lambda kv: sum(i["r_realized"] for i in kv[1]))[0]
        insights.append(f"Weakest engine this window: {ENGINE_LABELS.get(worst_engine, worst_engine)} -- its adaptive weight will drift down as sample size grows.")
        lines.append("")
        lines.append("Learning Insights:")
        lines.extend(f"  \u2022 {i}" for i in insights)
    lines.append("")
    lines.append(f"Recommended Optimizations: threshold_adj={state['governor']['threshold_adj']:+.2f}, "
                  f"24h signal EMA={state['governor']['daily_count_ema']:.1f} (target {TARGET_SIGNALS_PER_DAY_LOW:.0f}-{TARGET_SIGNALS_PER_DAY_HIGH:.0f})")
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict):
    now = time.gmtime()
    today_str = time.strftime("%Y-%m-%d", now)
    if now.tm_hour != DAILY_SUMMARY_UTC_HOUR:
        return
    if state.get("last_summary_date") == today_str:
        return
    send_telegram(generate_daily_summary(state))
    state["last_summary_date"] = today_str


# ============================================================================
# SECTION 11 -- MAIN SCAN
# ============================================================================

def _prefetch(symbol: str, candle_cache: dict[str, dict], reference_ms: int) -> tuple[str, Optional[dict]]:
    try:
        return symbol, fetch_all_candles(symbol, candle_cache, reference_ms)
    except Exception as e:  # noqa: BLE001 -- one symbol's failure must not kill the scan
        log.warning("Prefetch failed for %s: %s", symbol, e)
        return symbol, None


def run_scan():
    reference_ms = int(time.time() * 1000)
    state = load_state()
    candle_cache = load_candle_cache()
    state["bar_index"] += 1
    bar_index = state["bar_index"]

    snapshot = get_market_snapshot()

    bundles: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_prefetch, sym, candle_cache, reference_ms) for sym in set(WATCHLIST + ["BTC"])]
        for fut in as_completed(futures):
            sym, bundle = fut.result()
            if bundle:
                bundles[sym] = bundle

    if "BTC" not in bundles:
        log.error("No BTC data this scan; aborting.")
        save_candle_cache(candle_cache)
        return

    btc_bias, btc_strength = compute_btc_regime(bundles["BTC"])
    breadth = compute_breadth({s: b for s, b in bundles.items() if s != "BTC"}, btc_bias)

    regimes: dict[str, RegimeVector] = {}
    candidates: list[Candidate] = []
    for symbol in WATCHLIST:
        bundle = bundles.get(symbol)
        if not bundle:
            continue
        regime = build_regime_vector(state, symbol, bundle, btc_bias, btc_strength, breadth)
        regimes[symbol] = regime
        for engine_fn in SUB_ENGINES:
            try:
                cand = engine_fn(symbol, bundle, regime)
            except Exception as e:  # noqa: BLE001
                log.warning("%s failed on %s: %s", engine_fn.__name__, symbol, e)
                continue
            if cand is None:
                continue
            market_price = snapshot.get(symbol, {}).get("mark", cand.entry)
            cand = clamp_candidate_to_market(cand, market_price)
            candidates.append(cand)

    accepted = decision_engine_select(candidates, regimes, state, snapshot, bundles, bar_index)

    for item in accepted:
        cand, confidence = item["cand"], item["confidence"]
        grade = grade_for_confidence(confidence)
        text = format_signal(cand.symbol, cand, confidence, grade)
        msg_id = send_telegram(text)
        record_signal(state, cand, confidence, grade, bar_index, msg_id)
        log.info("Signal: %s %s via %s (score=%.2f conf=%.2f)",
                  cand.symbol, cand.direction, cand.engine, item["score"], confidence)

    candles_by_symbol = {s: b["15m"] for s, b in bundles.items()}
    check_active_signals(state, snapshot, candles_by_symbol)

    governor_adjust_threshold(state)
    maybe_send_daily_summary(state)
    prune_state(state)

    save_state(state)
    save_candle_cache(candle_cache)
    log.info("Scan complete. Candidates=%d Accepted=%d Open=%d BTC=%s(%.2f) Breadth=%.2f",
              len(candidates), len(accepted), len(state["open_signals"]), btc_bias, btc_strength, breadth)


def main():
    try:
        run_scan()
    except Exception:
        log.exception("Fatal error during scan")
        raise


if __name__ == "__main__":
    main()
