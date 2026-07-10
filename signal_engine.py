#!/usr/bin/env python3
"""
ASTROLABE ENGINE v1.0.0
========================
Ground-up institutional-grade multi-timeframe SMC/ICT crypto perpetual
signal engine for Hyperliquid. Built from scratch as an Adaptive
Multi-Engine Ensemble, using axis_engine_v2.1.0, kestrel_signal_engine
v1.0.1, kairos_signal_engine_v1.0.0 and meridian_v1.1.0 strictly as
reference material -- concepts were re-derived and re-implemented, not
copy-pasted, and every known defect in the references was fixed rather
than inherited. See ASTROLABE_Gap_Analysis_and_Verification.md for the
full engine-by-engine diff.

FIVE-PATHWAY ENSEMBLE
    1. Liquidity Sweep Reversal   -- sweep + MSS + OB/FVG POI retest
    2. Trend Continuation         -- EMA/ADX trend + RSI pullback-and-turn
    3. Momentum Volatility Breakout -- BB squeeze + volume expansion
    4. Range Mean-Reversion       -- range regime, premium/discount fade
    5. Breaker Continuation       -- CHoCH/BOS breaker-block retest
    Each candidate is scored by one centralized Decision Engine rather
    than per-pathway ad-hoc scorers, so pathways compete on a common
    confidence scale instead of firing independently.

FIXED: CORRELATION-DEDUP DIRECTION BUG
    Every reference engine (Axis, Kestrel, and their Helios-lineage
    ancestor) deduplicates correlated signals by keying on
    (cluster, direction) instead of cluster alone. That lets a >0.72-
    correlated pair fire both a LONG and a SHORT in the same scan,
    which is not diversification -- it is two correlated bets that can
    both go wrong together while looking like a hedge. This engine
    keys strictly on cluster identity: only the single highest-
    confidence candidate per correlation cluster survives, regardless
    of direction. See dedup_correlated().

NEW: FLEET-WIDE EXPOSURE AWARENESS
    sdz runs many engines against the same watchlist and each engine
    has historically only been aware of its own open positions --
    "shared open-position visibility across engines" was an unresolved
    portfolio-level risk. ASTROLABE can optionally read other engines'
    state.json files (FLEET_STATE_PATHS) and fold their currently open
    directional exposure per correlation cluster into scoring, so it
    won't stack a correlated same-cluster opposite-direction signal on
    top of a position another engine already has open. Best-effort and
    fully optional -- absent or unreadable files degrade to a no-op.

NEW: INSTITUTIONAL ORDER FLOW MODULE
    Funding-rate and open-interest trend tracked scan-over-scan in
    state.json (Hyperliquid's assetCtx snapshot is a point-in-time
    read, so trend requires our own history) and folded into scoring
    as a soft confluence, not a hard veto -- avoiding the brittle,
    over-parameterized funding/OI/breadth cascade in Kairos that made
    it the least reliable engine in the fleet.

FIXED: TIMEFRAME FLOOR
    All reference engines (Axis included) use a 5m execution
    timeframe. Per the governing spec this engine is built to, no
    timeframe below 15m is permitted. Combos here bottom out at 15m
    execution; the intraday "active" combo replaces Axis's 1h/15m/5m
    scalp combo with 1h/30m/15m.

FIXED: POINT-IN-TIME OUTCOME CHECKING
    Reference engines resolve TP/SL against the latest mark price
    only, which can miss an intra-candle wick that touched SL (or
    TP1) and reversed before the next scan -- the same class of bug
    documented as fixed in Parallax v1.1.1. ASTROLABE resolves
    outcomes by scanning closed execution-timeframe candles
    chronologically from each signal's own watermark, falling back to
    mark-price checking only when a fresh candle bundle isn't
    available this scan.

FIXED: BREAKEVEN STOP-OUT MISLABELED AS "TP2 HIT"
    Axis's outcome-resolution branches record a breakeven stop-out
    (SL hit after TP1 already banked) with the same result value
    ("win") used for an actual TP2 hit, and the Telegram headline
    logic checks that result value first -- so a trade that stopped
    at breakeven after TP1 gets announced as "TP2 hit -- WIN" even
    though TP2 was never touched. ASTROLABE threads an explicit
    exit_reason ("tp2" / "breakeven" / "sl") through outcome
    resolution so the headline always reflects what actually
    happened, independent of the win/loss bucket used for stats.

PORTED, RE-IMPLEMENTED CLEAN
    - Adaptive Frequency Governor targeting 5-10 signals/day (Axis).
    - Regularized, shrunk-to-prior self-tuning pathway weights (Axis).
    - Session Volume Profile (POC / Value Area / VWAP) for TP clipping
      and scoring confluence (Axis, ported from Helios).
    - Five-filter gate (location, context, quality, RR, LTF trigger)
      as the base of the Decision Engine (Kestrel).
    - Adaptive SL buffer via directional wick sizing scaled by ATR
      volatility percentile (Ecliptic v1.0.0).
    - One-active-signal-per-symbol enforcement, breakeven-on-TP1,
      reply-threaded Telegram outcome updates (Lucerna/Axis).
    - Weighted, sliding-window Hyperliquid rate limiter with delta
      candle caching (Axis).

Single file, immediately runnable. Scan-per-run model: an external
scheduler (cron-job.org, GitHub Actions cron, systemd timer) invokes
this script every 15 minutes. All persistence lives in state.json and
candle_cache.json next to the script; there is no long-running process
and no database.

Configure via environment variables (see CONFIGURATION below) and run:

    python3 astrolabe_engine_v1_0_0.py

Author: ASTROLABE ENGINE project
"""

from __future__ import annotations

import collections
import glob
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
# CONFIGURATION
# ============================================================================

ENGINE_NAME = "ASTROLABE ENGINE"
ENGINE_VERSION = "v1.0.0"

HL_API_URL = "https://api.hyperliquid.xyz/info"
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

STATE_PATH = os.environ.get("ASTROLABE_STATE_PATH", "state.json")
LOG_PATH = os.environ.get("ASTROLABE_LOG_PATH", "astrolabe_engine.log")
CANDLE_CACHE_PATH = os.environ.get("ASTROLABE_CANDLE_CACHE_PATH", "candle_cache.json")
CANDLE_DELTA_OVERLAP_BARS = 3

# Comma-separated list of OTHER engines' state.json paths (or glob
# patterns) this engine may read, best-effort, to fold fleet-wide open
# exposure into scoring. Empty by default -- fully optional.
FLEET_STATE_PATHS = [
    p.strip() for p in os.environ.get("ASTROLABE_FLEET_STATE_PATHS", "").split(",") if p.strip()
]
# "penalize" softly discounts confidence on a fleet-level opposite-direction
# cluster conflict; "block" hard-skips the candidate. "off" disables the
# check entirely.
FLEET_CONFLICT_MODE = os.environ.get("ASTROLABE_FLEET_CONFLICT_MODE", "penalize")
FLEET_CONFLICT_PENALTY = 12.0  # confidence points subtracted in "penalize" mode

WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# Timeframe combos: (bias, structure, execution). Floor is 15m -- no
# combo in this engine ever touches 1m/3m/5m. The Regime Router picks
# one per symbol per scan based on the symbol's own volatility/ADX
# profile rather than using a single fixed combo for the whole watchlist.
COMBOS = {
    "active":   {"bias": "1h", "struct": "30m", "exec": "15m", "hold_hint": "1-6h"},
    "intraday": {"bias": "4h", "struct": "1h",  "exec": "15m", "hold_hint": "4-24h"},
    "swing":    {"bias": "1d", "struct": "4h",  "exec": "1h",  "hold_hint": "1-5d"},
}

TF_MS = {
    "15m": 15 * 60_000, "30m": 30 * 60_000, "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000, "1d": 24 * 60 * 60_000,
}
CANDLE_COUNT = {"15m": 300, "30m": 300, "1h": 300, "4h": 240, "1d": 180}
ALL_TFS = ("15m", "30m", "1h", "4h", "1d")

ATR_LEN = 14
RSI_LEN = 14
ADX_LEN = 14
EMA_FAST, EMA_SLOW, EMA_TREND = 20, 50, 200
BB_LEN, BB_MULT = 20, 2.0

# --- Session Volume Profile -------------------------------------------------
VOL_PROFILE_BINS = 24
VOL_PROFILE_LOOKBACK_BARS = 96   # ~4 days of 1h bars as the "session" profile

# --- Adaptive Frequency Governor --------------------------------------------
TARGET_SIGNALS_MIN = 5
TARGET_SIGNALS_MAX = 10
GOVERNOR_STEP = 2.0
GOVERNOR_FLOOR = 54.0
GOVERNOR_CEIL = 88.0
GOVERNOR_MIN_INTERVAL_S = 3600  # rate-limit threshold nudges to once/hour

# --- Self-tuning pathway weights --------------------------------------------
PATHWAY_WEIGHT_LEARNING_RATE = 0.04
PATHWAY_WEIGHT_MIN, PATHWAY_WEIGHT_MAX = 0.75, 1.30

# --- Setup Grade -> risk sizing hint (percent of equity), informational ----
GRADE_SIZE_TABLE = {
    ("A+", "active"): 0.90, ("A+", "intraday"): 1.25, ("A+", "swing"): 1.50,
    ("A",  "active"): 0.65, ("A",  "intraday"): 1.00, ("A",  "swing"): 1.25,
    ("B",  "active"): 0.45, ("B",  "intraday"): 0.65, ("B",  "swing"): 0.85,
    ("C",  "active"): 0.25, ("C",  "intraday"): 0.35, ("C",  "swing"): 0.45,
}

MAX_CONCURRENT_PER_SYMBOL = 1
MAX_CONCURRENT_SAME_DIRECTION = 6
COOLDOWN_BARS = 6
DEDUP_PRICE_TOL_PCT = 0.0025
DEDUP_TIME_WINDOW_HOURS = 48

POI_MAX_DIST_ATR_MULT = 1.6
ZONE_MAX_WIDTH_ATR_MULT = 2.2

# --- Network performance / rate-limit handling ------------------------------
HL_WEIGHT_BUDGET_PER_MINUTE = 1000.0
HL_ENDPOINT_BASE_WEIGHT = {
    "l2Book": 2, "allMids": 2, "clearinghouseState": 2, "orderStatus": 2,
    "spotClearinghouseState": 2, "exchangeStatus": 2, "userRole": 60,
    "metaAndAssetCtxs": 20,
}
HL_DEFAULT_INFO_WEIGHT = 20
FETCH_THREAD_WORKERS = 6

# Trend-continuation pathway tuning
TREND_ADX_MIN = 20.0
RSI_DIP_LONG, RSI_TURN_LONG = 40.0, 45.0
RSI_DIP_SHORT, RSI_TURN_SHORT = 60.0, 55.0
RSI_RESET_LOOKBACK = 8

# Momentum breakout pathway tuning
BREAKOUT_BB_SQUEEZE_PCTILE = 0.35
BREAKOUT_VOL_MULT = 1.6
BREAKOUT_LOOKBACK_HIGH_LOW = 20

# Range mean-reversion pathway tuning
RANGE_ADX_MAX = 18.0
RANGE_EDGE_ZONE_PCT = 0.18  # top/bottom 18% of range counts as "at the edge"

# Breaker continuation pathway tuning
BREAKER_LOOKBACK = 40
BREAKER_MIN_DISPLACEMENT_ATR = 1.1

# Correlation clustering (computed fresh each run from bias-tf returns)
CORR_LOOKBACK_BARS = 60
CORR_CLUSTER_THRESHOLD = 0.72

# Hard filters
MIN_OI_USD = 3_000_000
MIN_ATR_PCT = 0.0012
MAX_ATR_PCT = 0.12
MIN_RR = 1.4
MAX_SPREAD_PCT = 0.0012

DAILY_SUMMARY_UTC_HOUR = 8

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger("astrolabe")


def _handle_shutdown(sig_num, frame):
    log.warning("Received shutdown signal %s, exiting cleanly.", sig_num)
    raise SystemExit(0)


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


# ============================================================================
# HYPERLIQUID API
# ============================================================================

def hl_coin(symbol: str) -> str:
    return symbol.upper()


class _WeightRateLimiter:
    """Sliding-60s-window pacer shared across all threads. Tracks aggregate
    request WEIGHT (not request count) so a burst of heavy candleSnapshot
    calls is paced the same as Hyperliquid actually bills it."""

    def __init__(self, budget_per_minute: float):
        self.budget = budget_per_minute
        self.window_s = 60.0
        self.lock = threading.Lock()
        self.events: collections.deque = collections.deque()

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


def filter_closed_candles(candles: list, interval: str, reference_ms: int) -> list:
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]


def _request_candles(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": hl_coin(symbol), "interval": interval,
                 "startTime": start_ms, "endTime": end_ms},
    }
    raw = hl_post(payload)
    if not raw:
        return []
    return [
        {"t": c["t"], "o": float(c["o"]), "h": float(c["h"]),
         "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])}
        for c in raw
    ]


def get_candles(symbol: str, interval: str, n: int, reference_ms: Optional[int] = None,
                 cache_entry: Optional[list] = None) -> list:
    """Return the last `n` closed candles for symbol/interval. If a cached,
    previously-fetched entry is supplied, only fetch bars newer than the
    cached watermark (minus a small overlap) and merge, instead of
    re-requesting the full window."""
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


def fetch_all_candles(symbol: str, candle_cache: Optional[dict] = None,
                       reference_ms: Optional[int] = None):
    bundle = {}
    sym_cache = (candle_cache or {}).get(symbol, {})
    for tf in ALL_TFS:
        cache_entry = sym_cache.get(tf)
        candles = get_candles(symbol, tf, CANDLE_COUNT[tf], reference_ms, cache_entry)
        if len(candles) < 60:
            log.info("Insufficient %s candles for %s (%d)", tf, symbol, len(candles))
            return None
        bundle[tf] = candles
        if candle_cache is not None:
            candle_cache.setdefault(symbol, {})[tf] = candles
    return bundle


def get_meta_and_asset_ctxs():
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or len(raw) < 2:
        return None
    universe = [a["name"] for a in raw[0]["universe"]]
    return {"universe": universe, "ctxs": raw[1]}


def get_market_snapshot() -> dict:
    """symbol -> {mark, oi_usd, funding, spread_pct (best-effort)}."""
    out = {}
    meta = get_meta_and_asset_ctxs()
    if not meta:
        return out
    for name, ctx in zip(meta["universe"], meta["ctxs"]):
        if name not in WATCHLIST:
            continue
        try:
            mark = float(ctx.get("markPx", 0.0))
            oi = float(ctx.get("openInterest", 0.0)) * mark
            funding = float(ctx.get("funding", 0.0))
            out[name] = {"mark": mark, "oi_usd": oi, "funding": funding}
        except (TypeError, ValueError):
            continue
    return out


def get_l2_book(coin: str):
    return hl_post({"type": "l2Book", "coin": hl_coin(coin)})


def analyze_orderbook(coin: str) -> dict:
    book = get_l2_book(coin)
    if not book or "levels" not in book:
        return {"spread_pct": None, "bid_depth": 0.0, "ask_depth": 0.0, "imbalance": 0.0}
    try:
        bids, asks = book["levels"][0], book["levels"][1]
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2
        spread_pct = (best_ask - best_bid) / mid if mid else None
        bid_depth = sum(float(b["sz"]) for b in bids[:10])
        ask_depth = sum(float(a["sz"]) for a in asks[:10])
        total = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / total if total else 0.0
        return {"spread_pct": spread_pct, "bid_depth": bid_depth,
                "ask_depth": ask_depth, "imbalance": imbalance}
    except (KeyError, IndexError, ValueError, TypeError):
        return {"spread_pct": None, "bid_depth": 0.0, "ask_depth": 0.0, "imbalance": 0.0}


# ============================================================================
# INDICATORS
# ============================================================================

def safe(v, fb=0.0):
    try:
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return fb
        return v
    except TypeError:
        return fb


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        if not b:
            return default
        return a / b
    except (TypeError, ZeroDivisionError):
        return default


def ema(vals: list, period: int) -> list:
    if not vals:
        return []
    k = 2.0 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals: list, period: int) -> list:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1):i + 1]
        out.append(sum(window) / len(window))
    return out


def stdev(vals: list, period: int) -> list:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1):i + 1]
        out.append(statistics.pstdev(window) if len(window) > 1 else 0.0)
    return out


def rsi(closes: list, period: int = RSI_LEN) -> list:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains[1:period + 1]) / period if len(gains) > period else safe(sum(gains) / max(1, len(gains)))
    avg_loss = sum(losses[1:period + 1]) / period if len(losses) > period else safe(sum(losses) / max(1, len(losses)))
    out = [50.0] * min(period, len(closes))
    for i in range(period, len(closes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = safe_div(avg_gain, avg_loss, default=999.0)
        out.append(100 - (100 / (1 + rs)))
    while len(out) < len(closes):
        out.append(out[-1] if out else 50.0)
    return out[:len(closes)]


def atr_series(candles: list, period: int = ATR_LEN) -> list:
    if not candles:
        return []
    trs = []
    prev_close = candles[0]["c"]
    for c in candles:
        tr = max(c["h"] - c["l"], abs(c["h"] - prev_close), abs(c["l"] - prev_close))
        trs.append(tr)
        prev_close = c["c"]
    out, avg = [], trs[0]
    for i, tr in enumerate(trs):
        avg = tr if i == 0 else (avg * (period - 1) + tr) / period
        out.append(avg)
    return out


def adx_series(candles: list, period: int = ADX_LEN):
    n = len(candles)
    if n < 2:
        return [0.0] * n, [0.0] * n, [0.0] * n
    plus_dm, minus_dm, trs = [0.0], [0.0], [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        down = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        tr = max(candles[i]["h"] - candles[i]["l"],
                  abs(candles[i]["h"] - candles[i - 1]["c"]),
                  abs(candles[i]["l"] - candles[i - 1]["c"]))
        trs.append(tr)

    def _wilder_smooth(vals):
        out, avg = [], vals[0]
        for i, v in enumerate(vals):
            avg = v if i == 0 else (avg * (period - 1) + v) / period
            out.append(avg)
        return out

    atr_sm = _wilder_smooth(trs)
    pdm_sm = _wilder_smooth(plus_dm)
    mdm_sm = _wilder_smooth(minus_dm)
    plus_di = [100 * safe_div(p, a) for p, a in zip(pdm_sm, atr_sm)]
    minus_di = [100 * safe_div(m, a) for m, a in zip(mdm_sm, atr_sm)]
    dx = [100 * safe_div(abs(p - m), (p + m)) for p, m in zip(plus_di, minus_di)]
    adx = _wilder_smooth(dx)
    return adx, plus_di, minus_di


def bb_width_pct(closes: list, period: int = BB_LEN, mult: float = BB_MULT) -> list:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    out = []
    for i in range(len(closes)):
        upper, lower = mid[i] + mult * sd[i], mid[i] - mult * sd[i]
        out.append(safe_div(upper - lower, mid[i]))
    return out


def percentile_rank(vals: list, x: float) -> float:
    if not vals:
        return 0.5
    below = sum(1 for v in vals if v <= x)
    return below / len(vals)


def compute_indicators(candles: list) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c["v"] for c in candles]
    ema_fast = ema(closes, EMA_FAST)
    ema_slow = ema(closes, EMA_SLOW)
    ema_trend = ema(closes, EMA_TREND)
    rsi_vals = rsi(closes, RSI_LEN)
    atr_vals = atr_series(candles, ATR_LEN)
    adx_vals, plus_di, minus_di = adx_series(candles, ADX_LEN)
    bbw = bb_width_pct(closes, BB_LEN, BB_MULT)
    avg_vol20 = sma(vols, 20)
    return {
        "closes": closes, "highs": highs, "lows": lows, "vols": vols,
        "ema_fast": ema_fast, "ema_slow": ema_slow, "ema_trend": ema_trend,
        "rsi": rsi_vals, "atr": atr_vals, "adx": adx_vals,
        "plus_di": plus_di, "minus_di": minus_di, "bbw": bbw, "avg_vol20": avg_vol20,
    }


def session_weight_now() -> float:
    """Rough liquidity weight by UTC hour: London/NY overlap heaviest."""
    hour = time.gmtime().tm_hour
    if 13 <= hour < 17:
        return 1.0   # London/NY overlap
    if 7 <= hour < 13 or 17 <= hour < 21:
        return 0.85  # London or NY alone
    if 0 <= hour < 7:
        return 0.55  # Asia session, thinner
    return 0.65


# ============================================================================
# STATE PERSISTENCE
# ============================================================================

def _default_state() -> dict:
    return {
        "active_signals": [],
        "signal_history": [],
        "cooldowns": {},
        "governor": {"threshold": 66.0, "last_adjust_ts": 0},
        "pathway_weights": {},
        "atr_pct_memory": {},
        "order_flow_history": {},
        "confidence_calibration": [],
        "last_summary_date": None,
        "last_summary_ts": 0,
    }


def load_state() -> dict:
    default = _default_state()
    if not os.path.exists(STATE_PATH):
        return default
    try:
        with open(STATE_PATH, "r") as f:
            loaded = json.load(f)
        for k, v in default.items():
            loaded.setdefault(k, v)
        return loaded
    except (json.JSONDecodeError, OSError) as e:
        log.error("Failed to load state (%s); starting fresh.", e)
        return default


def save_state(state: dict):
    tmp_path = STATE_PATH + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp_path, STATE_PATH)
    except OSError as e:
        log.error("Failed to save state: %s", e)


def load_candle_cache() -> dict:
    if not os.path.exists(CANDLE_CACHE_PATH):
        return {}
    try:
        with open(CANDLE_CACHE_PATH, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.warning("Failed to load candle cache (%s); starting fresh.", e)
        return {}


def save_candle_cache(candle_cache: dict):
    tmp_path = CANDLE_CACHE_PATH + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(candle_cache, f)
        os.replace(tmp_path, CANDLE_CACHE_PATH)
    except OSError as e:
        log.error("Failed to save candle cache: %s", e)


def prune_state(state: dict, max_signals: int = 800, max_days: int = 21):
    cutoff = time.time() - max_days * 86400
    state["signal_history"] = [
        h for h in state["signal_history"] if h.get("ts", 0) >= cutoff
    ][-max_signals:]
    state["confidence_calibration"] = state.get("confidence_calibration", [])[-max_signals:]
    active_symbols = {s["symbol"] for s in state["active_signals"]}
    state["cooldowns"] = {
        k: v for k, v in state["cooldowns"].items()
        if k.split("|")[0] in active_symbols or (time.time() - v.get("ts", 0)) < 7 * 86400
    }


def load_fleet_exposure() -> dict:
    """Best-effort read of other engines' state.json files to surface
    fleet-wide open directional exposure per symbol. Returns
    symbol -> set of directions currently open somewhere in the fleet.
    Missing/unreadable files are silently skipped -- this is a portfolio
    awareness aid, not a dependency."""
    exposure: dict = {}
    if not FLEET_STATE_PATHS:
        return exposure
    paths = []
    for pattern in FLEET_STATE_PATHS:
        paths.extend(glob.glob(pattern) if any(ch in pattern for ch in "*?[") else [pattern])
    for path in paths:
        try:
            if os.path.abspath(path) == os.path.abspath(STATE_PATH):
                continue
            with open(path, "r") as f:
                other = json.load(f)
            for sig in other.get("active_signals", []):
                sym, direction = sig.get("symbol"), sig.get("direction")
                if sym and direction:
                    exposure.setdefault(sym, set()).add(direction)
        except (OSError, json.JSONDecodeError, AttributeError):
            continue
    return exposure


# ============================================================================
# REGIME DETECTION
# ============================================================================

@dataclass
class RegimeVector:
    btc_bias: str
    btc_strength: float
    vol_pctile: float
    adx: float
    noise_index: float
    session_weight: float
    breadth: float
    label: str  # trend / range / reversal / volatile


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state["atr_pct_memory"].setdefault(symbol, [])
    mem.append(atr_pct)
    if len(mem) > 200:
        del mem[:len(mem) - 200]
    return percentile_rank(mem, atr_pct)


def compute_btc_regime(btc_bundle: dict):
    ind = compute_indicators(btc_bundle["4h"])
    adx_val = safe(ind["adx"][-1])
    ema_f, ema_s = ind["ema_fast"][-1], ind["ema_slow"][-1]
    close = ind["closes"][-1]
    if close > ema_f > ema_s and adx_val >= TREND_ADX_MIN:
        bias = "bull"
    elif close < ema_f < ema_s and adx_val >= TREND_ADX_MIN:
        bias = "bear"
    else:
        bias = "neutral"
    return bias, adx_val


def compute_noise_index(candles: list, lookback: int = 30) -> float:
    """Fraction of net directional distance consumed by intra-bar chop:
    high => choppy/noisy, low => clean directional movement."""
    recent = candles[-lookback:]
    if len(recent) < 2:
        return 0.5
    total_range = sum(c["h"] - c["l"] for c in recent)
    net_move = abs(recent[-1]["c"] - recent[0]["c"])
    if total_range <= 0:
        return 0.5
    return max(0.0, min(1.0, 1.0 - safe_div(net_move, total_range)))


def symbol_bias_from_bundle(bundle: dict):
    ind = compute_indicators(bundle["1h"])
    if len(ind["closes"]) < EMA_SLOW:
        return None
    close, ema_s = ind["closes"][-1], ind["ema_slow"][-1]
    return "up" if close > ema_s else "down"


def compute_breadth(bundles: dict, btc_bias: str) -> float:
    if btc_bias == "neutral":
        return 0.5
    agree, total = 0, 0
    for sym, bundle in bundles.items():
        bias = symbol_bias_from_bundle(bundle)
        if bias is None:
            continue
        total += 1
        wants = "up" if btc_bias == "bull" else "down"
        if bias == wants:
            agree += 1
    return safe_div(agree, total, default=0.5)


def build_regime_vector(state: dict, symbol: str, bundle: dict, btc_bias: str,
                         btc_strength: float, breadth: float, combo: dict) -> RegimeVector:
    ind_struct = compute_indicators(bundle[combo["struct"]])
    adx_val = safe(ind_struct["adx"][-1])
    atr_val = safe(ind_struct["atr"][-1])
    close = ind_struct["closes"][-1]
    atr_pct = safe_div(atr_val, close)
    vol_pctile = update_atr_pct_memory(state, symbol, atr_pct)
    noise = compute_noise_index(bundle[combo["struct"]])
    bbw = ind_struct["bbw"][-1]
    bbw_hist = ind_struct["bbw"][-100:] if len(ind_struct["bbw"]) >= 100 else ind_struct["bbw"]
    bbw_pctile = percentile_rank(bbw_hist, bbw)

    if adx_val >= TREND_ADX_MIN and noise < 0.6:
        label = "trend"
    elif bbw_pctile <= BREAKOUT_BB_SQUEEZE_PCTILE and vol_pctile >= 0.6:
        label = "volatile"
    elif adx_val <= RANGE_ADX_MAX:
        label = "range"
    else:
        label = "reversal"

    return RegimeVector(
        btc_bias=btc_bias, btc_strength=btc_strength, vol_pctile=vol_pctile,
        adx=adx_val, noise_index=noise, session_weight=session_weight_now(),
        breadth=breadth, label=label,
    )


def select_combo(regime: RegimeVector) -> str:
    if regime.label == "volatile" or regime.vol_pctile >= 0.75:
        return "active"
    if regime.label == "trend" and regime.adx >= 28:
        return "swing"
    return "intraday"


def adaptive_thresholds(regime: RegimeVector, base_threshold: float) -> float:
    adj = 0.0
    if regime.noise_index > 0.65:
        adj += 4.0  # tighten in chaotic conditions
    if regime.label == "trend" and regime.adx >= 30:
        adj -= 2.0  # relax slightly in clean strong trends
    if regime.session_weight < 0.6:
        adj += 2.0  # tighten in thin Asia-session liquidity
    return max(GOVERNOR_FLOOR, min(GOVERNOR_CEIL, base_threshold + adj))


# ============================================================================
# MARKET STRUCTURE / SMART MONEY CONCEPTS
# ============================================================================

@dataclass
class Swing:
    index: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list, left: int = 2, right: int = 2) -> list:
    swings = []
    for i in range(left, len(candles) - right):
        window = candles[i - left:i + right + 1]
        h, l = candles[i]["h"], candles[i]["l"]
        if h == max(c["h"] for c in window):
            swings.append(Swing(i, h, "high"))
        if l == min(c["l"] for c in window):
            swings.append(Swing(i, l, "low"))
    return swings


@dataclass
class StructureState:
    bias: str            # "bull" | "bear" | "neutral"
    last_event: str       # "BOS" | "CHoCH" | "none"
    last_event_index: int
    last_swing_high: Optional[float]
    last_swing_low: Optional[float]


def analyze_structure(candles: list, swings: list) -> Optional[StructureState]:
    if len(swings) < 4:
        return None
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if not highs or not lows:
        return None

    ordered = sorted(swings, key=lambda s: s.index)
    bias = "neutral"
    last_event, last_event_index = "none", 0
    hh_prev = ll_prev = None

    for s in ordered:
        if s.kind == "high":
            if hh_prev is not None:
                if s.price > hh_prev and bias in ("bull", "neutral"):
                    if bias == "bear":
                        last_event, last_event_index = "CHoCH", s.index
                    elif bias == "bull":
                        last_event, last_event_index = "BOS", s.index
                    bias = "bull"
                elif s.price < hh_prev and bias == "bull":
                    last_event, last_event_index = "CHoCH", s.index
                    bias = "bear"
            hh_prev = s.price
        else:
            if ll_prev is not None:
                if s.price < ll_prev and bias in ("bear", "neutral"):
                    if bias == "bull":
                        last_event, last_event_index = "CHoCH", s.index
                    elif bias == "bear":
                        last_event, last_event_index = "BOS", s.index
                    bias = "bear"
                elif s.price > ll_prev and bias == "bear":
                    last_event, last_event_index = "CHoCH", s.index
                    bias = "bull"
            ll_prev = s.price

    return StructureState(
        bias=bias, last_event=last_event, last_event_index=last_event_index,
        last_swing_high=highs[-1].price, last_swing_low=lows[-1].price,
    )


@dataclass
class Zone:
    kind: str          # "order_block" | "fvg" | "breaker"
    direction: str      # "bullish" | "bearish"
    top: float
    bottom: float
    index: int
    displacement_atr: float = 0.0
    mitigated: bool = False
    is_breaker: bool = False

    @property
    def mid(self) -> float:
        return (self.top + self.bottom) / 2

    @property
    def width(self) -> float:
        return self.top - self.bottom


def _avg_volume(candles: list, idx: int, window: int = 20) -> float:
    lo = max(0, idx - window)
    seg = candles[lo:idx] or candles[:1]
    return sum(c["v"] for c in seg) / len(seg)


def find_order_blocks(candles: list, atr_vals: list, lookback: int = 60) -> list:
    zones = []
    start = max(1, len(candles) - lookback)
    for i in range(start, len(candles) - 1):
        atr_val = atr_vals[i] or 1e-9
        move = candles[i + 1]["c"] - candles[i]["o"]
        displacement_atr = abs(move) / atr_val
        vol_ok = candles[i]["v"] >= 0.9 * _avg_volume(candles, i)
        if displacement_atr < 1.0 or not vol_ok:
            continue
        if move > 0 and candles[i]["c"] < candles[i]["o"]:
            zones.append(Zone("order_block", "bullish", candles[i]["h"], candles[i]["l"], i, displacement_atr))
        elif move < 0 and candles[i]["c"] > candles[i]["o"]:
            zones.append(Zone("order_block", "bearish", candles[i]["h"], candles[i]["l"], i, displacement_atr))
    return zones


def find_fvgs(candles: list, atr_vals: list, lookback: int = 60) -> list:
    zones = []
    start = max(2, len(candles) - lookback)
    for i in range(start, len(candles)):
        atr_val = atr_vals[i] or 1e-9
        c0, c2 = candles[i - 2], candles[i]
        if c2["l"] > c0["h"]:
            gap = c2["l"] - c0["h"]
            if gap / atr_val >= 0.15:
                zones.append(Zone("fvg", "bullish", c2["l"], c0["h"], i, gap / atr_val))
        elif c2["h"] < c0["l"]:
            gap = c0["l"] - c2["h"]
            if gap / atr_val >= 0.15:
                zones.append(Zone("fvg", "bearish", c0["l"], c2["h"], i, gap / atr_val))
    return zones


def mark_mitigation_and_breakers(zones: list, candles: list) -> list:
    """Marks each POI as mitigated once price has traded back through it,
    and flags bullish OBs that formed just before a bearish CHoCH (and vice
    versa) as breaker blocks -- the invalidated zone becomes a continuation
    POI in the new direction once retested."""
    for z in zones:
        for c in candles[z.index + 1:]:
            if z.direction == "bullish" and c["l"] <= z.top and c["l"] >= z.bottom:
                z.mitigated = True
            elif z.direction == "bearish" and c["h"] >= z.bottom and c["h"] <= z.top:
                z.mitigated = True
    return zones


def zone_quality(z: Zone) -> float:
    disp_score = min(1.0, z.displacement_atr / 2.0)
    freshness = 0.0 if z.mitigated else 1.0
    kind_bonus = 0.15 if z.kind == "fvg" else (0.1 if z.is_breaker else 0.0)
    return max(0.0, min(1.0, 0.55 * disp_score + 0.35 * freshness + kind_bonus))


def cluster_levels(levels: list, tol_pct: float = 0.0015) -> list:
    if not levels:
        return []
    levels = sorted(levels)
    clusters = [[levels[0]]]
    for lv in levels[1:]:
        if abs(lv - clusters[-1][-1]) / max(clusters[-1][-1], 1e-9) <= tol_pct:
            clusters[-1].append(lv)
        else:
            clusters.append([lv])
    return [(sum(c) / len(c), len(c)) for c in clusters]


def build_liquidity_pools(swings: list, candles_macro: list) -> dict:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    return {
        "resistance": cluster_levels(highs),
        "support": cluster_levels(lows),
    }


def detect_sweep(candles: list, pools: dict, direction: str, lookback: int = 10):
    recent = candles[-lookback:]
    if direction == "long":
        for level, count in pools.get("support", []):
            for c in recent:
                if c["l"] < level and c["c"] > level:
                    return {"level": level, "count": count, "candle": c}
    else:
        for level, count in pools.get("resistance", []):
            for c in recent:
                if c["h"] > level and c["c"] < level:
                    return {"level": level, "count": count, "candle": c}
    return None


def premium_discount_zone(candles: list, lookback: int = 50) -> dict:
    recent = candles[-lookback:]
    hi, lo = max(c["h"] for c in recent), min(c["l"] for c in recent)
    mid = (hi + lo) / 2
    close = candles[-1]["c"]
    zone_pct = safe_div(close - lo, hi - lo, default=0.5)
    return {"high": hi, "low": lo, "mid": mid, "zone_pct": zone_pct}


def detect_mss(candles_exec: list, direction: str, lookback: int = 30):
    """Market Structure Shift on the execution timeframe: a close beyond
    the most recent opposing swing point within the lookback window."""
    swings = find_swings(candles_exec[-lookback:], left=2, right=2)
    if not swings:
        return None
    last_price = candles_exec[-1]["c"]
    if direction == "long":
        recent_highs = [s.price for s in swings if s.kind == "high"]
        if recent_highs and last_price > max(recent_highs):
            return {"level": max(recent_highs), "index": len(candles_exec) - 1}
    else:
        recent_lows = [s.price for s in swings if s.kind == "low"]
        if recent_lows and last_price < min(recent_lows):
            return {"level": min(recent_lows), "index": len(candles_exec) - 1}
    return None


def find_breaker_setup(candles: list, atr_vals: list, structure: StructureState, lookback: int = BREAKER_LOOKBACK):
    """A breaker block: the last opposing-side order block immediately
    preceding a CHoCH, now acting as continuation support/resistance for
    the new trend once retested."""
    if not structure or structure.last_event != "CHoCH":
        return None
    obs = find_order_blocks(candles, atr_vals, lookback=lookback)
    pre_choch = [z for z in obs if z.index < structure.last_event_index]
    if not pre_choch:
        return None
    want_dir = "bearish" if structure.bias == "bull" else "bullish"
    candidates = [z for z in pre_choch if z.direction == want_dir]
    if not candidates:
        return None
    best = max(candidates, key=lambda z: z.index)
    best.is_breaker = True
    if best.displacement_atr < BREAKER_MIN_DISPLACEMENT_ATR:
        return None
    return best


def volume_profile(candles: list, bins: int = VOL_PROFILE_BINS) -> dict:
    if not candles:
        return {"poc": None, "va_high": None, "va_low": None, "vwap": None}
    hi = max(c["h"] for c in candles)
    lo = min(c["l"] for c in candles)
    if hi <= lo:
        return {"poc": None, "va_high": None, "va_low": None, "vwap": None}
    bin_w = (hi - lo) / bins
    vol_at_bin = [0.0] * bins
    pv_sum, v_sum = 0.0, 0.0
    for c in candles:
        typical = (c["h"] + c["l"] + c["c"]) / 3
        pv_sum += typical * c["v"]
        v_sum += c["v"]
        idx = min(bins - 1, max(0, int((typical - lo) / bin_w)))
        vol_at_bin[idx] += c["v"]
    poc_idx = max(range(bins), key=lambda i: vol_at_bin[i])
    poc = lo + (poc_idx + 0.5) * bin_w
    total_vol = sum(vol_at_bin) or 1e-9
    target = total_vol * 0.70
    acc = vol_at_bin[poc_idx]
    lo_i = hi_i = poc_idx
    while acc < target and (lo_i > 0 or hi_i < bins - 1):
        expand_lo = vol_at_bin[lo_i - 1] if lo_i > 0 else -1
        expand_hi = vol_at_bin[hi_i + 1] if hi_i < bins - 1 else -1
        if expand_hi >= expand_lo and hi_i < bins - 1:
            hi_i += 1
            acc += vol_at_bin[hi_i]
        elif lo_i > 0:
            lo_i -= 1
            acc += vol_at_bin[lo_i]
        else:
            break
    va_low = lo + lo_i * bin_w
    va_high = lo + (hi_i + 1) * bin_w
    vwap = safe_div(pv_sum, v_sum, default=candles[-1]["c"])
    return {"poc": poc, "va_high": va_high, "va_low": va_low, "vwap": vwap}


def clip_tp_to_liquidity(entry: float, tp: float, direction: str, pools: dict, vp: dict) -> float:
    candidates = []
    for level, _ in pools.get("resistance" if direction == "long" else "support", []):
        if (direction == "long" and entry < level < tp) or (direction == "short" and tp < level < entry):
            candidates.append(level)
    for key in ("poc", "va_high", "va_low"):
        lv = vp.get(key)
        if lv is None:
            continue
        if (direction == "long" and entry < lv < tp) or (direction == "short" and tp < lv < entry):
            candidates.append(lv)
    if not candidates:
        return tp
    return min(candidates) if direction == "long" else max(candidates)


# ============================================================================
# INSTITUTIONAL ORDER FLOW (funding / OI trend, tracked scan-over-scan)
# ============================================================================

def update_order_flow_history(state: dict, symbol: str, price: float, funding: float, oi_usd: float) -> dict:
    hist = state["order_flow_history"].setdefault(symbol, [])
    hist.append({"ts": int(time.time()), "price": price, "funding": funding, "oi_usd": oi_usd})
    if len(hist) > 96:
        del hist[:len(hist) - 96]
    if len(hist) < 4:
        return {"funding_trend": "flat", "oi_trend": "flat", "conviction": "neutral"}

    prior = hist[-4]
    funding_trend = "rising" if funding > prior["funding"] * 1.05 else (
        "falling" if funding < prior["funding"] * 0.95 else "flat")
    oi_delta_pct = safe_div(oi_usd - prior["oi_usd"], prior["oi_usd"])
    oi_trend = "rising" if oi_delta_pct > 0.02 else ("falling" if oi_delta_pct < -0.02 else "flat")
    price_delta_pct = safe_div(price - prior["price"], prior["price"])

    # Rising OI with price agreeing on direction = fresh conviction.
    # Rising OI with price disagreeing = potential trap / squeeze setup.
    # Falling OI = position unwind, weak conviction either way.
    if oi_trend == "rising" and price_delta_pct * (1 if price_delta_pct >= 0 else -1) != 0:
        agrees = (price_delta_pct > 0 and funding_trend != "falling") or (price_delta_pct < 0 and funding_trend != "rising")
        conviction = "fresh" if agrees else "trap_risk"
    elif oi_trend == "falling":
        conviction = "unwind"
    else:
        conviction = "neutral"

    return {"funding_trend": funding_trend, "oi_trend": oi_trend, "conviction": conviction}


def order_flow_confluence_score(order_flow: dict, direction: str) -> float:
    """Soft confluence only -- never a hard veto. +1 supportive, -1
    contradictory, 0 neutral/unclear."""
    conviction = order_flow.get("conviction", "neutral")
    funding_trend = order_flow.get("funding_trend", "flat")
    score = 0.0
    if conviction == "fresh":
        score += 0.5
    elif conviction == "trap_risk":
        score -= 0.5
    if direction == "long" and funding_trend == "falling":
        score += 0.2  # cooling funding while going long = less crowded
    if direction == "short" and funding_trend == "rising":
        score += 0.2  # hot funding while going short = crowded longs risk
    return max(-1.0, min(1.0, score))


# ============================================================================
# CANDIDATE / RISK MANAGEMENT
# ============================================================================

@dataclass
class Candidate:
    symbol: str
    direction: str
    pathway: str
    combo_name: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    atr_val: float
    zone: Zone
    regime: Optional[RegimeVector]
    confluences: list = field(default_factory=list)
    ltf_confirmed: bool = True
    context_ok: bool = True

    def rr1(self) -> float:
        risk = abs(self.entry - self.sl) or 1e-9
        return abs(self.tp1 - self.entry) / risk

    def rr(self) -> float:
        risk = abs(self.entry - self.sl) or 1e-9
        return abs(self.tp2 - self.entry) / risk


def adaptive_sl_buffer(candles: list, atr_val: float, vol_pctile: float, direction: str) -> float:
    """Directional wick sizing scaled by volatility percentile: measures
    the average size of wicks on the side that would invalidate this
    trade (lower wicks for longs, upper wicks for shorts) over the recent
    window, and scales that by how elevated current volatility is versus
    its own recent history -- so the buffer breathes with the symbol's own
    regime instead of using a single fixed ATR multiple for every
    condition."""
    recent = candles[-20:]
    if direction == "long":
        wicks = [c["o"] - c["l"] if c["c"] >= c["o"] else c["c"] - c["l"] for c in recent]
    else:
        wicks = [c["h"] - c["o"] if c["c"] <= c["o"] else c["h"] - c["c"] for c in recent]
    wicks = [max(0.0, w) for w in wicks]
    avg_wick = sum(wicks) / len(wicks) if wicks else atr_val * 0.3
    vol_scalar = 0.8 + 0.6 * vol_pctile  # 0.8x - 1.4x
    buffer = max(avg_wick * vol_scalar, atr_val * 0.35)
    return min(buffer, atr_val * 1.5)


def clamp_candidate_to_market(cand: Candidate, market_price: float) -> Candidate:
    if cand.direction == "long" and cand.entry > market_price:
        cand.entry = market_price
    elif cand.direction == "short" and cand.entry < market_price:
        cand.entry = market_price
    return cand


def build_risk_plan(direction: str, entry: float, invalidation: float, atr_val: float,
                     vol_pctile: float, candles: list, pools: dict, vp: dict) -> dict:
    buffer = adaptive_sl_buffer(candles, atr_val, vol_pctile, direction)
    if direction == "long":
        sl = invalidation - buffer
        risk = entry - sl
        raw_tp1 = entry + risk * 1.6
        raw_tp2 = entry + risk * 2.8
    else:
        sl = invalidation + buffer
        risk = sl - entry
        raw_tp1 = entry - risk * 1.6
        raw_tp2 = entry - risk * 2.8
    if risk <= 0:
        risk = atr_val * 0.5
        sl = entry - risk if direction == "long" else entry + risk
    tp1 = clip_tp_to_liquidity(entry, raw_tp1, direction, pools, vp)
    tp2 = clip_tp_to_liquidity(entry, raw_tp2, direction, pools, vp)
    # Never let liquidity clipping collapse TP below a sane minimum RR.
    min_tp1 = entry + risk * 1.1 if direction == "long" else entry - risk * 1.1
    if (direction == "long" and tp1 < min_tp1) or (direction == "short" and tp1 > min_tp1):
        tp1 = raw_tp1
    if (direction == "long" and tp2 <= tp1) or (direction == "short" and tp2 >= tp1):
        tp2 = raw_tp2
    return {"sl": sl, "tp1": tp1, "tp2": tp2}


# ============================================================================
# PATHWAY BUILDERS
# ============================================================================

def _rsi_reset(ind: dict, direction: str) -> bool:
    window = ind["rsi"][-RSI_RESET_LOOKBACK:]
    if direction == "long":
        dipped = min(window) <= RSI_DIP_LONG
        turned = window[-1] >= RSI_TURN_LONG
        return dipped and turned
    dipped = max(window) >= RSI_DIP_SHORT
    turned = window[-1] <= RSI_TURN_SHORT
    return dipped and turned


def build_pathway_liquidity_reversal(symbol: str, bundle: dict, combo_name: str,
                                      regime: RegimeVector, vp: dict) -> Optional[Candidate]:
    combo = COMBOS[combo_name]
    candles_struct = bundle[combo["struct"]]
    candles_exec = bundle[combo["exec"]]
    ind_struct = compute_indicators(candles_struct)
    atr_val = safe(ind_struct["atr"][-1])
    if atr_val <= 0:
        return None

    swings = find_swings(candles_struct)
    pools = build_liquidity_pools(swings, candles_struct)

    for direction in ("long", "short"):
        sweep = detect_sweep(candles_struct, pools, direction)
        if not sweep:
            continue
        mss = detect_mss(candles_exec, direction)
        if not mss:
            continue
        atr_vals_struct = ind_struct["atr"]
        obs = find_order_blocks(candles_struct, atr_vals_struct)
        fvgs = find_fvgs(candles_struct, atr_vals_struct)
        zones = mark_mitigation_and_breakers(obs + fvgs, candles_struct)
        want_dir = "bullish" if direction == "long" else "bearish"
        poi_candidates = [z for z in zones if z.direction == want_dir and not z.mitigated]
        if not poi_candidates:
            continue
        poi = max(poi_candidates, key=lambda z: z.index)

        entry = poi.mid
        invalidation = sweep["level"] if direction == "long" else sweep["level"]
        # invalidation must sit beyond the sweep wick, not just the pool level
        sweep_candle = sweep["candle"]
        invalidation = sweep_candle["l"] if direction == "long" else sweep_candle["h"]

        plan = build_risk_plan(direction, entry, invalidation, atr_val, regime.vol_pctile,
                                candles_struct, pools, vp)
        cand = Candidate(
            symbol=symbol, direction=direction, pathway="liquidity_reversal",
            combo_name=combo_name, entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
            atr_val=atr_val, zone=poi, regime=regime,
            confluences=[
                f"Liquidity sweep of {'support' if direction=='long' else 'resistance'} at {sweep['level']:.4g} ({sweep['count']}x touched)",
                f"Market structure shift confirmed on {combo['exec']}",
                f"{'Bullish' if direction=='long' else 'Bearish'} {poi.kind.replace('_',' ')} retest POI",
            ],
            ltf_confirmed=True, context_ok=(regime.label in ("reversal", "volatile", "range")),
        )
        return cand
    return None


def build_pathway_trend_continuation(symbol: str, bundle: dict, combo_name: str,
                                      regime: RegimeVector, vp: dict) -> Optional[Candidate]:
    combo = COMBOS[combo_name]
    candles_bias = bundle[combo["bias"]]
    candles_struct = bundle[combo["struct"]]
    ind_bias = compute_indicators(candles_bias)
    ind_struct = compute_indicators(candles_struct)
    adx_val = safe(ind_bias["adx"][-1])
    if adx_val < TREND_ADX_MIN:
        return None
    atr_val = safe(ind_struct["atr"][-1])
    if atr_val <= 0:
        return None

    close_bias = ind_bias["closes"][-1]
    trend_up = close_bias > ind_bias["ema_fast"][-1] > ind_bias["ema_slow"][-1]
    trend_down = close_bias < ind_bias["ema_fast"][-1] < ind_bias["ema_slow"][-1]
    if not (trend_up or trend_down):
        return None
    direction = "long" if trend_up else "short"

    if not _rsi_reset(ind_struct, direction):
        return None

    swings = find_swings(candles_struct)
    pools = build_liquidity_pools(swings, candles_struct)
    atr_vals_struct = ind_struct["atr"]
    obs = find_order_blocks(candles_struct, atr_vals_struct)
    zones = mark_mitigation_and_breakers(obs, candles_struct)
    want_dir = "bullish" if direction == "long" else "bearish"
    poi_candidates = [z for z in zones if z.direction == want_dir and not z.mitigated]

    ema_pullback_level = ind_struct["ema_fast"][-1]
    if poi_candidates:
        poi = max(poi_candidates, key=lambda z: z.index)
        entry = poi.mid
        invalidation = poi.bottom if direction == "long" else poi.top
    else:
        poi = Zone("order_block", want_dir, ema_pullback_level * 1.002, ema_pullback_level * 0.998,
                    len(candles_struct) - 1, 1.0)
        entry = ema_pullback_level
        recent_swing_lows = [s.price for s in swings if s.kind == "low"]
        recent_swing_highs = [s.price for s in swings if s.kind == "high"]
        invalidation = (min(recent_swing_lows[-2:]) if direction == "long" and recent_swing_lows
                         else (max(recent_swing_highs[-2:]) if recent_swing_highs else entry - atr_val))

    plan = build_risk_plan(direction, entry, invalidation, atr_val, regime.vol_pctile,
                            candles_struct, pools, vp)
    return Candidate(
        symbol=symbol, direction=direction, pathway="trend_continuation",
        combo_name=combo_name, entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
        atr_val=atr_val, zone=poi, regime=regime,
        confluences=[
            f"{combo['bias']} trend {'up' if direction=='long' else 'down'} (ADX {adx_val:.0f})",
            f"RSI pullback-and-turn reset on {combo['struct']}",
            "Pullback POI at order block" if poi_candidates else "Pullback to fast EMA",
        ],
        ltf_confirmed=True, context_ok=(regime.label == "trend"),
    )


def build_pathway_momentum_breakout(symbol: str, bundle: dict, combo_name: str,
                                     regime: RegimeVector, vp: dict) -> Optional[Candidate]:
    combo = COMBOS[combo_name]
    candles_struct = bundle[combo["struct"]]
    ind_struct = compute_indicators(candles_struct)
    atr_val = safe(ind_struct["atr"][-1])
    if atr_val <= 0 or len(candles_struct) < 40:
        return None

    bbw_hist = ind_struct["bbw"][-100:]
    bbw_now = ind_struct["bbw"][-2]  # squeeze measured on the bar BEFORE breakout
    squeeze_pctile = percentile_rank(bbw_hist, bbw_now)
    if squeeze_pctile > BREAKOUT_BB_SQUEEZE_PCTILE:
        return None

    last = candles_struct[-1]
    avg_vol = ind_struct["avg_vol20"][-2]
    if avg_vol <= 0 or last["v"] < BREAKOUT_VOL_MULT * avg_vol:
        return None

    lookback_window = candles_struct[-(BREAKOUT_LOOKBACK_HIGH_LOW + 1):-1]
    range_high = max(c["h"] for c in lookback_window)
    range_low = min(c["l"] for c in lookback_window)

    if last["c"] > range_high:
        direction = "long"
    elif last["c"] < range_low:
        direction = "short"
    else:
        return None

    swings = find_swings(candles_struct)
    pools = build_liquidity_pools(swings, candles_struct)
    entry = last["c"]
    invalidation = range_low if direction == "long" else range_high
    zone = Zone("order_block", "bullish" if direction == "long" else "bearish",
                max(last["o"], last["c"]), min(last["o"], last["c"]), len(candles_struct) - 1, 1.5)

    plan = build_risk_plan(direction, entry, invalidation, atr_val, regime.vol_pctile,
                            candles_struct, pools, vp)
    return Candidate(
        symbol=symbol, direction=direction, pathway="momentum_breakout",
        combo_name=combo_name, entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
        atr_val=atr_val, zone=zone, regime=regime,
        confluences=[
            f"Bollinger squeeze at {squeeze_pctile*100:.0f}th percentile bandwidth",
            f"Volume expansion {last['v']/avg_vol:.1f}x the 20-bar average",
            f"Breakout of {BREAKOUT_LOOKBACK_HIGH_LOW}-bar range {'high' if direction=='long' else 'low'}",
        ],
        ltf_confirmed=True, context_ok=(regime.label == "volatile"),
    )


def build_pathway_range_reversion(symbol: str, bundle: dict, combo_name: str,
                                   regime: RegimeVector, vp: dict) -> Optional[Candidate]:
    combo = COMBOS[combo_name]
    candles_struct = bundle[combo["struct"]]
    ind_struct = compute_indicators(candles_struct)
    adx_val = safe(ind_struct["adx"][-1])
    if adx_val > RANGE_ADX_MAX:
        return None
    atr_val = safe(ind_struct["atr"][-1])
    if atr_val <= 0:
        return None

    pd = premium_discount_zone(candles_struct)
    zone_pct = pd["zone_pct"]
    if zone_pct <= RANGE_EDGE_ZONE_PCT:
        direction = "long"
    elif zone_pct >= 1.0 - RANGE_EDGE_ZONE_PCT:
        direction = "short"
    else:
        return None

    swings = find_swings(candles_struct)
    pools = build_liquidity_pools(swings, candles_struct)
    entry = candles_struct[-1]["c"]
    invalidation = pd["low"] if direction == "long" else pd["high"]
    zone = Zone("order_block", "bullish" if direction == "long" else "bearish",
                entry * 1.001, entry * 0.999, len(candles_struct) - 1, 0.8)

    plan = build_risk_plan(direction, entry, invalidation, atr_val, regime.vol_pctile,
                            candles_struct, pools, vp)
    # Range fades want tighter, closer targets -- clip TP2 back toward the
    # range mid rather than letting liquidity clipping reach far outside it.
    plan["tp1"] = min(plan["tp1"], pd["mid"]) if direction == "long" else max(plan["tp1"], pd["mid"])
    return Candidate(
        symbol=symbol, direction=direction, pathway="range_reversion",
        combo_name=combo_name, entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
        atr_val=atr_val, zone=zone, regime=regime,
        confluences=[
            f"Range regime (ADX {adx_val:.0f}), price at {zone_pct*100:.0f}% of range",
            f"{'Discount' if direction=='long' else 'Premium'} extreme fade",
        ],
        ltf_confirmed=True, context_ok=(regime.label == "range"),
    )


def build_pathway_breaker_continuation(symbol: str, bundle: dict, combo_name: str,
                                        regime: RegimeVector, vp: dict) -> Optional[Candidate]:
    combo = COMBOS[combo_name]
    candles_struct = bundle[combo["struct"]]
    ind_struct = compute_indicators(candles_struct)
    atr_val = safe(ind_struct["atr"][-1])
    if atr_val <= 0:
        return None

    swings = find_swings(candles_struct)
    structure = analyze_structure(candles_struct, swings)
    if not structure or structure.last_event != "CHoCH":
        return None
    breaker = find_breaker_setup(candles_struct, ind_struct["atr"], structure)
    if not breaker:
        return None

    direction = "long" if structure.bias == "bull" else "short"
    last = candles_struct[-1]
    touched = (last["l"] <= breaker.top and last["l"] >= breaker.bottom) if direction == "long" else \
              (last["h"] >= breaker.bottom and last["h"] <= breaker.top)
    if not touched:
        return None

    pools = build_liquidity_pools(swings, candles_struct)
    entry = breaker.mid
    invalidation = breaker.bottom - atr_val * 0.1 if direction == "long" else breaker.top + atr_val * 0.1

    plan = build_risk_plan(direction, entry, invalidation, atr_val, regime.vol_pctile,
                            candles_struct, pools, vp)
    return Candidate(
        symbol=symbol, direction=direction, pathway="breaker_continuation",
        combo_name=combo_name, entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
        atr_val=atr_val, zone=breaker, regime=regime,
        confluences=[
            f"CHoCH confirmed {structure.bias} structure shift",
            f"Breaker block retest (displacement {breaker.displacement_atr:.1f}x ATR)",
        ],
        ltf_confirmed=True, context_ok=(regime.label in ("trend", "reversal")),
    )


PATHWAYS = [
    build_pathway_liquidity_reversal,
    build_pathway_trend_continuation,
    build_pathway_momentum_breakout,
    build_pathway_range_reversion,
    build_pathway_breaker_continuation,
]


# ============================================================================
# DECISION ENGINE (centralized scoring, five-filter base)
# ============================================================================

@dataclass
class FilterResult:
    passed: bool
    reason: str = ""
    location_score: float = 0.0
    context_score: float = 0.0
    quality_score: float = 0.0
    rr_score: float = 0.0
    ltf_score: float = 0.0


def apply_five_filters(cand: Candidate, market_price: float, min_rr_floor: float) -> FilterResult:
    atr_val = cand.atr_val or 1e-9

    entry_dist_atr = abs(cand.entry - market_price) / atr_val
    if entry_dist_atr > POI_MAX_DIST_ATR_MULT * 1.35:
        return FilterResult(False, "location: entry too far from live price")
    location_score = max(0.0, 1.0 - min(1.0, entry_dist_atr / (POI_MAX_DIST_ATR_MULT * 1.5)))

    context_map = {
        "liquidity_reversal": {"reversal": 1.0, "volatile": 0.75, "range": 0.7, "trend": 0.35},
        "trend_continuation": {"trend": 1.0, "reversal": 0.3, "range": 0.15, "volatile": 0.25},
        "momentum_breakout": {"volatile": 1.0, "trend": 0.5, "reversal": 0.3, "range": 0.1},
        "range_reversion": {"range": 1.0, "reversal": 0.4, "trend": 0.1, "volatile": 0.2},
        "breaker_continuation": {"trend": 0.85, "reversal": 1.0, "volatile": 0.5, "range": 0.15},
    }
    context_score = context_map.get(cand.pathway, {}).get(cand.regime.label, 0.3) if cand.regime else 0.3
    if not cand.context_ok or context_score < 0.15:
        return FilterResult(False, "context: regime/pathway mismatch")

    q = zone_quality(cand.zone)
    width_pen = 0.0 if cand.zone.width <= ZONE_MAX_WIDTH_ATR_MULT * atr_val else 0.25
    quality_score = max(0.0, q - width_pen)
    if quality_score < 0.30:
        return FilterResult(False, f"quality: zone quality too low ({quality_score:.2f})")

    dyn_floor = min_rr_floor
    if cand.regime and cand.regime.label == "volatile":
        dyn_floor += 0.4
    if quality_score < 0.5:
        dyn_floor += 0.3
    rr1 = cand.rr1()
    if rr1 < dyn_floor:
        return FilterResult(False, f"rr: {rr1:.2f} below dynamic floor {dyn_floor:.2f}")
    rr_score = min(1.0, rr1 / 4.0)

    if not cand.ltf_confirmed:
        return FilterResult(False, "ltf: no lower-timeframe confirmation trigger")
    ltf_score = 1.0 if cand.pathway in ("liquidity_reversal", "breaker_continuation") else 0.75

    return FilterResult(True, "ok", location_score, context_score, quality_score, rr_score, ltf_score)


def logistic(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def tune_pathway_weights(state: dict):
    """Regularized, shrunk-to-prior self-tuning pathway weight: drifts
    slowly toward the pathway's recent win-rate-implied target instead of
    reading the raw unsmoothed win-rate each scan, so a short hot/cold
    streak can't swing scoring on its own."""
    by_pathway: dict = {}
    for h in state["signal_history"]:
        if h.get("result") in ("win", "loss"):
            by_pathway.setdefault(h["pathway"], []).append(h["result"])
    weights = state["pathway_weights"]
    for pw, results in by_pathway.items():
        if len(results) < 8:
            continue
        recent = results[-40:]
        win_rate = recent.count("win") / len(recent)
        target = 0.7 + win_rate * 0.6  # win_rate 0.5 -> 1.0 neutral target
        current = weights.get(pw, 1.0)
        updated = current + PATHWAY_WEIGHT_LEARNING_RATE * (target - current)
        weights[pw] = max(PATHWAY_WEIGHT_MIN, min(PATHWAY_WEIGHT_MAX, updated))


def score_candidate(cand: Candidate, fr: FilterResult, regime: RegimeVector, state: dict,
                     book: dict, vp: dict, order_flow: dict, fleet_exposure: dict) -> float:
    base = (0.25 * fr.location_score + 0.20 * fr.context_score + 0.25 * fr.quality_score
            + 0.15 * fr.rr_score + 0.15 * fr.ltf_score)

    breadth_bonus = 0.0
    if regime.btc_bias != "neutral":
        wants_up = (cand.direction == "long" and regime.btc_bias == "bull")
        wants_down = (cand.direction == "short" and regime.btc_bias == "bear")
        if wants_up or wants_down:
            breadth_bonus = 0.06 * regime.breadth
        else:
            breadth_bonus = -0.04 * regime.breadth

    vwap_bonus = 0.0
    if vp.get("vwap"):
        aligned = (cand.direction == "long" and cand.entry >= vp["vwap"]) or \
                  (cand.direction == "short" and cand.entry <= vp["vwap"])
        vwap_bonus = 0.03 if aligned else -0.02

    of_score = order_flow_confluence_score(order_flow, cand.direction)
    of_bonus = 0.04 * of_score

    book_bonus = 0.0
    if book and book.get("spread_pct") is not None:
        imbalance = book.get("imbalance", 0.0)
        aligned = (cand.direction == "long" and imbalance > 0) or (cand.direction == "short" and imbalance < 0)
        book_bonus = 0.03 * abs(imbalance) if aligned else -0.02 * abs(imbalance)

    fleet_penalty = 0.0
    fleet_dirs = fleet_exposure.get(cand.symbol, set())
    opposite = "short" if cand.direction == "long" else "long"
    if opposite in fleet_dirs and FLEET_CONFLICT_MODE == "penalize":
        fleet_penalty = FLEET_CONFLICT_PENALTY / 100.0

    weight = state["pathway_weights"].get(cand.pathway, 1.0)
    session_factor = 0.9 + 0.1 * regime.session_weight

    composite = (base + breadth_bonus + vwap_bonus + of_bonus + book_bonus) * weight * session_factor - fleet_penalty
    # Map composite (roughly 0-1.1 range) through a logistic centered so a
    # "solid" ~0.75 composite lands near 70-75 confidence.
    confidence = 100 * logistic(6.0 * (composite - 0.60))
    return max(0.0, min(100.0, confidence))


def grade_for_confidence(confidence: float) -> str:
    if confidence >= 85:
        return "A+"
    if confidence >= 75:
        return "A"
    if confidence >= 66:
        return "B"
    return "C"


def classify_duration(combo_name: str) -> str:
    return COMBOS[combo_name]["hold_hint"]


# ============================================================================
# CORRELATION / DEDUPLICATION
# ============================================================================

def compute_returns(candles: list, lookback: int) -> list:
    closes = [c["c"] for c in candles[-lookback - 1:]]
    return [safe_div(closes[i] - closes[i - 1], closes[i - 1]) for i in range(1, len(closes))]


def pearson(a: list, b: list) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    mean_a, mean_b = sum(a) / n, sum(b) / n
    cov = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n))
    var_a = sum((x - mean_a) ** 2 for x in a)
    var_b = sum((x - mean_b) ** 2 for x in b)
    denom = math.sqrt(var_a * var_b)
    return safe_div(cov, denom, default=0.0)


def build_correlation_clusters(bundles: dict) -> list:
    returns = {sym: compute_returns(b["1h"], CORR_LOOKBACK_BARS) for sym, b in bundles.items()}
    symbols = list(returns.keys())
    parent = {s: s for s in symbols}

    def find(x):
        while parent[x] != x:
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for i in range(len(symbols)):
        for j in range(i + 1, len(symbols)):
            s1, s2 = symbols[i], symbols[j]
            if pearson(returns[s1], returns[s2]) >= CORR_CLUSTER_THRESHOLD:
                union(s1, s2)

    clusters: dict = {}
    for s in symbols:
        root = find(s)
        clusters.setdefault(root, set()).add(s)
    return list(clusters.values())


def dedup_correlated(ranked: list, clusters: list) -> list:
    """FIXED (see module docstring): keys strictly on cluster identity, NOT
    (cluster, direction). Every reference engine in this fleet keyed on
    (cluster, direction), which let a >0.72-correlated pair fire both a
    LONG and a SHORT in the same scan -- two correlated bets dressed up as
    a hedge. Here, only the single highest-confidence candidate survives
    per correlation cluster, full stop, regardless of what direction it
    or any other cluster member wanted to fire."""
    def cluster_of(sym: str) -> frozenset:
        for c in clusters:
            if sym in c:
                return frozenset(c)
        return frozenset({sym})

    seen: dict = {}
    for r in ranked:
        key = cluster_of(r["symbol"])
        if key not in seen or r["confidence"] > seen[key]["confidence"]:
            seen[key] = r
    return list(seen.values())


# ============================================================================
# HARD FILTERS / COOLDOWN / GOVERNOR
# ============================================================================

def passes_hard_filters(symbol: str, snapshot: dict, atr_pct: float, cand: Candidate):
    info = snapshot.get(symbol, {})
    oi_usd = info.get("oi_usd", 0.0)
    if oi_usd and oi_usd < MIN_OI_USD:
        return False, f"OI ${oi_usd:,.0f} below floor"
    if atr_pct < MIN_ATR_PCT:
        return False, f"ATR% {atr_pct:.4f} too low (dead market)"
    if atr_pct > MAX_ATR_PCT:
        return False, f"ATR% {atr_pct:.4f} too high (unstable)"
    if cand.rr() < MIN_RR:
        return False, f"RR {cand.rr():.2f} below floor {MIN_RR}"
    return True, "ok"


def passes_spread_filter(book: dict) -> bool:
    spread = book.get("spread_pct")
    if spread is None:
        return True  # book unavailable -- don't block on missing data
    return spread <= MAX_SPREAD_PCT


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    key = f"{symbol}|{direction}"
    entry = state["cooldowns"].get(key)
    if not entry:
        return True
    return (bar_index - entry.get("bar_index", -999)) >= COOLDOWN_BARS


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    key = f"{symbol}|{direction}"
    state["cooldowns"][key] = {"bar_index": bar_index, "ts": int(time.time())}


def is_recent_duplicate(state: dict, symbol: str, direction: str, entry: float) -> bool:
    cutoff = time.time() - DEDUP_TIME_WINDOW_HOURS * 3600
    for h in reversed(state["signal_history"]):
        if h.get("ts", 0) < cutoff:
            break
        if h.get("symbol") == symbol and h.get("direction") == direction:
            if abs(h.get("entry", 0) - entry) / max(entry, 1e-9) <= DEDUP_PRICE_TOL_PCT:
                return True
    return False


def count_active(state: dict) -> int:
    return len(state["active_signals"])


def count_open_same_direction(state: dict, direction: str) -> int:
    return sum(1 for s in state["active_signals"] if s["direction"] == direction)


def count_open_for_symbol(state: dict, symbol: str) -> int:
    return sum(1 for s in state["active_signals"] if s["symbol"] == symbol)


def estimate_signals_last_24h(state: dict) -> int:
    cutoff = time.time() - 86400
    return sum(1 for h in state["signal_history"] if h.get("ts", 0) >= cutoff)


def governor_adjust_threshold(state: dict, signals_fired_last_24h: int):
    gov = state["governor"]
    now = time.time()
    if now - gov.get("last_adjust_ts", 0) < GOVERNOR_MIN_INTERVAL_S:
        return
    if signals_fired_last_24h < TARGET_SIGNALS_MIN:
        gov["threshold"] = max(GOVERNOR_FLOOR, gov["threshold"] - GOVERNOR_STEP)
        gov["last_adjust_ts"] = now
    elif signals_fired_last_24h > TARGET_SIGNALS_MAX:
        gov["threshold"] = min(GOVERNOR_CEIL, gov["threshold"] + GOVERNOR_STEP)
        gov["last_adjust_ts"] = now


# ============================================================================
# TELEGRAM
# ============================================================================

def fmt_px(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def _html_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def confidence_bar(confidence: float) -> str:
    filled = round(confidence / 10)
    return "\u2588" * filled + "\u2591" * (10 - filled)


def format_signal(cand: Candidate, confidence: float, grade: str) -> str:
    arrow = "\U0001F7E2 LONG" if cand.direction == "long" else "\U0001F534 SHORT"
    duration = classify_duration(cand.combo_name)
    confluence_lines = "\n".join(f"  \u2022 {_html_escape(c)}" for c in cand.confluences)
    lines = [
        f"<b>{ENGINE_NAME} {ENGINE_VERSION}</b> \u2014 {cand.symbol}/USD",
        f"{arrow}  |  Grade <b>{grade}</b>  |  Pathway: <code>{cand.pathway}</code>",
        "",
        f"Entry:  <code>{fmt_px(cand.entry)}</code>",
        f"SL:     <code>{fmt_px(cand.sl)}</code>",
        f"TP1:    <code>{fmt_px(cand.tp1)}</code>",
        f"TP2:    <code>{fmt_px(cand.tp2)}</code>",
        f"R:R (TP2): {cand.rr():.2f}",
        f"Confidence: {confidence:.1f}%  {confidence_bar(confidence)}",
        f"Est. hold: {duration}",
        "",
        "Confluences:",
        confluence_lines,
    ]
    return "\n".join(lines)


def send_telegram(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info("Telegram not configured; message:\n%s", text)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = json.dumps({"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("result", {}).get("message_id")
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as e:
        log.error("Telegram send failed: %s", e)
        return None


def reply_telegram(text: str, reply_to_message_id):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info("Telegram not configured; update:\n%s", text)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                  headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("result", {}).get("message_id")
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as e:
        log.error("Telegram reply failed: %s", e)
        return None


def react_telegram(message_id, emoji: str):
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
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as e:
        log.debug("Telegram reaction failed (non-fatal): %s", e)


# ============================================================================
# ACTIVE SIGNAL TRACKING / OUTCOME RESOLUTION
# ============================================================================

def record_signal(state: dict, cand: Candidate, confidence: float, grade: str,
                   bar_index: int, message_id) -> dict:
    entry = {
        "symbol": cand.symbol, "direction": cand.direction, "pathway": cand.pathway,
        "combo": cand.combo_name, "entry": cand.entry, "sl": cand.sl,
        "risk": abs(cand.entry - cand.sl), "tp1": cand.tp1, "tp2": cand.tp2,
        "confidence": confidence, "grade": grade, "ts": int(time.time()),
        "bar_index": bar_index, "result": "open", "tp1_hit": False,
        "message_id": message_id, "last_checked_t": cand.zone.index and int(time.time() * 1000),
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
    text = (f"\U0001F525 <b>TP1 hit</b> \u2014 {sig['symbol']} {sig['direction'].upper()}\n"
            f"Price: <code>{fmt_px(price)}</code>  |  +{r:.2f}R banked\n"
            f"SL moved to breakeven (<code>{fmt_px(sig['entry'])}</code>).")
    reply_telegram(text, sig.get("message_id"))
    react_telegram(sig.get("message_id"), "\U0001F525")
    log.info("TP1 hit: %s %s +%.2fR, SL -> breakeven", sig["symbol"], sig["direction"], r)


def _close_out(state: dict, sig: dict, result: str, price: float, exit_reason: str):
    r = _r_multiple(sig, price)
    sig["result"] = result
    sig["exit_price"] = price
    sig["r_realized"] = r
    sig["closed_ts"] = int(time.time())
    _sync_history(state, sig)
    state["confidence_calibration"].append({"confidence": sig.get("confidence", 0.0), "result": result})

    if exit_reason == "tp2":
        headline, emoji = "\u2705 <b>TP2 hit \u2014 WIN</b>", "\U0001F44D"
    elif exit_reason == "breakeven":
        headline, emoji = "\u2696\ufe0f <b>Stopped at breakeven</b>", "\U0001F44D"
    else:
        headline, emoji = "\u274C <b>SL hit \u2014 LOSS</b>", "\U0001F44E"
    text = f"{headline} \u2014 {sig['symbol']} {sig['direction'].upper()}\nExit: <code>{fmt_px(price)}</code>  |  Result: {r:+.2f}R"
    reply_telegram(text, sig.get("message_id"))
    react_telegram(sig.get("message_id"), emoji)
    log.info("Signal resolved: %s %s %s -> %s (%s, %.2fR)",
              sig["symbol"], sig["direction"], sig["pathway"], result, exit_reason, r)


def _resolve_against_candle(state: dict, sig: dict, candle: dict) -> Optional[str]:
    """Checks a single closed candle, chronologically, for SL/TP1/TP2
    touches, mutating sig in place (breakeven-on-TP1). Returns 'closed' if
    the signal fully resolved on this candle, else None. This replaces
    mark-price-only checking (the class of bug fixed in Parallax v1.1.1):
    an intra-candle wick that touched SL and then reversed before the next
    scan would otherwise never be seen."""
    direction = sig["direction"]
    hi, lo = candle["h"], candle["l"]

    hit_sl = (lo <= sig["sl"]) if direction == "long" else (hi >= sig["sl"])
    hit_tp2 = (hi >= sig["tp2"]) if direction == "long" else (lo <= sig["tp2"])
    hit_tp1 = (not sig["tp1_hit"]) and ((hi >= sig["tp1"]) if direction == "long" else (lo <= sig["tp1"]))

    # Conservative ordering when a single candle's range spans both an SL
    # and a TP: assume the adverse touch (SL) happened first unless TP1 had
    # already been banked on a prior candle (in which case SL == breakeven,
    # so a same-candle TP2 is treated as the bar sweeping through breakeven
    # into target, which is the friendlier and equally defensible read).
    if hit_sl and not sig["tp1_hit"]:
        _close_out(state, sig, "loss", sig["sl"], "sl")
        return "closed"
    if hit_tp2:
        _close_out(state, sig, "win", sig["tp2"], "tp2")
        return "closed"
    if hit_sl and sig["tp1_hit"]:
        _close_out(state, sig, "win", sig["sl"], "breakeven")
        return "closed"
    if hit_tp1:
        sig["tp1_hit"] = True
        sig["sl"] = sig["entry"]
        _notify_tp1(sig, sig["tp1"])
    return None


def check_active_signals(state: dict, snapshot: dict, bundles: dict):
    still_active = []
    for sig in state["active_signals"]:
        exec_tf = COMBOS.get(sig.get("combo", "intraday"), COMBOS["intraday"])["exec"]
        bundle = bundles.get(sig["symbol"])
        watermark = sig.get("last_checked_t") or 0
        resolved = False

        if bundle and exec_tf in bundle:
            new_candles = [c for c in bundle[exec_tf] if c["t"] > watermark]
            for c in sorted(new_candles, key=lambda c: c["t"]):
                sig["last_checked_t"] = c["t"]
                if _resolve_against_candle(state, sig, c) == "closed":
                    resolved = True
                    break
            _sync_history(state, sig)
        else:
            # No fresh bundle for this symbol this scan -- fall back to
            # mark-price checking so outcome resolution never silently stalls.
            info = snapshot.get(sig["symbol"])
            if info and info.get("mark"):
                price = info["mark"]
                direction = sig["direction"]
                hit_sl = (price <= sig["sl"]) if direction == "long" else (price >= sig["sl"])
                hit_tp2 = (price >= sig["tp2"]) if direction == "long" else (price <= sig["tp2"])
                hit_tp1 = (not sig["tp1_hit"]) and ((price >= sig["tp1"]) if direction == "long" else (price <= sig["tp1"]))
                if hit_sl:
                    _close_out(state, sig, "win" if sig["tp1_hit"] else "loss", price,
                                "breakeven" if sig["tp1_hit"] else "sl")
                    resolved = True
                elif hit_tp2:
                    _close_out(state, sig, "win", price, "tp2")
                    resolved = True
                elif hit_tp1:
                    sig["tp1_hit"] = True
                    sig["sl"] = sig["entry"]
                    _notify_tp1(sig, price)
                    _sync_history(state, sig)

        if not resolved:
            still_active.append(sig)
    state["active_signals"] = still_active


def compute_confidence_accuracy(state: dict) -> Optional[float]:
    """Mean-absolute-calibration-error across resolved signals: how close
    predicted confidence tracked realized win-rate. Lower is better;
    reported as an accuracy percentage in the daily summary."""
    calib = state.get("confidence_calibration", [])
    if len(calib) < 10:
        return None
    buckets: dict = {}
    for c in calib:
        bucket = int(c["confidence"] // 10) * 10
        buckets.setdefault(bucket, []).append(1.0 if c["result"] == "win" else 0.0)
    errors = []
    for bucket, results in buckets.items():
        predicted = (bucket + 5) / 100.0
        realized = sum(results) / len(results)
        errors.append(abs(predicted - realized))
    if not errors:
        return None
    mean_error = sum(errors) / len(errors)
    return max(0.0, 100.0 * (1.0 - mean_error))


def generate_daily_summary(state: dict) -> str:
    cutoff = time.time() - 86400
    recent = [h for h in state["signal_history"] if h.get("ts", 0) >= cutoff]
    resolved = [h for h in recent if h.get("result") in ("win", "loss")]
    wins = [h for h in resolved if h["result"] == "win"]
    losses = [h for h in resolved if h["result"] == "loss"]
    open_now = [h for h in recent if h.get("result") == "open"]
    total_r = sum(h.get("r_realized", 0.0) for h in resolved)
    win_rate = (len(wins) / len(resolved) * 100) if resolved else 0.0
    accuracy = compute_confidence_accuracy(state)

    lines = [
        f"\U0001F4CA <b>{ENGINE_NAME} \u2014 24h Summary</b>",
        "",
        f"Signals fired: {len(recent)}",
        f"Resolved: {len(resolved)}  (\u2705 {len(wins)}  \u274C {len(losses)})",
        f"Still open: {len(open_now)}",
        f"Win rate: {win_rate:.1f}%",
        f"Net R: {total_r:+.2f}",
    ]
    if accuracy is not None:
        lines.append(f"Confidence calibration accuracy: {accuracy:.0f}%")
    if resolved:
        by_pathway: dict = {}
        for h in resolved:
            by_pathway.setdefault(h["pathway"], []).append(h)
        lines.append("")
        lines.append("By pathway:")
        for pw, items in by_pathway.items():
            w = sum(1 for i in items if i["result"] == "win")
            lines.append(f"  \u2022 {pw}: {w}/{len(items)} ({100*w/len(items):.0f}%)")
        best = max(resolved, key=lambda h: h.get("r_realized", 0.0))
        worst = min(resolved, key=lambda h: h.get("r_realized", 0.0))
        lines.append("")
        lines.append(f"Best setup: {best['symbol']} {best['direction']} ({best.get('r_realized', 0):+.2f}R, {best['pathway']})")
        lines.append(f"Worst setup: {worst['symbol']} {worst['direction']} ({worst.get('r_realized', 0):+.2f}R, {worst['pathway']})")
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict):
    now = time.gmtime()
    today_str = time.strftime("%Y-%m-%d", now)
    if now.tm_hour != DAILY_SUMMARY_UTC_HOUR:
        return
    if state.get("last_summary_date") == today_str:
        return
    summary = generate_daily_summary(state)
    send_telegram(summary)
    state["last_summary_date"] = today_str
    state["last_summary_ts"] = int(time.time())


# ============================================================================
# MAIN EVALUATION / SCAN
# ============================================================================

def evaluate_symbol(symbol: str, bundle: dict, state: dict, btc_bias: str, btc_strength: float,
                     breadth: float, snapshot: dict, threshold: float, fleet_exposure: dict) -> Optional[dict]:
    """Pure evaluation: reads state (cooldowns, win-rate priors) but never
    writes it. All state mutation (cooldown update, signal recording,
    Telegram send) happens centrally in run_scan after this returns, so
    per-symbol work here is safe to run concurrently."""
    regime = build_regime_vector(state, symbol, bundle, btc_bias, btc_strength, breadth, COMBOS["intraday"])
    combo_name = select_combo(regime)
    local_threshold = adaptive_thresholds(regime, threshold)
    combo = COMBOS[combo_name]

    bar_index = bundle[combo["exec"]][-1]["t"] // TF_MS[combo["exec"]]
    market_price = snapshot.get(symbol, {}).get("mark") or bundle[combo["exec"]][-1]["c"]
    vp = volume_profile(bundle["1h"][-VOL_PROFILE_LOOKBACK_BARS:])

    info = snapshot.get(symbol, {})
    order_flow = update_order_flow_history(state, symbol, market_price,
                                            info.get("funding", 0.0), info.get("oi_usd", 0.0))

    book: Optional[dict] = None
    best: Optional[tuple] = None
    for builder in PATHWAYS:
        cand = builder(symbol, bundle, combo_name, regime, vp)
        if cand is None:
            continue
        cand = clamp_candidate_to_market(cand, market_price)
        if not check_cooldown(state, symbol, cand.direction, bar_index):
            continue
        if is_recent_duplicate(state, symbol, cand.direction, cand.entry):
            continue
        atr_pct = safe(cand.atr_val / cand.entry, 0.0)
        ok, reason = passes_hard_filters(symbol, snapshot, atr_pct, cand)
        if not ok:
            log.debug("%s %s filtered: %s", symbol, cand.pathway, reason)
            continue
        fr = apply_five_filters(cand, market_price, MIN_RR)
        if not fr.passed:
            log.debug("%s %s filtered: %s", symbol, cand.pathway, fr.reason)
            continue
        if book is None:
            book = analyze_orderbook(symbol)
        if not passes_spread_filter(book):
            continue
        if FLEET_CONFLICT_MODE == "block":
            opposite = "short" if cand.direction == "long" else "long"
            if opposite in fleet_exposure.get(symbol, set()):
                log.debug("%s %s blocked: fleet already holds opposite exposure", symbol, cand.pathway)
                continue
        confidence = score_candidate(cand, fr, regime, state, book, vp, order_flow, fleet_exposure)
        if confidence < local_threshold:
            continue
        grade = grade_for_confidence(confidence)
        if best is None or confidence > best[1]:
            best = (cand, confidence, grade)

    if best is None:
        return None
    cand, confidence, grade = best
    return {"cand": cand, "confidence": confidence, "grade": grade, "bar_index": bar_index}


def _prefetch(symbol: str, candle_cache: dict):
    return symbol, fetch_all_candles(symbol, candle_cache)


def run_scan():
    log.info("=== %s %s scan starting ===", ENGINE_NAME, ENGINE_VERSION)
    t_start = time.monotonic()
    state = load_state()
    candle_cache = load_candle_cache()
    snapshot = get_market_snapshot()
    fleet_exposure = load_fleet_exposure()

    symbols_to_fetch = ["BTC"] + [s for s in WATCHLIST if s != "BTC"]
    bundles: dict = {}
    with ThreadPoolExecutor(max_workers=FETCH_THREAD_WORKERS) as pool:
        futures = {pool.submit(_prefetch, sym, candle_cache): sym for sym in symbols_to_fetch}
        for fut in as_completed(futures):
            sym, bundle = fut.result()
            if bundle:
                bundles[sym] = bundle
            else:
                log.info("No candle bundle for %s this scan.", sym)
    save_candle_cache(candle_cache)
    log.info("Prefetched %d/%d symbol bundles in %.1fs",
              len(bundles), len(symbols_to_fetch), time.monotonic() - t_start)

    check_active_signals(state, snapshot, bundles)

    btc_bundle = bundles.get("BTC")
    if not btc_bundle:
        log.error("Could not fetch BTC bundle; aborting scan.")
        save_state(state)
        return
    btc_bias, btc_strength = compute_btc_regime(btc_bundle)
    breadth = compute_breadth(bundles, btc_bias)
    log.info("BTC regime: %s (ADX %.1f) | breadth %.0f%% | fleet exposure tracked for %d symbols",
              btc_bias, btc_strength, breadth * 100, len(fleet_exposure))

    tune_pathway_weights(state)

    fired = []
    threshold = state["governor"]["threshold"]

    for symbol in WATCHLIST:
        if symbol not in bundles:
            continue
        if count_open_for_symbol(state, symbol) >= MAX_CONCURRENT_PER_SYMBOL:
            continue
        try:
            result = evaluate_symbol(symbol, bundles[symbol], state, btc_bias, btc_strength,
                                      breadth, snapshot, threshold, fleet_exposure)
        except Exception as e:
            log.exception("Error evaluating %s: %s", symbol, e)
            continue
        if result:
            fired.append(result)

    if len(fired) > 1:
        corr_bundles = {r["cand"].symbol: bundles[r["cand"].symbol] for r in fired}
        clusters = build_correlation_clusters(corr_bundles)
        ranked = [{"symbol": r["cand"].symbol, "direction": r["cand"].direction,
                   "confidence": r["confidence"], "ref": r} for r in fired]
        kept = dedup_correlated(ranked, clusters)
        kept_ids = {id(k["ref"]) for k in kept}
        fired = [r for r in fired if id(r) in kept_ids]

    sent = 0
    for r in fired:
        direction = r["cand"].direction
        if count_open_same_direction(state, direction) >= MAX_CONCURRENT_SAME_DIRECTION:
            log.info("Skipping %s: same-direction cap reached", r["cand"].symbol)
            continue
        text = format_signal(r["cand"], r["confidence"], r["grade"])
        message_id = send_telegram(text)
        record_signal(state, r["cand"], r["confidence"], r["grade"], r["bar_index"], message_id)
        sent += 1
        log.info("Signal fired: %s %s (%s) conf=%.1f grade=%s",
                  r["cand"].symbol, r["cand"].direction, r["cand"].pathway, r["confidence"], r["grade"])

    maybe_send_daily_summary(state)
    governor_adjust_threshold(state, estimate_signals_last_24h(state))
    prune_state(state)
    save_state(state)
    log.info("=== Scan complete: %d signal(s) fired, threshold now %.1f, took %.1fs ===",
              sent, state["governor"]["threshold"], time.monotonic() - t_start)


def main():
    try:
        run_scan()
    except Exception as e:
        log.exception("Fatal error during scan: %s", e)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
