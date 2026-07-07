"""
PARALLAX ENGINE v1.1.1
================================================================================

PHILOSOPHY
    A single setup, seen from a single timeframe, is an opinion. Parallax's
    edge is structural: every candidate must survive being looked at from
    multiple independent vantage points -- two timeframe pipelines (fast
    intraday, slow swing), three independent setup pathways (liquidity
    reversal, trend continuation, momentum breakout), and a cross-sectional
    market context (BTC regime, breadth, relative strength) -- before it is
    scored. Agreement across vantage points is treated as real information
    (a confidence bonus), not just a duplicate to suppress. The name reflects
    this: parallax is the apparent shift of an object when viewed from two
    different positions, and the true position is triangulated from both.

KEY INNOVATIONS (beyond what any single reference engine had)
    1. Pathway + pipeline convergence bonus: if two independent pathways, or
       both timeframe pipelines, independently qualify the same symbol and
       direction in the same scan, that agreement is a scored confluence
       (not merely a dedup problem to solve after the fact).
    2. A single governor loop tunes ONE global z-threshold against a rolling
       EMA of realized daily signal count toward the frequency target, and
       every regime/session/macro adjustment nudges the same threshold
       rather than maintaining parallel ad-hoc gates that can silently
       diverge (the fleet-wide correlation-dedup bug found in the prior
       audit was exactly this kind of silent divergence).
    3. Win-rate awareness never becomes a hard veto anywhere in this file
       (the Obsidian Edge catch-22 this fleet already learned from): it is
       either a continuous scoring nudge or a grade-floor filter, never a
       binary block on a cold-start or small sample.
    4. Correlation dedup is computed fresh every scan from live 1h returns
       (Pearson + union-find clustering) and is applied unconditionally
       before ranking -- it cannot be silently skipped for a subset of
       symbols the way the shared liquidity_confluence utility bug allowed.
    5. Macro-event awareness (high-impact calendar proximity) and
       cross-sectional context (market breadth, BTC-relative strength
       percentile) feed the SAME scoring vector as price-action confluences,
       instead of being bolted on as a separate veto layer.

ARCHITECTURE
    Two independent timeframe pipelines run per symbol per scan:
        FAST  : 4H bias  / 15m trigger  (session-aware, intraday)
        SLOW  : 1D bias  / 1h trigger   (always-on, swing)
    Within each pipeline, three independent pathways compete for the best
    candidate:
        liquidity_reversal    -- hard-gated SMC sequence (sweep -> displacement
                                  + CHoCH -> return to imbalance/OB), gated not
                                  scored, per Nyx's proven design
        trend_continuation    -- HTF structure + EMA-confirmed bias, pullback
                                  into discount/premium OTE or order block
        momentum_breakout     -- volatility compression or Donchian break,
                                  volume expansion, orderflow alignment
    Confidence is a continuous logistic score (not additive integer scoring)
    built from confluence weight, RR, regime favorability, BTC macro
    alignment, orderflow/orderbook alignment, historical pathway prior,
    market breadth, relative strength, macro-event proximity, and
    convergence bonuses. A self-tuning governor keeps the realized signal
    rate inside the target band without manual intervention.

INFRASTRUCTURE (unchanged, per spec)
    Data source : Hyperliquid API (info endpoint)
    Exchange    : Hyperliquid perpetuals
    Scheduler   : cron-job.org, every 15 minutes, scan-per-run
    State       : state.json, read/written every run, with .bak fallback
    Alerts      : Telegram (HTML), reaction-tracked outcomes, daily summary

This file is standalone and runnable. No TODOs. Constants marked "tune
against live/backtested data" are reasonable starting points carried over
by analogy from the reference fleet's own scaling choices, not the product
of a backtest -- validate before sizing real risk against these signals.
================================================================================
"""

from __future__ import annotations

import os
import sys
import time
import math
import json
import random
import pathlib
import threading
import statistics
import signal as _signal
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

# ═══════════════════════════════════════════════════════════════════════════
# ENV / CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
VERSION = "1.1.1"
ENGINE_NAME = "Parallax"

WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]

SECTOR_MAP: dict[str, str] = {
    "BTCUSDT": "btc",
    "ETHUSDT": "eth",
    "SOLUSDT": "eth_l1", "AVAXUSDT": "eth_l1", "SUIUSDT": "eth_l1", "APTUSDT": "eth_l1",
    "NEARUSDT": "eth_l1",
    "BNBUSDT": "bnb",
    "XRPUSDT": "payments", "XLMUSDT": "payments", "TRXUSDT": "payments", "LTCUSDT": "payments",
    "DOGEUSDT": "meme", "PENGUUSDT": "meme",
    "ADAUSDT": "layer1_alt", "DOTUSDT": "layer1_alt", "TAOUSDT": "layer1_alt",
    "LINKUSDT": "defi", "AAVEUSDT": "defi", "UNIUSDT": "defi",
    "ONDOUSDT": "defi", "PENDLEUSDT": "defi",
    "HYPEUSDT": "hype",
    "ZECUSDT": "privacy", "BCHUSDT": "privacy",
}
BTC_REGIME_EXEMPT_SECTORS: set[str] = {"hype", "defi"}
BTC_SYMBOL = "BTCUSDT"

INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}

# ── SESSION (fast pipeline only; slow pipeline is always-on) ────────────────
LONDON_OPEN_H, LONDON_CLOSE_H = 7, 12
NY_OPEN_H, NY_CLOSE_H = 13, 20
DEAD_ZONE_START_H, DEAD_ZONE_END_H = 12, 13
WEEKEND_MODE_ENABLED = True
WEEKEND_THRESHOLD_BUMP = 0.4  # added to the z-threshold on Sat/Sun (thinner books)

# ── FREQUENCY TARGET / GOVERNOR ──────────────────────────────────────────────
TARGET_SIGNALS_MIN = 5.0
TARGET_SIGNALS_MAX = 10.0
GOVERNOR_FLOOR = -1.5
GOVERNOR_CEIL = 3.5
GOVERNOR_STEP = 0.08
GOVERNOR_MIN_INTERVAL_S = 60 * 30

# ── PORTFOLIO / DIVERSIFICATION CAPS ─────────────────────────────────────────
TOP_N_SIGNALS_PER_SCAN = 4
MAX_SAME_DIRECTION = 3
MAX_PER_SECTOR = 1
MAX_PER_SYMBOL_PER_SCAN = 1        # across both pipelines combined
MAX_CONCURRENT_ACTIVE_SIGNALS = 10  # global brake, both pipelines combined
CORR_LOOKBACK_BARS = 72             # hourly closes
CORR_CLUSTER_THRESHOLD = 0.72

# ── LIQUIDITY / EXECUTION SAFETY ─────────────────────────────────────────────
MIN_OI_USD = 500_000
MIN_ATR_PCT = 0.0025
MAX_ATR_PCT = 0.09
MIN_RR = 1.2
MAX_ENTRY_DRIFT_R = 0.5   # re-checked against a live price fetch right before send
DEDUP_TIME_WINDOW_HOURS = 6
DEDUP_PRICE_TOL_PCT = 0.006

# ── OI / FUNDING ──────────────────────────────────────────────────────────────
FUNDING_ALIGN_THRESHOLD = 0.0001

# ── BTC REGIME ────────────────────────────────────────────────────────────────
BTC_REGIME_FILTER_ENABLED = True

# ── MACRO CALENDAR (ForexFactory public JSON feed) ──────────────────────────
MACRO_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
MACRO_CACHE_TTL_S = 60 * 60 * 3
MACRO_WINDOW_BEFORE_MINS = 45
MACRO_WINDOW_AFTER_MINS = 30
MACRO_EVENT_KEYWORDS = (
    "fomc", "interest rate", "cpi", "ppi", "nonfarm", "non-farm", "unemployment",
    "gdp", "fed chair", "powell", "pce", "retail sales",
)
MACRO_HIGH_ATR_SUPPRESS_PCT = 0.020
MACRO_ATR_PCTILE_HIGH = 0.80
MACRO_ATR_PCTILE_HIGH_MULT = 1.6

# ── MARKET BREADTH / RELATIVE STRENGTH ──────────────────────────────────────
BREADTH_EXTREME_LONG_THRESHOLD = 0.88
BREADTH_CROWDED_LONG_THRESHOLD = 0.72
BREADTH_CROWDED_LONG_THRESHOLD_MIXED = 0.62
BREADTH_WEAK_LONG_THRESHOLD = 0.30
BREADTH_EXTREME_SHORT_THRESHOLD = 0.12
BREADTH_WEAK_SHORT_THRESHOLD = 0.70
RS_TOP_PERCENTILE = 0.20
RS_BOTTOM_PERCENTILE = 0.20
RS_LOOKBACK_BARS = 42  # ~7 days of 4h bars

# ── WIN-RATE / COOLDOWN ─────────────────────────────────────────────────────
MIN_SAMPLE_FOR_PRIOR = 8
GRADE_FLOOR_ON_COLD_SYMBOL = "B"   # symbols with a losing recent streak need B+ to fire
POST_LOSS_COOLDOWN_BARS = 3
FAILED_BREAKOUT_LOOKBACK_BARS = 12

# ── INDICATOR PERIODS ────────────────────────────────────────────────────────
RSI_LEN = 14
ATR_LEN = 14
ADX_LEN = 14
BB_LEN = 20
BB_MULT = 2.0
DONCHIAN_LEN = 20
EMA_FAST, EMA_MID, EMA_SLOW = 21, 50, 200

STATE_FILE = pathlib.Path("state.json")
STATE_VERSION = 1
DAILY_SUMMARY_HOUR_UTC = 8
MAX_SIGNAL_HISTORY = 600


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE CONFIG — two independent timeframe combos
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Pipeline:
    id: str
    label: str
    bias_tf: str            # HTF used for structure/EMA bias
    trigger_tf: str         # execution timeframe for entries
    n_bias: int
    n_trigger: int
    session_gated: bool
    cooldown_hours: float
    active_ttl_hours: float
    fill_timeout_hours: float
    hold_hint: str
    sl_buffer_atr: float
    entry_max_dist_atr: float
    tp1_min_rr: float
    tp2_min_rr: float
    tp1_fallback_rr: float
    tp2_fallback_rr: float
    tp3_fallback_rr: float
    swing_left: int
    swing_right: int


PIPELINES: dict[str, Pipeline] = {
    "fast": Pipeline(
        id="fast", label="4H/15m", bias_tf="4h", trigger_tf="15m",
        n_bias=180, n_trigger=220, session_gated=True,
        cooldown_hours=4, active_ttl_hours=30, fill_timeout_hours=6,
        hold_hint="intraday",
        sl_buffer_atr=0.55, entry_max_dist_atr=0.60,
        tp1_min_rr=1.2, tp2_min_rr=2.2,
        tp1_fallback_rr=1.5, tp2_fallback_rr=2.8, tp3_fallback_rr=4.5,
        swing_left=2, swing_right=2,
    ),
    "slow": Pipeline(
        id="slow", label="1D/1h", bias_tf="1d", trigger_tf="1h",
        n_bias=120, n_trigger=220, session_gated=False,
        cooldown_hours=14, active_ttl_hours=96, fill_timeout_hours=18,
        hold_hint="swing",
        sl_buffer_atr=0.70, entry_max_dist_atr=0.80,
        tp1_min_rr=1.3, tp2_min_rr=2.5,
        tp1_fallback_rr=1.6, tp2_fallback_rr=3.2, tp3_fallback_rr=5.5,
        swing_left=3, swing_right=3,
    ),
}
# NOTE: starting points carried over by analogy from the reference fleet's
# own scaling choices, not the product of a backtest -- validate before
# sizing real risk against these signals.


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Swing:
    index: int
    price: float
    kind: str  # "high" | "low"


@dataclass
class StructureEvent:
    index: int
    kind: str       # "bos" | "choch"
    direction: str  # "bull" | "bear"
    level: float
    quality_atr: float = 0.0  # how many ATRs the break cleared the prior pivot by


@dataclass
class StructureState:
    bias: str
    events: list = field(default_factory=list)
    last_swing_high: Optional[Swing] = None
    last_swing_low: Optional[Swing] = None


@dataclass
class Zone:
    high: float
    low: float
    direction: str   # "bull" | "bear"
    index: int
    kind: str        # "ob" | "fvg" | "ifvg"
    state: str = "fresh"       # fresh | tested | mitigated
    mitigation_pct: float = 0.0


@dataclass
class LiquidityPool:
    level: float
    direction: str   # "buyside" (equal highs) | "sellside" (equal lows)
    touches: int
    index: int
    swept: bool = False


@dataclass
class SweepEvent:
    index: int
    level: float
    wick_extreme: float
    atr_ratio: float


@dataclass
class RegimeVector:
    btc_bias: str
    btc_strength: float
    vol_pctile: float
    adx_bias: float
    session_weight: float
    noise_index: float
    breadth_pct: float
    rs_percentile: Optional[float]

    def composite_favorability(self) -> float:
        trend_component = min(self.adx_bias / 35.0, 1.0)
        noise_penalty = max(0.0, 1.0 - self.noise_index)
        return round(0.45 * trend_component + 0.30 * noise_penalty + 0.25 * self.session_weight, 4)


@dataclass
class Candidate:
    symbol: str
    direction: str
    pipeline_id: str
    pathway: str
    entry_zone_high: float
    entry_zone_low: float
    exact_entry: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    take_profit_3: Optional[float]
    atr_val: float
    confluences: list = field(default_factory=list)   # str tags, "caution:" prefix = negative
    structure_quality: float = 0.0
    poi_state: str = ""

    def rr(self) -> float:
        risk = abs(self.exact_entry - self.stop_loss)
        reward = abs(self.take_profit_1 - self.exact_entry)
        return reward / risk if risk > 0 else 0.0


@dataclass
class Signal:
    candidate: Candidate
    confidence: float
    grade: str
    duration: str
    z_breakdown: dict = field(default_factory=dict)
    convergence_tags: list = field(default_factory=list)
    timestamp: str = ""


# ═══════════════════════════════════════════════════════════════════════════
# HYPERLIQUID API — adaptive-backoff rate limiter (ported from Nyx/Castellan)
# ═══════════════════════════════════════════════════════════════════════════

_hl_lock = threading.Lock()
_hl_last_req_ts = 0.0
_hl_min_interval = 0.20
_HL_MIN_INTERVAL_FLOOR = 0.20
_HL_MIN_INTERVAL_CEIL = 0.60
_hl_consecutive_ok = 0
_hl_session = requests.Session()
_tg_session = requests.Session()


def hl_coin(symbol: str) -> str:
    return symbol.replace("USDT", "")


def hl_post(payload: dict, retries: int = 5, timeout: int = 15):
    global _hl_last_req_ts, _hl_min_interval, _hl_consecutive_ok
    for attempt in range(retries):
        try:
            with _hl_lock:
                elapsed = time.time() - _hl_last_req_ts
                wait = _hl_min_interval - elapsed
                if wait > 0:
                    time.sleep(wait)
                _hl_last_req_ts = time.time()

            r = _hl_session.post(HL_INFO_URL, json=payload,
                                  headers={"Content-Type": "application/json"}, timeout=timeout)
            if r.status_code == 429:
                with _hl_lock:
                    _hl_min_interval = min(_HL_MIN_INTERVAL_CEIL, _hl_min_interval * 1.25 + 0.02)
                    _hl_consecutive_ok = 0
                time.sleep(min(20.0, 1.0 * (2 ** attempt)) + random.uniform(0, 0.3))
                continue
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "error" in data:
                raise ValueError(f"Hyperliquid API error (HTTP 200): {data['error']}")
            with _hl_lock:
                _hl_consecutive_ok += 1
                if _hl_consecutive_ok >= 10:
                    _hl_min_interval = _HL_MIN_INTERVAL_FLOOR
                    _hl_consecutive_ok = 0
                else:
                    _hl_min_interval = max(_HL_MIN_INTERVAL_FLOOR, _hl_min_interval - 0.0025)
            return data
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(min(10.0, 0.5 * (2 ** attempt)))
    return None


def current_bar_open_ms(ref_ms: int, interval: str) -> int:
    return (ref_ms // INTERVAL_MS[interval]) * INTERVAL_MS[interval]


def filter_valid_candles(candles: list[dict]) -> list[dict]:
    return [c for c in candles if c["h"] > c["l"]]


def get_candles(symbol: str, interval: str, n: int) -> list[dict]:
    iv_ms = INTERVAL_MS[interval]
    ref_ms = int(time.time() * 1000)
    end_ms = current_bar_open_ms(ref_ms, interval)
    start_ms = end_ms - iv_ms * (n + 5)
    raw = hl_post({
        "type": "candleSnapshot",
        "req": {"coin": hl_coin(symbol), "interval": interval,
                 "startTime": start_ms, "endTime": end_ms},
    })
    if not raw:
        return []
    candles = [{"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])}
               for c in raw]
    valid = [c for c in candles if c["t"] < end_ms][-n:]
    return filter_valid_candles(valid)


_candle_cache: dict[str, dict] = {}
_CANDLE_CACHE_TTL_S = 45 * 60


def get_candles_cached(symbol: str, tf: str, n: int) -> list[dict]:
    """HTF candles only (4h/1d) -- caller must not cache fast-closing tfs."""
    key = f"{symbol}:{tf}"
    entry = _candle_cache.get(key)
    if entry and (time.time() - entry["ts"]) < _CANDLE_CACHE_TTL_S and entry["n"] >= n:
        return entry["candles"][-n:]
    candles = get_candles(symbol, tf, n)
    _candle_cache[key] = {"candles": candles, "ts": time.time(), "n": n}
    return candles


def fetch_all_mids() -> dict[str, float]:
    try:
        raw = hl_post({"type": "allMids"})
        return {k: float(v) for k, v in raw.items()} if raw else {}
    except Exception as e:
        print(f"  [MIDS] fetch error: {e}")
        return {}


_market_ctx: dict[str, dict] = {}


def fetch_all_market_ctx() -> None:
    """Funding, open interest, and mark price for every listed asset in one
    call -- backs the OI liquidity gate and the funding scoring bonus."""
    try:
        raw = hl_post({"type": "metaAndAssetCtxs"})
        if not raw or len(raw) < 2:
            return
        universe = raw[0].get("universe", [])
        ctx_list = raw[1]
        for i, asset in enumerate(universe):
            coin = asset.get("name", "")
            if not coin or i >= len(ctx_list):
                continue
            ctx = ctx_list[i]
            mark = float(ctx["markPx"]) if ctx.get("markPx") is not None else None
            oi_coins = float(ctx["openInterest"]) if ctx.get("openInterest") is not None else None
            _market_ctx[coin] = {
                "funding_rate": float(ctx["funding"]) if ctx.get("funding") is not None else None,
                "oi_usd": (oi_coins * mark) if (oi_coins is not None and mark is not None) else None,
                "mark_px": mark,
            }
        print(f"  [MARKET CTX] Fetched {len(_market_ctx)} assets")
    except Exception as e:
        print(f"  [MARKET CTX] fetch error: {e}")


def get_market_ctx(symbol: str) -> dict:
    return _market_ctx.get(hl_coin(symbol), {})


def get_l2_book(coin: str) -> Optional[dict]:
    try:
        return hl_post({"type": "l2Book", "coin": coin}, retries=2, timeout=8)
    except Exception:
        return None


def analyze_orderbook(symbol: str) -> dict:
    """Top-of-book imbalance: (bid_notional - ask_notional) / total, within a
    tight band around mid. Positive favors longs, negative favors shorts."""
    book = get_l2_book(hl_coin(symbol))
    if not book or "levels" not in book:
        return {"imbalance": 0.0, "available": False}
    try:
        bids, asks = book["levels"][0], book["levels"][1]
        bid_notional = sum(float(b["px"]) * float(b["sz"]) for b in bids[:15])
        ask_notional = sum(float(a["px"]) * float(a["sz"]) for a in asks[:15])
        total = bid_notional + ask_notional
        imbalance = (bid_notional - ask_notional) / total if total > 0 else 0.0
        return {"imbalance": round(imbalance, 4), "available": True}
    except Exception:
        return {"imbalance": 0.0, "available": False}


# ═══════════════════════════════════════════════════════════════════════════
# INDICATOR LIBRARY
# ═══════════════════════════════════════════════════════════════════════════

def safe(v, fb: float = 0.0) -> float:
    try:
        if v is None or math.isnan(v) or math.isinf(v):
            return fb
        return float(v)
    except (TypeError, ValueError):
        return fb


def ema(vals: list[float], period: int) -> list[float]:
    if not vals:
        return []
    k = 2.0 / (period + 1.0)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        if i + 1 < period:
            out.append(float("nan"))
        else:
            out.append(sum(vals[i + 1 - period:i + 1]) / period)
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        if i + 1 < period:
            out.append(float("nan"))
        else:
            window = vals[i + 1 - period:i + 1]
            out.append(statistics.pstdev(window))
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    out = [50.0] * (period + 1)
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss > 0 else 100.0
        out.append(100.0 - (100.0 / (1.0 + rs)))
    return out[:len(closes)] if len(out) >= len(closes) else out + [out[-1]] * (len(closes) - len(out))


def atr(highs, lows, closes, period: int = ATR_LEN) -> list[float]:
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    out, run = [], trs[0]
    for i, tr in enumerate(trs):
        if i < period:
            run = sum(trs[:i + 1]) / (i + 1)
        else:
            run = (run * (period - 1) + tr) / period
        out.append(run)
    return out


def adx_dmi(highs, lows, closes, period: int = ADX_LEN):
    n = len(closes)
    if n < period * 2:
        return [0.0] * n, [0.0] * n, [0.0] * n
    plus_dm, minus_dm, trs = [0.0], [0.0], [highs[0] - lows[0]]
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))

    def wilder_smooth(vals):
        out, run = [], sum(vals[:period])
        for i in range(n):
            if i < period:
                out.append(float("nan"))
            elif i == period:
                out.append(run)
            else:
                run = run - (run / period) + vals[i]
                out.append(run)
        return out

    tr_s = wilder_smooth(trs)
    pdm_s = wilder_smooth(plus_dm)
    mdm_s = wilder_smooth(minus_dm)
    plus_di, minus_di, dx = [], [], []
    for i in range(n):
        if math.isnan(tr_s[i]) or tr_s[i] == 0:
            plus_di.append(0.0); minus_di.append(0.0); dx.append(0.0)
            continue
        pdi = 100 * pdm_s[i] / tr_s[i]
        mdi = 100 * mdm_s[i] / tr_s[i]
        plus_di.append(pdi); minus_di.append(mdi)
        denom = pdi + mdi
        dx.append(100 * abs(pdi - mdi) / denom if denom > 0 else 0.0)
    adx = ema(dx, period)
    return adx, plus_di, minus_di


def bollinger(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT):
    mid = sma(closes, period)
    sd = stdev(closes, period)
    upper = [m + mult * s if not math.isnan(m) else float("nan") for m, s in zip(mid, sd)]
    lower = [m - mult * s if not math.isnan(m) else float("nan") for m, s in zip(mid, sd)]
    width_pct = [(u - l) / m if (not math.isnan(m) and m > 0) else float("nan")
                 for u, l, m in zip(upper, lower, mid)]
    return mid, upper, lower, width_pct


def donchian(highs, lows, period: int = DONCHIAN_LEN):
    upper, lower = [], []
    for i in range(len(highs)):
        if i + 1 < period:
            upper.append(float("nan")); lower.append(float("nan"))
        else:
            upper.append(max(highs[i + 1 - period:i + 1]))
            lower.append(min(lows[i + 1 - period:i + 1]))
    return upper, lower


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
    """Regular divergence only (reversal signal): price makes a fresh
    extreme, RSI does not confirm it."""
    if len(closes) < lookback + 2:
        return None
    window_c = closes[-lookback:]
    window_r = rsi_vals[-lookback:]
    lo_idx = window_c.index(min(window_c))
    hi_idx = window_c.index(max(window_c))
    if lo_idx < lookback - 3 and window_c[-1] <= min(window_c[:lo_idx] + [window_c[-1]]) * 1.001:
        if window_c[-1] <= window_c[lo_idx] and window_r[-1] > window_r[lo_idx]:
            return "bullish"
    if hi_idx < lookback - 3 and window_c[-1] >= max(window_c[:hi_idx] + [window_c[-1]]) * 0.999:
        if window_c[-1] >= window_c[hi_idx] and window_r[-1] < window_r[hi_idx]:
            return "bearish"
    return None


def compute_indicators(candles: list[dict]) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c["v"] for c in candles]
    adx_v, plus_di, minus_di = adx_dmi(highs, lows, closes, ADX_LEN)
    bb_mid, bb_up, bb_lo, bb_width = bollinger(closes)
    don_up, don_lo = donchian(highs, lows)
    r = rsi(closes)
    return {
        "closes": closes, "highs": highs, "lows": lows, "vols": vols,
        "ema_fast": ema(closes, EMA_FAST), "ema_mid": ema(closes, EMA_MID),
        "ema_slow": ema(closes, EMA_SLOW),
        "atr": atr(highs, lows, closes),
        "adx": adx_v, "plus_di": plus_di, "minus_di": minus_di,
        "rsi": r, "rsi_divergence": detect_rsi_divergence(closes, r),
        "bb_mid": bb_mid, "bb_up": bb_up, "bb_lo": bb_lo, "bb_width_pct": bb_width,
        "don_up": don_up, "don_lo": don_lo,
        "vol_sma": sma(vols, 20), "obv": obv(closes, vols),
    }


_indicator_cache: dict[str, dict] = {}


def get_cached_indicators(symbol: str, tf: str, candles: list[dict]) -> dict:
    key = f"{symbol}:{tf}:{len(candles)}:{candles[-1]['t'] if candles else 0}"
    cached = _indicator_cache.get(key)
    if cached is not None:
        return cached
    ind = compute_indicators(candles)
    _indicator_cache[key] = ind
    return ind


def clear_indicator_cache() -> None:
    _indicator_cache.clear()


# ═══════════════════════════════════════════════════════════════════════════
# REGIME DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def session_weight_now() -> float:
    """Crypto trades 24/7 but liquidity still clusters around the US/EU
    overlap. Weight scans favorably during high-liquidity hours and slightly
    down in the quiet 00:00-05:00 UTC stretch, without hard-blocking any
    hour (only the fast pipeline's session gate hard-blocks)."""
    hour = time.gmtime().tm_hour
    if NY_OPEN_H <= hour <= NY_CLOSE_H:
        return 1.0
    if 0 <= hour <= 5:
        return 0.75
    return 0.9


def is_fast_pipeline_session() -> bool:
    hour = time.gmtime().tm_hour
    if DEAD_ZONE_START_H <= hour < DEAD_ZONE_END_H:
        return False
    return (LONDON_OPEN_H <= hour < LONDON_CLOSE_H) or (NY_OPEN_H <= hour < NY_CLOSE_H)


def is_weekend_utc() -> bool:
    return datetime.now(timezone.utc).weekday() >= 5


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state.setdefault("atr_pct_memory", {}).setdefault(symbol, [])
    mem.append(atr_pct)
    state["atr_pct_memory"][symbol] = mem[-250:]
    if len(mem) < 10:
        return 0.5
    sorted_mem = sorted(mem)
    rank = sum(1 for x in sorted_mem if x <= atr_pct)
    return rank / len(sorted_mem)


def compute_noise_index(candles: list[dict], lookback: int = 30) -> float:
    """Ratio of net displacement to total path length -- low means choppy/
    overlapping candles, high means directional travel."""
    window = candles[-lookback:]
    if len(window) < 5:
        return 0.5
    net = abs(window[-1]["c"] - window[0]["c"])
    path = sum(abs(window[i]["c"] - window[i - 1]["c"]) for i in range(1, len(window)))
    efficiency = safe(net / path, 0.5) if path else 0.5
    return round(1.0 - min(efficiency, 1.0), 4)


def compute_btc_regime(btc_bundle: dict) -> tuple[str, float]:
    ind = get_cached_indicators("BTCUSDT", "4h", btc_bundle["4h"])
    price = ind["closes"][-1]
    ef, es, et = ind["ema_fast"][-1], ind["ema_mid"][-1], ind["ema_slow"][-1]
    adx_v = ind["adx"][-1]
    if price > ef > es > et:
        bias = "bullish"
    elif price < ef < es < et:
        bias = "bearish"
    else:
        bias = "neutral"
    return bias, safe(adx_v, 0.0)


def btc_regime_blocks(direction: str, symbol: str, btc_bias: str, btc_strength: float) -> bool:
    if not BTC_REGIME_FILTER_ENABLED or symbol == BTC_SYMBOL:
        return False
    if SECTOR_MAP.get(symbol) in BTC_REGIME_EXEMPT_SECTORS:
        return False
    if btc_strength < 20:
        return False  # BTC itself isn't trending strongly enough to matter
    if direction == "long" and btc_bias == "bearish":
        return True
    if direction == "short" and btc_bias == "bullish":
        return True
    return False


def build_regime_vector(state: dict, symbol: str, bundle: dict, pipeline: Pipeline,
                         btc_bias: str, btc_strength: float,
                         breadth_pct: float, rs_percentile: Optional[float]) -> RegimeVector:
    ind_bias = get_cached_indicators(symbol, pipeline.bias_tf, bundle[pipeline.bias_tf])
    atr_pct = safe(ind_bias["atr"][-1] / ind_bias["closes"][-1], 0.01)
    vol_pctile = update_atr_pct_memory(state, symbol, atr_pct)
    noise = compute_noise_index(bundle[pipeline.trigger_tf])
    return RegimeVector(
        btc_bias=btc_bias, btc_strength=btc_strength, vol_pctile=vol_pctile,
        adx_bias=safe(ind_bias["adx"][-1], 0.0), session_weight=session_weight_now(),
        noise_index=noise, breadth_pct=breadth_pct, rs_percentile=rs_percentile,
    )


def adaptive_threshold(regime: RegimeVector, base_threshold: float) -> float:
    """Nudges the governor's base z-threshold up in noisy/choppy conditions
    and down in clean ones -- bounded so the governor still owns the
    long-run level. Weekend thinness adds a further bump."""
    fav = regime.composite_favorability()
    adj = (0.5 - fav) * 3.0
    if WEEKEND_MODE_ENABLED and is_weekend_utc():
        adj += WEEKEND_THRESHOLD_BUMP
    return max(GOVERNOR_FLOOR, min(GOVERNOR_CEIL, base_threshold + adj))


# ── MARKET BREADTH (cross-sectional, filled in during the prefetch pass) ───
_breadth_above_ema50: dict[str, bool] = {}
_breadth_lock = threading.Lock()


def reset_breadth_cache() -> None:
    with _breadth_lock:
        _breadth_above_ema50.clear()


def record_breadth(symbol: str, above_ema50: bool) -> None:
    with _breadth_lock:
        _breadth_above_ema50[symbol] = above_ema50


def compute_market_breadth() -> dict:
    with _breadth_lock:
        results = dict(_breadth_above_ema50)
    if not results:
        return {"pct": 0.5, "label": "Breadth: unknown"}
    pct = sum(1 for v in results.values() if v) / len(results)
    if pct < BREADTH_WEAK_LONG_THRESHOLD:
        label = f"Breadth {pct*100:.0f}% >EMA50 (weak)"
    elif pct > BREADTH_WEAK_SHORT_THRESHOLD:
        label = f"Breadth {pct*100:.0f}% >EMA50 (overbought)"
    else:
        label = f"Breadth {pct*100:.0f}% >EMA50 (healthy)"
    return {"pct": pct, "label": label}


def breadth_score_adjustment(direction: str, rs_pct: Optional[float], btc_bias: str) -> tuple[float, str]:
    breadth = compute_market_breadth()
    pct, label = breadth["pct"], breadth["label"]
    is_mixed = btc_bias == "neutral"
    crowded_thresh = BREADTH_CROWDED_LONG_THRESHOLD_MIXED if is_mixed else BREADTH_CROWDED_LONG_THRESHOLD
    adj = 0.0
    if direction == "long":
        if pct > BREADTH_EXTREME_LONG_THRESHOLD:
            adj = -0.8
        elif pct > crowded_thresh:
            adj = -0.6 if (rs_pct is not None and rs_pct <= -RS_TOP_PERCENTILE) else -0.3
        elif pct < BREADTH_WEAK_LONG_THRESHOLD:
            adj = -0.3
    else:
        if pct < BREADTH_EXTREME_SHORT_THRESHOLD:
            adj = -0.8
        elif pct > BREADTH_WEAK_SHORT_THRESHOLD:
            adj = -0.6 if (rs_pct is not None and rs_pct >= RS_TOP_PERCENTILE) else -0.3
    return adj, label


# ── RELATIVE STRENGTH (return vs BTC, cross-sectionally percentile-ranked) ─
_rs_returns: dict[str, float] = {}
_rs_lock = threading.Lock()


def reset_rs_cache() -> None:
    with _rs_lock:
        _rs_returns.clear()


def record_rs_return(symbol: str, return_pct: float) -> None:
    with _rs_lock:
        _rs_returns[symbol] = return_pct


def compute_relative_strength(symbol: str) -> tuple[Optional[float], Optional[float]]:
    with _rs_lock:
        scores = dict(_rs_returns)
    btc_r, sym_r = scores.get(BTC_SYMBOL), scores.get(symbol)
    if btc_r is None or sym_r is None:
        return None, None
    rs = sym_r - btc_r
    others = sorted(v - btc_r for k, v in scores.items() if k != BTC_SYMBOL)
    if not others:
        return rs, 0.5
    below = sum(1 for v in others if v <= rs)
    return rs, below / len(others)


# ── MACRO CALENDAR ────────────────────────────────────────────────────────
_FF_TZ = timezone.utc  # feed timestamps are already ISO/UTC-normalized upstream


def fetch_macro_calendar(state: dict) -> list[dict]:
    cache = state.get("macro_calendar_cache", {})
    if time.time() - cache.get("fetched_at", 0) < MACRO_CACHE_TTL_S:
        return cache.get("events", [])
    try:
        resp = requests.get(MACRO_CALENDAR_URL, timeout=10)
        resp.raise_for_status()
        raw_events = resp.json()
    except Exception as e:
        print(f"  [MACRO] fetch failed: {e} -- using cache")
        return cache.get("events", [])
    events = []
    for ev in raw_events:
        impact = str(ev.get("impact", "")).lower()
        title = str(ev.get("title", "")).lower()
        if impact != "high" or not any(kw in title for kw in MACRO_EVENT_KEYWORDS):
            continue
        try:
            dt = datetime.fromisoformat(ev.get("date", "").replace("Z", "+00:00"))
            events.append({"name": ev.get("title", "event"), "datetime_utc": dt.isoformat()})
        except Exception:
            continue
    state["macro_calendar_cache"] = {"fetched_at": time.time(), "events": events}
    print(f"  [MACRO] Loaded {len(events)} high-impact events")
    return events


def macro_filter(state: dict, atr_pct: float, atr_pctile: float) -> dict:
    events = fetch_macro_calendar(state)
    now = datetime.now(timezone.utc)
    nearest_name, nearest_mins = None, None
    for ev in events:
        try:
            ev_dt = datetime.fromisoformat(ev["datetime_utc"])
        except Exception:
            continue
        mins = (ev_dt - now).total_seconds() / 60.0
        if -MACRO_WINDOW_AFTER_MINS <= mins <= MACRO_WINDOW_BEFORE_MINS:
            if nearest_mins is None or abs(mins) < abs(nearest_mins):
                nearest_name, nearest_mins = ev["name"], mins
    if nearest_name is None:
        return {"in_window": False, "score_adj": 0.0, "hard_suppress": False, "label": None}
    threshold = MACRO_HIGH_ATR_SUPPRESS_PCT
    if atr_pctile > MACRO_ATR_PCTILE_HIGH:
        threshold *= MACRO_ATR_PCTILE_HIGH_MULT
    return {
        "in_window": True, "score_adj": -0.7, "hard_suppress": atr_pct < threshold,
        "label": f"Macro: {nearest_name} ({int(nearest_mins):+d}m)",
    }


# ═══════════════════════════════════════════════════════════════════════════
# MARKET STRUCTURE & LIQUIDITY
# ═══════════════════════════════════════════════════════════════════════════

SWING_MIN_ATR = 0.5  # a swing only registers once price travels this many ATRs
                      # from the prior pivot -- filters mechanically
                      # insignificant micro-pivots out of BOS/CHoCH detection


def find_swings(candles: list[dict], atr_vals: list[float], left: int = 2, right: int = 2) -> list[Swing]:
    swings: list[Swing] = []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        a = atr_vals[i] if i < len(atr_vals) and not math.isnan(atr_vals[i]) else 0.0
        if candles[i]["h"] == max(window_h):
            if not swings or swings[-1].kind != "high" or \
               abs(candles[i]["h"] - swings[-1].price) >= SWING_MIN_ATR * a:
                swings.append(Swing(i, candles[i]["h"], "high"))
        if candles[i]["l"] == min(window_l):
            if not swings or swings[-1].kind != "low" or \
               abs(swings[-1].price - candles[i]["l"]) >= SWING_MIN_ATR * a:
                swings.append(Swing(i, candles[i]["l"], "low"))
    return swings


def analyze_structure(candles: list[dict], swings: list[Swing], atr_vals: list[float]) -> StructureState:
    if len(swings) < 2:
        return StructureState(bias="neutral")
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    events: list[StructureEvent] = []
    bias = "neutral"
    last_high = highs[-1] if highs else None
    last_low = lows[-1] if lows else None

    for i in range(2, len(swings)):
        cur = swings[i]
        prior_same_kind = next((s for s in reversed(swings[:i]) if s.kind == cur.kind), None)
        if prior_same_kind is None:
            continue
        a = atr_vals[cur.index] if cur.index < len(atr_vals) and not math.isnan(atr_vals[cur.index]) else 0.0
        if cur.kind == "high" and cur.price > prior_same_kind.price:
            quality = safe((cur.price - prior_same_kind.price) / a, 0.0) if a else 0.0
            kind = "choch" if bias == "bearish" else "bos"
            events.append(StructureEvent(cur.index, kind, "bull", cur.price, quality))
            bias = "bullish"
        elif cur.kind == "low" and cur.price < prior_same_kind.price:
            quality = safe((prior_same_kind.price - cur.price) / a, 0.0) if a else 0.0
            kind = "choch" if bias == "bullish" else "bos"
            events.append(StructureEvent(cur.index, kind, "bear", cur.price, quality))
            bias = "bearish"

    return StructureState(bias=bias, events=events, last_swing_high=last_high, last_swing_low=last_low)


def _cluster_levels(levels: list[float], tol_pct: float = 0.0018) -> list[tuple[float, int]]:
    if not levels:
        return []
    levels = sorted(levels)
    clusters: list[list[float]] = [[levels[0]]]
    for lv in levels[1:]:
        if abs(lv - clusters[-1][-1]) / clusters[-1][-1] <= tol_pct:
            clusters[-1].append(lv)
        else:
            clusters.append([lv])
    return [(sum(c) / len(c), len(c)) for c in clusters]


def build_liquidity_pools(swings: list[Swing]) -> list[LiquidityPool]:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    pools = []
    for level, touches in _cluster_levels(highs):
        if touches >= 2:
            pools.append(LiquidityPool(level, "buyside", touches, 0))
    for level, touches in _cluster_levels(lows):
        if touches >= 2:
            pools.append(LiquidityPool(level, "sellside", touches, 0))
    return pools


def detect_sweep(candles: list[dict], pools: list[LiquidityPool], direction: str,
                  atr_val: float, lookback: int = 10) -> Optional[SweepEvent]:
    """direction 'long' looks for a sellside sweep (stop hunt below lows)
    followed by a close back above; 'short' the mirror on buyside pools."""
    side = "sellside" if direction == "long" else "buyside"
    relevant = [p for p in pools if p.direction == side and not p.swept]
    if not relevant:
        return None
    window = candles[-lookback:]
    best = None
    for offset, bar in enumerate(window):
        idx = len(candles) - lookback + offset
        for pool in relevant:
            if side == "sellside" and bar["l"] < pool.level and bar["c"] > pool.level:
                ratio = safe((pool.level - bar["l"]) / atr_val, 0.0) if atr_val else 0.0
                if ratio >= 0.10 and (best is None or ratio > best.atr_ratio):
                    best = SweepEvent(idx, pool.level, bar["l"], ratio)
            elif side == "buyside" and bar["h"] > pool.level and bar["c"] < pool.level:
                ratio = safe((bar["h"] - pool.level) / atr_val, 0.0) if atr_val else 0.0
                if ratio >= 0.10 and (best is None or ratio > best.atr_ratio):
                    best = SweepEvent(idx, pool.level, bar["h"], ratio)
    return best


def premium_discount_zone(candles: list[dict], lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    rng = hi - lo
    if rng <= 0:
        return {"zone": "equilibrium", "pct": 0.5, "high": hi, "low": lo}
    pct = (candles[-1]["c"] - lo) / rng
    if pct >= 0.62:
        zone = "premium"
    elif pct <= 0.38:
        zone = "discount"
    else:
        zone = "equilibrium"
    return {"zone": zone, "pct": round(pct, 3), "high": hi, "low": lo}


def fib_ote_confluence(direction: str, entry: float, swing_high: float, swing_low: float) -> Optional[str]:
    """Optimal-trade-entry check: does the proposed entry sit inside the
    61.8-79% retracement band of the most recent impulse leg?"""
    rng = swing_high - swing_low
    if rng <= 0:
        return None
    if direction == "long":
        ote_lo = swing_high - rng * 0.79
        ote_hi = swing_high - rng * 0.618
    else:
        ote_lo = swing_low + rng * 0.618
        ote_hi = swing_low + rng * 0.79
    lo, hi = min(ote_lo, ote_hi), max(ote_lo, ote_hi)
    if lo <= entry <= hi:
        return "fib_ote_61_79"
    return None


def find_order_blocks(candles: list[dict], structure: StructureState, atr_vals: list[float],
                       lookback: int = 60) -> list[Zone]:
    """Last opposite-direction candle before a BOS/CHoCH impulse leg."""
    zones = []
    for ev in structure.events[-6:]:
        start = max(0, ev.index - lookback)
        seg = candles[start:ev.index + 1]
        if len(seg) < 3:
            continue
        if ev.direction == "bull":
            candidates = [i for i in range(len(seg) - 1) if seg[i]["c"] < seg[i]["o"]]
            if candidates:
                c = seg[candidates[-1]]
                zones.append(Zone(c["h"], c["l"], "bull", start + candidates[-1], "ob"))
        else:
            candidates = [i for i in range(len(seg) - 1) if seg[i]["c"] > seg[i]["o"]]
            if candidates:
                c = seg[candidates[-1]]
                zones.append(Zone(c["h"], c["l"], "bear", start + candidates[-1], "ob"))
    return zones


def find_fvgs(candles: list[dict], atr_val: float, lookback: int = 60, min_size_atr: float = 0.15) -> list[Zone]:
    zones = []
    start = max(2, len(candles) - lookback)
    for i in range(start, len(candles)):
        c0, c2 = candles[i - 2], candles[i]
        if c2["l"] > c0["h"] and (c2["l"] - c0["h"]) >= min_size_atr * atr_val:
            zones.append(Zone(c2["l"], c0["h"], "bull", i, "fvg"))
        elif c2["h"] < c0["l"] and (c0["l"] - c2["h"]) >= min_size_atr * atr_val:
            zones.append(Zone(c0["l"], c2["h"], "bear", i, "fvg"))
    return zones


def mark_zone_lifecycle(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    """Freshness/mitigation state, checked against every candle formed after
    the zone. A zone touched but not closed through is 'tested'; one closed
    through more than 70% is 'mitigated' and no longer a valid POI."""
    for z in zones:
        touches = 0
        deepest = 0.0
        for c in candles[z.index + 1:]:
            if z.direction == "bull":
                if c["l"] <= z.high:
                    touches += 1
                    depth = (z.high - max(c["l"], z.low)) / (z.high - z.low) if z.high > z.low else 0.0
                    deepest = max(deepest, depth)
            else:
                if c["h"] >= z.low:
                    touches += 1
                    depth = (min(c["h"], z.high) - z.low) / (z.high - z.low) if z.high > z.low else 0.0
                    deepest = max(deepest, depth)
        z.mitigation_pct = round(deepest, 3)
        z.state = "mitigated" if deepest >= 0.7 else ("tested" if touches > 0 else "fresh")
    return zones


def nearest_untested_poi(zones: list[Zone], direction: str, price: float, atr_val: float,
                          max_atr_distance: float = 3.0) -> Optional[Zone]:
    want_dir = "bull" if direction == "long" else "bear"
    candidates = [z for z in zones if z.direction == want_dir and z.state in ("fresh", "tested")
                  and abs(price - ((z.high + z.low) / 2)) <= max_atr_distance * atr_val]
    if not candidates:
        return None
    return min(candidates, key=lambda z: abs(price - ((z.high + z.low) / 2)))


# ═══════════════════════════════════════════════════════════════════════════
# ORDERFLOW / VOLUME
# ═══════════════════════════════════════════════════════════════════════════

def orderflow_proxy(candles: list[dict], direction: str, lookback: int = 24) -> dict:
    """Without a resident order-book feed on every bar, intrabar buy/sell
    pressure is approximated via close-location-value weighted by volume --
    a standard cumulative-delta proxy. Net pressure and its short-term slope
    are compared against trade direction for alignment."""
    window = candles[-lookback:]
    cvd, series, buy_vol, sell_vol = 0.0, [], 0.0, 0.0
    for bar in window:
        rng = bar["h"] - bar["l"]
        clv = ((bar["c"] - bar["l"]) - (bar["h"] - bar["c"])) / rng if rng > 0 else 0.0
        delta = clv * bar["v"]
        cvd += delta
        series.append(cvd)
        if delta >= 0:
            buy_vol += abs(delta)
        else:
            sell_vol += abs(delta)
    total = buy_vol + sell_vol
    buy_ratio = (buy_vol / total) if total > 0 else 0.5
    slope = (series[-1] - series[-6]) if len(series) >= 6 else 0.0
    aligned = (direction == "long" and buy_ratio > 0.52 and slope > 0) or \
              (direction == "short" and buy_ratio < 0.48 and slope < 0)
    return {"buy_ratio": round(buy_ratio, 3), "slope": slope, "aligned": aligned}


def volume_confirmation(candles: list[dict], ind: dict) -> dict:
    vol_now = candles[-1]["v"]
    vol_sma_now = next((v for v in reversed(ind["vol_sma"]) if not math.isnan(v)), None)
    if not vol_sma_now or vol_sma_now <= 0:
        return {"ratio": 1.0, "expanding": False}
    ratio = vol_now / vol_sma_now
    return {"ratio": round(ratio, 3), "expanding": ratio >= 1.15}


# ═══════════════════════════════════════════════════════════════════════════
# RISK MANAGEMENT — adaptive SL buffer, TP planning
# ═══════════════════════════════════════════════════════════════════════════

def adaptive_sl_buffer(atr_val: float, vol_pctile: float, pipeline: Pipeline) -> float:
    """Wider buffer in high-vol-percentile regimes (avoid wick-outs), tighter
    in calm ones (preserve RR)."""
    mult = pipeline.sl_buffer_atr
    if vol_pctile > 0.75:
        mult *= 1.35
    elif vol_pctile < 0.25:
        mult *= 0.85
    return atr_val * mult


def clip_tp_to_liquidity(entry: float, tp: float, direction: str, pools: list[LiquidityPool]) -> float:
    """If an opposing liquidity pool sits between entry and the raw TP,
    clip the target just short of it -- price is more likely to react
    there than to punch straight through on the first attempt."""
    side = "buyside" if direction == "long" else "sellside"
    candidates = [p.level for p in pools if p.direction == side]
    if direction == "long":
        between = [lv for lv in candidates if entry < lv < tp]
        if between:
            return min(between) * 0.999
    else:
        between = [lv for lv in candidates if tp < lv < entry]
        if between:
            return max(between) * 1.001
    return tp


def enforce_tp_order(entry: float, tp1: float, tp2: float, direction: str) -> float:
    if direction == "long" and tp2 <= tp1:
        return tp1 * 1.01
    if direction == "short" and tp2 >= tp1:
        return tp1 * 0.99
    return tp2


def build_risk_plan(direction: str, entry: float, atr_val: float, vol_pctile: float,
                     pipeline: Pipeline, pools: list[LiquidityPool]) -> dict:
    sl_buf = adaptive_sl_buffer(atr_val, vol_pctile, pipeline)
    # This floor exists only to guard against a degenerate (near-zero) buffer
    # -- it must stay below every real sl_buf the pipelines can produce, or
    # it silently overrides the vol-adaptive sizing above and every SL ends
    # up at the same flat distance regardless of pipeline or vol regime.
    risk = max(sl_buf, atr_val * 0.30)
    if direction == "long":
        sl = entry - risk
        tp1 = entry + risk * pipeline.tp1_fallback_rr
        tp2 = entry + risk * pipeline.tp2_fallback_rr
        tp3 = entry + risk * pipeline.tp3_fallback_rr
    else:
        sl = entry + risk
        tp1 = entry - risk * pipeline.tp1_fallback_rr
        tp2 = entry - risk * pipeline.tp2_fallback_rr
        tp3 = entry - risk * pipeline.tp3_fallback_rr
    tp1 = clip_tp_to_liquidity(entry, tp1, direction, pools)
    tp2 = clip_tp_to_liquidity(entry, tp2, direction, pools)
    tp2 = enforce_tp_order(entry, tp1, tp2, direction)
    return {"stop_loss": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3, "risk": risk}


# ═══════════════════════════════════════════════════════════════════════════
# PATHWAY 1 — LIQUIDITY REVERSAL (hard-gated SMC sequence, per Nyx's design:
# sweep -> displacement+CHoCH -> return to imbalance. These are gates, not
# scoring bonuses -- a candidate that fails any one of them does not exist.)
# ═══════════════════════════════════════════════════════════════════════════

def pathway_liquidity_reversal(symbol: str, bundle: dict, pipeline: Pipeline,
                                near_miss: Counter) -> Optional[Candidate]:
    bias_candles = bundle[pipeline.bias_tf]
    trig_candles = bundle[pipeline.trigger_tf]
    if len(bias_candles) < 60 or len(trig_candles) < 60:
        return None

    ind_bias = get_cached_indicators(symbol, pipeline.bias_tf, bias_candles)
    ind_trig = get_cached_indicators(symbol, pipeline.trigger_tf, trig_candles)
    atr_bias = ind_bias["atr"][-1]
    atr_trig = ind_trig["atr"][-1]
    if not atr_bias or not atr_trig:
        return None

    # Gate 1: HTF structure + EMA double-confirmed bias (a lone swing read
    # can't set direction; EMA must agree, per Nyx's fix over its own v1).
    swings_bias = find_swings(bias_candles, ind_bias["atr"], pipeline.swing_left, pipeline.swing_right)
    struct_bias = analyze_structure(bias_candles, swings_bias, ind_bias["atr"])
    price = bias_candles[-1]["c"]
    ef, es = ind_bias["ema_fast"][-1], ind_bias["ema_mid"][-1]
    ema_bull = price > ef > es
    ema_bear = price < ef < es
    if struct_bias.bias == "bullish" and ema_bull:
        direction = "long"
    elif struct_bias.bias == "bearish" and ema_bear:
        direction = "short"
    else:
        near_miss["liq_rev:no_htf_bias"] += 1
        return None

    pd_zone = premium_discount_zone(bias_candles)
    if direction == "long" and pd_zone["zone"] == "premium":
        near_miss["liq_rev:wrong_side_of_pd"] += 1
        return None
    if direction == "short" and pd_zone["zone"] == "discount":
        near_miss["liq_rev:wrong_side_of_pd"] += 1
        return None

    # Gate 2: sweep of external liquidity on the trigger timeframe.
    swings_trig = find_swings(trig_candles, ind_trig["atr"], 2, 2)
    pools = build_liquidity_pools(swings_trig)
    sweep = detect_sweep(trig_candles, pools, direction, atr_trig)
    if sweep is None:
        near_miss["liq_rev:no_sweep"] += 1
        return None

    # Gate 3: displacement + CHoCH back in the intended direction after the
    # sweep (a real reversal, not just a stop-run continuation of the sweep).
    post_sweep = trig_candles[sweep.index:]
    if len(post_sweep) < 3:
        near_miss["liq_rev:insufficient_bars_post_sweep"] += 1
        return None
    leg_start_close = post_sweep[0]["c"]
    leg_end_close = post_sweep[-1]["c"]
    displacement_atr = safe(abs(leg_end_close - leg_start_close) / atr_trig, 0.0)
    if displacement_atr < 0.8:
        near_miss["liq_rev:weak_displacement"] += 1
        return None
    body_ratios = [abs(c["c"] - c["o"]) / (c["h"] - c["l"]) for c in post_sweep if c["h"] > c["l"]]
    avg_body_ratio = sum(body_ratios) / len(body_ratios) if body_ratios else 0.0
    if avg_body_ratio < 0.55:
        near_miss["liq_rev:weak_body_ratio"] += 1
        return None
    moved_bull = leg_end_close > leg_start_close
    if (direction == "long" and not moved_bull) or (direction == "short" and moved_bull):
        near_miss["liq_rev:displacement_wrong_way"] += 1
        return None

    # Gate 4: return to an imbalance / order block for entry.
    fvgs = mark_zone_lifecycle(find_fvgs(trig_candles, atr_trig), trig_candles)
    obs = mark_zone_lifecycle(find_order_blocks(trig_candles,
                               analyze_structure(trig_candles, swings_trig, ind_trig["atr"]),
                               ind_trig["atr"]), trig_candles)
    poi = nearest_untested_poi(fvgs + obs, direction, trig_candles[-1]["c"], atr_trig,
                                pipeline.entry_max_dist_atr)
    if poi is None:
        near_miss["liq_rev:no_poi"] += 1
        return None

    entry = (poi.high + poi.low) / 2
    confluences = ["sweep_of_liquidity", "displacement_choch"]
    if displacement_atr >= 1.6:
        confluences.append("strong_displacement")
    if poi.kind == "fvg":
        confluences.append("fvg_entry")
    else:
        confluences.append("order_block_entry")
    if poi.state == "fresh":
        confluences.append("fresh_poi")
    ofp = orderflow_proxy(trig_candles, direction)
    if ofp["aligned"]:
        confluences.append("orderflow_aligned")
    else:
        confluences.append("caution:orderflow_against")
    vol = volume_confirmation(trig_candles, ind_trig)
    if vol["expanding"]:
        confluences.append("volume_expansion")
    ote = fib_ote_confluence(direction,
                              entry,
                              struct_bias.last_swing_high.price if struct_bias.last_swing_high else entry,
                              struct_bias.last_swing_low.price if struct_bias.last_swing_low else entry)
    if ote:
        confluences.append(ote)

    risk_plan = build_risk_plan(direction, entry, atr_trig, 0.5, pipeline, pools)
    return Candidate(
        symbol=symbol, direction=direction, pipeline_id=pipeline.id, pathway="liquidity_reversal",
        entry_zone_high=poi.high, entry_zone_low=poi.low, exact_entry=entry,
        stop_loss=risk_plan["stop_loss"], take_profit_1=risk_plan["tp1"],
        take_profit_2=risk_plan["tp2"], take_profit_3=risk_plan["tp3"], atr_val=atr_trig,
        confluences=confluences, structure_quality=displacement_atr, poi_state=poi.state,
    )


# ═══════════════════════════════════════════════════════════════════════════
# PATHWAY 2 — TREND CONTINUATION (HTF bias + pullback into discount/premium
# or a fresh order block, momentum-confirmed)
# ═══════════════════════════════════════════════════════════════════════════

def pathway_trend_continuation(symbol: str, bundle: dict, pipeline: Pipeline,
                                 near_miss: Counter) -> Optional[Candidate]:
    bias_candles = bundle[pipeline.bias_tf]
    trig_candles = bundle[pipeline.trigger_tf]
    if len(bias_candles) < 60 or len(trig_candles) < 60:
        return None

    ind_bias = get_cached_indicators(symbol, pipeline.bias_tf, bias_candles)
    ind_trig = get_cached_indicators(symbol, pipeline.trigger_tf, trig_candles)
    atr_trig = ind_trig["atr"][-1]
    if not atr_trig:
        return None

    price = bias_candles[-1]["c"]
    ef, es, et = ind_bias["ema_fast"][-1], ind_bias["ema_mid"][-1], ind_bias["ema_slow"][-1]
    adx_bias = safe(ind_bias["adx"][-1], 0.0)
    if price > ef > es > et and adx_bias >= 20:
        direction = "long"
    elif price < ef < es < et and adx_bias >= 20:
        direction = "short"
    else:
        near_miss["trend_cont:no_trend"] += 1
        return None

    pd_zone = premium_discount_zone(bias_candles)
    wants_zone = "discount" if direction == "long" else "premium"
    swings_trig = find_swings(trig_candles, ind_trig["atr"], 2, 2)
    struct_trig = analyze_structure(trig_candles, swings_trig, ind_trig["atr"])
    obs = mark_zone_lifecycle(find_order_blocks(trig_candles, struct_trig, ind_trig["atr"]), trig_candles)
    poi = nearest_untested_poi(obs, direction, trig_candles[-1]["c"], atr_trig, pipeline.entry_max_dist_atr)

    if pd_zone["zone"] != wants_zone and poi is None:
        near_miss["trend_cont:no_pullback_zone"] += 1
        return None

    ind_1h_equiv = ind_trig
    r = ind_1h_equiv["rsi"][-1]
    momentum_ok = (direction == "long" and 35 <= r <= 68) or (direction == "short" and 32 <= r <= 65)
    if not momentum_ok:
        near_miss["trend_cont:momentum_extended"] += 1
        return None

    entry = ((poi.high + poi.low) / 2) if poi else trig_candles[-1]["c"]

    confluences = ["htf_trend_confirmed"]
    if adx_bias >= 28:
        confluences.append("strong_adx")
    if pd_zone["zone"] == wants_zone:
        confluences.append(f"{wants_zone}_pullback")
    if poi is not None:
        confluences.append("order_block_entry")
        if poi.state == "fresh":
            confluences.append("fresh_poi")
    ofp = orderflow_proxy(trig_candles, direction)
    if ofp["aligned"]:
        confluences.append("orderflow_aligned")
    else:
        confluences.append("caution:orderflow_against")
    if ind_trig["rsi_divergence"] == ("bullish" if direction == "short" else "bearish"):
        confluences.append("caution:counter_divergence")

    pools = build_liquidity_pools(swings_trig)
    risk_plan = build_risk_plan(direction, entry, atr_trig, 0.5, pipeline, pools)
    return Candidate(
        symbol=symbol, direction=direction, pipeline_id=pipeline.id, pathway="trend_continuation",
        entry_zone_high=(poi.high if poi else entry * 1.001),
        entry_zone_low=(poi.low if poi else entry * 0.999),
        exact_entry=entry, stop_loss=risk_plan["stop_loss"], take_profit_1=risk_plan["tp1"],
        take_profit_2=risk_plan["tp2"], take_profit_3=risk_plan["tp3"], atr_val=atr_trig,
        confluences=confluences, structure_quality=adx_bias / 40.0,
        poi_state=(poi.state if poi else "n/a"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# PATHWAY 3 — MOMENTUM BREAKOUT (volatility compression or Donchian break,
# volume expansion, orderflow alignment -- for trending/volatile regimes)
# ═══════════════════════════════════════════════════════════════════════════

def pathway_momentum_breakout(symbol: str, bundle: dict, pipeline: Pipeline,
                               near_miss: Counter) -> Optional[Candidate]:
    trig_candles = bundle[pipeline.trigger_tf]
    if len(trig_candles) < 60:
        return None
    ind = get_cached_indicators(symbol, pipeline.trigger_tf, trig_candles)
    atr_val = ind["atr"][-1]
    if not atr_val:
        return None

    price = trig_candles[-1]["c"]
    don_up, don_lo = ind["don_up"][-2], ind["don_lo"][-2]  # prior bar's channel
    bb_width_now = ind["bb_width_pct"][-1]
    bb_width_hist = [w for w in ind["bb_width_pct"][-40:] if not math.isnan(w)]
    squeeze = bb_width_hist and bb_width_now <= sorted(bb_width_hist)[max(0, int(len(bb_width_hist) * 0.25))]

    direction = None
    if not math.isnan(don_up) and price > don_up:
        direction = "long"
    elif not math.isnan(don_lo) and price < don_lo:
        direction = "short"
    if direction is None:
        near_miss["mom_break:no_breakout"] += 1
        return None

    vol = volume_confirmation(trig_candles, ind)
    if not vol["expanding"]:
        near_miss["mom_break:no_volume_expansion"] += 1
        return None

    ofp = orderflow_proxy(trig_candles, direction)
    if not ofp["aligned"]:
        near_miss["mom_break:orderflow_against"] += 1
        return None

    adx_v = safe(ind["adx"][-1], 0.0)
    if adx_v < 18:
        near_miss["mom_break:weak_adx"] += 1
        return None

    entry = price
    confluences = ["donchian_breakout", "volume_expansion", "orderflow_aligned"]
    if squeeze:
        confluences.append("post_compression_release")
    if adx_v >= 25:
        confluences.append("strong_adx")
    else:
        confluences.append("caution:moderate_adx")

    swings = find_swings(trig_candles, ind["atr"], 2, 2)
    pools = build_liquidity_pools(swings)
    risk_plan = build_risk_plan(direction, entry, atr_val, 0.6, pipeline, pools)
    # Zone is centered on exact_entry (same convention as the other two
    # pathways, where entry is the POI midpoint) instead of pinning entry to
    # one edge of the zone.
    zone_pad = entry * 0.00075
    return Candidate(
        symbol=symbol, direction=direction, pipeline_id=pipeline.id, pathway="momentum_breakout",
        entry_zone_high=entry + zone_pad,
        entry_zone_low=entry - zone_pad,
        exact_entry=entry, stop_loss=risk_plan["stop_loss"], take_profit_1=risk_plan["tp1"],
        take_profit_2=risk_plan["tp2"], take_profit_3=risk_plan["tp3"], atr_val=atr_val,
        confluences=confluences, structure_quality=adx_v / 40.0, poi_state="n/a",
    )


PATHWAYS = {
    "liquidity_reversal": pathway_liquidity_reversal,
    "trend_continuation": pathway_trend_continuation,
    "momentum_breakout": pathway_momentum_breakout,
}


# ═══════════════════════════════════════════════════════════════════════════
# SCORING / CONFIDENCE ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def logistic(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def setup_prior_winrate(state: dict, pathway: str, symbol: str) -> tuple[float, int]:
    """Live win-rate for this pathway, falling back to a neutral 0.5 prior
    until enough history accumulates so early samples don't dominate."""
    history = [h for h in state.get("signal_history", [])
               if h.get("pathway") == pathway and h.get("result") in ("win", "loss")]
    n = len(history)
    if n < MIN_SAMPLE_FOR_PRIOR:
        return 0.5, n
    wins = sum(1 for h in history if h["result"] == "win")
    return wins / n, n


def symbol_recent_streak(state: dict, symbol: str, direction: str, lookback: int = 6) -> int:
    """Positive = winning streak, negative = losing streak, for the
    grade-floor gate (never a hard veto -- see win_rate_suppression_grade)."""
    hist = [h for h in state.get("signal_history", [])
            if h.get("symbol") == symbol and h.get("direction") == direction
            and h.get("result") in ("win", "loss")]
    hist = hist[-lookback:]
    streak = 0
    for h in reversed(hist):
        sign = 1 if h["result"] == "win" else -1
        if streak == 0 or (streak > 0) == (sign > 0):
            streak += sign
        else:
            break
    return streak


def score_candidate(cand: Candidate, regime: RegimeVector, state: dict,
                     book_imbalance: float, macro: dict, breadth_adj: float,
                     rs_percentile: Optional[float], convergence_tags: list) -> tuple[float, dict]:
    z = 0.0
    breakdown = {}

    caution_count = sum(1 for c in cand.confluences if c.startswith("caution:"))
    positive_count = len(cand.confluences) - caution_count
    breakdown["confluence"] = 0.85 * (positive_count - 1.5) - 0.75 * caution_count
    z += breakdown["confluence"]

    breakdown["rr"] = 1.0 * (cand.rr() - MIN_RR)
    z += breakdown["rr"]

    breakdown["regime"] = 1.2 * (regime.composite_favorability() - 0.5)
    z += breakdown["regime"]

    if cand.symbol != BTC_SYMBOL:
        btc_agree = (cand.direction == "long" and regime.btc_bias == "bullish") or \
                    (cand.direction == "short" and regime.btc_bias == "bearish")
        btc_against = (cand.direction == "long" and regime.btc_bias == "bearish") or \
                      (cand.direction == "short" and regime.btc_bias == "bullish")
        breakdown["btc_alignment"] = 0.45 if btc_agree else (-0.65 if btc_against else 0.0)
    else:
        breakdown["btc_alignment"] = 0.0
    z += breakdown["btc_alignment"]

    breakdown["orderbook"] = 0.55 * book_imbalance if cand.direction == "long" else -0.55 * book_imbalance
    z += breakdown["orderbook"]

    prior, n_samples = setup_prior_winrate(state, cand.pathway, cand.symbol)
    prior_weight = min(1.0, n_samples / 25.0)
    breakdown["historical_prior"] = 1.3 * prior_weight * (prior - 0.5)
    z += breakdown["historical_prior"]

    breakdown["breadth"] = breadth_adj
    z += breakdown["breadth"]

    if rs_percentile is not None:
        if rs_percentile >= (1.0 - RS_TOP_PERCENTILE):
            breakdown["relative_strength"] = 0.35 if cand.direction == "long" else -0.35
        elif rs_percentile <= RS_BOTTOM_PERCENTILE:
            breakdown["relative_strength"] = -0.35 if cand.direction == "long" else 0.35
        else:
            breakdown["relative_strength"] = 0.0
    else:
        breakdown["relative_strength"] = 0.0
    z += breakdown["relative_strength"]

    breakdown["macro"] = macro.get("score_adj", 0.0)
    z += breakdown["macro"]

    # Convergence bonuses: independent agreement is real information.
    breakdown["convergence"] = 0.5 * len(convergence_tags)
    z += breakdown["convergence"]

    confidence = round(100 * logistic(z), 2)
    return confidence, breakdown


def grade_for_confidence(confidence: float) -> str:
    if confidence >= 80:
        return "A+"
    if confidence >= 68:
        return "A"
    if confidence >= 56:
        return "B"
    return "C"


_GRADE_ORDER = {"C": 0, "B": 1, "A": 2, "A+": 3}


def grade_at_least(grade: str, floor: str) -> bool:
    return _GRADE_ORDER.get(grade, 0) >= _GRADE_ORDER.get(floor, 0)


def win_rate_suppression_grade(state: dict, symbol: str, direction: str) -> Optional[str]:
    """Cold-streak awareness as a GRADE FLOOR, never a hard veto -- the
    Obsidian Edge lesson this fleet already learned. A losing streak raises
    the bar for that symbol+direction rather than blocking it outright."""
    streak = symbol_recent_streak(state, symbol, direction)
    if streak <= -2:
        return GRADE_FLOOR_ON_COLD_SYMBOL
    return None


def classify_duration(pipeline: Pipeline) -> str:
    return pipeline.hold_hint


# ═══════════════════════════════════════════════════════════════════════════
# CORRELATION CLUSTERING & DEDUPLICATION
# ═══════════════════════════════════════════════════════════════════════════

def compute_returns(candles: list[dict], lookback: int) -> list[float]:
    closes = [c["c"] for c in candles[-lookback - 1:]]
    return [safe((closes[i] - closes[i - 1]) / closes[i - 1], 0.0) for i in range(1, len(closes))]


def pearson(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    try:
        return statistics.correlation(a, b)
    except (statistics.StatisticsError, ValueError):
        return 0.0


def build_correlation_clusters(hourly_candles: dict[str, list[dict]]) -> list[set[str]]:
    """Computed fresh every scan from live 1h returns -- unconditionally,
    for every symbol with data, so it cannot silently skip a subset the way
    the shared liquidity_confluence utility bug allowed in the prior audit."""
    returns = {sym: compute_returns(c, CORR_LOOKBACK_BARS) for sym, c in hourly_candles.items() if c}
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

    clusters: dict[str, set] = {}
    for s in symbols:
        root = find(s)
        clusters.setdefault(root, set()).add(s)
    return list(clusters.values())


def dedup_correlated(ranked: list[Signal], clusters: list[set[str]]) -> list[Signal]:
    def cluster_of(sym: str) -> frozenset:
        for c in clusters:
            if sym in c:
                return frozenset(c)
        return frozenset({sym})

    best: dict[tuple, Signal] = {}
    for sig in ranked:
        key = (cluster_of(sig.candidate.symbol), sig.candidate.direction)
        if key not in best or sig.confidence > best[key].confidence:
            best[key] = sig
    return list(best.values())


def dedup_same_symbol(ranked: list[Signal]) -> list[Signal]:
    """Across both pipelines: keep only the single best signal per symbol
    per scan (Nyx's fix over the dual-combo design that let both fire)."""
    best: dict[str, Signal] = {}
    for sig in ranked:
        sym = sig.candidate.symbol
        if sym not in best or sig.confidence > best[sym].confidence:
            best[sym] = sig
    return list(best.values())


def apply_portfolio_caps(ranked: list[Signal], state: dict) -> list[Signal]:
    ranked = sorted(ranked, key=lambda s: s.confidence, reverse=True)
    accepted: list[Signal] = []
    dir_count = Counter()
    sector_count = Counter()
    concurrent_now = len([1 for v in state.get("active_signals", {}).values() if not v.get("resolved")])

    for sig in ranked:
        if len(accepted) >= TOP_N_SIGNALS_PER_SCAN:
            break
        if concurrent_now + len(accepted) >= MAX_CONCURRENT_ACTIVE_SIGNALS:
            break
        direction = sig.candidate.direction
        sector = SECTOR_MAP.get(sig.candidate.symbol, "other")
        if dir_count[direction] >= MAX_SAME_DIRECTION:
            continue
        if sector_count[sector] >= MAX_PER_SECTOR:
            continue
        accepted.append(sig)
        dir_count[direction] += 1
        sector_count[sector] += 1
    return accepted


# ═══════════════════════════════════════════════════════════════════════════
# HARD FILTERS, COOLDOWN, GOVERNOR
# ═══════════════════════════════════════════════════════════════════════════

def passes_hard_filters(symbol: str, cand: Candidate, atr_pct: float) -> tuple[bool, str]:
    ctx = get_market_ctx(symbol)
    oi_usd = ctx.get("oi_usd")
    if oi_usd is not None and oi_usd < MIN_OI_USD:
        return False, f"OI too low (${oi_usd:,.0f})"
    if not (MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT):
        return False, f"ATR% out of band ({atr_pct:.4f})"
    if cand.rr() < MIN_RR:
        return False, f"RR too low ({cand.rr():.2f})"
    mark = ctx.get("mark_px")
    if mark is not None:
        max_dist = cand.atr_val * 3.0
        if abs(cand.exact_entry - mark) > max_dist:
            return False, "entry too far from live price"
    return True, "ok"


def check_cooldown(state: dict, symbol: str, direction: str, pipeline_id: str, bar_index: int) -> bool:
    key = f"{symbol}:{direction}:{pipeline_id}"
    last = state.setdefault("cooldowns", {}).get(key, -999)
    cooldown_bars = int(PIPELINES[pipeline_id].cooldown_hours * (60 / 15))  # in 15m-equivalent bars
    return (bar_index - last) >= cooldown_bars


def update_cooldown(state: dict, symbol: str, direction: str, pipeline_id: str, bar_index: int) -> None:
    state.setdefault("cooldowns", {})[f"{symbol}:{direction}:{pipeline_id}"] = bar_index


def apply_post_loss_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    """After a loss, briefly tighten before the same symbol+direction can
    fire again -- independent of the pathway's own cooldown."""
    key = f"loss_cd:{symbol}:{direction}"
    last_loss_bar = state.setdefault("post_loss_cooldown", {}).get(key)
    if last_loss_bar is None:
        return True
    return (bar_index - last_loss_bar) >= POST_LOSS_COOLDOWN_BARS


def record_loss_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> None:
    state.setdefault("post_loss_cooldown", {})[f"loss_cd:{symbol}:{direction}"] = bar_index


def is_recent_duplicate(state: dict, symbol: str, direction: str, entry: float) -> bool:
    cutoff = time.time() - DEDUP_TIME_WINDOW_HOURS * 3600
    for h in state.get("signal_history", []):
        if h.get("symbol") != symbol or h.get("direction") != direction:
            continue
        if h.get("ts", 0) < cutoff:
            continue
        if h.get("entry") and abs(h["entry"] - entry) / entry <= DEDUP_PRICE_TOL_PCT:
            return True
    return False


def governor_adjust_threshold(state: dict) -> None:
    gov = state.setdefault("governor", {"threshold": 0.0, "daily_count_ema": 7.0, "last_adjust_ts": 0.0})
    now = time.time()
    count_24h = estimate_signals_last_24h(state)
    gov["daily_count_ema"] = 0.85 * gov["daily_count_ema"] + 0.15 * count_24h
    if now - gov.get("last_adjust_ts", 0) < GOVERNOR_MIN_INTERVAL_S:
        return
    ema_count = gov["daily_count_ema"]
    if ema_count < TARGET_SIGNALS_MIN:
        gov["threshold"] = max(GOVERNOR_FLOOR, gov["threshold"] - GOVERNOR_STEP)
        gov["last_adjust_ts"] = now
    elif ema_count > TARGET_SIGNALS_MAX:
        gov["threshold"] = min(GOVERNOR_CEIL, gov["threshold"] + GOVERNOR_STEP)
        gov["last_adjust_ts"] = now


def estimate_signals_last_24h(state: dict) -> int:
    cutoff = time.time() - 86400
    return sum(1 for h in state.get("signal_history", []) if h.get("ts", 0) >= cutoff)


# ═══════════════════════════════════════════════════════════════════════════
# STATE PERSISTENCE (defensive: .bak fallback + schema version check)
# ═══════════════════════════════════════════════════════════════════════════

def _default_state() -> dict:
    return {
        "_version": STATE_VERSION,
        "signal_history": [],
        "active_signals": {},
        "cooldowns": {},
        "post_loss_cooldown": {},
        "atr_pct_memory": {},
        "macro_calendar_cache": {},
        "governor": {"threshold": 0.0, "daily_count_ema": 7.0, "last_adjust_ts": 0.0},
        "last_scan_ts": 0.0,
        "last_summary_date": "",
        "win_rate": {},
    }


def load_state() -> dict:
    for path in (STATE_FILE, STATE_FILE.with_suffix(".bak")):
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
            if data.get("_version", 1) != STATE_VERSION:
                print(f"  [STATE] Schema version mismatch in {path} -- starting fresh")
                continue
            fresh = _default_state()
            for k, v in fresh.items():
                data.setdefault(k, v)
            if path != STATE_FILE:
                print(f"  [STATE] Loaded from backup {path}")
            print(f"  [STATE] Loaded {len(data.get('signal_history', []))} history rows, "
                  f"{len(data.get('active_signals', {}))} active signals")
            return data
        except Exception as e:
            print(f"  [STATE] Failed to load {path}: {e}")
    print("  [STATE] Starting fresh -- no valid state file found")
    return _default_state()


def save_state(state: dict) -> None:
    try:
        state_json = json.dumps(state, indent=2, default=str)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(state_json)
        if STATE_FILE.exists():
            STATE_FILE.replace(STATE_FILE.with_suffix(".bak"))
        os.replace(tmp, STATE_FILE)
        print(f"  [STATE] Saved {len(state.get('signal_history', []))} history rows, "
              f"{len(state.get('active_signals', {}))} active signals")
    except Exception as e:
        print(f"  [STATE] Save error: {e}")


def prune_state(state: dict) -> None:
    hist = state.get("signal_history", [])
    if len(hist) > MAX_SIGNAL_HISTORY:
        state["signal_history"] = hist[-MAX_SIGNAL_HISTORY:]
    now = time.time()
    for bucket, max_days in (("atr_pct_memory", None),):
        pass  # atr memory is length-capped at insert time, not by age
    stale = [k for k, s in state.get("active_signals", {}).items()
             if s.get("resolved") or now - s.get("sent_at", 0) > s.get("ttl_hours", 96) * 3600]
    for k in stale:
        state["active_signals"].pop(k, None)


def record_signal(state: dict, sig: Signal) -> str:
    cand = sig.candidate
    hist_id = f"{cand.symbol}_{cand.direction}_{int(time.time()*1000)}_{random.randint(100,999)}"
    state.setdefault("signal_history", []).append({
        "id": hist_id, "symbol": cand.symbol, "direction": cand.direction,
        "pathway": cand.pathway, "pipeline_id": cand.pipeline_id,
        "confidence": sig.confidence, "grade": sig.grade, "entry": cand.exact_entry,
        "stop_loss": cand.stop_loss, "tp1": cand.take_profit_1, "tp2": cand.take_profit_2,
        "result": "pending", "ts": time.time(),
    })
    return hist_id


def compute_win_rates(state: dict) -> dict:
    resolved = [h for h in state.get("signal_history", []) if h.get("result") in ("win", "loss")]
    out = {"overall": {"wins": 0, "losses": 0}}
    for h in resolved:
        bucket = out["overall"]
        bucket["wins" if h["result"] == "win" else "losses"] += 1
        key = f"pathway:{h.get('pathway','?')}"
        b2 = out.setdefault(key, {"wins": 0, "losses": 0})
        b2["wins" if h["result"] == "win" else "losses"] += 1
        key = f"pipeline:{h.get('pipeline_id','?')}"
        b3 = out.setdefault(key, {"wins": 0, "losses": 0})
        b3["wins" if h["result"] == "win" else "losses"] += 1
    return out


def get_win_rate_summary(state: dict) -> str:
    rates = compute_win_rates(state)
    overall = rates["overall"]
    total = overall["wins"] + overall["losses"]
    pct = (overall["wins"] / total * 100) if total else 0.0
    lines = [f"[WIN RATE] {overall['wins']}W / {overall['losses']}L ({pct:.1f}%)"]
    for key, b in rates.items():
        if key == "overall":
            continue
        t = b["wins"] + b["losses"]
        if t == 0:
            continue
        p = b["wins"] / t * 100
        lines.append(f"  [{key}] {b['wins']}W / {b['losses']}L ({p:.1f}%)")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

def fmt_px(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def fmt_px_copy(v: float) -> str:
    """Same precision as fmt_px but with no thousands separators, so tapping
    the <code> span in Telegram copies a clean number straight into an
    exchange order field instead of one with a stray comma in it."""
    return fmt_px(v).replace(",", "")


def confidence_bar(confidence: float) -> str:
    filled = round(confidence / 10)
    return "█" * filled + "░" * (10 - filled)


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_confluence_line(tag: str) -> str:
    label = tag.replace("caution:", "⚠️ ").replace("_", " ")
    return label if tag.startswith("caution:") else f"✓ {label}"


def format_signal_message(sig: Signal) -> str:
    cand = sig.candidate
    pipeline = PIPELINES[cand.pipeline_id]
    arrow = "🟢 LONG" if cand.direction == "long" else "🔴 SHORT"
    lines = [
        f"<b>{arrow} — {cand.symbol}</b>",
        f"<i>{ENGINE_NAME} v{VERSION} | {pipeline.label} | {cand.pathway.replace('_',' ').title()}</i>",
        "",
        f"<b>Entry Zone:</b> <code>{fmt_px_copy(cand.entry_zone_low)}</code> – <code>{fmt_px_copy(cand.entry_zone_high)}</code>",
        f"<b>Exact Entry:</b> <code>{fmt_px_copy(cand.exact_entry)}</code>",
        f"<b>Stop Loss:</b> <code>{fmt_px_copy(cand.stop_loss)}</code>",
        f"<b>TP1:</b> <code>{fmt_px_copy(cand.take_profit_1)}</code>  (RR {cand.rr():.2f})",
        f"<b>TP2:</b> <code>{fmt_px_copy(cand.take_profit_2)}</code>",
    ]
    if cand.take_profit_3:
        lines.append(f"<b>TP3 (runner):</b> <code>{fmt_px_copy(cand.take_profit_3)}</code>")
    lines += [
        "",
        f"<b>Confidence:</b> {sig.confidence:.1f}% {confidence_bar(sig.confidence)}",
        f"<b>Grade:</b> {sig.grade}  |  <b>Duration:</b> {sig.duration}",
        "",
        "<b>Confluences:</b>",
    ]
    for tag in cand.confluences:
        lines.append(format_confluence_line(tag))
    if sig.convergence_tags:
        lines.append("")
        lines.append("<b>Convergence:</b>")
        for tag in sig.convergence_tags:
            lines.append(f"✓ {tag.replace('_', ' ')}")
    return "\n".join(lines)


def send_telegram_get_id(text: str) -> Optional[int]:
    try:
        r = _tg_session.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        return data.get("result", {}).get("message_id")
    except Exception as e:
        print(f"  [TELEGRAM] send error: {e}")
        return None


def reply_telegram(text: str, reply_to_message_id: Optional[int]) -> Optional[int]:
    try:
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        if reply_to_message_id:
            payload["reply_to_message_id"] = reply_to_message_id
        r = _tg_session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                              json=payload, timeout=15)
        r.raise_for_status()
        return r.json().get("result", {}).get("message_id")
    except Exception as e:
        print(f"  [TELEGRAM] reply error: {e}")
        return None


def react_to_message(message_id: Optional[int], emoji: str) -> None:
    if not message_id:
        return
    try:
        _tg_session.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction",
            json={"chat_id": TG_CHAT_ID, "message_id": message_id,
                  "reaction": [{"type": "emoji", "emoji": emoji}]}, timeout=10,
        )
    except Exception as e:
        print(f"  [TELEGRAM] react error: {e}")


def live_price_drift_ok(cand: Candidate, all_mids: dict[str, float]) -> bool:
    """Re-checked against a live price fetch immediately before send -- if
    price has already run away from the plan since the candles closed,
    refuse to fire (ported from Nyx/Castellan)."""
    live = all_mids.get(hl_coin(cand.symbol))
    if live is None:
        return True  # can't verify -- don't block on a missing mid
    risk = abs(cand.exact_entry - cand.stop_loss)
    if risk <= 0:
        return True
    drift_r = abs(live - cand.exact_entry) / risk
    return drift_r <= MAX_ENTRY_DRIFT_R


# ═══════════════════════════════════════════════════════════════════════════
# ACTIVE SIGNAL TRACKING
# ═══════════════════════════════════════════════════════════════════════════

def track_signal(state: dict, sig: Signal, message_id: Optional[int], hist_id: str) -> None:
    cand = sig.candidate
    key = f"{cand.symbol}_{cand.pipeline_id}_{int(time.time()*1000)}"
    pipeline = PIPELINES[cand.pipeline_id]
    state.setdefault("active_signals", {})[key] = {
        "symbol": cand.symbol, "direction": cand.direction, "pipeline_id": cand.pipeline_id,
        "pathway": cand.pathway, "message_id": message_id, "hist_id": hist_id,
        "entry": cand.exact_entry, "stop_loss": cand.stop_loss,
        "tp1": cand.take_profit_1, "tp2": cand.take_profit_2, "tp3": cand.take_profit_3,
        "tp1_hit": False, "tp2_hit": False, "tp3_hit": False, "sl_hit": False,
        "resolved": False, "sent_at": time.time(), "ttl_hours": pipeline.active_ttl_hours,
        # Candle-close watermark consumed by check_active_signals -- only
        # candles at/after this timestamp still need to be scanned for a hit.
        "last_checked_ms": int(time.time() * 1000),
    }


def record_outcome(state: dict, hist_id: str, symbol: str, direction: str, bar_index: int,
                    result: str) -> None:
    for h in state.get("signal_history", []):
        if h.get("id") == hist_id:
            h["result"] = result
            h["resolved_ts"] = time.time()
            break
    if result == "loss":
        record_loss_cooldown(state, symbol, direction, bar_index)


def check_active_signals(state: dict, bar_index: int) -> None:
    """Resolve active signals off closed 15m candle ranges, never off a
    single live price snapshot. A point-in-time price can miss an
    intra-period SL or TP touch entirely if price round-trips between two
    15-minute runs -- and, worse, can report a hit out of order: e.g.
    flashing a TP1 win for a signal that was actually stopped out earlier in
    that same gap, simply because by the time the next snapshot was taken
    price had already recovered past SL and gone on to tag TP1. Walking
    every closed candle since the last check in chronological order, and
    always resolving stop loss before take-profit within a given candle,
    keeps both the presence and the sequencing of a hit correct."""
    active = state.get("active_signals", {})
    if not active:
        return

    tf = "15m"  # finest interval this file fetches; matches the cron cadence
    tf_ms = INTERVAL_MS[tf]
    now_ms = int(time.time() * 1000)

    # One candle fetch per symbol per run, sized to cover the oldest gap
    # among that symbol's unresolved signals, shared by every signal on it.
    unresolved_by_symbol: dict[str, list[str]] = {}
    for key, s in active.items():
        if not s.get("resolved"):
            unresolved_by_symbol.setdefault(s["symbol"], []).append(key)

    candles_by_symbol: dict[str, list[dict]] = {}
    for symbol, keys in unresolved_by_symbol.items():
        oldest_since_ms = min(
            active[k].get("last_checked_ms", int(active[k]["sent_at"] * 1000)) for k in keys
        )
        gap_candles = max(0, now_ms - oldest_since_ms) // tf_ms
        n_needed = min(600, max(2, gap_candles + 3))
        try:
            candles_by_symbol[symbol] = get_candles(symbol, tf, n_needed)
        except Exception as e:
            print(f"[TRACK CANDLES ERROR] {symbol}: {e}")
            candles_by_symbol[symbol] = []

    for key, s in list(active.items()):
        if s.get("resolved"):
            continue
        direction = s["direction"]
        since_ms = s.get("last_checked_ms", int(s["sent_at"] * 1000))
        candles = [c for c in candles_by_symbol.get(s["symbol"], []) if c["t"] >= since_ms]

        for c in candles:
            lo, hi = c["l"], c["h"]
            any_tp_hit = s["tp1_hit"] or s["tp2_hit"] or s["tp3_hit"]

            if s["stop_loss"] and not s["sl_hit"] and not any_tp_hit:
                hit = (lo <= s["stop_loss"]) if direction == "long" else (hi >= s["stop_loss"])
                if hit:
                    s["sl_hit"] = True
                    react_to_message(s["message_id"], "❌")
                    reply_telegram(f"❌ {s['symbol']} hit Stop Loss.", s["message_id"])
                    record_outcome(state, s["hist_id"], s["symbol"], direction, bar_index, "loss")
                    s["resolved"] = True
                    break  # SL closes the trade -- nothing later in this or any later candle matters

            if s["tp1"] and not s["tp1_hit"]:
                hit = (hi >= s["tp1"]) if direction == "long" else (lo <= s["tp1"])
                if hit:
                    s["tp1_hit"] = True
                    react_to_message(s["message_id"], "✅")
                    reply_telegram(f"✅ {s['symbol']} hit TP1.", s["message_id"])
                    if not s["tp2"]:
                        record_outcome(state, s["hist_id"], s["symbol"], direction, bar_index, "win")
                        s["resolved"] = True
                        break

            if s["tp2"] and s["tp1_hit"] and not s["tp2_hit"]:
                hit = (hi >= s["tp2"]) if direction == "long" else (lo <= s["tp2"])
                if hit:
                    s["tp2_hit"] = True
                    react_to_message(s["message_id"], "🎯")
                    reply_telegram(f"🎯 {s['symbol']} hit TP2.", s["message_id"])
                    if not s["tp3"]:
                        record_outcome(state, s["hist_id"], s["symbol"], direction, bar_index, "win")
                        s["resolved"] = True
                        break

            if s["tp3"] and s["tp2_hit"] and not s["tp3_hit"]:
                hit = (hi >= s["tp3"]) if direction == "long" else (lo <= s["tp3"])
                if hit:
                    s["tp3_hit"] = True
                    react_to_message(s["message_id"], "🏆")
                    reply_telegram(f"🏆 {s['symbol']} hit TP3 (full runner).", s["message_id"])
                    record_outcome(state, s["hist_id"], s["symbol"], direction, bar_index, "win")
                    s["resolved"] = True
                    break

        if candles:
            s["last_checked_ms"] = candles[-1]["t"] + tf_ms

        if not s["resolved"] and time.time() - s["sent_at"] > s.get("ttl_hours", 96) * 3600:
            record_outcome(state, s["hist_id"], s["symbol"], direction, bar_index,
                           "win" if s["tp1_hit"] else "timeout")
            s["resolved"] = True

    for key in [k for k, s in active.items() if s.get("resolved")]:
        active.pop(key, None)


# ═══════════════════════════════════════════════════════════════════════════
# DAILY PERFORMANCE SUMMARY
# ═══════════════════════════════════════════════════════════════════════════

def should_send_summary(state: dict) -> bool:
    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")
    if state.get("last_summary_date") == today_str:
        return False
    return now.hour >= DAILY_SUMMARY_HOUR_UTC


def generate_daily_summary(state: dict) -> str:
    rates = compute_win_rates(state)
    overall = rates["overall"]
    total = overall["wins"] + overall["losses"]
    pct = (overall["wins"] / total * 100) if total else 0.0
    count_24h = estimate_signals_last_24h(state)
    gov = state.get("governor", {})
    breadth = compute_market_breadth()
    lines = [
        f"<b>📊 {ENGINE_NAME} Daily Summary</b>",
        f"<i>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</i>",
        "",
        f"Signals (24h): {count_24h}",
        f"Overall: {overall['wins']}W / {overall['losses']}L ({pct:.1f}%)",
        f"Governor threshold: {gov.get('threshold', 0.0):+.2f} "
        f"(target {int(TARGET_SIGNALS_MIN)}-{int(TARGET_SIGNALS_MAX)}/day, "
        f"EMA {gov.get('daily_count_ema', 0.0):.1f})",
        f"{breadth['label']}",
        "",
        "<b>By pathway:</b>",
    ]
    for key, b in rates.items():
        if not key.startswith("pathway:"):
            continue
        t = b["wins"] + b["losses"]
        if t == 0:
            continue
        p = b["wins"] / t * 100
        lines.append(f"  {key.split(':')[1]}: {b['wins']}W/{b['losses']}L ({p:.1f}%)")
    lines.append("")
    lines.append("<b>By pipeline:</b>")
    for key, b in rates.items():
        if not key.startswith("pipeline:"):
            continue
        t = b["wins"] + b["losses"]
        if t == 0:
            continue
        p = b["wins"] / t * 100
        lines.append(f"  {key.split(':')[1]}: {b['wins']}W/{b['losses']}L ({p:.1f}%)")
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict) -> None:
    if not should_send_summary(state):
        return
    text = generate_daily_summary(state)
    send_telegram_get_id(text)
    state["last_summary_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ═══════════════════════════════════════════════════════════════════════════
# SYMBOL EVALUATION — orchestrates both pipelines x three pathways, applies
# convergence bonuses, hard filters, cooldowns, and scoring.
# ═══════════════════════════════════════════════════════════════════════════

def fetch_symbol_bundle(symbol: str) -> Optional[dict]:
    try:
        bundle = {
            "15m": get_candles(symbol, "15m", 220),
            "1h": get_candles(symbol, "1h", 220),
            "4h": get_candles_cached(symbol, "4h", 180),
            "1d": get_candles_cached(symbol, "1d", 120),
        }
        if any(len(v) < 50 for v in bundle.values()):
            return None
        return bundle
    except Exception as e:
        print(f"  [FETCH] {symbol} error: {e}")
        return None


def evaluate_symbol(symbol: str, bundle: dict, state: dict, btc_bias: str, btc_strength: float,
                     bar_index: int, near_miss: Counter) -> list[Signal]:
    signals: list[Signal] = []
    if btc_regime_blocks("long", symbol, btc_bias, btc_strength) and \
       btc_regime_blocks("short", symbol, btc_bias, btc_strength):
        return signals  # degenerate, but keep the check symmetrical/explicit

    pipeline_candidates: dict[str, list[Candidate]] = {"fast": [], "slow": []}

    for pipeline_id, pipeline in PIPELINES.items():
        if pipeline.session_gated and not is_fast_pipeline_session():
            continue
        for pathway_name, builder in PATHWAYS.items():
            try:
                cand = builder(symbol, bundle, pipeline, near_miss)
            except Exception as e:
                print(f"  [PATHWAY ERROR] {symbol}/{pipeline_id}/{pathway_name}: {e}")
                cand = None
            if cand is not None:
                pipeline_candidates[pipeline_id].append(cand)

    if not pipeline_candidates["fast"] and not pipeline_candidates["slow"]:
        return signals

    # Convergence detection across pathways (within a pipeline) and across
    # pipelines (same symbol+direction) -- agreement is scored, not just
    # deduplicated away.
    all_cands = pipeline_candidates["fast"] + pipeline_candidates["slow"]
    by_direction: dict[str, list[Candidate]] = {"long": [], "short": []}
    for c in all_cands:
        by_direction[c.direction].append(c)

    ind_1h_book = analyze_orderbook(symbol)
    ctx = get_market_ctx(symbol)
    funding = ctx.get("funding_rate")
    rs_val, rs_pctile = compute_relative_strength(symbol)

    for pipeline_id, cands in pipeline_candidates.items():
        if not cands:
            continue
        pipeline = PIPELINES[pipeline_id]
        # best-of-three pathways for this pipeline, ranked by raw structure quality
        cand = max(cands, key=lambda c: (len(c.confluences), c.structure_quality))

        if btc_regime_blocks(cand.direction, symbol, btc_bias, btc_strength):
            near_miss[f"{pipeline_id}:btc_regime_block"] += 1
            continue
        if not check_cooldown(state, symbol, cand.direction, pipeline_id, bar_index):
            near_miss[f"{pipeline_id}:cooldown"] += 1
            continue
        if not apply_post_loss_cooldown(state, symbol, cand.direction, bar_index):
            near_miss[f"{pipeline_id}:post_loss_cooldown"] += 1
            continue
        if is_recent_duplicate(state, symbol, cand.direction, cand.exact_entry):
            near_miss[f"{pipeline_id}:duplicate"] += 1
            continue

        ind_bias = get_cached_indicators(symbol, pipeline.bias_tf, bundle[pipeline.bias_tf])
        atr_pct = safe(ind_bias["atr"][-1] / ind_bias["closes"][-1], 0.01)
        ok, reason = passes_hard_filters(symbol, cand, atr_pct)
        if not ok:
            near_miss[f"{pipeline_id}:hard_filter:{reason}"] += 1
            continue

        breadth = compute_market_breadth()
        regime = build_regime_vector(state, symbol, bundle, pipeline, btc_bias, btc_strength,
                                      breadth["pct"], rs_pctile)

        macro = macro_filter(state, atr_pct, regime.vol_pctile)
        if macro.get("hard_suppress"):
            near_miss[f"{pipeline_id}:macro_suppress"] += 1
            continue

        if funding is not None and abs(funding) > FUNDING_ALIGN_THRESHOLD:
            funding_aligned = (cand.direction == "long" and funding < 0) or \
                              (cand.direction == "short" and funding > 0)
            cand.confluences.append("funding_aligned" if funding_aligned else "caution:funding_against")

        convergence_tags = []
        same_dir_other_pathways = [c for c in by_direction[cand.direction]
                                    if c.pathway != cand.pathway and c.pipeline_id == pipeline_id]
        if same_dir_other_pathways:
            convergence_tags.append("pathway_convergence")
        other_pipeline_id = "slow" if pipeline_id == "fast" else "fast"
        if any(c.direction == cand.direction for c in pipeline_candidates[other_pipeline_id]):
            convergence_tags.append("cross_pipeline_convergence")

        breadth_adj, breadth_label = breadth_score_adjustment(cand.direction, rs_val, btc_bias)
        confidence, breakdown = score_candidate(cand, regime, state, ind_1h_book.get("imbalance", 0.0),
                                                 macro, breadth_adj, rs_pctile, convergence_tags)

        z_effective = sum(breakdown.values())
        base_threshold = state.get("governor", {}).get("threshold", 0.0)
        threshold = adaptive_threshold(regime, base_threshold)
        if z_effective < threshold:
            near_miss[f"{pipeline_id}:below_threshold"] += 1
            continue

        grade = grade_for_confidence(confidence)
        floor = win_rate_suppression_grade(state, symbol, cand.direction)
        if floor and not grade_at_least(grade, floor):
            near_miss[f"{pipeline_id}:cold_streak_grade_floor"] += 1
            continue

        signals.append(Signal(
            candidate=cand, confidence=confidence, grade=grade,
            duration=classify_duration(pipeline), z_breakdown=breakdown,
            convergence_tags=convergence_tags,
            timestamp=datetime.now(timezone.utc).isoformat(),
        ))

    return signals


# ═══════════════════════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════════════════════

def _bar_index_now() -> int:
    """15m-equivalent bar index since epoch -- used for cooldown bookkeeping
    that must survive across scans without storing wall-clock timestamps."""
    return int(time.time() // (15 * 60))


def _prefetch(symbol: str) -> tuple[str, Optional[dict]]:
    return symbol, fetch_symbol_bundle(symbol)


def run_scan(state: dict) -> list[Signal]:
    clear_indicator_cache()
    reset_breadth_cache()
    reset_rs_cache()
    near_miss: Counter = Counter()
    bar_index = _bar_index_now()

    print(f"  [SCAN] Prefetching {len(WATCHLIST)} symbols (candles, market ctx)...")
    fetch_all_market_ctx()
    bundles: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_prefetch, sym) for sym in WATCHLIST]
        for fut in as_completed(futures):
            sym, bundle = fut.result()
            if bundle is not None:
                bundles[sym] = bundle

    if BTC_SYMBOL not in bundles:
        print("  [SCAN] Aborting -- no BTC data (regime filter needs it)")
        return []

    btc_bias, btc_strength = compute_btc_regime(bundles[BTC_SYMBOL])
    print(f"  [SCAN] BTC regime: {btc_bias} (ADX {btc_strength:.1f})")

    # Cross-sectional passes: breadth (price vs EMA50 on 4h) and relative
    # strength (return over RS_LOOKBACK_BARS on 4h) need every symbol's
    # data gathered before any single symbol can be scored against them.
    for sym, bundle in bundles.items():
        ind4h = get_cached_indicators(sym, "4h", bundle["4h"])
        above = ind4h["closes"][-1] > ind4h["ema_mid"][-1]
        record_breadth(sym, above)
        closes = ind4h["closes"]
        if len(closes) > RS_LOOKBACK_BARS:
            ret = safe((closes[-1] - closes[-RS_LOOKBACK_BARS]) / closes[-RS_LOOKBACK_BARS], 0.0)
            record_rs_return(sym, ret)

    governor_adjust_threshold(state)

    all_signals: list[Signal] = []
    for sym, bundle in bundles.items():
        try:
            sigs = evaluate_symbol(sym, bundle, state, btc_bias, btc_strength, bar_index, near_miss)
            all_signals.extend(sigs)
        except Exception as e:
            print(f"  [EVAL ERROR] {sym}: {e}")

    if near_miss:
        top_reasons = ", ".join(f"{k}={v}" for k, v in near_miss.most_common(8))
        print(f"  [NEAR-MISS] {top_reasons}")

    if not all_signals:
        print("  [SCAN] No candidates cleared all gates this scan.")
        return []

    # Correlation clustering (fresh, from live 1h returns) + dedup, applied
    # unconditionally before ranking.
    hourly = {sym: b["1h"] for sym, b in bundles.items()}
    clusters = build_correlation_clusters(hourly)
    ranked = dedup_correlated(all_signals, clusters)
    ranked = dedup_same_symbol(ranked)
    accepted = apply_portfolio_caps(ranked, state)

    if not accepted:
        print("  [SCAN] Candidates existed but none survived portfolio caps.")
        return []

    all_mids = fetch_all_mids()
    sent: list[Signal] = []
    for sig in accepted:
        cand = sig.candidate
        if not live_price_drift_ok(cand, all_mids):
            print(f"  [DRIFT] {cand.symbol} skipped -- price ran away from plan before send")
            continue
        text = format_signal_message(sig)
        message_id = send_telegram_get_id(text)
        hist_id = record_signal(state, sig)
        track_signal(state, sig, message_id, hist_id)
        update_cooldown(state, cand.symbol, cand.direction, cand.pipeline_id, bar_index)
        sent.append(sig)
        print(f"  [SENT] {cand.symbol} {cand.direction.upper()} | {cand.pathway} | "
              f"{PIPELINES[cand.pipeline_id].label} | conf={sig.confidence:.1f} grade={sig.grade}")

    return sent


# ═══════════════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════════════

_shutdown_state_ref: dict = {}


def _shutdown_handler(signum, frame):
    print(f"\n  [SHUTDOWN] Received signal {signum} -- saving state before exit.")
    if _shutdown_state_ref:
        save_state(_shutdown_state_ref)
    sys.exit(0)


def main() -> None:
    print("=" * 70)
    print(f"  {ENGINE_NAME} Engine v{VERSION}  [dual-pipeline, multi-pathway]")
    print(f"  Pipelines: {', '.join(p.label for p in PIPELINES.values())}")
    print(f"  Pathways: {', '.join(PATHWAYS.keys())}")
    print(f"  Target: {int(TARGET_SIGNALS_MIN)}-{int(TARGET_SIGNALS_MAX)} signals/day | "
          f"Top {TOP_N_SIGNALS_PER_SCAN}/scan | Sector cap {MAX_PER_SECTOR} | "
          f"Same-dir cap {MAX_SAME_DIRECTION}")
    print(f"  Global concurrency cap: {MAX_CONCURRENT_ACTIVE_SIGNALS} | OI floor: ${MIN_OI_USD:,.0f}")
    print("=" * 70)

    _signal.signal(_signal.SIGTERM, _shutdown_handler)
    _signal.signal(_signal.SIGINT, _shutdown_handler)

    state = load_state()
    _shutdown_state_ref.update(state)
    print(f"\n{get_win_rate_summary(state)}")

    bar_index = _bar_index_now()
    if state.get("active_signals"):
        print(f"\n[TRACKING] Checking {len(state['active_signals'])} active signal(s)...")
        try:
            check_active_signals(state, bar_index)
        except Exception as e:
            print(f"[TRACK ERROR] {e}")
    else:
        print("\n[TRACKING] No active signals to check.")

    try:
        run_scan(state)
    except Exception as e:
        print(f"[MAIN ERROR] {e}")
        send_telegram_get_id(f"⚠️ {ENGINE_NAME} Engine error: {e}")

    try:
        maybe_send_daily_summary(state)
    except Exception as e:
        print(f"[SUMMARY ERROR] {e}")

    prune_state(state)
    save_state(state)
    print("  [DONE] Scan complete. Exiting.")


if __name__ == "__main__":
    main()
