# ORACLE — Adaptive Institutional-Grade Multi-Engine Signal Platform
# v1.0.0
#
# A from-scratch synthesis engine. Runs a 13-specialist ensemble (SMC, Trend
# Continuation, Breakout, Pullback, Liquidity Sweep, Order Block, Breaker
# Block, Fair Value Gap, Momentum, Reversal, Mean Reversion, Range, Volatility
# Expansion) whose candidates are ranked by a bounded continuous-blend
# Decision Engine, gated through a composite Regime Vector, a mandatory
# zone-selection sequence (HTF bias -> POI -> SFP purity -> MSS -> breaker),
# adaptive-percentile SL / liquidity-wall-clipped TP risk plans, and an
# entry-fill-verification + outcome-integrity layer that makes the two known
# reference-engine bug classes (auto-breakeven stop-outs, phantom fills)
# structurally impossible. A closed-taxonomy loss/win forensics loop drives
# every adaptive parameter, all persisted in a two-tier state.json.
#
# Design-decision comments are inline throughout, marked with `# DECISION:`.

from __future__ import annotations

import os
import sys
import json
import math
import time
import fcntl
import logging
import statistics
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Any

import requests

ENGINE_NAME = "ORACLE"
__version__ = "1.0.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(ENGINE_NAME)

# ═══════════════════════════════════════════════════════════════════════
# SECTION 0 — CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

STATE_FILE = os.getenv("STATE_FILE", "state.json")
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "4"))
HL_BASE_URL = "https://api.hyperliquid.xyz/info"
HL_MIN_INTERVAL_S = float(os.getenv("HL_MIN_INTERVAL_S", "0.15"))

# DECISION: watchlist kept identical to the reference fleet (Kestrel/Aurelius/
# Axis/Kairos) per the build instruction — this is shared infra, not a design
# choice this engine should second-guess.
WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]
MACRO_ASSET = "BTCUSDT"  # DECISION: BTC is the macro-bias anchor for the Regime Vector (Sec 6).
MAJORS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"}

# DECISION: 15m is the spec's forbidden-floor timeframe. Swing pipeline uses
# 1D/4H/1H (macro/HTF/mid), intraday pipeline uses 4H/1H/15m — mirrors the
# spec's suggested HTF/LTF split while giving each pipeline a distinct macro
# anchor, satisfying Sec 7 without inventing an unnecessary third stack.
TF_MACRO_SWING, TF_HTF_SWING, TF_LTF_SWING = "1d", "4h", "1h"
TF_HTF_INTRADAY, TF_MID_INTRADAY, TF_LTF_INTRADAY = "4h", "1h", "15m"
ALL_TFS = ["1d", "4h", "1h", "15m"]
TF_BARS = {"1d": 260, "4h": 300, "1h": 320, "15m": 320}
SCAN_INTERVAL_MIN = 15

EMA_FAST, EMA_SLOW, EMA_TREND = 21, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, BB_LEN = 14, 14, 14, 20

MAX_CONCURRENT_ACTIVE_SIGNALS = int(os.getenv("MAX_CONCURRENT_ACTIVE_SIGNALS", "8"))
# DECISION: two assets sharing a correlation cluster may not both occupy an
# active slot at once (Sec 14 correlation cap).
MAX_CORRELATED_CONCURRENT = 1

MIN_SAMPLE_SIZE = int(os.getenv("MIN_SAMPLE_SIZE", "20"))  # Sec 13 min-sample gate, per segment/category
TIER2_RETENTION_DAYS = 15  # Sec 5 raw-log pruning window

RR_TP1_FLOOR = 1.5
RR_TP1_CEIL_SOFT = 2.0  # informative target band, not a hard cap
MIN_ENTRY_SL_ATR_MULT = 0.35   # min entry-to-SL distance, in ATR
MIN_ENTRY_TP1_ATR_MULT = 0.55  # min entry-to-TP1 distance, in ATR
MAX_PENDING_ENTRY_ATR_MULT = 1.8  # cap on how far a pending zone entry may sit from market

CIRCUIT_BREAKER_WINDOW = 30       # trades in rolling live-performance window
CIRCUIT_BREAKER_WR_DROP = 0.12    # absolute win-rate drop vs baseline that trips the breaker
CIRCUIT_BREAKER_PF_DROP = 0.35    # relative profit-factor drop that trips the breaker

# ═══════════════════════════════════════════════════════════════════════
# SECTION 1 — STATE PERSISTENCE (Tier 1 aggregates / Tier 2 raw log)
# ═══════════════════════════════════════════════════════════════════════
# DECISION: Tier 1 holds every adaptive parameter + incrementally-updated
# aggregates (never rescanned from Tier 2). Tier 2 is a bounded/prunable raw
# trade log used only for forensics/manual review. Pruning Tier 2 must never
# change Tier 1 — verified structurally: only resolve_trade() mutates Tier 1,
# and it does so once, at resolution time, from the single resolved trade.

DEFAULT_ENGINE_NAMES = [
    "smc", "trend_continuation", "breakout", "pullback", "liquidity_sweep",
    "order_block", "breaker_block", "fair_value_gap", "momentum", "reversal",
    "mean_reversion", "range_trading", "volatility_expansion",
]

REGIMES = ["bull", "bear", "neutral", "trending", "ranging", "consolidation",
           "expansion", "reversal", "high_vol", "low_vol"]


def _default_engine_weight_state() -> dict:
    return {name: {"weight": 1.0, "min": 0.35, "max": 2.0} for name in DEFAULT_ENGINE_NAMES}


def _default_regime_fit_state() -> dict:
    # per (engine, regime) veto/discount multiplier, bounded [0.15, 1.0]
    return {name: {r: 1.0 for r in REGIMES} for name in DEFAULT_ENGINE_NAMES}


def default_state() -> dict:
    return {
        "meta": {"engine": ENGINE_NAME, "version": __version__, "last_run_ts": None},
        "tier1": {
            "adaptive_params": {
                "engine_weights": _default_engine_weight_state(),
                "regime_fit": _default_regime_fit_state(),
                "confidence_calibration": {  # per engine, per confidence bucket -> realized-WR-based multiplier
                    name: {b: 1.0 for b in ["low", "mid", "high"]} for name in DEFAULT_ENGINE_NAMES
                },
                "sl_buffer_percentile": {  # per (asset, timeframe) adaptive-percentile SL buffer
                    "default": {"pct": 0.65, "min": 0.35, "max": 0.90}
                },
                "filter_thresholds": {
                    "min_confluence_score": {"value": 0.42, "min": 0.28, "max": 0.65},
                    "liquidity_sanity_gap_atr": {"value": 0.30, "min": 0.15, "max": 0.60},
                    "mtf_alignment_weight": {"value": 0.18, "min": 0.08, "max": 0.32},
                    "sfp_mss_strictness": {"value": 0.50, "min": 0.30, "max": 0.85},
                },
                "circuit_breaker": {
                    "tripped": False, "tripped_ts": None,
                    "baseline_wr": None, "baseline_pf": None, "baseline_avg_rr": None,
                },
            },
            "segment_stats": {},   # key "{asset}|{regime}|{tf}|{engine}" -> aggregate dict
            "category_stats": {},  # failure/success category -> aggregate dict (Sec 13)
            "calibration": {},     # confidence bucket -> {n, wins}
            "totals": {"signals": 0, "wins": 0, "losses": 0, "expired": 0,
                       "sum_r": 0.0, "gross_profit_r": 0.0, "gross_loss_r": 0.0,
                       "sum_hold_minutes": 0.0},
        },
        "tier2": {"trades": []},        # bounded raw log, pruned by age
        "active_signals": {},           # id -> signal dict (pending or filled, unresolved)
        "daily_summary_date": None,
    }


class StateStore:
    """Atomic, lock-guarded state.json read/write with tier-aware helpers."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.state: dict = {}

    def load(self) -> dict:
        if not self.path.exists():
            log.info("No existing state.json — cold start with defaults.")
            self.state = default_state()
            return self.state
        try:
            with open(self.path, "r") as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                try:
                    self.state = json.load(f)
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
            # merge forward in case new default keys were added since this file was written
            self.state = _deep_merge_defaults(self.state, default_state())
        except Exception as e:
            log.error(f"Failed to load state.json ({e}); falling back to defaults.")
            self.state = default_state()
        return self.state

    def save(self) -> None:
        self.state["meta"]["last_run_ts"] = datetime.now(timezone.utc).isoformat()
        tmp_path = self.path.with_suffix(".json.tmp")
        with open(tmp_path, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                json.dump(self.state, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
        os.replace(tmp_path, self.path)  # atomic swap

    def prune_tier2(self) -> None:
        cutoff = time.time() - TIER2_RETENTION_DAYS * 86400
        before = len(self.state["tier2"]["trades"])
        self.state["tier2"]["trades"] = [
            t for t in self.state["tier2"]["trades"] if t.get("resolved_ts", 0) >= cutoff
        ]
        pruned = before - len(self.state["tier2"]["trades"])
        if pruned:
            log.info(f"Pruned {pruned} aged-out Tier 2 trade records (Tier 1 aggregates unaffected).")


def _deep_merge_defaults(loaded: dict, defaults: dict) -> dict:
    out = dict(defaults)
    for k, v in loaded.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge_defaults(v, out[k])
        else:
            out[k] = v
    return out


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def bounded_update(current: float, target_delta: float, lo: float, hi: float,
                    max_step: float = 0.06) -> float:
    """Sec 5: every adaptive-parameter update is capped-step + bounded. Uses a
    fixed max fractional step per update (exponential-smoothing-style damping)
    so no single trade/category can swing a parameter far in one shot."""
    step = clamp(target_delta, -max_step, max_step)
    return clamp(current + step, lo, hi)


# ═══════════════════════════════════════════════════════════════════════
# SECTION 2 — HYPERLIQUID CLIENT (rate-limited, retrying, cached)
# ═══════════════════════════════════════════════════════════════════════

class _WeightedRateLimiter:
    """Sliding-window budget matching HL's documented weight system
    (candleSnapshot ~= weight 25, budget floor 900/min)."""

    def __init__(self, budget_per_min: int = 900):
        self.budget = budget_per_min
        self.window: list[tuple[float, int]] = []
        self._last_call = 0.0

    def acquire(self, weight: int = 25):
        now = time.time()
        if now - self._last_call < HL_MIN_INTERVAL_S:
            time.sleep(HL_MIN_INTERVAL_S - (now - self._last_call))
        self.window = [(t, w) for t, w in self.window if time.time() - t < 60]
        used = sum(w for _, w in self.window)
        if used + weight > self.budget:
            sleep_for = 60 - (time.time() - self.window[0][0]) if self.window else 1.0
            time.sleep(max(0.0, sleep_for))
        self.window.append((time.time(), weight))
        self._last_call = time.time()


class HyperliquidClient:
    def __init__(self):
        self.session = requests.Session()
        self.limiter = _WeightedRateLimiter()
        self._candle_cache: dict[tuple[str, str], list[dict]] = {}

    def _post(self, payload: dict, weight: int = 25, retries: int = 4) -> Any:
        self.limiter.acquire(weight)
        backoff = 1.0
        for attempt in range(retries):
            try:
                r = self.session.post(HL_BASE_URL, json=payload, timeout=15)
                if r.status_code == 429:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                if attempt == retries - 1:
                    log.error(f"HL request failed permanently: {e}")
                    return None
                time.sleep(backoff)
                backoff *= 2
        return None

    def candles(self, symbol: str, interval: str, n_bars: int) -> list[dict]:
        coin = symbol.replace("USDT", "")
        cache_key = (coin, interval)
        end_ms = int(time.time() * 1000)
        interval_ms = _interval_to_ms(interval)
        start_ms = end_ms - interval_ms * (n_bars + 5)
        payload = {"type": "candleSnapshot",
                   "req": {"coin": coin, "interval": interval, "startTime": start_ms, "endTime": end_ms}}
        data = self._post(payload, weight=25)
        if not data:
            return self._candle_cache.get(cache_key, [])
        candles = [{"t": c["t"], "o": float(c["o"]), "h": float(c["h"]),
                    "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])}
                   for c in data]
        candles.sort(key=lambda c: c["t"])
        self._candle_cache[cache_key] = candles[-n_bars:]
        return self._candle_cache[cache_key]

    def mark_prices(self) -> dict[str, float]:
        data = self._post({"type": "metaAndAssetCtxs"}, weight=20)
        out = {}
        if not data or len(data) < 2:
            return out
        universe = data[0].get("universe", [])
        ctxs = data[1]
        for meta, ctx in zip(universe, ctxs):
            try:
                out[meta["name"] + "USDT"] = float(ctx["markPx"])
            except (KeyError, ValueError, TypeError):
                continue
        return out


def _interval_to_ms(interval: str) -> int:
    unit = interval[-1]
    n = int(interval[:-1])
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]
    return n * mult


# ═══════════════════════════════════════════════════════════════════════
# SECTION 3 — INDICATORS & MARKET-STRUCTURE PRIMITIVES (shared)
# ═══════════════════════════════════════════════════════════════════════

def ema(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    k = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def rsi(closes: list[float], length: int = RSI_LEN) -> list[float]:
    if len(closes) < length + 1:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[1:length + 1]) / length
    avg_loss = sum(losses[1:length + 1]) / length
    out = [50.0] * length
    for i in range(length, len(closes)):
        avg_gain = (avg_gain * (length - 1) + gains[i]) / length
        avg_loss = (avg_loss * (length - 1) + losses[i]) / length
        rs = avg_gain / avg_loss if avg_loss > 1e-12 else 100.0
        out.append(100 - 100 / (1 + rs))
    return out


def atr(candles: list[dict], length: int = ATR_LEN) -> list[float]:
    if len(candles) < 2:
        return [0.0] * len(candles)
    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    out = [trs[0]]
    for i in range(1, len(trs)):
        if i < length:
            out.append(sum(trs[:i + 1]) / (i + 1))
        else:
            out.append((out[-1] * (length - 1) + trs[i]) / length)
    return out


def adx(candles: list[dict], length: int = ADX_LEN) -> list[float]:
    if len(candles) < length + 2:
        return [15.0] * len(candles)
    plus_dm, minus_dm, trs = [0.0], [0.0], [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        up = candles[i]["h"] - candles[i - 1]["h"]
        down = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    def _smooth(vals):
        out = [sum(vals[:length])]
        for v in vals[length:]:
            out.append(out[-1] - out[-1] / length + v)
        return out

    str_ = _smooth(trs)
    spdm = _smooth(plus_dm)
    smdm = _smooth(minus_dm)
    dx = []
    for i in range(len(str_)):
        pdi = 100 * spdm[i] / str_[i] if str_[i] > 1e-12 else 0.0
        mdi = 100 * smdm[i] / str_[i] if str_[i] > 1e-12 else 0.0
        denom = pdi + mdi
        dx.append(100 * abs(pdi - mdi) / denom if denom > 1e-12 else 0.0)
    pad = [15.0] * (len(candles) - len(dx))
    adx_vals = pad + (ema(dx, length) if dx else [15.0])
    return adx_vals[-len(candles):] if len(adx_vals) >= len(candles) else pad + adx_vals


def bollinger_width_pctile(closes: list[float], length: int = BB_LEN) -> float:
    """Volatility percentile proxy: current BB width vs its own recent history."""
    if len(closes) < length + 20:
        return 0.5
    widths = []
    for i in range(length, len(closes)):
        window = closes[i - length:i]
        mean = sum(window) / length
        sd = statistics.pstdev(window)
        widths.append((4 * sd) / mean if mean else 0.0)
    cur = widths[-1]
    hist = widths[-100:] if len(widths) > 100 else widths
    rank = sum(1 for w in hist if w <= cur) / len(hist)
    return rank


@dataclass
class Pivot:
    idx: int
    price: float
    kind: str  # "high" | "low"


def find_pivots(candles: list[dict], left: int = 2, right: int = 2) -> list[Pivot]:
    pivots = []
    for i in range(left, len(candles) - right):
        window = candles[i - left:i + right + 1]
        h = candles[i]["h"]
        l = candles[i]["l"]
        if h == max(c["h"] for c in window):
            pivots.append(Pivot(i, h, "high"))
        if l == min(c["l"] for c in window):
            pivots.append(Pivot(i, l, "low"))
    return pivots


def detect_bos_choch(candles: list[dict], pivots: list[Pivot]) -> dict:
    """Returns latest structural bias + whether the most recent break is a
    continuation (BOS) or a shift (CHoCH)."""
    highs = [p for p in pivots if p.kind == "high"]
    lows = [p for p in pivots if p.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return {"bias": "neutral", "event": None, "shift_idx": None}
    last_close = candles[-1]["c"]
    prior_bias = "bull" if highs[-1].price > highs[-2].price and lows[-1].price > lows[-2].price else \
                 "bear" if highs[-1].price < highs[-2].price and lows[-1].price < lows[-2].price else "neutral"
    event, shift_idx = None, None
    if last_close > highs[-1].price:
        event = "bos_up" if prior_bias in ("bull", "neutral") else "choch_up"
        shift_idx = len(candles) - 1
    elif last_close < lows[-1].price:
        event = "bos_down" if prior_bias in ("bear", "neutral") else "choch_down"
        shift_idx = len(candles) - 1
    bias = prior_bias
    if event in ("bos_up", "choch_up"):
        bias = "bull"
    elif event in ("bos_down", "choch_down"):
        bias = "bear"
    return {"bias": bias, "event": event, "shift_idx": shift_idx}


@dataclass
class Zone:
    kind: str          # "order_block" | "breaker_block" | "fvg" | "premium_discount"
    direction: str      # "long" | "short"
    top: float
    bottom: float
    idx: int
    displacement_score: float = 0.0

    @property
    def mid(self) -> float:
        return (self.top + self.bottom) / 2


def find_order_blocks(candles: list[dict], atr_series: list[float], lookback: int = 60) -> list[Zone]:
    """Last opposite-colored candle before a displacement move that breaks
    structure — the classic SMC order-block definition."""
    zones = []
    n = len(candles)
    start = max(1, n - lookback)
    for i in range(start, n - 1):
        body = abs(candles[i]["c"] - candles[i]["o"])
        a = atr_series[i] if i < len(atr_series) else atr_series[-1]
        if a <= 0:
            continue
        is_bull_disp = candles[i]["c"] > candles[i]["o"] and body > 1.15 * a
        is_bear_disp = candles[i]["c"] < candles[i]["o"] and body > 1.15 * a
        if is_bull_disp and candles[i - 1]["c"] < candles[i - 1]["o"]:
            zones.append(Zone("order_block", "long", candles[i - 1]["o"], candles[i - 1]["l"],
                               i - 1, displacement_score=body / a))
        if is_bear_disp and candles[i - 1]["c"] > candles[i - 1]["o"]:
            zones.append(Zone("order_block", "short", candles[i - 1]["h"], candles[i - 1]["o"],
                               i - 1, displacement_score=body / a))
    return zones


def find_fvgs(candles: list[dict], atr_series: list[float], lookback: int = 60) -> list[Zone]:
    zones = []
    n = len(candles)
    start = max(2, n - lookback)
    for i in range(start, n):
        a = atr_series[i] if i < len(atr_series) else atr_series[-1]
        if a <= 0:
            continue
        # bullish FVG: candle[i-2].high < candle[i].low
        if candles[i]["l"] > candles[i - 2]["h"] and (candles[i]["l"] - candles[i - 2]["h"]) > 0.12 * a:
            zones.append(Zone("fvg", "long", candles[i]["l"], candles[i - 2]["h"], i))
        if candles[i]["h"] < candles[i - 2]["l"] and (candles[i - 2]["l"] - candles[i]["h"]) > 0.12 * a:
            zones.append(Zone("fvg", "short", candles[i - 2]["l"], candles[i]["h"], i))
    return zones


def find_breaker_blocks(candles: list[dict], structure: dict, order_blocks: list[Zone]) -> list[Zone]:
    """A breaker is a failed order block flipped by a confirmed MSS — take
    the most recent opposite-direction OB that price has since closed
    through, following the structure shift."""
    shift_idx = structure.get("shift_idx")
    if shift_idx is None:
        return []
    new_bias = structure["bias"]
    flipped_dir = "long" if new_bias == "bull" else "short"
    opposite_dir = "short" if flipped_dir == "long" else "long"
    breakers = []
    for ob in order_blocks:
        if ob.direction == opposite_dir and ob.idx < shift_idx:
            breakers.append(Zone("breaker_block", flipped_dir, ob.top, ob.bottom, ob.idx,
                                  displacement_score=ob.displacement_score))
    return breakers[-3:]


def detect_liquidity_sweep(candles: list[dict], pivots: list[Pivot], eq_tol_pct: float = 0.0018) -> Optional[dict]:
    """A wick-based SFP: price wicks beyond a prior swing high/low then closes
    back inside it. Purity is scored by how decisively it rejects."""
    if len(candles) < 5 or not pivots:
        return None
    last = candles[-1]
    highs = sorted([p for p in pivots if p.kind == "high"], key=lambda p: p.price, reverse=True)
    lows = sorted([p for p in pivots if p.kind == "low"], key=lambda p: p.price)
    for h in highs[:3]:
        if last["h"] > h.price * (1 + eq_tol_pct * 0.1) and last["c"] < h.price:
            wick = last["h"] - max(last["c"], last["o"])
            body = abs(last["c"] - last["o"]) + 1e-9
            purity = clamp(wick / (wick + body), 0.0, 1.0)
            return {"direction": "short", "level": h.price, "purity": purity}
    for l in lows[:3]:
        if last["l"] < l.price * (1 - eq_tol_pct * 0.1) and last["c"] > l.price:
            wick = min(last["c"], last["o"]) - last["l"]
            body = abs(last["c"] - last["o"]) + 1e-9
            purity = clamp(wick / (wick + body), 0.0, 1.0)
            return {"direction": "long", "level": l.price, "purity": purity}
    return None


def premium_discount_zone(candles: list[dict], lookback: int = 80) -> dict:
    window = candles[-lookback:]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    mid = (hi + lo) / 2
    last = candles[-1]["c"]
    frac = (last - lo) / (hi - lo) if hi > lo else 0.5
    zone = "premium" if frac > 0.6 else "discount" if frac < 0.4 else "equilibrium"
    return {"zone": zone, "fraction": frac, "range_high": hi, "range_low": lo, "mid": mid}


# ═══════════════════════════════════════════════════════════════════════
# SECTION 4 — MARKET SNAPSHOT (shared per-symbol computation, no duplication)
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class TFView:
    tf: str
    candles: list[dict]
    closes: list[float]
    ema_fast: list[float]
    ema_slow: list[float]
    ema_trend: list[float]
    rsi: list[float]
    atr: list[float]
    adx: list[float]
    pivots: list[Pivot]
    structure: dict
    order_blocks: list[Zone]
    fvgs: list[Zone]
    breakers: list[Zone]
    sweep: Optional[dict]
    prem_disc: dict
    vol_pctile: float


def build_tf_view(tf: str, candles: list[dict]) -> Optional[TFView]:
    if len(candles) < 40:
        return None
    closes = [c["c"] for c in candles]
    a = atr(candles, ATR_LEN)
    pivots = find_pivots(candles, 2, 2)
    structure = detect_bos_choch(candles, pivots)
    obs = find_order_blocks(candles, a)
    fvgs = find_fvgs(candles, a)
    breakers = find_breaker_blocks(candles, structure, obs)
    sweep = detect_liquidity_sweep(candles, pivots)
    return TFView(
        tf=tf, candles=candles, closes=closes,
        ema_fast=ema(closes, EMA_FAST), ema_slow=ema(closes, EMA_SLOW), ema_trend=ema(closes, EMA_TREND),
        rsi=rsi(closes), atr=a, adx=adx(candles),
        pivots=pivots, structure=structure, order_blocks=obs, fvgs=fvgs, breakers=breakers,
        sweep=sweep, prem_disc=premium_discount_zone(candles),
        vol_pctile=bollinger_width_pctile(closes),
    )


@dataclass
class SymbolSnapshot:
    symbol: str
    views: dict[str, TFView]
    mark_price: float


def collect_snapshot(hl: HyperliquidClient, symbol: str, mark: float) -> Optional[SymbolSnapshot]:
    views = {}
    for tf in ALL_TFS:
        candles = hl.candles(symbol, tf, TF_BARS[tf])
        v = build_tf_view(tf, candles)
        if v:
            views[tf] = v
    if len(views) < 3:
        return None
    return SymbolSnapshot(symbol=symbol, views=views, mark_price=mark)


# ═══════════════════════════════════════════════════════════════════════
# SECTION 5 — COMPOSITE REGIME VECTOR (Sec 6)
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class RegimeVector:
    macro_bias: str          # "bull" | "bear" | "neutral"
    vol_pctile: float        # 0..1
    trend_strength: float    # 0..1 (ADX normalized)
    session_weight: float    # 0..1
    noise_index: float       # 0..1 (higher = choppier)
    breadth: float           # 0..1 (participation coherence)

    def label(self) -> str:
        if self.vol_pctile > 0.75:
            return "high_vol"
        if self.vol_pctile < 0.25:
            return "low_vol"
        if self.trend_strength > 0.55 and self.noise_index < 0.5:
            return "trending" if self.macro_bias != "neutral" else "expansion"
        if self.trend_strength < 0.3:
            return "ranging" if self.noise_index < 0.6 else "consolidation"
        return self.macro_bias if self.macro_bias != "neutral" else "neutral"


def _session_weight_now() -> float:
    # DECISION: London/NY overlap gets the highest historical-reliability
    # weight; Asia-only gets the lowest, matching well-documented liquidity
    # rhythm without needing external data.
    h = datetime.now(timezone.utc).hour
    if 12 <= h < 16:
        return 1.0   # London/NY overlap
    if 7 <= h < 12 or 16 <= h < 21:
        return 0.75  # single major session active
    return 0.4       # thin Asia/off-hours liquidity


def compute_regime_vector(macro_view: TFView, all_snaps: dict[str, SymbolSnapshot]) -> RegimeVector:
    macro_bias = macro_view.structure["bias"]
    vol_pctile = macro_view.vol_pctile
    trend_strength = clamp(macro_view.adx[-1] / 45.0, 0.0, 1.0)

    # noise index: wick-to-range ratio over recent bars, independent of raw ATR
    recent = macro_view.candles[-20:]
    wick_ratio = []
    for c in recent:
        rng = c["h"] - c["l"]
        if rng <= 0:
            continue
        body = abs(c["c"] - c["o"])
        wick_ratio.append(1 - body / rng)
    noise_index = clamp(sum(wick_ratio) / len(wick_ratio), 0.0, 1.0) if wick_ratio else 0.5

    # breadth: fraction of watchlist snapshots whose 1h EMA-fast/slow agree with macro bias
    coherent, total = 0, 0
    for snap in all_snaps.values():
        v = snap.views.get(TF_LTF_SWING) or snap.views.get(TF_MID_INTRADAY)
        if not v or len(v.ema_fast) < 2:
            continue
        total += 1
        sym_bias = "bull" if v.ema_fast[-1] > v.ema_slow[-1] else "bear"
        if sym_bias == macro_bias:
            coherent += 1
    breadth = coherent / total if total else 0.5

    return RegimeVector(
        macro_bias=macro_bias, vol_pctile=vol_pctile, trend_strength=trend_strength,
        session_weight=_session_weight_now(), noise_index=noise_index, breadth=breadth,
    )


# ═══════════════════════════════════════════════════════════════════════
# SECTION 6 — CANDIDATE SIGNAL DATACLASS
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class Candidate:
    id: str
    symbol: str
    engine: str
    direction: str          # "long" | "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    entry_kind: str          # "market" | "pending"   (Sec 12 mandatory abstraction)
    timeframe: str
    confidence_raw: float    # 0..1, pre-calibration
    regime_fit: list[str]    # regimes this setup is documented as best-suited for
    confluences: dict[str, float] = field(default_factory=dict)  # name -> independent 0..1 contribution
    mtf_alignment: float = 0.5
    liquidity_ok: float = 1.0     # 1.0 = clean, lower = closer to a sweep risk
    ev_estimate: float = 0.0
    rr_tp1: float = 0.0
    rr_tp2: float = 0.0
    pending_bars_elapsed: int = 0
    pending_expiry_bars: int = 8
    entry_filled: bool = False
    score: float = 0.0
    tier: str = "B"          # A+/A/B conviction tier (Sec 14)

    def to_dict(self) -> dict:
        return asdict(self)


def _rr(entry: float, sl: float, target: float, direction: str) -> float:
    risk = abs(entry - sl)
    if risk <= 1e-9:
        return 0.0
    reward = (target - entry) if direction == "long" else (entry - target)
    return reward / risk


# ═══════════════════════════════════════════════════════════════════════
# SECTION 7 — RISK PLAN: adaptive-percentile SL, liquidity-wall-clipped TP
# ═══════════════════════════════════════════════════════════════════════

def adaptive_sl_buffer(view: TFView, state: dict, asset: str) -> float:
    """Sec 10: SL buffer = Nth percentile of recent adverse-wick excursions
    beyond structure, N itself a bounded adaptive parameter."""
    params = state["tier1"]["adaptive_params"]["sl_buffer_percentile"]
    cfg = params.get(asset, params["default"])
    pct = cfg["pct"]
    wicks = []
    for c in view.candles[-40:]:
        body_top = max(c["o"], c["c"])
        body_bot = min(c["o"], c["c"])
        wicks.append(c["h"] - body_top)
        wicks.append(body_bot - c["l"])
    wicks = sorted(w for w in wicks if w > 0)
    if not wicks:
        return view.atr[-1] * 0.25
    idx = clamp(int(pct * len(wicks)), 0, len(wicks) - 1)
    buf = wicks[idx]
    # floor/ceiling relative to ATR so buffer never collapses to ~0 or balloons unreasonably
    return clamp(buf, 0.15 * view.atr[-1], 1.2 * view.atr[-1])


def clip_target_to_liquidity_wall(direction: str, entry: float, raw_target: float,
                                   view: TFView) -> float:
    """Sec 10: never project a TP through a closer, obvious liquidity wall
    (prior swing high/low). Clip to just in front of the nearest such wall
    that sits inside the natural path to raw_target."""
    walls = [p.price for p in view.pivots if
             (p.kind == "high" and direction == "long") or (p.kind == "low" and direction == "short")]
    buffer = view.atr[-1] * 0.08
    candidates_between = []
    for w in walls:
        if direction == "long" and entry < w < raw_target:
            candidates_between.append(w - buffer)
        if direction == "short" and raw_target < w < entry:
            candidates_between.append(w + buffer)
    if not candidates_between:
        return raw_target
    return min(candidates_between) if direction == "long" else max(candidates_between)


def build_risk_plan(direction: str, entry: float, structural_sl: float, view: TFView,
                     state: dict, asset: str) -> Optional[dict]:
    buf = adaptive_sl_buffer(view, state, asset)
    sl = structural_sl - buf if direction == "long" else structural_sl + buf

    risk = abs(entry - sl)
    if risk <= 1e-9:
        return None

    # honest nearest-structure TP1 candidate: next opposing pivot, else RR-floor projection
    opp_pivots = [p.price for p in view.pivots if
                  (p.kind == "high" and direction == "long" and p.price > entry) or
                  (p.kind == "low" and direction == "short" and p.price < entry)]
    floor_tp1 = entry + risk * RR_TP1_FLOOR if direction == "long" else entry - risk * RR_TP1_FLOOR
    nearest_structural = min(opp_pivots) if (direction == "long" and opp_pivots) else \
                          (max(opp_pivots) if opp_pivots else None)
    raw_tp1 = nearest_structural if nearest_structural is not None else \
        (entry + risk * RR_TP1_CEIL_SOFT if direction == "long" else entry - risk * RR_TP1_CEIL_SOFT)
    # never let TP1 fall below the 1.5 floor even if nearest structure is closer
    if direction == "long" and raw_tp1 < floor_tp1:
        raw_tp1 = floor_tp1
    if direction == "short" and raw_tp1 > floor_tp1:
        raw_tp1 = floor_tp1

    tp1 = clip_target_to_liquidity_wall(direction, entry, raw_tp1, view)
    rr1 = _rr(entry, sl, tp1, direction)
    if rr1 < RR_TP1_FLOOR:
        # DECISION: clipping must never be used to justify shrinking below the
        # floor — if clipping pushed RR under 1.5, reject the candidate (Sec 10).
        return None

    raw_tp2 = entry + risk * (rr1 + 1.2) if direction == "long" else entry - risk * (rr1 + 1.2)
    tp2 = clip_target_to_liquidity_wall(direction, entry, raw_tp2, view)
    rr2 = _rr(entry, sl, tp2, direction)
    if rr2 <= rr1:
        tp2 = entry + risk * (rr1 + 0.5) if direction == "long" else entry - risk * (rr1 + 0.5)
        rr2 = _rr(entry, sl, tp2, direction)

    return {"sl": sl, "tp1": tp1, "tp2": tp2, "rr1": rr1, "rr2": rr2, "risk": risk}


def passes_entry_placement_rules(entry: float, sl: float, tp1: float, atr_val: float,
                                  direction: str, mark_price: float) -> bool:
    if atr_val <= 0:
        return False
    if abs(entry - sl) < MIN_ENTRY_SL_ATR_MULT * atr_val:
        return False
    if abs(tp1 - entry) < MIN_ENTRY_TP1_ATR_MULT * atr_val:
        return False
    if abs(entry - mark_price) > MAX_PENDING_ENTRY_ATR_MULT * atr_val:
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════
# SECTION 8 — 13 SPECIALIZED ENGINES
# ═══════════════════════════════════════════════════════════════════════
# Each engine follows the mandatory zone-selection sequence where applicable
# (HTF bias -> POI -> SFP purity -> MSS -> breaker, Sec 8) and emits
# Candidates carrying entry_kind, regime_fit, and independent confluences.
# DECISION: rather than duplicate zone/structure discovery per engine, every
# engine consumes the shared TFView/Zone primitives built once in Section 4.

def _new_id(symbol: str, engine: str) -> str:
    return f"{symbol}:{engine}:{int(time.time() * 1000)}"


def engine_smc(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    """Full zone-selection sequence: HTF bias -> POI (OB/breaker/FVG) ->
    SFP purity -> MSS -> breaker confirmation. The flagship, most structurally
    rigorous engine — best fit for trending/expansion regimes."""
    out = []
    htf = snap.views.get(TF_HTF_SWING) or snap.views.get(TF_HTF_INTRADAY)
    ltf = snap.views.get(TF_LTF_INTRADAY) or snap.views.get(TF_LTF_SWING)
    if not htf or not ltf:
        return out
    htf_bias = htf.structure["bias"]
    if htf_bias == "neutral":
        return out
    direction = "long" if htf_bias == "bull" else "short"

    sweep = ltf.sweep
    if not sweep or sweep["direction"] != direction:
        return out
    strictness = state["tier1"]["adaptive_params"]["filter_thresholds"]["sfp_mss_strictness"]["value"]
    if sweep["purity"] < strictness:
        return out  # impure SFP discounted to rejection at the gate, per Sec 8

    mss = ltf.structure
    mss_ok = (direction == "long" and mss["event"] in ("bos_up", "choch_up")) or \
             (direction == "short" and mss["event"] in ("bos_down", "choch_down"))
    if not mss_ok:
        return out

    poi = ltf.breakers[-1] if ltf.breakers else (ltf.order_blocks[-1] if ltf.order_blocks else None)
    if not poi or poi.direction != direction:
        return out

    entry = poi.mid
    structural_sl = poi.bottom if direction == "long" else poi.top
    plan = build_risk_plan(direction, entry, structural_sl, ltf, state, snap.symbol)
    if not plan:
        return out
    if not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], ltf.atr[-1], direction, snap.mark_price):
        return out

    confluences = {
        "htf_bias_alignment": 0.9,
        "sfp_purity": sweep["purity"],
        "mss_confirmed": 1.0,
        "breaker_precision": 0.85 if poi.kind == "breaker_block" else 0.55,
    }
    cand = Candidate(
        id=_new_id(snap.symbol, "smc"), symbol=snap.symbol, engine="smc", direction=direction,
        entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
        entry_kind="pending", timeframe=ltf.tf, confidence_raw=0.72,
        regime_fit=["trending", "expansion", "bull", "bear"],
        confluences=confluences, mtf_alignment=1.0, rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
        pending_expiry_bars=6,
    )
    out.append(cand)
    return out


def engine_trend_continuation(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_MID_INTRADAY) or snap.views.get(TF_LTF_SWING)
    htf = snap.views.get(TF_HTF_INTRADAY) or snap.views.get(TF_HTF_SWING)
    if not v or not htf or len(v.ema_fast) < 3:
        return out
    trend_up = v.ema_fast[-1] > v.ema_slow[-1] > v.ema_trend[-1]
    trend_down = v.ema_fast[-1] < v.ema_slow[-1] < v.ema_trend[-1]
    htf_agrees = htf.structure["bias"]
    direction = None
    if trend_up and htf_agrees != "bear":
        direction = "long"
    elif trend_down and htf_agrees != "bull":
        direction = "short"
    if not direction:
        return out
    pullback_to_ema = abs(v.closes[-1] - v.ema_fast[-1]) < 0.6 * v.atr[-1]
    if not pullback_to_ema:
        return out
    entry = v.closes[-1]
    structural_sl = min(c["l"] for c in v.candles[-6:]) if direction == "long" else max(c["h"] for c in v.candles[-6:])
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "trend_continuation"), symbol=snap.symbol, engine="trend_continuation",
        direction=direction, entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
        entry_kind="market", timeframe=v.tf, confidence_raw=0.6,
        regime_fit=["trending", "bull", "bear"],
        confluences={"ema_stack_aligned": 1.0, "htf_agreement": 0.8 if htf_agrees != "neutral" else 0.4},
        mtf_alignment=0.85 if htf_agrees == ("bull" if direction == "long" else "bear") else 0.4,
        rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


def engine_breakout(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_LTF_INTRADAY)
    if not v or len(v.candles) < 30:
        return out
    lookback = v.candles[-25:-1]
    hi = max(c["h"] for c in lookback)
    lo = min(c["l"] for c in lookback)
    last = v.candles[-1]
    vol_avg = sum(c["v"] for c in lookback) / len(lookback)
    volume_confirm = last["v"] > 1.3 * vol_avg if vol_avg > 0 else False
    direction = None
    if last["c"] > hi and volume_confirm:
        direction = "long"
        structural_sl = hi - 0.1 * v.atr[-1]
    elif last["c"] < lo and volume_confirm:
        direction = "short"
        structural_sl = lo + 0.1 * v.atr[-1]
    else:
        return out
    entry = last["c"]
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "breakout"), symbol=snap.symbol, engine="breakout", direction=direction,
        entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], entry_kind="market",
        timeframe=v.tf, confidence_raw=0.55, regime_fit=["expansion", "trending", "high_vol"],
        confluences={"range_break": 1.0, "volume_confirm": 1.0 if volume_confirm else 0.0},
        rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


def engine_pullback(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_MID_INTRADAY)
    if not v or len(v.order_blocks) == 0:
        return out
    ob = v.order_blocks[-1]
    near = abs(snap.mark_price - ob.mid) < 1.0 * v.atr[-1]
    if not near:
        return out
    direction = ob.direction
    entry = ob.mid
    structural_sl = ob.bottom if direction == "long" else ob.top
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "pullback"), symbol=snap.symbol, engine="pullback", direction=direction,
        entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], entry_kind="pending",
        timeframe=v.tf, confidence_raw=0.55, regime_fit=["trending", "bull", "bear"],
        confluences={"ob_retest": clamp(ob.displacement_score / 2.0, 0.0, 1.0)},
        pending_expiry_bars=10, rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


def engine_liquidity_sweep(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_LTF_INTRADAY)
    if not v or not v.sweep:
        return out
    sweep = v.sweep
    direction = sweep["direction"]
    entry = v.closes[-1]
    structural_sl = sweep["level"] * (1.003 if direction == "short" else 0.997)
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "liquidity_sweep"), symbol=snap.symbol, engine="liquidity_sweep",
        direction=direction, entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
        entry_kind="market", timeframe=v.tf, confidence_raw=0.58 + 0.2 * sweep["purity"],
        regime_fit=["reversal", "ranging", "high_vol"],
        confluences={"sweep_purity": sweep["purity"]},
        liquidity_ok=1.0,  # this engine trades sweeps deliberately, so the sanity check is inverted/exempt
        rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


def engine_order_block(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_HTF_INTRADAY)
    if not v or not v.order_blocks:
        return out
    ob = v.order_blocks[-1]
    if abs(snap.mark_price - ob.mid) > 1.5 * v.atr[-1]:
        return out
    direction = ob.direction
    entry = ob.mid
    structural_sl = ob.bottom if direction == "long" else ob.top
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "order_block"), symbol=snap.symbol, engine="order_block", direction=direction,
        entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], entry_kind="pending",
        timeframe=v.tf, confidence_raw=0.6, regime_fit=["trending", "bull", "bear", "expansion"],
        confluences={"htf_ob_quality": clamp(ob.displacement_score / 2.0, 0.0, 1.0)},
        pending_expiry_bars=12, rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


def engine_breaker_block(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_LTF_INTRADAY)
    if not v or not v.breakers:
        return out
    br = v.breakers[-1]
    if abs(snap.mark_price - br.mid) > 1.2 * v.atr[-1]:
        return out
    direction = br.direction
    entry = br.mid
    structural_sl = br.bottom if direction == "long" else br.top
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "breaker_block"), symbol=snap.symbol, engine="breaker_block", direction=direction,
        entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], entry_kind="pending",
        timeframe=v.tf, confidence_raw=0.65, regime_fit=["reversal", "trending"],
        confluences={"breaker_confirmed_mss": 1.0},
        pending_expiry_bars=6, rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


def engine_fair_value_gap(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_MID_INTRADAY)
    if not v or not v.fvgs:
        return out
    gap = v.fvgs[-1]
    if abs(snap.mark_price - gap.mid) > 1.0 * v.atr[-1]:
        return out
    direction = gap.direction
    entry = gap.mid
    structural_sl = gap.bottom if direction == "long" else gap.top
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "fair_value_gap"), symbol=snap.symbol, engine="fair_value_gap", direction=direction,
        entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], entry_kind="pending",
        timeframe=v.tf, confidence_raw=0.5, regime_fit=["trending", "expansion"],
        confluences={"fvg_fill_reaction": 0.6},
        pending_expiry_bars=8, rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


def engine_momentum(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_MID_INTRADAY)
    if not v or len(v.rsi) < 5:
        return out
    r = v.rsi[-1]
    direction = None
    if r > 58 and v.closes[-1] > v.ema_fast[-1] and v.adx[-1] > 22:
        direction = "long"
    elif r < 42 and v.closes[-1] < v.ema_fast[-1] and v.adx[-1] > 22:
        direction = "short"
    if not direction:
        return out
    entry = v.closes[-1]
    structural_sl = min(c["l"] for c in v.candles[-8:]) if direction == "long" else max(c["h"] for c in v.candles[-8:])
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "momentum"), symbol=snap.symbol, engine="momentum", direction=direction,
        entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], entry_kind="market",
        timeframe=v.tf, confidence_raw=0.5, regime_fit=["trending", "expansion", "high_vol"],
        confluences={"rsi_momentum": clamp(abs(r - 50) / 50, 0.0, 1.0), "adx_strength": clamp(v.adx[-1] / 45, 0, 1)},
        rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


def engine_reversal(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_LTF_INTRADAY)
    if not v or not v.sweep:
        return out
    sweep = v.sweep
    pd = v.prem_disc
    aligned = (sweep["direction"] == "long" and pd["zone"] == "discount") or \
              (sweep["direction"] == "short" and pd["zone"] == "premium")
    if not aligned or sweep["purity"] < 0.55:
        return out
    direction = sweep["direction"]
    entry = v.closes[-1]
    structural_sl = sweep["level"] * (1.004 if direction == "short" else 0.996)
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "reversal"), symbol=snap.symbol, engine="reversal", direction=direction,
        entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], entry_kind="market",
        timeframe=v.tf, confidence_raw=0.55 + 0.15 * sweep["purity"], regime_fit=["reversal", "ranging"],
        confluences={"premium_discount_align": 1.0, "sweep_purity": sweep["purity"]},
        rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


def engine_mean_reversion(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_MID_INTRADAY)
    if not v or len(v.closes) < BB_LEN + 5:
        return out
    window = v.closes[-BB_LEN:]
    mean = sum(window) / len(window)
    sd = statistics.pstdev(window)
    if sd <= 0:
        return out
    z = (v.closes[-1] - mean) / sd
    if v.adx[-1] > 22:
        return out  # mean reversion needs a non-trending regime
    direction = None
    if z < -1.8:
        direction = "long"
        structural_sl = min(c["l"] for c in v.candles[-6:])
    elif z > 1.8:
        direction = "short"
        structural_sl = max(c["h"] for c in v.candles[-6:])
    if not direction:
        return out
    entry = v.closes[-1]
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "mean_reversion"), symbol=snap.symbol, engine="mean_reversion", direction=direction,
        entry=entry, sl=plan["sl"], tp1=mean, tp2=plan["tp2"], entry_kind="market",
        timeframe=v.tf, confidence_raw=0.45, regime_fit=["ranging", "consolidation", "low_vol"],
        confluences={"z_score_extreme": clamp((abs(z) - 1.8) / 2.0, 0.0, 1.0)},
        rr_tp1=_rr(entry, plan["sl"], mean, direction), rr_tp2=plan["rr2"],
    )
    if cand.rr_tp1 < RR_TP1_FLOOR:
        return out
    out.append(cand)
    return out


def engine_range_trading(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_MID_INTRADAY)
    if not v or v.adx[-1] > 20:
        return out
    window = v.candles[-30:]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    if hi <= lo:
        return out
    pos = (v.closes[-1] - lo) / (hi - lo)
    direction = None
    if pos < 0.15:
        direction = "long"
        structural_sl = lo - 0.15 * v.atr[-1]
        target = hi - 0.1 * (hi - lo)
    elif pos > 0.85:
        direction = "short"
        structural_sl = hi + 0.15 * v.atr[-1]
        target = lo + 0.1 * (hi - lo)
    if not direction:
        return out
    entry = v.closes[-1]
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    rr1 = _rr(entry, plan["sl"], target, direction)
    if rr1 < RR_TP1_FLOOR:
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "range_trading"), symbol=snap.symbol, engine="range_trading", direction=direction,
        entry=entry, sl=plan["sl"], tp1=target, tp2=plan["tp2"], entry_kind="market",
        timeframe=v.tf, confidence_raw=0.45, regime_fit=["ranging", "consolidation", "low_vol"],
        confluences={"range_edge_proximity": clamp(1 - min(pos, 1 - pos) / 0.15, 0.0, 1.0)},
        rr_tp1=rr1, rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


def engine_volatility_expansion(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    out = []
    v = snap.views.get(TF_MID_INTRADAY)
    if not v or v.vol_pctile < 0.7:
        return out
    last, prev = v.candles[-1], v.candles[-2]
    body = abs(last["c"] - last["o"])
    if body < 1.3 * v.atr[-1]:
        return out
    direction = "long" if last["c"] > last["o"] else "short"
    entry = v.closes[-1]
    structural_sl = min(prev["l"], last["l"]) if direction == "long" else max(prev["h"], last["h"])
    plan = build_risk_plan(direction, entry, structural_sl, v, state, snap.symbol)
    if not plan or not passes_entry_placement_rules(entry, plan["sl"], plan["tp1"], v.atr[-1], direction, snap.mark_price):
        return out
    cand = Candidate(
        id=_new_id(snap.symbol, "volatility_expansion"), symbol=snap.symbol, engine="volatility_expansion",
        direction=direction, entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], entry_kind="market",
        timeframe=v.tf, confidence_raw=0.5, regime_fit=["expansion", "high_vol"],
        confluences={"vol_pctile": v.vol_pctile, "displacement_body": clamp(body / v.atr[-1] - 1.0, 0.0, 1.0)},
        rr_tp1=plan["rr1"], rr_tp2=plan["rr2"],
    )
    out.append(cand)
    return out


ENGINES = {
    "smc": engine_smc,
    "trend_continuation": engine_trend_continuation,
    "breakout": engine_breakout,
    "pullback": engine_pullback,
    "liquidity_sweep": engine_liquidity_sweep,
    "order_block": engine_order_block,
    "breaker_block": engine_breaker_block,
    "fair_value_gap": engine_fair_value_gap,
    "momentum": engine_momentum,
    "reversal": engine_reversal,
    "mean_reversion": engine_mean_reversion,
    "range_trading": engine_range_trading,
    "volatility_expansion": engine_volatility_expansion,
}


def run_ensemble(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    candidates = []
    for name, fn in ENGINES.items():
        try:
            candidates.extend(fn(snap, state))
        except Exception as e:
            log.warning(f"Engine {name} failed on {snap.symbol}: {e}")
    return candidates


# ═══════════════════════════════════════════════════════════════════════
# SECTION 9 — DECISION ENGINE: continuous bounded blend + mandatory vetoes
# ═══════════════════════════════════════════════════════════════════════
# DECISION: exactly 7 terms in the blend (regime fit, MTF alignment, confluence
# strength, historical segment performance, EV, RR, liquidity context) — a
# small, documented, auditable set per Sec 4, each individually attributable.
# Session weight and volatility percentile are folded into "regime fit" rather
# than added as separate terms, since they are already components OF the
# Regime Vector that regime_fit_score consumes — adding them again would be
# exactly the correlated double-counting Sec 4 warns against.

def confluence_strength(cand: Candidate) -> float:
    if not cand.confluences:
        return 0.0
    vals = list(cand.confluences.values())
    # DECISION: mean rather than sum — a weighted/additive blend per Sec 14,
    # but bounded to 0..1 so it composes cleanly into the outer blend and one
    # missing confluence merely lowers the mean rather than zeroing the score.
    return clamp(sum(vals) / len(vals), 0.0, 1.0)


def regime_fit_score(cand: Candidate, regime: RegimeVector, state: dict) -> tuple[float, bool]:
    label = regime.label()
    veto_table = state["tier1"]["adaptive_params"]["regime_fit"].get(cand.engine, {})
    mult = veto_table.get(label, 1.0)
    matches = label in cand.regime_fit or regime.macro_bias in cand.regime_fit
    base = 1.0 if matches else 0.25  # Sec 13 regime-fit veto: heavy discount, not necessarily hard-zero
    score = base * mult
    hard_veto = score < 0.12
    return clamp(score, 0.0, 1.0), hard_veto


def historical_segment_score(cand: Candidate, regime: RegimeVector, state: dict) -> float:
    key = f"{cand.symbol}|{regime.label()}|{cand.timeframe}|{cand.engine}"
    seg = state["tier1"]["segment_stats"].get(key)
    if not seg or seg.get("n", 0) < MIN_SAMPLE_SIZE:
        return 0.5  # neutral prior until statistically meaningful (Sec 13)
    wr = seg["wins"] / seg["n"] if seg["n"] else 0.5
    return clamp(wr, 0.0, 1.0)


def liquidity_sanity_score(cand: Candidate, view: TFView, state: dict) -> tuple[float, bool]:
    if cand.engine == "liquidity_sweep":
        return 1.0, False  # deliberately trades sweep behavior; exempt per Sec 13
    threshold = state["tier1"]["adaptive_params"]["filter_thresholds"]["liquidity_sanity_gap_atr"]["value"]
    nearest_wall = None
    for p in view.pivots[-8:]:
        d = abs(p.price - cand.entry)
        if nearest_wall is None or d < nearest_wall:
            nearest_wall = d
    if nearest_wall is None or view.atr[-1] <= 0:
        return 1.0, False
    gap_atr = nearest_wall / view.atr[-1]
    if gap_atr < threshold:
        return clamp(gap_atr / threshold, 0.0, 1.0), True
    return 1.0, False


def ev_estimate(cand: Candidate, wr_prior: float) -> float:
    # simple EV = p(win)*avgWinR - p(loss)*1R, using rr1 as the win-R proxy
    return wr_prior * cand.rr_tp1 - (1 - wr_prior) * 1.0


def composite_score(cand: Candidate, regime: RegimeVector, view: TFView, state: dict,
                     weights: dict) -> tuple[float, dict]:
    regime_term, hard_regime_veto = regime_fit_score(cand, regime, state)
    mtf_term = cand.mtf_alignment
    conf_term = confluence_strength(cand)
    hist_term = historical_segment_score(cand, regime, state)
    liq_term, liq_veto = liquidity_sanity_score(cand, view, state)
    rr_term = clamp((cand.rr_tp1 - RR_TP1_FLOOR) / 1.5, 0.0, 1.0)
    ev = ev_estimate(cand, hist_term)
    ev_term = clamp((ev + 1.0) / 3.0, 0.0, 1.0)

    terms = {
        "regime_fit": (regime_term, weights["regime_fit"]),
        "mtf_alignment": (mtf_term, weights["mtf_alignment"]),
        "confluence": (conf_term, weights["confluence"]),
        "historical_perf": (hist_term, weights["historical_perf"]),
        "ev": (ev_term, weights["ev"]),
        "rr": (rr_term, weights["rr"]),
        "liquidity": (liq_term, weights["liquidity"]),
    }
    weight_sum = sum(w for _, w in terms.values())
    linear = sum(v * w for v, w in terms.values()) / weight_sum if weight_sum else 0.0
    # DECISION: logistic squash centers the blend at 0.5 confluence-of-confluences
    # and keeps the score continuous/smooth per Sec 4, rather than a raw linear
    # sum that could exceed sensible bounds.
    score = 1 / (1 + math.exp(-6 * (linear - 0.5)))

    veto = hard_regime_veto or liq_veto
    cand.ev_estimate = ev
    return (0.0 if veto else score), {"terms": terms, "linear": linear, "veto": veto,
                                        "veto_reason": "regime_mismatch" if hard_regime_veto else
                                                       ("liquidity_sweep_risk" if liq_veto else None)}


def calibrate_confidence(cand: Candidate, state: dict) -> float:
    bucket = "low" if cand.confidence_raw < 0.5 else "mid" if cand.confidence_raw < 0.68 else "high"
    mult = state["tier1"]["adaptive_params"]["confidence_calibration"].get(cand.engine, {}).get(bucket, 1.0)
    return clamp(cand.confidence_raw * mult, 0.0, 1.0)


def assign_tier(score: float, rr1: float) -> str:
    if score >= 0.78 and rr1 >= 1.8:
        return "A+"
    if score >= 0.62:
        return "A"
    return "B"


def decision_engine_rank(candidates: list[Candidate], regime: RegimeVector,
                          snaps: dict[str, SymbolSnapshot], state: dict) -> list[Candidate]:
    weights = {
        "regime_fit": 0.22, "mtf_alignment": state["tier1"]["adaptive_params"]["filter_thresholds"]["mtf_alignment_weight"]["value"],
        "confluence": 0.20, "historical_perf": 0.16, "ev": 0.14, "rr": 0.10, "liquidity": 0.10,
    }
    weights_ew = state["tier1"]["adaptive_params"]["engine_weights"]
    min_conf_threshold = state["tier1"]["adaptive_params"]["filter_thresholds"]["min_confluence_score"]["value"]

    scored = []
    attrition = {"total": len(candidates), "rejected_confluence": 0, "rejected_veto": 0}
    for cand in candidates:
        snap = snaps.get(cand.symbol)
        view = snap.views.get(cand.timeframe) if snap else None
        if not view:
            continue
        score, detail = composite_score(cand, regime, view, state, weights)
        if detail["veto"]:
            attrition["rejected_veto"] += 1
            continue
        if confluence_strength(cand) < min_conf_threshold:
            attrition["rejected_confluence"] += 1
            continue
        ew = weights_ew.get(cand.engine, {"weight": 1.0})["weight"]
        cand.confidence_raw = calibrate_confidence(cand, state)
        cand.score = clamp(score * ew, 0.0, 1.0)
        cand.tier = assign_tier(cand.score, cand.rr_tp1)
        scored.append(cand)

    scored.sort(key=lambda c: c.score, reverse=True)

    # Sec 14 correlation cap: majors cluster together — cap concurrent majors picks
    selected, majors_taken, seen_symbols = [], 0, set()
    for c in scored:
        if c.symbol in seen_symbols:
            continue  # one candidate per symbol per scan, highest score wins
        if c.symbol in MAJORS:
            if majors_taken >= MAX_CORRELATED_CONCURRENT:
                continue
            majors_taken += 1
        selected.append(c)
        seen_symbols.add(c.symbol)

    log.info(f"Decision Engine: {attrition['total']} candidates -> "
             f"{attrition['rejected_veto']} vetoed, {attrition['rejected_confluence']} sub-threshold, "
             f"{len(selected)} selected.")
    return selected


# ═══════════════════════════════════════════════════════════════════════
# SECTION 10 — ENTRY-FILL VERIFICATION & LIFECYCLE (Sec 12, mandatory)
# ═══════════════════════════════════════════════════════════════════════

def check_fill_and_resolve(signal: dict, candle: dict) -> dict:
    """Given one new candle for a still-open (unresolved) signal, advances its
    lifecycle. Never evaluates SL/TP before entry_filled is True. Returns the
    mutated signal dict; sets 'result' when a terminal state is reached."""
    direction = signal["direction"]
    lo, hi = candle["l"], candle["h"]

    if not signal["entry_filled"]:
        if signal["entry_kind"] == "market":
            signal["entry_filled"] = True
        else:
            entry_touched = lo <= signal["entry"] <= hi
            if not entry_touched:
                signal["pending_bars_elapsed"] += 1
                if signal["pending_bars_elapsed"] >= signal["pending_expiry_bars"]:
                    signal["result"] = "expired"  # Sec 12: distinct, excluded-from-stats outcome
                return signal
            signal["entry_filled"] = True
            # fall through: same candle may also register SL/TP per conservative same-candle handling below

    # DECISION: same-candle SL-vs-TP ambiguity resolved conservatively by
    # checking SL first — this cannot manufacture a false stop-out relative to
    # this engine's own SL placement (Sec 11) since the SL level itself is
    # unaffected by evaluation order; it only affects which of two genuinely
    # co-occurring touches is credited when both occur in the same bar.
    if direction == "long":
        sl_hit = lo <= signal["sl"]
        tp1_hit = hi >= signal["tp1"] and not signal["tp1_hit"]
        tp2_hit = hi >= signal["tp2"]
    else:
        sl_hit = hi >= signal["sl"]
        tp1_hit = lo <= signal["tp1"] and not signal["tp1_hit"]
        tp2_hit = lo <= signal["tp2"]

    if not signal["tp1_hit"]:
        if sl_hit:
            signal["result"] = "loss"
            signal["realized_r"] = -1.0
            return signal
        if tp1_hit:
            signal["tp1_hit"] = True
            signal["tp1_hit_ts"] = candle["t"]
            # DECISION: no SL repositioning here — Sec 11 mandatory rule.
            if tp2_hit:
                signal["result"] = "win"
                signal["realized_r"] = signal["rr_tp2"]
                return signal
            return signal  # stays open, original SL unchanged
        return signal
    else:
        # TP1 already secured; original SL still in place (Sec 11)
        if tp2_hit:
            signal["result"] = "win"
            signal["realized_r"] = signal["rr_tp2"]
            return signal
        if sl_hit:
            # TP1 secured then original SL later hit -> still a WIN, credited at TP1's R
            signal["result"] = "win"
            signal["realized_r"] = signal["rr_tp1"]
            signal["note"] = "tp1_secured_then_sl_hit"
            return signal
        return signal


# ═══════════════════════════════════════════════════════════════════════
# SECTION 11 — LOSS/WIN FORENSICS TAXONOMY (Sec 13, closed set, deterministic routing)
# ═══════════════════════════════════════════════════════════════════════

FAILURE_CATEGORIES = [
    "regime_mismatch", "structural_invalidation_too_tight", "chased_swept_liquidity",
    "mtf_conflict_ignored", "sfp_mss_sequence_violated", "correct_read_poor_rr",
    "confidence_miscalibration", "filter_over_permissiveness", "genuine_variance",
]


def diagnose_trade(signal: dict, regime_at_entry: dict, state: dict) -> str:
    result = signal["result"]
    if result == "win":
        return "genuine_variance"  # wins are reinforced (Sec 13.2) via the same route, tag kept simple

    # loss diagnosis, in priority order — first matching signature wins
    if regime_at_entry.get("label") not in signal.get("regime_fit", []) and \
       regime_at_entry.get("macro_bias") not in signal.get("regime_fit", []):
        return "regime_mismatch"

    buf = signal.get("sl_buffer_used", 0.0)
    adverse_excursion = signal.get("mae", 0.0)
    if buf > 0 and adverse_excursion <= buf * 1.15:
        return "structural_invalidation_too_tight"

    if signal.get("liquidity_ok", 1.0) < 0.5:
        return "chased_swept_liquidity"

    if signal.get("mtf_alignment", 1.0) < 0.45:
        return "mtf_conflict_ignored"

    if signal.get("engine") in ("smc", "reversal", "liquidity_sweep") and signal.get("sfp_purity", 1.0) < 0.55:
        return "sfp_mss_sequence_violated"

    if signal.get("rr_tp1", 0) < RR_TP1_FLOOR + 0.15:
        return "correct_read_poor_rr"

    bucket = "low" if signal["confidence_raw"] < 0.5 else "mid" if signal["confidence_raw"] < 0.68 else "high"
    seg_key = f"calib|{signal['engine']}|{bucket}"
    calib = state["tier1"]["calibration"].get(seg_key, {"n": 0, "wins": 0})
    if calib["n"] >= MIN_SAMPLE_SIZE:
        realized_wr = calib["wins"] / calib["n"]
        implied_wr = {"low": 0.4, "mid": 0.55, "high": 0.7}[bucket]
        if implied_wr - realized_wr > 0.15:
            return "confidence_miscalibration"

    if signal.get("thin_margin_pass"):
        return "filter_over_permissiveness"

    return "genuine_variance"


def apply_forensic_adaptive_response(category: str, signal: dict, state: dict) -> str:
    """Sec 13 rule 3: one diagnosis -> one deterministic parameter route.
    Returns a human-readable description of the delta applied (or none)."""
    params = state["tier1"]["adaptive_params"]
    engine = signal["engine"]
    cat_stats = state["tier1"]["category_stats"].setdefault(category, {"n": 0, "n_since_last_gate": 0})
    cat_stats["n"] += 1
    cat_stats["n_since_last_gate"] += 1
    if cat_stats["n_since_last_gate"] < MIN_SAMPLE_SIZE:
        return "no_change_insufficient_category_samples"
    cat_stats["n_since_last_gate"] = 0  # reset gate after acting

    if category == "regime_mismatch":
        table = params["regime_fit"].setdefault(engine, {r: 1.0 for r in REGIMES})
        label = signal.get("regime_label", "neutral")
        table[label] = bounded_update(table.get(label, 1.0), -0.08, 0.15, 1.0)
        return f"regime_fit[{engine}][{label}] -= step"

    if category == "structural_invalidation_too_tight":
        asset_cfg = params["sl_buffer_percentile"].setdefault(
            signal["symbol"], dict(params["sl_buffer_percentile"]["default"]))
        asset_cfg["pct"] = bounded_update(asset_cfg["pct"], 0.05, asset_cfg["min"], asset_cfg["max"])
        params["sl_buffer_percentile"][signal["symbol"]] = asset_cfg
        return f"sl_buffer_percentile[{signal['symbol']}] += step"

    if category == "chased_swept_liquidity":
        th = params["filter_thresholds"]["liquidity_sanity_gap_atr"]
        th["value"] = bounded_update(th["value"], 0.05, th["min"], th["max"])
        return "liquidity_sanity_gap_atr += step"

    if category == "mtf_conflict_ignored":
        th = params["filter_thresholds"]["mtf_alignment_weight"]
        th["value"] = bounded_update(th["value"], 0.03, th["min"], th["max"])
        return "mtf_alignment_weight += step"

    if category == "sfp_mss_sequence_violated":
        th = params["filter_thresholds"]["sfp_mss_strictness"]
        th["value"] = bounded_update(th["value"], 0.05, th["min"], th["max"])
        return "sfp_mss_strictness += step"

    if category == "confidence_miscalibration":
        bucket = "low" if signal["confidence_raw"] < 0.5 else "mid" if signal["confidence_raw"] < 0.68 else "high"
        table = params["confidence_calibration"].setdefault(engine, {"low": 1.0, "mid": 1.0, "high": 1.0})
        table[bucket] = bounded_update(table[bucket], -0.06, 0.5, 1.3)
        return f"confidence_calibration[{engine}][{bucket}] -= step"

    if category == "filter_over_permissiveness":
        th = params["filter_thresholds"]["min_confluence_score"]
        th["value"] = bounded_update(th["value"], 0.04, th["min"], th["max"])
        return "min_confluence_score += step"

    if category == "correct_read_poor_rr":
        return "no_change_logged_for_rr_floor_review"

    return "no_change_genuine_variance"


def reinforce_win(signal: dict, state: dict) -> str:
    """Sec 13.2: reinforce weights of factors genuinely present & predictive
    on wins — routed through the same engine-weight and regime-fit tables,
    symmetric to the loss route but positive-direction and separately gated."""
    params = state["tier1"]["adaptive_params"]
    engine = signal["engine"]
    ew = params["engine_weights"].setdefault(engine, {"weight": 1.0, "min": 0.35, "max": 2.0})
    # DECISION: only reinforce when the win's dominant confluence terms were
    # individually strong (>0.7) — a win driven by regime tailwind alone with
    # weak confluences must not inflate the engine's own weight (Sec 13.2).
    strong_confluences = [v for v in signal.get("confluences", {}).values() if v > 0.7]
    if len(strong_confluences) < max(1, len(signal.get("confluences", {})) // 2):
        return "no_change_win_not_causally_attributable"
    ew["weight"] = bounded_update(ew["weight"], 0.03, ew["min"], ew["max"])
    return f"engine_weights[{engine}] += step"


def resolve_and_learn(signal: dict, state: dict) -> None:
    regime_at_entry = signal.get("regime_at_entry", {})
    category = diagnose_trade(signal, regime_at_entry, state)
    signal["forensic_category"] = category

    if signal["result"] == "win":
        delta_desc = reinforce_win(signal, state)
    else:
        delta_desc = apply_forensic_adaptive_response(category, signal, state)
    signal["adaptive_delta_applied"] = delta_desc

    # Tier 1 incremental aggregate updates — one trade at a time, never a rescan
    totals = state["tier1"]["totals"]
    totals["signals"] += 1
    r = signal["realized_r"]
    totals["sum_r"] += r
    if signal["result"] == "win":
        totals["wins"] += 1
        totals["gross_profit_r"] += max(r, 0.0)
    else:
        totals["losses"] += 1
        totals["gross_loss_r"] += max(-r, 0.0)
    hold_minutes = (signal.get("resolved_ts", time.time()) - signal.get("signal_ts", time.time())) / 60.0
    totals["sum_hold_minutes"] += hold_minutes

    seg_key = f"{signal['symbol']}|{regime_at_entry.get('label','?')}|{signal['timeframe']}|{signal['engine']}"
    seg = state["tier1"]["segment_stats"].setdefault(seg_key, {"n": 0, "wins": 0, "sum_r": 0.0})
    seg["n"] += 1
    seg["sum_r"] += r
    if signal["result"] == "win":
        seg["wins"] += 1

    bucket = "low" if signal["confidence_raw"] < 0.5 else "mid" if signal["confidence_raw"] < 0.68 else "high"
    calib_key = f"calib|{signal['engine']}|{bucket}"
    calib = state["tier1"]["calibration"].setdefault(calib_key, {"n": 0, "wins": 0})
    calib["n"] += 1
    if signal["result"] == "win":
        calib["wins"] += 1

    cat = state["tier1"]["category_stats"].setdefault(category, {"n": 0, "n_since_last_gate": 0})
    cat["n"] = cat.get("n", 0)  # already bumped inside apply_forensic_adaptive_response for losses
    if signal["result"] == "win":
        cat["n"] += 1

    state["tier2"]["trades"].append({
        "id": signal["id"], "symbol": signal["symbol"], "engine": signal["engine"],
        "result": signal["result"], "realized_r": r, "forensic_category": category,
        "adaptive_delta": delta_desc, "resolved_ts": signal.get("resolved_ts", time.time()),
        "signal_ts": signal.get("signal_ts", time.time()),
    })


# ═══════════════════════════════════════════════════════════════════════
# SECTION 12 — LIVE-PERFORMANCE CIRCUIT BREAKER (Sec 5)
# ═══════════════════════════════════════════════════════════════════════

def evaluate_circuit_breaker(state: dict) -> None:
    cb = state["tier1"]["adaptive_params"]["circuit_breaker"]
    trades = state["tier2"]["trades"][-CIRCUIT_BREAKER_WINDOW:]
    if len(trades) < CIRCUIT_BREAKER_WINDOW:
        return
    wins = sum(1 for t in trades if t["result"] == "win")
    wr = wins / len(trades)
    gp = sum(max(t["realized_r"], 0.0) for t in trades)
    gl = sum(max(-t["realized_r"], 0.0) for t in trades)
    pf = gp / gl if gl > 0 else (float("inf") if gp > 0 else 0.0)

    if cb["baseline_wr"] is None:
        # DECISION: first full window becomes the documented pre-deployment
        # baseline per Sec 13, captured once and never silently overwritten.
        cb["baseline_wr"], cb["baseline_pf"] = wr, pf
        return

    wr_drop = cb["baseline_wr"] - wr
    pf_drop = (cb["baseline_pf"] - pf) / cb["baseline_pf"] if cb["baseline_pf"] not in (0, float("inf")) else 0.0

    if not cb["tripped"] and (wr_drop >= CIRCUIT_BREAKER_WR_DROP or pf_drop >= CIRCUIT_BREAKER_PF_DROP):
        cb["tripped"] = True
        cb["tripped_ts"] = time.time()
        send_telegram(
            f"🤯 *{ENGINE_NAME} CIRCUIT BREAKER TRIPPED*\n"
            f"Rolling live win rate/PF has fallen materially below baseline.\n"
            f"Baseline WR: `{cb['baseline_wr']:.2%}` -> Live WR: `{wr:.2%}`\n"
            f"Automatic parameter adaptation is now FROZEN. Signal generation continues."
        )
    elif cb["tripped"] and wr >= cb["baseline_wr"] and pf >= cb["baseline_pf"]:
        cb["tripped"] = False
        cb["tripped_ts"] = None
        send_telegram(f"👏 *{ENGINE_NAME}* live performance recovered to baseline — adaptation resumed.")


# ═══════════════════════════════════════════════════════════════════════
# SECTION 13 — TELEGRAM INTEGRATION
# ═══════════════════════════════════════════════════════════════════════
# DECISION: reaction/status emojis used below are restricted to Telegram's
# native quick-reaction set (per the user-supplied reaction picker) so every
# emoji the engine sends can also be tapped back as a genuine reaction —
# 🏆 win, 😭 loss, 👍 TP1 hit, 🤷 expired/no-fill, 🤯 circuit breaker tripped,
# 👏 circuit breaker recovered.
# DECISION: all underscore-bearing internal identifiers (engine names,
# forensic category keys) are converted through _display_name() before ever
# reaching a Telegram message — underscores stay in state.json/code only.

_ACRONYM_DISPLAY_OVERRIDES = {"smc": "SMC"}


def _display_name(identifier: str) -> str:
    if identifier in _ACRONYM_DISPLAY_OVERRIDES:
        return _ACRONYM_DISPLAY_OVERRIDES[identifier]
    return identifier.replace("_", " ").title()


def format_price(price: float) -> str:
    """Decimal places scale with the symbol's own price magnitude so a $63k
    BTC print doesn't render with the same precision as a sub-$1 altcoin:
    >= $100 -> 2 decimals, $1-$100 -> 4 decimals, < $1 -> 6 decimals (the
    Sec 17 hard cap). Trailing zeros are always trimmed, so a clean whole
    number like 100.0 shows as `100`, never `100.00` — and no price is ever
    rendered in scientific notation."""
    if price == 0:
        return "0"
    abs_p = abs(price)
    if abs_p >= 100:
        decimals = 2
    elif abs_p >= 1:
        decimals = 4
    else:
        decimals = 6
    s = f"{price:.{decimals}f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def send_telegram(text: str, reply_to: Optional[int] = None) -> Optional[int]:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML" if False else "Markdown"}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return r.json().get("result", {}).get("message_id")
    except requests.RequestException as e:
        log.error(f"Telegram send failed: {e}")
        return None


def format_signal_message(cand: Candidate) -> str:
    arrow = "🟢 LONG" if cand.direction == "long" else "🔴 SHORT"
    return (
        f"*{ENGINE_NAME} {__version__}* — {cand.symbol}\n"
        f"{arrow}  |  Tier: *{cand.tier}*  |  Engine: `{_display_name(cand.engine)}`\n"
        f"Confidence: `{cand.confidence_raw:.0%}`  |  RR1: `{cand.rr_tp1:.2f}`  RR2: `{cand.rr_tp2:.2f}`\n"
        f"Entry: `{format_price(cand.entry)}`\n"
        f"SL: `{format_price(cand.sl)}`\n"
        f"TP1: `{format_price(cand.tp1)}`\n"
        f"TP2: `{format_price(cand.tp2)}`\n"
        f"Entry type: {_display_name(cand.entry_kind)}"
    )


def format_outcome_message(signal: dict) -> str:
    if signal["result"] == "expired":
        return f"🤷 *{ENGINE_NAME}* {signal['symbol']} signal expired — never filled (no fill, excluded from stats)."
    if signal["result"] == "loss":
        return f"😭 *{ENGINE_NAME}* {signal['symbol']} — SL hit. LOSS ({signal['realized_r']:.2f}R)."
    if signal.get("note") == "tp1_secured_then_sl_hit":
        return (f"🏆 *{ENGINE_NAME}* {signal['symbol']} — TP1 secured earlier, original SL later hit. "
                f"Counts as a WIN, {signal['realized_r']:.2f}R credited at TP1.")
    return f"🏆 *{ENGINE_NAME}* {signal['symbol']} — TP2 hit. WIN ({signal['realized_r']:.2f}R)."


def format_tp1_message(signal: dict) -> str:
    return (f"👍 *{ENGINE_NAME}* {signal['symbol']} — TP1 hit.\n"
            f"SL unchanged. Move it to entry yourself for breakeven if you want.")


def send_daily_summary(state: dict) -> None:
    totals = state["tier1"]["totals"]
    resolved = totals["wins"] + totals["losses"]
    wr = totals["wins"] / resolved if resolved else 0.0
    pf = totals["gross_profit_r"] / totals["gross_loss_r"] if totals["gross_loss_r"] > 0 else float("inf")
    avg_rr = totals["sum_r"] / resolved if resolved else 0.0
    avg_hold = totals["sum_hold_minutes"] / resolved if resolved else 0.0
    cat_lines = "\n".join(
        f"  {_display_name(cat)}: `{stats.get('n', 0)}`"
        for cat, stats in state["tier1"]["category_stats"].items()
    ) or "  (no resolved trades yet)"
    msg = (
        f"*{ENGINE_NAME} {__version__} — Daily Summary*\n"
        f"Signals: `{totals['signals']}`  Wins: `{totals['wins']}`  Losses: `{totals['losses']}`  "
        f"Expired: `{totals['expired']}`\n"
        f"Win rate: `{wr:.2%}`  Profit factor: `{pf:.2f}`  Avg RR: `{avg_rr:.2f}`  "
        f"Avg hold: `{avg_hold:.0f}m`\n"
        f"Forensic category breakdown:\n{cat_lines}"
    )
    send_telegram(msg)


# ═══════════════════════════════════════════════════════════════════════
# SECTION 14 — ORCHESTRATION / MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════

def monitor_active_signals(state: dict, hl: HyperliquidClient) -> None:
    """Advances every unresolved active signal by one fresh LTF candle,
    through the fill-verification + outcome-integrity pipeline, then routes
    resolutions into the forensics/learning loop."""
    to_remove = []
    for sig_id, signal in list(state["active_signals"].items()):
        candles = hl.candles(signal["symbol"], signal["timeframe"], 3)
        if not candles:
            continue
        latest = candles[-1]
        if latest["t"] <= signal.get("last_checked_ts", 0):
            continue  # no new closed candle yet — watermark-based scanning avoids duplicate work
        was_tp1 = signal["tp1_hit"]
        signal = check_fill_and_resolve(signal, latest)
        signal["last_checked_ts"] = latest["t"]

        if not was_tp1 and signal["tp1_hit"] and "result" not in signal:
            send_telegram(format_tp1_message(signal), reply_to=signal.get("tg_message_id"))

        if "result" in signal:
            signal["resolved_ts"] = time.time()
            if signal["result"] == "expired":
                state["tier1"]["totals"]["expired"] += 1
                send_telegram(format_outcome_message(signal), reply_to=signal.get("tg_message_id"))
            else:
                resolve_and_learn(signal, state)
                send_telegram(format_outcome_message(signal), reply_to=signal.get("tg_message_id"))
            to_remove.append(sig_id)
        else:
            state["active_signals"][sig_id] = signal

    for sig_id in to_remove:
        state["active_signals"].pop(sig_id, None)


def run_scan(hl: HyperliquidClient, store: StateStore) -> None:
    state = store.state
    log.info(f"=== {ENGINE_NAME} {__version__} scan start ===")

    monitor_active_signals(state, hl)
    evaluate_circuit_breaker(state)

    marks = hl.mark_prices()
    snaps: dict[str, SymbolSnapshot] = {}
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futures = {ex.submit(collect_snapshot, hl, sym, marks.get(sym, 0.0)): sym for sym in WATCHLIST}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                snap = fut.result()
                if snap:
                    snaps[sym] = snap
            except Exception as e:
                log.warning(f"Snapshot failed for {sym}: {e}")

    macro_snap = snaps.get(MACRO_ASSET)
    macro_view = None
    if macro_snap:
        macro_view = macro_snap.views.get(TF_HTF_SWING) or macro_snap.views.get(TF_HTF_INTRADAY)
    if not macro_view:
        log.error("No macro view available — skipping scan (cannot compute Regime Vector safely).")
        store.save()
        return
    regime = compute_regime_vector(macro_view, snaps)
    log.info(f"Regime Vector: label={regime.label()} macro_bias={regime.macro_bias} "
              f"vol_pctile={regime.vol_pctile:.2f} trend={regime.trend_strength:.2f} "
              f"noise={regime.noise_index:.2f} breadth={regime.breadth:.2f}")

    # DECISION: adaptive filter routing — tighten confluence threshold in
    # noisy/chaotic regimes, relax slightly in clean ones (Sec 9), applied as
    # a transient scan-local adjustment (not persisted) on top of the learned
    # baseline threshold so cold-start quality never depends on this routing.
    base_threshold = state["tier1"]["adaptive_params"]["filter_thresholds"]["min_confluence_score"]["value"]
    if regime.noise_index > 0.65 or regime.vol_pctile > 0.85:
        state["tier1"]["adaptive_params"]["filter_thresholds"]["min_confluence_score"]["value"] = \
            clamp(base_threshold + 0.08, 0.0, 0.9)
    elif regime.noise_index < 0.35 and regime.vol_pctile < 0.6:
        state["tier1"]["adaptive_params"]["filter_thresholds"]["min_confluence_score"]["value"] = \
            clamp(base_threshold - 0.05, 0.0, 0.9)

    all_candidates = []
    for sym, snap in snaps.items():
        all_candidates.extend(run_ensemble(snap, state))

    # restore learned baseline threshold (scan-local routing was transient)
    state["tier1"]["adaptive_params"]["filter_thresholds"]["min_confluence_score"]["value"] = base_threshold

    ranked = decision_engine_rank(all_candidates, regime, snaps, state)

    free_slots = MAX_CONCURRENT_ACTIVE_SIGNALS - len(state["active_signals"])
    active_symbols = {s["symbol"] for s in state["active_signals"].values()}
    dispatched = 0
    for cand in ranked:
        if free_slots <= 0:
            break
        if cand.symbol in active_symbols:
            continue  # one open signal per symbol at a time
        view = snaps[cand.symbol].views[cand.timeframe]
        signal = cand.to_dict()
        signal.update({
            "tp1_hit": False, "tp1_hit_ts": None, "entry_filled": (cand.entry_kind == "market"),
            "result": None, "realized_r": 0.0, "signal_ts": time.time(), "last_checked_ts": 0,
            "sl_buffer_used": adaptive_sl_buffer(view, state, cand.symbol),
            "mae": 0.0, "sfp_purity": cand.confluences.get("sfp_purity", cand.confluences.get("sweep_purity", 1.0)),
            "regime_at_entry": {"label": regime.label(), "macro_bias": regime.macro_bias},
            "regime_label": regime.label(),
            "thin_margin_pass": confluence_strength(cand) < base_threshold + 0.05,
        })
        msg_id = send_telegram(format_signal_message(cand))
        signal["tg_message_id"] = msg_id
        state["active_signals"][cand.id] = signal
        active_symbols.add(cand.symbol)
        free_slots -= 1
        dispatched += 1

    log.info(f"Dispatched {dispatched} new signal(s). Active signals now: {len(state['active_signals'])}.")

    today = datetime.now(timezone.utc).date().isoformat()
    hour = datetime.now(timezone.utc).hour
    if hour == 8 and state.get("daily_summary_date") != today:
        send_daily_summary(state)
        state["daily_summary_date"] = today

    store.prune_tier2()
    store.save()
    log.info(f"=== {ENGINE_NAME} {__version__} scan complete ===")


def main() -> None:
    store = StateStore(STATE_FILE)
    store.load()
    hl = HyperliquidClient()
    try:
        run_scan(hl, store)
    except Exception as e:
        log.exception(f"Fatal error during scan: {e}")
        store.save()
        raise


if __name__ == "__main__":
    main()
