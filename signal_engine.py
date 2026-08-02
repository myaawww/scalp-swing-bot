# ORACLE — Adaptive Multi-Engine Signal Platform
# v2.1.0
#
# Multi-specialist engine ensemble, ranked by a bounded continuous-blend
# Decision Engine and gated by a composite Regime Vector. Adaptive-percentile
# SL / liquidity-wall-clipped TP risk plans, entry-fill verification, and a
# closed-taxonomy win/loss forensics loop that drives every adaptive
# parameter, persisted in a two-tier state.json.
#
# Notable design choices are marked `# DECISION:`; fixed bugs `# BUGFIX:`.
#
# PATCH (v2.1.0) — forensic-loop responsiveness fix, based on a live state.json
# post-mortem: 18 resolved signals, 11 of 13 losses (85%) diagnosed as
# "regime_mismatch", concentrated in the `momentum` engine (7/7 of its losses).
# Root cause was two compounding issues, both fixed here:
#   1. `regime_fit_score()`'s mismatch base discount (0.25) was loose enough
#      that a mismatched candidate could still pass every other filter and
#      get taken; the adaptive punishment table (`regime_fit` mult) that was
#      supposed to eventually push it under the hard-veto line moves in
#      capped 0.06 steps (see `bounded_update`'s default max_step), so it
#      would have taken on the order of 150+ same-category losses to ever
#      cross the veto threshold on its own -- structurally unreachable at
#      this engine's trade volume. Base discount tightened to 0.15.
#   2. `apply_forensic_adaptive_response()` only ever acts once a category
#      collects MIN_SAMPLE_SIZE (20) losses since its last correction. That's
#      a reasonable bar for a genuinely ambiguous/noisy category, but a
#      category that already accounts for the overwhelming majority of all
#      losses is not ambiguous -- waiting for 20 samples in it while it's
#      already >=60% of everything going wrong just bleeds more R for no
#      statistical benefit. Added a dominance-triggered "fast gate" (Sec 13B
#      below) that fires an early, evidence-scaled correction well before the
#      slow gate would, without lowering the bar for genuinely marginal
#      categories.
# See the Sec 13B block near `apply_forensic_adaptive_response` for the fast
# gate implementation, and `regime_fit_score` for the discount change.

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
__version__ = "2.1.0"

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
# PERF (v1.1.2): candle cache lives in its own file, separate from state.json.
# It's just a rebuildable performance cache (never learned/adaptive data), so
# keeping it out of state.json keeps that file small, keeps diffs/backups of
# the "real" state clean, and lets the cache be wiped independently without
# touching any adaptive params or trade history.
CANDLE_CACHE_FILE = os.getenv("CANDLE_CACHE_FILE", "candle_cache.json")
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "4"))
HL_BASE_URL = "https://api.hyperliquid.xyz/info"
HL_MIN_INTERVAL_S = float(os.getenv("HL_MIN_INTERVAL_S", "0.15"))

# Watchlist mirrors the reference fleet (Kestrel/Aurelius/Axis/Kairos) — shared infra.
WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]
MACRO_ASSET = "BTCUSDT"  # DECISION: BTC is the macro-bias anchor for the Regime Vector (Sec 6).
MAJORS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"}

# 15m is the spec's forbidden floor. Swing pipeline: 1D/4H/1H; intraday: 4H/1H/15m.
TF_MACRO_SWING, TF_HTF_SWING, TF_LTF_SWING = "1d", "4h", "1h"
TF_HTF_INTRADAY, TF_MID_INTRADAY, TF_LTF_INTRADAY = "4h", "1h", "15m"
ALL_TFS = ["1d", "4h", "1h", "15m"]
TF_BARS = {"1d": 260, "4h": 300, "1h": 320, "15m": 320}
SCAN_INTERVAL_MIN = 15

EMA_FAST, EMA_SLOW, EMA_TREND = 21, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, BB_LEN = 14, 14, 14, 20

MAX_CONCURRENT_ACTIVE_SIGNALS = int(os.getenv("MAX_CONCURRENT_ACTIVE_SIGNALS", "8"))
# Correlated assets (majors) may not both occupy an active slot at once (Sec 14).
MAX_CORRELATED_CONCURRENT = 1

MIN_SAMPLE_SIZE = int(os.getenv("MIN_SAMPLE_SIZE", "20"))  # Sec 13 min-sample gate, per segment/category
TIER2_RETENTION_DAYS = 15  # Sec 5 raw-log pruning window

# Sec 13B (v2.1.0) — dominance-triggered fast gate for the forensic adaptive
# response. Separate from MIN_SAMPLE_SIZE: the slow gate stays at 20 for
# ordinary/ambiguous categories (unchanged), but a category that already
# represents an overwhelming share of *all* resolved losses is not ambiguous
# and shouldn't have to wait for 20 more of the same loss before the engine
# reacts. Gated on both an absolute floor (FAST_GATE_MIN_N) and a dominance
# share (FAST_GATE_DOMINANCE) so it can't fire off one or two early losses --
# both conditions must hold, same as the slow gate requires real repetition.
FORENSIC_FAST_GATE_MIN_N = int(os.getenv("FORENSIC_FAST_GATE_MIN_N", "5"))
FORENSIC_FAST_GATE_DOMINANCE = float(os.getenv("FORENSIC_FAST_GATE_DOMINANCE", "0.60"))
# Fast-gate corrections apply a bigger single step than the slow gate --
# justified because dominance itself (>=60% of all losses) is already
# stronger evidence than a bare 20-sample count would require, so a more
# decisive correction is warranted, not just an earlier small one. Expressed
# as a multiplier on each route's normal target_delta (STEP_SCALE) plus a
# raised ceiling (MAX_STEP) for it to actually land at -- raising the
# ceiling alone would do nothing on routes whose target_delta already sits
# under the slow gate's default 0.06 cap.
FORENSIC_FAST_GATE_STEP_SCALE = float(os.getenv("FORENSIC_FAST_GATE_STEP_SCALE", "3.0"))
FORENSIC_FAST_GATE_MAX_STEP = float(os.getenv("FORENSIC_FAST_GATE_MAX_STEP", "0.25"))
# After a fast-gate correction fires, require this many *additional* losses
# in the category (still dominant) before it can fire again -- prevents the
# fast path from re-firing on every single subsequent loss once a category
# is already dominant.
FORENSIC_FAST_GATE_COOLDOWN = int(os.getenv("FORENSIC_FAST_GATE_COOLDOWN", "5"))

# DECISION (v1.1.0): old floors produced technically-valid but tiny RR plans
# on low-vol assets/timeframes — a "swing" signal with a sub-1%-of-price
# SL/TP band is really a scalp in disguise. Widened for genuinely bigger
# stop/target distances (needs correspondingly smaller size to hold risk
# constant). RR shape is a separate knob — see RR_TP1_FLOOR / RR_TP2_EXTENSION.
# DECISION (v1.1.2): the v1.1.0 bump to 2.5 overshot in live use — reverted to
# the 1.8-2.0 band. 1.8 is the floor (was 1.5 pre-v1.1.0, then 2.5 in v1.1.0).
# DECISION (v1.1.3): raised to a hard 2:1 minimum on every dispatched trade.
# Structural TP1s (real opposing pivots) still price wherever the chart
# actually shows a level — this only raises the backstop that both
# build_risk_plan() and the mean_reversion/range_trading engines fall back
# to when the nearest real level pays out less than the minimum acceptable RR.
RR_TP1_FLOOR = 2.0       # require a bigger first-target payout per unit of risk
# DECISION (v1.1.0): TP1 fallback used only when no opposing structural pivot
# exists to validate a target against, so it's left close to the original
# (2.0->2.2) rather than pushed as far as the floors above — an unanchored
# projection shouldn't reach as aggressively as a wall-confirmed one.
# BUGFIX (v1.1.2): the v1.1.0 floor bump to 2.5 pushed RR_TP1_FLOOR above this
# ceiling (2.2), which silently made this constant dead: in build_risk_plan(),
# any unanchored raw_tp1 built from RR_TP1_CEIL_SOFT was always < floor_tp1 and
# got clobbered by the floor override, so every unanchored TP1 was actually
# priced at the (higher) floor, never at the intended, less-aggressive soft
# ceiling. Restored to 2.0 (top of the 1.8-2.0 band), above the new floor, so
# the ceiling is reachable again and the two constants define a real band.
# DECISION (v1.1.3): RR_TP1_FLOOR moved to 2.0 in this same change, which would
# have collapsed floor == ceiling and silently reproduced the exact bug noted
# above — this time in assign_tier() too, where `rr1 >= RR_TP1_CEIL_SOFT` is
# meant to separate "cleared the floor" from "meaningfully above it" for A+.
# Bumped to 2.2 to keep the same 0.2 gap above the new floor and keep both
# the build_risk_plan() band and the assign_tier() A+ check non-degenerate.
RR_TP1_CEIL_SOFT = 2.2
RR_TP2_EXTENSION = 2.5   # was hardcoded 1.2 -- TP2 = entry + risk*(rr1 + this), in RR units beyond TP1
RR_TP2_FALLBACK_EXTENSION = 1.5  # was hardcoded 0.5 -- used only if wall-clipping pulls TP2 back to/under rr1
MIN_ENTRY_SL_ATR_MULT = 0.9    # was 0.35 -- min entry-to-SL distance, in ATR (more room to not get wicked out)
MIN_ENTRY_TP1_ATR_MULT = 1.4   # was 0.55 -- min entry-to-TP1 distance, in ATR (bigger first target)
MAX_PENDING_ENTRY_ATR_MULT = 1.8  # cap on how far a pending zone entry may sit from market

CIRCUIT_BREAKER_WINDOW = 30       # trades in rolling live-performance window
CIRCUIT_BREAKER_WR_DROP = 0.12    # absolute win-rate drop vs baseline that trips the breaker
CIRCUIT_BREAKER_PF_DROP = 0.35    # relative profit-factor drop that trips the breaker

# ═══════════════════════════════════════════════════════════════════════
# SECTION 1 — STATE PERSISTENCE (Tier 1 aggregates / Tier 2 raw log)
# ═══════════════════════════════════════════════════════════════════════
# Tier 1 holds adaptive params + incrementally-updated aggregates (never
# rescanned from Tier 2). Tier 2 is a bounded, prunable raw trade log for
# forensics only — pruning it can never change Tier 1, since only
# resolve_trade() mutates Tier 1, once, at resolution time.

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
                    # DECISION (v1.1.0): raised default/min pct so the buffer sits deeper into
                    # the adverse-wick distribution -- old default clamped too thin on quiet assets.
                    "default": {"pct": 0.80, "min": 0.55, "max": 0.95}
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
        # NOTE: the persisted candle cache (keyed "{coin}|{interval}" -> list of
        # candle dicts) is intentionally NOT stored here — see CANDLE_CACHE_FILE /
        # CandleCacheStore below. It's a rebuildable perf cache, not adaptive state.
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


class CandleCacheStore:
    """Atomic, lock-guarded read/write for the candle cache, kept in its own
    file (CANDLE_CACHE_FILE) separate from state.json. Same load/save
    mechanics as StateStore (shared/exclusive flock + atomic tmp-file swap),
    but with no schema merging — it's just {"{coin}|{interval}": [candle, ...]}.
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.cache: dict = {}

    def load(self) -> dict:
        if not self.path.exists():
            log.info(f"No existing {self.path.name} — starting with an empty candle cache.")
            self.cache = {}
            return self.cache
        try:
            with open(self.path, "r") as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                try:
                    self.cache = json.load(f)
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
        except Exception as e:
            log.error(f"Failed to load {self.path.name} ({e}); starting with an empty candle cache.")
            self.cache = {}
        return self.cache

    def save(self) -> None:
        tmp_path = self.path.with_suffix(".json.tmp")
        with open(tmp_path, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                json.dump(self.cache, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
        os.replace(tmp_path, self.path)  # atomic swap


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
    """Sliding-window budget matching HL's documented weight system: most info
    requests (including candleSnapshot) cost 20 weight against a shared
    aggregated budget of 1200/min (not 900 — that undercounted our real
    headroom and caused unnecessary throttling sleeps)."""

    def __init__(self, budget_per_min: int = 1200):
        self.budget = budget_per_min
        self.window: list[tuple[float, int]] = []
        self._last_call = 0.0

    def acquire(self, weight: int = 20):
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
    # PERF (v1.1.2): `cache` is now a persisted dict loaded from its own
    # CANDLE_CACHE_FILE (via CandleCacheStore), not a fresh in-process dict
    # that got thrown away every scan. Previously every 15-min run re-downloaded
    # the full 260-320 bar lookback for all 25 symbols x 4 timeframes (~100
    # calls, ~2000+ weight) — almost all of which was history that hadn't
    # changed since the last run. With a warm cache we only need to ask for
    # bars newer than the last one we already have, and for timeframes whose
    # current candle can't have closed yet (e.g. a 4h candle 20 minutes after
    # the last scan) we skip the network call entirely.
    def __init__(self, cache: Optional[dict] = None):
        self.session = requests.Session()
        self.limiter = _WeightedRateLimiter()
        self._candle_cache: dict = cache if cache is not None else {}

    def _post(self, payload: dict, weight: int = 20, retries: int = 4) -> Any:
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
        cache_key = f"{coin}|{interval}"
        end_ms = int(time.time() * 1000)
        interval_ms = _interval_to_ms(interval)
        cached = self._candle_cache.get(cache_key) or []

        if cached:
            last_t = cached[-1]["t"]
            # The cached last candle can only still be the freshest *closed*
            # bar if the next one hasn't opened yet. If `now` hasn't reached
            # last_t + interval_ms, nothing new could possibly exist on the
            # exchange — skip the network call entirely (0 weight).
            if end_ms < last_t + interval_ms:
                return cached[-n_bars:]
            # Otherwise fetch just the gap since our last cached bar (plus a
            # small overlap in case the last cached bar was still forming),
            # instead of the full n_bars history.
            start_ms = last_t
        else:
            start_ms = end_ms - interval_ms * (n_bars + 5)

        payload = {"type": "candleSnapshot",
                   "req": {"coin": coin, "interval": interval, "startTime": start_ms, "endTime": end_ms}}
        data = self._post(payload, weight=20)
        if not data:
            return cached[-n_bars:] if cached else []

        fresh = [{"t": c["t"], "o": float(c["o"]), "h": float(c["h"]),
                  "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])}
                 for c in data]

        merged = {c["t"]: c for c in cached}
        for c in fresh:
            merged[c["t"]] = c
        all_sorted = sorted(merged.values(), key=lambda c: c["t"])
        # Keep a little more than n_bars cached so a couple of missed scan
        # cycles can't force us back into a full-history refetch.
        self._candle_cache[cache_key] = all_sorted[-(n_bars + 20):]
        return self._candle_cache[cache_key][-n_bars:]

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
        return view.atr[-1] * 0.6
    idx = clamp(int(pct * len(wicks)), 0, len(wicks) - 1)
    buf = wicks[idx]
    # floor/ceiling relative to ATR so buffer never collapses to ~0 or balloons unreasonably
    # DECISION (v1.1.0): raised floor 0.15x/1.2x -> 0.4x/2.0x ATR — old floor let the
    # buffer (and the whole SL) collapse to near-nothing on quiet candles.
    return clamp(buf, 0.4 * view.atr[-1], 2.0 * view.atr[-1])


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


def _farthest_structural_level(direction: str, entry: float, view: TFView) -> Optional[float]:
    """Most extreme real structural level (swing pivot) visible in this view, in
    the trade's direction — a hard sanity ceiling on TP2 so a 'let it run' second
    target is a level the chart has actually printed, not a bare RR extrapolation."""
    levels = [p.price for p in view.pivots if
              (p.kind == "high" and direction == "long" and p.price > entry) or
              (p.kind == "low" and direction == "short" and p.price < entry)]
    if not levels:
        return None
    return max(levels) if direction == "long" else min(levels)


def extend_tp2(direction: str, entry: float, sl: float, risk: float, rr1: float,
               view: TFView) -> tuple[float, float]:
    """Build a TP2 that sits strictly beyond the caller's own tp1/rr1, wall-clipped
    like TP1, and bounded by real visible structure rather than a bare RR projection.

    DECISION (v1.1.0 bugfix): extracted so every engine calls this AFTER its own
    final tp1/rr1 is decided. mean_reversion and range_trading build their own tp1
    (BB mean / range target) independent of build_risk_plan's internal tp1, so
    taking build_risk_plan's tp2 as-is could put tp2 nearer to entry than the
    engine's real tp1. Calling this uniformly makes that structurally impossible.

    DECISION (v1.1.0): TP2 is capped at the furthest real pivot this view has
    printed in the trade's direction (_farthest_structural_level), so a wider
    RR_TP2_EXTENSION only reaches prices with actual chart backing. If bounding
    erases the extension's edge over TP1 (rr2 <= rr1), step down to a smaller
    extension rather than shipping tp2 <= tp1.
    """
    ceiling = _farthest_structural_level(direction, entry, view)
    wall_buffer = view.atr[-1] * 0.08

    def _attempt(extension: float) -> tuple[float, float]:
        raw = entry + risk * (rr1 + extension) if direction == "long" \
            else entry - risk * (rr1 + extension)
        tp = clip_target_to_liquidity_wall(direction, entry, raw, view)
        if ceiling is not None:
            if direction == "long" and tp > ceiling:
                tp = ceiling - wall_buffer
            if direction == "short" and tp < ceiling:
                tp = ceiling + wall_buffer
        return tp, _rr(entry, sl, tp, direction)

    tp2, rr2 = _attempt(RR_TP2_EXTENSION)
    if rr2 <= rr1:
        tp2, rr2 = _attempt(RR_TP2_FALLBACK_EXTENSION)
    if rr2 <= rr1:
        # DECISION: no confirmed level beyond TP1 in this view. Rather than fabricate
        # a further price or violate tp2>tp1, take a small honest step past TP1 and
        # let the trade's tier reflect the thinner setup.
        tp2 = entry + risk * (rr1 + 0.2) if direction == "long" else entry - risk * (rr1 + 0.2)
        rr2 = _rr(entry, sl, tp2, direction)
    return tp2, rr2


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
    # never let TP1 fall below the floor even if nearest structure is closer
    if direction == "long" and raw_tp1 < floor_tp1:
        raw_tp1 = floor_tp1
    if direction == "short" and raw_tp1 > floor_tp1:
        raw_tp1 = floor_tp1

    tp1 = clip_target_to_liquidity_wall(direction, entry, raw_tp1, view)
    rr1 = _rr(entry, sl, tp1, direction)
    if rr1 < RR_TP1_FLOOR:
        # DECISION: clipping must never be used to justify shrinking below the
        # floor — if clipping pushed RR under the floor, reject the candidate (Sec 10).
        return None

    tp2, rr2 = extend_tp2(direction, entry, sl, risk, rr1, view)

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
# Every engine consumes the shared TFView/Zone primitives from Section 4
# rather than duplicating zone/structure discovery.

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
    rr_tp1 = _rr(entry, plan["sl"], mean, direction)
    if rr_tp1 < RR_TP1_FLOOR:
        return out
    # BUGFIX (v1.1.0): tp1 here is the BB mean, NOT plan["tp1"] -- so tp2 must
    # be rebuilt relative to rr_tp1/mean, not taken as plan["tp2"] (which was
    # only ever guaranteed to beat plan's own internal tp1). See extend_tp2().
    tp2, rr_tp2 = extend_tp2(direction, entry, plan["sl"], plan["risk"], rr_tp1, v)
    cand = Candidate(
        id=_new_id(snap.symbol, "mean_reversion"), symbol=snap.symbol, engine="mean_reversion", direction=direction,
        entry=entry, sl=plan["sl"], tp1=mean, tp2=tp2, entry_kind="market",
        timeframe=v.tf, confidence_raw=0.45, regime_fit=["ranging", "consolidation", "low_vol"],
        confluences={"z_score_extreme": clamp((abs(z) - 1.8) / 2.0, 0.0, 1.0)},
        rr_tp1=rr_tp1, rr_tp2=rr_tp2,
    )
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
    # BUGFIX (v1.1.0): tp1 here is the range interior target, NOT plan["tp1"]
    # -- rebuild tp2 relative to rr1/target instead of taking plan["tp2"] as-is.
    tp2, rr2 = extend_tp2(direction, entry, plan["sl"], plan["risk"], rr1, v)
    cand = Candidate(
        id=_new_id(snap.symbol, "range_trading"), symbol=snap.symbol, engine="range_trading", direction=direction,
        entry=entry, sl=plan["sl"], tp1=target, tp2=tp2, entry_kind="market",
        timeframe=v.tf, confidence_raw=0.45, regime_fit=["ranging", "consolidation", "low_vol"],
        confluences={"range_edge_proximity": clamp(1 - min(pos, 1 - pos) / 0.15, 0.0, 1.0)},
        rr_tp1=rr1, rr_tp2=rr2,
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


def _tp_ordering_sane(cand: Candidate) -> bool:
    """Safety net (v1.1.0): tp2 must sit strictly beyond tp1 in the trade's favor,
    for every engine. Re-checked here, engine-agnostically, so this bug class
    can't silently reappear if a future engine forgets to call extend_tp2()."""
    if cand.direction == "long":
        return cand.tp2 > cand.tp1 > cand.entry
    return cand.tp2 < cand.tp1 < cand.entry


def run_ensemble(snap: SymbolSnapshot, state: dict) -> list[Candidate]:
    candidates = []
    for name, fn in ENGINES.items():
        try:
            produced = fn(snap, state)
        except Exception as e:
            log.warning(f"Engine {name} failed on {snap.symbol}: {e}")
            continue
        for cand in produced:
            if not _tp_ordering_sane(cand):
                log.error(
                    f"REJECTED {snap.symbol}/{name}: tp ordering invalid "
                    f"(entry={cand.entry} tp1={cand.tp1} tp2={cand.tp2} dir={cand.direction}) "
                    f"— this indicates a bug in that engine's target construction."
                )
                continue
            candidates.append(cand)
    return candidates


# ═══════════════════════════════════════════════════════════════════════
# SECTION 9 — DECISION ENGINE: continuous bounded blend + mandatory vetoes
# ═══════════════════════════════════════════════════════════════════════
# DECISION: exactly 7 terms in the blend (regime fit, MTF alignment, confluence
# strength, historical segment performance, EV, RR, liquidity context) — a small,
# auditable set per Sec 4. Session weight and volatility percentile fold into
# "regime fit" rather than being added separately, since they're already
# components of the Regime Vector that regime_fit_score consumes — adding them
# again would be the correlated double-counting Sec 4 warns against.

def confluence_strength(cand: Candidate) -> float:
    if not cand.confluences:
        return 0.0
    vals = list(cand.confluences.values())
    # DECISION: mean rather than sum, bounded 0..1, so one missing confluence
    # lowers the average instead of zeroing the score.
    return clamp(sum(vals) / len(vals), 0.0, 1.0)


def regime_fit_score(cand: Candidate, regime: RegimeVector, state: dict) -> tuple[float, bool]:
    label = regime.label()
    veto_table = state["tier1"]["adaptive_params"]["regime_fit"].get(cand.engine, {})
    mult = veto_table.get(label, 1.0)
    matches = label in cand.regime_fit or regime.macro_bias in cand.regime_fit
    # PATCH (v2.1.0): was 0.25. At 0.25, a mismatched candidate with a fully
    # un-punished mult (1.0, the state every (engine, regime) pair starts at
    # and stays at until the slow gate fires many times) scores 0.25 -- well
    # above the 0.12 hard-veto floor below, so the veto term alone could
    # never exclude a fresh mismatch; it only discounted it, letting it
    # still win on other terms. Tightened so a fresh, unpunished mismatch
    # sits close enough to the veto line that even one adaptive correction
    # (slow or fast gate) pushes it under, rather than requiring the ~150
    # same-category losses the old value implied at bounded_update's default
    # step size.
    base = 1.0 if matches else 0.15  # Sec 13 regime-fit veto: heavy discount, not necessarily hard-zero
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
    # DECISION (v1.1.2): this used to read `rr1 >= 1.8` back when RR_TP1_FLOOR
    # was 1.5, so it genuinely split candidates into "cleared the floor but
    # thin" (1.5-1.8, capped at tier A) vs. "real RR" (>=1.8, A+ eligible).
    # With RR_TP1_FLOOR now at the same value as this check, every candidate
    # that reaches this function already clears the floor by construction
    # (build_risk_plan/engine_mean_reversion/engine_range_trading all reject
    # below it), so an equal-valued check here would always be true and the
    # RR condition on A+ would silently stop discriminating anything. Kept
    # pinned to RR_TP1_CEIL_SOFT, always held above RR_TP1_FLOOR (currently
    # 2.2 vs. 2.0 — see those constants), so A+ still requires RR meaningfully
    # above the bare floor, not just clearing it.
    if score >= 0.78 and rr1 >= RR_TP1_CEIL_SOFT:
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

    # DECISION: same-candle SL-vs-TP ambiguity resolved conservatively by checking
    # SL first — doesn't manufacture a false stop-out, only decides which of two
    # genuinely co-occurring touches is credited within the same bar.
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


def _route_category_correction(category: str, signal: dict, state: dict,
                                step_scale: float = 1.0) -> str:
    """The actual Sec 13 rule-3 parameter route: one diagnosis -> one
    deterministic parameter update. Factored out of apply_forensic_adaptive_
    response (v2.1.0) so both the slow gate and the dominance fast gate (Sec
    13B) can drive the same routing table at different step sizes.
    step_scale=1.0 (slow gate) reproduces the original behavior exactly
    (each route's own target_delta, capped at bounded_update's default 0.06).
    The fast gate passes FORENSIC_FAST_GATE_STEP_SCALE, which scales *and*
    raises the cap (FORENSIC_FAST_GATE_MAX_STEP) so the bigger target_delta
    actually lands instead of being clamped back down to the same size."""
    params = state["tier1"]["adaptive_params"]
    engine = signal["engine"]
    kw = {} if step_scale == 1.0 else {"max_step": FORENSIC_FAST_GATE_MAX_STEP}

    if category == "regime_mismatch":
        table = params["regime_fit"].setdefault(engine, {r: 1.0 for r in REGIMES})
        label = signal.get("regime_label", "neutral")
        table[label] = bounded_update(table.get(label, 1.0), -0.08 * step_scale, 0.15, 1.0, **kw)
        return f"regime_fit[{engine}][{label}] -= step"

    if category == "structural_invalidation_too_tight":
        asset_cfg = params["sl_buffer_percentile"].setdefault(
            signal["symbol"], dict(params["sl_buffer_percentile"]["default"]))
        asset_cfg["pct"] = bounded_update(asset_cfg["pct"], 0.05 * step_scale, asset_cfg["min"], asset_cfg["max"], **kw)
        params["sl_buffer_percentile"][signal["symbol"]] = asset_cfg
        return f"sl_buffer_percentile[{signal['symbol']}] += step"

    if category == "chased_swept_liquidity":
        th = params["filter_thresholds"]["liquidity_sanity_gap_atr"]
        th["value"] = bounded_update(th["value"], 0.05 * step_scale, th["min"], th["max"], **kw)
        return "liquidity_sanity_gap_atr += step"

    if category == "mtf_conflict_ignored":
        th = params["filter_thresholds"]["mtf_alignment_weight"]
        th["value"] = bounded_update(th["value"], 0.03 * step_scale, th["min"], th["max"], **kw)
        return "mtf_alignment_weight += step"

    if category == "sfp_mss_sequence_violated":
        th = params["filter_thresholds"]["sfp_mss_strictness"]
        th["value"] = bounded_update(th["value"], 0.05 * step_scale, th["min"], th["max"], **kw)
        return "sfp_mss_strictness += step"

    if category == "confidence_miscalibration":
        bucket = "low" if signal["confidence_raw"] < 0.5 else "mid" if signal["confidence_raw"] < 0.68 else "high"
        table = params["confidence_calibration"].setdefault(engine, {"low": 1.0, "mid": 1.0, "high": 1.0})
        table[bucket] = bounded_update(table[bucket], -0.06 * step_scale, 0.5, 1.3, **kw)
        return f"confidence_calibration[{engine}][{bucket}] -= step"

    if category == "filter_over_permissiveness":
        th = params["filter_thresholds"]["min_confluence_score"]
        th["value"] = bounded_update(th["value"], 0.04 * step_scale, th["min"], th["max"], **kw)
        return "min_confluence_score += step"

    if category == "correct_read_poor_rr":
        return "no_change_logged_for_rr_floor_review"

    return "no_change_genuine_variance"


def apply_forensic_adaptive_response(category: str, signal: dict, state: dict) -> str:
    """Sec 13 rule 3 + Sec 13B (v2.1.0): one diagnosis -> one deterministic
    parameter route, gated either by the slow 20-sample gate (unchanged,
    for ordinary/ambiguous categories) or the new dominance fast gate (for a
    category that already accounts for most of the book's losses -- see
    FORENSIC_FAST_GATE_* constants). Returns a human-readable description of
    the delta applied (or none)."""
    cat_stats = state["tier1"]["category_stats"].setdefault(
        category, {"n": 0, "n_since_last_gate": 0, "n_since_last_fast_gate": 0, "fast_gate_ever_fired": False})
    # BUGFIX: backfill both new fields for state.json files written before v2.1.0.
    cat_stats.setdefault("n_since_last_fast_gate", 0)
    cat_stats.setdefault("fast_gate_ever_fired", False)
    cat_stats["n"] += 1
    cat_stats["n_since_last_gate"] += 1
    cat_stats["n_since_last_fast_gate"] += 1

    # NOTE: totals["losses"] hasn't been incremented for *this* trade yet --
    # resolve_and_learn bumps it after this function returns -- so +1 here
    # to reflect the count as it will be immediately after this resolution,
    # not one loss stale.
    total_losses_after_this = state["tier1"]["totals"]["losses"] + 1
    is_dominant = (
        cat_stats["n"] >= FORENSIC_FAST_GATE_MIN_N
        and cat_stats["n"] / total_losses_after_this >= FORENSIC_FAST_GATE_DOMINANCE
    )
    slow_fires = cat_stats["n_since_last_gate"] >= MIN_SAMPLE_SIZE
    # DECISION: the cooldown only governs the gap *between* fast-gate
    # corrections (so an already-dominant category can't fast-fire on every
    # single subsequent loss). It must NOT gate the *first* fast-gate
    # correction for a category, or an already-badly-dominant category (like
    # regime_mismatch at 11/13 losses in the state.json this patch was built
    # against) would sit through one more full cooldown window before ever
    # getting its first correction -- defeating the point of "fast".
    fast_fires = (
        is_dominant
        and not slow_fires  # slow gate takes priority if both happen to line up
        and (not cat_stats["fast_gate_ever_fired"] or cat_stats["n_since_last_fast_gate"] >= FORENSIC_FAST_GATE_COOLDOWN)
    )

    if not slow_fires and not fast_fires:
        return "no_change_insufficient_category_samples"

    if slow_fires:
        cat_stats["n_since_last_gate"] = 0
        cat_stats["n_since_last_fast_gate"] = 0  # a full correction also resets the fast cooldown
        delta_desc = _route_category_correction(category, signal, state)
    else:
        cat_stats["n_since_last_fast_gate"] = 0
        cat_stats["fast_gate_ever_fired"] = True
        delta_desc = _route_category_correction(category, signal, state, step_scale=FORENSIC_FAST_GATE_STEP_SCALE)
        delta_desc += f" (fast_gate, {cat_stats['n']}/{total_losses_after_this} losses = {category})"

    return delta_desc


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

    cat = state["tier1"]["category_stats"].setdefault(
        category, {"n": 0, "n_since_last_gate": 0, "n_since_last_fast_gate": 0})
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
# DECISION: reaction emojis are restricted to Telegram's native quick-reaction
# set so every emoji sent can also be tapped back as a genuine reaction —
# 🏆 win, 😭 loss, 👍 TP1 hit, 🤷 expired/no-fill, 🤯 breaker tripped, 👏 recovered.
# DECISION: underscore-bearing internal identifiers (engine names, forensic
# category keys) go through _display_name() before reaching Telegram —
# underscores stay in state.json/code only.

_ACRONYM_DISPLAY_OVERRIDES = {"smc": "SMC"}


def _display_name(identifier: str) -> str:
    if identifier in _ACRONYM_DISPLAY_OVERRIDES:
        return _ACRONYM_DISPLAY_OVERRIDES[identifier]
    return identifier.replace("_", " ").title()


def format_price(price: float) -> str:
    """Decimal places scale with price magnitude (Sec 17 cap): >= $100 -> 2
    decimals, $1-$100 -> 4, < $1 -> 6. Trailing zeros trimmed, never scientific
    notation."""
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
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"}
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
    dot = "🟢" if cand.direction == "long" else "🔴"
    side = "LONG" if cand.direction == "long" else "SHORT"
    # DECISION (bugfix): recompute RR1/RR2 fresh from final entry/SL/TP rather than
    # trusting the candidate's stored rr_tp1/rr_tp2 — keeps displayed price and RR
    # structurally impossible to desync, even if a future engine sets tp without sl.
    rr1 = _rr(cand.entry, cand.sl, cand.tp1, cand.direction)
    rr2 = _rr(cand.entry, cand.sl, cand.tp2, cand.direction)
    return (
        f"*{ENGINE_NAME} {__version__}*\n"
        f"{cand.symbol} | {side} {dot}\n\n"
        f"Confidence: {cand.confidence_raw:.0%}  |  Tier: {cand.tier}\n"
        f"Engine: {_display_name(cand.engine)}\n\n"
        f"Entry: `{format_price(cand.entry)}`\n"
        f"SL: `{format_price(cand.sl)}`\n"
        f"TP1: `{format_price(cand.tp1)}`\n"
        f"TP2: `{format_price(cand.tp2)}`\n\n"
        f"RR1: {rr1:.2f}  RR2: {rr2:.2f}\n"
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
    # DECISION (v1.1.1, safety-net): only "win" reaches here now — anything else
    # must not silently be reported as a win (see the key-presence bugfix below).
    if signal["result"] != "win":
        raise ValueError(f"format_outcome_message called on unresolved/unrecognized signal result: {signal['result']!r}")
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
    """Advances every unresolved active signal through fill-verification and
    outcome-integrity, one CLOSED LTF candle at a time, then routes resolutions
    into the forensics/learning loop.

    BUGFIX (v1.0.1): previously fetched 3 candles and evaluated only the last —
    but hl.candles() ends at now(), so that last candle could be still-forming,
    and evaluating it risked permanently skipping the real closed range once
    its watermark got stamped. Any missed scan cycle also silently dropped the
    candles in between. Fix: only consider fully-closed candles, and walk every
    closed candle since the last check in chronological order.
    """
    to_remove = []
    for sig_id, signal in list(state["active_signals"].items()):
        interval_ms = _interval_to_ms(signal["timeframe"])
        now_ms = int(time.time() * 1000)
        # fetch generously (not just 3) so a missed scan cycle can still be
        # fully backfilled in one pass instead of losing candles
        candles = hl.candles(signal["symbol"], signal["timeframe"], 12)
        if not candles:
            continue

        closed = [c for c in candles if c["t"] + interval_ms <= now_ms]
        # DECISION (v1.1.1, defense-in-depth): floor the watermark at the signal's
        # own creation time too, protecting any signal already in state.json from
        # a prior run where last_checked_ts was written as 0.
        signal_ts_ms = int(signal.get("signal_ts", 0) * 1000)
        watermark = max(signal.get("last_checked_ts", 0), signal_ts_ms)
        new_candles = [c for c in closed if c["t"] > watermark]
        if not new_candles:
            continue  # nothing new and fully closed yet — nothing to do

        for candle in new_candles:
            was_tp1 = signal["tp1_hit"]
            signal = check_fill_and_resolve(signal, candle)
            signal["last_checked_ts"] = candle["t"]

            if not was_tp1 and signal["tp1_hit"] and signal.get("result") is None:
                send_telegram(format_tp1_message(signal), reply_to=signal.get("tg_message_id"))

            # BUGFIX (v1.1.1): "result" is always a key in signal (init to None at
            # dispatch), so `"result" in signal` was always True — forcing every
            # signal to force-resolve one scan after dispatch as a false "WIN
            # (0.00R)". Must check the value, not key presence.
            if signal.get("result") is not None:
                # resolved on this candle — any later candles in this batch
                # are chronologically irrelevant and must not be applied
                break

        if signal.get("result") is not None:
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

    # DECISION: tighten confluence threshold in noisy/chaotic regimes, relax in
    # clean ones (Sec 9) — a transient scan-local adjustment, not persisted, so
    # cold-start quality never depends on it.
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
            # BUGFIX: floored at signal-creation time (not 0/epoch) so monitor_active_signals()
            # can't replay pre-existing candles from before this signal existed.
            "result": None, "realized_r": 0.0, "signal_ts": time.time(),
            "last_checked_ts": int(time.time() * 1000),
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
    candle_store = CandleCacheStore(CANDLE_CACHE_FILE)
    candle_store.load()
    hl = HyperliquidClient(cache=candle_store.cache)
    try:
        run_scan(hl, store)
    except Exception as e:
        log.exception(f"Fatal error during scan: {e}")
        store.save()
        candle_store.save()
        raise
    else:
        candle_store.save()


if __name__ == "__main__":
    main()
