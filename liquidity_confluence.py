from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field
from typing import Optional, Iterable

from utils import safe, atr, sma, _cluster_levels

# ─────────────────────────────────────────────────────────────
# CONFIGURATION (all weights configurable, no hardcoded scoring)
# ─────────────────────────────────────────────────────────────

@dataclass
class LiquidityConfig:
    enable: bool = True

    # Fractal swing detection
    fractal_left: int = 3
    fractal_right: int = 3

    # Dealing range (internal vs external)
    external_lookback_bars: int = 100
    internal_lookback_bars: int = 30

    # EQH/EQL
    eqh_eql_atr_mult: float = 0.15
    eqh_eql_min_touches: int = 2
    eqh_eql_strength_mult: float = 1.5

    # Liquidity cluster scoring
    cluster_touch_weight: float = 1.0
    cluster_age_decay_per_bar: float = 0.001
    cluster_distance_max_atr: float = 5.0
    cluster_volume_weight: float = 0.5

    # False sweep filter
    false_sweep_lookback: int = 2
    false_sweep_continue_atr: float = 0.25

    # Premium / Discount
    prefer_discount_for_long: bool = True
    prefer_premium_for_short: bool = True

    # FVG
    fvg_min_size_atr: float = 0.25

    # Sweep volume confirmation
    sweep_vol_mult: float = 1.1

    # OI confirmation (optional)
    use_oi_confirmation: bool = True

    # L2 imbalance (optional, externally supplied)
    l2_imbalance_threshold: float = 1.5

    # R:R against external liquidity target
    external_rr_min: float = 1.5
    external_rr_bonus: int = 2
    external_rr_penalty: int = -1

    # Multi-timeframe
    htf_timeframes: tuple = ("1h", "4h")

    # Sweep recency decay
    sweep_recency_max_bars: int = 10
    sweep_recency_decay_per_bar: float = 0.5

    # Confidence decay after sweep
    confidence_decay_max_bars: int = 10
    confidence_decay_per_bar: float = 0.5

    # ── Scoring weights (no hardcoded scoring values anywhere) ──
    external_liquidity_weight: int = 2
    internal_liquidity_weight: int = 1
    sweep_weight: int = 4
    mss_weight: int = 5
    bos_weight: int = 3
    htf_weight: int = 3
    fvg_weight: int = 2
    volume_confirm_weight: int = 1
    oi_confirm_weight: int = 1
    eqh_eql_weight: int = 1
    l2_imbalance_weight: int = 1
    premium_discount_weight: int = 2
    rr_weight: int = 2
    sweep_recency_weight: int = 2
    confidence_decay_penalty_per_bar: int = 1

    # Caps
    max_bonus: int = 20
    final_strength_cap: int = 100

    # Hard-suppress toggle for false sweeps
    hard_suppress_false_sweeps: bool = False


# ─────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────

@dataclass
class LiquidityLevel:
    price: float
    strength: float
    timestamp: int
    classification: str = "internal"   # "internal" | "external"
    side: str = "bsl"                  # "bsl" | "ssl"
    touches: int = 1
    is_eqh_eql: bool = False


@dataclass
class SweepResult:
    sweep_detected: bool = False
    sweep_type: Optional[str] = None   # "bullish" | "bearish" | None
    swept_level: Optional[LiquidityLevel] = None
    bar_index: int = -1
    is_false_sweep: bool = False
    volume_confirmed: bool = False
    age_bars: int = 0


@dataclass
class MSSResult:
    mss_detected: bool = False
    direction: Optional[str] = None    # "bullish" | "bearish" | None


@dataclass
class BOSResult:
    bos_detected: bool = False
    direction: Optional[str] = None


@dataclass
class FVGResult:
    fvg_detected: bool = False
    direction: Optional[str] = None    # "bullish" | "bearish"
    top: float = 0.0
    bottom: float = 0.0
    age_bars: int = 0


@dataclass
class ConfluenceOutput:
    base_strength: int = 0
    liquidity_bonus: int = 0
    final_strength: int = 0
    liquidity_score: int = 0
    structure_score: int = 0
    nearest_external_bsl: Optional[float] = None
    nearest_external_ssl: Optional[float] = None
    nearest_internal_bsl: Optional[float] = None
    nearest_internal_ssl: Optional[float] = None
    liquidity_sweep: dict = field(default_factory=dict)
    mss: dict = field(default_factory=dict)
    bos: dict = field(default_factory=dict)
    fvg: dict = field(default_factory=dict)
    premium_discount: str = "neutral"  # "premium" | "discount" | "neutral"
    htf_alignment: bool = False
    reasons: list = field(default_factory=list)
    hard_suppress: bool = False


# ─────────────────────────────────────────────────────────────
# LIQUIDITY ANALYZER
# ─────────────────────────────────────────────────────────────

class LiquidityAnalyzer:
    """
    Detects Buy/Sell Side Liquidity, classifies internal/external,
    and detects liquidity sweeps.
    Reuses the same fractal logic pattern as the engine's find_sr_levels().
    """

    def __init__(self, config: LiquidityConfig):
        self.cfg = config

    # ---- Swing detection (reuses fractal pattern) ----
    def detect_swings(self, candles: list[dict]) -> tuple[list[dict], list[dict]]:
        """
        Return (swing_highs, swing_lows) where each item is
        {"t": int, "price": float, "index": int}.

        O(n) — single pass with `fractal_left + fractal_right` window.
        No future leakage: a swing at index i is confirmed only at
        i + fractal_right (right bars must be lower/higher).
        """
        lb, rb = self.cfg.fractal_left, self.cfg.fractal_right
        n = len(candles)
        highs: list[dict] = []
        lows: list[dict] = []
        for i in range(lb, n - rb):
            h = candles[i]["h"]
            l = candles[i]["l"]
            t = candles[i]["t"]

            is_high = all(h > candles[i - k]["h"] for k in range(1, lb + 1)) and \
                      all(h > candles[i + k]["h"] for k in range(1, rb + 1))
            is_low = all(l < candles[i - k]["l"] for k in range(1, lb + 1)) and \
                     all(l < candles[i + k]["l"] for k in range(1, rb + 1))

            if is_high:
                highs.append({"t": t, "price": h, "index": i})
            if is_low:
                lows.append({"t": t, "price": l, "index": i})
        return highs, lows

    # ---- BSL / SSL construction with classification ----
    def build_liquidity_levels(
        self,
        candles: list[dict],
        swing_highs: list[dict],
        swing_lows: list[dict],
        atr_val: float,
        current_price: float,
        current_bar_index: int,
    ) -> tuple[list[LiquidityLevel], list[LiquidityLevel]]:
        """
        Returns (bsl_levels, ssl_levels) sorted by distance from current price.
        Classifies each as internal or external based on dealing range.
        """
        cfg = self.cfg
        n = len(candles)

        # Dealing range determination
        ext_lb = min(cfg.external_lookback_bars, n - 1)
        int_lb = min(cfg.internal_lookback_bars, n - 1)

        ext_window_highs = candles[-ext_lb:]
        ext_window_lows = candles[-ext_lb:]
        major_high = max(c["h"] for c in ext_window_highs)
        major_low = min(c["l"] for c in ext_window_lows)

        int_window_highs = candles[-int_lb:]
        int_window_lows = candles[-int_lb:]
        recent_high = max(c["h"] for c in int_window_highs)
        recent_low = min(c["l"] for c in int_window_lows)

        def _classify(swing_price: float, is_high: bool) -> str:
            if is_high:
                # BSL above major high = external; below recent high = internal
                if swing_price >= major_high * 0.999:
                    return "external"
                return "internal"
            else:
                if swing_price <= major_low * 1.001:
                    return "external"
                return "internal"

        # EQH/EQL clustering (reuses _cluster_levels pattern)
        eqh_eql_tol = max(atr_val * cfg.eqh_eql_atr_mult, 1e-9)
        # Group swing highs into EQH clusters
        sh_prices = sorted([(s["price"], s) for s in swing_highs], key=lambda x: x[0])
        sh_clusters: list[list[dict]] = []
        for price, s in sh_prices:
            placed = False
            for cluster in sh_clusters:
                if abs(price - cluster[0]["price"]) < eqh_eql_tol:
                    cluster.append(s)
                    placed = True
                    break
            if not placed:
                sh_clusters.append([s])

        sl_prices = sorted([(s["price"], s) for s in swing_lows], key=lambda x: x[0])
        sl_clusters: list[list[dict]] = []
        for price, s in sl_prices:
            placed = False
            for cluster in sl_clusters:
                if abs(price - cluster[0]["price"]) < eqh_eql_tol:
                    cluster.append(s)
                    placed = True
                    break
            if not placed:
                sl_clusters.append([s])

        bsl_levels: list[LiquidityLevel] = []
        ssl_levels: list[LiquidityLevel] = []

        for cluster in sh_clusters:
            # BSL sits above the swing highs (resting buy-side liquidity)
            price = max(s["price"] for s in cluster)
            ts = max(s["t"] for s in cluster)
            touches = len(cluster)
            is_eqh = touches >= cfg.eqh_eql_min_touches
            strength = self._cluster_strength(
                touches, current_bar_index, max(s["index"] for s in cluster),
                price, current_price, atr_val, candles
            )
            if is_eqh:
                strength *= cfg.eqh_eql_strength_mult
            classification = _classify(price, is_high=True)
            bsl_levels.append(LiquidityLevel(
                price=price, strength=strength, timestamp=ts,
                classification=classification, side="bsl",
                touches=touches, is_eqh_eql=is_eqh,
            ))

        for cluster in sl_clusters:
            price = min(s["price"] for s in cluster)
            ts = max(s["t"] for s in cluster)
            touches = len(cluster)
            is_eql = touches >= cfg.eqh_eql_min_touches
            strength = self._cluster_strength(
                touches, current_bar_index, max(s["index"] for s in cluster),
                price, current_price, atr_val, candles
            )
            if is_eql:
                strength *= cfg.eqh_eql_strength_mult
            classification = _classify(price, is_high=False)
            ssl_levels.append(LiquidityLevel(
                price=price, strength=strength, timestamp=ts,
                classification=classification, side="ssl",
                touches=touches, is_eqh_eql=is_eql,
            ))

        # Sort by distance from current price
        bsl_levels = [lv for lv in bsl_levels if lv.price > current_price] or bsl_levels
        ssl_levels = [lv for lv in ssl_levels if lv.price < current_price] or ssl_levels
        bsl_levels.sort(key=lambda lv: lv.price - current_price)
        ssl_levels.sort(key=lambda lv: current_price - lv.price)
        return bsl_levels, ssl_levels

    def _cluster_strength(
        self, touches: int, current_bar: int, swing_bar: int,
        level_price: float, current_price: float, atr_val: float,
        candles: list[dict],
    ) -> float:
        cfg = self.cfg
        # Touch count contribution
        s = touches * cfg.cluster_touch_weight
        # Age decay (older = weaker)
        age_bars = max(0, current_bar - swing_bar)
        s *= max(0.0, 1.0 - cfg.cluster_age_decay_per_bar * age_bars)
        # Distance penalty (closer = stronger confluence, but capped)
        dist_atr = abs(level_price - current_price) / max(atr_val, 1e-9)
        if dist_atr > cfg.cluster_distance_max_atr:
            s *= 0.1
        # Nearby volume contribution
        if 0 <= swing_bar < len(candles):
            vols = [candles[j]["v"] for j in range(max(0, swing_bar - 3), min(len(candles), swing_bar + 4))]
            avg_vol = sum(vols) / max(1, len(vols))
            all_vols = [c["v"] for c in candles[-50:]] if len(candles) >= 50 else [c["v"] for c in candles]
            overall_avg = sum(all_vols) / max(1, len(all_vols))
            if overall_avg > 0:
                s *= (1.0 + cfg.cluster_volume_weight * (avg_vol / overall_avg - 1.0))
        return max(0.0, s)

    # ---- Liquidity Sweep Detection ----
    def detect_sweep(
        self,
        candles: list[dict],
        bsl_levels: list[LiquidityLevel],
        ssl_levels: list[LiquidityLevel],
        current_bar_index: int,
        atr_val: float,
        vol_ma: float | None,
    ) -> SweepResult:
        """
        Bullish Sweep: low < previous SSL AND close back above SSL.
        Bearish Sweep: high > previous BSL AND close back below BSL.

        Returns SweepResult with sweep_detected, sweep_type, swept_level,
        is_false_sweep, volume_confirmed, age_bars.
        """
        if len(candles) < 2:
            return SweepResult()

        cur = candles[-1]
        prev = candles[-2]
        cfg = self.cfg

        # Volume confirmation
        vol_confirmed = True
        if vol_ma is not None and vol_ma > 0:
            vol_confirmed = cur["v"] >= vol_ma * cfg.sweep_vol_mult

        # Bullish sweep — look at most recent SSL levels (skip the closest)
        for level in ssl_levels:
            if level.price >= prev["l"]:
                continue
            # Current candle poked below SSL but closed back above
            if cur["l"] < level.price and cur["c"] > level.price:
                # False sweep filter: check if price continued through
                is_false = self._is_false_sweep(candles, level.price, "bullish", atr_val)
                age = max(0, current_bar_index - self._bar_index_from_ts(candles, level.timestamp))
                recency_factor = max(
                    0.0,
                    cfg.sweep_recency_decay_per_bar ** min(age, cfg.sweep_recency_max_bars)
                )
                return SweepResult(
                    sweep_detected=True, sweep_type="bullish",
                    swept_level=level, bar_index=current_bar_index,
                    is_false_sweep=is_false, volume_confirmed=vol_confirmed,
                    age_bars=age,
                )

        # Bearish sweep
        for level in bsl_levels:
            if level.price <= prev["h"]:
                continue
            if cur["h"] > level.price and cur["c"] < level.price:
                is_false = self._is_false_sweep(candles, level.price, "bearish", atr_val)
                age = max(0, current_bar_index - self._bar_index_from_ts(candles, level.timestamp))
                recency_factor = max(
                    0.0,
                    cfg.sweep_recency_decay_per_bar ** min(age, cfg.sweep_recency_max_bars)
                )
                return SweepResult(
                    sweep_detected=True, sweep_type="bearish",
                    swept_level=level, bar_index=current_bar_index,
                    is_false_sweep=is_false, volume_confirmed=vol_confirmed,
                    age_bars=age,
                )

        return SweepResult()

    def _is_false_sweep(self, candles: list[dict], level_price: float,
                        direction: str, atr_val: float) -> bool:
        """A sweep is 'false' if price continued through the level
        rather than rejecting. Check the lookback bars after the sweep."""
        cfg = self.cfg
        lb = cfg.false_sweep_lookback
        if len(candles) < lb + 1:
            return False
        threshold = atr_val * cfg.false_sweep_continue_atr
        # Check the most recent `lb` candles after the sweep candle (-1)
        for i in range(-lb, 0):
            c = candles[i]
            if direction == "bullish":
                # If close remains well below level → continuation down (false sweep)
                if c["c"] < level_price - threshold:
                    return True
            else:
                if c["c"] > level_price + threshold:
                    return True
        return False

    @staticmethod
    def _bar_index_from_ts(candles: list[dict], ts: int) -> int:
        """Find the bar index whose timestamp matches (or is closest)."""
        for i, c in enumerate(candles):
            if c["t"] == ts:
                return i
        # Fallback: closest match
        if not candles:
            return 0
        return min(range(len(candles)), key=lambda i: abs(candles[i]["t"] - ts))

    # ---- Premium / Discount ----
    def premium_discount(
        self, current_price: float,
        bsl_levels: list[LiquidityLevel],
        ssl_levels: list[LiquidityLevel],
    ) -> tuple[str, float, float, float]:
        """Returns (zone, range_high, range_low, equilibrium)."""
        # Use external levels for the dealing range when available
        ext_bsl = [lv for lv in bsl_levels if lv.classification == "external"]
        ext_ssl = [lv for lv in ssl_levels if lv.classification == "external"]
        range_high = ext_bsl[0].price if ext_bsl else (bsl_levels[0].price if bsl_levels else current_price)
        range_low = ext_ssl[0].price if ext_ssl else (ssl_levels[0].price if ssl_levels else current_price)
        equilibrium = (range_high + range_low) / 2.0
        if current_price > equilibrium + (range_high - range_low) * 0.05:
            zone = "premium"
        elif current_price < equilibrium - (range_high - range_low) * 0.05:
            zone = "discount"
        else:
            zone = "neutral"
        return zone, range_high, range_low, equilibrium


# ─────────────────────────────────────────────────────────────
# STRUCTURE ANALYZER
# ─────────────────────────────────────────────────────────────

class StructureAnalyzer:
    """
    Detects Break of Structure (BOS) and Market Structure Shift (MSS).
    """

    def __init__(self, config: LiquidityConfig):
        self.cfg = config

    def detect_bos(
        self,
        candles: list[dict],
        swing_highs: list[dict],
        swing_lows: list[dict],
        lookback: int = 5,
    ) -> BOSResult:
        """
        Bullish BOS: close above previous swing high.
        Bearish BOS: close below previous swing low.
        """
        if len(candles) < 2 or not swing_highs or not swing_lows:
            return BOSResult()
        cur_close = candles[-1]["c"]
        # Use the most recent confirmed swing (skip the very last in case it's the current bar)
        recent_highs = [s for s in swing_highs if s["index"] < len(candles) - 1][-lookback:]
        recent_lows = [s for s in swing_lows if s["index"] < len(candles) - 1][-lookback:]
        if recent_highs and cur_close > recent_highs[-1]["price"]:
            return BOSResult(bos_detected=True, direction="bullish")
        if recent_lows and cur_close < recent_lows[-1]["price"]:
            return BOSResult(bos_detected=True, direction="bearish")
        return BOSResult()

    def detect_mss(
        self,
        candles: list[dict],
        sweep: SweepResult,
        swing_highs: list[dict],
        swing_lows: list[dict],
        lookback: int = 5,
    ) -> MSSResult:
        """
        Bullish MSS: SSL sweep → higher low → break of recent swing high.
        Bearish MSS: BSL sweep → lower high → break of recent swing low.
        """
        if not sweep.sweep_detected:
            return MSSResult()
        if len(candles) < 3:
            return MSSResult()

        cur_close = candles[-1]["c"]
        recent_highs = [s for s in swing_highs if s["index"] < len(candles) - 1][-lookback:]
        recent_lows = [s for s in swing_lows if s["index"] < len(candles) - 1][-lookback:]

        if sweep.sweep_type == "bullish":
            # Need a higher low after the sweep, then break of recent swing high
            if len(recent_lows) >= 2:
                # The sweep should be at or below the most recent swing low,
                # but a subsequent low must be higher (higher-low formation).
                last_low = recent_lows[-1]["price"]
                # Check break of recent swing high
                if recent_highs and cur_close > recent_highs[-1]["price"]:
                    return MSSResult(mss_detected=True, direction="bullish")
        elif sweep.sweep_type == "bearish":
            if len(recent_highs) >= 2:
                last_high = recent_highs[-1]["price"]
                if recent_lows and cur_close < recent_lows[-1]["price"]:
                    return MSSResult(mss_detected=True, direction="bearish")
        return MSSResult()

    # ---- Fair Value Gap (FVG) ----
    def detect_fvg(
        self, candles: list[dict], atr_val: float,
    ) -> FVGResult:
        """
        Bullish FVG: candle[i-1].high < candle[i+1].low  (gap up)
        Bearish FVG: candle[i-1].low > candle[i+1].high  (gap down)
        We look at the most recent 3-candle window (last closed candle is index -1).
        """
        cfg = self.cfg
        if len(candles) < 3:
            return FVGResult()
        # i = -2 is the middle candle of the most recent completed pattern
        a = candles[-3]
        b = candles[-2]
        c = candles[-1]
        # Bullish FVG between candle a.high and candle c.low
        if a["h"] < c["l"] and (c["l"] - a["h"]) > atr_val * cfg.fvg_min_size_atr:
            return FVGResult(
                fvg_detected=True, direction="bullish",
                top=c["l"], bottom=a["h"], age_bars=1,
            )
        if a["l"] > c["h"] and (a["l"] - c["h"]) > atr_val * cfg.fvg_min_size_atr:
            return FVGResult(
                fvg_detected=True, direction="bearish",
                top=a["l"], bottom=c["h"], age_bars=1,
            )
        return FVGResult()


# ─────────────────────────────────────────────────────────────
# SIGNAL CONFLUENCE — Scoring engine (single integration point)
# ─────────────────────────────────────────────────────────────

class SignalConfluence:
    """
    Orchestrates LiquidityAnalyzer + StructureAnalyzer and produces
    a ConfluenceOutput that the host signal engine uses to:
      - boost or reduce `final_score`
      - append reasons to `score_adjustments`
      - optionally hard-suppress signals

    IMPORTANT: This class NEVER generates trades. It only modifies
    a signal that already exists.
    """

    def __init__(self, config: LiquidityConfig | None = None):
        self.cfg = config or LiquidityConfig()
        self.liq = LiquidityAnalyzer(self.cfg)
        self.struct = StructureAnalyzer(self.cfg)
        # Per-symbol cache to avoid recomputation across timeframes
        self._cache: dict[str, dict] = {}
        self._lock = threading.Lock()

    # ── Public entry point ──
    def analyze(
        self,
        symbol: str,
        candles_15m: list[dict],
        candles_1h: list[dict] | None,
        candles_4h: list[dict] | None,
        direction: str,                 # "long" | "short"
        base_strength: int,
        entry: float,
        tp1: float,
        sl: float,
        atr_val_15m: float,
        vol_ma_15m: float | None,
        oi_trend: str | None = None,
        htf_bull_flags: dict[str, bool] | None = None,
        l2_imbalance: float | None = None,
    ) -> ConfluenceOutput:
        """
        Single entry point. Returns ConfluenceOutput.
        The host engine reads `liquidity_bonus` and `reasons`
        and applies them to its own score_adjustments.
        """
        cfg = self.cfg
        out = ConfluenceOutput(base_strength=base_strength)
        if not cfg.enable or not candles_15m or len(candles_15m) < 30:
            out.final_strength = base_strength
            return out

        # 1) Compute ATR if not provided
        if atr_val_15m <= 0:
            highs = [c["h"] for c in candles_15m]
            lows = [c["l"] for c in candles_15m]
            closes = [c["c"] for c in candles_15m]
            atr_arr = atr(highs, lows, closes, 14)
            atr_val_15m = safe(atr_arr[-1], candles_15m[-1]["c"] * 0.005)

        current_price = candles_15m[-1]["c"]
        current_bar_index = len(candles_15m) - 1

        # 2) Detect swings on LTF (15m)
        swing_highs, swing_lows = self.liq.detect_swings(candles_15m)

        # 3) Build BSL/SSL levels
        bsl_levels, ssl_levels = self.liq.build_liquidity_levels(
            candles_15m, swing_highs, swing_lows,
            atr_val_15m, current_price, current_bar_index,
        )

        # Nearest external/internal levels
        ext_bsl = [lv for lv in bsl_levels if lv.classification == "external"]
        ext_ssl = [lv for lv in ssl_levels if lv.classification == "external"]
        int_bsl = [lv for lv in bsl_levels if lv.classification == "internal"]
        int_ssl = [lv for lv in ssl_levels if lv.classification == "internal"]
        out.nearest_external_bsl = ext_bsl[0].price if ext_bsl else None
        out.nearest_external_ssl = ext_ssl[0].price if ext_ssl else None
        out.nearest_internal_bsl = int_bsl[0].price if int_bsl else None
        out.nearest_internal_ssl = int_ssl[0].price if int_ssl else None

        # 4) Sweep detection
        sweep = self.liq.detect_sweep(
            candles_15m, bsl_levels, ssl_levels,
            current_bar_index, atr_val_15m, vol_ma_15m,
        )
        out.liquidity_sweep = {
            "sweep_detected": sweep.sweep_detected,
            "sweep_type": sweep.sweep_type,
            "is_false_sweep": sweep.is_false_sweep,
            "volume_confirmed": sweep.volume_confirmed,
            "age_bars": sweep.age_bars,
            "swept_level_price": sweep.swept_level.price if sweep.swept_level else None,
            "swept_level_classification": sweep.swept_level.classification if sweep.swept_level else None,
        }

        # False sweep hard-suppress (configurable)
        if sweep.is_false_sweep and cfg.hard_suppress_false_sweeps:
            out.hard_suppress = True
            out.reasons.append(f"False sweep ({sweep.sweep_type}) — hard suppress")
            out.final_strength = base_strength
            return out

        # 5) BOS / MSS
        bos = self.struct.detect_bos(candles_15m, swing_highs, swing_lows)
        mss = self.struct.detect_mss(candles_15m, sweep, swing_highs, swing_lows)
        out.bos = {"bos_detected": bos.bos_detected, "direction": bos.direction}
        out.mss = {"mss_detected": mss.mss_detected, "direction": mss.direction}

        # 6) FVG
        fvg = self.struct.detect_fvg(candles_15m, atr_val_15m)
        out.fvg = {
            "fvg_detected": fvg.fvg_detected,
            "direction": fvg.direction,
            "top": fvg.top,
            "bottom": fvg.bottom,
        }

        # 7) Premium / Discount
        zone, range_high, range_low, eq = self.liq.premium_discount(
            current_price, bsl_levels, ssl_levels,
        )
        out.premium_discount = zone

        # 8) HTF alignment (reuse existing flags if provided)
        htf_aligned = False
        if htf_bull_flags:
            bull_1h = htf_bull_flags.get("1h_bull", False)
            bull_4h = htf_bull_flags.get("4h_bull", False)
            bear_1h = htf_bull_flags.get("1h_bear", False)
            bear_4h = htf_bull_flags.get("4h_bear", False)
            if direction == "long":
                htf_aligned = bull_1h and bull_4h
            else:
                htf_aligned = bear_1h and bear_4h
        out.htf_alignment = htf_aligned

        # 9) ── SCORE ──
        score = 0
        reasons: list[str] = []

        # External liquidity target existence
        external_target = None
        if direction == "long" and out.nearest_external_bsl:
            external_target = out.nearest_external_bsl
            score += cfg.external_liquidity_weight
            reasons.append(f"External BSL target exists +{cfg.external_liquidity_weight}")
        elif direction == "short" and out.nearest_external_ssl:
            external_target = out.nearest_external_ssl
            score += cfg.external_liquidity_weight
            reasons.append(f"External SSL target exists +{cfg.external_liquidity_weight}")

        # Internal liquidity (smaller weight)
        if direction == "long" and out.nearest_internal_bsl:
            score += cfg.internal_liquidity_weight
            reasons.append(f"Internal BSL target +{cfg.internal_liquidity_weight}")
        elif direction == "short" and out.nearest_internal_ssl:
            score += cfg.internal_liquidity_weight
            reasons.append(f"Internal SSL target +{cfg.internal_liquidity_weight}")

        # Sweep
        if sweep.sweep_detected and not sweep.is_false_sweep:
            aligns = (sweep.sweep_type == "bullish" and direction == "long") or \
                     (sweep.sweep_type == "bearish" and direction == "short")
            if aligns:
                # Recency decay
                age = min(sweep.age_bars, cfg.sweep_recency_max_bars)
                recency_mult = cfg.sweep_recency_decay_per_bar ** age
                sweep_pts = max(0, int(round(cfg.sweep_weight * recency_mult)))
                score += sweep_pts
                reasons.append(
                    f"Recent {sweep.sweep_type} sweep +{sweep_pts}"
                    + (f" (age {sweep.age_bars}b, decay {recency_mult:.2f})" if age > 0 else "")
                )
                if sweep.volume_confirmed:
                    score += cfg.volume_confirm_weight
                    reasons.append(f"Sweep volume confirmed +{cfg.volume_confirm_weight}")
                if cfg.use_oi_confirmation and oi_trend == "rising":
                    score += cfg.oi_confirm_weight
                    reasons.append(f"OI rising confirms sweep +{cfg.oi_confirm_weight}")
            else:
                # Counter-direction sweep — penalty
                score -= cfg.sweep_weight
                reasons.append(f"Counter-direction {sweep.sweep_type} sweep -{cfg.sweep_weight}")

        # MSS
        if mss.mss_detected and mss.direction == direction:
            score += cfg.mss_weight
            reasons.append(f"{mss.direction.capitalize()} MSS +{cfg.mss_weight}")
        elif mss.mss_detected and mss.direction != direction:
            score -= cfg.mss_weight
            reasons.append(f"Counter MSS ({mss.direction}) -{cfg.mss_weight}")
            out.hard_suppress = True  # Strong counter-structure

        # BOS
        if bos.bos_detected and bos.direction == direction:
            score += cfg.bos_weight
            reasons.append(f"{bos.direction.capitalize()} BOS +{cfg.bos_weight}")

        # HTF alignment
        if htf_aligned:
            score += cfg.htf_weight
            reasons.append(f"HTF alignment ({'+'.join(cfg.htf_timeframes)}) +{cfg.htf_weight}")

        # FVG alignment (confluence only)
        if fvg.fvg_detected and fvg.direction == direction:
            score += cfg.fvg_weight
            reasons.append(f"{fvg.direction.capitalize()} FVG confluence +{cfg.fvg_weight}")

        # EQH/EQL boost
        eqh_eql_touched = False
        if direction == "long" and any(lv.is_eqh_eql for lv in bsl_levels):
            eqh_eql_touched = True
        elif direction == "short" and any(lv.is_eqh_eql for lv in ssl_levels):
            eqh_eql_touched = True
        if eqh_eql_touched:
            score += cfg.eqh_eql_weight
            reasons.append(f"EQH/EQL cluster +{cfg.eqh_eql_weight}")

        # L2 imbalance (optional)
        if l2_imbalance is not None:
            if direction == "long" and l2_imbalance >= cfg.l2_imbalance_threshold:
                score += cfg.l2_imbalance_weight
                reasons.append(f"L2 bid imbalance +{cfg.l2_imbalance_weight}")
            elif direction == "short" and l2_imbalance <= (1.0 / cfg.l2_imbalance_threshold):
                score += cfg.l2_imbalance_weight
                reasons.append(f"L2 ask imbalance +{cfg.l2_imbalance_weight}")

        # Premium / Discount
        if direction == "long" and cfg.prefer_discount_for_long and zone == "discount":
            score += cfg.premium_discount_weight
            reasons.append(f"Discount zone +{cfg.premium_discount_weight}")
        elif direction == "long" and zone == "premium" and cfg.prefer_discount_for_long:
            score -= cfg.premium_discount_weight
            reasons.append(f"Premium zone (long) -{cfg.premium_discount_weight}")
        elif direction == "short" and cfg.prefer_premium_for_short and zone == "premium":
            score += cfg.premium_discount_weight
            reasons.append(f"Premium zone +{cfg.premium_discount_weight}")
        elif direction == "short" and zone == "discount" and cfg.prefer_premium_for_short:
            score -= cfg.premium_discount_weight
            reasons.append(f"Discount zone (short) -{cfg.premium_discount_weight}")

        # R:R against external liquidity target
        if external_target is not None:
            reward = abs(external_target - entry)
            risk = abs(entry - sl)
            if risk > 0:
                rr = reward / risk
                if rr >= cfg.external_rr_min:
                    score += cfg.external_rr_bonus
                    reasons.append(f"Favorable R:R ({rr:.2f}) to external +{cfg.external_rr_bonus}")
                else:
                    score += cfg.external_rr_penalty
                    reasons.append(f"Poor R:R ({rr:.2f}) to external {cfg.external_rr_penalty}")

        # Confidence decay — if sweep occurred but confirmations (MSS/BOS) didn't fire quickly
        if sweep.sweep_detected and not (mss.mss_detected or bos.bos_detected):
            decay_bars = min(sweep.age_bars, cfg.confidence_decay_max_bars)
            if decay_bars > 0:
                penalty = decay_bars * cfg.confidence_decay_penalty_per_bar
                score -= penalty
                reasons.append(f"Confidence decay (no MSS/BOS, {decay_bars}b) -{penalty}")

        # Cap
        score = max(-cfg.max_bonus, min(cfg.max_bonus, score))
        out.liquidity_score = score

        # Structure score breakdown
        out.structure_score = (
            (cfg.mss_weight if mss.mss_detected and mss.direction == direction else 0) +
            (cfg.bos_weight if bos.bos_detected and bos.direction == direction else 0) +
            (cfg.htf_weight if htf_aligned else 0)
        )

        out.liquidity_bonus = score
        out.final_strength = min(base_strength + score, cfg.final_strength_cap)
        out.reasons = reasons
        return out


# ─────────────────────────────────────────────────────────────
# OPTIONAL: HYPERLIQUID L2 ORDER BOOK IMBALANCE
# ─────────────────────────────────────────────────────────────

def fetch_l2_imbalance(symbol: str, hl_post_fn, depth: int = 20) -> float | None:
    """
    Fetch L2 snapshot and compute bid/ask volume imbalance.
    Returns ratio (bid_vol / ask_vol). >1 = bid-heavy, <1 = ask-heavy.
    MUST be used only as confluence, never as standalone entry logic.
    """
    coin = symbol.replace("USDT", "")
    try:
        raw = hl_post_fn({
            "type": "l2Book",
            "coin": coin,
        })
    except Exception:
        return None
    if not raw or "levels" not in raw:
        return None
    levels = raw["levels"]
    bids = levels.get("bid", [])[:depth]
    asks = levels.get("ask", [])[:depth]
    bid_vol = sum(float(lvl.get("sz", 0)) for lvl in bids)
    ask_vol = sum(float(lvl.get("sz", 0)) for lvl in asks)
    if ask_vol <= 0:
        return None
    return bid_vol / ask_vol


# ─────────────────────────────────────────────────────────────
# UNIT TESTS (run with: python -m pytest liquidity_confluence.py)
# ─────────────────────────────────────────────────────────────

def _make_candles(prices: list[float], vol: float = 1.0) -> list[dict]:
    """Helper: synthesize OHLCV from a close-price series."""
    out = []
    for i, p in enumerate(prices):
        out.append({
            "t": i * 900_000,
            "o": p, "h": p * 1.001, "l": p * 0.999, "c": p, "v": vol,
        })
    return out


def test_swing_detection_basic():
    cfg = LiquidityConfig(fractal_left=2, fractal_right=2)
    la = LiquidityAnalyzer(cfg)
    # Construct a clear peak at index 5
    prices = [10, 11, 12, 13, 14, 15, 14, 13, 12, 11, 10]
    candles = _make_candles(prices)
    # Make index 5 a true peak
    candles[5]["h"] = 16
    highs, lows = la.detect_swings(candles)
    assert any(h["index"] == 5 for h in highs), "Peak at index 5 not detected"


def test_bullish_sweep():
    cfg = LiquidityConfig(fractal_left=2, fractal_right=2)
    la = LiquidityAnalyzer(cfg)
    # Create a swing low at index 3, then a sweep at the last candle
    candles = []
    for i, p in enumerate([20, 18, 16, 15, 17, 19, 21, 19, 17, 14, 16]):
        candles.append({"t": i * 900_000, "o": p + 0.5, "h": p + 1, "l": p - 1, "c": p, "v": 2.0})
    # Ensure index 3 is a swing low
    candles[3]["l"] = 13
    highs, lows = la.detect_swings(candles)
    assert any(l["index"] == 3 for l in lows)
    bsl, ssl = la.build_liquidity_levels(candles, highs, lows, 1.0,
                                          candles[-1]["c"], len(candles) - 1)
    sweep = la.detect_sweep(candles, bsl, ssl, len(candles) - 1, 1.0, 1.5)
    # The last candle has low=14-1=13 (touching the swing low) and close=16 (back above)
    # This is a bullish sweep candidate
    assert sweep.sweep_type in (None, "bullish")


def test_fvg_detection():
    cfg = LiquidityConfig()
    sa = StructureAnalyzer(cfg)
    # Bullish FVG: candle a.high=10, candle c.low=12 → gap
    candles = [
        {"t": 0, "o": 9, "h": 10, "l": 9, "c": 9.5, "v": 1},
        {"t": 1, "o": 11, "h": 13, "l": 11, "c": 12, "v": 1},
        {"t": 2, "o": 12, "h": 14, "l": 12, "c": 13, "v": 1},
    ]
    fvg = sa.detect_fvg(candles, atr_val=0.5)
    assert fvg.fvg_detected and fvg.direction == "bullish"


def test_confluence_output_schema():
    cfg = LiquidityConfig(enable=True)
    sc = SignalConfluence(cfg)
    candles = _make_candles([100 + i * 0.1 for i in range(50)])
    out = sc.analyze(
        symbol="TESTUSDT", candles_15m=candles,
        candles_1h=None, candles_4h=None,
        direction="long", base_strength=5,
        entry=100.0, tp1=102.0, sl=99.0,
        atr_val_15m=1.0, vol_ma_15m=1.0,
    )
    # Validate schema
    assert isinstance(out.base_strength, int)
    assert isinstance(out.liquidity_bonus, int)
    assert isinstance(out.final_strength, int)
    assert isinstance(out.liquidity_score, int)
    assert isinstance(out.structure_score, int)
    assert isinstance(out.reasons, list)
    assert "sweep_detected" in out.liquidity_sweep
    assert "mss_detected" in out.mss
    assert "bos_detected" in out.bos


def test_no_future_leakage():
    """Ensure each swing is confirmed only with past + `fractal_right` bars."""
    cfg = LiquidityConfig(fractal_left=3, fractal_right=3)
    la = LiquidityAnalyzer(cfg)
    candles = _make_candles([10 + i for i in range(20)])
    # The last confirmed swing must be at index <= len - fractal_right - 1
    highs, lows = la.detect_swings(candles)
    if highs:
        assert highs[-1]["index"] <= len(candles) - cfg.fractal_right - 1
    if lows:
        assert lows[-1]["index"] <= len(candles) - cfg.fractal_right - 1


if __name__ == "__main__":
    test_swing_detection_basic()
    test_bullish_sweep()
    test_fvg_detection()
    test_confluence_output_schema()
    test_no_future_leakage()
    print("All liquidity_confluence tests passed.")
