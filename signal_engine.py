# ══════════════════════════════════════════════════════════════════════════
#  AEGIS — Adaptive Multi-Engine Institutional Signal Intelligence
#  v1.0.0
#
#  Philosophy: no single strategy survives every regime. AEGIS runs a bank
#  of independent specialized engines (SMC Order Block, Breaker Block, Fair
#  Value Gap, Liquidity Sweep, Trend Continuation, Breakout, Pullback and
#  Range Mean-Reversion) against every symbol in parallel, then hands every
#  candidate to a centralized Decision Engine that scores it on regime fit,
#  multi-timeframe alignment, liquidity, volatility, confluence and each
#  engine's own live-tracked historical expectancy — combined with adaptive,
#  not fixed, per-engine weights. A Learning Store closes the loop: every
#  completed trade is attributed back to the engine and regime that produced
#  it, updating win-rate, average R and confidence calibration, which in
#  turn reshapes future weighting — bounded so a short losing streak cannot
#  whipsaw the system into overfitting.
# ══════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import os
import sys
import json
import math
import time
import threading
import signal as os_signal
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

ENGINE_NAME = "AEGIS"
__version__ = "1.0.0"

# ══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════

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

WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]
MAJORS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"}

# ── TIMEFRAME STACK (nothing below 15m, per spec) ──────────────────────────
# MACRO 1D -> long bias, premium/discount range, PDH/PDL/PWH/PWL
# HTF   4H -> primary structure, zone map (OB/Breaker/FVG)
# MID   1H -> liquidity sweep + intermediate structure shift (CHoCH/BOS)
# LTF  15m -> execution trigger, entry timing, MSS confirmation
TF_MACRO, TF_HTF, TF_MID, TF_LTF = "1d", "4h", "1h", "15m"
TF_BARS = {TF_MACRO: 120, TF_HTF: 260, TF_MID: 300, TF_LTF: 300}
SCAN_INTERVAL_MIN = 15

EMA_FAST, EMA_SLOW, EMA_TREND = 21, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, BB_LEN = 14, 14, 14, 20
BB_MULT = 2.0

# ── ZONE / SMC DETECTION ─────────────────────────────────────────────────
OB_DISPLACEMENT_ATR_MULT = 1.15
OB_BOS_LOOKBACK = 25
FVG_MIN_GAP_ATR_MULT = 0.12
ZONE_MAX_WIDTH_ATR_MULT = 1.8
ZONE_LOOKBACK_HTF = 90
ZONE_LOOKBACK_LTF = 80
PIVOT_LEFT, PIVOT_RIGHT = 2, 2
LIQUIDITY_EQ_TOLERANCE_PCT = 0.0018

SWEEP_LOOKBACK_MID = 16
SWEEP_MAX_DEPTH_ATR_MULT = 1.10
SWEEP_MIN_WICK_RATIO = 0.35
MSS_LOOKBACK_LTF = 40
MSS_DISPLACEMENT_ATR_MULT = 0.55
MSS_MIN_CLOSE_MARGIN_ATR_MULT = 0.08
BREAKER_SEARCH_BARS = 8

# ── RISK ─────────────────────────────────────────────────────────────────
MIN_RR_FLOOR = 1.5
MIN_RR_TARGET = 2.0
EXT_RR_LEVELS = [2.0, 2.5, 3.0, 4.0, 5.0]
SL_BUFFER_ATR_MIN_MULT = 0.25
SL_BUFFER_ATR_MAX_MULT = 0.85
LIQUIDITY_ROOM_BUFFER_ATR_MULT = 0.25
POI_MAX_DIST_ATR_MULT = 1.4
POI_MAX_PCT_OF_PRICE = 0.02

# ── VOLATILITY / LIQUIDITY GATES ────────────────────────────────────────
MIN_ATR_PCT = 0.20
MAX_ATR_PCT = 9.0
SPREAD_WARN_PCT = 0.20
SPREAD_SUPPRESS_PCT = 0.45
SPREAD_EXEMPT = MAJORS
MIN_OI_USD = 400_000.0

# ── SCORING / FREQUENCY GOVERNOR ─────────────────────────────────────────
BASE_MIN_CONFIDENCE = 58.0
MAX_SIGNALS_PER_SCAN_DEFAULT = 5
MAX_SIGNALS_PER_SCAN_TRENDING = 8
MAX_CONCURRENT_ACTIVE_SIGNALS = 16
MAX_SIGNAL_HISTORY = 2000
COOLDOWN_BARS_LTF = 3
DUPLICATE_ENTRY_TOLERANCE_PCT = 0.0035

GOVERNOR_LOOKBACK_SIGNALS = 40
GOVERNOR_MAX_SHIFT = 8.0
GOVERNOR_TARGET_WINRATE = 0.50

# ── ADAPTIVE ENGINE WEIGHTING (learning store) ──────────────────────────
ENGINE_NAMES = [
    "smc_order_block", "smc_breaker", "smc_fvg", "liquidity_sweep",
    "trend_continuation", "breakout", "pullback", "mean_reversion_range",
]
WEIGHT_MIN, WEIGHT_MAX = 0.55, 1.55
WEIGHT_LOOKBACK_TRADES = 30
WEIGHT_LEARNING_RATE = 0.10   # bounded nudge per weight-update cycle

SESSION_WINDOWS = {"asia": (0, 8), "london": (7, 12), "ny": (12, 21), "off": (21, 24)}
SESSION_SCORE_BONUS = {"asia": 0.0, "london": 2.0, "ny": 2.5, "off": -1.5}

# ══════════════════════════════════════════════════════════════════════════
#  HYPERLIQUID API CLIENT  (shared cache, throttled, retried)
# ══════════════════════════════════════════════════════════════════════════

_session = requests.Session()
_req_lock = threading.Lock()
_last_req_ts = 0.0


def _throttle():
    global _last_req_ts
    with _req_lock:
        wait = HL_MIN_INTERVAL_S - (time.time() - _last_req_ts)
        if wait > 0:
            time.sleep(wait)
        _last_req_ts = time.time()


def hl_post(payload: dict, retries: int = 4, timeout: int = 12):
    for attempt in range(retries):
        _throttle()
        try:
            r = _session.post(HL_BASE_URL, json=payload, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 502, 503, 504):
                time.sleep(0.5 * (attempt + 1))
                continue
            return None
        except requests.RequestException:
            time.sleep(0.4 * (attempt + 1))
    return None


def hl_coin(symbol: str) -> str:
    return symbol[:-4] if symbol.endswith("USDT") else symbol


_INTERVAL_MIN = {"15m": 15, "1h": 60, "4h": 240, "1d": 1440}


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    mult = _INTERVAL_MIN[interval] * 60_000
    return (reference_ms // mult) * mult


def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    open_ms = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < open_ms]


def get_candles(symbol: str, interval: str, n: int, reference_ms: int) -> Optional[list[dict]]:
    coin = hl_coin(symbol)
    end = reference_ms
    start = end - n * _INTERVAL_MIN[interval] * 60_000 * 2
    payload = {"type": "candleSnapshot", "req": {"coin": coin, "interval": interval, "startTime": start, "endTime": end}}
    raw = hl_post(payload)
    if not raw or not isinstance(raw, list):
        return None
    candles = [{"t": c["t"], "o": float(c["o"]), "h": float(c["h"]),
                "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])} for c in raw]
    candles = filter_closed_candles(candles, interval, reference_ms)
    return candles[-n:] if len(candles) >= 5 else None


def fetch_all_candles(symbol: str, reference_ms: int) -> Optional[dict[str, list[dict]]]:
    out = {}
    for tf in (TF_LTF, TF_MID, TF_HTF, TF_MACRO):
        c = get_candles(symbol, tf, TF_BARS[tf], reference_ms)
        if c is None or len(c) < 40:
            return None
        out[tf] = c
    return out


_meta_ctx_cache: Optional[dict] = None
_meta_ctx_lock = threading.Lock()


def get_meta_and_asset_ctxs() -> Optional[dict]:
    global _meta_ctx_cache
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or not isinstance(raw, list) or len(raw) < 2:
        return _meta_ctx_cache
    universe = raw[0].get("universe", [])
    ctxs = raw[1]
    out = {}
    for meta, ctx in zip(universe, ctxs):
        name = meta.get("name")
        if not name:
            continue
        try:
            out[name] = {
                "funding": float(ctx.get("funding", 0.0)),
                "oi": float(ctx.get("openInterest", 0.0)),
                "mark_px": float(ctx.get("markPx", 0.0)),
                "day_vol_usd": float(ctx.get("dayNtlVlm", 0.0)),
            }
        except (TypeError, ValueError):
            continue
    with _meta_ctx_lock:
        _meta_ctx_cache = out
    return out


def get_l2_spread_pct(symbol: str) -> Optional[float]:
    raw = hl_post({"type": "l2Book", "coin": hl_coin(symbol)})
    if not raw or "levels" not in raw:
        return None
    try:
        bids, asks = raw["levels"][0], raw["levels"][1]
        if not bids or not asks:
            return None
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2
        return ((best_ask - best_bid) / mid) * 100 if mid > 0 else None
    except (KeyError, IndexError, ValueError, TypeError):
        return None

# ══════════════════════════════════════════════════════════════════════════
#  MATH / INDICATORS
# ══════════════════════════════════════════════════════════════════════════

def safe(v, fb=0.0):
    try:
        if v is None or math.isnan(v) or math.isinf(v):
            return fb
        return v
    except TypeError:
        return fb


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


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
        window = vals[max(0, i - period + 1): i + 1]
        out.append(sum(window) / len(window))
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1): i + 1]
        m = sum(window) / len(window)
        out.append(math.sqrt(sum((x - m) ** 2 for x in window) / len(window)))
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g, avg_l = gains[0], losses[0]
    out = [50.0]
    for i in range(1, len(closes)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = safe_div(avg_g, avg_l, 999.0)
        out.append(100 - 100 / (1 + rs) if avg_l > 0 else 100.0)
    return out


def atr_series(candles: list[dict], period: int = ATR_LEN) -> list[float]:
    if not candles:
        return []
    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    out = [trs[0]]
    for i in range(1, len(trs)):
        out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def adx_series(candles: list[dict], period: int = ADX_LEN) -> list[float]:
    n = len(candles)
    if n < period + 2:
        return [15.0] * n
    plus_dm, minus_dm, trs = [0.0], [0.0], [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        dn = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    def wilder(vals):
        out = [sum(vals[:period])]
        for v in vals[period:]:
            out.append(out[-1] - out[-1] / period + v)
        return out

    tr_s, pdm_s, mdm_s = wilder(trs), wilder(plus_dm), wilder(minus_dm)
    dx = []
    for i in range(len(tr_s)):
        pdi = 100 * safe_div(pdm_s[i], tr_s[i])
        mdi = 100 * safe_div(mdm_s[i], tr_s[i])
        dx.append(100 * safe_div(abs(pdi - mdi), pdi + mdi))
    pad = [dx[0] if dx else 15.0] * (n - len(dx))
    return sma(pad + dx, period)


def bb_width_pct(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    out = []
    for i in range(len(closes)):
        upper, lower = mid[i] + mult * sd[i], mid[i] - mult * sd[i]
        out.append(safe_div(upper - lower, mid[i]) * 100 if mid[i] else 0.0)
    return out


def percentile_rank(vals: list[float], x: float) -> float:
    if not vals:
        return 0.5
    below = sum(1 for v in vals if v <= x)
    return below / len(vals)


def obv_series(closes: list[float], volumes: list[float]) -> list[float]:
    out = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            out.append(out[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            out.append(out[-1] - volumes[i])
        else:
            out.append(out[-1])
    return out


def _avg_volume(candles: list[dict], idx: int, window: int = 20) -> float:
    lo = max(0, idx - window)
    seg = candles[lo:idx]
    return (sum(c["v"] for c in seg) / len(seg)) if seg else (candles[idx]["v"] or 1.0)


class Indicators:
    """Computed once per (symbol, timeframe) per scan and shared by every
    specialized engine — avoids the classic mistake of eight engines each
    recomputing ATR/RSI/ADX independently."""

    def __init__(self, candles: list[dict]):
        closes = [c["c"] for c in candles]
        vols = [c["v"] for c in candles]
        self.candles = candles
        self.closes = closes
        self.ema_fast = ema(closes, EMA_FAST)
        self.ema_slow = ema(closes, EMA_SLOW)
        self.ema_trend = ema(closes, min(EMA_TREND, max(2, len(closes) - 1)))
        self.rsi = rsi(closes, RSI_LEN)
        self.atr = atr_series(candles, ATR_LEN)
        self.adx = adx_series(candles, ADX_LEN)
        self.bbw = bb_width_pct(closes, BB_LEN, BB_MULT)
        self.obv = obv_series(closes, vols)

    @property
    def last_atr(self) -> float:
        return self.atr[-1] if self.atr else 0.0

    @property
    def last_close(self) -> float:
        return self.closes[-1] if self.closes else 0.0

# ══════════════════════════════════════════════════════════════════════════
#  STRUCTURE / SWINGS / REGIME
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class Swing:
    index: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list[dict], left: int = PIVOT_LEFT, right: int = PIVOT_RIGHT) -> list[Swing]:
    out = []
    n = len(candles)
    for i in range(left, n - right):
        seg_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        seg_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        if candles[i]["h"] == max(seg_h):
            out.append(Swing(i, candles[i]["h"], "high"))
        if candles[i]["l"] == min(seg_l):
            out.append(Swing(i, candles[i]["l"], "low"))
    return out


@dataclass
class StructureState:
    bias: str          # "bullish" | "bearish" | "neutral"
    range_high: float
    range_low: float
    eq: float
    last_bos_index: int = -1
    last_choch_index: int = -1


def analyze_structure(candles: list[dict], swings: list[Swing]) -> Optional[StructureState]:
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return None

    def _recent_bias(points: list[Swing]) -> str:
        if len(points) < 2:
            return "neutral"
        window = points[-5:]
        last = window[-1].price
        ref = sorted(p.price for p in window[:-1])
        ref_mid = ref[len(ref) // 2]
        if last > ref_mid:
            return "up"
        if last < ref_mid:
            return "down"
        return "flat"

    high_dir = _recent_bias(highs)
    low_dir = _recent_bias(lows)
    if high_dir == "up" and low_dir == "up":
        bias = "bullish"
    elif high_dir == "down" and low_dir == "down":
        bias = "bearish"
    else:
        bias = "neutral"
    range_high = max(h.price for h in highs[-6:])
    range_low = min(l.price for l in lows[-6:])
    if range_high <= range_low:
        return None
    return StructureState(bias, range_high, range_low, (range_high + range_low) / 2)


def detect_bos_choch(candles: list[dict], swings: list[Swing], lookback: int = 40) -> dict:
    """Returns latest BOS/CHoCH classification over the trailing window:
    BOS = structure break in the direction of prevailing bias (continuation).
    CHoCH = structure break against prevailing bias (potential reversal)."""
    highs = sorted([s for s in swings if s.kind == "high"], key=lambda s: s.index)
    lows = sorted([s for s in swings if s.kind == "low"], key=lambda s: s.index)
    if len(highs) < 2 or len(lows) < 2:
        return {"event": None, "direction": None, "index": -1}
    n = len(candles)
    start = max(0, n - lookback)
    prior_bias = "bullish" if highs[-2].price < highs[-1].price else "bearish"
    for i in range(start, n):
        c = candles[i]
        if c["c"] > highs[-1].price and prior_bias != "bearish_break":
            return {"event": "BOS" if prior_bias == "bullish" else "CHoCH", "direction": "bullish", "index": i}
        if c["c"] < lows[-1].price and prior_bias != "bullish_break":
            return {"event": "BOS" if prior_bias == "bearish" else "CHoCH", "direction": "bearish", "index": i}
    return {"event": None, "direction": None, "index": -1}


def price_zone(price: float, structure: StructureState) -> str:
    return "premium" if price >= structure.eq else "discount"


@dataclass
class Regime:
    label: str          # "trend" | "range" | "reversal" | "volatile"
    direction: str       # "bullish" | "bearish" | "neutral"
    adx: float
    bbw_pctile: float
    atr_pct: float
    strength: float


def classify_regime(candles_htf: list[dict], candles_mid: list[dict]) -> Regime:
    closes_htf = [c["c"] for c in candles_htf]
    adx_htf = adx_series(candles_htf, ADX_LEN)[-1]
    bbw = bb_width_pct(closes_htf, BB_LEN, BB_MULT)
    bbw_pctile = percentile_rank(bbw[-60:], bbw[-1])
    atr_htf = atr_series(candles_htf, ATR_LEN)[-1]
    atr_pct = safe_div(atr_htf, candles_htf[-1]["c"]) * 100

    ema_fast_v = ema(closes_htf, EMA_FAST)[-1]
    ema_slow_v = ema(closes_htf, EMA_SLOW)[-1]
    ema_trend_v = ema(closes_htf, min(EMA_TREND, len(closes_htf) - 1))[-1]
    price = closes_htf[-1]

    if price > ema_fast_v > ema_slow_v > ema_trend_v:
        direction, align = "bullish", 1.0
    elif price < ema_fast_v < ema_slow_v < ema_trend_v:
        direction, align = "bearish", 1.0
    elif price > ema_slow_v:
        direction, align = "bullish", 0.5
    elif price < ema_slow_v:
        direction, align = "bearish", 0.5
    else:
        direction, align = "neutral", 0.0

    swings_mid = find_swings(candles_mid)
    structure = analyze_structure(candles_mid, swings_mid)
    struct_bias = structure.bias if structure else "neutral"
    strength = min(1.0, (adx_htf / 40.0) * 0.6 + align * 0.4)

    if adx_htf >= 24 and align >= 0.5 and struct_bias == direction:
        label = "trend"
    elif adx_htf < 18 and bbw_pctile < 0.45:
        label = "range"
    elif bbw_pctile > 0.85 or atr_pct > MAX_ATR_PCT * 0.7:
        label = "volatile"
    else:
        label = "reversal"

    return Regime(label, direction, adx_htf, bbw_pctile, atr_pct, strength)

# ══════════════════════════════════════════════════════════════════════════
#  SMC ZONE ENGINE — ORDER BLOCKS / BREAKER BLOCKS / FVGs / LIQUIDITY
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class Zone:
    low: float
    high: float
    kind: str            # "demand" | "supply"
    origin: str           # "ob" | "breaker" | "fvg"
    index: int
    displacement_atr: float = 0.0
    vol_ratio: float = 1.0
    mitigated: bool = False
    flipped: bool = False

    @property
    def mid(self) -> float:
        return (self.low + self.high) / 2

    @property
    def width(self) -> float:
        return self.high - self.low

    def contains(self, price: float, buf: float = 0.0) -> bool:
        return (self.low - buf) <= price <= (self.high + buf)


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int) -> list[Zone]:
    zones = []
    n = len(candles)
    start = max(2, n - lookback)
    for i in range(start, n):
        c = candles[i]
        a = atr_vals[i] or 1e-9
        body = c["c"] - c["o"]
        if abs(body) < OB_DISPLACEMENT_ATR_MULT * a:
            continue
        back_lo = max(0, i - OB_BOS_LOOKBACK)
        if body > 0:
            prior_high = max((candles[j]["h"] for j in range(back_lo, i)), default=c["h"])
            if c["c"] <= prior_high:
                continue
            for j in range(i - 1, back_lo - 1, -1):
                ob = candles[j]
                if ob["c"] < ob["o"]:
                    zones.append(Zone(ob["l"], ob["h"], "demand", "ob", j,
                                       displacement_atr=abs(body) / a,
                                       vol_ratio=safe_div(c["v"], _avg_volume(candles, i), 1.0)))
                    break
        else:
            prior_low = min((candles[j]["l"] for j in range(back_lo, i)), default=c["l"])
            if c["c"] >= prior_low:
                continue
            for j in range(i - 1, back_lo - 1, -1):
                ob = candles[j]
                if ob["c"] > ob["o"]:
                    zones.append(Zone(ob["l"], ob["h"], "supply", "ob", j,
                                       displacement_atr=abs(body) / a,
                                       vol_ratio=safe_div(c["v"], _avg_volume(candles, i), 1.0)))
                    break
    atr_last = atr_vals[-1] or 1e-9
    zones = [z for z in zones if z.width <= ZONE_MAX_WIDTH_ATR_MULT * atr_last]
    return zones[-14:]


def find_fvgs(candles: list[dict], atr_vals: list[float], lookback: int) -> list[Zone]:
    zones = []
    n = len(candles)
    start = max(2, n - lookback)
    for i in range(start, n):
        a, b, c = candles[i - 2], candles[i - 1], candles[i]
        atrv = atr_vals[i] or 1e-9
        if a["h"] < c["l"] and (c["l"] - a["h"]) >= FVG_MIN_GAP_ATR_MULT * atrv:
            zones.append(Zone(a["h"], c["l"], "demand", "fvg", i - 1,
                               displacement_atr=abs(b["c"] - b["o"]) / atrv,
                               vol_ratio=safe_div(b["v"], _avg_volume(candles, i - 1), 1.0)))
        elif a["l"] > c["h"] and (a["l"] - c["h"]) >= FVG_MIN_GAP_ATR_MULT * atrv:
            zones.append(Zone(c["h"], a["l"], "supply", "fvg", i - 1,
                               displacement_atr=abs(b["c"] - b["o"]) / atrv,
                               vol_ratio=safe_div(b["v"], _avg_volume(candles, i - 1), 1.0)))
    return zones[-14:]


def mark_mitigation_and_breakers(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    out = []
    for z in zones:
        zc = Zone(z.low, z.high, z.kind, z.origin, z.index, z.displacement_atr, z.vol_ratio)
        for c in candles[z.index + 1:]:
            touched = c["l"] <= zc.high and c["h"] >= zc.low
            if not touched:
                continue
            closed_through = (c["c"] > zc.high) if zc.kind == "supply" else (c["c"] < zc.low)
            if closed_through:
                zc.mitigated = True
                if zc.origin == "ob":
                    zc.flipped = True
                    zc.kind = "demand" if zc.kind == "supply" else "supply"
                    zc.origin = "breaker"
                break
        out.append(zc)
    return out


def zone_quality(z: Zone) -> float:
    disp_score = min(1.0, z.displacement_atr / 2.2)
    vol_score = min(1.0, z.vol_ratio / 1.8)
    fresh_score = 0.0 if (z.mitigated and not z.flipped) else 1.0
    tight_score = 0.7
    origin_bonus = 0.12 if z.origin in ("breaker", "fvg") else 0.0
    q = 0.35 * disp_score + 0.25 * vol_score + 0.20 * fresh_score + 0.20 * tight_score + origin_bonus
    return max(0.0, min(1.0, q))


def cluster_levels(levels: list[float], tol_pct: float = LIQUIDITY_EQ_TOLERANCE_PCT) -> list[tuple[float, int]]:
    if not levels:
        return []
    levels = sorted(levels)
    clusters, cur = [], [levels[0]]
    for v in levels[1:]:
        if abs(v - cur[-1]) / max(cur[-1], 1e-9) <= tol_pct:
            cur.append(v)
        else:
            clusters.append((sum(cur) / len(cur), len(cur)))
            cur = [v]
    clusters.append((sum(cur) / len(cur), len(cur)))
    return clusters


def build_liquidity_pools(swings: list[Swing], candles_macro: list[dict]) -> dict:
    highs = cluster_levels([s.price for s in swings if s.kind == "high"])
    lows = cluster_levels([s.price for s in swings if s.kind == "low"])
    pdh = candles_macro[-2]["h"] if len(candles_macro) > 1 else candles_macro[-1]["h"]
    pdl = candles_macro[-2]["l"] if len(candles_macro) > 1 else candles_macro[-1]["l"]
    pwh = max((c["h"] for c in candles_macro[-7:]), default=pdh)
    pwl = min((c["l"] for c in candles_macro[-7:]), default=pdl)
    return {"eq_highs": highs, "eq_lows": lows, "pdh": pdh, "pdl": pdl, "pwh": pwh, "pwl": pwl}


def detect_sweep(candles: list[dict], pools: dict, direction: str, atr_val: float,
                  lookback: int = SWEEP_LOOKBACK_MID) -> Optional[dict]:
    """A sweep = a wick pierces a liquidity level beyond it, but the body
    closes back inside — evidence that resting stops/liquidity were taken
    before institutional order flow reversed."""
    seg = candles[-lookback:]
    levels = ([p for p, _ in pools["eq_highs"]] + [pools["pdh"], pools["pwh"]]) if direction == "bearish" else \
             ([p for p, _ in pools["eq_lows"]] + [pools["pdl"], pools["pwl"]])
    for c in reversed(seg):
        rng = max(c["h"] - c["l"], 1e-9)
        if direction == "bearish":
            for lvl in levels:
                if c["h"] > lvl and c["c"] < lvl:
                    wick_ratio = (c["h"] - max(c["o"], c["c"])) / rng
                    depth = (c["h"] - lvl) / (atr_val or 1e-9)
                    if wick_ratio >= SWEEP_MIN_WICK_RATIO and depth <= SWEEP_MAX_DEPTH_ATR_MULT:
                        return {"level": lvl, "wick_ratio": wick_ratio, "depth_atr": depth, "bar_high": c["h"]}
        else:
            for lvl in levels:
                if c["l"] < lvl and c["c"] > lvl:
                    wick_ratio = (min(c["o"], c["c"]) - c["l"]) / rng
                    depth = (lvl - c["l"]) / (atr_val or 1e-9)
                    if wick_ratio >= SWEEP_MIN_WICK_RATIO and depth <= SWEEP_MAX_DEPTH_ATR_MULT:
                        return {"level": lvl, "wick_ratio": wick_ratio, "depth_atr": depth, "bar_low": c["l"]}
    return None


def detect_mss(candles_exec: list[dict], direction: str, atr_val: float,
                lookback: int = MSS_LOOKBACK_LTF) -> Optional[dict]:
    """Market Structure Shift on the execution timeframe: a displacement
    candle closing beyond the most recent opposing swing, with margin."""
    swings = find_swings(candles_exec[-lookback:])
    if not swings:
        return None
    seg = candles_exec[-lookback:]
    if direction == "bullish":
        recent_highs = [s.price for s in swings if s.kind == "high"]
        if not recent_highs:
            return None
        trigger = max(recent_highs)
        for i, c in enumerate(seg):
            body = c["c"] - c["o"]
            if c["c"] > trigger + MSS_MIN_CLOSE_MARGIN_ATR_MULT * atr_val and body > MSS_DISPLACEMENT_ATR_MULT * atr_val:
                return {"index": i, "trigger": trigger, "close": c["c"]}
    else:
        recent_lows = [s.price for s in swings if s.kind == "low"]
        if not recent_lows:
            return None
        trigger = min(recent_lows)
        for i, c in enumerate(seg):
            body = c["o"] - c["c"]
            if c["c"] < trigger - MSS_MIN_CLOSE_MARGIN_ATR_MULT * atr_val and body > MSS_DISPLACEMENT_ATR_MULT * atr_val:
                return {"index": i, "trigger": trigger, "close": c["c"]}
    return None


def find_breaker_after_mss(candles_exec: list[dict], direction: str) -> Optional[Zone]:
    seg = candles_exec[-BREAKER_SEARCH_BARS:]
    for c in reversed(seg):
        if direction == "bullish" and c["c"] < c["o"]:
            return Zone(c["l"], c["h"], "demand", "breaker", len(candles_exec) - 1)
        if direction == "bearish" and c["c"] > c["o"]:
            return Zone(c["h"], c["l"] if False else c["l"], "supply", "breaker", len(candles_exec) - 1)
    return None


def volume_profile(candles: list[dict], bins: int = 24) -> dict:
    if not candles:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0}
    lo = min(c["l"] for c in candles)
    hi = max(c["h"] for c in candles)
    if hi <= lo:
        return {"poc": candles[-1]["c"], "vah": hi, "val": lo}
    width = (hi - lo) / bins
    vol_bins = [0.0] * bins
    for c in candles:
        mid = (c["h"] + c["l"]) / 2
        idx = min(bins - 1, max(0, int((mid - lo) / width)))
        vol_bins[idx] += c["v"]
    poc_idx = vol_bins.index(max(vol_bins))
    poc = lo + (poc_idx + 0.5) * width
    total = sum(vol_bins) or 1.0
    order = sorted(range(bins), key=lambda i: vol_bins[i], reverse=True)
    acc, chosen = 0.0, []
    for i in order:
        acc += vol_bins[i]
        chosen.append(i)
        if acc / total >= 0.70:
            break
    vah = lo + (max(chosen) + 1) * width
    val = lo + min(chosen) * width
    return {"poc": poc, "vah": vah, "val": val}

# ══════════════════════════════════════════════════════════════════════════
#  CANDIDATE MODEL
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class Candidate:
    engine: str            # one of ENGINE_NAMES — attribution for learning
    symbol: str
    direction: str          # "long" | "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    zone: Optional[Zone]
    raw_score: float        # 0-1 engine-local quality before ensemble scoring
    atr_val: float
    tags: list = field(default_factory=list)

    @property
    def rr1(self) -> float:
        risk = abs(self.entry - self.sl) or 1e-9
        return abs(self.tp1 - self.entry) / risk

    @property
    def rr2(self) -> float:
        risk = abs(self.entry - self.sl) or 1e-9
        return abs(self.tp2 - self.entry) / risk


def _sl_buffer(atr_val: float, zone_q: float) -> float:
    # tighter buffer for higher-quality zones, wider when the zone is shaky
    mult = SL_BUFFER_ATR_MAX_MULT - zone_q * (SL_BUFFER_ATR_MAX_MULT - SL_BUFFER_ATR_MIN_MULT)
    return atr_val * mult


def _build_targets(entry: float, sl: float, direction: str, pools: dict, vp: dict) -> tuple[float, float]:
    risk = abs(entry - sl)
    tp1 = entry + risk * MIN_RR_TARGET if direction == "long" else entry - risk * MIN_RR_TARGET
    ext = risk * EXT_RR_LEVELS[2]
    tp2 = entry + ext if direction == "long" else entry - ext
    # clip TP2 to the nearest real liquidity pool / volume node so we don't
    # target thin air past where price realistically travels
    candidates = [pools.get("pdh"), pools.get("pwh"), vp.get("vah")] if direction == "long" else \
                 [pools.get("pdl"), pools.get("pwl"), vp.get("val")]
    candidates = [c for c in candidates if c and ((c > entry) if direction == "long" else (c < entry))]
    if candidates:
        nearest = min(candidates, key=lambda c: abs(c - entry))
        buffered = nearest - risk * LIQUIDITY_ROOM_BUFFER_ATR_MULT if direction == "long" else \
                   nearest + risk * LIQUIDITY_ROOM_BUFFER_ATR_MULT
        if direction == "long":
            tp2 = min(tp2, max(buffered, tp1))
        else:
            tp2 = max(tp2, min(buffered, tp1))
    return tp1, tp2


@dataclass
class MarketBundle:
    symbol: str
    candles: dict           # tf -> candles
    ind: dict                # tf -> Indicators
    regime: Regime
    structure_htf: StructureState
    structure_mid: StructureState
    swings_htf: list
    swings_mid: list
    swings_ltf: list
    htf_zones: list
    pools: dict
    vp: dict
    market_price: float


def build_bundle(symbol: str, candles: dict[str, list]) -> Optional[MarketBundle]:
    ind = {tf: Indicators(candles[tf]) for tf in candles}
    regime = classify_regime(candles[TF_HTF], candles[TF_MID])
    swings_htf = find_swings(candles[TF_HTF])
    swings_mid = find_swings(candles[TF_MID])
    swings_ltf = find_swings(candles[TF_LTF])
    structure_htf = analyze_structure(candles[TF_HTF], swings_htf)
    structure_mid = analyze_structure(candles[TF_MID], swings_mid)
    if structure_htf is None or structure_mid is None:
        return None
    obs = find_order_blocks(candles[TF_HTF], ind[TF_HTF].atr, ZONE_LOOKBACK_HTF)
    obs = mark_mitigation_and_breakers(obs, candles[TF_HTF])
    fvgs = find_fvgs(candles[TF_HTF], ind[TF_HTF].atr, ZONE_LOOKBACK_HTF)
    htf_zones = obs + fvgs
    pools = build_liquidity_pools(swings_htf, candles[TF_MACRO])
    vp = volume_profile(candles[TF_MID][-80:])
    return MarketBundle(symbol, candles, ind, regime, structure_htf, structure_mid,
                         swings_htf, swings_mid, swings_ltf, htf_zones, pools, vp,
                         candles[TF_LTF][-1]["c"])


# ══════════════════════════════════════════════════════════════════════════
#  SPECIALIZED ENGINES
#  Each takes a MarketBundle and returns 0+ Candidates. Engines are fully
#  independent — none calls another — so the Decision Engine can weigh,
#  rank and learn from them separately.
# ══════════════════════════════════════════════════════════════════════════

def _nearest_zone(zones: list[Zone], price: float, kind: str, max_dist_atr: float, atr_val: float) -> Optional[Zone]:
    cands = [z for z in zones if z.kind == kind and not (z.mitigated and not z.flipped)]
    cands = [z for z in cands if abs(z.mid - price) <= max_dist_atr * atr_val]
    if not cands:
        return None
    return min(cands, key=lambda z: abs(z.mid - price))


def engine_smc_order_block(mb: MarketBundle) -> list[Candidate]:
    """Price returning into a fresh, unmitigated HTF order block in the
    direction of HTF bias — classic institutional entry."""
    out = []
    price = mb.market_price
    atr_val = mb.ind[TF_LTF].last_atr or 1e-9
    direction = "long" if mb.structure_htf.bias == "bullish" else ("short" if mb.structure_htf.bias == "bearish" else None)
    if direction is None:
        return out
    kind = "demand" if direction == "long" else "supply"
    zone = _nearest_zone([z for z in mb.htf_zones if z.origin == "ob"], price, kind, POI_MAX_DIST_ATR_MULT, atr_val)
    if zone is None or zone.mitigated:
        return out
    q = zone_quality(zone)
    if q < 0.35:
        return out
    entry = zone.mid
    buf = _sl_buffer(atr_val, q)
    sl = zone.low - buf if direction == "long" else zone.high + buf
    tp1, tp2 = _build_targets(entry, sl, direction, mb.pools, mb.vp)
    out.append(Candidate("smc_order_block", mb.symbol, direction, entry, sl, tp1, tp2, zone, q, atr_val,
                          tags=["OB", mb.structure_htf.bias]))
    return out


def engine_smc_breaker(mb: MarketBundle) -> list[Candidate]:
    """A mitigated OB that has flipped polarity (breaker) and is now being
    retested from the new side — the strongest SMC footprint available."""
    out = []
    price = mb.market_price
    atr_val = mb.ind[TF_LTF].last_atr or 1e-9
    direction = "long" if mb.structure_htf.bias == "bullish" else ("short" if mb.structure_htf.bias == "bearish" else None)
    if direction is None:
        return out
    kind = "demand" if direction == "long" else "supply"
    breakers = [z for z in mb.htf_zones if z.origin == "breaker" and z.kind == kind]
    zone = _nearest_zone(breakers, price, kind, POI_MAX_DIST_ATR_MULT, atr_val)
    if zone is None:
        return out
    q = min(1.0, zone_quality(zone) + 0.10)
    if q < 0.40:
        return out
    entry = zone.mid
    buf = _sl_buffer(atr_val, q)
    sl = zone.low - buf if direction == "long" else zone.high + buf
    tp1, tp2 = _build_targets(entry, sl, direction, mb.pools, mb.vp)
    out.append(Candidate("smc_breaker", mb.symbol, direction, entry, sl, tp1, tp2, zone, q, atr_val,
                          tags=["Breaker", mb.structure_htf.bias]))
    return out


def engine_smc_fvg(mb: MarketBundle) -> list[Candidate]:
    """Price rebalancing into an unfilled HTF Fair Value Gap aligned with
    HTF bias — imbalance fill entries."""
    out = []
    price = mb.market_price
    atr_val = mb.ind[TF_LTF].last_atr or 1e-9
    direction = "long" if mb.structure_htf.bias == "bullish" else ("short" if mb.structure_htf.bias == "bearish" else None)
    if direction is None:
        return out
    kind = "demand" if direction == "long" else "supply"
    fvgs = [z for z in mb.htf_zones if z.origin == "fvg" and z.kind == kind]
    zone = _nearest_zone(fvgs, price, kind, POI_MAX_DIST_ATR_MULT * 0.9, atr_val)
    if zone is None:
        return out
    q = zone_quality(zone)
    if q < 0.30:
        return out
    entry = zone.mid
    buf = _sl_buffer(atr_val, q)
    sl = zone.low - buf if direction == "long" else zone.high + buf
    tp1, tp2 = _build_targets(entry, sl, direction, mb.pools, mb.vp)
    out.append(Candidate("smc_fvg", mb.symbol, direction, entry, sl, tp1, tp2, zone, q, atr_val,
                          tags=["FVG", mb.structure_htf.bias]))
    return out


def engine_liquidity_sweep(mb: MarketBundle) -> list[Candidate]:
    """Liquidity sweep on MID timeframe followed by an MSS on LTF — the
    canonical reversal pathway: stops taken, structure shifts, enter on
    the breaker left behind."""
    out = []
    atr_mid = mb.ind[TF_MID].last_atr or 1e-9
    atr_ltf = mb.ind[TF_LTF].last_atr or 1e-9
    price = mb.market_price
    for direction in ("long", "short"):
        smc_dir = "bullish" if direction == "long" else "bearish"
        sweep = detect_sweep(mb.candles[TF_MID], mb.pools, smc_dir, atr_mid)
        if sweep is None:
            continue
        mss = detect_mss(mb.candles[TF_LTF], smc_dir, atr_ltf)
        if mss is None:
            continue
        breaker = find_breaker_after_mss(mb.candles[TF_LTF], smc_dir)
        entry = breaker.mid if breaker else price
        atr_val = atr_ltf
        q = min(1.0, 0.45 + sweep["wick_ratio"] * 0.3 + (0.15 if breaker else 0.0))
        buf = _sl_buffer(atr_val, q)
        if breaker:
            sl = breaker.low - buf if direction == "long" else breaker.high + buf
        else:
            sl = sweep.get("bar_low", price - atr_val * 1.2) - buf if direction == "long" else \
                 sweep.get("bar_high", price + atr_val * 1.2) + buf
        tp1, tp2 = _build_targets(entry, sl, direction, mb.pools, mb.vp)
        out.append(Candidate("liquidity_sweep", mb.symbol, direction, entry, sl, tp1, tp2, breaker, q, atr_val,
                              tags=["Sweep", "MSS"]))
    return out


def engine_trend_continuation(mb: MarketBundle) -> list[Candidate]:
    """Strong-regime pullback to the 21/50 EMA confluence in the direction
    of an established trend — no reversal thesis, pure continuation."""
    out = []
    if mb.regime.label != "trend":
        return out
    ind = mb.ind[TF_MID]
    price = mb.market_price
    atr_val = ind.last_atr or 1e-9
    direction = "long" if mb.regime.direction == "bullish" else "short"
    ema_zone_lo = min(ind.ema_fast[-1], ind.ema_slow[-1])
    ema_zone_hi = max(ind.ema_fast[-1], ind.ema_slow[-1])
    dist = min(abs(price - ema_zone_lo), abs(price - ema_zone_hi))
    if dist > atr_val * 1.2:
        return out
    r = ind.rsi[-1]
    if direction == "long" and r > 68:
        return out
    if direction == "short" and r < 32:
        return out
    entry = price
    q = min(1.0, mb.regime.strength)
    buf = _sl_buffer(atr_val, q)
    sl = ema_zone_lo - buf if direction == "long" else ema_zone_hi + buf
    tp1, tp2 = _build_targets(entry, sl, direction, mb.pools, mb.vp)
    out.append(Candidate("trend_continuation", mb.symbol, direction, entry, sl, tp1, tp2, None, q, atr_val,
                          tags=["Trend", "EMA-pullback"]))
    return out


def engine_breakout(mb: MarketBundle) -> list[Candidate]:
    """Volatility-expansion breakout of a prior consolidation range with
    displacement and volume confirmation."""
    out = []
    ind_ltf = mb.ind[TF_LTF]
    candles = mb.candles[TF_LTF]
    atr_val = ind_ltf.last_atr or 1e-9
    lookback = candles[-24:-1]
    if not lookback:
        return out
    range_hi = max(c["h"] for c in lookback)
    range_lo = min(c["l"] for c in lookback)
    last = candles[-1]
    vol_avg = _avg_volume(candles, len(candles) - 1, 20)
    if last["c"] > range_hi and (last["c"] - last["o"]) > 0.8 * atr_val and last["v"] > 1.3 * vol_avg:
        direction = "long"
        entry = last["c"]
        sl = range_hi - _sl_buffer(atr_val, 0.6)
        q = min(1.0, 0.5 + (last["v"] / vol_avg - 1.0) * 0.2)
        tp1, tp2 = _build_targets(entry, sl, direction, mb.pools, mb.vp)
        out.append(Candidate("breakout", mb.symbol, direction, entry, sl, tp1, tp2, None, q, atr_val,
                              tags=["Breakout"]))
    elif last["c"] < range_lo and (last["o"] - last["c"]) > 0.8 * atr_val and last["v"] > 1.3 * vol_avg:
        direction = "short"
        entry = last["c"]
        sl = range_lo + _sl_buffer(atr_val, 0.6)
        q = min(1.0, 0.5 + (last["v"] / vol_avg - 1.0) * 0.2)
        tp1, tp2 = _build_targets(entry, sl, direction, mb.pools, mb.vp)
        out.append(Candidate("breakout", mb.symbol, direction, entry, sl, tp1, tp2, None, q, atr_val,
                              tags=["Breakout"]))
    return out


def engine_pullback(mb: MarketBundle) -> list[Candidate]:
    """Shallow retracement (38-61%) of the most recent LTF impulse leg,
    inside an aligned MID structure bias — earlier and tighter than the
    trend-continuation engine, aimed at intraday swings."""
    out = []
    swings = mb.swings_ltf[-6:]
    if len(swings) < 3:
        return out
    atr_val = mb.ind[TF_LTF].last_atr or 1e-9
    price = mb.market_price
    last_low = next((s for s in reversed(swings) if s.kind == "low"), None)
    last_high = next((s for s in reversed(swings) if s.kind == "high"), None)
    if not last_low or not last_high:
        return out
    if last_high.index > last_low.index and mb.structure_mid.bias == "bullish":
        leg = last_high.price - last_low.price
        if leg <= 0:
            return out
        retr = (last_high.price - price) / leg
        if 0.35 <= retr <= 0.65:
            direction = "long"
            entry = price
            sl = last_low.price - _sl_buffer(atr_val, 0.5)
            q = 0.55
            tp1, tp2 = _build_targets(entry, sl, direction, mb.pools, mb.vp)
            out.append(Candidate("pullback", mb.symbol, direction, entry, sl, tp1, tp2, None, q, atr_val,
                                  tags=["Pullback", "Fib38-61"]))
    elif last_low.index > last_high.index and mb.structure_mid.bias == "bearish":
        leg = last_high.price - last_low.price
        if leg <= 0:
            return out
        retr = (price - last_low.price) / leg
        if 0.35 <= retr <= 0.65:
            direction = "short"
            entry = price
            sl = last_high.price + _sl_buffer(atr_val, 0.5)
            q = 0.55
            tp1, tp2 = _build_targets(entry, sl, direction, mb.pools, mb.vp)
            out.append(Candidate("pullback", mb.symbol, direction, entry, sl, tp1, tp2, None, q, atr_val,
                                  tags=["Pullback", "Fib38-61"]))
    return out


def engine_mean_reversion_range(mb: MarketBundle) -> list[Candidate]:
    """Fade the edge of a well-defined range back toward equilibrium — only
    fires in confirmed range regime, with RSI exhaustion confirmation."""
    out = []
    if mb.regime.label != "range":
        return out
    price = mb.market_price
    atr_val = mb.ind[TF_MID].last_atr or 1e-9
    r = mb.ind[TF_MID].rsi[-1]
    rng = mb.structure_mid
    band = (rng.range_high - rng.range_low) * 0.15
    if price >= rng.range_high - band and r > 65:
        direction = "short"
        entry = price
        sl = rng.range_high + _sl_buffer(atr_val, 0.5)
        tp1 = rng.eq
        tp2 = rng.range_low
        q = min(1.0, (r - 50) / 50)
        out.append(Candidate("mean_reversion_range", mb.symbol, direction, entry, sl, tp1, tp2, None, q, atr_val,
                              tags=["Range", "Fade-high"]))
    elif price <= rng.range_low + band and r < 35:
        direction = "long"
        entry = price
        sl = rng.range_low - _sl_buffer(atr_val, 0.5)
        tp1 = rng.eq
        tp2 = rng.range_high
        q = min(1.0, (50 - r) / 50)
        out.append(Candidate("mean_reversion_range", mb.symbol, direction, entry, sl, tp1, tp2, None, q, atr_val,
                              tags=["Range", "Fade-low"]))
    return out


ALL_ENGINES = [
    engine_smc_order_block, engine_smc_breaker, engine_smc_fvg, engine_liquidity_sweep,
    engine_trend_continuation, engine_breakout, engine_pullback, engine_mean_reversion_range,
]


def run_all_engines(mb: MarketBundle) -> list[Candidate]:
    out = []
    for fn in ALL_ENGINES:
        try:
            out.extend(fn(mb))
        except Exception as e:
            print(f"    [ENGINE ERROR] {fn.__name__} on {mb.symbol}: {e}")
    return out

# ══════════════════════════════════════════════════════════════════════════
#  LEARNING STORE — per-engine historical performance, adaptive weights
# ══════════════════════════════════════════════════════════════════════════

def _default_engine_stats() -> dict:
    return {name: {"wins": 0, "losses": 0, "sum_r": 0.0, "trades": 0, "weight": 1.0,
                    "conf_buckets": {}} for name in ENGINE_NAMES}


def get_engine_stats(state: dict) -> dict:
    stats = state.setdefault("engine_stats", _default_engine_stats())
    for name in ENGINE_NAMES:
        stats.setdefault(name, _default_engine_stats()[name])
    return stats


def engine_weight(state: dict, engine: str) -> float:
    return get_engine_stats(state).get(engine, {}).get("weight", 1.0)


def engine_expectancy(state: dict, engine: str) -> float:
    """Average R per trade for this engine, 0.0 if insufficient sample."""
    s = get_engine_stats(state).get(engine, {})
    trades = s.get("trades", 0)
    if trades < 5:
        return 0.0
    return safe_div(s.get("sum_r", 0.0), trades)


def _confidence_bucket(conf: float) -> str:
    return f"{int(conf // 10) * 10}-{int(conf // 10) * 10 + 9}"


def record_trade_outcome(state: dict, engine: str, confidence: float, r_multiple: float, won: bool,
                          regime_label: str, notes: dict) -> None:
    """Attribute a completed trade back to the engine that produced it and
    update expectancy + confidence calibration. Also appends a causal
    record (why it won/lost) to a bounded rolling log for later review."""
    stats = get_engine_stats(state)
    s = stats.setdefault(engine, _default_engine_stats()[engine])
    s["trades"] += 1
    s["sum_r"] += r_multiple
    if won:
        s["wins"] += 1
    else:
        s["losses"] += 1
    bucket = _confidence_bucket(confidence)
    cb = s.setdefault("conf_buckets", {}).setdefault(bucket, {"n": 0, "wins": 0})
    cb["n"] += 1
    if won:
        cb["wins"] += 1

    log = state.setdefault("trade_review_log", [])
    log.append({
        "ts": datetime.now(timezone.utc).isoformat(), "engine": engine, "confidence": confidence,
        "r_multiple": r_multiple, "won": won, "regime": regime_label, **notes,
    })
    if len(log) > 500:
        del log[: len(log) - 500]


def update_adaptive_weights(state: dict) -> None:
    """Nudge each engine's weight toward its trailing realized expectancy,
    bounded to [WEIGHT_MIN, WEIGHT_MAX] and rate-limited so a short losing
    or winning streak cannot whipsaw the ensemble — preserves the core
    philosophy while letting persistently strong/weak engines drift."""
    stats = get_engine_stats(state)
    for name, s in stats.items():
        trades = s.get("trades", 0)
        if trades < WEIGHT_LOOKBACK_TRADES // 3:
            continue
        win_rate = safe_div(s.get("wins", 0), max(1, s.get("wins", 0) + s.get("losses", 0)))
        avg_r = safe_div(s.get("sum_r", 0.0), trades)
        # target: engines with positive expectancy and >50% win-rate drift up
        target = 1.0 + max(-0.45, min(0.55, avg_r * 0.25 + (win_rate - 0.5) * 0.6))
        current = s.get("weight", 1.0)
        new_w = current + (target - current) * WEIGHT_LEARNING_RATE
        s["weight"] = max(WEIGHT_MIN, min(WEIGHT_MAX, new_w))


def calibration_adjustment(state: dict, engine: str, confidence: float) -> float:
    """If a confidence bucket for this engine has historically under- or
    over-performed its implied win-rate, nudge the score back toward
    reality instead of trusting the raw model output blindly."""
    s = get_engine_stats(state).get(engine, {})
    bucket = s.get("conf_buckets", {}).get(_confidence_bucket(confidence))
    if not bucket or bucket["n"] < 8:
        return 0.0
    realized_wr = bucket["wins"] / bucket["n"]
    implied_wr = (int(_confidence_bucket(confidence).split("-")[0]) + 5) / 100.0
    delta = realized_wr - implied_wr
    return max(-6.0, min(6.0, delta * 20))

# ══════════════════════════════════════════════════════════════════════════
#  DECISION ENGINE — centralized scoring across every candidate
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class FilterResult:
    passed: bool
    reason: str = ""
    location_score: float = 0.0
    context_score: float = 0.0
    quality_score: float = 0.0
    rr_score: float = 0.0
    confirm_score: float = 0.0


def apply_five_filters(cand: Candidate, mb: MarketBundle, min_rr: float) -> FilterResult:
    # 1. LOCATION — is the entry actually near a defensible POI / not chasing?
    dist_atr = abs(cand.entry - mb.market_price) / (cand.atr_val or 1e-9)
    if dist_atr > POI_MAX_DIST_ATR_MULT:
        return FilterResult(False, "entry too far from market")
    location_score = max(0.0, 1.0 - dist_atr / POI_MAX_DIST_ATR_MULT)

    # 2. CONTEXT — does direction align with HTF/MID structure (not fighting bias)?
    htf_dir = mb.structure_htf.bias
    mid_dir = mb.structure_mid.bias
    cand_dir = "bullish" if cand.direction == "long" else "bearish"
    align = (1.0 if htf_dir == cand_dir else (0.4 if htf_dir == "neutral" else 0.0))
    align += (0.6 if mid_dir == cand_dir else (0.2 if mid_dir == "neutral" else 0.0))
    context_score = min(1.0, align / 1.6)
    if context_score < 0.15 and cand.engine not in ("liquidity_sweep", "mean_reversion_range"):
        return FilterResult(False, "against higher-timeframe structure")

    # 3. QUALITY — zone/setup quality already computed by the engine
    quality_score = max(0.0, min(1.0, cand.raw_score))
    if quality_score < 0.25:
        return FilterResult(False, "setup quality too low")

    # 4. RR — risk/reward floor, validated against real candle extremes only
    if cand.rr1 < min_rr:
        return FilterResult(False, f"RR {cand.rr1:.2f} below floor {min_rr}")
    rr_score = min(1.0, cand.rr2 / EXT_RR_LEVELS[2])

    # 5. LTF CONFIRMATION — momentum not flatly opposing the trade on entry TF
    ltf = mb.ind[TF_LTF]
    r = ltf.rsi[-1]
    if cand.direction == "long" and r < 22:
        confirm_score = 0.3
    elif cand.direction == "short" and r > 78:
        confirm_score = 0.3
    else:
        confirm_score = 1.0

    return FilterResult(True, "", location_score, context_score, quality_score, rr_score, confirm_score)


def session_now(reference_ms: int) -> str:
    hour = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).hour
    for name, (start, end) in SESSION_WINDOWS.items():
        if start <= hour < end:
            return name
    return "off"


def compute_confidence(cand: Candidate, fr: FilterResult, mb: MarketBundle, state: dict,
                        reference_ms: int, htf_macro_alignment: float, confluence_count: int) -> float:
    base = (
        fr.location_score * 18 + fr.context_score * 24 + fr.quality_score * 22 +
        fr.rr_score * 14 + fr.confirm_score * 10
    )
    regime_fit = 8.0 if (
        (cand.engine == "trend_continuation" and mb.regime.label == "trend") or
        (cand.engine == "mean_reversion_range" and mb.regime.label == "range") or
        (cand.engine in ("liquidity_sweep", "smc_breaker") and mb.regime.label in ("reversal", "volatile"))
    ) else 0.0
    macro_bonus = htf_macro_alignment * 6.0
    confluence_bonus = min(10.0, (confluence_count - 1) * 4.0)
    session_bonus = SESSION_SCORE_BONUS.get(session_now(reference_ms), 0.0)
    weight = engine_weight(state, cand.engine)
    calib = calibration_adjustment(state, cand.engine, base)

    score = (base + regime_fit + macro_bonus + confluence_bonus + session_bonus + calib) * weight
    return max(0.0, min(100.0, score))


def grade_for_confidence(confidence: float) -> str:
    if confidence >= 85:
        return "A+"
    if confidence >= 75:
        return "A"
    if confidence >= 65:
        return "B"
    return "C"


def clamp_entry_to_market(entry, sl, tp1, tp2, market_price, atr_val):
    max_dist = min(atr_val * POI_MAX_DIST_ATR_MULT, market_price * POI_MAX_PCT_OF_PRICE)
    if abs(entry - market_price) > max_dist:
        direction_sign = 1 if entry > market_price else -1
        shift = (abs(entry - market_price) - max_dist) * direction_sign
        entry -= shift
        sl -= shift
        tp1 -= shift
        tp2 -= shift
    return entry, sl, tp1, tp2


def priority_score(cand: Candidate, confidence: float) -> float:
    return confidence + cand.rr2 * 2.0


def decide(mb: MarketBundle, state: dict, market_ctx: dict, reference_ms: int,
           min_confidence: float) -> list[dict]:
    candidates = run_all_engines(mb)
    if not candidates:
        return []

    macro_closes = [c["c"] for c in mb.candles[TF_MACRO]]
    macro_bias = "bullish" if mb.candles[TF_MACRO][-1]["c"] > ema(macro_closes, min(EMA_SLOW, len(macro_closes) - 1))[-1] else "bearish"
    htf_macro_alignment = 1.0 if macro_bias == mb.structure_htf.bias else (0.3 if mb.structure_htf.bias != "neutral" else 0.5)

    # confluence: how many independent engines agree on the same direction
    long_engines = {c.engine for c in candidates if c.direction == "long"}
    short_engines = {c.engine for c in candidates if c.direction == "short"}

    results = []
    for cand in candidates:
        cand.entry, cand.sl, cand.tp1, cand.tp2 = clamp_entry_to_market(
            cand.entry, cand.sl, cand.tp1, cand.tp2, mb.market_price, cand.atr_val)

        fr = apply_five_filters(cand, mb, MIN_RR_FLOOR)
        if not fr.passed:
            continue

        confluence_count = len(long_engines) if cand.direction == "long" else len(short_engines)
        confidence = compute_confidence(cand, fr, mb, state, reference_ms, htf_macro_alignment, confluence_count)
        if confidence < min_confidence:
            continue

        grade = grade_for_confidence(confidence)
        results.append({"symbol": mb.symbol, "direction": cand.direction, "cand": cand,
                         "confidence": confidence, "grade": grade, "regime": mb.regime.label})
    return results

# ══════════════════════════════════════════════════════════════════════════
#  STATE PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════

def _default_state() -> dict:
    return {
        "active_signals": {}, "signal_history": [], "cooldowns": {}, "recent_entries": {},
        "engine_stats": _default_engine_stats(), "trade_review_log": [],
        "governor": {"min_confidence_shift": 0.0}, "last_daily_summary_date": None,
        "atr_pct_memory": {},
    }


def load_state() -> dict:
    p = Path(STATE_FILE)
    if not p.exists():
        return _default_state()
    try:
        data = json.loads(p.read_text())
        base = _default_state()
        base.update(data)
        for k, v in _default_state().items():
            base.setdefault(k, v)
        return base
    except (json.JSONDecodeError, OSError):
        return _default_state()


def save_state(state: dict) -> None:
    tmp = Path(STATE_FILE + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str))
    tmp.replace(STATE_FILE)


def prune_state(state: dict) -> None:
    if len(state["signal_history"]) > MAX_SIGNAL_HISTORY:
        state["signal_history"] = state["signal_history"][-MAX_SIGNAL_HISTORY:]
    cutoff = time.time() - 21 * 86400
    state["cooldowns"] = {k: v for k, v in state["cooldowns"].items() if v.get("ts", 0) > cutoff - 90000}
    state["recent_entries"] = {k: v for k, v in state["recent_entries"].items() if v.get("ts", 0) > cutoff}


# ══════════════════════════════════════════════════════════════════════════
#  COOLDOWN / DEDUP / CORRELATION
# ══════════════════════════════════════════════════════════════════════════

def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    key = f"{symbol}:{direction}"
    cd = state["cooldowns"].get(key)
    if cd is None:
        return True
    return (bar_index - cd.get("bar", -999)) >= COOLDOWN_BARS_LTF


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> None:
    state["cooldowns"][f"{symbol}:{direction}"] = {"bar": bar_index, "ts": time.time()}


def is_recent_duplicate(state: dict, symbol: str, direction: str, entry: float) -> bool:
    key = f"{symbol}:{direction}"
    rec = state["recent_entries"].get(key)
    if rec is None:
        return False
    return abs(entry - rec["entry"]) / max(entry, 1e-9) <= DUPLICATE_ENTRY_TOLERANCE_PCT


def mark_recent_entry(state: dict, symbol: str, direction: str, entry: float) -> None:
    state["recent_entries"][f"{symbol}:{direction}"] = {"entry": entry, "ts": time.time()}


def compute_returns(candles: list[dict], lookback: int) -> list[float]:
    closes = [c["c"] for c in candles[-lookback:]]
    return [safe_div(closes[i] - closes[i - 1], closes[i - 1]) for i in range(1, len(closes))]


def pearson(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    ma, mb_ = sum(a) / n, sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb_) for i in range(n))
    va = math.sqrt(sum((x - ma) ** 2 for x in a))
    vb = math.sqrt(sum((x - mb_) ** 2 for x in b))
    return safe_div(cov, va * vb)


def build_correlation_clusters(bundles: dict[str, MarketBundle]) -> list[set]:
    syms = list(bundles.keys())
    rets = {s: compute_returns(bundles[s].candles[TF_MID], 60) for s in syms}
    clusters: list[set] = []
    assigned: set = set()
    for i, s1 in enumerate(syms):
        if s1 in assigned:
            continue
        cluster = {s1}
        for s2 in syms[i + 1:]:
            if s2 in assigned:
                continue
            if pearson(rets[s1], rets[s2]) >= 0.80:
                cluster.add(s2)
        clusters.append(cluster)
        assigned |= cluster
    return clusters


def dedup_correlated(ranked: list[dict], clusters: list[set]) -> list[dict]:
    chosen, used_clusters = [], set()
    for r in ranked:
        sym = r["symbol"]
        cluster_id = next((i for i, c in enumerate(clusters) if sym in c), None)
        key = (cluster_id, r["direction"])
        if cluster_id is not None and key in used_clusters:
            continue
        chosen.append(r)
        if cluster_id is not None:
            used_clusters.add(key)
    return chosen


def governor_threshold(state: dict) -> float:
    hist = [h for h in state["signal_history"] if h.get("sent") and h.get("result") in ("win", "loss")][-GOVERNOR_LOOKBACK_SIGNALS:]
    if len(hist) < 12:
        return BASE_MIN_CONFIDENCE + state["governor"]["min_confidence_shift"]
    wins = sum(1 for h in hist if h["result"] == "win")
    wr = wins / len(hist)
    error = GOVERNOR_TARGET_WINRATE - wr
    shift = max(-GOVERNOR_MAX_SHIFT, min(GOVERNOR_MAX_SHIFT, error * 40))
    state["governor"]["min_confidence_shift"] = shift
    return BASE_MIN_CONFIDENCE + shift


def dynamic_max_signals(breadth_pct: float, regime_labels: list[str]) -> int:
    trending_frac = safe_div(sum(1 for r in regime_labels if r == "trend"), max(1, len(regime_labels)))
    if trending_frac > 0.4 or breadth_pct > 0.65 or breadth_pct < 0.35:
        return MAX_SIGNALS_PER_SCAN_TRENDING
    return MAX_SIGNALS_PER_SCAN_DEFAULT


def compute_breadth_pct(bundles: dict[str, MarketBundle]) -> float:
    if not bundles:
        return 0.5
    above = sum(1 for mb in bundles.values() if mb.market_price > mb.ind[TF_HTF].ema_slow[-1])
    return above / len(bundles)


def count_active(state: dict) -> int:
    return len(state["active_signals"])

# ══════════════════════════════════════════════════════════════════════════
#  TELEGRAM — messaging, reactions, replies, full trade lifecycle
# ══════════════════════════════════════════════════════════════════════════

TG_API = f"https://api.telegram.org/bot{TG_BOT_TOKEN}"


def _sanitize_error(e: Exception) -> str:
    return str(e).replace(TG_BOT_TOKEN or "", "***")


def tg_escape(value) -> str:
    text = str(value)
    for ch in "_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def fmt_px(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def send_telegram(text: str) -> Optional[int]:
    try:
        r = _session.post(f"{TG_API}/sendMessage", json={
            "chat_id": TG_CHAT_ID, "text": text, "parse_mode": "MarkdownV2",
            "disable_web_page_preview": True,
        }, timeout=10)
        data = r.json()
        if data.get("ok"):
            return data["result"]["message_id"]
        print(f"  [TG] send failed: {data}")
        return None
    except Exception as e:
        print(f"  [TG] send error: {_sanitize_error(e)}")
        return None


def reply_to_telegram(message_id: int, text: str) -> Optional[int]:
    try:
        r = _session.post(f"{TG_API}/sendMessage", json={
            "chat_id": TG_CHAT_ID, "text": text, "parse_mode": "MarkdownV2",
            "reply_to_message_id": message_id, "disable_web_page_preview": True,
        }, timeout=10)
        data = r.json()
        return data["result"]["message_id"] if data.get("ok") else None
    except Exception as e:
        print(f"  [TG] reply error: {_sanitize_error(e)}")
        return None


def react_to_message(message_id: int, emoji: str) -> None:
    try:
        _session.post(f"{TG_API}/setMessageReaction", json={
            "chat_id": TG_CHAT_ID, "message_id": message_id, "reaction": [{"type": "emoji", "emoji": emoji}],
        }, timeout=10)
    except Exception:
        pass


def confidence_bar(confidence: float) -> str:
    filled = int(round(confidence / 10))
    return "\u2588" * filled + "\u2591" * (10 - filled)


def format_signal(cand: Candidate, confidence: float, grade: str, rank: int, regime: str) -> str:
    coin = hl_coin(cand.symbol)
    arrow = "\U0001F7E2 LONG" if cand.direction == "long" else "\U0001F534 SHORT"
    engine_label = cand.engine.replace("_", " ").title()
    lines = [
        f"*{tg_escape(ENGINE_NAME)} v{tg_escape(__version__)}*  \\#{rank}",
        f"{tg_escape(coin)}USDT  {tg_escape(arrow)}  \\[{tg_escape(grade)}\\]",
        "",
        f"Engine: {tg_escape(engine_label)}",
        f"Regime: {tg_escape(regime)}",
        f"Confidence: {confidence:.0f}/100  {tg_escape(confidence_bar(confidence))}",
        "",
        f"Entry: `{fmt_px(cand.entry)}`",
        f"SL: `{fmt_px(cand.sl)}`",
        f"TP1: `{fmt_px(cand.tp1)}`  \\(RR {cand.rr1:.2f}\\)",
        f"TP2: `{fmt_px(cand.tp2)}`  \\(RR {cand.rr2:.2f}\\)",
        "",
        f"Tags: {tg_escape(', '.join(cand.tags))}",
    ]
    return "\n".join(lines)


def track_signal(state: dict, symbol: str, direction: str, msg_id: int, cand: Candidate,
                  confidence: float, grade: str, bar_index: int, hist_id: str) -> None:
    state["active_signals"][hist_id] = {
        "symbol": symbol, "direction": direction, "engine": cand.engine, "entry": cand.entry,
        "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2, "confidence": confidence, "grade": grade,
        "msg_id": msg_id, "status": "activated", "opened_bar": bar_index,
        "opened_ts": time.time(), "tp1_hit": False, "be_moved": False,
        "original_risk": abs(cand.entry - cand.sl) or 1e-9,
    }


def record_signal_history(state: dict, symbol: str, direction: str, engine: str,
                           confidence: float, grade: str, sent: bool) -> str:
    hist_id = f"{symbol}-{direction}-{int(time.time() * 1000)}"
    state["signal_history"].append({
        "id": hist_id, "symbol": symbol, "direction": direction, "engine": engine,
        "confidence": confidence, "grade": grade, "sent": sent, "result": None,
        "ts": datetime.now(timezone.utc).isoformat(),
    })
    return hist_id


def check_active_signals(state: dict, market_prices: dict[str, float]) -> None:
    """Re-evaluates every open signal against the latest close, sends the
    reply-thread lifecycle update, and — on terminal outcomes — feeds the
    result back into the Learning Store."""
    to_remove = []
    for hist_id, sig in list(state["active_signals"].items()):
        price = market_prices.get(sig["symbol"])
        if price is None:
            continue
        direction = sig["direction"]
        hit_sl = (price <= sig["sl"]) if direction == "long" else (price >= sig["sl"])
        hit_tp1 = (price >= sig["tp1"]) if direction == "long" else (price <= sig["tp1"])
        hit_tp2 = (price >= sig["tp2"]) if direction == "long" else (price <= sig["tp2"])

        risk = sig.get("original_risk") or (abs(sig["entry"] - sig["sl"]) or 1e-9)

        if hit_sl and not sig["tp1_hit"]:
            reply_to_telegram(sig["msg_id"], tg_escape("\u274C SL hit \u2014 closed."))
            react_to_message(sig["msg_id"], "\U0001F44E")
            r_mult = -1.0
            _finalize(state, hist_id, sig, "loss", r_mult)
            to_remove.append(hist_id)
            continue

        if hit_sl and sig["tp1_hit"] and sig["be_moved"]:
            reply_to_telegram(sig["msg_id"], tg_escape("\u26AA Break-even stop hit \u2014 closed flat after TP1."))
            r_mult = safe_div(abs(sig["tp1"] - sig["entry"]), risk)
            _finalize(state, hist_id, sig, "win", r_mult)
            to_remove.append(hist_id)
            continue

        if hit_tp2:
            reply_to_telegram(sig["msg_id"], tg_escape("\U0001F3C1 TP2 hit \u2014 target reached, closed."))
            react_to_message(sig["msg_id"], "\U0001F525")
            r_mult = safe_div(abs(sig["tp2"] - sig["entry"]), risk)
            _finalize(state, hist_id, sig, "win", r_mult)
            to_remove.append(hist_id)
            continue

        if hit_tp1 and not sig["tp1_hit"]:
            sig["tp1_hit"] = True
            sig["be_moved"] = True
            sig["sl"] = sig["entry"]
            reply_to_telegram(sig["msg_id"], tg_escape("\u2705 TP1 hit \u2014 stop moved to break-even, riding for TP2."))
            react_to_message(sig["msg_id"], "\U0001F44D")

    for hist_id in to_remove:
        del state["active_signals"][hist_id]


def _finalize(state: dict, hist_id: str, sig: dict, result: str, r_multiple: float) -> None:
    for h in state["signal_history"]:
        if h["id"] == hist_id:
            h["result"] = result
            h["r_multiple"] = r_multiple
            break
    record_trade_outcome(state, sig["engine"], sig["confidence"], r_multiple, result == "win",
                          sig.get("regime", "unknown"),
                          {"symbol": sig["symbol"], "direction": sig["direction"], "grade": sig.get("grade")})
    update_adaptive_weights(state)


def build_daily_summary(state: dict) -> str:
    hist = [h for h in state["signal_history"] if h.get("result") in ("win", "loss")]
    last24 = hist[-60:]
    wins = sum(1 for h in last24 if h["result"] == "win")
    losses = sum(1 for h in last24 if h["result"] == "loss")
    total = wins + losses
    wr = safe_div(wins, total) * 100
    avg_r = safe_div(sum(h.get("r_multiple", 0.0) for h in last24), total)
    stats = get_engine_stats(state)
    best = max(stats.items(), key=lambda kv: safe_div(kv[1]["sum_r"], max(1, kv[1]["trades"])), default=(None, None))
    worst = min(stats.items(), key=lambda kv: safe_div(kv[1]["sum_r"], max(1, kv[1]["trades"])), default=(None, None))

    lines = [
        f"*{tg_escape(ENGINE_NAME)} Daily Summary*",
        f"Trades closed: {total}  |  Win rate: {wr:.0f}%  |  Avg R: {avg_r:.2f}",
        f"Active signals: {count_active(state)}",
        "",
        f"Best engine: {tg_escape(best[0]) if best[0] else 'n/a'}",
        f"Weakest engine: {tg_escape(worst[0]) if worst[0] else 'n/a'}",
        "",
        f"Governor confidence shift: {state['governor']['min_confidence_shift']:+.1f}",
    ]
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict) -> None:
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    if now.hour == 8 and state.get("last_daily_summary_date") != today:
        send_telegram(build_daily_summary(state))
        state["last_daily_summary_date"] = today

# ══════════════════════════════════════════════════════════════════════════
#  MAIN EXECUTION FLOW
# ══════════════════════════════════════════════════════════════════════════

_shutdown = False


def _handle_sigterm(signum, frame):
    global _shutdown
    _shutdown = True


os_signal.signal(os_signal.SIGTERM, _handle_sigterm)


def collect_bundle(symbol: str, reference_ms: int) -> Optional[MarketBundle]:
    candles = fetch_all_candles(symbol, reference_ms)
    if candles is None:
        return None
    try:
        return build_bundle(symbol, candles)
    except Exception as e:
        print(f"    [BUNDLE ERROR] {symbol}: {e}")
        return None


def hard_gates_pass(symbol: str, mb: MarketBundle, market_ctx: dict) -> bool:
    atr_pct = safe_div(mb.ind[TF_HTF].last_atr, mb.market_price) * 100
    if not (MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT):
        return False
    ctx = market_ctx.get(symbol, {})
    if symbol not in SPREAD_EXEMPT:
        spread = ctx.get("spread_pct")
        if spread is not None and spread > SPREAD_SUPPRESS_PCT:
            return False
    oi = ctx.get("oi", 0.0) * ctx.get("mark_px", 0.0)
    if oi and oi < MIN_OI_USD:
        return False
    return True


def scan_symbol(symbol: str, state: dict, mb: Optional[MarketBundle], market_ctx: dict,
                 reference_ms: int, bar_index_ltf: int, min_confidence: float) -> list[dict]:
    if mb is None:
        return []
    if not hard_gates_pass(symbol, mb, market_ctx):
        return []
    results = decide(mb, state, market_ctx, reference_ms, min_confidence)
    out = []
    for r in results:
        cand, direction = r["cand"], r["direction"]
        if not check_cooldown(state, symbol, direction, bar_index_ltf):
            continue
        if is_recent_duplicate(state, symbol, direction, cand.entry):
            continue
        out.append(r)
    return out


def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] {ENGINE_NAME} v{__version__} scan starting...")
    reference_ms = int(time.time() * 1000)
    bar_index_ltf = reference_ms // (15 * 60 * 1000)
    state = load_state()
    prune_state(state)

    print("[INIT] Fetching market context...")
    meta_ctx = get_meta_and_asset_ctxs() or {}
    market_ctx = {f"{coin}USDT": data for coin, data in meta_ctx.items()}

    if _shutdown:
        save_state(state)
        sys.exit(0)

    print("[PHASE 1] Collecting candle bundles across all timeframes...")
    bundles: dict[str, MarketBundle] = {}
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {ex.submit(collect_bundle, sym, reference_ms): sym for sym in WATCHLIST}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                mb = fut.result()
                if mb is not None:
                    bundles[sym] = mb
            except Exception as e:
                print(f"    ERROR building bundle {sym}: {e}")

    resolved = [s for s in WATCHLIST if s in bundles]
    print(f"  Resolved {len(resolved)}/{len(WATCHLIST)} symbols")
    if len(resolved) < 10:
        print("  [ABORT] Too few symbols resolved this cycle - skipping to avoid bad breadth reads")
        save_state(state)
        return

    print("[PHASE 1b] Spread checks (majors exempt)...")
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {ex.submit(get_l2_spread_pct, sym): sym for sym in resolved if sym not in SPREAD_EXEMPT}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                spread = fut.result()
                if spread is not None:
                    market_ctx.setdefault(sym, {})["spread_pct"] = spread
            except Exception:
                pass

    breadth_pct = compute_breadth_pct(bundles)
    regime_labels = [mb.regime.label for mb in bundles.values()]
    min_confidence = governor_threshold(state)
    max_signals = dynamic_max_signals(breadth_pct, regime_labels)
    print(f"  Governor min-confidence: {min_confidence:.1f}  |  Max signals: {max_signals} "
          f"(breadth {breadth_pct*100:.0f}%)")

    try:
        corr_clusters = build_correlation_clusters(bundles)
    except Exception as e:
        print(f"  [CORR] clustering failed, falling back to singletons: {e}")
        corr_clusters = [{s} for s in resolved]

    if _shutdown:
        save_state(state)
        sys.exit(0)

    print("[PHASE 2] Running ensemble engines across symbols...")
    pending: list[dict] = []
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {
            ex.submit(scan_symbol, sym, state, bundles.get(sym), market_ctx,
                      reference_ms, bar_index_ltf, min_confidence): sym
            for sym in resolved
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                res = fut.result()
                pending.extend(res)
            except Exception as e:
                print(f"    ERROR scanning {sym}: {e}")

    pending.sort(key=lambda r: priority_score(r["cand"], r["confidence"]), reverse=True)
    deduped = dedup_correlated(pending, corr_clusters)

    room = max(0, MAX_CONCURRENT_ACTIVE_SIGNALS - count_active(state))
    cap = min(max_signals, room)
    top = deduped[:cap]
    dropped = deduped[cap:]

    for r in dropped:
        record_signal_history(state, r["symbol"], r["direction"], r["cand"].engine, r["confidence"], r["grade"], sent=False)
    if dropped:
        print(f"  Dropped {len(dropped)} lower-priority setup(s) (cap={cap})")

    fired = 0
    for rank, r in enumerate(top, start=1):
        symbol, direction, cand, confidence, grade = r["symbol"], r["direction"], r["cand"], r["confidence"], r["grade"]
        msg = format_signal(cand, confidence, grade, rank, r["regime"])
        msg_id = send_telegram(msg)
        hist_id = record_signal_history(state, symbol, direction, cand.engine, confidence, grade, sent=True)
        if msg_id:
            state["signal_history"][-1]["regime"] = r["regime"]
            update_cooldown(state, symbol, direction, bar_index_ltf)
            mark_recent_entry(state, symbol, direction, cand.entry)
            track_signal(state, symbol, direction, msg_id, cand, confidence, grade, bar_index_ltf, hist_id)
            state["active_signals"][hist_id]["regime"] = r["regime"]
            print(f"  #{rank} {hl_coin(symbol)} {direction.upper()} [{grade}] engine={cand.engine} "
                  f"conf={confidence:.0f} entry={cand.entry:.4f} sl={cand.sl:.4f} "
                  f"tp1={cand.tp1:.4f} tp2={cand.tp2:.4f}")
            fired += 1
        else:
            print(f"  #{rank} {hl_coin(symbol)} {direction.upper()} - Telegram send failed, not tracked")
        time.sleep(0.4)

    print("[TRACK] Checking active signals against latest prices...")
    market_prices = {sym: mb.market_price for sym, mb in bundles.items()}
    check_active_signals(state, market_prices)

    maybe_send_daily_summary(state)
    save_state(state)
    print(f"Scan complete. {fired} signal(s) fired. {count_active(state)} active.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            send_telegram(tg_escape(f"AEGIS crashed: {e}"))
        except Exception:
            pass
        raise
    finally:
        _session.close()
