#!/usr/bin/env python3
"""
AETHERION Signal Engine v1.0.0
================================

# pip install requests

AETHERION is a dual-rail intraday & swing confluence engine for Hyperliquid
perpetuals. Two independent pipelines (4H/15m intraday, 1D/1h swing) each
evaluate three structurally independent pathways — liquidity reversal, trend
continuation, and momentum breakout — then fuse them through ensemble-agreement
scoring with funding/OI derivatives, cross-sectional breadth, and liquidity
structure. Quality vs. frequency is balanced exclusively via a fixed,
regime-conditioned rule table decided during backtesting — there is no online
self-tuning loop that adjusts thresholds from live trading outcomes.

ADAPTIVE QUALITY / FREQUENCY MECHANISM (inspectable, not a black box)
----------------------------------------------------------------------
Each symbol receives a RegimeVector each scan (ADX trend strength, ATR
percentile, Bollinger-width percentile, noise index, BTC macro bias, market
breadth). These map deterministically to:
  - clean trend   -> confidence floor -8, liquidity floor x0.75
  - choppy tape   -> confidence floor +10, liquidity floor x1.40
  - high volatility -> widen SL/TP 1.35x; tighten breakout follow-through
  - macro event window -> confidence floor +6
Every suppressed candidate is logged with the blocking filter name.

Infrastructure: Hyperliquid info API | state.json | cron every 15m | Telegram

Usage:
  python aetherion_engine_v1_0_0.py
  python aetherion_engine_v1_0_0.py --dry-run
  python aetherion_engine_v1_0_0.py backtest
  python aetherion_engine_v1_0_0.py backtest all

Env: TG_BOT_TOKEN, TG_CHAT_ID (optional for dry-run)
     AETHERION_DRY_RUN=1, AETHERION_STATE_PATH, AETHERION_LOG_PATH
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import signal as os_signal
import statistics
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

# ==============================================================================
# CONFIGURATION
# ==============================================================================

ENGINE_NAME = "AETHERION"
VERSION = "1.0.0"

HL_INFO_URL = os.environ.get("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
STATE_PATH = os.environ.get("AETHERION_STATE_PATH", "state.json")
LOG_PATH = os.environ.get("AETHERION_LOG_PATH", "aetherion_engine.log")
DRY_RUN = os.environ.get("AETHERION_DRY_RUN", "0") == "1"

WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

SECTOR_MAP = {
    "BTC": "btc", "ETH": "eth",
    "SOL": "eth_l1", "AVAX": "eth_l1", "SUI": "eth_l1", "APT": "eth_l1", "NEAR": "eth_l1",
    "BNB": "bnb",
    "XRP": "payments", "XLM": "payments", "TRX": "payments", "LTC": "payments",
    "DOGE": "meme", "PENGU": "meme",
    "ADA": "layer1_alt", "DOT": "layer1_alt", "TAO": "layer1_alt",
    "LINK": "defi", "AAVE": "defi", "UNI": "defi", "ONDO": "defi", "PENDLE": "defi",
    "HYPE": "hype", "ZEC": "privacy", "BCH": "privacy",
}
BTC_SYMBOL = "BTC"
BTC_REGIME_EXEMPT = {"hype", "defi"}

INTERVAL_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
CANDLE_COUNTS = {"15m": 300, "1h": 300, "4h": 300, "1d": 220}

# Fixed thresholds (regime-conditioned adjustments applied at runtime — not live-tuned)
BASE_CONFIDENCE_FLOOR = 58.0
TREND_REGIME_RELIEF = 8.0
CHOP_REGIME_PENALTY = 10.0
MACRO_EVENT_PENALTY = 6.0
WEEKEND_PENALTY = 4.0

MIN_RR = 1.25
MIN_STOP_PCT = 0.004
MIN_OI_USD = 500_000
MIN_ATR_PCT = 0.0025
MAX_ATR_PCT = 0.09
MAX_SPREAD_PCT = 0.0012
MIN_REL_VOLUME = 0.55
MAX_ENTRY_DRIFT_R = 0.5

FUNDING_EXTREME = 0.0006
OI_DIVERGENCE_LOOKBACK = 8

TOP_N_PER_SCAN = 4
MAX_SAME_DIRECTION = 3
MAX_PER_SECTOR = 1
MAX_CONCURRENT = 10
MAX_PORTFOLIO_RISK_PCT = 12.0
PER_TRADE_RISK_PCT = 1.0
DAILY_LOSS_LIMIT_PCT = 6.0

CORR_LOOKBACK = 72
CORR_THRESHOLD = 0.72
DEDUP_HOURS = 6
DEDUP_PRICE_TOL = 0.006

ENSEMBLE_BONUS_3 = 0.9
ENSEMBLE_BONUS_4 = 1.6
ENSEMBLE_CONFLICT = -1.3

TAKER_FEE = 0.00045
SLIPPAGE_PCT = 0.0006
MIN_SAMPLE = 20
BACKTEST_SYMBOLS = WATCHLIST

HL_RPS = 8.0
FETCH_WORKERS = 6

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stderr)],
)
log = logging.getLogger(ENGINE_NAME)

# ==============================================================================
# HELPERS
# ==============================================================================

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def safe(x, default: float = 0.0) -> float:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return default
        return float(x)
    except (TypeError, ValueError):
        return default


def pct(a: float, b: float) -> float:
    return safe((a - b) / b) if b else 0.0


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_day_key(dt: Optional[datetime] = None) -> str:
    dt = dt or now_utc()
    return dt.strftime("%Y-%m-%d")


def log_suppressed(symbol: str, direction: str, pathway: str, reason: str, conf: float = 0.0) -> None:
    log.info("SUPPRESSED | %s %s | %s | %s | conf=%.1f", symbol, direction, pathway, reason, conf)


# ==============================================================================
# HYPERLIQUID API
# ==============================================================================

class _Pacer:
    def __init__(self, rps: float):
        self._gap = 1.0 / rps
        self._last = 0.0

    def wait(self):
        now = time.monotonic()
        d = now - self._last
        if d < self._gap:
            time.sleep(self._gap - d)
        self._last = time.monotonic()


_pacer = _Pacer(HL_RPS)


def hl_post(body: dict, timeout: float = 12.0):
    for attempt in range(4):
        _pacer.wait()
        try:
            r = requests.post(HL_INFO_URL, json=body, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            log.warning("HL %s HTTP %s", body.get("type"), r.status_code)
        except requests.RequestException as e:
            log.warning("HL %s error: %s", body.get("type"), e)
        time.sleep(min(2 ** attempt, 8))
    return None


def fetch_candles(symbol: str, interval: str, n: int, end_ms: Optional[int] = None) -> Optional[list[dict]]:
    end_ms = end_ms or int(time.time() * 1000)
    start_ms = end_ms - n * INTERVAL_MS[interval]
    data = hl_post({"type": "candleSnapshot", "req": {
        "coin": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms,
    }})
    if not data or not isinstance(data, list):
        return None
    out = []
    for c in data:
        try:
            out.append({"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                        "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])})
        except (KeyError, TypeError, ValueError):
            continue
    return out or None


def load_market_ctx() -> dict[str, dict]:
    data = hl_post({"type": "metaAndAssetCtxs"})
    ctx: dict[str, dict] = {}
    if not data or len(data) < 2:
        return ctx
    universe = data[0].get("universe", [])
    asset_ctxs = data[1]
    for i, u in enumerate(universe):
        if i >= len(asset_ctxs):
            break
        name = u.get("name")
        a = asset_ctxs[i]
        try:
            mark = float(a.get("markPx", 0))
            ctx[name] = {
                "funding": float(a.get("funding", 0)),
                "open_interest": float(a.get("openInterest", 0)),
                "mark_px": mark,
                "oi_usd": float(a.get("openInterest", 0)) * mark,
                "day_ntl_vlm": float(a.get("dayNtlVlm", 0)),
            }
        except (TypeError, ValueError):
            continue
    return ctx


def fetch_l2(symbol: str) -> Optional[dict]:
    return hl_post({"type": "l2Book", "coin": symbol})


def book_metrics(book: Optional[dict]) -> tuple[float, float]:
    if not book or "levels" not in book:
        return 0.01, 0.0
    try:
        bids, asks = book["levels"][0], book["levels"][1]
        if not bids or not asks:
            return 0.01, 0.0
        bb, ba = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (bb + ba) / 2
        spread = (ba - bb) / mid if mid else 0.01
        depth = sum(float(x["px"]) * float(x["sz"]) for x in bids[:5])
        depth += sum(float(x["px"]) * float(x["sz"]) for x in asks[:5])
        return spread, depth
    except (KeyError, IndexError, TypeError, ValueError):
        return 0.01, 0.0


def fetch_bundle(symbol: str) -> Optional[dict]:
    try:
        c15 = fetch_candles(symbol, "15m", CANDLE_COUNTS["15m"])
        c1h = fetch_candles(symbol, "1h", CANDLE_COUNTS["1h"])
        c4h = fetch_candles(symbol, "4h", CANDLE_COUNTS["4h"])
        c1d = fetch_candles(symbol, "1d", CANDLE_COUNTS["1d"])
        if not all([c15, c1h, c4h, c1d]):
            log.info("[DATA] %s incomplete candles — skip", symbol)
            return None
        return {"15m": c15, "1h": c1h, "4h": c4h, "1d": c1d, "book": fetch_l2(symbol)}
    except Exception as e:
        log.warning("[DATA] %s fetch error: %s", symbol, e)
        return None


def fetch_all_mids() -> dict[str, float]:
    data = hl_post({"type": "allMids"})
    if not isinstance(data, dict):
        return {}
    return {k: float(v) for k, v in data.items() if _try_float(v)}


def _try_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ==============================================================================
# INDICATORS
# ==============================================================================

def ema(vals: list[float], period: int) -> list[float]:
    if not vals:
        return []
    k = 2 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def true_range(candles: list[dict]) -> list[float]:
    tr = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    return tr


def atr_series(candles: list[dict], period: int = 14) -> list[float]:
    return ema(true_range(candles), period)


def rsi(closes: list[float], period: int = 14) -> list[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag, al = ema(gains, period), ema(losses, period)
    out = []
    for g, l in zip(ag, al):
        out.append(100.0 if l == 0 else 100 - 100 / (1 + g / l))
    return out


def adx(candles: list[dict], period: int = 14) -> list[float]:
    if len(candles) < period + 2:
        return [15.0] * len(candles)
    pdm, mdm = [0.0], [0.0]
    for i in range(1, len(candles)):
        up = candles[i]["h"] - candles[i - 1]["h"]
        dn = candles[i - 1]["l"] - candles[i]["l"]
        pdm.append(up if up > dn and up > 0 else 0.0)
        mdm.append(dn if dn > up and dn > 0 else 0.0)
    tr = true_range(candles)
    atr_s = ema(tr, period)
    pdi = [100 * (p / a) if a else 0 for p, a in zip(ema(pdm, period), atr_s)]
    mdi = [100 * (m / a) if a else 0 for m, a in zip(ema(mdm, period), atr_s)]
    dx = [100 * abs(p - m) / (p + m) if (p + m) else 0 for p, m in zip(pdi, mdi)]
    return ema(dx, period)


def bb_width(closes: list[float], period: int = 20, n_std: float = 2.0) -> list[float]:
    out = []
    for i in range(len(closes)):
        w = closes[max(0, i - period + 1):i + 1]
        if len(w) < 2:
            out.append(0.0)
            continue
        mid = sum(w) / len(w)
        sd = statistics.pstdev(w)
        out.append(safe((mid + n_std * sd - (mid - n_std * sd)) / mid) if mid else 0.0)
    return out


def bb_pctile(bw: list[float], lookback: int = 100) -> float:
    hist = bw[-lookback:] if len(bw) >= 5 else bw
    if len(hist) < 5:
        return 0.5
    cur = hist[-1]
    return sum(1 for x in hist if x <= cur) / len(hist)


def noise_index(closes: list[float], ema_mid: list[float], atr_vals: list[float], lb: int = 20) -> float:
    n = min(lb, len(closes))
    if n < 5:
        return 0.5
    devs = [abs(closes[-i] - ema_mid[-i]) for i in range(1, n + 1)]
    a = atr_vals[-1] if atr_vals and atr_vals[-1] else 1e-9
    return clamp((sum(devs) / n) / a / 1.5, 0.0, 1.0)


def donchian(candles: list[dict], period: int = 20):
    highs, lows = [c["h"] for c in candles], [c["l"] for c in candles]
    up, dn = [], []
    for i in range(len(candles)):
        lo = max(0, i - period + 1)
        up.append(max(highs[lo:i + 1]))
        dn.append(min(lows[lo:i + 1]))
    return up, dn


def swing_points(candles: list[dict], lb: int = 3):
    hi_idx, lo_idx = [], []
    n = len(candles)
    for i in range(lb, n - lb):
        seg = candles[i - lb:i + lb + 1]
        if candles[i]["h"] == max(c["h"] for c in seg):
            hi_idx.append(i)
        if candles[i]["l"] == min(c["l"] for c in seg):
            lo_idx.append(i)
    return hi_idx, lo_idx


def detect_sweep(candles: list[dict], lookback: int = 20) -> Optional[dict]:
    if len(candles) < lookback + 2:
        return None
    seg = candles[-(lookback + 6):]
    hi_idx, lo_idx = swing_points(seg, 2)
    last = seg[-1]
    for hi in reversed(hi_idx[:-1] if hi_idx else []):
        lvl = seg[hi]["h"]
        if last["h"] > lvl and last["c"] < lvl:
            return {"direction": "bearish", "level": lvl}
    for li in reversed(lo_idx[:-1] if lo_idx else []):
        lvl = seg[li]["l"]
        if last["l"] < lvl and last["c"] > lvl:
            return {"direction": "bullish", "level": lvl}
    return None


def detect_bos(candles: list[dict]) -> dict:
    hi_idx, lo_idx = swing_points(candles, 3)
    if len(hi_idx) < 2 or len(lo_idx) < 2:
        return {"bias": "neutral", "event": None}
    last = candles[-1]["c"]
    lh, ll = candles[hi_idx[-1]]["h"], candles[lo_idx[-1]]["l"]
    if last > lh:
        return {"bias": "bullish", "event": "bos_up", "level": lh}
    if last < ll:
        return {"bias": "bearish", "event": "bos_down", "level": ll}
    return {"bias": "neutral", "event": None}


def order_blocks(candles: list[dict], atr_vals: list[float], lb: int = 40) -> list[dict]:
    out = []
    seg = candles[-lb:]
    for i in range(1, len(seg) - 1):
        body = seg[i]["c"] - seg[i]["o"]
        nxt = seg[i + 1]["c"] - seg[i + 1]["o"]
        a = atr_vals[i] if i < len(atr_vals) else 1e-9
        if abs(nxt) > 1.5 * a and abs(body) > 0:
            prev = seg[i]
            if nxt > 0 and prev["c"] < prev["o"]:
                out.append({"type": "bullish", "top": prev["h"], "bottom": prev["l"]})
            elif nxt < 0 and prev["c"] > prev["o"]:
                out.append({"type": "bearish", "top": prev["h"], "bottom": prev["l"]})
    return out


def volume_profile(candles: list[dict], bins: int = 24) -> dict:
    seg = candles[-96:] if len(candles) > 96 else candles
    if not seg:
        return {"poc": None, "va_high": None, "va_low": None}
    lo, hi = min(c["l"] for c in seg), max(c["h"] for c in seg)
    if hi <= lo:
        return {"poc": None, "va_high": None, "va_low": None}
    w = (hi - lo) / bins
    vol = [0.0] * bins
    for c in seg:
        vol[clamp(int((c["c"] - lo) / w), 0, bins - 1)] += c["v"]
    poc_i = max(range(bins), key=lambda i: vol[i])
    poc = lo + (poc_i + 0.5) * w
    return {"poc": poc, "va_high": lo + (poc_i + 2) * w, "va_low": lo + max(0, poc_i - 1) * w}


def vwap(candles: list[dict]) -> float:
    seg = candles[-96:] if len(candles) > 96 else candles
    num = sum((c["h"] + c["l"] + c["c"]) / 3 * c["v"] for c in seg)
    den = sum(c["v"] for c in seg) or 1.0
    return num / den


def rel_volume(candles: list[dict], lb: int = 40) -> float:
    if len(candles) < lb + 1:
        return 1.0
    hist = [c["v"] for c in candles[-(lb + 1):-1]]
    med = statistics.median(hist) if hist else 1.0
    return safe(candles[-1]["v"] / med, 1.0) if med else 1.0


_IND_CACHE: dict = {}


def get_indicators(symbol: str, tf: str, candles: list[dict]) -> dict:
    key = (symbol, tf, candles[-1]["t"] if candles else 0)
    if key in _IND_CACHE:
        return _IND_CACHE[key]
    closes = [c["c"] for c in candles]
    atr_v = atr_series(candles, 14)
    bw = bb_width(closes, 20)
    ind = {
        "closes": closes, "atr": atr_v,
        "ema_fast": ema(closes, 20), "ema_mid": ema(closes, 50),
        "ema_slow": ema(closes, 200) if len(closes) >= 200 else ema(closes, max(20, len(closes) // 2)),
        "rsi": rsi(closes), "adx": adx(candles),
        "bb_width": bw, "bb_pctile": bb_pctile(bw),
        "structure": detect_bos(candles), "sweep": detect_sweep(candles),
        "obs": order_blocks(candles, atr_v), "vp": volume_profile(candles),
        "vwap": vwap(candles), "rel_vol": rel_volume(candles),
        "don_up": donchian(candles)[0], "don_dn": donchian(candles)[1],
    }
    ind["noise"] = noise_index(closes, ind["ema_mid"], atr_v)
    _IND_CACHE[key] = ind
    return ind


def clear_ind_cache():
    _IND_CACHE.clear()


def atr_pct(ind: dict) -> float:
    c, a = ind["closes"][-1], ind["atr"][-1]
    return safe(a / c) if c else 0.0


# ==============================================================================
# REGIME (fixed rule table — no live self-tuning)
# ==============================================================================

_BREADTH: list[bool] = []


def reset_breadth():
    global _BREADTH
    _BREADTH = []


def record_breadth(above: bool):
    _BREADTH.append(above)


def market_breadth() -> float:
    return sum(_BREADTH) / len(_BREADTH) if _BREADTH else 0.5


def compute_btc_regime(bundle: dict) -> tuple[str, float]:
    ind = get_indicators(BTC_SYMBOL, "4h", bundle["4h"])
    adx_v, price = ind["adx"][-1], ind["closes"][-1]
    if price > ind["ema_mid"][-1] > ind["ema_slow"][-1] and adx_v > 20:
        return "bullish", adx_v
    if price < ind["ema_mid"][-1] < ind["ema_slow"][-1] and adx_v > 20:
        return "bearish", adx_v
    return "neutral", adx_v


def funding_oi_regime(symbol: str, ctx: dict, bundle: dict) -> dict:
    info = ctx.get(symbol, {})
    funding = safe(info.get("funding"))
    tag = None
    if funding > FUNDING_EXTREME:
        tag = "funding_extreme_long"
    elif funding < -FUNDING_EXTREME:
        tag = "funding_extreme_short"
    ind1h = get_indicators(symbol, "1h", bundle["1h"])
    closes = ind1h["closes"]
    slope = pct(closes[-1], closes[-min(OI_DIVERGENCE_LOOKBACK, len(closes) - 1)])
    div = None
    if abs(slope) > 0.01:
        if slope > 0 and funding < 0:
            div = "bullish_oi_funding_divergence"
        elif slope < 0 and funding > 0:
            div = "bearish_oi_funding_divergence"
    return {"funding": funding, "extreme_tag": tag, "divergence": div}


def vol_pctile(ind: dict) -> float:
    hist = ind["atr"][-60:] if len(ind["atr"]) >= 60 else ind["atr"]
    if len(hist) < 5:
        return 0.5
    cur = hist[-1]
    return sum(1 for x in hist if x <= cur) / len(hist)


@dataclass
class RegimeVector:
    btc_bias: str
    btc_strength: float
    breadth: float
    symbol_regime: str = "neutral"
    bb_pctile: float = 0.5
    noise: float = 0.5


def classify_regime(adx_v: float, bb_p: float, noise: float, atr_p: float) -> str:
    if atr_p > 0.85:
        return "high_vol"
    if adx_v >= 25 and noise < 0.45:
        return "clean"
    if adx_v < 15 and (bb_p < 0.20 or noise >= 0.65):
        return "choppy"
    return "neutral"


def build_regime(btc_bias: str, btc_strength: float, symbol: str, bundle: dict) -> RegimeVector:
    ind = get_indicators(symbol, "1h", bundle["1h"])
    sym_reg = classify_regime(ind["adx"][-1], ind["bb_pctile"], ind["noise"], vol_pctile(ind))
    return RegimeVector(btc_bias, btc_strength, market_breadth(), sym_reg, ind["bb_pctile"], ind["noise"])


def confidence_floor(regime: RegimeVector, macro_hot: bool = False) -> float:
    floor = BASE_CONFIDENCE_FLOOR
    if regime.symbol_regime == "clean":
        floor -= TREND_REGIME_RELIEF
    elif regime.symbol_regime == "choppy":
        floor += CHOP_REGIME_PENALTY
    if macro_hot:
        floor += MACRO_EVENT_PENALTY
    if now_utc().weekday() >= 5:
        floor += WEEKEND_PENALTY
    if regime.btc_strength > 30:
        floor -= 2.0
    return clamp(floor, 45.0, 85.0)


def liquidity_mult(regime: RegimeVector) -> float:
    if regime.symbol_regime == "clean":
        return 0.75
    if regime.symbol_regime == "choppy":
        return 1.40
    return 1.0


def macro_hot_flag() -> bool:
    return False  # no third-party calendar — Hyperliquid-only data policy


# ==============================================================================
# PIPELINES & PATHWAYS
# ==============================================================================

@dataclass
class Pipeline:
    id: str
    label: str
    bias_tf: str
    trigger_tf: str


PIPELINES = {
    "fast": Pipeline("fast", "Intraday 4H/15m", "4h", "15m"),
    "slow": Pipeline("slow", "Swing 1D/1h", "1d", "1h"),
}


@dataclass
class Candidate:
    symbol: str
    direction: str
    pathway: str
    pipeline_id: str
    entry: float
    stop: float
    tp1: float
    tp2: float
    rr: float
    tags: list = field(default_factory=list)


def clip_tp(direction: str, price: float, tp1: float, tp2: float, ind: dict) -> tuple[float, float]:
    levels = []
    vp = ind["vp"]
    for k in ("va_high", "va_low", "poc"):
        if vp.get(k):
            levels.append(vp[k])
    for ob in ind["obs"]:
        levels.extend([ob["top"], ob["bottom"]])
    if direction == "long":
        above = [x for x in levels if x > price]
        if above:
            near = min(above)
            if near < tp1:
                tp1 = max(tp1 * 0.65, near)
    else:
        below = [x for x in levels if x < price]
        if below:
            near = max(below)
            if near > tp1:
                tp1 = min(tp1 * 1.35, near) if tp1 > 0 else near
    return tp1, tp2


def pathway_liquidity_reversal(symbol: str, pipe: Pipeline, bundle: dict) -> Optional[Candidate]:
    ind = get_indicators(symbol, pipe.trigger_tf, bundle[pipe.trigger_tf])
    sweep = ind["sweep"]
    if not sweep:
        return None
    direction = "long" if sweep["direction"] == "bullish" else "short"
    struct = ind["structure"]
    want = "bullish" if direction == "long" else "bearish"
    if struct["bias"] not in ("neutral", want):
        return None
    price, a = ind["closes"][-1], ind["atr"][-1]
    lvl = sweep["level"]
    if direction == "long":
        stop = min(lvl, price - a) - 0.15 * a
        risk = price - stop
        tp1, tp2 = price + risk * 1.5, price + risk * 2.5
    else:
        stop = max(lvl, price + a) + 0.15 * a
        risk = stop - price
        tp1, tp2 = price - risk * 1.5, price - risk * 2.5
    if risk <= 0:
        return None
    tp1, tp2 = clip_tp(direction, price, tp1, tp2, ind)
    rr = safe(abs(tp1 - price) / risk)
    return Candidate(symbol, direction, "liquidity_reversal", pipe.id, price, stop, tp1, tp2, rr,
                     ["sfp_sweep", struct["event"] or "structure"])


def pathway_trend_continuation(symbol: str, pipe: Pipeline, bundle: dict) -> Optional[Candidate]:
    ind_b = get_indicators(symbol, pipe.bias_tf, bundle[pipe.bias_tf])
    ind = get_indicators(symbol, pipe.trigger_tf, bundle[pipe.trigger_tf])
    c_b, em_m, em_s = ind_b["closes"][-1], ind_b["ema_mid"][-1], ind_b["ema_slow"][-1]
    if c_b > em_m > em_s:
        bias = "bullish"
    elif c_b < em_m < em_s:
        bias = "bearish"
    else:
        return None
    direction = "long" if bias == "bullish" else "short"
    price, a = ind["closes"][-1], ind["atr"][-1]
    zones = [z for z in ind["obs"] if z["type"] == ("bullish" if direction == "long" else "bearish")]
    if not zones:
        return None
    z = zones[-1]
    if direction == "long" and not (z["bottom"] * 0.998 <= price <= z["top"] * 1.01):
        return None
    if direction == "short" and not (z["bottom"] * 0.99 <= price <= z["top"] * 1.002):
        return None
    if direction == "long":
        stop = z["bottom"] - 0.2 * a
        risk = price - stop
        tp1, tp2 = price + risk * 1.6, price + risk * 2.8
    else:
        stop = z["top"] + 0.2 * a
        risk = stop - price
        tp1, tp2 = price - risk * 1.6, price - risk * 2.8
    if risk <= 0:
        return None
    tp1, tp2 = clip_tp(direction, price, tp1, tp2, ind)
    rr = safe(abs(tp1 - price) / risk)
    return Candidate(symbol, direction, "trend_continuation", pipe.id, price, stop, tp1, tp2, rr,
                     ["htf_pullback", "order_block"])


def pathway_momentum_breakout(symbol: str, pipe: Pipeline, bundle: dict, follow_bars: int = 1) -> Optional[Candidate]:
    ind = get_indicators(symbol, pipe.trigger_tf, bundle[pipe.trigger_tf])
    candles = bundle[pipe.trigger_tf]
    if len(candles) < 25:
        return None
    price, a = ind["closes"][-1], ind["atr"][-1]
    prior_up, prior_dn = ind["don_up"][-2], ind["don_dn"][-2]
    recent = candles[-follow_bars:]
    broke_up = price > prior_up and all(c["c"] > prior_up for c in recent)
    broke_dn = price < prior_dn and all(c["c"] < prior_dn for c in recent)
    if not broke_up and not broke_dn:
        return None
    if ind["rel_vol"] < 1.2:
        return None
    direction = "long" if broke_up else "short"
    level = prior_up if direction == "long" else prior_dn
    last = candles[-1]
    if direction == "long" and last["c"] <= level:
        return None
    if direction == "short" and last["c"] >= level:
        return None
    if direction == "long":
        stop = level - 0.3 * a
        risk = price - stop
        tp1, tp2 = price + risk * 1.4, price + risk * 2.4
    else:
        stop = level + 0.3 * a
        risk = stop - price
        tp1, tp2 = price - risk * 1.4, price - risk * 2.4
    if risk <= 0:
        return None
    tp1, tp2 = clip_tp(direction, price, tp1, tp2, ind)
    rr = safe(abs(tp1 - price) / risk)
    return Candidate(symbol, direction, "momentum_breakout", pipe.id, price, stop, tp1, tp2, rr,
                     ["donchian_break", "volume_confirmed"])


PATHWAYS = {
    "liquidity_reversal": pathway_liquidity_reversal,
    "trend_continuation": pathway_trend_continuation,
    "momentum_breakout": pathway_momentum_breakout,
}


# ==============================================================================
# ENSEMBLE SCORING
# ==============================================================================

def family_votes(symbol: str, direction: str, pipe: Pipeline, bundle: dict) -> tuple[list[str], list[str]]:
    ind = get_indicators(symbol, pipe.trigger_tf, bundle[pipe.trigger_tf])
    agree, conflict = [], []
    trend = "long" if ind["closes"][-1] > ind["ema_mid"][-1] else "short"
    (agree if trend == direction else conflict).append("trend")
    r = ind["rsi"][-1]
    mom = "long" if r > 52 else ("short" if r < 48 else None)
    if mom:
        (agree if mom == direction else conflict).append("momentum")
    last = bundle[pipe.trigger_tf][-1]
    cd = "long" if last["c"] >= last["o"] else "short"
    if ind["rel_vol"] > 1.0:
        (agree if cd == direction else conflict).append("volume")
    sb = {"bullish": "long", "bearish": "short"}.get(ind["structure"]["bias"])
    if sb:
        (agree if sb == direction else conflict).append("structure")
    return agree, conflict


def ensemble_adj(agree: list[str], conflict: list[str]) -> tuple[float, str]:
    if len(conflict) >= 2:
        return ENSEMBLE_CONFLICT, "conflict"
    if len(agree) >= 4:
        return ENSEMBLE_BONUS_4, "full_agreement"
    if len(agree) >= 3:
        return ENSEMBLE_BONUS_3, "strong_agreement"
    return 0.0, "mixed"


def logistic(z: float) -> float:
    return 100.0 / (1.0 + math.exp(-z))


PATHWAY_WEIGHT = {"liquidity_reversal": 1.0, "trend_continuation": 0.85, "momentum_breakout": 0.8}


def score_candidate(cand: Candidate, regime: RegimeVector, funding: dict,
                    agree: list[str], conflict: list[str], spread: float, depth: float,
                    vwap_val: float) -> tuple[float, dict]:
    z: dict[str, float] = {"base": 0.4}
    z["pathway"] = (PATHWAY_WEIGHT[cand.pathway] - 0.8) * 1.5
    z["rr"] = clamp((cand.rr - MIN_RR) * 0.8, -0.5, 1.4)
    btc_dir = {"bullish": "long", "bearish": "short"}.get(regime.btc_bias)
    sec = SECTOR_MAP.get(cand.symbol, "other")
    if btc_dir and sec not in BTC_REGIME_EXEMPT:
        z["btc"] = 0.7 if btc_dir == cand.direction else -0.9
    else:
        z["btc"] = 0.0
    br = (regime.breadth - 0.5) * 2.0
    z["breadth"] = clamp(br if cand.direction == "long" else -br, -0.6, 0.6)
    z["ensemble"] = ensemble_adj(agree, conflict)[0]
    z["funding"] = 0.0
    if funding.get("extreme_tag") == "funding_extreme_short" and cand.direction == "long":
        z["funding"] += 0.5
    elif funding.get("extreme_tag") == "funding_extreme_long" and cand.direction == "short":
        z["funding"] += 0.5
    elif funding.get("extreme_tag"):
        z["funding"] -= 0.4
    if funding.get("divergence") == "bullish_oi_funding_divergence" and cand.direction == "long":
        z["funding"] += 0.35
    elif funding.get("divergence") == "bearish_oi_funding_divergence" and cand.direction == "short":
        z["funding"] += 0.35
    if spread > MAX_SPREAD_PCT * 0.6:
        z["liq"] = -0.3
    else:
        z["liq"] = 0.0
    if depth < MIN_OI_USD * 0.3:
        z["liq"] = z.get("liq", 0) - 0.2
    z["vwap"] = 0.4 if ((cand.entry >= vwap_val and cand.direction == "long") or
                        (cand.entry <= vwap_val and cand.direction == "short")) else -0.25
    return logistic(sum(z.values()) * 1.6), z


def grade_for(conf: float) -> str:
    if conf >= 82:
        return "A+"
    if conf >= 72:
        return "A"
    if conf >= 62:
        return "B"
    if conf >= 52:
        return "C"
    return "D"


# ==============================================================================
# FILTERS
# ==============================================================================

def liquidity_ok(symbol: str, ctx: dict, ind: dict, spread: float, depth: float, mult: float) -> bool:
    info = ctx.get(symbol, {})
    oi = safe(info.get("oi_usd"))
    if oi < MIN_OI_USD * mult:
        return False
    ap = atr_pct(ind)
    if not (MIN_ATR_PCT <= ap <= MAX_ATR_PCT):
        return False
    if spread > MAX_SPREAD_PCT:
        return False
    if depth < MIN_OI_USD * 0.3 * mult:
        return False
    if ind["rel_vol"] < MIN_REL_VOLUME * min(mult, 1.15):
        return False
    return True


def stop_ok(cand: Candidate) -> bool:
    if not cand.entry:
        return False
    return abs(cand.entry - cand.stop) / cand.entry >= MIN_STOP_PCT


def freshness_ok(cand: Candidate, live: Optional[float]) -> bool:
    if live is None:
        return True
    risk = abs(cand.entry - cand.stop)
    if risk <= 0:
        return False
    return abs(live - cand.entry) / risk <= MAX_ENTRY_DRIFT_R


def widen_high_vol(cand: Candidate) -> Candidate:
    entry, risk = cand.entry, abs(cand.entry - cand.stop)
    w = 1.35
    nr = risk * w
    if cand.direction == "long":
        cand.stop = entry - nr
        cand.tp1 = entry + (cand.tp1 - entry) * w
        cand.tp2 = entry + (cand.tp2 - entry) * w
    else:
        cand.stop = entry + nr
        cand.tp1 = entry - (entry - cand.tp1) * w
        cand.tp2 = entry - (entry - cand.tp2) * w
    cand.rr = safe(abs(cand.tp1 - entry) / nr) if nr else 0
    return cand


# ==============================================================================
# CORRELATION & PORTFOLIO
# ==============================================================================

class UnionFind:
    def __init__(self, items):
        self.p = {i: i for i in items}

    def find(self, x):
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[ra] = rb


def corr_clusters(hourly: dict[str, list[dict]]) -> dict[str, int]:
    rets: dict[str, list[float]] = {}
    for sym, candles in hourly.items():
        seg = candles[-CORR_LOOKBACK:]
        if len(seg) < 10:
            continue
        rets[sym] = [pct(seg[i]["c"], seg[i - 1]["c"]) for i in range(1, len(seg))]
    syms = list(rets.keys())
    uf = UnionFind(syms)
    for i in range(len(syms)):
        for j in range(i + 1, len(syms)):
            a, b = rets[syms[i]], rets[syms[j]]
            n = min(len(a), len(b))
            if n < 10:
                continue
            try:
                if abs(statistics.correlation(a[-n:], b[-n:])) >= CORR_THRESHOLD:
                    uf.union(syms[i], syms[j])
    ids: dict[str, int] = {}
    nxt = 0
    root_map: dict[str, int] = {}
    for s in syms:
        r = uf.find(s)
        if r not in root_map:
            root_map[r] = nxt
            nxt += 1
        ids[s] = root_map[r]
    return ids


@dataclass
class Signal:
    candidate: Candidate
    confidence: float
    grade: str
    ensemble: str
    tags: list
    ts: str


def dedup_corr(signals: list[Signal], clusters: dict[str, int]) -> list[Signal]:
    best: dict[tuple, Signal] = {}
    for s in signals:
        k = (clusters.get(s.candidate.symbol, hash(s.candidate.symbol)), s.candidate.direction)
        if k not in best or s.confidence > best[k].confidence:
            best[k] = s
    return sorted(best.values(), key=lambda x: x.confidence, reverse=True)


def dedup_symbol(signals: list[Signal]) -> list[Signal]:
    best: dict[str, Signal] = {}
    for s in signals:
        sym = s.candidate.symbol
        if sym not in best or s.confidence > best[sym].confidence:
            best[sym] = s
    return sorted(best.values(), key=lambda x: x.confidence, reverse=True)


def apply_caps(signals: list[Signal], state: dict) -> list[Signal]:
    out: list[Signal] = []
    dirs = Counter()
    sectors = Counter()
    active_n = len(state.get("active_signals", []))
    risk = sum(a.get("risk_pct", PER_TRADE_RISK_PCT) for a in state.get("active_signals", []))
    for s in signals:
        if len(out) >= TOP_N_PER_SCAN:
            break
        if active_n >= MAX_CONCURRENT:
            break
        if risk + PER_TRADE_RISK_PCT > MAX_PORTFOLIO_RISK_PCT:
            break
        d, sec = s.candidate.direction, SECTOR_MAP.get(s.candidate.symbol, "other")
        if dirs[d] >= MAX_SAME_DIRECTION or sectors[sec] >= MAX_PER_SECTOR:
            continue
        out.append(s)
        dirs[d] += 1
        sectors[sec] += 1
        active_n += 1
        risk += PER_TRADE_RISK_PCT
    return out


# ==============================================================================
# STATE
# ==============================================================================

def default_state() -> dict:
    return {"active_signals": [], "history": [], "daily": {}, "last_summary_day": None,
            "dedup": [], "suppressed_log": []}


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return default_state()
    try:
        with open(STATE_PATH, "r") as f:
            st = json.load(f)
        for k, v in default_state().items():
            st.setdefault(k, v)
        return st
    except (json.JSONDecodeError, OSError) as e:
        log.warning("state load failed (%s), trying .bak", e)
        try:
            with open(STATE_PATH + ".bak", "r") as f:
                return json.load(f)
        except OSError:
            return default_state()


def save_state(state: dict):
    if DRY_RUN:
        log.info("[DRY-RUN] state commit suppressed")
        return
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r") as src, open(STATE_PATH + ".bak", "w") as dst:
                dst.write(src.read())
    except OSError:
        pass
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_PATH)


def daily_bucket(state: dict) -> dict:
    day = utc_day_key()
    d = state.setdefault("daily", {})
    if day not in d:
        d[day] = {"signal_count": 0, "realized_pnl_pct": 0.0, "paused": False}
    for k in list(d.keys()):
        if k != day and isinstance(k, str):
            del d[k]
    return d[day]


def daily_loss_paused(state: dict) -> bool:
    b = daily_bucket(state)
    return b.get("realized_pnl_pct", 0) <= -DAILY_LOSS_LIMIT_PCT or b.get("paused", False)


def is_duplicate(state: dict, cand: Candidate) -> bool:
    now = time.time()
    for e in state.get("dedup", []):
        if e.get("symbol") != cand.symbol or e.get("direction") != cand.direction:
            continue
        if now - e.get("ts", 0) > DEDUP_HOURS * 3600:
            continue
        if abs(cand.entry - e.get("entry", 0)) / max(cand.entry, 1e-9) <= DEDUP_PRICE_TOL:
            return True
    return False


def record_dedup(state: dict, cand: Candidate):
    state.setdefault("dedup", []).append({"symbol": cand.symbol, "direction": cand.direction,
                                          "entry": cand.entry, "ts": time.time()})
    state["dedup"] = state["dedup"][-200:]


# ==============================================================================
# TELEGRAM
# ==============================================================================

def send_telegram(text: str) -> Optional[int]:
    if DRY_RUN:
        log.info("[DRY-RUN] Telegram:\n%s", text)
        return None
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.warning("Telegram not configured")
        return None
    try:
        r = requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        if r.status_code == 200:
            return r.json().get("result", {}).get("message_id")
    except requests.RequestException as e:
        log.error("Telegram error: %s", e)
    return None


def react_tg(msg_id: Optional[int], emoji: str):
    if DRY_RUN or not msg_id or not TG_BOT_TOKEN:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction",
                      json={"chat_id": TG_CHAT_ID, "message_id": msg_id,
                            "reaction": [{"type": "emoji", "emoji": emoji}]}, timeout=8)
    except requests.RequestException:
        pass


def fmt_px(v: float) -> str:
    if v >= 100:
        return f"{v:.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}"


def format_signal(sig: Signal) -> str:
    c = sig.candidate
    arrow = "🟢 LONG" if c.direction == "long" else "🔴 SHORT"
    confs = ", ".join(c.tags[:6])
    return (
        f"<i>{ENGINE_NAME} v{VERSION}</i>\n"
        f"<b>{arrow} — {c.symbol}</b>  Grade: <b>{sig.grade}</b>\n"
        f"{PIPELINES[c.pipeline_id].label} | {c.pathway.replace('_', ' ')}\n\n"
        f"Entry: <code>{fmt_px(c.entry)}</code>\n"
        f"Stop Loss: <code>{fmt_px(c.stop)}</code>\n"
        f"TP1: <code>{fmt_px(c.tp1)}</code>\n"
        f"TP2: <code>{fmt_px(c.tp2)}</code>\n"
        f"R:R: {c.rr:.2f}\n"
        f"Confidence: <b>{sig.confidence:.1f}</b>/100\n"
        f"Ensemble: {sig.ensemble}\n"
        f"Confluences: {confs}"
    )


def track_signal(state: dict, sig: Signal, msg_id: Optional[int]):
    c = sig.candidate
    state["active_signals"].append({
        "id": str(uuid.uuid4()), "symbol": c.symbol, "direction": c.direction,
        "pathway": c.pathway, "pipeline": c.pipeline_id,
        "entry": c.entry, "stop": c.stop, "tp1": c.tp1, "tp2": c.tp2,
        "confidence": sig.confidence, "risk_pct": PER_TRADE_RISK_PCT,
        "msg_id": msg_id, "opened_ts": time.time(), "tp1_hit": False,
    })
    daily_bucket(state)["signal_count"] += 1
    state.setdefault("history", []).append({
        "symbol": c.symbol, "direction": c.direction, "pathway": c.pathway,
        "confidence": sig.confidence, "timestamp": sig.ts, "outcome": "open",
    })


def check_active(state: dict, mids: dict[str, float]):
    still = []
    for sig in state.get("active_signals", []):
        sym, d = sig["symbol"], sig["direction"]
        px = mids.get(sym)
        if px is None:
            still.append(sig)
            continue
        hit_sl = (px <= sig["stop"]) if d == "long" else (px >= sig["stop"])
        hit_tp2 = (px >= sig["tp2"]) if d == "long" else (px <= sig["tp2"])
        hit_tp1 = (px >= sig["tp1"]) if d == "long" else (px <= sig["tp1"])
        if hit_sl:
            result = "breakeven" if sig.get("tp1_hit") else "loss"
            pnl = 0.0 if result == "breakeven" else -PER_TRADE_RISK_PCT
            daily_bucket(state)["realized_pnl_pct"] += pnl
            if daily_bucket(state)["realized_pnl_pct"] <= -DAILY_LOSS_LIMIT_PCT:
                daily_bucket(state)["paused"] = True
            react_tg(sig.get("msg_id"), "👍" if result == "breakeven" else "😭")
            state["history"].append({**sig, "outcome": result, "closed_ts": time.time()})
            continue
        if hit_tp2:
            rr = abs(sig["tp2"] - sig["entry"]) / max(abs(sig["entry"] - sig["stop"]), 1e-9)
            daily_bucket(state)["realized_pnl_pct"] += PER_TRADE_RISK_PCT * rr
            react_tg(sig.get("msg_id"), "🏆")
            state["history"].append({**sig, "outcome": "win", "closed_ts": time.time()})
            continue
        if hit_tp1 and not sig.get("tp1_hit"):
            sig["tp1_hit"] = True
            sig["stop"] = sig["entry"]
            react_tg(sig.get("msg_id"), "🔥")
            still.append(sig)
            continue
        still.append(sig)
    state["active_signals"] = still


# ==============================================================================
# EVALUATION
# ==============================================================================

def evaluate_symbol(symbol: str, bundle: dict, ctx: dict, state: dict,
                    btc_bias: str, btc_strength: float, near_miss: Counter) -> list[Signal]:
    if any(a.get("symbol") == symbol for a in state.get("active_signals", [])):
        near_miss[f"{symbol}:active"] += 1
        return []
    regime = build_regime(btc_bias, btc_strength, symbol, bundle)
    liq_mult = liquidity_mult(regime)
    spread, depth = book_metrics(bundle.get("book"))
    macro = macro_hot_flag()
    floor = confidence_floor(regime, macro)
    follow = 2 if regime.symbol_regime == "high_vol" else 1
    out: list[Signal] = []

    for pid, pipe in PIPELINES.items():
        ind_trig = get_indicators(symbol, pipe.trigger_tf, bundle[pipe.trigger_tf])
        if not liquidity_ok(symbol, ctx, ind_trig, spread, depth, liq_mult):
            near_miss[f"{pid}:liquidity"] += 1
            continue
        best: Optional[Candidate] = None
        for pname, fn in PATHWAYS.items():
            try:
                if pname == "momentum_breakout":
                    cand = fn(symbol, pipe, bundle, follow)
                else:
                    cand = fn(symbol, pipe, bundle)
            except Exception as e:
                log.debug("pathway %s/%s: %s", symbol, pname, e)
                cand = None
            if cand is None:
                near_miss[f"{pid}:{pname}:no_setup"] += 1
                continue
            if cand.rr < MIN_RR:
                near_miss[f"{pid}:{pname}:rr"] += 1
                continue
            if not stop_ok(cand):
                near_miss[f"{pid}:{pname}:stop_tight"] += 1
                continue
            if best is None or cand.rr > best.rr:
                best = cand
        if best is None:
            continue
        if regime.symbol_regime == "high_vol":
            best = widen_high_vol(best)
            if best.pathway == "momentum_breakout" and ind_trig["rel_vol"] < 1.5:
                near_miss[f"{pid}:hv_followthrough"] += 1
                continue
        agree, conflict = family_votes(symbol, best.direction, pipe, bundle)
        if len(conflict) >= 3:
            near_miss[f"{pid}:ensemble_conflict"] += 1
            continue
        funding = funding_oi_regime(symbol, ctx, bundle)
        conf, _ = score_candidate(best, regime, funding, agree, conflict, spread, depth, ind_trig["vwap"])
        if conf < floor:
            log_suppressed(symbol, best.direction, best.pathway, f"below floor {floor:.1f}", conf)
            near_miss[f"{pid}:threshold"] += 1
            continue
        if is_duplicate(state, best):
            near_miss[f"{pid}:dedup"] += 1
            continue
        ens_label = ensemble_adj(agree, conflict)[1]
        out.append(Signal(best, conf, grade_for(conf), ens_label, best.tags, now_utc().isoformat()))
    return out


def run_scan():
    clear_ind_cache()
    reset_breadth()
    state = load_state()
    near_miss: Counter = Counter()

    if daily_loss_paused(state):
        log.info("[SCAN] Daily loss limit — paused for UTC day")
        save_state(state)
        return

    log.info("[SCAN] Fetching %d symbols...", len(WATCHLIST))
    ctx = load_market_ctx()
    mids = fetch_all_mids()
    check_active(state, mids)

    bundles: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futs = {ex.submit(fetch_bundle, s): s for s in WATCHLIST}
        for fut in as_completed(futs):
            sym = futs[fut]
            bundle = fut.result()
            if bundle:
                bundles[sym] = bundle

    if BTC_SYMBOL not in bundles:
        log.error("[SCAN] No BTC data — abort")
        save_state(state)
        return

    btc_bias, btc_strength = compute_btc_regime(bundles[BTC_SYMBOL])
    log.info("[SCAN] BTC regime: %s (strength %.1f)", btc_bias, btc_strength)

    for sym, bundle in bundles.items():
        ind4 = get_indicators(sym, "4h", bundle["4h"])
        record_breadth(ind4["closes"][-1] > ind4["ema_mid"][-1])

    all_sigs: list[Signal] = []
    hourly = {s: b["1h"] for s, b in bundles.items()}
    for sym, bundle in bundles.items():
        try:
            all_sigs.extend(evaluate_symbol(sym, bundle, ctx, state, btc_bias, btc_strength, near_miss))
        except Exception as e:
            log.warning("[EVAL] %s: %s", sym, e)

    if near_miss:
        log.info("[NEAR-MISS] %s", ", ".join(f"{k}={v}" for k, v in near_miss.most_common(12)))

    if not all_sigs:
        log.info("[SCAN] No signals this scan")
        save_state(state)
        return

    ranked = dedup_symbol(dedup_corr(sorted(all_sigs, key=lambda s: s.confidence, reverse=True),
                                     corr_clusters(hourly)))
    accepted = apply_caps(ranked, state)
    if not accepted:
        log.info("[SCAN] Candidates blocked by portfolio caps")
        save_state(state)
        return

    fresh_mids = fetch_all_mids()
    sent = 0
    for sig in accepted:
        c = sig.candidate
        if not freshness_ok(c, fresh_mids.get(c.symbol)):
            log_suppressed(c.symbol, c.direction, c.pathway, "signal decayed", sig.confidence)
            continue
        msg = format_signal(sig)
        msg_id = send_telegram(msg)
        track_signal(state, sig, msg_id)
        record_dedup(state, c)
        sent += 1
        log.info("[SENT] %s %s %s conf=%.1f grade=%s", c.symbol, c.direction, c.pathway, sig.confidence, sig.grade)

    log.info("[SCAN] Sent %d signal(s)", sent)
    save_state(state)


# ==============================================================================
# BACKTEST
# ==============================================================================

def _simulate(c15, c1h, c4h, c1d, symbol, min_rr_override=None) -> list[dict]:
    trades = []
    open_t = None
    min_rr = min_rr_override if min_rr_override is not None else MIN_RR
    n = len(c15)
    for i in range(250, n):
        t_now = c15[i]["t"]
        bundle = {
            "15m": c15[:i + 1],
            "1h": [c for c in c1h if c["t"] <= t_now],
            "4h": [c for c in c4h if c["t"] <= t_now],
            "1d": [c for c in c1d if c["t"] <= t_now],
        }
        if len(bundle["1h"]) < 60:
            continue
        if open_t:
            bar = c15[i]
            d = open_t["direction"]
            hit_sl = bar["l"] <= open_t["stop"] if d == "long" else bar["h"] >= open_t["stop"]
            hit_tp = bar["h"] >= open_t["tp1"] if d == "long" else bar["l"] <= open_t["tp1"]
            if hit_sl:
                trades.append({**open_t, "outcome": "loss", "r_multiple": -1.0})
                open_t = None
            elif hit_tp:
                trades.append({**open_t, "outcome": "win", "r_multiple": open_t["rr"]})
                open_t = None
            continue
        clear_ind_cache()
        pipe = PIPELINES["fast"]
        ind = get_indicators(symbol, pipe.trigger_tf, bundle[pipe.trigger_tf])
        ap = atr_pct(ind)
        if not (MIN_ATR_PCT <= ap <= MAX_ATR_PCT):
            continue
        best = None
        for fn in (pathway_trend_continuation, pathway_liquidity_reversal):
            try:
                c = fn(symbol, pipe, bundle)
            except Exception:
                c = None
            if c and c.rr >= min_rr and stop_ok(c):
                if best is None or c.rr > best.rr:
                    best = c
        try:
            c = pathway_momentum_breakout(symbol, pipe, bundle, 1)
            if c and c.rr >= min_rr and stop_ok(c):
                if best is None or c.rr > best.rr:
                    best = c
        except Exception:
            pass
        if best is None:
            continue
        open_t = {"symbol": symbol, "direction": best.direction, "entry": best.entry,
                  "stop": best.stop, "tp1": best.tp1, "rr": best.rr, "entry_ts": t_now}
    return trades


def _apply_costs(trades: list[dict]) -> list[dict]:
    out = []
    for t in trades:
        risk_pct = abs(t["entry"] - t["stop"]) / t["entry"] if t["entry"] else 0.01
        cost_r = (TAKER_FEE * 2 + SLIPPAGE_PCT * 2) / max(risk_pct, 1e-6)
        out.append({**t, "net_r": t["r_multiple"] - cost_r})
    return out


def _summ(trades: list[dict], key: str = "r_multiple") -> dict:
    n = len(trades)
    if n == 0:
        return {"n": 0, "win_rate": None, "avg_r": None, "meaningful": False}
    wins = sum(1 for t in trades if t["outcome"] == "win")
    avg = statistics.mean(t[key] for t in trades)
    return {"n": n, "win_rate": wins / n, "avg_r": avg, "meaningful": n >= MIN_SAMPLE}


def _baseline_ma(c15: list[dict]) -> list[dict]:
    closes = [c["c"] for c in c15]
    fast, slow = ema(closes, 10), ema(closes, 30)
    trades, open_t = [], None
    for i in range(31, len(c15)):
        bar = c15[i]
        if open_t:
            d = open_t["direction"]
            hit_sl = bar["l"] <= open_t["stop"] if d == "long" else bar["h"] >= open_t["stop"]
            hit_tp = bar["h"] >= open_t["tp1"] if d == "long" else bar["l"] <= open_t["tp1"]
            if hit_sl:
                trades.append({**open_t, "outcome": "loss", "r_multiple": -1.0})
                open_t = None
            elif hit_tp:
                trades.append({**open_t, "outcome": "win", "r_multiple": 1.5})
                open_t = None
            continue
        up = fast[i - 1] <= slow[i - 1] and fast[i] > slow[i]
        dn = fast[i - 1] >= slow[i - 1] and fast[i] < slow[i]
        if not up and not dn:
            continue
        price = closes[i]
        a = statistics.pstdev(closes[max(0, i - 20):i + 1]) or price * 0.01
        direction = "long" if up else "short"
        stop = price - a if direction == "long" else price + a
        tp1 = price + a * 1.5 if direction == "long" else price - a * 1.5
        open_t = {"direction": direction, "entry": price, "stop": stop, "tp1": tp1}
    return trades


def walk_forward_backtest(symbol: str, n_windows: int = 4) -> dict:
    c15 = fetch_candles(symbol, "15m", 6000)
    c1h = fetch_candles(symbol, "1h", 1500)
    c4h = fetch_candles(symbol, "4h", 1500)
    c1d = fetch_candles(symbol, "1d", 400)
    if not all([c15, c1h, c4h, c1d]):
        return {"symbol": symbol, "error": "insufficient data"}
    n = len(c15)
    holdout_start = int(n * 0.85)
    pool = c15[:holdout_start]
    holdout = c15[holdout_start:]
    wsize = len(pool) // n_windows
    windows = []
    for w in range(n_windows):
        seg = pool[w * wsize:(w + 1) * wsize]
        if len(seg) < 300:
            continue
        tr = _apply_costs(_simulate(seg, c1h, c4h, c1d, symbol))
        windows.append({"window": w, "gross": _summ(tr), "net": _summ(tr, "net_r")})
    hold_tr = _apply_costs(_simulate(holdout, c1h, c4h, c1d, symbol))
    holdout_sum = {"gross": _summ(hold_tr), "net": _summ(hold_tr, "net_r")}
    orig = MIN_RR
    sensitivity = {}
    base_net = holdout_sum["net"]["avg_r"]
    collapsed = False
    for mult, label in [(0.9, "-10%"), (1.1, "+10%")]:
        ht = _apply_costs(_simulate(holdout, c1h, c4h, c1d, symbol, min_rr_override=orig * mult))
        sensitivity[label] = _summ(ht, "net_r")
        if base_net and sensitivity[label]["avg_r"] is not None and base_net > 0:
            if sensitivity[label]["avg_r"] < base_net * 0.3:
                collapsed = True
    baseline = _summ(_apply_costs([{**t, "entry": t.get("entry", 1), "stop": t.get("stop", 0),
                                    "r_multiple": t["r_multiple"]} for t in _baseline_ma(holdout)]), "net_r")
    return {
        "symbol": symbol, "windows": windows, "holdout": holdout_sum,
        "sensitivity": sensitivity, "overfit_flag": collapsed,
        "baseline_holdout": baseline,
        "beats_baseline": (holdout_sum["net"]["avg_r"] is not None and baseline.get("avg_r") is not None and
                           holdout_sum["net"]["avg_r"] > baseline["avg_r"]) if baseline.get("meaningful") else None,
        "min_sample": MIN_SAMPLE,
    }


def run_backtest_suite(symbols: Optional[list[str]] = None) -> dict:
    symbols = symbols or BACKTEST_SYMBOLS[:6]
    results = {}
    for sym in symbols:
        log.info("[BACKTEST] %s", sym)
        try:
            results[sym] = walk_forward_backtest(sym)
        except Exception as e:
            results[sym] = {"symbol": sym, "error": str(e)}
    return results


# ==============================================================================
# ENTRYPOINT
# ==============================================================================

_shutdown_state: dict = {}


def _shutdown(signum, frame):
    log.warning("Shutdown signal %s — saving state", signum)
    if _shutdown_state:
        save_state(_shutdown_state)
    sys.exit(0)


def main():
    global DRY_RUN
    parser = argparse.ArgumentParser(description=f"{ENGINE_NAME} Signal Engine v{VERSION}")
    parser.add_argument("--dry-run", action="store_true", help="Full scan, no Telegram/state commits")
    parser.add_argument("mode", nargs="?", default="scan", choices=["scan", "backtest"])
    parser.add_argument("symbols", nargs="*", help="backtest symbols or 'all'")
    args = parser.parse_args()
    if args.dry_run:
        DRY_RUN = True

    print("=" * 72)
    print(f"  {ENGINE_NAME} v{VERSION} — dual-pipeline ensemble confluence engine")
    print(f"  Pipelines: {', '.join(p.label for p in PIPELINES.values())}")
    print(f"  Watchlist: {len(WATCHLIST)} symbols | Top {TOP_N_PER_SCAN}/scan | Dry-run: {DRY_RUN}")
    print("=" * 72)

    if args.mode == "backtest":
        syms = WATCHLIST if args.symbols and args.symbols[0].lower() == "all" else (args.symbols or None)
        print(json.dumps(run_backtest_suite(syms), indent=2, default=str))
        return

    os_signal.signal(os_signal.SIGTERM, _shutdown)
    os_signal.signal(os_signal.SIGINT, _shutdown)
    st = load_state()
    _shutdown_state.clear()
    _shutdown_state.update(st)
    try:
        run_scan()
    except Exception as e:
        log.error("[MAIN] %s", e, exc_info=True)
        send_telegram(f"⚠️ {ENGINE_NAME} error: {e}")
    log.info("[DONE]")


if __name__ == "__main__":
    main()
