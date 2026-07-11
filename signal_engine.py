#!/usr/bin/env python3
"""
Aurelius Adaptive Signal Engine (ASE) — v1.0.0

Multi-engine SMC/adaptive trading signal system for Hyperliquid perpetuals.
Single-file, cron/GitHub Actions friendly. Requires the third-party
`requests` package. Secrets (Hyperliquid API, Telegram bot token) are read
from environment variables only.
"""

from __future__ import annotations

import os
import sys
import json
import time
import math
import signal
import logging
import hashlib
import tempfile
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional, Any

try:
    import requests
except ImportError:  # pragma: no cover - hard dependency, documented in requirements.txt
    print("FATAL: the 'requests' package is required. pip install -r requirements.txt", file=sys.stderr)
    raise

ENGINE_NAME = "Aurelius Adaptive Signal Engine"
ENGINE_SHORT = "Aurelius ASE"
ENGINE_VERSION = "1.0.0"

# ==============================================================================
# SECTION: CONFIGURATION
# ==============================================================================
# All secrets are read from environment variables / GitHub Actions secrets.
# Nothing sensitive is hardcoded anywhere in this file.

HYPERLIQUID_API_URL = os.environ.get("HYPERLIQUID_API_URL", "https://api.hyperliquid.xyz")
HYPERLIQUID_API_KEY = os.environ.get("HYPERLIQUID_API_KEY", "")  # optional: only needed for private endpoints
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

# Watchlist is intentionally fixed and identical across every run.
# Override via WATCHLIST env var (CSV) if desired. Mirrors axis_engine's watchlist.
DEFAULT_WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]
WATCHLIST = [s.strip().upper() for s in os.environ.get("WATCHLIST", ",".join(DEFAULT_WATCHLIST)).split(",") if s.strip()]

STATE_PATH = os.environ.get("ASE_STATE_PATH", "state.json")
CANDLE_CACHE_PATH = os.environ.get("ASE_CANDLE_CACHE_PATH", "candle_cache.json")

# Two-tier HTF/LTF design: 4h/1h for bias and zones, 15m for execution timing.
TF_HTF_BIAS = "4h"      # macro bias / structure
TF_HTF_ZONES = "1h"     # HTF order blocks / breaker blocks / FVGs
TF_LTF_EXEC = "15m"     # confirmation, precision entry, execution timing
TF_DAILY = "1d"         # daily S/R, premium/discount, volatility regime anchor
ALL_TIMEFRAMES = [TF_LTF_EXEC, TF_HTF_ZONES, TF_HTF_BIAS, TF_DAILY]
CANDLES_PER_TF = {TF_LTF_EXEC: 300, TF_HTF_ZONES: 300, TF_HTF_BIAS: 300, TF_DAILY: 220}

SCAN_INTERVAL_MINUTES = 15  # cron cadence
DAILY_SUMMARY_HOUR_UTC = 8

MAX_SIGNALS_PER_RUN = 3          # "1-2 quality trades per scan when conditions allow" + headroom
MAX_CONCURRENT_ACTIVE_SIGNALS = 12
MIN_EXPECTED_RR = 1.5
BASE_CONFIDENCE_THRESHOLD = 62.0  # 0-100 scale, adaptively tightened/relaxed
SIGNAL_COOLDOWN_BARS_LTF = 6      # cooldown per symbol+direction, in 15m bars
CORRELATION_DEDUP_THRESHOLD = 0.80

STATE_SCHEMA_VERSION = 1
MAX_HISTORY_RECORDS = 1500
STATE_MAX_AGE_DAYS = 45

REQUEST_TIMEOUT_SECS = 12
MAX_RETRIES = 4
RETRY_BACKOFF_BASE = 1.6

log = logging.getLogger("aurelius")


def setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    log.setLevel(logging.INFO)
    log.handlers.clear()
    log.addHandler(handler)


class ShutdownRequested(Exception):
    """Raised on SIGTERM so the run loop can save state and exit cleanly."""


def _handle_shutdown(sig_num, frame):  # noqa: ARG001
    raise ShutdownRequested(f"signal {sig_num}")


# ==============================================================================
# SECTION 1: DATA LAYER — Hyperliquid client, shared candle cache, rate limiting
# ==============================================================================
# Design decision: a single weight-based rate limiter shared across ALL engines
# and ALL symbols in a run, to stay under Hyperliquid's per-minute weight budget.

class WeightRateLimiter:
    """Token-bucket limiter tracking Hyperliquid's per-minute request weight."""

    def __init__(self, budget_per_minute: float = 1150.0):
        self.budget_per_minute = budget_per_minute
        self._window_start = time.monotonic()
        self._used = 0.0

    def acquire(self, weight: float) -> None:
        now = time.monotonic()
        elapsed = now - self._window_start
        if elapsed >= 60.0:
            self._window_start = now
            self._used = 0.0
            elapsed = 0.0
        if self._used + weight > self.budget_per_minute:
            sleep_for = max(0.0, 60.0 - elapsed)
            if sleep_for > 0:
                log.warning("Rate limiter: sleeping %.1fs to stay within budget", sleep_for)
                time.sleep(sleep_for)
            self._window_start = time.monotonic()
            self._used = 0.0
        self._used += weight


_RATE_LIMITER = WeightRateLimiter()
_SESSION = requests.Session()


def hl_coin(symbol: str) -> str:
    """Normalize a watchlist symbol into the coin identifier Hyperliquid expects."""
    return symbol.strip().upper()


def hl_post(payload: dict, retries: int = MAX_RETRIES, timeout: int = REQUEST_TIMEOUT_SECS,
            weight: float = 20.0) -> Optional[Any]:
    """POST to the Hyperliquid info endpoint with exponential backoff + jitter."""
    url = f"{HYPERLIQUID_API_URL}/info"
    headers = {"Content-Type": "application/json"}
    if HYPERLIQUID_API_KEY:
        headers["Authorization"] = f"Bearer {HYPERLIQUID_API_KEY}"
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            _RATE_LIMITER.acquire(weight)
            resp = _SESSION.post(url, json=payload, headers=headers, timeout=timeout)
            if resp.status_code == 429:
                sleep_s = (RETRY_BACKOFF_BASE ** attempt) + 0.25 * attempt
                log.warning("HL 429 rate-limited, backing off %.1fs (attempt %d/%d)", sleep_s, attempt + 1, retries)
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_err = exc
            sleep_s = (RETRY_BACKOFF_BASE ** attempt) + 0.2 * attempt
            log.warning("HL request failed (attempt %d/%d): %s -- retrying in %.1fs",
                        attempt + 1, retries, exc, sleep_s)
            time.sleep(sleep_s)
    log.error("HL request permanently failed after %d retries: %s", retries, last_err)
    return None


def interval_ms(interval: str) -> int:
    unit = interval[-1]
    n = int(interval[:-1])
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]
    return n * mult


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = interval_ms(interval)
    return (reference_ms // step) * step


def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    """Drop the still-forming candle so indicators never see an incomplete bar."""
    bar_open = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c.get("t", 0) < bar_open]


def get_candles(symbol: str, interval: str, n: int, reference_ms: int,
                 candle_cache: Optional[dict] = None) -> Optional[list[dict]]:
    """Fetch n closed candles for symbol/interval, using and populating the shared cache."""
    cache_key = f"{symbol}:{interval}"
    if candle_cache is not None:
        cached = candle_cache.get(cache_key)
        if cached and cached.get("reference_ms") == reference_ms:
            return cached["candles"]

    step = interval_ms(interval)
    end_ms = current_bar_open_ms(reference_ms, interval)
    start_ms = end_ms - step * (n + 5)
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": hl_coin(symbol), "interval": interval, "startTime": start_ms, "endTime": end_ms},
    }
    raw = hl_post(payload, weight=20.0)
    if raw is None or not isinstance(raw, list):
        return None
    candles = [
        {"t": c["t"], "o": float(c["o"]), "h": float(c["h"]), "l": float(c["l"]),
         "c": float(c["c"]), "v": float(c.get("v", 0.0))}
        for c in raw
    ]
    candles = filter_closed_candles(candles, interval, reference_ms)
    candles = candles[-n:]
    if candle_cache is not None:
        candle_cache[cache_key] = {"reference_ms": reference_ms, "candles": candles}
    return candles


def fetch_all_candles(symbol: str, reference_ms: int, candle_cache: dict) -> Optional[dict[str, list[dict]]]:
    """Fetch every timeframe needed for one symbol, sharing the cache across engines."""
    bundle: dict[str, list[dict]] = {}
    for tf in ALL_TIMEFRAMES:
        n = CANDLES_PER_TF[tf]
        candles = get_candles(symbol, tf, n, reference_ms, candle_cache)
        if not candles or len(candles) < min(60, n // 2):
            log.warning("Insufficient %s candles for %s (%d)", tf, symbol,
                        len(candles) if candles else 0)
            return None
        bundle[tf] = candles
    return bundle


_META_CACHE: dict = {}


def get_meta_and_ctx(reference_ms: int) -> Optional[tuple[list[str], list[dict]]]:
    """Shared universe metadata + funding/OI context, cached for the whole run."""
    if _META_CACHE.get("reference_ms") == reference_ms:
        return _META_CACHE["universe"], _META_CACHE["ctxs"]
    raw = hl_post({"type": "metaAndAssetCtxs"}, weight=20.0)
    if not raw or not isinstance(raw, list) or len(raw) < 2:
        return None
    universe = [u["name"] for u in raw[0].get("universe", [])]
    ctxs = raw[1]
    _META_CACHE.update({"reference_ms": reference_ms, "universe": universe, "ctxs": ctxs})
    return universe, ctxs


def get_market_snapshot(reference_ms: int) -> dict[str, dict]:
    """Return {symbol: {mark_price, funding, open_interest}} for the whole watchlist."""
    result = get_meta_and_ctx(reference_ms)
    if not result:
        return {}
    universe, ctxs = result
    snap: dict[str, dict] = {}
    for sym in WATCHLIST:
        try:
            idx = universe.index(hl_coin(sym))
        except ValueError:
            continue
        if idx >= len(ctxs):
            continue
        ctx = ctxs[idx]
        try:
            snap[sym] = {
                "mark_price": float(ctx.get("markPx", 0.0)),
                "funding": float(ctx.get("funding", 0.0)),
                "open_interest": float(ctx.get("openInterest", 0.0)) * float(ctx.get("markPx", 0.0) or 0.0),
            }
        except (TypeError, ValueError):
            continue
    return snap


def get_l2_spread_pct(symbol: str) -> Optional[float]:
    """Best bid/ask spread as a % of mid, used as a liquidity-quality filter."""
    raw = hl_post({"type": "l2Book", "coin": hl_coin(symbol)}, weight=2.0)
    if not raw or "levels" not in raw:
        return None
    try:
        bids, asks = raw["levels"][0], raw["levels"][1]
        if not bids or not asks:
            return None
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return None
        return (best_ask - best_bid) / mid * 100.0
    except (KeyError, IndexError, ValueError, TypeError):
        return None


# ==============================================================================
# SECTION 2: INDICATOR LIBRARY (pure functions, shared/cached per symbol+timeframe)
# ==============================================================================

def safe(v: Any, fallback: float = 0.0) -> float:
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return fallback
        return f
    except (TypeError, ValueError):
        return fallback


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b not in (0, 0.0) else default


def closes_of(candles: list[dict]) -> list[float]:
    return [c["c"] for c in candles]


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(values: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(values)):
        if i + 1 < period:
            out.append(values[i])
        else:
            out.append(sum(values[i + 1 - period:i + 1]) / period)
    return out


def stdev(values: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(values)):
        if i + 1 < period:
            out.append(0.0)
        else:
            window = values[i + 1 - period:i + 1]
            out.append(statistics.pstdev(window))
    return out


def rsi(closes: list[float], period: int = 14) -> list[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[1:period + 1]) / period if len(gains) > period else sum(gains) / max(len(gains), 1)
    avg_loss = sum(losses[1:period + 1]) / period if len(losses) > period else sum(losses) / max(len(losses), 1)
    out = [50.0] * min(period, len(closes))
    for i in range(period, len(closes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = safe_div(avg_gain, avg_loss, default=0.0)
        val = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + rs))
        out.append(val)
    while len(out) < len(closes):
        out.append(out[-1] if out else 50.0)
    return out[:len(closes)]


def atr_series(candles: list[dict], period: int = 14) -> list[float]:
    if not candles:
        return []
    trs = []
    prev_close = candles[0]["c"]
    for c in candles:
        tr = max(c["h"] - c["l"], abs(c["h"] - prev_close), abs(c["l"] - prev_close))
        trs.append(tr)
        prev_close = c["c"]
    out = []
    running = trs[0]
    for i, tr in enumerate(trs):
        if i == 0:
            running = tr
        else:
            running = (running * (period - 1) + tr) / period
        out.append(running)
    return out


def adx_series(candles: list[dict], period: int = 14) -> tuple[list[float], list[float], list[float]]:
    if len(candles) < 2:
        n = len(candles)
        return [0.0] * n, [0.0] * n, [0.0] * n
    plus_dm, minus_dm, trs = [0.0], [0.0], [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        up = candles[i]["h"] - candles[i - 1]["h"]
        down = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        tr = max(candles[i]["h"] - candles[i]["l"],
                  abs(candles[i]["h"] - candles[i - 1]["c"]),
                  abs(candles[i]["l"] - candles[i - 1]["c"]))
        trs.append(tr)

    def _wilder_smooth(vals: list[float]) -> list[float]:
        out = [vals[0]]
        for v in vals[1:]:
            out.append(out[-1] - (out[-1] / period) + v)
        return out

    smoothed_tr = _wilder_smooth(trs)
    smoothed_plus = _wilder_smooth(plus_dm)
    smoothed_minus = _wilder_smooth(minus_dm)
    plus_di = [safe_div(p, t, 0.0) * 100 for p, t in zip(smoothed_plus, smoothed_tr)]
    minus_di = [safe_div(m, t, 0.0) * 100 for m, t in zip(smoothed_minus, smoothed_tr)]
    dx = [safe_div(abs(p - m), (p + m), 0.0) * 100 for p, m in zip(plus_di, minus_di)]
    adx = sma(dx, period)
    return adx, plus_di, minus_di


def bollinger_width_pct(closes: list[float], period: int = 20, mult: float = 2.0) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    out = []
    for i in range(len(closes)):
        upper = mid[i] + mult * sd[i]
        lower = mid[i] - mult * sd[i]
        out.append(safe_div(upper - lower, mid[i], 0.0) * 100)
    return out


def obv_series(candles: list[dict]) -> list[float]:
    out = [0.0]
    for i in range(1, len(candles)):
        if candles[i]["c"] > candles[i - 1]["c"]:
            out.append(out[-1] + candles[i]["v"])
        elif candles[i]["c"] < candles[i - 1]["c"]:
            out.append(out[-1] - candles[i]["v"])
        else:
            out.append(out[-1])
    return out


def percentile_rank(values: list[float], x: float) -> float:
    if not values:
        return 50.0
    below = sum(1 for v in values if v <= x)
    return below / len(values) * 100.0


def detect_rsi_divergence(closes: list[float], rsi_vals: list[float], lookback: int = 25) -> Optional[str]:
    """Regular bullish/bearish divergence over recent swing lows/highs."""
    if len(closes) < lookback + 2:
        return None
    window_c = closes[-lookback:]
    window_r = rsi_vals[-lookback:]
    lows_idx = [i for i in range(1, len(window_c) - 1) if window_c[i] < window_c[i - 1] and window_c[i] < window_c[i + 1]]
    highs_idx = [i for i in range(1, len(window_c) - 1) if window_c[i] > window_c[i - 1] and window_c[i] > window_c[i + 1]]
    if len(lows_idx) >= 2:
        i1, i2 = lows_idx[-2], lows_idx[-1]
        if window_c[i2] < window_c[i1] and window_r[i2] > window_r[i1]:
            return "bullish"
    if len(highs_idx) >= 2:
        i1, i2 = highs_idx[-2], highs_idx[-1]
        if window_c[i2] > window_c[i1] and window_r[i2] < window_r[i1]:
            return "bearish"
    return None


def daily_vwap(candles: list[dict]) -> float:
    if not candles:
        return 0.0
    pv, vol = 0.0, 0.0
    for c in candles:
        typical = (c["h"] + c["l"] + c["c"]) / 3.0
        pv += typical * c["v"]
        vol += c["v"]
    return safe_div(pv, vol, candles[-1]["c"])


_INDICATOR_CACHE: dict[str, dict] = {}


def compute_indicators(symbol: str, timeframe: str, candles: list[dict], reference_ms: int) -> dict:
    """Compute (and cache) the full indicator set once per symbol/timeframe/run."""
    cache_key = f"{symbol}:{timeframe}:{reference_ms}"
    if cache_key in _INDICATOR_CACHE:
        return _INDICATOR_CACHE[cache_key]
    closes = closes_of(candles)
    rsi_vals = rsi(closes, 14)
    atr_vals = atr_series(candles, 14)
    adx_vals, plus_di, minus_di = adx_series(candles, 14)
    bb_width = bollinger_width_pct(closes, 20, 2.0)
    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)
    ema200 = ema(closes, 200) if len(closes) >= 50 else ema(closes, min(50, len(closes)))
    obv_vals = obv_series(candles)
    atr_pct = safe_div(atr_vals[-1], closes[-1], 0.0) * 100 if atr_vals and closes else 0.0
    ind = {
        "closes": closes, "rsi": rsi_vals, "atr": atr_vals, "atr_pct": atr_pct,
        "adx": adx_vals, "plus_di": plus_di, "minus_di": minus_di,
        "bb_width_pct": bb_width, "ema20": ema20, "ema50": ema50, "ema200": ema200,
        "obv": obv_vals, "divergence": detect_rsi_divergence(closes, rsi_vals),
        "vwap": daily_vwap(candles[-96:]) if timeframe == TF_LTF_EXEC else None,
        "avg_volume": sum(c["v"] for c in candles[-20:]) / max(1, min(20, len(candles))),
    }
    _INDICATOR_CACHE[cache_key] = ind
    return ind


def reset_indicator_cache() -> None:
    _INDICATOR_CACHE.clear()


# ==============================================================================
# SECTION 3: STATE & PERSISTENCE (atomic writes)
# ==============================================================================

def _default_state() -> dict:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "active_signals": [],           # currently open, tracked signals
        "signal_history": [],           # closed signals (win/loss/cancelled)
        "engine_stats": {},             # per-engine learning stats
        "cooldowns": {},                # f"{symbol}:{direction}" -> last bar index
        "atr_pct_memory": {},           # symbol -> list of recent atr_pct samples
        "confidence_calibration": {"buckets": {}},  # predicted bucket -> [n, wins]
        "last_daily_summary_date": None,
        "run_count": 0,
        "last_run_at": None,
    }


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        log.info("No existing state file at %s -- initializing fresh state.", STATE_PATH)
        return _default_state()
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)
        if state.get("schema_version") != STATE_SCHEMA_VERSION:
            log.warning("State schema version mismatch -- migrating to fresh defaults, preserving history.")
            fresh = _default_state()
            fresh["signal_history"] = state.get("signal_history", [])
            fresh["engine_stats"] = state.get("engine_stats", {})
            return fresh
        defaults = _default_state()
        for k, v in defaults.items():
            state.setdefault(k, v)
        return state
    except (json.JSONDecodeError, OSError) as exc:
        log.error("Failed to load state (%s) -- starting fresh to avoid crash loop.", exc)
        return _default_state()


def _atomic_write_json(path: str, data: dict) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def save_state(state: dict) -> None:
    try:
        state["last_run_at"] = datetime.now(timezone.utc).isoformat()
        _atomic_write_json(STATE_PATH, state)
    except OSError as exc:
        log.error("Failed to save state: %s", exc)


def load_candle_cache() -> dict:
    if not os.path.exists(CANDLE_CACHE_PATH):
        return {}
    try:
        with open(CANDLE_CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_candle_cache(candle_cache: dict) -> None:
    try:
        _atomic_write_json(CANDLE_CACHE_PATH, candle_cache)
    except OSError as exc:
        log.warning("Failed to persist candle cache (non-fatal): %s", exc)


def prune_state(state: dict, max_records: int = MAX_HISTORY_RECORDS, max_days: int = STATE_MAX_AGE_DAYS) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
    cutoff_iso = cutoff.isoformat()
    hist = state.get("signal_history", [])
    hist = [h for h in hist if h.get("closed_at", "9999") >= cutoff_iso]
    if len(hist) > max_records:
        hist = hist[-max_records:]
    state["signal_history"] = hist


# ==============================================================================
# SECTION 4: MARKET STRUCTURE & SMC PRIMITIVES
# ==============================================================================
# Order Blocks, Breaker Blocks, Fair Value Gaps, BOS/CHoCH, liquidity sweeps,
# premium/discount zones.

@dataclass
class Swing:
    index: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    swings: list[Swing] = []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        if candles[i]["h"] == max(window_h) and window_h.count(candles[i]["h"]) == 1:
            swings.append(Swing(i, candles[i]["h"], "high"))
        if candles[i]["l"] == min(window_l) and window_l.count(candles[i]["l"]) == 1:
            swings.append(Swing(i, candles[i]["l"], "low"))
    return swings


@dataclass
class StructureState:
    bias: str            # "bullish" | "bearish" | "neutral"
    last_bos_index: Optional[int]
    last_choch_index: Optional[int]
    last_high: Optional[float]
    last_low: Optional[float]
    higher_highs: bool
    higher_lows: bool


def analyze_structure(candles: list[dict], swings: list[Swing]) -> StructureState:
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return StructureState("neutral", None, None,
                               highs[-1].price if highs else None,
                               lows[-1].price if lows else None, False, False)

    higher_highs = highs[-1].price > highs[-2].price
    higher_lows = lows[-1].price > lows[-2].price
    lower_highs = highs[-1].price < highs[-2].price
    lower_lows = lows[-1].price < lows[-2].price

    bias = "neutral"
    if higher_highs and higher_lows:
        bias = "bullish"
    elif lower_highs and lower_lows:
        bias = "bearish"

    last_bos_index = None
    last_choch_index = None
    closes = closes_of(candles)
    prior_bias = bias
    for i in range(2, len(closes)):
        recent_highs = [s.price for s in highs if s.index < i]
        recent_lows = [s.price for s in lows if s.index < i]
        if recent_highs and closes[i] > max(recent_highs[-1:]):
            if prior_bias == "bearish":
                last_choch_index = i
            last_bos_index = i
            prior_bias = "bullish"
        elif recent_lows and closes[i] < min(recent_lows[-1:]):
            if prior_bias == "bullish":
                last_choch_index = i
            last_bos_index = i
            prior_bias = "bearish"

    return StructureState(bias, last_bos_index, last_choch_index,
                           highs[-1].price, lows[-1].price, higher_highs, higher_lows)


@dataclass
class Zone:
    kind: str          # "order_block" | "breaker_block" | "fvg"
    direction: str      # "bullish" | "bearish"
    top: float
    bottom: float
    index: int
    mitigated: bool = False
    quality: float = 0.5


def _avg_volume(candles: list[dict], idx: int, window: int = 20) -> float:
    lo = max(0, idx - window)
    seg = candles[lo:idx] or candles[:max(1, idx)]
    return sum(c["v"] for c in seg) / max(1, len(seg))


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int = 80) -> list[Zone]:
    """Last opposite candle before a strong displacement move (impulse >= 1x ATR)."""
    zones: list[Zone] = []
    n = len(candles)
    start = max(1, n - lookback)
    for i in range(start, n):
        body = abs(candles[i]["c"] - candles[i]["o"])
        atr_i = atr_vals[i] if i < len(atr_vals) else atr_vals[-1]
        if atr_i <= 0 or body < 1.0 * atr_i:
            continue
        bullish_impulse = candles[i]["c"] > candles[i]["o"]
        j = i - 1
        if j < 0:
            continue
        if bullish_impulse and candles[j]["c"] < candles[j]["o"]:
            vol_boost = 1.15 if candles[i]["v"] > _avg_volume(candles, i) else 1.0
            zones.append(Zone("order_block", "bullish", candles[j]["h"], candles[j]["l"], j,
                               quality=min(1.0, 0.5 + 0.25 * (body / atr_i) * vol_boost)))
        elif not bullish_impulse and candles[j]["c"] > candles[j]["o"]:
            vol_boost = 1.15 if candles[i]["v"] > _avg_volume(candles, i) else 1.0
            zones.append(Zone("order_block", "bearish", candles[j]["h"], candles[j]["l"], j,
                               quality=min(1.0, 0.5 + 0.25 * (body / atr_i) * vol_boost)))
    return zones


def find_fvgs(candles: list[dict], atr_vals: list[float], lookback: int = 80) -> list[Zone]:
    """Three-candle imbalance: candle[i-1].high < candle[i+1].low (bullish) or inverse."""
    zones: list[Zone] = []
    n = len(candles)
    start = max(1, n - lookback)
    for i in range(start, n - 1):
        atr_i = atr_vals[i] if i < len(atr_vals) else (atr_vals[-1] if atr_vals else 0.0)
        if candles[i - 1]["h"] < candles[i + 1]["l"]:
            gap = candles[i + 1]["l"] - candles[i - 1]["h"]
            if atr_i > 0 and gap >= 0.15 * atr_i:
                zones.append(Zone("fvg", "bullish", candles[i + 1]["l"], candles[i - 1]["h"], i,
                                   quality=min(1.0, 0.4 + 0.4 * safe_div(gap, atr_i, 0.0))))
        if candles[i - 1]["l"] > candles[i + 1]["h"]:
            gap = candles[i - 1]["l"] - candles[i + 1]["h"]
            if atr_i > 0 and gap >= 0.15 * atr_i:
                zones.append(Zone("fvg", "bearish", candles[i - 1]["l"], candles[i + 1]["h"], i,
                                   quality=min(1.0, 0.4 + 0.4 * safe_div(gap, atr_i, 0.0))))
    return zones


def mark_mitigation(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    for z in zones:
        for c in candles[z.index + 1:]:
            if z.direction == "bullish" and c["l"] <= z.top:
                z.mitigated = True
                break
            if z.direction == "bearish" and c["h"] >= z.bottom:
                z.mitigated = True
                break
    return zones


def derive_breaker_blocks(zones: list[Zone], structure: StructureState, candles: list[dict]) -> list[Zone]:
    """A breaker block is a mitigated (failed) order block whose failure caused a CHoCH/BOS."""
    breakers: list[Zone] = []
    if structure.last_bos_index is None:
        return breakers
    for z in zones:
        if z.kind != "order_block" or not z.mitigated:
            continue
        if z.index >= structure.last_bos_index:
            continue
        flipped_dir = "bearish" if z.direction == "bullish" else "bullish"
        breakers.append(Zone("breaker_block", flipped_dir, z.top, z.bottom, z.index,
                              quality=min(1.0, z.quality + 0.1)))
    return breakers


def cluster_levels(levels: list[float], tol_pct: float = 0.0015) -> list[tuple[float, int]]:
    if not levels:
        return []
    levels_sorted = sorted(levels)
    clusters: list[list[float]] = [[levels_sorted[0]]]
    for lv in levels_sorted[1:]:
        if abs(lv - clusters[-1][-1]) / max(clusters[-1][-1], 1e-9) <= tol_pct:
            clusters[-1].append(lv)
        else:
            clusters.append([lv])
    return [(sum(c) / len(c), len(c)) for c in clusters]


def build_liquidity_pools(swings: list[Swing]) -> dict:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    return {
        "buy_side": cluster_levels(highs),   # resting buy-stops above equal highs
        "sell_side": cluster_levels(lows),   # resting sell-stops below equal lows
    }


def detect_sweep(candles: list[dict], pools: dict, direction: str, lookback: int = 12) -> Optional[dict]:
    """Detect a wick-based liquidity sweep followed by a close back inside range."""
    recent = candles[-lookback:]
    if direction == "bullish":
        for level, count in pools.get("sell_side", []):
            for c in recent:
                if c["l"] < level and c["c"] > level:
                    return {"level": level, "count": count, "candle": c}
    else:
        for level, count in pools.get("buy_side", []):
            for c in recent:
                if c["h"] > level and c["c"] < level:
                    return {"level": level, "count": count, "candle": c}
    return None


def premium_discount_zone(candles: list[dict], lookback: int = 60) -> dict:
    seg = candles[-lookback:]
    hi = max(c["h"] for c in seg)
    lo = min(c["l"] for c in seg)
    mid = (hi + lo) / 2.0
    price = candles[-1]["c"]
    zone = "equilibrium"
    if price > mid + 0.1 * (hi - mid):
        zone = "premium"
    elif price < mid - 0.1 * (mid - lo):
        zone = "discount"
    return {"high": hi, "low": lo, "mid": mid, "zone": zone}


@dataclass
class MSSEvent:
    direction: str
    index: int
    break_level: float


def detect_mss(candles: list[dict], direction: str, lookback: int = 30) -> Optional[MSSEvent]:
    """Market structure shift on the execution timeframe: close beyond a recent swing."""
    seg = candles[-lookback:]
    swings = find_swings(seg, left=1, right=1)
    closes = closes_of(seg)
    if direction == "bullish":
        recent_highs = [s.price for s in swings if s.kind == "high"]
        if not recent_highs:
            return None
        level = recent_highs[-1]
        for i in range(len(seg) - 1, 0, -1):
            if closes[i] > level:
                return MSSEvent("bullish", i, level)
    else:
        recent_lows = [s.price for s in swings if s.kind == "low"]
        if not recent_lows:
            return None
        level = recent_lows[-1]
        for i in range(len(seg) - 1, 0, -1):
            if closes[i] < level:
                return MSSEvent("bearish", i, level)
    return None


# ==============================================================================
# SECTION 5: REGIME DETECTION & ADAPTIVE CONTEXT
# ==============================================================================

@dataclass
class AdaptiveContext:
    symbol: str
    trend_strength: float       # 0-100, from ADX
    trend_direction: str        # "up" | "down" | "flat"
    volatility_pctile: float    # 0-100, atr_pct vs own history
    volatility_state: str       # "expansion" | "normal" | "compression"
    is_ranging: bool
    session_weight: float       # 0.5-1.0 liquidity/session quality multiplier
    breadth: float               # -1..1 fraction of watchlist aligned with btc bias
    btc_regime: str
    noise_index: float           # 0-1, higher = choppier/less tradable
    spread_pct: Optional[float]
    quality_multiplier: float    # combined multiplier used to tighten/relax thresholds


def session_weight_now(reference_ms: int) -> float:
    hour = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).hour
    # London/NY overlap (12-16 UTC) and London open (7-11 UTC) carry the deepest liquidity.
    if 12 <= hour < 16:
        return 1.0
    if 7 <= hour < 12 or 16 <= hour < 20:
        return 0.9
    if 0 <= hour < 4:
        return 0.6
    return 0.75


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state["atr_pct_memory"].setdefault(symbol, [])
    mem.append(atr_pct)
    if len(mem) > 200:
        del mem[: len(mem) - 200]
    return percentile_rank(mem, atr_pct)


def compute_noise_index(candles: list[dict], lookback: int = 30) -> float:
    """Ratio of net displacement to summed absolute movement -- low = choppy."""
    seg = candles[-lookback:]
    if len(seg) < 2:
        return 0.5
    net = abs(seg[-1]["c"] - seg[0]["c"])
    total = sum(abs(seg[i]["c"] - seg[i - 1]["c"]) for i in range(1, len(seg)))
    efficiency = safe_div(net, total, 0.0)
    return round(1.0 - efficiency, 4)


def compute_btc_regime(btc_bundle: Optional[dict]) -> tuple[str, float]:
    if not btc_bundle:
        return "neutral", 0.0
    htf = btc_bundle[TF_HTF_BIAS]
    closes = closes_of(htf)
    ema50 = ema(closes, 50)
    adx_vals, _, _ = adx_series(htf, 14)
    strength = adx_vals[-1] if adx_vals else 0.0
    if closes[-1] > ema50[-1] and strength > 20:
        return "bull_trend", strength
    if closes[-1] < ema50[-1] and strength > 20:
        return "bear_trend", strength
    return "neutral", strength


def compute_breadth(bundles: dict[str, dict], btc_bias: str) -> float:
    if btc_bias == "neutral" or not bundles:
        return 0.0
    aligned, total = 0, 0
    for sym, bundle in bundles.items():
        htf = bundle.get(TF_HTF_BIAS)
        if not htf:
            continue
        closes = closes_of(htf)
        e50 = ema(closes, 50)
        total += 1
        above = closes[-1] > e50[-1]
        if (btc_bias == "bull_trend" and above) or (btc_bias == "bear_trend" and not above):
            aligned += 1
    return safe_div(aligned, total, 0.0) * 2 - 1 if total else 0.0


def build_adaptive_context(state: dict, symbol: str, bundle: dict, btc_bias: str,
                            btc_strength: float, breadth: float, reference_ms: int,
                            spread_pct: Optional[float]) -> AdaptiveContext:
    htf = bundle[TF_HTF_BIAS]
    ltf = bundle[TF_LTF_EXEC]
    ind_htf = compute_indicators(symbol, TF_HTF_BIAS, htf, reference_ms)
    ind_ltf = compute_indicators(symbol, TF_LTF_EXEC, ltf, reference_ms)

    adx_val = ind_htf["adx"][-1] if ind_htf["adx"] else 0.0
    direction = "flat"
    if ind_htf["closes"][-1] > ind_htf["ema50"][-1] and adx_val > 18:
        direction = "up"
    elif ind_htf["closes"][-1] < ind_htf["ema50"][-1] and adx_val > 18:
        direction = "down"

    atr_pct = ind_ltf["atr_pct"]
    vol_pctile = update_atr_pct_memory(state, symbol, atr_pct)
    if vol_pctile >= 75:
        vol_state = "expansion"
    elif vol_pctile <= 25:
        vol_state = "compression"
    else:
        vol_state = "normal"

    is_ranging = adx_val < 18
    noise = compute_noise_index(ltf, 30)
    sess = session_weight_now(reference_ms)

    quality = 1.0
    quality *= 0.85 if noise > 0.7 else (1.05 if noise < 0.35 else 1.0)
    quality *= sess
    quality *= 0.9 if vol_state == "compression" else (0.95 if vol_state == "expansion" else 1.0)
    if spread_pct is not None and spread_pct > 0.15:
        quality *= 0.85

    return AdaptiveContext(
        symbol=symbol, trend_strength=adx_val, trend_direction=direction,
        volatility_pctile=vol_pctile, volatility_state=vol_state, is_ranging=is_ranging,
        session_weight=sess, breadth=breadth, btc_regime=btc_bias, noise_index=noise,
        spread_pct=spread_pct, quality_multiplier=round(quality, 4),
    )


def adaptive_confidence_threshold(ctx: AdaptiveContext, base: float = BASE_CONFIDENCE_THRESHOLD) -> float:
    """Tighten in chaotic/low-quality markets, relax in clean/high-quality markets."""
    adj = base
    if ctx.quality_multiplier < 0.85:
        adj += 8.0
    elif ctx.quality_multiplier > 1.05:
        adj -= 4.0
    if ctx.noise_index > 0.75:
        adj += 6.0
    if ctx.volatility_state == "compression":
        adj += 3.0
    return max(50.0, min(85.0, adj))


# ==============================================================================
# SECTION 6: SHARED CANDIDATE SCHEMA & RISK-MANAGEMENT PRIMITIVES
# ==============================================================================

@dataclass
class Candidate:
    engine: str
    symbol: str
    direction: str            # "long" | "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float          # raw, pre-decision-engine confidence (0-100)
    expected_rr: float
    confluences: list[str] = field(default_factory=list)
    regime_suitability: list[str] = field(default_factory=list)
    meta: dict = field(default_factory=dict)


def structure_based_sl(direction: str, candles: list[dict], atr_val: float, buffer_mult: float = 0.25,
                        lookback: int = 8) -> float:
    """SL beyond the most recent structural swing low/high, using candle wicks only -- never midpoint."""
    seg = candles[-lookback:]
    buffer = max(atr_val * buffer_mult, 1e-9)
    if direction == "long":
        return min(c["l"] for c in seg) - buffer
    return max(c["h"] for c in seg) + buffer


def validate_sl_tp_against_candles(direction: str, entry: float, sl: float, tp1: float, tp2: float,
                                    candles: list[dict], lookback: int = 40) -> bool:
    """Validate using candle highs/lows only, never midpoint or live price."""
    seg = candles[-lookback:]
    hi = max(c["h"] for c in seg)
    lo = min(c["l"] for c in seg)
    if direction == "long":
        if not (sl < entry < tp1 <= tp2 if tp2 >= tp1 else sl < entry < tp1):
            if not (sl < entry < tp1):
                return False
        if sl < lo - (hi - lo) * 0.5:  # implausibly far SL vs recent range
            return False
    else:
        if not (sl > entry > tp1):
            return False
        if sl > hi + (hi - lo) * 0.5:
            return False
    if entry <= 0 or sl <= 0 or tp1 <= 0:
        return False
    return True


def avoids_obvious_liquidity(direction: str, sl: float, pools: dict, atr_val: float) -> bool:
    """Reject setups whose SL sits exactly on top of an obvious resting-liquidity cluster."""
    buffer = atr_val * 0.15
    pool_side = pools.get("sell_side" if direction == "long" else "buy_side", [])
    for level, count in pool_side:
        if count >= 2 and abs(sl - level) <= buffer:
            return False
    return True


def compute_expected_rr(direction: str, entry: float, sl: float, tp1: float, tp2: float) -> float:
    risk = abs(entry - sl)
    if risk <= 0:
        return 0.0
    reward = 0.6 * abs(tp1 - entry) + 0.4 * abs(tp2 - entry)
    return round(safe_div(reward, risk, 0.0), 2)


def clamp_entry_to_market(entry: float, market_price: float, max_dev_pct: float = 1.5) -> float:
    """Pull a stale/derived entry back toward the live mark price if it has drifted
    too far (>max_dev_pct) to be a realistic fill."""
    if market_price <= 0 or entry <= 0:
        return entry
    dev_pct = abs(entry - market_price) / market_price * 100
    return market_price if dev_pct > max_dev_pct else entry


ENTRY_SLIPPAGE_BUFFER_PCT = 0.0006  # ~6bps: a real fill always has some spread/slippage


def apply_entry_slippage_buffer(direction: str, entry: float, market_price: float,
                                 buffer_pct: float = ENTRY_SLIPPAGE_BUFFER_PCT) -> float:
    """If entry sits at (or within a hair of) the live mark price, nudge it a small
    realistic distance away. A signal whose entry equals the current price is
    already stale by the time it's read, and can't be told apart from SL/TP levels
    that happen to sit on the mark price too."""
    if market_price <= 0 or entry <= 0:
        return entry
    if abs(entry - market_price) / market_price > buffer_pct:
        return entry  # already meaningfully away from market -- leave it alone
    return market_price * (1 + buffer_pct) if direction == "long" else market_price * (1 - buffer_pct)


def build_take_profits(direction: str, entry: float, sl: float, pools: dict, zones: list[Zone],
                        min_rr: float = 1.5, max_rr: float = 4.0) -> tuple[float, float]:
    risk = abs(entry - sl)
    tp1 = entry + risk * min_rr if direction == "long" else entry - risk * min_rr
    tp2_default = entry + risk * max_rr if direction == "long" else entry - risk * max_rr
    pool_side = pools.get("buy_side" if direction == "long" else "sell_side", [])
    candidate_levels = [lvl for lvl, cnt in pool_side]
    if direction == "long":
        beyond = sorted(l for l in candidate_levels if l > tp1)
        tp2 = beyond[0] if beyond else tp2_default
    else:
        beyond = sorted((l for l in candidate_levels if l < tp1), reverse=True)
        tp2 = beyond[0] if beyond else tp2_default

    tp1_r, tp2_r = round(tp1, 6), round(tp2, 6)
    # Guard on the *rounded* values: a liquidity-pool level only a fraction of a
    # unit beyond TP1 can otherwise collapse onto it after round(x, 6).
    if direction == "long" and tp2_r <= tp1_r:
        tp2_r = round(tp2_default, 6) if tp2_default > tp1 else round(tp1 + max(risk * 0.5, 1e-6), 6)
        if tp2_r <= tp1_r:
            tp2_r = round(tp1 + max(risk * 0.5, 1e-6), 6)
    elif direction == "short" and tp2_r >= tp1_r:
        tp2_r = round(tp2_default, 6) if tp2_default < tp1 else round(tp1 - max(risk * 0.5, 1e-6), 6)
        if tp2_r >= tp1_r:
            tp2_r = round(tp1 - max(risk * 0.5, 1e-6), 6)
    return tp1_r, tp2_r


# ==============================================================================
# SECTION 7: SPECIALIZED ENGINE ENSEMBLE
# ==============================================================================
# Every engine below independently returns zero or more Candidate objects with
# direction/entry/SL/TP1/TP2/confidence/expected_rr/confluences/regime tags.
# All engines share one SymbolAnalysis bundle (built once per symbol) so no
# structure/zone/pool computation is duplicated across engines.

@dataclass
class SymbolAnalysis:
    symbol: str
    bundle: dict
    ctx: AdaptiveContext
    structure_htf: StructureState
    structure_ltf: StructureState
    zones_htf: list[Zone]
    zones_ltf: list[Zone]
    breaker_zones: list[Zone]
    pools_htf: dict
    pd_zone: dict
    market_price: float
    ind_ltf: dict
    ind_htf: dict
    ind_d: dict


def build_symbol_analysis(symbol: str, bundle: dict, ctx: AdaptiveContext, reference_ms: int,
                           market_price: float) -> SymbolAnalysis:
    htf = bundle[TF_HTF_ZONES]
    ltf = bundle[TF_LTF_EXEC]
    daily = bundle[TF_DAILY]
    bias_tf = bundle[TF_HTF_BIAS]

    ind_ltf = compute_indicators(symbol, TF_LTF_EXEC, ltf, reference_ms)
    ind_htf = compute_indicators(symbol, TF_HTF_ZONES, htf, reference_ms)
    ind_d = compute_indicators(symbol, TF_DAILY, daily, reference_ms)

    swings_htf = find_swings(bias_tf, 2, 2)
    structure_htf = analyze_structure(bias_tf, swings_htf)
    swings_ltf = find_swings(ltf, 2, 2)
    structure_ltf = analyze_structure(ltf, swings_ltf)

    zones_htf = find_order_blocks(htf, ind_htf["atr"]) + find_fvgs(htf, ind_htf["atr"])
    zones_htf = mark_mitigation(zones_htf, htf)
    zones_ltf = find_order_blocks(ltf, ind_ltf["atr"]) + find_fvgs(ltf, ind_ltf["atr"])
    zones_ltf = mark_mitigation(zones_ltf, ltf)
    breaker_zones = derive_breaker_blocks(zones_htf, structure_htf, htf)

    pools_htf = build_liquidity_pools(swings_htf)
    pd_zone = premium_discount_zone(daily, 60)

    return SymbolAnalysis(symbol, bundle, ctx, structure_htf, structure_ltf, zones_htf, zones_ltf,
                           breaker_zones, pools_htf, pd_zone, market_price, ind_ltf, ind_htf, ind_d)


def _finalize(engine_name: str, symbol: str, direction: str, entry: float, sl: float,
              confidence: float, confluences: list[str], regimes: list[str],
              sa: SymbolAnalysis, meta: Optional[dict] = None) -> Optional[Candidate]:
    # Resolve the entry we're actually going to publish *before* deriving SL/TP from
    # it, so every downstream distance is consistent with the final entry.
    entry = clamp_entry_to_market(entry, sa.market_price)
    entry = apply_entry_slippage_buffer(direction, entry, sa.market_price)

    atr_val = sa.ind_ltf["atr"][-1] if sa.ind_ltf["atr"] else 0.0
    tp1, tp2 = build_take_profits(direction, entry, sl, sa.pools_htf, sa.zones_htf)
    if not avoids_obvious_liquidity(direction, sl, sa.pools_htf, atr_val):
        sl = sl - atr_val * 0.2 if direction == "long" else sl + atr_val * 0.2
        tp1, tp2 = build_take_profits(direction, entry, sl, sa.pools_htf, sa.zones_htf)  # SL moved -- re-derive
    if not validate_sl_tp_against_candles(direction, entry, sl, tp1, tp2, sa.bundle[TF_LTF_EXEC]):
        return None
    rr = compute_expected_rr(direction, entry, sl, tp1, tp2)
    if rr < 1.0:
        return None
    return Candidate(engine=engine_name, symbol=symbol, direction=direction, entry=round(entry, 6),
                      sl=round(sl, 6), tp1=round(tp1, 6), tp2=round(tp2, 6),
                      confidence=round(min(99.0, max(1.0, confidence)), 2), expected_rr=rr,
                      confluences=confluences, regime_suitability=regimes, meta=meta or {})


# --- 1. Smart Money Concept (sweep -> MSS -> OB/breaker entry) -----------------

def engine_smc(sa: SymbolAnalysis) -> list[Candidate]:
    out = []
    for direction in ("bullish", "bearish"):
        sweep = detect_sweep(sa.bundle[TF_HTF_ZONES], sa.pools_htf, direction, lookback=15)
        if not sweep:
            continue
        mss = detect_mss(sa.bundle[TF_LTF_EXEC], direction, lookback=25)
        if not mss:
            continue
        trade_dir = "long" if direction == "bullish" else "short"
        entry = sa.market_price
        atr_val = sa.ind_ltf["atr"][-1] if sa.ind_ltf["atr"] else 0.0
        sl = structure_based_sl(trade_dir, sa.bundle[TF_LTF_EXEC], atr_val)
        confidence = 68 + min(15, sweep["count"] * 3) + (8 if sa.pd_zone["zone"] != ("premium" if trade_dir == "long" else "discount") else 0)
        conf_list = ["liquidity sweep", "MSS confirmation", f"{sa.pd_zone['zone']} zone"]
        cand = _finalize("SMC", sa.symbol, trade_dir, entry, sl, confidence, conf_list,
                          ["reversal", "high_volatility", "trending"], sa,
                          meta={"sweep_level": sweep["level"]})
        if cand:
            out.append(cand)
    return out


# --- 2. Trend Continuation -----------------------------------------------------

def engine_trend_continuation(sa: SymbolAnalysis) -> list[Candidate]:
    ctx = sa.ctx
    if ctx.trend_direction == "flat":
        return []
    direction = "long" if ctx.trend_direction == "up" else "short"
    ltf = sa.bundle[TF_LTF_EXEC]
    ind = sa.ind_ltf
    price = ltf[-1]["c"]
    pulled_back = (price <= ind["ema20"][-1] * 1.003) if direction == "long" else (price >= ind["ema20"][-1] * 0.997)
    if not pulled_back:
        return []
    rsi_ok = (40 <= ind["rsi"][-1] <= 62) if direction == "long" else (38 <= ind["rsi"][-1] <= 60)
    if not rsi_ok:
        return []
    atr_val = ind["atr"][-1]
    sl = structure_based_sl(direction, ltf, atr_val, buffer_mult=0.3, lookback=6)
    confidence = 60 + min(20, ctx.trend_strength - 18) + (6 if ctx.breadth * (1 if direction == "long" else -1) > 0.2 else 0)
    cand = _finalize("TrendContinuation", sa.symbol, direction, price, sl, confidence,
                      ["EMA20 pullback", f"ADX {ctx.trend_strength:.0f}", "HTF trend aligned"],
                      ["trending", "bull", "bear"], sa)
    return [cand] if cand else []


# --- 3. Breakout ----------------------------------------------------------------

def engine_breakout(sa: SymbolAnalysis) -> list[Candidate]:
    ltf = sa.bundle[TF_LTF_EXEC]
    ind = sa.ind_ltf
    if len(ltf) < 25:
        return []
    recent = ltf[-25:-1]
    hi = max(c["h"] for c in recent)
    lo = min(c["l"] for c in recent)
    last = ltf[-1]
    vol_confirm = last["v"] > ind["avg_volume"] * 1.3
    out = []
    if last["c"] > hi and vol_confirm:
        atr_val = ind["atr"][-1]
        sl = max(hi - atr_val * 0.5, structure_based_sl("long", ltf, atr_val))
        confidence = 58 + (10 if sa.ctx.volatility_state == "expansion" else 0) + (8 if sa.pd_zone["zone"] != "premium" else -5)
        cand = _finalize("Breakout", sa.symbol, "long", last["c"], sl, confidence,
                          ["range high breakout", "volume confirmation"], ["breakout", "volatility_expansion"], sa)
        if cand:
            out.append(cand)
    if last["c"] < lo and vol_confirm:
        atr_val = ind["atr"][-1]
        sl = min(lo + atr_val * 0.5, structure_based_sl("short", ltf, atr_val))
        confidence = 58 + (10 if sa.ctx.volatility_state == "expansion" else 0) + (8 if sa.pd_zone["zone"] != "discount" else -5)
        cand = _finalize("Breakout", sa.symbol, "short", last["c"], sl, confidence,
                          ["range low breakdown", "volume confirmation"], ["breakout", "volatility_expansion"], sa)
        if cand:
            out.append(cand)
    return out


# --- 4. Pullback (to HTF order block / EMA confluence in a trend) --------------

def engine_pullback(sa: SymbolAnalysis) -> list[Candidate]:
    if sa.ctx.trend_direction == "flat":
        return []
    direction = "long" if sa.ctx.trend_direction == "up" else "short"
    want_kind = "bullish" if direction == "long" else "bearish"
    zones = [z for z in sa.zones_htf if z.kind == "order_block" and z.direction == want_kind and not z.mitigated]
    if not zones:
        return []
    price = sa.market_price
    zone = min(zones, key=lambda z: abs(price - (z.top + z.bottom) / 2))
    inside = zone.bottom <= price <= zone.top
    near = abs(price - (zone.top if direction == "short" else zone.bottom)) / max(price, 1e-9) < 0.01
    if not (inside or near):
        return []
    atr_val = sa.ind_ltf["atr"][-1]
    sl = (zone.bottom - atr_val * 0.25) if direction == "long" else (zone.top + atr_val * 0.25)
    confidence = 62 + zone.quality * 15 + (5 if sa.pd_zone["zone"] == ("discount" if direction == "long" else "premium") else 0)
    cand = _finalize("Pullback", sa.symbol, direction, price, sl, confidence,
                      ["HTF order block retest", f"zone quality {zone.quality:.2f}"],
                      ["trending", "pullback"], sa)
    return [cand] if cand else []


# --- 5. Liquidity Sweep (standalone, LTF sweep + reclaim, no full MSS needed) --

def engine_liquidity_sweep(sa: SymbolAnalysis) -> list[Candidate]:
    ltf = sa.bundle[TF_LTF_EXEC]
    swings_ltf = find_swings(ltf, 2, 2)
    pools_ltf = build_liquidity_pools(swings_ltf)
    out = []
    for direction, trade_dir in (("bullish", "long"), ("bearish", "short")):
        sweep = detect_sweep(ltf, pools_ltf, direction, lookback=8)
        if not sweep:
            continue
        atr_val = sa.ind_ltf["atr"][-1]
        sl = (sweep["candle"]["l"] - atr_val * 0.2) if trade_dir == "long" else (sweep["candle"]["h"] + atr_val * 0.2)
        confidence = 60 + min(18, sweep["count"] * 4)
        cand = _finalize("LiquiditySweep", sa.symbol, trade_dir, sa.market_price, sl, confidence,
                          ["equal highs/lows swept", "reclaim close"], ["reversal", "ranging"], sa)
        if cand:
            out.append(cand)
    return out


# --- 6. Order Block (fresh, untested HTF OB reaction) --------------------------

def engine_order_block(sa: SymbolAnalysis) -> list[Candidate]:
    fresh = [z for z in sa.zones_htf if z.kind == "order_block" and not z.mitigated]
    out = []
    price = sa.market_price
    for z in fresh:
        direction = "long" if z.direction == "bullish" else "short"
        touching = z.bottom * 0.999 <= price <= z.top * 1.001
        if not touching:
            continue
        atr_val = sa.ind_ltf["atr"][-1]
        sl = (z.bottom - atr_val * 0.25) if direction == "long" else (z.top + atr_val * 0.25)
        confidence = 60 + z.quality * 18
        cand = _finalize("OrderBlock", sa.symbol, direction, price, sl, confidence,
                          [f"untested {z.direction} OB", f"quality {z.quality:.2f}"],
                          ["trending", "ranging"], sa)
        if cand:
            out.append(cand)
            break  # one high-quality OB signal per symbol is enough
    return out


# --- 7. Breaker Block -----------------------------------------------------------

def engine_breaker_block(sa: SymbolAnalysis) -> list[Candidate]:
    price = sa.market_price
    out = []
    for z in sa.breaker_zones:
        direction = "long" if z.direction == "bullish" else "short"
        touching = z.bottom * 0.999 <= price <= z.top * 1.001
        if not touching:
            continue
        atr_val = sa.ind_ltf["atr"][-1]
        sl = (z.bottom - atr_val * 0.25) if direction == "long" else (z.top + atr_val * 0.25)
        confidence = 63 + z.quality * 16
        cand = _finalize("BreakerBlock", sa.symbol, direction, price, sl, confidence,
                          ["failed OB flipped to breaker", "post-BOS retest"], ["reversal", "trending"], sa)
        if cand:
            out.append(cand)
            break
    return out


# --- 8. Fair Value Gap -----------------------------------------------------------

def engine_fair_value_gap(sa: SymbolAnalysis) -> list[Candidate]:
    price = sa.market_price
    fvgs = [z for z in sa.zones_ltf if z.kind == "fvg" and not z.mitigated]
    out = []
    for z in fvgs:
        direction = "long" if z.direction == "bullish" else "short"
        touching = z.bottom <= price <= z.top
        if not touching:
            continue
        if sa.ctx.trend_direction == "up" and direction == "short":
            continue
        if sa.ctx.trend_direction == "down" and direction == "long":
            continue
        atr_val = sa.ind_ltf["atr"][-1]
        sl = (z.bottom - atr_val * 0.2) if direction == "long" else (z.top + atr_val * 0.2)
        confidence = 58 + z.quality * 18
        cand = _finalize("FairValueGap", sa.symbol, direction, price, sl, confidence,
                          ["LTF FVG fill", f"quality {z.quality:.2f}"], ["trending", "pullback"], sa)
        if cand:
            out.append(cand)
            break
    return out


# --- 9. Momentum ------------------------------------------------------------------

def engine_momentum(sa: SymbolAnalysis) -> list[Candidate]:
    ind = sa.ind_ltf
    if len(ind["closes"]) < 3:
        return []
    roc = safe_div(ind["closes"][-1] - ind["closes"][-4], ind["closes"][-4], 0.0) * 100 if len(ind["closes"]) > 4 else 0.0
    out = []
    strong_up = ind["rsi"][-1] > 58 and ind["adx"][-1] > 22 and roc > 0.3
    strong_down = ind["rsi"][-1] < 42 and ind["adx"][-1] > 22 and roc < -0.3
    atr_val = ind["atr"][-1]
    if strong_up:
        sl = structure_based_sl("long", sa.bundle[TF_LTF_EXEC], atr_val, buffer_mult=0.35, lookback=5)
        confidence = 58 + min(20, (ind["rsi"][-1] - 58))
        cand = _finalize("Momentum", sa.symbol, "long", sa.market_price, sl, confidence,
                          [f"RSI {ind['rsi'][-1]:.0f}", f"ROC {roc:.2f}%"], ["trending", "high_volatility"], sa)
        if cand:
            out.append(cand)
    if strong_down:
        sl = structure_based_sl("short", sa.bundle[TF_LTF_EXEC], atr_val, buffer_mult=0.35, lookback=5)
        confidence = 58 + min(20, (42 - ind["rsi"][-1]))
        cand = _finalize("Momentum", sa.symbol, "short", sa.market_price, sl, confidence,
                          [f"RSI {ind['rsi'][-1]:.0f}", f"ROC {roc:.2f}%"], ["trending", "high_volatility"], sa)
        if cand:
            out.append(cand)
    return out


# --- 10. Reversal (RSI divergence at HTF premium/discount extreme) --------------

def engine_reversal(sa: SymbolAnalysis) -> list[Candidate]:
    div = sa.ind_ltf.get("divergence")
    if not div:
        return []
    if div == "bullish" and sa.pd_zone["zone"] != "discount":
        return []
    if div == "bearish" and sa.pd_zone["zone"] != "premium":
        return []
    direction = "long" if div == "bullish" else "short"
    atr_val = sa.ind_ltf["atr"][-1]
    sl = structure_based_sl(direction, sa.bundle[TF_LTF_EXEC], atr_val, buffer_mult=0.3, lookback=8)
    confidence = 61 + (10 if sa.ctx.is_ranging else 0)
    cand = _finalize("Reversal", sa.symbol, direction, sa.market_price, sl, confidence,
                      [f"RSI {div} divergence", f"{sa.pd_zone['zone']} zone"], ["reversal", "ranging"], sa)
    return [cand] if cand else []


# --- 11. Mean Reversion (extreme deviation from EMA/VWAP snaps back) -----------

def engine_mean_reversion(sa: SymbolAnalysis) -> list[Candidate]:
    if not sa.ctx.is_ranging:
        return []
    ind = sa.ind_ltf
    price = ind["closes"][-1]
    ema20 = ind["ema20"][-1]
    dev_pct = safe_div(price - ema20, ema20, 0.0) * 100
    atr_pct = ind["atr_pct"]
    out = []
    if atr_pct <= 0:
        return []
    z = safe_div(dev_pct, max(atr_pct, 0.01), 0.0)
    if z <= -1.4 and ind["rsi"][-1] < 35:
        atr_val = ind["atr"][-1]
        sl = price - atr_val * 1.2
        confidence = 57 + min(18, abs(z) * 6)
        cand = _finalize("MeanReversion", sa.symbol, "long", price, sl, confidence,
                          [f"deviation z={z:.2f}", "RSI oversold"], ["ranging", "low_volatility"], sa)
        if cand:
            out.append(cand)
    if z >= 1.4 and ind["rsi"][-1] > 65:
        atr_val = ind["atr"][-1]
        sl = price + atr_val * 1.2
        confidence = 57 + min(18, abs(z) * 6)
        cand = _finalize("MeanReversion", sa.symbol, "short", price, sl, confidence,
                          [f"deviation z={z:.2f}", "RSI overbought"], ["ranging", "low_volatility"], sa)
        if cand:
            out.append(cand)
    return out


# --- 12. Range Trading (fade range extremes with structure confirmation) -------

def engine_range_trading(sa: SymbolAnalysis) -> list[Candidate]:
    if not sa.ctx.is_ranging:
        return []
    daily = sa.bundle[TF_DAILY]
    seg = daily[-20:]
    hi = max(c["h"] for c in seg)
    lo = min(c["l"] for c in seg)
    width = hi - lo
    if width <= 0:
        return []
    price = sa.market_price
    pos = safe_div(price - lo, width, 0.5)
    out = []
    atr_val = sa.ind_ltf["atr"][-1]
    if pos <= 0.12:
        sl = lo - atr_val * 0.6
        confidence = 56 + (12 if sa.ind_ltf["rsi"][-1] < 40 else 0)
        cand = _finalize("RangeTrading", sa.symbol, "long", price, sl, confidence,
                          ["range low fade", f"pos {pos:.2f}"], ["ranging", "low_volatility"], sa)
        if cand:
            out.append(cand)
    if pos >= 0.88:
        sl = hi + atr_val * 0.6
        confidence = 56 + (12 if sa.ind_ltf["rsi"][-1] > 60 else 0)
        cand = _finalize("RangeTrading", sa.symbol, "short", price, sl, confidence,
                          ["range high fade", f"pos {pos:.2f}"], ["ranging", "low_volatility"], sa)
        if cand:
            out.append(cand)
    return out


# --- 13. Volatility Expansion (BB squeeze release) -------------------------------

def engine_volatility_expansion(sa: SymbolAnalysis) -> list[Candidate]:
    ind = sa.ind_ltf
    widths = ind["bb_width_pct"]
    if len(widths) < 30:
        return []
    recent_min = min(widths[-30:-1])
    was_squeezed = widths[-2] <= recent_min * 1.15
    now_expanding = widths[-1] > widths[-2] * 1.15
    if not (was_squeezed and now_expanding):
        return []
    direction = "long" if ind["closes"][-1] > ind["closes"][-2] else "short"
    atr_val = ind["atr"][-1]
    sl = structure_based_sl(direction, sa.bundle[TF_LTF_EXEC], atr_val, buffer_mult=0.4, lookback=6)
    confidence = 59 + (10 if sa.ctx.volatility_state == "expansion" else 0)
    cand = _finalize("VolatilityExpansion", sa.symbol, direction, sa.market_price, sl, confidence,
                      ["Bollinger squeeze release", "directional close"], ["volatility_expansion"], sa)
    return [cand] if cand else []


ALL_ENGINES = {
    "SMC": engine_smc,
    "TrendContinuation": engine_trend_continuation,
    "Breakout": engine_breakout,
    "Pullback": engine_pullback,
    "LiquiditySweep": engine_liquidity_sweep,
    "OrderBlock": engine_order_block,
    "BreakerBlock": engine_breaker_block,
    "FairValueGap": engine_fair_value_gap,
    "Momentum": engine_momentum,
    "Reversal": engine_reversal,
    "MeanReversion": engine_mean_reversion,
    "RangeTrading": engine_range_trading,
    "VolatilityExpansion": engine_volatility_expansion,
}


def run_all_engines(sa: SymbolAnalysis) -> list[Candidate]:
    candidates: list[Candidate] = []
    for name, fn in ALL_ENGINES.items():
        try:
            candidates.extend(fn(sa))
        except Exception as exc:  # an individual engine must never crash the run
            log.warning("Engine %s raised on %s: %s", name, sa.symbol, exc)
    return candidates


# ==============================================================================
# SECTION 8: DECISION ENGINE
# ==============================================================================
# Combines market regime, MTF alignment, institutional bias, liquidity,
# volatility, volume, historical per-engine performance, confidence
# calibration, EV, RR, and confluence strength -- using ADAPTIVE (learned)
# weights rather than fixed weights.

DEFAULT_ENGINE_WEIGHT = 1.0
MIN_ENGINE_WEIGHT = 0.4
MAX_ENGINE_WEIGHT = 1.8


def get_engine_weight(state: dict, engine_name: str) -> float:
    stats = state["engine_stats"].get(engine_name)
    if not stats or stats.get("n", 0) < 8:
        return DEFAULT_ENGINE_WEIGHT
    win_rate = safe_div(stats["wins"], stats["n"], 0.5)
    profit_factor = stats.get("profit_factor", 1.0)
    calibration_error = stats.get("calibration_error", 0.15)
    weight = DEFAULT_ENGINE_WEIGHT
    weight *= (0.6 + win_rate)                       # win rate 0->0.6x, 1->1.6x
    weight *= min(1.3, max(0.7, profit_factor / 1.4))
    weight *= max(0.75, 1.0 - calibration_error)
    return round(max(MIN_ENGINE_WEIGHT, min(MAX_ENGINE_WEIGHT, weight)), 4)


def score_candidate(cand: Candidate, sa: SymbolAnalysis, state: dict) -> float:
    ctx = sa.ctx
    engine_weight = get_engine_weight(state, cand.engine)

    mtf_alignment = 0.0
    if (cand.direction == "long" and ctx.trend_direction == "up") or \
       (cand.direction == "short" and ctx.trend_direction == "down"):
        mtf_alignment = 1.0
    elif ctx.trend_direction == "flat":
        mtf_alignment = 0.5

    regime_match = 1.0 if (
        (ctx.is_ranging and "ranging" in cand.regime_suitability) or
        (not ctx.is_ranging and any(r in cand.regime_suitability for r in ("trending", "breakout", "pullback"))) or
        (ctx.volatility_state == "expansion" and "volatility_expansion" in cand.regime_suitability) or
        (ctx.volatility_state == "compression" and "low_volatility" in cand.regime_suitability)
    ) else 0.6

    institutional_bias = 1.0 if (
        (cand.direction == "long" and sa.structure_htf.bias == "bullish") or
        (cand.direction == "short" and sa.structure_htf.bias == "bearish")
    ) else (0.55 if sa.structure_htf.bias == "neutral" else 0.35)

    breadth_component = 0.5 + 0.5 * (ctx.breadth if cand.direction == "long" else -ctx.breadth)
    breadth_component = max(0.0, min(1.0, breadth_component))

    liquidity_component = 1.0 - (ctx.spread_pct / 0.3) if ctx.spread_pct is not None else 0.85
    liquidity_component = max(0.3, min(1.0, liquidity_component))

    volatility_component = {"expansion": 0.9, "normal": 1.0, "compression": 0.8}[ctx.volatility_state]
    confluence_component = min(1.0, 0.35 + 0.15 * len(cand.confluences))
    ev_component = min(1.0, cand.expected_rr / 3.0)
    confidence_component = cand.confidence / 100.0

    raw = (
        0.20 * confidence_component +
        0.15 * mtf_alignment +
        0.13 * institutional_bias +
        0.12 * ev_component +
        0.10 * confluence_component +
        0.10 * regime_match +
        0.08 * liquidity_component +
        0.07 * volatility_component +
        0.05 * breadth_component
    ) * 100.0

    return round(raw * engine_weight * ctx.quality_multiplier, 3)


def pearson_corr(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    try:
        return statistics.correlation(a, b)
    except (statistics.StatisticsError, ValueError):
        return 0.0


def deduplicate_correlated(ranked: list[tuple[Candidate, float]], bundles: dict[str, dict],
                            threshold: float = CORRELATION_DEDUP_THRESHOLD) -> list[tuple[Candidate, float]]:
    """Keep at most one signal per correlated cluster, regardless of direction.
    A long on symbol A and a short on a highly-correlated symbol B are still both
    exposure to the same underlying factor once fleet-wide sizing is considered --
    not a hedge -- so both firing is the failure mode this guards against."""
    kept: list[tuple[Candidate, float]] = []
    kept_returns: dict[str, list[float]] = {}
    for cand, score in ranked:
        htf = bundles.get(cand.symbol, {}).get(TF_HTF_BIAS, [])
        closes = closes_of(htf)[-40:]
        returns = [safe_div(closes[i] - closes[i - 1], closes[i - 1], 0.0) for i in range(1, len(closes))]
        is_dup = False
        for other_symbol, other_returns in kept_returns.items():
            if other_symbol == cand.symbol:
                continue
            corr = pearson_corr(returns, other_returns)
            if abs(corr) >= threshold:
                is_dup = True
                break
        if not is_dup:
            kept.append((cand, score))
            kept_returns[cand.symbol] = returns
    return kept


def decision_engine_select(all_candidates: list[Candidate], analyses: dict[str, SymbolAnalysis],
                            state: dict, max_signals: int = MAX_SIGNALS_PER_RUN) -> list[tuple[Candidate, float]]:
    scored: list[tuple[Candidate, float]] = []
    for cand in all_candidates:
        sa = analyses[cand.symbol]
        if cand.expected_rr < MIN_EXPECTED_RR:
            continue
        threshold = adaptive_confidence_threshold(sa.ctx)
        score = score_candidate(cand, sa, state)
        if cand.confidence < threshold:
            continue
        scored.append((cand, score))

    scored.sort(key=lambda t: t[1], reverse=True)

    best_per_symbol: dict[str, tuple[Candidate, float]] = {}
    for cand, score in scored:
        if cand.symbol not in best_per_symbol or score > best_per_symbol[cand.symbol][1]:
            best_per_symbol[cand.symbol] = (cand, score)
    ranked = sorted(best_per_symbol.values(), key=lambda t: t[1], reverse=True)

    bundles = {sym: sa.bundle for sym, sa in analyses.items()}
    ranked = deduplicate_correlated(ranked, bundles)

    return ranked[:max_signals]


# ==============================================================================
# SECTION 9: LEARNING SYSTEM
# ==============================================================================

def _default_engine_stats() -> dict:
    return {"n": 0, "wins": 0, "losses": 0, "gross_profit_r": 0.0, "gross_loss_r": 0.0,
            "profit_factor": 1.0, "sum_rr": 0.0, "avg_rr": 0.0, "sum_hold_secs": 0.0,
            "avg_hold_secs": 0.0, "calibration_error": 0.15, "mae_sum": 0.0, "mfe_sum": 0.0}


def record_signal_history(state: dict, cand: Candidate, score: float, sa: SymbolAnalysis,
                           msg_id: Optional[int]) -> str:
    sig_id = hashlib.sha1(f"{cand.symbol}{cand.engine}{time.time()}".encode()).hexdigest()[:12]
    entry_rec = {
        "id": sig_id, "engine": cand.engine, "symbol": cand.symbol, "direction": cand.direction,
        "entry": cand.entry, "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2,
        "confidence": cand.confidence, "expected_rr": cand.expected_rr, "score": score,
        "regime": {"trend_direction": sa.ctx.trend_direction, "is_ranging": sa.ctx.is_ranging,
                    "volatility_state": sa.ctx.volatility_state, "btc_regime": sa.ctx.btc_regime},
        "confluences": cand.confluences, "opened_at": datetime.now(timezone.utc).isoformat(),
        "status": "activated", "tg_message_id": msg_id, "mae_r": 0.0, "mfe_r": 0.0,
    }
    state["active_signals"].append(entry_rec)
    return sig_id


def _r_multiple(sig: dict, price: float) -> float:
    risk = abs(sig["entry"] - sig["sl"])
    if risk <= 0:
        return 0.0
    if sig["direction"] == "long":
        return safe_div(price - sig["entry"], risk, 0.0)
    return safe_div(sig["entry"] - price, risk, 0.0)


def update_engine_learning(state: dict, sig: dict, result: str) -> None:
    stats = state["engine_stats"].setdefault(sig["engine"], _default_engine_stats())
    stats["n"] += 1
    r = _r_multiple(sig, sig.get("_close_price", sig["entry"]))
    if result == "tp2":
        stats["wins"] += 1
        stats["gross_profit_r"] += max(r, 0.0)
    elif result == "breakeven":
        stats["wins"] += 1  # TP1 was already banked before the stop moved to breakeven
        stats["gross_profit_r"] += max(r, 0.0)
    elif result == "sl":
        stats["losses"] += 1
        stats["gross_loss_r"] += max(-r, 0.0)
    stats["sum_rr"] += r
    stats["avg_rr"] = safe_div(stats["sum_rr"], stats["n"], 0.0)
    stats["profit_factor"] = safe_div(stats["gross_profit_r"], max(stats["gross_loss_r"], 1e-6), stats["gross_profit_r"] or 1.0)
    hold_secs = sig.get("_hold_secs", 0.0)
    stats["sum_hold_secs"] += hold_secs
    stats["avg_hold_secs"] = safe_div(stats["sum_hold_secs"], stats["n"], 0.0)
    stats["mae_sum"] += sig.get("mae_r", 0.0)
    stats["mfe_sum"] += sig.get("mfe_r", 0.0)

    if result == "breakeven":
        return  # a scratch confirms neither the entry thesis nor the confidence estimate -- exclude from calibration

    predicted_p = sig["confidence"] / 100.0
    actual = 1.0 if result == "tp2" else 0.0
    brier = (predicted_p - actual) ** 2
    stats["calibration_error"] = round(0.9 * stats["calibration_error"] + 0.1 * brier, 4)

    bucket = str(int(sig["confidence"] // 10) * 10)
    cal = state["confidence_calibration"]["buckets"].setdefault(bucket, {"n": 0, "wins": 0})
    cal["n"] += 1
    cal["wins"] += 1 if actual == 1.0 else 0


def check_active_signals(state: dict, market_prices: dict[str, float], candle_bundles: dict[str, dict],
                          telegram_enabled: bool) -> None:
    still_active = []
    for sig in state["active_signals"]:
        price = market_prices.get(sig["symbol"])
        candles = candle_bundles.get(sig["symbol"], {}).get(TF_LTF_EXEC)
        if price is None and not candles:
            still_active.append(sig)
            continue
        recent = candles[-1] if candles else None
        hi = recent["h"] if recent else price
        lo = recent["l"] if recent else price
        cur = recent["c"] if recent else price

        r_now = _r_multiple(sig, cur)
        sig["mfe_r"] = max(sig.get("mfe_r", 0.0), r_now)
        sig["mae_r"] = min(sig.get("mae_r", 0.0), r_now)

        hit_sl = (lo <= sig["sl"]) if sig["direction"] == "long" else (hi >= sig["sl"])
        hit_tp1 = (hi >= sig["tp1"]) if sig["direction"] == "long" else (lo <= sig["tp1"])
        hit_tp2 = (hi >= sig["tp2"]) if sig["direction"] == "long" else (lo <= sig["tp2"])

        result = None
        if sig["status"] == "activated":
            # Conservative intrabar ordering: if a candle's range touches both a stop
            # and a target, assume the stop happened first.
            if hit_sl:
                result = "sl"
            elif hit_tp2:
                result = "tp2"
            elif hit_tp1:
                sig["status"] = "tp1"
                sig["sl"] = sig["entry"]  # move stop to breakeven after TP1
                if telegram_enabled and sig.get("tg_message_id"):
                    react_to_message(sig["tg_message_id"], "tp1")
                    reply_to_telegram(sig["tg_message_id"],
                                       f"🔥 <b>TP1 hit</b> on {sig['symbol']} ({sig['engine']}) -- "
                                       f"SL moved to breakeven @ <code>{fmt_px(sig['entry'])}</code>")
                still_active.append(sig)
                continue
        elif sig["status"] == "tp1":
            # sig["sl"] now equals entry, so hit_sl here means the breakeven stop
            # was hit, not the original SL -- must not be labeled a loss.
            if hit_sl:
                result = "breakeven"
            elif hit_tp2:
                result = "tp2"

        if result:
            sig["status"] = "closed"
            sig["result"] = result
            sig["closed_at"] = datetime.now(timezone.utc).isoformat()
            sig["_close_price"] = {"tp2": sig["tp2"], "breakeven": sig["entry"]}.get(result, sig["sl"])
            opened = datetime.fromisoformat(sig["opened_at"])
            sig["_hold_secs"] = (datetime.now(timezone.utc) - opened).total_seconds()
            update_engine_learning(state, sig, result)
            state["signal_history"].append(sig)
            if telegram_enabled and sig.get("tg_message_id"):
                r_final = _r_multiple(sig, sig["_close_price"])
                labels = {"tp2": "🏆 TP2 -- full close", "sl": "😭 SL hit", "breakeven": "🤝 Breakeven stop"}
                react_to_message(sig["tg_message_id"], result)
                reply_to_telegram(sig["tg_message_id"],
                                   f"{labels.get(result, result.upper())} on {sig['symbol']} ({sig['engine']}) "
                                   f"-- {r_final:+.2f}R")
        else:
            still_active.append(sig)
    state["active_signals"] = still_active


# ==============================================================================
# SECTION 10: TELEGRAM INTEGRATION
# ==============================================================================

TELEGRAM_API_BASE = "https://api.telegram.org"
REACTION_EMOJIS = {"activated": "⚡", "tp1": "🔥", "tp2": "🏆", "sl": "😭", "breakeven": "🤝", "cancelled": "🤷"}


def telegram_enabled() -> bool:
    return bool(TG_BOT_TOKEN and TG_CHAT_ID)


def send_telegram(text: str) -> Optional[int]:
    if not telegram_enabled():
        log.info("Telegram not configured -- skipping send. Message would have been:\n%s", text)
        return None
    url = f"{TELEGRAM_API_BASE}/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        resp = _SESSION.post(url, json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
                              timeout=REQUEST_TIMEOUT_SECS)
        resp.raise_for_status()
        return resp.json().get("result", {}).get("message_id")
    except (requests.RequestException, ValueError) as exc:
        log.error("Telegram send failed: %s", exc)
        return None


def reply_to_telegram(message_id: int, text: str) -> None:
    if not telegram_enabled():
        return
    url = f"{TELEGRAM_API_BASE}/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        _SESSION.post(url, json={"chat_id": TG_CHAT_ID, "text": text,
                                  "reply_to_message_id": message_id, "parse_mode": "HTML"},
                      timeout=REQUEST_TIMEOUT_SECS)
    except requests.RequestException as exc:
        log.error("Telegram reply failed: %s", exc)


def react_to_message(message_id: int, status: str) -> None:
    if not telegram_enabled():
        return
    emoji = REACTION_EMOJIS.get(status, "⚡")
    url = f"{TELEGRAM_API_BASE}/bot{TG_BOT_TOKEN}/setMessageReaction"
    try:
        _SESSION.post(url, json={"chat_id": TG_CHAT_ID, "message_id": message_id,
                                  "reaction": [{"type": "emoji", "emoji": emoji}]}, timeout=REQUEST_TIMEOUT_SECS)
    except requests.RequestException as exc:
        log.debug("Telegram reaction failed (non-fatal): %s", exc)


def fmt_px(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:,.6f}"


def format_signal_message(cand: Candidate, sa: SymbolAnalysis, score: float) -> str:
    direction_label = "LONG 🟢" if cand.direction == "long" else "SHORT 🔴"
    lines = [
        f"<b>{ENGINE_SHORT} v{ENGINE_VERSION}</b>",
        f"<b>{cand.symbol}/USD — {direction_label}</b>",
        f"Engine: <b>{cand.engine}</b>  |  Confidence: <b>{cand.confidence:.0f}%</b>  |  Score: {score:.1f}",
        "",
        f"Entry: <code>{fmt_px(cand.entry)}</code>",
        f"SL: <code>{fmt_px(cand.sl)}</code>",
        f"TP1: <code>{fmt_px(cand.tp1)}</code>",
        f"TP2: <code>{fmt_px(cand.tp2)}</code>",
        f"Expected RR: <b>{cand.expected_rr:.2f}</b>",
        "",
        f"Regime: {sa.ctx.trend_direction} / {'ranging' if sa.ctx.is_ranging else 'trending'} / vol-{sa.ctx.volatility_state}",
        f"Confluences: {', '.join(cand.confluences)}",
        "",
        "Status: Activated 🚀",
    ]
    return "\n".join(lines)


def send_signal(cand: Candidate, sa: SymbolAnalysis, score: float) -> Optional[int]:
    text = format_signal_message(cand, sa, score)
    msg_id = send_telegram(text)
    if msg_id:
        react_to_message(msg_id, "activated")
    return msg_id


def should_send_daily_summary(state: dict, now: datetime) -> bool:
    if now.hour < DAILY_SUMMARY_HOUR_UTC:
        return False
    last = state.get("last_daily_summary_date")
    today_str = now.strftime("%Y-%m-%d")
    return last != today_str


def build_daily_summary(state: dict, now: datetime) -> str:
    today_str = now.strftime("%Y-%m-%d")
    todays = [h for h in state["signal_history"] if h.get("closed_at", "").startswith(today_str)
              or h.get("opened_at", "").startswith(today_str)]
    total = len(todays)
    wins = sum(1 for h in todays if h.get("result") in ("tp2", "breakeven"))
    losses = sum(1 for h in todays if h.get("result") == "sl")
    breakevens = sum(1 for h in todays if h.get("result") == "breakeven")
    win_rate = safe_div(wins, max(wins + losses, 1), 0.0) * 100
    avg_rr = safe_div(sum(_r_multiple(h, h.get("_close_price", h["entry"])) for h in todays), max(total, 1), 0.0)
    avg_hold = safe_div(sum(h.get("_hold_secs", 0.0) for h in todays), max(total, 1), 0.0) / 3600.0
    gross_profit = sum(max(_r_multiple(h, h.get("_close_price", h["entry"])), 0) for h in todays)
    gross_loss = sum(max(-_r_multiple(h, h.get("_close_price", h["entry"])), 0) for h in todays)
    profit_factor = safe_div(gross_profit, max(gross_loss, 1e-6), gross_profit or 0.0)

    by_regime: dict[str, list[int]] = {}   # [decisive trades, wins, breakevens]
    by_engine: dict[str, list[int]] = {}
    for h in todays:
        rk = "ranging" if h.get("regime", {}).get("is_ranging") else "trending"
        by_regime.setdefault(rk, [0, 0, 0])
        by_engine.setdefault(h["engine"], [0, 0, 0])
        result = h.get("result")
        by_regime[rk][0] += 1
        by_regime[rk][1] += 1 if result in ("tp2", "breakeven") else 0
        by_engine[h["engine"]][0] += 1
        by_engine[h["engine"]][1] += 1 if result in ("tp2", "breakeven") else 0
        if result == "breakeven":
            by_regime[rk][2] += 1
            by_engine[h["engine"]][2] += 1

    best = max(todays, key=lambda h: _r_multiple(h, h.get("_close_price", h["entry"])), default=None)
    worst = min(todays, key=lambda h: _r_multiple(h, h.get("_close_price", h["entry"])), default=None)

    cal = state["confidence_calibration"]["buckets"]
    cal_lines = [f"  {b}-{int(b)+9}%: {v['wins']}/{v['n']}" for b, v in sorted(cal.items())] or ["  (insufficient data)"]

    def _fmt_breakdown(d: dict[str, list[int]]) -> list[str]:
        out = []
        for k, v in d.items():
            line = f"  {k}: {v[1]}/{v[0]} wins"
            if v[2]:
                line += f"  (+{v[2]} BE)"
            out.append(line)
        return out

    lines = [
        f"<b>{ENGINE_SHORT} v{ENGINE_VERSION} — Daily Summary</b>",
        f"Date: {today_str} UTC",
        "",
        f"Total signals: {total}  |  Wins: {wins}  |  Losses: {losses}  |  Breakeven: {breakevens}",
        f"Win rate: {win_rate:.1f}%  |  Profit factor: {profit_factor:.2f}  |  Avg RR: {avg_rr:.2f}",
        f"Avg hold time: {avg_hold:.1f}h",
        "",
        "By regime:",
    ] + _fmt_breakdown(by_regime) + [
        "",
        "By engine:",
    ] + _fmt_breakdown(by_engine) + [
        "",
        f"Best setup: {best['symbol']} ({best['engine']}) {_r_multiple(best, best.get('_close_price', best['entry'])):.2f}R" if best else "Best setup: n/a",
        f"Worst setup: {worst['symbol']} ({worst['engine']}) {_r_multiple(worst, worst.get('_close_price', worst['entry'])):.2f}R" if worst else "Worst setup: n/a",
        "",
        "Confidence calibration (bucket: wins/n):",
    ] + cal_lines + [
        "",
        "Learning: engine weights adapt continuously from win rate, profit factor, and calibration error (see engine_stats).",
    ]
    return "\n".join(lines)


# ==============================================================================
# SECTION 11: ORCHESTRATION / MAIN SCAN LOOP
# ==============================================================================

def cooldown_key(symbol: str, direction: str) -> str:
    return f"{symbol}:{direction}"


def is_on_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    last = state["cooldowns"].get(cooldown_key(symbol, direction))
    return last is not None and (bar_index - last) < SIGNAL_COOLDOWN_BARS_LTF


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> None:
    state["cooldowns"][cooldown_key(symbol, direction)] = bar_index


def has_active_signal(state: dict, symbol: str) -> bool:
    return any(s["symbol"] == symbol and s["status"] != "closed" for s in state["active_signals"])


def run_scan(reference_ms: Optional[int] = None) -> dict:
    """Executes exactly one scan-per-run cycle. Returns a summary dict for logging/tests."""
    setup_logging()
    reset_indicator_cache()
    if reference_ms is None:
        reference_ms = int(time.time() * 1000)
    now = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc)

    state = load_state()
    candle_cache = load_candle_cache()
    state["run_count"] = state.get("run_count", 0) + 1

    summary = {"signals_sent": 0, "symbols_scanned": 0, "errors": 0}

    try:
        market_snapshot = get_market_snapshot(reference_ms)

        bundles: dict[str, dict] = {}
        for symbol in WATCHLIST:
            try:
                bundle = fetch_all_candles(symbol, reference_ms, candle_cache)
                if bundle:
                    bundles[symbol] = bundle
                    summary["symbols_scanned"] += 1
            except Exception as exc:
                log.error("Failed to fetch candles for %s: %s", symbol, exc)
                summary["errors"] += 1

        if not bundles:
            log.error("No symbol data available this run -- aborting scan, preserving state.")
            save_state(state)
            return summary

        btc_bundle = bundles.get("BTC")
        btc_bias, btc_strength = compute_btc_regime(btc_bundle)
        breadth = compute_breadth(bundles, btc_bias)

        analyses: dict[str, SymbolAnalysis] = {}
        all_candidates: list[Candidate] = []

        for symbol, bundle in bundles.items():
            try:
                market_price = market_snapshot.get(symbol, {}).get("mark_price") or bundle[TF_LTF_EXEC][-1]["c"]
                spread_pct = get_l2_spread_pct(symbol)
                ctx = build_adaptive_context(state, symbol, bundle, btc_bias, btc_strength, breadth,
                                              reference_ms, spread_pct)
                sa = build_symbol_analysis(symbol, bundle, ctx, reference_ms, market_price)
                analyses[symbol] = sa

                if has_active_signal(state, symbol):
                    continue

                candidates = run_all_engines(sa)
                bar_index = len(bundle[TF_LTF_EXEC])
                candidates = [c for c in candidates if not is_on_cooldown(state, symbol, c.direction, bar_index)]
                all_candidates.extend(candidates)
            except Exception as exc:
                log.error("Analysis failed for %s: %s", symbol, exc)
                summary["errors"] += 1

        selected = decision_engine_select(all_candidates, analyses, state, MAX_SIGNALS_PER_RUN)

        tg_on = telegram_enabled()
        for cand, score in selected:
            sa = analyses[cand.symbol]
            if len(state["active_signals"]) >= MAX_CONCURRENT_ACTIVE_SIGNALS:
                log.info("Max concurrent active signals reached -- holding remaining candidates.")
                break
            msg_id = send_signal(cand, sa, score) if tg_on else None
            record_signal_history(state, cand, score, sa, msg_id)
            bar_index = len(sa.bundle[TF_LTF_EXEC])
            update_cooldown(state, cand.symbol, cand.direction, bar_index)
            summary["signals_sent"] += 1
            log.info("Signal: %s %s %s conf=%.1f rr=%.2f score=%.1f",
                      cand.engine, cand.symbol, cand.direction, cand.confidence, cand.expected_rr, score)

        market_prices = {sym: v.get("mark_price", 0.0) for sym, v in market_snapshot.items()}
        check_active_signals(state, market_prices, bundles, tg_on)

        if should_send_daily_summary(state, now):
            summary_text = build_daily_summary(state, now)
            send_telegram(summary_text)
            state["last_daily_summary_date"] = now.strftime("%Y-%m-%d")

        prune_state(state)

    except ShutdownRequested:
        log.warning("Shutdown requested -- saving state and exiting cleanly.")
    except Exception as exc:
        log.exception("Unhandled error during scan (state preserved): %s", exc)
        summary["errors"] += 1
    finally:
        save_state(state)
        save_candle_cache(candle_cache)

    log.info("Scan complete: symbols=%d signals=%d errors=%d",
              summary["symbols_scanned"], summary["signals_sent"], summary["errors"])
    return summary


def main() -> int:
    setup_logging()
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)
    log.info("%s v%s starting scan-per-run cycle.", ENGINE_NAME, ENGINE_VERSION)
    if not HYPERLIQUID_API_URL:
        log.error("HYPERLIQUID_API_URL not configured.")
        return 1
    try:
        run_scan()
    except ShutdownRequested:
        log.warning("Exited due to shutdown signal.")
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
