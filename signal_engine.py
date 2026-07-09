#!/usr/bin/env python3
# pip install requests
"""
================================================================================
 CHAMELEON-X  --  Adaptive Confluence Signal Engine  (Hyperliquid perpetuals)
================================================================================
A single, unified crypto-perps signal engine, built to the CHAMELEON-X master
build spec. It is a synthesis, not a port of any one predecessor: the
watchlist and core infrastructure below were seeded from six prior engines
found on disk (Axis, Ecliptic, Kairos, Kestrel, Meridian, Zenith) which all
shared one operator watchlist and one Hyperliquid + Telegram operating model.
Every piece of *decision logic* (regime/chaos classification, confluence
scoring, the self-balancing adaptive threshold, entry/SL/TP construction) is
implemented fresh, directly from the CHAMELEON-X spec, rather than inherited
from any single predecessor's quirks.

WHAT IT DOES
    Scans a Hyperliquid perpetuals watchlist on a fixed cadence (every 15
    minutes, driven by an external cron trigger), scores multi-timeframe
    confluence per candidate setup, and -- only when a candidate clears the
    engine's own, continuously self-adjusted confluence bar -- posts a high
    conviction entry/SL/TP alert to Telegram. It then tracks every open alert
    candle-by-candle to its outcome (SL / TP1 / TP2 / breakeven) and reacts on
    the original Telegram message when it resolves. Once a day it posts a
    24h performance summary.

WHAT IT DOES NOT DO
    It never places orders, holds funds, or touches a private key. It is a
    read-only market-data + notification system against Hyperliquid's public
    endpoints only.

WHY "CHAMELEON-X"
    A chameleon survives by changing to match its environment, not by being
    one fixed color. This engine doesn't hard-code what "good" looks like --
    it senses the regime and the noise level every run and adapts its own
    strictness (Section 7), its stop-buffer width, and its setup family
    (Section 5) in real time.

OPERATING MODEL
    Stateless scan-per-run. cron-job.org (or any scheduler) hits one HTTP
    endpoint every 15 minutes -- see `--serve` below -- or the script can be
    invoked directly for a single one-shot scan. Each invocation:

        read state.json -> resolve watchlist -> pull fresh Hyperliquid data
        -> classify regime + chaos -> score candidates -> recompute the
        adaptive threshold -> emit signals that clear it -> update tracked
        open signals (SL/TP/expiry) -> maybe send the 08:00 UTC daily summary
        -> write state.json -> exit / respond 200.

    No database, no long-running process required. Single file:

        python3 chameleon_x.py                 # one-shot scan, then exit
        python3 chameleon_x.py --serve          # HTTP server for cron-job.org
        python3 chameleon_x.py --serve --port 8080

    Required env vars (unless CHAMELEON_DRY_RUN=true): TG_BOT_TOKEN, TG_CHAT_ID
    Optional env vars: CHAMELEON_STATE_FILE, CHAMELEON_RUN_LOG,
        CHAMELEON_DRY_RUN=true   (skip Telegram calls, print to stdout instead)
        CHAMELEON_SHADOW_MODE=true (log-only calibration mode: run full
            analysis incl. near-misses, never post to Telegram -- Section 15.6)
        HL_INFO_URL, SCAN_WORKERS, HL_MIN_INTERVAL_S

ACCEPTANCE-CHECKLIST TRACEABILITY
    Section references ("Section N") in comments throughout this file point
    back to the corresponding section of the CHAMELEON-X master build prompt,
    so behavior can be audited against the spec directly.
================================================================================
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

import requests

__version__ = "1.0.0"
ENGINE_NAME = "CHAMELEON-X"
ENGINE_TAG = "CHAMELEON-X"

# ============================================================================
# SECTION 16 / CONFIGURATION
# ============================================================================

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
DRY_RUN = os.getenv("CHAMELEON_DRY_RUN", "false").strip().lower() in ("1", "true", "yes")
SHADOW_MODE = os.getenv("CHAMELEON_SHADOW_MODE", "false").strip().lower() in ("1", "true", "yes")

# Section 15.8 -- secrets hygiene: token/chat id come from the environment
# only, never hardcoded or committed. Both DRY_RUN and SHADOW_MODE skip real
# Telegram calls, so local testing never requires live credentials.
if not DRY_RUN and not SHADOW_MODE:
    if not TG_BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN env var is required (or set CHAMELEON_DRY_RUN=true)")
    if not TG_CHAT_ID:
        raise RuntimeError("TG_CHAT_ID env var is required (or set CHAMELEON_DRY_RUN=true)")

HL_INFO_URL = os.getenv("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
STATE_FILE = os.getenv("CHAMELEON_STATE_FILE", "state.json")
RUN_LOG_FILE = os.getenv("CHAMELEON_RUN_LOG", "chameleon_x_runs.log")
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "6"))
HL_MIN_INTERVAL_S = float(os.getenv("HL_MIN_INTERVAL_S", "0.12"))

# --- Section 3: Watchlist --------------------------------------------------
# Core static list seeded identically from all six reference engines on disk
# (Axis, Ecliptic, Kairos, Kestrel, Meridian, Zenith all shared this exact
# operator watchlist -- native Hyperliquid coin symbols, Axis/Ecliptic/Zenith
# form rather than the *USDT-suffixed form some references used for a
# different venue's naming convention).
CORE_WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# Section 3 "Dynamic extension": daily-refreshed, liquidity-ranked additions
# on top of the core list.
DYNAMIC_TOP_N = int(os.getenv("CHAMELEON_DYNAMIC_TOP_N", "30"))
DYNAMIC_MIN_DAY_NTL_VLM = float(os.getenv("CHAMELEON_MIN_DAY_VLM", "5000000"))  # liquidity floor, $ notional/24h
WATCHLIST_REFRESH_INTERVAL_S = 24 * 3600

# --- Section 4: Multi-timeframe architecture (4-TF cascade) ----------------
TF_MACRO = "4h"      # regime + chaos, nothing trades directly off this
TF_SWING = "1h"      # swing thesis layer
TF_INTRADAY = "15m"  # intraday thesis layer / primary scanning cadence
TF_ENTRY = "5m"      # entry trigger + SL precision for intraday setups only
                      # (swing setups use 15m as their own SL-reference tf --
                      #  see Section 9's reference_candle rule)

INTERVAL_MS = {
    "5m": 5 * 60_000, "15m": 15 * 60_000, "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000, "1d": 24 * 60 * 60_000,
}
# Candle counts per timeframe. 4h gets extra history so the chaos index's
# 90-day ATR percentile (Section 5B) has a real distribution to rank against
# (90d / 4h bars ~= 540 bars); everything else follows the "150-300 is
# plenty" guidance from Section 3.
CANDLE_COUNT = {"4h": 560, "1h": 300, "15m": 300, "5m": 200}

# Indicator periods
EMA_FAST, EMA_MID, EMA_SLOW = 20, 50, 200
RSI_LEN = 14
ATR_LEN = 14
ADX_LEN = 14
BB_LEN, BB_MULT = 20, 2.0
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9

# --- Section 6: Confluence scoring model (points per factor) ---------------
PTS_TREND = 25
PTS_MOMENTUM = 20
PTS_STRUCTURE = 25
PTS_VOLUME = 15
PTS_KEY_LEVEL = 10
PTS_PERP_CONTRARIAN = 5
assert PTS_TREND + PTS_MOMENTUM + PTS_STRUCTURE + PTS_VOLUME + PTS_KEY_LEVEL + PTS_PERP_CONTRARIAN == 100

COUNTER_TREND_PENALTY = 8  # Section 5: counter-trend trades held to a stricter bar

# --- Section 7: Self-balancing adaptive threshold controller ---------------
BASE_THRESHOLD = 62.0
THRESHOLD_FLOOR = 50.0
THRESHOLD_CEILING = 76.0
DAILY_TARGET_MIN = 5
DAILY_TARGET_MAX = 10
CHAOS_EMA_ALPHA = 0.35  # smoothing for chaos_index_ema in state

# --- Section 9: Entry / SL / TP construction --------------------------------
SL_ATR_MULT_BASE = 0.35          # base ATR multiplier for the SL wick-safety buffer
SL_FIXED_MIN_BUFFER_PCT = 0.0015  # fixed_min_buffer, as a fraction of price
TP1_FALLBACK_R = 1.5
TP2_FALLBACK_R = 2.75
MIN_TP1_R = 1.0                  # quality floor: never post a trivial-reward setup

# --- Section 8 / 12: Cooldown, dedup, expiry --------------------------------
SYMBOL_COOLDOWN_S = 2 * 3600
DEDUP_ENTRY_OVERLAP_PCT = 0.35   # zones counted as "materially overlapping" past this fraction
SIGNAL_EXPIRY_BARS = 6           # bars of the signal's own trigger timeframe

# --- Section 15.1: Correlation guard ----------------------------------------
CORRELATION_SUPPRESS_THRESHOLD = 0.75
CORRELATION_LOOKBACK_BARS = 60   # 15m returns window used to estimate correlation

# --- Section 15.3: Funding / OI extremes ------------------------------------
FUNDING_EXTREME_ABS = 0.0004     # ~annualized-hourly funding rate considered "extreme"


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] {msg}"
    print(line, flush=True)


# ============================================================================
# SECTION 3 / 11: HYPERLIQUID API LAYER
# ============================================================================

_hl_request_lock = threading.Lock()
_hl_last_request_ts = 0.0
_hl_session = requests.Session()


def _throttle() -> None:
    global _hl_last_request_ts
    with _hl_request_lock:
        now = time.monotonic()
        wait = HL_MIN_INTERVAL_S - (now - _hl_last_request_ts)
        if wait > 0:
            time.sleep(wait)
        _hl_last_request_ts = time.monotonic()


def hl_post(payload: dict, retries: int = 5, timeout: int = 12):
    """POST to Hyperliquid /info with exponential backoff on 429/5xx
    (Section 15.2: API resilience -- never crash the whole scan on one
    failed call)."""
    for attempt in range(retries):
        _throttle()
        try:
            r = _hl_session.post(HL_INFO_URL, json=payload, timeout=timeout)
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(min(1.5 * (attempt + 1) + random.random(), 20))
                continue
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            if attempt == retries - 1:
                log(f"hl_post failed after {retries} attempts: {e}")
                return None
            time.sleep(0.6 * (attempt + 1) + random.random() * 0.3)
    return None


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = INTERVAL_MS[interval]
    return (reference_ms // step) * step


def filter_closed_candles(candles: list, interval: str, reference_ms: int) -> list:
    if not candles:
        return candles
    open_bar = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < open_bar]


def get_candles(symbol: str, interval: str, n: int, reference_ms: int) -> Optional[list]:
    end_ms = reference_ms
    start_ms = end_ms - (n + 5) * INTERVAL_MS[interval]
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms},
    }
    raw = hl_post(payload)
    if raw is None or not isinstance(raw, list):
        return None
    candles = []
    for c in raw:
        try:
            candles.append({
                "t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"]),
            })
        except (KeyError, TypeError, ValueError):
            continue
    candles = filter_closed_candles(candles, interval, reference_ms)
    return candles[-n:] if candles else []


def fetch_symbol_candles(symbol: str, reference_ms: int, need_macro_refresh: bool,
                          need_swing_refresh: bool, cached: dict) -> Optional[dict]:
    """Section 4 efficiency note: only refetch 4h/1h when a new candle has
    actually closed since the last run; always refetch 15m/5m (live every
    scan)."""
    out = {}
    plan = [
        (TF_MACRO, need_macro_refresh),
        (TF_SWING, need_swing_refresh),
        (TF_INTRADAY, True),
        (TF_ENTRY, True),
    ]
    for tf, must_fetch in plan:
        if not must_fetch and cached.get(tf):
            out[tf] = cached[tf]
            continue
        c = get_candles(symbol, tf, CANDLE_COUNT[tf], reference_ms)
        if c is None or len(c) < 40:
            # keep stale cache rather than dropping the symbol entirely if
            # we at least have something from a previous run
            if cached.get(tf):
                out[tf] = cached[tf]
                continue
            return None
        out[tf] = c
    return out


def get_meta_and_asset_ctxs() -> Optional[dict]:
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or not isinstance(raw, list) or len(raw) < 2:
        return None
    meta, ctxs = raw[0], raw[1]
    universe = meta.get("universe", [])
    out = {}
    for i, u in enumerate(universe):
        if i >= len(ctxs):
            break
        name = u.get("name")
        ctx = ctxs[i]
        try:
            out[name] = {
                "funding": float(ctx.get("funding", 0.0) or 0.0),
                "oi": float(ctx.get("openInterest", 0.0) or 0.0),
                "mark_px": float(ctx.get("markPx", 0.0) or 0.0),
                "day_ntl_vlm": float(ctx.get("dayNtlVlm", 0.0) or 0.0),
                "prev_day_px": float(ctx.get("prevDayPx", 0.0) or 0.0),
            }
        except (TypeError, ValueError):
            continue
    return out


def resolve_dynamic_watchlist(state: dict, reference_ms: int) -> list:
    """Section 3: daily-refreshed liquidity-ranked extension on top of the
    core static list. Cached in state.json; only recomputed once per day."""
    wl = state.setdefault("watchlist", {})
    last_refresh = wl.get("last_refreshed_ms", 0)
    if reference_ms - last_refresh < WATCHLIST_REFRESH_INTERVAL_S * 1000 and wl.get("dynamic_extension") is not None:
        return sorted(set(CORE_WATCHLIST) | set(wl.get("dynamic_extension", [])))

    ctxs = get_meta_and_asset_ctxs()
    if not ctxs:
        log("watchlist refresh: metaAndAssetCtxs unavailable, keeping previous dynamic extension")
        return sorted(set(CORE_WATCHLIST) | set(wl.get("dynamic_extension", [])))

    ranked = sorted(
        ((sym, c["day_ntl_vlm"], c["oi"]) for sym, c in ctxs.items()
         if c["day_ntl_vlm"] >= DYNAMIC_MIN_DAY_NTL_VLM),
        key=lambda t: (t[1] + t[2]), reverse=True,
    )
    top = [sym for sym, _, _ in ranked[:DYNAMIC_TOP_N]]
    excluded_low_liq = [sym for sym, c in ctxs.items() if c["day_ntl_vlm"] < DYNAMIC_MIN_DAY_NTL_VLM]

    dynamic_extension = sorted(set(top) - set(CORE_WATCHLIST))
    wl["dynamic_extension"] = dynamic_extension
    wl["last_refreshed_ms"] = reference_ms
    wl["last_refreshed_utc"] = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).isoformat()
    wl["excluded_low_liquidity"] = excluded_low_liq[:50]
    wl["core"] = CORE_WATCHLIST
    log(f"watchlist refreshed: {len(dynamic_extension)} dynamic additions, "
        f"{len(excluded_low_liq)} symbols excluded below liquidity floor")
    return sorted(set(CORE_WATCHLIST) | set(dynamic_extension))


# ============================================================================
# MATH / INDICATORS
# ============================================================================

def safe(v, fb=0.0):
    try:
        f = float(v)
        return f if math.isfinite(f) else fb
    except (TypeError, ValueError):
        return fb


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


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
        if i < period - 1:
            out.append(sum(vals[:i + 1]) / (i + 1))
        else:
            out.append(sum(vals[i - period + 1:i + 1]) / period)
    return out


def stdev(vals: list, period: int) -> list:
    out = []
    for i in range(len(vals)):
        if i < period - 1:
            out.append(0.0)
        else:
            window = vals[i - period + 1:i + 1]
            m = sum(window) / period
            out.append(math.sqrt(sum((x - m) ** 2 for x in window) / period))
    return out


def rsi(closes: list, period: int = RSI_LEN) -> list:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    out = [50.0] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(closes)):
        if i > period:
            avg_g = (avg_g * (period - 1) + gains[i - 1]) / period
            avg_l = (avg_l * (period - 1) + losses[i - 1]) / period
        rs = safe_div(avg_g, avg_l, default=999.0) if avg_l != 0 else 999.0
        out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def atr_series(candles: list, period: int = ATR_LEN) -> list:
    trs = []
    for i, c in enumerate(candles):
        if i == 0:
            trs.append(c["h"] - c["l"])
        else:
            pc = candles[i - 1]["c"]
            trs.append(max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc)))
    out = [trs[0]]
    for i in range(1, len(trs)):
        if i < period:
            out.append(sum(trs[:i + 1]) / (i + 1))
        else:
            out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def adx_dmi(candles: list, period: int = ADX_LEN) -> tuple:
    n = len(candles)
    plus_dm, minus_dm, trs = [0.0] * n, [0.0] * n, [0.0] * n
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        dn = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm[i] = up if (up > dn and up > 0) else 0.0
        minus_dm[i] = dn if (dn > up and dn > 0) else 0.0
        pc = candles[i - 1]["c"]
        trs[i] = max(candles[i]["h"] - candles[i]["l"], abs(candles[i]["h"] - pc), abs(candles[i]["l"] - pc))

    def wilder_smooth(vals):
        out = [0.0] * n
        if n <= period:
            return out
        out[period] = sum(vals[1:period + 1])
        for i in range(period + 1, n):
            out[i] = out[i - 1] - out[i - 1] / period + vals[i]
        return out

    str_ = wilder_smooth(trs)
    s_plus = wilder_smooth(plus_dm)
    s_minus = wilder_smooth(minus_dm)
    plus_di = [0.0] * n
    minus_di = [0.0] * n
    dx = [0.0] * n
    for i in range(n):
        if str_[i] > 0:
            plus_di[i] = 100.0 * s_plus[i] / str_[i]
            minus_di[i] = 100.0 * s_minus[i] / str_[i]
        denom = plus_di[i] + minus_di[i]
        dx[i] = 100.0 * abs(plus_di[i] - minus_di[i]) / denom if denom > 0 else 0.0
    adx = [0.0] * n
    start = period * 2
    if n > start:
        adx[start] = sum(dx[period:start]) / period if start > period else 0.0
        for i in range(start + 1, n):
            adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period
    return adx, plus_di, minus_di


def bollinger(closes: list, period: int = BB_LEN, mult: float = BB_MULT) -> tuple:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    upper = [mid[i] + mult * sd[i] for i in range(len(closes))]
    lower = [mid[i] - mult * sd[i] for i in range(len(closes))]
    width_pct = [safe_div(upper[i] - lower[i], mid[i]) * 100.0 for i in range(len(closes))]
    return upper, mid, lower, width_pct


def macd_hist(closes: list, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL) -> list:
    ef, es = ema(closes, fast), ema(closes, slow)
    macd_line = [ef[i] - es[i] for i in range(len(closes))]
    sig = ema(macd_line, signal)
    return [macd_line[i] - sig[i] for i in range(len(closes))]


def obv(closes: list, volumes: list) -> list:
    out = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            out.append(out[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            out.append(out[-1] - volumes[i])
        else:
            out.append(out[-1])
    return out


def percentile_rank(vals: list, x: float) -> float:
    if not vals:
        return 50.0
    below = sum(1 for v in vals if v <= x)
    return 100.0 * below / len(vals)


def detect_rsi_divergence(closes: list, rsi_vals: list, lookback: int = 25) -> Optional[str]:
    """Bonus factor for reversal setups at regime exhaustion (Section
    15.4)."""
    if len(closes) < lookback + 5:
        return None
    seg_c, seg_r = closes[-lookback:], rsi_vals[-lookback:]
    lo_i = seg_c.index(min(seg_c))
    hi_i = seg_c.index(max(seg_c))
    if lo_i > lookback * 0.5 and lo_i > 0:
        prior_i = seg_c[:lo_i].index(min(seg_c[:lo_i]))
        if seg_c[lo_i] < seg_c[prior_i] and seg_r[lo_i] > seg_r[prior_i]:
            return "bullish"
    if hi_i > lookback * 0.5 and hi_i > 0:
        prior_i = seg_c[:hi_i].index(max(seg_c[:hi_i]))
        if seg_c[hi_i] > seg_c[prior_i] and seg_r[hi_i] < seg_r[prior_i]:
            return "bearish"
    return None


def swing_points(candles: list, left: int = 2, right: int = 2) -> tuple:
    """Simple fractal swing-high/low detector used for structure + key
    levels (Sections 5, 6, 9)."""
    highs_idx, lows_idx = [], []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        if candles[i]["h"] == max(window_h) and window_h.count(candles[i]["h"]) == 1:
            highs_idx.append(i)
        if candles[i]["l"] == min(window_l) and window_l.count(candles[i]["l"]) == 1:
            lows_idx.append(i)
    return highs_idx, lows_idx


def structure_bias(candles: list, lookback: int = 20) -> str:
    """HH/HL vs LH/LL read over the trailing window (Section 5A)."""
    seg = candles[-lookback:] if len(candles) >= lookback else candles
    highs_idx, lows_idx = swing_points(seg, 1, 1)
    highs = [seg[i]["h"] for i in highs_idx]
    lows = [seg[i]["l"] for i in lows_idx]
    hh = sum(1 for i in range(1, len(highs)) if highs[i] > highs[i - 1])
    lh = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i - 1])
    hl = sum(1 for i in range(1, len(lows)) if lows[i] > lows[i - 1])
    ll = sum(1 for i in range(1, len(lows)) if lows[i] < lows[i - 1])
    bull_votes = hh + hl
    bear_votes = lh + ll
    if bull_votes > bear_votes + 1:
        return "bullish"
    if bear_votes > bull_votes + 1:
        return "bearish"
    return "neutral"


def detect_whipsaws(candles: list, lookback: int = 20) -> int:
    """False-breakout / quick-reversal count over the last `lookback`
    candles (Section 5B whipsaw_count component)."""
    seg = candles[-lookback:] if len(candles) >= lookback else candles
    count = 0
    for i in range(2, len(seg)):
        prev, cur, nxt_ref = seg[i - 2], seg[i - 1], seg[i]
        # broke prior high then closed back below it within 1-2 bars
        if cur["h"] > prev["h"] and cur["c"] < prev["h"] and nxt_ref["c"] < prev["h"]:
            count += 1
        elif cur["l"] < prev["l"] and cur["c"] > prev["l"] and nxt_ref["c"] > prev["l"]:
            count += 1
    return count


def detect_liquidity_sweep(candles: list, direction: str, lookback: int = 20) -> Optional[dict]:
    """A candle that wicks beyond a recent swing high/low then closes back
    inside it -- the reversal trigger pattern used by both the neutral
    mean-reversion pathway and the counter-trend exhaustion pathway
    (Section 5)."""
    if len(candles) < lookback + 3:
        return None
    seg = candles[-lookback:]
    highs_idx, lows_idx = swing_points(seg[:-1], 1, 1)
    last = candles[-1]
    if direction == "long":  # sweep of a swing low, closing back above it
        if not lows_idx:
            return None
        piv = seg[lows_idx[-1]]
        if last["l"] < piv["l"] and last["c"] > piv["l"]:
            return {"candle": last, "pivot": piv}
    else:  # short: sweep of a swing high, closing back below it
        if not highs_idx:
            return None
        piv = seg[highs_idx[-1]]
        if last["h"] > piv["h"] and last["c"] < piv["h"]:
            return {"candle": last, "pivot": piv}
    return None


def is_pin_bar(c: dict, direction: str) -> bool:
    body = abs(c["c"] - c["o"])
    rng = c["h"] - c["l"]
    if rng <= 0:
        return False
    if direction == "long":
        lower_wick = min(c["c"], c["o"]) - c["l"]
        return lower_wick > body * 2 and lower_wick / rng > 0.5
    upper_wick = c["h"] - max(c["c"], c["o"])
    return upper_wick > body * 2 and upper_wick / rng > 0.5


def is_engulfing(prev: dict, cur: dict, direction: str) -> bool:
    if direction == "long":
        return cur["c"] > cur["o"] and cur["c"] >= prev["o"] and cur["o"] <= prev["c"]
    return cur["c"] < cur["o"] and cur["c"] <= prev["o"] and cur["o"] >= prev["c"]


@dataclass
class Indicators:
    candles: list
    closes: list
    highs: list
    lows: list
    vols: list
    ema_fast: list
    ema_mid: list
    ema_slow: list
    rsi: list
    atr: list
    adx: list
    plus_di: list
    minus_di: list
    bb_upper: list
    bb_mid: list
    bb_lower: list
    bb_width_pct: list
    macd_h: list
    obv: list
    divergence: Optional[str]
    avg_vol20: float


def compute_indicators(candles: list) -> Indicators:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c["v"] for c in candles]
    r = rsi(closes)
    return Indicators(
        candles=candles, closes=closes, highs=highs, lows=lows, vols=vols,
        ema_fast=ema(closes, EMA_FAST), ema_mid=ema(closes, EMA_MID), ema_slow=ema(closes, EMA_SLOW),
        rsi=r, atr=atr_series(candles), **dict(zip(("adx", "plus_di", "minus_di"), adx_dmi(candles))),
        **dict(zip(("bb_upper", "bb_mid", "bb_lower", "bb_width_pct"), bollinger(closes))),
        macd_h=macd_hist(closes), obv=obv(closes, vols),
        divergence=detect_rsi_divergence(closes, r),
        avg_vol20=sum(vols[-20:]) / max(1, len(vols[-20:])),
    )


# ============================================================================
# SECTION 5: REGIME & CHAOS CLASSIFICATION
# ============================================================================

@dataclass
class RegimeRead:
    direction: str          # "bullish" | "bearish" | "neutral"
    adx: float
    ema_stack_up: bool
    ema_stack_down: bool
    structure: str


def classify_regime(ind4h: Indicators) -> RegimeRead:
    """Section 5A. Computed per-symbol from that symbol's own 4H series."""
    ef, em, es = ind4h.ema_fast[-1], ind4h.ema_mid[-1], ind4h.ema_slow[-1]
    ef_prev, em_prev = ind4h.ema_fast[-5], ind4h.ema_mid[-5]
    stack_up = ef > em > es and ef > ef_prev and em > em_prev
    stack_down = ef < em < es and ef < ef_prev and em < em_prev
    adx_now = ind4h.adx[-1]
    struct = structure_bias(ind4h.candles)
    if stack_up and adx_now >= 20 and struct in ("bullish", "neutral"):
        direction = "bullish"
    elif stack_down and adx_now >= 20 and struct in ("bearish", "neutral"):
        direction = "bearish"
    elif adx_now < 18 or struct == "neutral":
        direction = "neutral"
    elif stack_up:
        direction = "bullish"
    elif stack_down:
        direction = "bearish"
    else:
        direction = "neutral"
    return RegimeRead(direction=direction, adx=adx_now, ema_stack_up=stack_up,
                       ema_stack_down=stack_down, structure=struct)


@dataclass
class ChaosRead:
    chaos_index: float
    atr_percentile: float
    wick_ratio_avg: float
    whipsaw_scaled: float


def compute_chaos_index(ind4h: Indicators) -> ChaosRead:
    """Section 5B, formula reproduced exactly:
        chaos_index = 0.40*atr_percentile + 0.35*wick_ratio_avg(as %) + 0.25*whipsaw_scaled
    """
    atr_hist = ind4h.atr[-540:] if len(ind4h.atr) >= 540 else ind4h.atr
    atr_pctile = percentile_rank(atr_hist[:-1], ind4h.atr[-1]) if len(atr_hist) > 1 else 50.0

    last20 = ind4h.candles[-20:] if len(ind4h.candles) >= 20 else ind4h.candles
    ratios = []
    for c in last20:
        rng = c["h"] - c["l"]
        if rng > 0:
            ratios.append((rng - abs(c["c"] - c["o"])) / rng)
    wick_ratio_avg = sum(ratios) / len(ratios) if ratios else 0.0

    whipsaws = detect_whipsaws(ind4h.candles, lookback=20)
    whipsaw_scaled = min(100.0, whipsaws * 100.0 / 20.0 * 4)  # scale 0-20 occurrences -> 0-100

    chaos = 0.40 * atr_pctile + 0.35 * (wick_ratio_avg * 100.0) + 0.25 * whipsaw_scaled
    chaos = max(0.0, min(100.0, chaos))
    return ChaosRead(chaos_index=chaos, atr_percentile=atr_pctile,
                      wick_ratio_avg=wick_ratio_avg, whipsaw_scaled=whipsaw_scaled)


# ============================================================================
# SECTION 6: CONFLUENCE SCORING MODEL
# ============================================================================

@dataclass
class ScoreBreakdown:
    trend: float = 0.0
    momentum: float = 0.0
    structure: float = 0.0
    volume: float = 0.0
    key_level: float = 0.0
    perp_contrarian: float = 0.0
    notes: list = field(default_factory=list)

    @property
    def total(self) -> float:
        return (self.trend + self.momentum + self.structure +
                self.volume + self.key_level + self.perp_contrarian)


def score_trend_alignment(direction: str, regime: RegimeRead, ind_setup: Indicators,
                           is_range_fade: bool) -> tuple:
    """0-25: HTF EMA stack alignment + ADX strength in the trade's
    direction, or explicit range-fade logic in a neutral regime."""
    if is_range_fade:
        # in neutral regime, "alignment" means low ADX / genuine range, which
        # is exactly what makes a fade valid
        pts = 25.0 * max(0.0, (1 - min(regime.adx, 25) / 25.0))
        return pts, f"range-fade context (4H ADX {regime.adx:.0f})"
    aligned = (direction == "long" and regime.ema_stack_up) or (direction == "short" and regime.ema_stack_down)
    adx_component = min(regime.adx, 40.0) / 40.0
    pts = (18.0 if aligned else 6.0) + 7.0 * adx_component
    pts = min(pts, PTS_TREND)
    label = "aligned" if aligned else "counter-trend"
    return pts, f"{label} with 4H regime (ADX {regime.adx:.0f})"


def score_momentum(direction: str, ind: Indicators, is_reversal: bool) -> tuple:
    """0-20: RSI zone, MACD histogram slope, divergence bonus on reversal
    setups."""
    r = ind.rsi[-1]
    pts = 0.0
    notes = []
    if direction == "long":
        if 40 <= r <= 65:
            pts += 8
        elif r < 40:
            pts += 5
        rsi_ok = r < 70
    else:
        if 35 <= r <= 60:
            pts += 8
        elif r > 60:
            pts += 5
        rsi_ok = r > 30
    if not rsi_ok:
        pts *= 0.5
        notes.append("RSI extended")
    hist_now, hist_prev = ind.macd_h[-1], ind.macd_h[-3] if len(ind.macd_h) > 3 else ind.macd_h[-1]
    slope_favors = (hist_now > hist_prev) if direction == "long" else (hist_now < hist_prev)
    if slope_favors:
        pts += 7
        notes.append("MACD histogram turning in favor")
    if is_reversal and ind.divergence == ("bullish" if direction == "long" else "bearish"):
        pts += 5
        notes.append(f"{ind.divergence} RSI divergence")
    return min(pts, PTS_MOMENTUM), "; ".join(notes) if notes else f"RSI {r:.0f}"


def score_structure(direction: str, ind: Indicators, pattern: str) -> tuple:
    """0-25: break of structure, liquidity-sweep-and-reversal, order-block
    /zone retest, engulfing/pin bar at a defined level."""
    pts = 0.0
    notes = []
    sweep = detect_liquidity_sweep(ind.candles, direction)
    if sweep:
        pts += 12
        notes.append("liquidity-sweep reversal candle")
    last, prev = ind.candles[-1], ind.candles[-2]
    if is_pin_bar(last, direction):
        pts += 7
        notes.append("pin bar")
    elif is_engulfing(prev, last, direction):
        pts += 7
        notes.append("engulfing candle")
    struct = structure_bias(ind.candles)
    if (direction == "long" and struct == "bullish") or (direction == "short" and struct == "bearish"):
        pts += 6
        notes.append(f"{struct} structure (HH/HL)" if direction == "long" else f"{struct} structure (LH/LL)")
    if pattern == "breakout_retest":
        pts += 4
        notes.append("breakout-retest")
    elif pattern == "compression_breakout":
        pts += 4
        notes.append("range-compression resolving")
    return min(pts, PTS_STRUCTURE), "; ".join(notes) if notes else "no strong price-action trigger"


def score_volume(direction: str, ind: Indicators) -> tuple:
    """0-15: volume vs rolling average at the trigger candle, OBV slope."""
    last_vol = ind.vols[-1]
    pts = 0.0
    notes = []
    if ind.avg_vol20 > 0 and last_vol > ind.avg_vol20 * 1.3:
        pts += 9
        notes.append("volume expansion on trigger candle")
    elif ind.avg_vol20 > 0 and last_vol > ind.avg_vol20:
        pts += 5
    obv_slope = ind.obv[-1] - ind.obv[-6] if len(ind.obv) > 6 else 0.0
    obv_favors = (obv_slope > 0) if direction == "long" else (obv_slope < 0)
    if obv_favors:
        pts += 6
        notes.append("OBV confirming")
    return min(pts, PTS_VOLUME), "; ".join(notes) if notes else "average volume"


def score_key_level(direction: str, price: float, ind: Indicators) -> tuple:
    """0-10: proximity to prior swing high/low, VWAP proxy, session
    high/low, round number, or fib retracement zone."""
    pts = 0.0
    notes = []
    highs_idx, lows_idx = swing_points(ind.candles, 2, 2)
    levels = [ind.candles[i]["h"] for i in highs_idx[-5:]] + [ind.candles[i]["l"] for i in lows_idx[-5:]]
    atr_now = ind.atr[-1] or (price * 0.005)
    near_level = any(abs(price - lvl) <= atr_now * 0.6 for lvl in levels)
    if near_level:
        pts += 5
        notes.append("near prior swing level")
    round_step = 10 ** (len(str(int(price))) - 2) if price >= 1 else 0.01
    if round_step and abs(price % round_step) / round_step < 0.05:
        pts += 2
        notes.append("near round number")
    ema_mid = ind.ema_mid[-1]
    if abs(price - ema_mid) <= atr_now * 0.75:
        pts += 3
        notes.append("at EMA50 confluence")
    return min(pts, PTS_KEY_LEVEL), "; ".join(notes) if notes else "no key-level confluence"


def score_perp_contrarian(direction: str, symbol: str, asset_ctxs: Optional[dict]) -> tuple:
    """0-5: funding rate / OI skew supporting the trade direction at an
    extreme (Section 15.3)."""
    if not asset_ctxs or symbol not in asset_ctxs:
        return 0.0, ""
    funding = asset_ctxs[symbol]["funding"]
    # extreme positive funding (longs paying) supports a contrarian short;
    # extreme negative funding supports a contrarian long
    if direction == "short" and funding > FUNDING_EXTREME_ABS:
        return PTS_PERP_CONTRARIAN, f"funding {funding*100:.3f}% favors short squeeze-out"
    if direction == "long" and funding < -FUNDING_EXTREME_ABS:
        return PTS_PERP_CONTRARIAN, f"funding {funding*100:.3f}% favors long squeeze-out"
    return 0.0, ""


def score_candidate(direction: str, regime: RegimeRead, ind_setup: Indicators, price: float,
                     symbol: str, pattern: str, is_range_fade: bool, is_reversal: bool,
                     asset_ctxs: Optional[dict]) -> ScoreBreakdown:
    sb = ScoreBreakdown()
    sb.trend, n1 = score_trend_alignment(direction, regime, ind_setup, is_range_fade)
    sb.momentum, n2 = score_momentum(direction, ind_setup, is_reversal)
    sb.structure, n3 = score_structure(direction, ind_setup, pattern)
    sb.volume, n4 = score_volume(direction, ind_setup)
    sb.key_level, n5 = score_key_level(direction, price, ind_setup)
    sb.perp_contrarian, n6 = score_perp_contrarian(direction, symbol, asset_ctxs)
    sb.notes = [n for n in (n3, n1, n2, n4, n5, n6) if n]
    return sb


# ============================================================================
# SECTION 7: SELF-BALANCING ADAPTIVE THRESHOLD CONTROLLER
# ============================================================================

def compute_adaptive_threshold(state: dict, chaos_index: float, reference_ms: int) -> float:
    """Reproduces the illustrative controller in Section 7 exactly, with the
    constants promoted to the CONFIGURATION block above."""
    adaptive = state.setdefault("adaptive", {})
    prev_ema = adaptive.get("chaos_index_ema", chaos_index)
    chaos_ema = CHAOS_EMA_ALPHA * chaos_index + (1 - CHAOS_EMA_ALPHA) * prev_ema
    adaptive["chaos_index_ema"] = chaos_ema

    if chaos_ema > 70:
        adj = 10.0
    elif chaos_ema > 55:
        adj = 5.0
    elif chaos_ema < 30:
        adj = -8.0
    elif chaos_ema < 45:
        adj = -4.0
    else:
        adj = 0.0

    now_utc = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc)
    hours_elapsed = now_utc.hour + now_utc.minute / 60.0
    hours_left = 24.0 - hours_elapsed
    expected_by_now = (hours_elapsed / 24.0) * DAILY_TARGET_MIN
    signals_today = adaptive.get("signals_today", 0)
    if signals_today < expected_by_now * 0.7 and hours_left > 2:
        adj -= 3.0

    new_threshold = max(THRESHOLD_FLOOR, min(THRESHOLD_CEILING, BASE_THRESHOLD + adj))
    adaptive["current_threshold"] = new_threshold
    adaptive["base_threshold"] = BASE_THRESHOLD
    adaptive["daily_target_min"] = DAILY_TARGET_MIN
    return new_threshold


def roll_daily_counters_if_needed(state: dict, reference_ms: int) -> None:
    adaptive = state.setdefault("adaptive", {})
    today = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    if adaptive.get("date_utc") != today:
        adaptive["date_utc"] = today
        adaptive["signals_today"] = 0


# ============================================================================
# SECTION 9: ENTRY / SL / TP CONSTRUCTION
# ============================================================================

@dataclass
class Levels:
    entry_low: float
    entry_high: float
    sl: float
    tp1: float
    tp2: Optional[float]
    rr1: float
    rr2: Optional[float]


def build_sl_buffer(atr_ref: float, price: float, chaos_index: float) -> float:
    """Buffer scales with chaos: wider in high-chaos conditions, tighter
    (better R:R, still safe) in clean conditions -- Section 9."""
    chaos_mult = 1.0 + (chaos_index / 100.0)  # 1.0x (clean) .. 2.0x (max chaos)
    atr_component = SL_ATR_MULT_BASE * chaos_mult * atr_ref
    fixed_min = SL_FIXED_MIN_BUFFER_PCT * price
    return max(fixed_min, atr_component)


def find_take_profits(direction: str, entry: float, sl: float, ind: Indicators, chaos_index: float) -> tuple:
    """Section 9 TP construction: nearest meaningful structure for TP1,
    next major structure for TP2, with R-multiple fallbacks and a
    single-TP mode when only one clean level exists."""
    risk = abs(entry - sl)
    if risk <= 0:
        return None, None
    highs_idx, lows_idx = swing_points(ind.candles, 2, 2)
    if direction == "long":
        candidates = sorted(h for h in (ind.candles[i]["h"] for i in highs_idx) if h > entry)
    else:
        candidates = sorted((l for l in (ind.candles[i]["l"] for i in lows_idx) if l < entry), reverse=True)

    def r_of(level):
        return abs(level - entry) / risk

    tp1 = next((lvl for lvl in candidates if r_of(lvl) >= MIN_TP1_R), None)
    if tp1 is None:
        tp1 = entry + TP1_FALLBACK_R * risk if direction == "long" else entry - TP1_FALLBACK_R * risk
    tp2 = next((lvl for lvl in candidates if r_of(lvl) > r_of(tp1) + 0.25), None)
    if tp2 is None:
        fallback2 = entry + TP2_FALLBACK_R * risk if direction == "long" else entry - TP2_FALLBACK_R * risk
        # only offer a synthetic TP2 if it's meaningfully beyond TP1
        tp2 = fallback2 if r_of(fallback2) > r_of(tp1) + 0.5 else None
    return tp1, tp2


def construct_levels(direction: str, entry_ref_price: float, trigger_candle: dict,
                      ref_candles_for_sl: list, ind_setup: Indicators, chaos_index: float,
                      atr_ref: float) -> Optional[Levels]:
    """Section 9, golden rule: SL/TP always derived from real candle
    structure, never from a blind percentage of mid-price."""
    # entry zone: current price to a small ATR fraction beyond it
    zone_pad = atr_ref * 0.15
    if direction == "long":
        entry_low, entry_high = entry_ref_price, entry_ref_price + zone_pad
    else:
        entry_low, entry_high = entry_ref_price - zone_pad, entry_ref_price

    # reference candle for the SL = the swing-low/high on the SL-reference
    # timeframe (5m for intraday, 15m for swing -- passed in by the caller)
    if direction == "long":
        ref_low = min(c["l"] for c in ref_candles_for_sl[-8:])
    else:
        ref_high = max(c["h"] for c in ref_candles_for_sl[-8:])

    buffer = build_sl_buffer(atr_ref, entry_ref_price, chaos_index)
    if direction == "long":
        sl = ref_low - buffer
    else:
        sl = ref_high + buffer

    entry_mid = (entry_low + entry_high) / 2.0
    tp1, tp2 = find_take_profits(direction, entry_mid, sl, ind_setup, chaos_index)
    if tp1 is None:
        return None
    risk = abs(entry_mid - sl)
    if risk <= 0:
        return None
    rr1 = abs(tp1 - entry_mid) / risk
    if rr1 < MIN_TP1_R:
        return None  # quality floor: never post a trivial-reward setup
    rr2 = (abs(tp2 - entry_mid) / risk) if tp2 is not None else None
    return Levels(entry_low=entry_low, entry_high=entry_high, sl=sl, tp1=tp1, tp2=tp2, rr1=rr1, rr2=rr2)


# ============================================================================
# SECTION 5 / 8: CANDIDATE / SETUP DETECTION
# ============================================================================

@dataclass
class Candidate:
    symbol: str
    direction: str          # "long" | "short"
    style: str              # "intraday" | "swing"
    regime: str             # "bullish" | "bearish" | "neutral"
    pattern: str
    score: ScoreBreakdown
    levels: Levels
    is_countertrend: bool
    reason: str


def _one_liner(direction: str, pattern: str, regime: str, notes: list) -> str:
    dir_word = "Long" if direction == "long" else "Short"
    pattern_word = pattern.replace("_", " ")
    tail = notes[0] if notes else "confluence of trend, momentum and structure"
    return f"{dir_word} {pattern_word} in a {regime} regime -- {tail}."


def detect_setups_for_timeframe(symbol: str, style: str, regime: RegimeRead,
                                 ind_setup: Indicators, ind_trigger: Indicators,
                                 ref_candles_for_sl: list, chaos_index: float,
                                 asset_ctxs: Optional[dict]) -> list:
    """Runs the regime-appropriate setup family (Section 5 coverage
    requirement) against one timeframe layer (1h for swing, 15m for
    intraday), using the trigger timeframe (5m/15m) purely to confirm the
    close-beyond-level entry trigger and to price the SL (Section 9)."""
    out = []
    price = ind_trigger.closes[-1]
    trigger_candle = ind_trigger.candles[-1]
    atr_ref = ind_trigger.atr[-1] or (price * 0.004)

    def try_candidate(direction, pattern, is_range_fade, is_reversal, is_countertrend):
        sb = score_candidate(direction, regime, ind_setup, price, symbol, pattern,
                              is_range_fade, is_reversal, asset_ctxs)
        levels = construct_levels(direction, price, trigger_candle, ref_candles_for_sl,
                                   ind_setup, chaos_index, atr_ref)
        if levels is None:
            return None
        cand = Candidate(
            symbol=symbol, direction=direction, style=style, regime=regime.direction,
            pattern=pattern, score=sb, levels=levels, is_countertrend=is_countertrend,
            reason=_one_liner(direction, pattern, regime.direction, sb.notes),
        )
        return cand

    ema_mid = ind_setup.ema_mid[-1]
    atr_setup = ind_setup.atr[-1] or (ind_setup.closes[-1] * 0.004)
    near_ema = abs(ind_setup.closes[-1] - ema_mid) <= atr_setup * 1.2
    broke_prior_high = trigger_candle["c"] > max(c["h"] for c in ind_trigger.candles[-6:-1])
    broke_prior_low = trigger_candle["c"] < min(c["l"] for c in ind_trigger.candles[-6:-1])

    if regime.direction == "bullish":
        if near_ema or broke_prior_high:
            pattern = "breakout_retest" if broke_prior_high else "pullback_continuation"
            c = try_candidate("long", pattern, False, False, False)
            if c:
                out.append(c)
        # counter-trend short only at HTF exhaustion / liquidity-sweep, stricter bar
        sweep = detect_liquidity_sweep(ind_setup.candles, "short")
        if sweep:
            c = try_candidate("short", "liquidity_sweep_reversal", False, True, True)
            if c:
                out.append(c)

    elif regime.direction == "bearish":
        if near_ema or broke_prior_low:
            pattern = "breakout_retest" if broke_prior_low else "pullback_continuation"
            c = try_candidate("short", pattern, False, False, False)
            if c:
                out.append(c)
        sweep = detect_liquidity_sweep(ind_setup.candles, "long")
        if sweep:
            c = try_candidate("long", "liquidity_sweep_reversal", False, True, True)
            if c:
                out.append(c)

    else:  # neutral / ranging -- Section 5's explicit gap-filler
        sweep_long = detect_liquidity_sweep(ind_setup.candles, "long")
        sweep_short = detect_liquidity_sweep(ind_setup.candles, "short")
        if sweep_long:
            c = try_candidate("long", "range_fade_reversal", True, True, False)
            if c:
                out.append(c)
        if sweep_short:
            c = try_candidate("short", "range_fade_reversal", True, True, False)
            if c:
                out.append(c)
        bb_width = ind_setup.bb_width_pct[-1]
        bb_width_hist = ind_setup.bb_width_pct[-60:-1] if len(ind_setup.bb_width_pct) > 61 else ind_setup.bb_width_pct[:-1]
        if bb_width_hist and percentile_rank(bb_width_hist, bb_width) < 20:
            if broke_prior_high:
                c = try_candidate("long", "compression_breakout", False, False, False)
                if c:
                    out.append(c)
            elif broke_prior_low:
                c = try_candidate("short", "compression_breakout", False, False, False)
                if c:
                    out.append(c)
    return out


def apply_required_threshold(candidates: list, base_threshold: float) -> list:
    passed = []
    for c in candidates:
        required = base_threshold + (COUNTER_TREND_PENALTY if c.is_countertrend else 0.0)
        if c.score.total >= required:
            passed.append(c)
    return passed


def soft_dedup(candidates: list) -> list:
    """Section 8: don't post both an intraday and a swing signal on the
    same symbol/direction with materially overlapping entry zones in the
    same run -- keep the higher-scoring one."""
    kept = []
    candidates_sorted = sorted(candidates, key=lambda c: c.score.total, reverse=True)
    for c in candidates_sorted:
        dup = False
        for k in kept:
            if k.symbol == c.symbol and k.direction == c.direction:
                lo = max(k.levels.entry_low, c.levels.entry_low)
                hi = min(k.levels.entry_high, c.levels.entry_high)
                overlap = max(0.0, hi - lo)
                span = max(k.levels.entry_high - k.levels.entry_low,
                           c.levels.entry_high - c.levels.entry_low, 1e-9)
                if overlap / span > DEDUP_ENTRY_OVERLAP_PCT:
                    dup = True
                    break
        if not dup:
            kept.append(c)
    return kept


def correlation_guard(candidates: list, returns_by_symbol: dict) -> list:
    """Section 15.1: when multiple candidates in the same run are highly
    correlated, let the single strongest-scoring one through and suppress
    the rest for that run."""
    if len(candidates) <= 1:
        return candidates
    candidates_sorted = sorted(candidates, key=lambda c: c.score.total, reverse=True)
    kept = []
    suppressed_symbols = set()
    for c in candidates_sorted:
        if c.symbol in suppressed_symbols:
            continue
        kept.append(c)
        r1 = returns_by_symbol.get(c.symbol)
        if not r1:
            continue
        for other in candidates_sorted:
            if other.symbol == c.symbol or other.symbol in suppressed_symbols:
                continue
            r2 = returns_by_symbol.get(other.symbol)
            if not r2:
                continue
            corr = pearson_corr(r1, r2)
            if corr is not None and corr > CORRELATION_SUPPRESS_THRESHOLD:
                suppressed_symbols.add(other.symbol)
    return kept


def pearson_corr(a: list, b: list) -> Optional[float]:
    n = min(len(a), len(b))
    if n < 5:
        return None
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return None
    return cov / math.sqrt(va * vb)


def returns_series(candles: list, n: int = CORRELATION_LOOKBACK_BARS) -> list:
    closes = [c["c"] for c in candles[-(n + 1):]]
    return [(closes[i] / closes[i - 1] - 1.0) for i in range(1, len(closes))] if len(closes) > 1 else []


# ============================================================================
# STATE MANAGEMENT (Section 11)
# ============================================================================

def default_state() -> dict:
    return {
        "meta": {"engine": ENGINE_NAME, "version": __version__, "last_run_utc": None, "last_run_id": None},
        "watchlist": {"core": CORE_WATCHLIST, "dynamic_extension": [], "last_refreshed_ms": 0,
                      "excluded_low_liquidity": []},
        "candle_cache": {},
        "adaptive": {
            "base_threshold": BASE_THRESHOLD, "current_threshold": BASE_THRESHOLD,
            "chaos_index_ema": 40.0, "rolling_win_rate_30": 0.5, "signals_today": 0,
            "daily_target_min": DAILY_TARGET_MIN, "date_utc": None, "last_20_outcomes": [],
        },
        "open_signals": [],
        "closed_signals_30d": [],
        "daily_summary": {"last_sent_date_utc": None},
        "symbol_cooldowns": {},
        "next_signal_id": 1,
        "run_lock": {"locked": False, "locked_at_ms": 0, "run_id": None},
    }


def load_state() -> dict:
    p = Path(STATE_FILE)
    if not p.exists():
        return default_state()
    try:
        with p.open("r") as f:
            state = json.load(f)
        # backfill any keys added by a newer version of the engine
        base = default_state()
        for k, v in base.items():
            state.setdefault(k, v)
        return state
    except (json.JSONDecodeError, OSError) as e:
        log(f"state load failed ({e}); starting from a fresh default state")
        return default_state()


def save_state(state: dict) -> None:
    p = Path(STATE_FILE)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w") as f:
        json.dump(state, f, indent=2, default=str)
    tmp.replace(p)


def acquire_run_lock(state: dict, reference_ms: int, run_id: str, stale_after_s: int = 600) -> bool:
    """Section 11 idempotency: guard against cron-job.org double-firing
    inside the same 15-minute window."""
    lock = state.setdefault("run_lock", {"locked": False, "locked_at_ms": 0, "run_id": None})
    if lock.get("locked") and (reference_ms - lock.get("locked_at_ms", 0)) < stale_after_s * 1000:
        return False
    lock["locked"] = True
    lock["locked_at_ms"] = reference_ms
    lock["run_id"] = run_id
    return True


def release_run_lock(state: dict) -> None:
    state["run_lock"] = {"locked": False, "locked_at_ms": 0, "run_id": None}


def append_run_log(record: dict) -> None:
    """Section 15.7 observability: append-only run log, separate from
    state.json, recording every run's regime read, chaos index, threshold
    used, candidates considered, and why each did/didn't fire."""
    try:
        with open(RUN_LOG_FILE, "a") as f:
            f.write(json.dumps(record, default=str) + "\n")
    except OSError as e:
        log(f"run log write failed: {e}")


# ============================================================================
# TELEGRAM (Sections 12, 13, 14)
# ============================================================================

AVAILABLE_REACTIONS = {"🔥", "🏆", "💔", "🤝", "👀", "👍"}


def send_telegram(text: str) -> Optional[int]:
    if DRY_RUN or SHADOW_MODE:
        print("----- (DRY RUN / SHADOW) TELEGRAM SEND -----")
        print(text)
        print("---------------------------------------------")
        return random.randint(1, 999_999) if DRY_RUN else None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        r.raise_for_status()
        return r.json().get("result", {}).get("message_id")
    except requests.RequestException as e:
        log(f"send_telegram failed: {e}")
        return None


def react_telegram(message_id: Optional[int], emoji: str) -> None:
    """Section 13: bots hold one reaction at a time (later outcomes
    overwrite earlier ones); wrap in try/except and fall back to a safe
    universal default (👍) if the specific emoji is rejected."""
    if DRY_RUN or SHADOW_MODE or not message_id:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"

    def _attempt(e):
        r = requests.post(url, json={
            "chat_id": TG_CHAT_ID, "message_id": message_id, "reaction": [{"type": "emoji", "emoji": e}],
        }, timeout=10)
        r.raise_for_status()

    try:
        _attempt(emoji)
    except requests.RequestException as e1:
        log(f"react_telegram({emoji}) failed: {e1}; falling back to 👍")
        try:
            _attempt("👍")
        except requests.RequestException as e2:
            log(f"react_telegram fallback also failed: {e2}")


def fmt_num(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}"


def format_signal_message(signal_id: int, c: Candidate) -> str:
    """Section 12 template, reproduced exactly (HTML parse_mode, <pre>
    block for copy-paste-friendly levels)."""
    direction_word = "LONG" if c.direction == "long" else "SHORT"
    dot = "🟢" if c.direction == "long" else "🔴"
    style_word = "Intraday" if c.style == "intraday" else "Swing"
    regime_word = c.regime.capitalize()
    lv = c.levels

    lines = [
        f"🦎 <b>{ENGINE_NAME}</b> — Signal #{signal_id}",
        "━━━━━━━━━━━━━━━",
        f"<b>{c.symbol}-PERP · {direction_word}</b> {dot}",
        f"Style: {style_word}  |  Regime: {regime_word}",
        "",
        "<pre>",
        f"ENTRY   {fmt_num(lv.entry_low)}–{fmt_num(lv.entry_high)}",
        f"SL      {fmt_num(lv.sl)}",
        f"TP1     {fmt_num(lv.tp1)}",
    ]
    if lv.tp2 is not None:
        lines.append(f"TP2     {fmt_num(lv.tp2)}")
    lines.append("</pre>")
    lines.append("")
    rr_line = f"R:R → TP1 {lv.rr1:.2f}R" + (f" | TP2 {lv.rr2:.2f}R" if lv.rr2 is not None else "")
    lines.append(rr_line)
    lines.append(f"Confluence: {c.score.total:.0f}/100")
    lines.append(f"Setup: {c.reason}")
    expiry_bars = SIGNAL_EXPIRY_BARS
    lines.append(f"Valid until: {expiry_bars} candles or invalidation")
    lines.append("")
    lines.append("⚠️ Not financial advice — manage your own risk.")
    return "\n".join(lines)


def format_daily_summary(state: dict, reference_ms: int) -> str:
    """Section 14 template, reproduced exactly."""
    date_str = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    closed_today = [s for s in state.get("closed_signals_30d", [])
                     if s.get("closed_utc_date") == date_str]
    wins = [s for s in closed_today if s["result"] in ("TP1_HIT", "TP2_HIT", "TP_HIT")]
    losses = [s for s in closed_today if s["result"] == "SL_HIT"]
    breakeven = [s for s in closed_today if s["result"] == "BREAKEVEN"]
    still_running = [s for s in state.get("open_signals", [])]
    n_issued = len(closed_today) + len(still_running)
    win_rate = (len(wins) / max(1, len(closed_today))) * 100.0

    def r_mult(s):
        return s.get("r_multiple", 0.0)

    best = max(closed_today, key=r_mult, default=None)
    worst = min(closed_today, key=r_mult, default=None)

    regimes = {"bullish": 0, "bearish": 0, "neutral": 0}
    styles = {"intraday": 0, "swing": 0}
    for s in closed_today + still_running:
        regimes[s.get("regime_at_entry", "neutral")] = regimes.get(s.get("regime_at_entry", "neutral"), 0) + 1
        styles[s.get("style", "intraday")] = styles.get(s.get("style", "intraday"), 0) + 1

    adaptive = state.get("adaptive", {})
    lines = [
        f"🦎 <b>{ENGINE_NAME}</b> — Daily Summary",
        f"{date_str}, 00:00–24:00 UTC",
        "━━━━━━━━━━━━━━━",
        f"Signals issued: {n_issued}",
        f"🏆 Wins (TP1+): {len(wins)} ({win_rate:.0f}%)",
        f"💔 Losses (SL): {len(losses)}",
        f"🤝 Breakeven: {len(breakeven)}",
        f"🔄 Still running: {len(still_running)}",
        "",
        f"Best trade: {best['symbol']} +{r_mult(best):.2f}R" if best else "Best trade: n/a",
        f"Worst trade: {worst['symbol']} {r_mult(worst):+.2f}R" if worst else "Worst trade: n/a",
        "",
        f"Regime mix: {regimes.get('bullish',0)} bullish · {regimes.get('bearish',0)} bearish · "
        f"{regimes.get('neutral',0)} neutral",
        f"Style mix: {styles.get('intraday',0)} intraday · {styles.get('swing',0)} swing",
        f"Current adaptive threshold: {adaptive.get('current_threshold', BASE_THRESHOLD):.0f}/100  "
        f"(chaos index: {adaptive.get('chaos_index_ema', 0.0):.0f}/100)",
    ]
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict, reference_ms: int) -> None:
    now = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc)
    today = now.strftime("%Y-%m-%d")
    ds = state.setdefault("daily_summary", {"last_sent_date_utc": None})
    if now.hour >= 8 and ds.get("last_sent_date_utc") != today:
        text = format_daily_summary(state, reference_ms)
        send_telegram(text)
        ds["last_sent_date_utc"] = today
        log("daily summary sent")


# ============================================================================
# SIGNAL TRACKING / RESOLUTION (Sections 9, 12, 13)
# ============================================================================

def track_new_signal(state: dict, c: Candidate, msg_id: Optional[int], reference_ms: int) -> dict:
    sig_id = state["next_signal_id"]
    state["next_signal_id"] += 1
    lv = c.levels
    sig = {
        "id": sig_id, "symbol": c.symbol, "direction": "LONG" if c.direction == "long" else "SHORT",
        "style": c.style, "entry_low": lv.entry_low, "entry_high": lv.entry_high,
        "sl": lv.sl, "sl_original": lv.sl, "tp1": lv.tp1, "tp2": lv.tp2,
        "status": "PENDING",  # PENDING until entry zone is filled, then OPEN
        "opened_utc": datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).isoformat(),
        "opened_ms": reference_ms, "bars_since_open": 0,
        "telegram_message_id": msg_id, "telegram_chat_id": TG_CHAT_ID,
        "confluence_score": c.score.total, "regime_at_entry": c.regime,
        "reaction_state": None, "tp1_hit": False,
    }
    state.setdefault("open_signals", []).append(sig)
    adaptive = state.setdefault("adaptive", {})
    adaptive["signals_today"] = adaptive.get("signals_today", 0) + 1
    cooldowns = state.setdefault("symbol_cooldowns", {})
    cooldowns[c.symbol] = datetime.fromtimestamp(
        reference_ms / 1000 + SYMBOL_COOLDOWN_S, tz=timezone.utc).isoformat()
    return sig


def _r_multiple(sig: dict, price: float) -> float:
    risk = abs(sig["entry_low" if sig["direction"] == "LONG" else "entry_high"] - sig["sl_original"])
    if risk <= 0:
        return 0.0
    entry_mid = (sig["entry_low"] + sig["entry_high"]) / 2.0
    if sig["direction"] == "LONG":
        return (price - entry_mid) / risk
    return (entry_mid - price) / risk


def update_open_signals(state: dict, candles_by_symbol_tf: dict, reference_ms: int) -> None:
    """Checks the high/low range of every candle since the last check (not
    just the latest close), per Section 9's monitoring rule, and reacts on
    the original message the moment an outcome resolves (Section 13)."""
    still_open = []
    for sig in state.get("open_signals", []):
        symbol, tf = sig["symbol"], ("5m" if sig["style"] == "intraday" else "15m")
        candles = candles_by_symbol_tf.get(symbol, {}).get(tf, [])
        new_candles = [c for c in candles if c["t"] > sig.get("last_checked_ms", sig["opened_ms"])]
        resolved = False

        for c in new_candles:
            sig["bars_since_open"] = sig.get("bars_since_open", 0) + 1
            if sig["status"] == "PENDING":
                filled = (sig["direction"] == "LONG" and c["l"] <= sig["entry_high"]) or \
                         (sig["direction"] == "SHORT" and c["h"] >= sig["entry_low"])
                if filled:
                    sig["status"] = "OPEN"
                elif sig["bars_since_open"] >= SIGNAL_EXPIRY_BARS:
                    sig["status"] = "EXPIRED"
                    _finalize_signal(state, sig, "EXPIRED", c["c"], reference_ms)
                    resolved = True
                    break
                continue

            if sig["direction"] == "LONG":
                hit_sl = c["l"] <= sig["sl"]
                hit_tp1 = (not sig["tp1_hit"]) and c["h"] >= sig["tp1"]
                hit_tp2 = sig["tp2"] is not None and c["h"] >= sig["tp2"]
            else:
                hit_sl = c["h"] >= sig["sl"]
                hit_tp1 = (not sig["tp1_hit"]) and c["l"] <= sig["tp1"]
                hit_tp2 = sig["tp2"] is not None and c["l"] <= sig["tp2"]

            if hit_sl and not sig["tp1_hit"]:
                _finalize_signal(state, sig, "SL_HIT", sig["sl"], reference_ms)
                resolved = True
                break
            if hit_sl and sig["tp1_hit"] and sig.get("breakeven_armed") and abs(sig["sl"] - _entry_mid(sig)) < 1e-9:
                _finalize_signal(state, sig, "BREAKEVEN", sig["sl"], reference_ms)
                resolved = True
                break
            if hit_tp2:
                _finalize_signal(state, sig, "TP2_HIT", sig["tp2"], reference_ms)
                resolved = True
                break
            if hit_tp1:
                sig["tp1_hit"] = True
                if sig["tp2"] is None:
                    _finalize_signal(state, sig, "TP_HIT", sig["tp1"], reference_ms)
                    resolved = True
                    break
                # move SL to breakeven and react 🔥 (single-TP setups skip straight to 🏆 above)
                sig["sl"] = _entry_mid(sig)
                sig["breakeven_armed"] = True
                react_telegram(sig["telegram_message_id"], "🔥")
                sig["reaction_state"] = "🔥"

        sig["last_checked_ms"] = reference_ms
        if not resolved:
            still_open.append(sig)
    state["open_signals"] = still_open


def _entry_mid(sig: dict) -> float:
    return (sig["entry_low"] + sig["entry_high"]) / 2.0


def _finalize_signal(state: dict, sig: dict, result: str, price: float, reference_ms: int) -> None:
    sig["status"] = "CLOSED"
    sig["result"] = result
    sig["close_price"] = price
    sig["closed_utc"] = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).isoformat()
    sig["closed_utc_date"] = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    sig["r_multiple"] = _r_multiple(sig, price)
    emoji_map = {"TP1_HIT": "🔥", "TP2_HIT": "🏆", "TP_HIT": "🏆", "SL_HIT": "💔",
                 "BREAKEVEN": "🤝", "EXPIRED": None}
    emoji = emoji_map.get(result)
    if emoji:
        react_telegram(sig["telegram_message_id"], emoji)
        sig["reaction_state"] = emoji
    closed = state.setdefault("closed_signals_30d", [])
    closed.append(sig)
    cutoff_ms = reference_ms - 30 * 24 * 3600 * 1000
    state["closed_signals_30d"] = [s for s in closed if s.get("opened_ms", 0) >= cutoff_ms]

    adaptive = state.setdefault("adaptive", {})
    outcomes = adaptive.setdefault("last_20_outcomes", [])
    tag = {"TP1_HIT": "W", "TP2_HIT": "W", "TP_HIT": "W", "SL_HIT": "L", "BREAKEVEN": "BE", "EXPIRED": None}.get(result)
    if tag:
        outcomes.append(tag)
        adaptive["last_20_outcomes"] = outcomes[-20:]
        wins = sum(1 for o in adaptive["last_20_outcomes"] if o == "W")
        decided = sum(1 for o in adaptive["last_20_outcomes"] if o in ("W", "L"))
        if decided:
            adaptive["rolling_win_rate_30"] = wins / decided


# ============================================================================
# MAIN SCAN ORCHESTRATION
# ============================================================================

def build_symbol_candles(symbol: str, reference_ms: int, state: dict) -> Optional[dict]:
    cache = state.setdefault("candle_cache", {})
    sym_cache_meta = cache.get(symbol, {})
    need_macro = current_bar_open_ms(reference_ms, TF_MACRO) != sym_cache_meta.get("4h_last_open_bar")
    need_swing = current_bar_open_ms(reference_ms, TF_SWING) != sym_cache_meta.get("1h_last_open_bar")
    cached_candles = sym_cache_meta.get("_candles", {})
    bundle = fetch_symbol_candles(symbol, reference_ms, need_macro, need_swing, cached_candles)
    if bundle is None:
        return None
    cache[symbol] = {
        "4h_last_open_bar": current_bar_open_ms(reference_ms, TF_MACRO),
        "1h_last_open_bar": current_bar_open_ms(reference_ms, TF_SWING),
        # NOTE: full candle arrays are kept in-memory for this run only and
        # NOT persisted back into state.json (would bloat the file); only
        # the "last closed bar" markers are persisted so the next run knows
        # whether a refetch is needed.
    }
    return bundle


def scan_symbol(symbol: str, reference_ms: int, state: dict, asset_ctxs: Optional[dict]) -> tuple:
    """Returns (candidates: list[Candidate], candle_bundle: dict|None,
    run_log_entry: dict) for one symbol."""
    bundle = build_symbol_candles(symbol, reference_ms, state)
    entry_log = {"symbol": symbol, "fetched": bundle is not None}
    if bundle is None:
        entry_log["reason"] = "candle fetch failed"
        return [], None, entry_log

    ind4h = compute_indicators(bundle[TF_MACRO])
    ind1h = compute_indicators(bundle[TF_SWING])
    ind15m = compute_indicators(bundle[TF_INTRADAY])
    ind5m = compute_indicators(bundle[TF_ENTRY])

    regime = classify_regime(ind4h)
    chaos = compute_chaos_index(ind4h)
    entry_log["regime"] = regime.direction
    entry_log["chaos_index"] = round(chaos.chaos_index, 1)

    swing_candidates = detect_setups_for_timeframe(
        symbol, "swing", regime, ind1h, ind15m, bundle[TF_INTRADAY], chaos.chaos_index, asset_ctxs)
    intraday_candidates = detect_setups_for_timeframe(
        symbol, "intraday", regime, ind15m, ind5m, bundle[TF_ENTRY], chaos.chaos_index, asset_ctxs)

    all_candidates = swing_candidates + intraday_candidates
    entry_log["candidates_considered"] = [
        {"style": c.style, "direction": c.direction, "pattern": c.pattern, "score": round(c.score.total, 1)}
        for c in all_candidates
    ]
    return all_candidates, bundle, entry_log


def run_once() -> dict:
    reference_ms = int(time.time() * 1000)
    run_id = f"run_{datetime.fromtimestamp(reference_ms/1000, tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    state = load_state()

    if not acquire_run_lock(state, reference_ms, run_id):
        log("run skipped: another run is already in progress (idempotency lock held)")
        return {"status": "skipped_locked"}

    try:
        roll_daily_counters_if_needed(state, reference_ms)
        watchlist = resolve_dynamic_watchlist(state, reference_ms)
        asset_ctxs = get_meta_and_asset_ctxs()

        # cooldown filter (Section 12)
        cooldowns = state.get("symbol_cooldowns", {})
        now_iso = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc)
        active_watchlist = [
            s for s in watchlist
            if s not in cooldowns or datetime.fromisoformat(cooldowns[s]) <= now_iso
        ]

        all_candidates = []
        candle_bundles = {}
        run_log_entries = []

        with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
            futures = {pool.submit(scan_symbol, s, reference_ms, state, asset_ctxs): s for s in active_watchlist}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    cands, bundle, log_entry = fut.result()
                except Exception as e:  # noqa: BLE001 -- Section 15.2 API resilience
                    log(f"scan_symbol({sym}) raised: {e}")
                    cands, bundle, log_entry = [], None, {"symbol": sym, "error": str(e)}
                all_candidates.extend(cands)
                if bundle is not None:
                    candle_bundles[sym] = bundle
                run_log_entries.append(log_entry)

        # global chaos index for the adaptive threshold: BTC's 4H read as
        # market proxy (state.json schema has one global chaos_index_ema
        # driving one global current_threshold -- Section 7/11).
        btc_bundle = candle_bundles.get("BTC")
        if btc_bundle:
            btc_chaos = compute_chaos_index(compute_indicators(btc_bundle[TF_MACRO])).chaos_index
        else:
            btc_chaos = state.get("adaptive", {}).get("chaos_index_ema", 40.0)
        current_threshold = compute_adaptive_threshold(state, btc_chaos, reference_ms)

        qualifying = apply_required_threshold(all_candidates, current_threshold)
        qualifying = soft_dedup(qualifying)
        returns_by_symbol = {
            sym: returns_series(b[TF_INTRADAY]) for sym, b in candle_bundles.items()
        }
        qualifying = correlation_guard(qualifying, returns_by_symbol)

        emitted = []
        if not SHADOW_MODE:
            for c in qualifying:
                text = format_signal_message(state["next_signal_id"], c)
                msg_id = send_telegram(text)
                sig = track_new_signal(state, c, msg_id, reference_ms)
                emitted.append(sig)
                log(f"SIGNAL emitted: {c.symbol} {c.direction} {c.style} score={c.score.total:.0f} "
                    f"threshold={current_threshold:.0f}")
        else:
            log(f"SHADOW MODE: {len(qualifying)} candidates would have fired "
                f"(threshold={current_threshold:.0f}); {len(all_candidates)} total considered")

        update_open_signals(state, candle_bundles, reference_ms)
        maybe_send_daily_summary(state, reference_ms)

        state["meta"]["last_run_utc"] = now_iso.isoformat()
        state["meta"]["last_run_id"] = run_id

        append_run_log({
            "run_id": run_id, "utc": now_iso.isoformat(), "watchlist_size": len(active_watchlist),
            "global_chaos_index": round(btc_chaos, 1), "adaptive_threshold": round(current_threshold, 1),
            "total_candidates": len(all_candidates), "qualifying": len(qualifying),
            "emitted": len(emitted), "symbols": run_log_entries,
        })

        summary = {
            "status": "ok", "run_id": run_id, "watchlist_size": len(active_watchlist),
            "candidates_considered": len(all_candidates), "signals_emitted": len(emitted),
            "adaptive_threshold": round(current_threshold, 1), "global_chaos_index": round(btc_chaos, 1),
        }
        log(f"run complete: {summary}")
        return summary
    finally:
        release_run_lock(state)
        save_state(state)


# ============================================================================
# ENTRYPOINTS: one-shot CLI run, or HTTP server for cron-job.org
# ============================================================================

class _ScanHTTPHandler(BaseHTTPRequestHandler):
    def _handle(self):
        try:
            result = run_once()
            body = json.dumps(result).encode()
            self.send_response(200)
        except Exception as e:  # noqa: BLE001
            log(f"HTTP-triggered run failed: {e}")
            body = json.dumps({"status": "error", "error": str(e)}).encode()
            self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def log_message(self, fmt, *args):  # quiet the default stderr access log
        log("HTTP " + (fmt % args))


def serve(port: int) -> None:
    """Section 11 operating model: a single HTTP endpoint that cron-job.org
    (or any scheduler) hits every 15 minutes. Any path triggers one scan."""
    server = ThreadingHTTPServer(("0.0.0.0", port), _ScanHTTPHandler)
    log(f"{ENGINE_NAME} listening on :{port} -- point cron-job.org at this endpoint every 15 minutes")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description=f"{ENGINE_NAME} v{__version__}")
    parser.add_argument("--serve", action="store_true", help="run as an HTTP server for cron-job.org")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8080")))
    args = parser.parse_args()

    if args.serve:
        serve(args.port)
    else:
        run_once()


if __name__ == "__main__":
    main()
