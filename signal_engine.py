"""
Scalp Swing Bot v10 – Python Signal Engine  (v3 — Full Upgrade)
Hyperliquid Perpetuals edition
Timeframe map: 4H bias / 1H middle / 15m execution
Runs on GitHub Actions every 15 min, sends Telegram alerts.
No orders are placed — signal only.

v2 FIXES (aligned to Pine Script v10 logic)
────────────────────────────────────────────
[FIX 1] h4_bull / h4_bear was incorrectly aliased to h1_bull / h1_bear.
        Now correctly computed from the 4H EMA arrays (ef4h > es4h).

[FIX 2] All HTF EMA/ADX values now use [-2] (last confirmed closed bar)
        instead of [-1], matching Pine's request.security lookahead_off
        which returns close[1] — the previous fully closed bar.
        Affects: 1H EMA (ef1h/es1h), 4H EMA (ef4h/es4h), 4H ADX, Daily 200 EMA.

[FIX 3] trend_held() offset corrected to -(offset+1) to align with the
        [-2]=bar[1] convention, matching Pine's nz(ef[1])>nz(es[1]) checks.

[FIX 4] pull_touched_long/short now use ef15_prev (previous bar's fast EMA)
        instead of ef15, matching Pine's localEMAf[1] reference.

[FIX 5] obv_slope indexing corrected to -(1+OBV_LEN) for clarity and
        consistency with Pine's obvVal > obvVal[obvLen].

v3 UPGRADES
────────────────────────────────────────────
[P1] Real OI Trend — replaces 4H volume proxy with actual Hyperliquid OI values.
     OI history stored in state.json per symbol. Detects confirmation / divergence.

[P2] Historical Performance Analytics — tracks every resolved signal, computes
     per-symbol / per-type / per-score / per-direction win rates. Adjusts score.

[P3] BTC Market Regime Filter — soft score-based penalty (score -= 2) when BTC
     EMAs disagree with signal direction. No hard suppression except at threshold.

[P4] Economic Calendar Filter — fetches FOMC/CPI/NFP etc. from open-source feed,
     caches locally, applies score -= 1 inside ±60/30min risk windows. Hard suppress
     only when ATR is also elevated (genuine tail-risk).

[P5] Relative Strength Ranking — coin 7d return vs BTC 7d return. +1 / -1 score.

[P6] Breakout Volume Upgrade — BREAK signals now require cur_v >= vm15 * 1.5.

[P7] Market Breadth Filter — % of watchlist above 4H EMA50 reusing already-fetched
     EMA data. Soft score -= 1 at extremes.

FINAL SCORING STACK (order of application):
  1. OI Confirmation
  2. BTC Regime + Market Breadth  (paired market-context adjustments)
  3. Relative Strength
  4. Historical Win Rate
  5. Macro Filter
  6. Volume Confirmation
  → Suppression threshold checked only after all 6 steps above.

REACTION FEATURE
────────────────────────────────────────────
On every run, active signals are checked against 15m candles fetched
from the signal's bar time up to now. Each candle's high/low is walked
oldest→newest to correctly resolve TP1→TP2 vs SL hit order, avoiding
false 😭 reactions when TP1 is hit first within the same 15m window.

  🔥  when TP1 is hit before SL
  🏆  when TP2 is also hit (full winner)
  😭 when SL is hit before TP1
Signal tracking state is persisted in state.json under "active_signals".
Signals are auto-expired after SIGNAL_MAX_AGE_BARS (≈12 h by default).
"""

import os, json, time, math, random, threading, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── CONFIG ───────────────────────────────────────────────────
TG_BOT_TOKEN          = os.environ["TG_BOT_TOKEN"]
TG_CHAT_ID            = os.environ["TG_CHAT_ID"]
STATE_FILE            = "state.json"
SCAN_WORKERS          = int(os.getenv("SCAN_WORKERS", "2"))
HL_MIN_INTERVAL_S     = float(os.getenv("HL_MIN_INTERVAL_S", "0.18"))
HL_MIN_INTERVAL_MAX_S = float(os.getenv("HL_MIN_INTERVAL_MAX_S", "0.60"))
HL_TF_WORKERS         = int(os.getenv("HL_TF_WORKERS", "1"))

_hl_request_lock    = threading.Lock()
_hl_last_request_ts = 0.0
_hl_min_interval_s  = HL_MIN_INTERVAL_S

_hl_session = requests.Session()

# ── HARDCODED WATCHLIST ───────────────────────────────────────
# Format: standard USDT pairs — hl_coin() strips the suffix automatically
WATCHLIST = [
    "BTCUSDT",
    "ETHUSDT",
    "HYPEUSDT",
    "ZECUSDT",
    "NEARUSDT",
    "ONDOUSDT",
    "SUIUSDT",
    "PENGUUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "TRXUSDT",
    "TONUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "DOTUSDT",
    "TAOUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "AAVEUSDT",
    "XRPUSDT",
    "XLMUSDT",
    "UNIUSDT",
    "LTCUSDT",
    "APTUSDT",
    "PENDLEUSDT",
]

# ── INDICATOR LENGTHS ────────────────────────────────────────
FAST_LEN  = 21
SLOW_LEN  = 50
TREND_LEN = 200
RSI_LEN   = 14
ATR_LEN   = 14
ADX_LEN   = 14
BB_LEN    = 20
BB_MULT   = 2.0
VOL_LEN   = 20
OBV_LEN   = 3

# ── RISK / SCORE ─────────────────────────────────────────────
MIN_SCORE       = 4
TP1_MULT        = 1.0
TP2_MULT        = 1.5
SL_MULT         = 1.0
COOLDOWN_BARS   = 32
GLOBAL_COOLDOWN = 16
MAX_SIGNALS_PER_SCAN = 3   # top N signals to fire per scan; rest are dropped

# ── FILTERS ──────────────────────────────────────────────────
ADX_BREAK_GATE  = 20.0
ADX_SCORE_MIN   = 20.0
RSI_LONG_MIN    = 45.0;  RSI_LONG_MAX  = 65.0
RSI_SHORT_MIN   = 35.0;  RSI_SHORT_MAX = 55.0
VOL_SCORE_MULT  = 1.0
MAX_ATR_PCT     = 10.0
MIN_ATR_PCT     = 0.2
WICK_FILTER     = 0.45
RANGE_PCT_BREAK = 0.30
PULL_ZONE_MULT  = 0.25
TREND_HOLD_BARS = 2
USE_ROLLING_VWAP  = True
ROLLING_VWAP_LEN  = 16
USE_D200_FILTER   = True
USE_DAILY_ADX     = True
MIN_DAILY_ADX     = 20.0
PULL_REQUIRES_4H  = True

# ── ACCURACY FILTERS (v3) ─────────────────────────────────────
# Suppress signal when funding headwind exceeds this per-8h rate.
FUNDING_SUPPRESS_EXTREME: float = 0.0010

# Minimum ATR-multiples of clearance required between entry and the
# nearest S/R level in the trade direction.
SR_CLEARANCE_ATR_MULT: float = 0.3

# ── OI TREND (v3) ─────────────────────────────────────────────
# Number of historical OI snapshots to store per symbol.
OI_HISTORY_DEPTH: int = 6
# Minimum OI change % to count as "expanding" or "contracting".
OI_CHANGE_THRESHOLD_PCT: float = 1.0

# ── BREAKOUT VOLUME UPGRADE (P6) ──────────────────────────────
BREAK_VOL_MULT: float = 1.5   # BREAK signals require cur_v >= vm15 * 1.5

# ── RELATIVE STRENGTH (P5) ────────────────────────────────────
RS_TOP_PERCENTILE: float    = 0.20   # top 20% → +1
RS_BOTTOM_PERCENTILE: float = 0.20   # bottom 20% → -1

# ── MARKET BREADTH (P7) ───────────────────────────────────────
BREADTH_WEAK_LONG_THRESHOLD:   float = 0.15   # < 15% above EMA50 → -1 for longs
BREADTH_WEAK_SHORT_THRESHOLD:  float = 0.85   # > 85% above EMA50 → -1 for shorts

# ── HISTORICAL WIN RATE (P2) ──────────────────────────────────
WIN_RATE_MIN_SAMPLE: int   = 20
WIN_RATE_HIGH_THRESH: float = 0.65   # > 65% → +1
WIN_RATE_LOW_THRESH: float  = 0.45   # < 45% → -1

# ── ECONOMIC CALENDAR (P4) ────────────────────────────────────
MACRO_WINDOW_BEFORE_MINS: int = 60
MACRO_WINDOW_AFTER_MINS:  int = 30
# ATR threshold above which macro risk window becomes a hard suppression.
# Expressed as percentage (e.g. 3.0 = 3% ATR/price).
MACRO_HIGH_ATR_SUPPRESS_PCT: float = 3.0
# Calendar cache TTL in seconds (1 hour).
MACRO_CACHE_TTL_S: int = 3600

# ── CANDLE COUNTS PER TIMEFRAME ──────────────────────────────
N_15M = 300
N_1H  = 60
N_4H  = 60
N_1D  = 210

# ── REACTION SETTINGS ────────────────────────────────────────
REACT_TP1          = "🔥"
REACT_TP2          = "🏆"
REACT_SL           = "😭"
SIGNAL_MAX_AGE_BARS = 48

# ── HYPERLIQUID ENDPOINT ──────────────────────────────────────
HL_INFO_URL = "https://api.hyperliquid.xyz/info"

INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}


# ═══════════════════════════════════════════════════════════════
# HYPERLIQUID DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def hl_coin(symbol: str) -> str:
    """Convert BTCUSDT → BTC (Hyperliquid perp coin name)."""
    return symbol.replace("USDT", "")


def hl_post(payload: dict) -> any:
    """POST to Hyperliquid /info with retry + 429 backoff."""
    max_attempts  = int(os.getenv("HL_MAX_ATTEMPTS", "6"))
    base_sleep_s  = float(os.getenv("HL_BASE_SLEEP_S", "0.75"))
    for attempt in range(max_attempts):
        try:
            global _hl_last_request_ts
            global _hl_min_interval_s
            with _hl_request_lock:
                now     = time.time()
                elapsed = now - _hl_last_request_ts
                wait_s  = _hl_min_interval_s - elapsed
                if wait_s > 0:
                    time.sleep(wait_s)
                _hl_last_request_ts = time.time()

            r = _hl_session.post(
                HL_INFO_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            if r.status_code == 429:
                with _hl_request_lock:
                    _hl_min_interval_s = min(
                        HL_MIN_INTERVAL_MAX_S, _hl_min_interval_s * 1.25 + 0.02
                    )
                retry_after = r.headers.get("Retry-After")
                try:
                    retry_after_s = float(retry_after) if retry_after is not None else None
                except ValueError:
                    retry_after_s = None
                sleep_s = retry_after_s if retry_after_s is not None else (base_sleep_s * (2 ** attempt))
                sleep_s = min(20.0, max(base_sleep_s, sleep_s)) + random.uniform(0.0, 0.35)
                time.sleep(sleep_s)
                continue
            r.raise_for_status()

            with _hl_request_lock:
                _hl_min_interval_s = max(HL_MIN_INTERVAL_S, _hl_min_interval_s - 0.0025)
            return r.json()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            sleep_s = min(20.0, base_sleep_s * (2 ** attempt)) + random.uniform(0.0, 0.25)
            time.sleep(sleep_s)


def get_candles(symbol: str, interval: str, n: int, start_time_ms: int | None = None) -> list[dict]:
    """
    Fetch last n closed candles for symbol on interval via Hyperliquid.
    Returns list of dicts: {t, o, h, l, c, v}  newest last.
    """
    coin   = hl_coin(symbol)
    iv_ms  = INTERVAL_MS.get(interval, 60 * 60 * 1000)
    end_ms = int(time.time() * 1000)

    if start_time_ms is not None:
        computed_start_ms = start_time_ms
    else:
        computed_start_ms = end_ms - iv_ms * (n + 10)

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin":      coin,
            "interval":  interval,
            "startTime": computed_start_ms,
            "endTime":   end_ms,
        },
    }

    raw = hl_post(payload)

    candles = []
    for c in raw:
        candles.append({
            "t":  int(c["t"]),
            "o":  float(c["o"]),
            "h":  float(c["h"]),
            "l":  float(c["l"]),
            "c":  float(c["c"]),
            "v":  float(c["v"]),
            "qv": float(c["v"]),
        })

    if candles and (candles[-1]["t"] + iv_ms) > end_ms:
        candles = candles[:-1]

    return candles[-n:]


def fetch_all_candles(symbol: str) -> tuple[list, list, list, list] | None:
    """
    Fetch all 4 timeframes concurrently for one symbol.
    Returns (candles_15m, candles_1h, candles_4h, candles_d)
    or None if 15m data is insufficient.
    """
    candles_15m = get_candles(symbol, "15m", N_15M)
    if len(candles_15m) < 50:
        return None

    results = {}
    with ThreadPoolExecutor(max_workers=max(1, HL_TF_WORKERS)) as ex:
        futures = {
            ex.submit(get_candles, symbol, "1h", N_1H): "1h",
            ex.submit(get_candles, symbol, "4h", N_4H): "4h",
            ex.submit(get_candles, symbol, "1d", N_1D): "1d",
        }
        for fut in as_completed(futures):
            tf = futures[fut]
            results[tf] = fut.result()

    return candles_15m, results["1h"], results["4h"], results["1d"]


# ═══════════════════════════════════════════════════════════════
# FUNDING RATE + OPEN INTEREST (with real OI trend — P1)
# ═══════════════════════════════════════════════════════════════

FUNDING_WARN_EXTREME   = 0.0010
FUNDING_WARN_HIGH      = 0.0005

# metaAndAssetCtxs cache — fetched once per run, shared across all symbols
_meta_cache: dict | None = None
_meta_cache_lock = threading.Lock()


def get_meta_and_asset_ctxs() -> dict | None:
    """
    Fetch metaAndAssetCtxs ONCE per run and cache in memory.
    Returns {coin: {funding, open_interest_coins, mark_px}} for all coins.
    """
    global _meta_cache
    with _meta_cache_lock:
        if _meta_cache is not None:
            return _meta_cache
    try:
        payload = {"type": "metaAndAssetCtxs"}
        data = hl_post(payload)
        meta       = data[0]
        asset_ctxs = data[1]
        universe   = meta.get("universe", [])
        cache = {}
        for i, asset in enumerate(universe):
            name = asset.get("name", "")
            if not name:
                continue
            ctx = asset_ctxs[i]
            funding = float(ctx["funding"])      if ctx.get("funding")      is not None else None
            oi_coins = float(ctx["openInterest"]) if ctx.get("openInterest") is not None else None
            mark     = float(ctx["markPx"])       if ctx.get("markPx")       is not None else None
            cache[name] = {
                "funding":          funding,
                "open_interest_coins": oi_coins,
                "mark_px":          mark,
            }
        with _meta_cache_lock:
            _meta_cache = cache
        return cache
    except Exception as e:
        print(f"  [META CACHE] fetch failed: {e}")
    return None


def get_market_context(symbol: str) -> dict | None:
    """
    Returns funding + OI (USD notional) for a symbol using the cached meta call.
    """
    coin  = hl_coin(symbol)
    cache = get_meta_and_asset_ctxs()
    if cache is None or coin not in cache:
        return None
    entry    = cache[coin]
    funding  = entry.get("funding")
    oi_coins = entry.get("open_interest_coins")
    mark     = entry.get("mark_px")
    oi_usd   = oi_coins * mark if (oi_coins is not None and mark is not None) else None
    return {"funding": funding, "open_interest": oi_usd, "open_interest_coins": oi_coins}


# ── P1: Real OI Trend ─────────────────────────────────────────

def update_oi_history(state: dict, symbol: str, oi_coins: float | None) -> None:
    """
    Append current OI (in coin units) to per-symbol rolling history in state.json.
    Keeps the last OI_HISTORY_DEPTH snapshots.
    Skips append if oi_coins is None.
    """
    if oi_coins is None:
        return
    oi_hist = state.setdefault("oi_history", {})
    symbol_hist = oi_hist.setdefault(symbol, [])
    symbol_hist.append({"ts": int(time.time()), "oi": oi_coins})
    # Keep only the most recent OI_HISTORY_DEPTH entries
    oi_hist[symbol] = symbol_hist[-OI_HISTORY_DEPTH:]


def compute_oi_trend(state: dict, symbol: str, current_price: float, price_direction: str) -> dict:
    """
    Compute OI trend metrics from stored OI history.

    price_direction: "up" or "down" (based on current 15m bar).

    Returns:
      {
        "oi_trend":        "rising" | "falling" | "flat" | "unknown",
        "oi_change_pct":   float | None,
        "oi_acceleration": float | None,   # change of change
        "score_adj":       int,            # +1, 0, or -1
        "label":           str,            # for Telegram
        "breakdown_tag":   str,            # short tag for breakdown line
      }
    """
    oi_hist = state.get("oi_history", {}).get(symbol, [])
    if len(oi_hist) < 2:
        return {
            "oi_trend": "unknown", "oi_change_pct": None,
            "oi_acceleration": None, "score_adj": 0,
            "label": "OI Trend: Unknown", "breakdown_tag": "OI?"
        }

    recent = oi_hist[-1]["oi"]
    prior  = oi_hist[-2]["oi"]
    if prior == 0:
        return {
            "oi_trend": "unknown", "oi_change_pct": None,
            "oi_acceleration": None, "score_adj": 0,
            "label": "OI Trend: Unknown", "breakdown_tag": "OI?"
        }

    oi_change_pct = (recent - prior) / prior * 100.0

    # OI acceleration: compare recent change to the change before it
    oi_acceleration = None
    if len(oi_hist) >= 3:
        prior2 = oi_hist[-3]["oi"]
        if prior2 != 0:
            prev_change = (prior - prior2) / prior2 * 100.0
            oi_acceleration = oi_change_pct - prev_change

    oi_rising  = oi_change_pct >  OI_CHANGE_THRESHOLD_PCT
    oi_falling = oi_change_pct < -OI_CHANGE_THRESHOLD_PCT

    if oi_rising:
        oi_trend = "rising"
    elif oi_falling:
        oi_trend = "falling"
    else:
        oi_trend = "flat"

    # Scoring logic
    # Confirmation:  price up + OI up  OR  price down + OI up
    # Divergence:    price up + OI down OR  price down + OI down
    if price_direction == "up" and oi_rising:
        score_adj     = 1
        condition     = "Bullish Confirmation"
        breakdown_tag = "OI↑"
    elif price_direction == "down" and oi_rising:
        score_adj     = 1
        condition     = "Bearish Confirmation"
        breakdown_tag = "OI↑"
    elif price_direction == "up" and oi_falling:
        score_adj     = -1
        condition     = "Weak Breakout (OI Divergence)"
        breakdown_tag = "OI Divergence"
    elif price_direction == "down" and oi_falling:
        score_adj     = -1
        condition     = "Weak Breakdown (OI Divergence)"
        breakdown_tag = "OI Divergence"
    else:
        score_adj     = 0
        condition     = "Neutral"
        breakdown_tag = "OI→"

    label = (
        f"OI Trend: {oi_trend.capitalize()}  "
        f"OI Change: {oi_change_pct:+.1f}%"
    )

    return {
        "oi_trend":        oi_trend,
        "oi_change_pct":   oi_change_pct,
        "oi_acceleration": oi_acceleration,
        "score_adj":       score_adj,
        "label":           label,
        "breakdown_tag":   breakdown_tag,
        "condition":       condition,
    }


def format_funding(rate: float | None, direction: str) -> str:
    if rate is None:
        return "Funding: n/a"

    pct    = rate * 100
    per_8h = f"{pct:+.4f}%/8h"

    headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
    tailwind = (rate < 0 and direction == "long") or (rate > 0 and direction == "short")

    abs_rate = abs(rate)
    if headwind and abs_rate >= FUNDING_WARN_EXTREME:
        tag = "⚠️ EXTREME — longs paying heavily" if direction == "long" else "⚠️ EXTREME — shorts paying heavily"
    elif headwind and abs_rate >= FUNDING_WARN_HIGH:
        tag = "⚡ elevated against trade"
    elif tailwind and abs_rate >= FUNDING_WARN_HIGH:
        tag = "✅ in your favour"
    else:
        tag = "✅ neutral"

    return f"Funding: {per_8h}  {tag}"


def format_oi(oi_usd: float | None) -> str:
    if oi_usd is None:
        return "OI: n/a"
    if oi_usd >= 1_000_000_000:
        return f"OI: ${oi_usd / 1_000_000_000:.2f}B"
    if oi_usd >= 1_000_000:
        return f"OI: ${oi_usd / 1_000_000:.1f}M"
    return f"OI: ${oi_usd:,.0f}"


def find_sr_levels(candles_15m: list[dict], n_levels: int = 2) -> tuple[list[float], list[float]]:
    cur = candles_15m[-1]["c"]
    pivots_high = []
    pivots_low  = []
    window = candles_15m[-101:-1]
    for i in range(1, len(window) - 1):
        if window[i]["h"] > window[i-1]["h"] and window[i]["h"] > window[i+1]["h"]:
            pivots_high.append(window[i]["h"])
        if window[i]["l"] < window[i-1]["l"] and window[i]["l"] < window[i+1]["l"]:
            pivots_low.append(window[i]["l"])
    resistances = sorted([p for p in pivots_high if p > cur], key=lambda x: x - cur)[:n_levels]
    supports    = sorted([p for p in pivots_low  if p < cur], key=lambda x: cur - x)[:n_levels]
    return supports, resistances


# ═══════════════════════════════════════════════════════════════
# P3: BTC MARKET REGIME FILTER
# ═══════════════════════════════════════════════════════════════

# Cache BTC EMAs computed during main scan so btc-specific computation
# doesn't need an extra API call; populated once per run.
_btc_regime_cache: dict | None = None
_btc_regime_lock = threading.Lock()


def compute_btc_regime(candles_1h: list[dict], candles_4h: list[dict]) -> dict:
    """
    Compute BTC EMA21/50 on 4H and 1H.
    Returns {"bullish": bool, "bearish": bool, "label": str}
    """
    def _arr(candles):
        return [c["c"] for c in candles]

    c4h = _arr(candles_4h)
    c1h = _arr(candles_1h)

    ef4h_arr = ema(c4h, FAST_LEN)
    es4h_arr = ema(c4h, SLOW_LEN)
    ef1h_arr = ema(c1h, FAST_LEN)
    es1h_arr = ema(c1h, SLOW_LEN)

    ef4h = safe(ef4h_arr[-2])
    es4h = safe(es4h_arr[-2])
    ef1h = safe(ef1h_arr[-2])
    es1h = safe(es1h_arr[-2])

    btc_bullish = (ef4h > es4h) and (ef1h > es1h)
    btc_bearish = (ef4h < es4h) and (ef1h < es1h)

    if btc_bullish:
        label = "BTC Regime: Bullish"
    elif btc_bearish:
        label = "BTC Regime: Bearish"
    else:
        label = "BTC Regime: Mixed"

    return {"bullish": btc_bullish, "bearish": btc_bearish, "label": label}


def set_btc_regime(regime: dict):
    global _btc_regime_cache
    with _btc_regime_lock:
        _btc_regime_cache = regime


def get_btc_regime() -> dict | None:
    with _btc_regime_lock:
        return _btc_regime_cache


def apply_btc_regime_adjustment(direction: str, symbol: str) -> tuple[int, str]:
    """
    Returns (score_adj, label_suffix).
    BTCUSDT bypasses this filter entirely.
    """
    if hl_coin(symbol) == "BTC":
        return 0, "BTC Regime: N/A (BTC itself)"

    regime = get_btc_regime()
    if regime is None:
        return 0, "BTC Regime: Unknown"

    adj   = 0
    label = regime["label"]

    if direction == "long" and regime["bearish"]:
        adj    = -2
        label += " (-2)"
    elif direction == "short" and regime["bullish"]:
        adj    = -2
        label += " (-2)"

    return adj, label


# ═══════════════════════════════════════════════════════════════
# P7: MARKET BREADTH FILTER
# ═══════════════════════════════════════════════════════════════

# Populated during the BTC/watchlist pre-scan; keyed by symbol
_breadth_ema50_above: dict[str, bool] = {}
_breadth_lock = threading.Lock()


def record_breadth_result(symbol: str, price_above_ema50: bool):
    with _breadth_lock:
        _breadth_ema50_above[symbol] = price_above_ema50


def compute_market_breadth() -> dict:
    """
    Compute breadth from the results already cached during the main scan.
    Returns {breadth_50_pct, label}
    """
    with _breadth_lock:
        results = dict(_breadth_ema50_above)

    if not results:
        return {"breadth_50_pct": 0.5, "label": "Market Breadth: Unknown"}

    above = sum(1 for v in results.values() if v)
    pct   = above / len(results)

    if pct < 0.15:
        label = f"Market Breadth: {pct*100:.0f}% > EMA50 (Weak)"
    elif pct > 0.85:
        label = f"Market Breadth: {pct*100:.0f}% > EMA50 (Overbought)"
    else:
        label = f"Market Breadth: {pct*100:.0f}% > EMA50 (Healthy)"

    return {"breadth_50_pct": pct, "label": label}


def apply_breadth_adjustment(direction: str) -> tuple[int, str]:
    """Returns (score_adj, label)."""
    breadth = compute_market_breadth()
    pct     = breadth["breadth_50_pct"]
    label   = breadth["label"]
    adj     = 0

    if direction == "long" and pct < BREADTH_WEAK_LONG_THRESHOLD:
        adj    = -1
        label += " (-1)"
    elif direction == "short" and pct > BREADTH_WEAK_SHORT_THRESHOLD:
        adj    = -1
        label += " (-1)"

    return adj, label


# ═══════════════════════════════════════════════════════════════
# P5: RELATIVE STRENGTH RANKING
# ═══════════════════════════════════════════════════════════════

# Populated during main scan: {symbol: 7d_return_pct}
_rs_scores: dict[str, float] = {}
_rs_lock = threading.Lock()


def record_rs_return(symbol: str, return_7d_pct: float):
    with _rs_lock:
        _rs_scores[symbol] = return_7d_pct


def compute_relative_strength(symbol: str) -> dict:
    """
    Compute RS = coin_7d_return - btc_7d_return.
    Rank within all recorded symbols and determine percentile.
    Returns {rs_pct, percentile, score_adj, label}
    """
    with _rs_lock:
        scores = dict(_rs_scores)

    btc_return = scores.get("BTCUSDT", None)
    coin_return = scores.get(symbol, None)

    if btc_return is None or coin_return is None:
        return {"rs_pct": None, "percentile": None, "score_adj": 0, "label": "Relative Strength: N/A"}

    rs = coin_return - btc_return

    # Compute all RS values to determine percentile
    all_rs = sorted([v - btc_return for v in scores.values()])
    n = len(all_rs)
    try:
        rank = next(i for i, v in enumerate(all_rs) if v >= rs)
        percentile = rank / max(n - 1, 1)
    except StopIteration:
        percentile = 1.0

    if percentile >= (1.0 - RS_TOP_PERCENTILE):
        score_adj = 1
    elif percentile <= RS_BOTTOM_PERCENTILE:
        score_adj = -1
    else:
        score_adj = 0

    label = f"Relative Strength: {rs:+.1f}%"
    return {"rs_pct": rs, "percentile": percentile, "score_adj": score_adj, "label": label}


# ═══════════════════════════════════════════════════════════════
# P2: HISTORICAL PERFORMANCE ANALYTICS
# ═══════════════════════════════════════════════════════════════

def record_resolved_signal_analytics(state: dict, sig_record: dict):
    """
    Append a resolved signal record to state["signal_analytics"].
    Fields: symbol, direction, signal_type, score, funding_rate,
            atr_pct, oi_change_pct, result, timestamp.
    """
    analytics = state.setdefault("signal_analytics", [])
    analytics.append(sig_record)
    # Keep last 500 records to avoid unbounded growth
    state["signal_analytics"] = analytics[-500:]


def compute_win_rate_analytics(state: dict, symbol: str, direction: str,
                                signal_type: str, score: int) -> dict:
    """
    Look up historical performance and return a confidence adjustment.
    Minimum sample size: WIN_RATE_MIN_SAMPLE.

    Returns {win_rate, sample_size, score_adj, label}
    """
    analytics = state.get("signal_analytics", [])

    def win_rate_for(records):
        if len(records) < WIN_RATE_MIN_SAMPLE:
            return None, len(records)
        wins = sum(1 for r in records if r.get("result") in ("tp1", "tp2"))
        return wins / len(records), len(records)

    # Priority: most specific filter first (symbol + direction + type)
    specific = [r for r in analytics
                if r.get("symbol") == symbol
                and r.get("direction") == direction
                and r.get("signal_type") == signal_type]

    wr, n = win_rate_for(specific)

    if wr is None:
        # Fall back to symbol + direction
        broader = [r for r in analytics
                   if r.get("symbol") == symbol
                   and r.get("direction") == direction]
        wr, n = win_rate_for(broader)

    if wr is None:
        # Fall back to symbol only
        symbol_only = [r for r in analytics if r.get("symbol") == symbol]
        wr, n = win_rate_for(symbol_only)

    if wr is None:
        return {"win_rate": None, "sample_size": n, "score_adj": 0,
                "label": "Win Rate: Insufficient data"}

    if wr > WIN_RATE_HIGH_THRESH:
        score_adj = 1
    elif wr < WIN_RATE_LOW_THRESH:
        score_adj = -1
    else:
        score_adj = 0

    label = f"Win Rate: {wr*100:.0f}%  Sample: {n}"
    return {"win_rate": wr, "sample_size": n, "score_adj": score_adj, "label": label}


def append_resolved_to_analytics(state: dict):
    """
    Called after check_active_signals() — converts newly resolved signals
    into analytics records. Reads state["resolved_signals"] for entries
    that don't yet have an analytics record.

    This cross-links active signal metadata (score, signal_type, etc.)
    stored in the resolved_signals entry with the analytics store.
    """
    resolved = state.get("resolved_signals", [])
    analytics_keys = {
        (r.get("symbol"), r.get("timestamp"))
        for r in state.get("signal_analytics", [])
    }
    for entry in resolved:
        key = (entry.get("symbol"), entry.get("resolved_at"))
        if key in analytics_keys:
            continue
        # Build analytics record — use metadata if it was stored on resolve
        rec = {
            "symbol":       entry.get("symbol"),
            "direction":    entry.get("direction"),
            "signal_type":  entry.get("signal_type", "UNKNOWN"),
            "score":        entry.get("score", 0),
            "funding_rate": entry.get("funding_rate"),
            "atr_pct":      entry.get("atr_pct"),
            "oi_change_pct": entry.get("oi_change_pct"),
            "result":       entry.get("outcome"),
            "timestamp":    entry.get("resolved_at"),
        }
        record_resolved_signal_analytics(state, rec)


# ═══════════════════════════════════════════════════════════════
# P4: ECONOMIC CALENDAR FILTER
# ═══════════════════════════════════════════════════════════════

# High-impact event keywords we care about
MACRO_EVENT_KEYWORDS = [
    "fomc", "federal funds", "interest rate decision",
    "cpi", "consumer price index",
    "ppi", "producer price index",
    "nfp", "nonfarm payroll", "non-farm payroll",
    "gdp", "gross domestic product",
    "ecb", "bank of england", "boe",
]

# Free calendar source: TradingEconomics-style open endpoint via investing.com
# We use the Stlouisfed / FRED calendar-adjacent approach: a known open API.
# Fallback: fetch from https://nfs.faireconomy.media/ff_calendar_thisweek.json
MACRO_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"


def fetch_macro_calendar(state: dict) -> list[dict]:
    """
    Fetch macro calendar events from the free ForexFactory-style JSON feed.
    Caches results in state["macro_calendar"] with a TTL of MACRO_CACHE_TTL_S.

    Returns list of {name, datetime_utc (ISO string), impact} dicts.
    Only HIGH-impact events matching MACRO_EVENT_KEYWORDS are returned.
    """
    cache_entry = state.get("macro_calendar_cache", {})
    cached_at   = cache_entry.get("fetched_at", 0)
    now_ts      = int(time.time())

    if now_ts - cached_at < MACRO_CACHE_TTL_S:
        return cache_entry.get("events", [])

    try:
        resp = requests.get(MACRO_CALENDAR_URL, timeout=10)
        resp.raise_for_status()
        raw_events = resp.json()
    except Exception as e:
        print(f"  [MACRO CAL] fetch failed: {e} — using cached/empty")
        return cache_entry.get("events", [])

    events = []
    for ev in raw_events:
        impact  = str(ev.get("impact", "")).lower()
        title   = str(ev.get("title", "")).lower()
        ev_date = ev.get("date", "")     # e.g. "2025-01-15"
        ev_time = ev.get("time", "")     # e.g. "08:30am" or "All Day"

        if impact not in ("high",):
            continue

        keyword_match = any(kw in title for kw in MACRO_EVENT_KEYWORDS)
        if not keyword_match:
            continue

        # Parse datetime — ForexFactory feed uses EST, convert to UTC (+5h)
        try:
            if "day" in ev_time.lower() or ev_time.strip() == "":
                # All-day event — treat as 14:00 UTC (common US open)
                dt_str = f"{ev_date} 14:00"
                dt_utc = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            else:
                # e.g. "8:30am"
                dt_str = f"{ev_date} {ev_time}"
                dt_local = datetime.strptime(dt_str, "%Y-%m-%d %I:%M%p")
                dt_utc = dt_local.replace(tzinfo=timezone.utc) + timedelta(hours=5)
        except Exception:
            continue

        events.append({
            "name":         ev.get("title", "Unknown event"),
            "datetime_utc": dt_utc.isoformat(),
            "impact":       impact,
        })

    state["macro_calendar_cache"] = {
        "fetched_at": now_ts,
        "events":     events,
    }
    print(f"  [MACRO CAL] Loaded {len(events)} high-impact events")
    return events


def apply_macro_filter(state: dict, atr_pct: float) -> dict:
    """
    Check whether current time is within the risk window of any macro event.
    Returns {in_window, event_name, mins_to_event, score_adj, label, hard_suppress}
    """
    events = fetch_macro_calendar(state)
    now_utc = datetime.now(timezone.utc)

    nearest_event = None
    nearest_mins  = None

    for ev in events:
        try:
            ev_dt = datetime.fromisoformat(ev["datetime_utc"])
        except Exception:
            continue
        mins_delta = (ev_dt - now_utc).total_seconds() / 60.0

        # Within window: -30 min (after) to +60 min (before)
        if -MACRO_WINDOW_AFTER_MINS <= mins_delta <= MACRO_WINDOW_BEFORE_MINS:
            if nearest_mins is None or abs(mins_delta) < abs(nearest_mins):
                nearest_event = ev["name"]
                nearest_mins  = mins_delta

    if nearest_event is None:
        return {
            "in_window": False, "event_name": None, "mins_to_event": None,
            "score_adj": 0, "label": "Macro Risk: None", "hard_suppress": False,
        }

    # Inside risk window
    score_adj     = -1
    hard_suppress = False

    # Hard suppress ONLY if ATR is also elevated (tail-risk exception)
    if atr_pct >= MACRO_HIGH_ATR_SUPPRESS_PCT:
        hard_suppress = True

    if nearest_mins >= 0:
        label = f"⚠️ Macro Event Nearby\n{nearest_event} in {int(nearest_mins)} minutes"
    else:
        label = f"⚠️ Macro Event Nearby\n{nearest_event} {int(abs(nearest_mins))} minutes ago"

    return {
        "in_window":     True,
        "event_name":    nearest_event,
        "mins_to_event": nearest_mins,
        "score_adj":     score_adj,
        "label":         label,
        "hard_suppress": hard_suppress,
    }


# ═══════════════════════════════════════════════════════════════
# INDICATOR MATH
# ═══════════════════════════════════════════════════════════════

def ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return [float("nan")] * len(values)
    k = 2.0 / (period + 1)
    result = [float("nan")] * len(values)
    result[period - 1] = sum(values[:period]) / period
    for i in range(period, len(values)):
        result[i] = values[i] * k + result[i - 1] * (1 - k)
    return result


def rsi(closes: list[float], period: int) -> list[float]:
    if len(closes) < period + 1:
        return [float("nan")] * len(closes)
    result = [float("nan")] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    rs = avg_g / avg_l if avg_l != 0 else float("inf")
    result[period] = 100 - 100 / (1 + rs)
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l != 0 else float("inf")
        result[i + 1] = 100 - 100 / (1 + rs)
    return result


def atr(highs, lows, closes, period: int) -> list[float]:
    trs = [float("nan")]
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)
    result = [float("nan")] * len(closes)
    if len(trs) < period + 1:
        return result
    result[period] = sum(trs[1:period + 1]) / period
    for i in range(period + 1, len(trs)):
        result[i] = (result[i - 1] * (period - 1) + trs[i]) / period
    return result


def bollinger(closes, period: int, mult: float):
    basis_arr = [float("nan")] * len(closes)
    upper_arr = [float("nan")] * len(closes)
    lower_arr = [float("nan")] * len(closes)
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1: i + 1]
        m  = sum(window) / period
        sd = math.sqrt(sum((x - m) ** 2 for x in window) / period)
        basis_arr[i] = m
        upper_arr[i] = m + mult * sd
        lower_arr[i] = m - mult * sd
    return basis_arr, upper_arr, lower_arr


def adx_dmi(highs, lows, closes, period: int):
    n        = len(closes)
    plus_dm  = [0.0] * n
    minus_dm = [0.0] * n
    tr_arr   = [0.0] * n
    for i in range(1, n):
        up   = highs[i]    - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm[i]  = up   if (up > down  and up > 0)   else 0
        minus_dm[i] = down if (down > up  and down > 0) else 0
        tr_arr[i]   = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )

    def wilder_smooth(arr):
        res = [0.0] * n
        if n <= period:
            return res
        res[period] = sum(arr[1:period + 1])
        for i in range(period + 1, n):
            res[i] = res[i - 1] - res[i - 1] / period + arr[i]
        return res

    sm_tr   = wilder_smooth(tr_arr)
    sm_plus = wilder_smooth(plus_dm)
    sm_min  = wilder_smooth(minus_dm)

    di_plus  = [float("nan")] * n
    di_minus = [float("nan")] * n
    dx_arr   = [float("nan")] * n
    adx_arr  = [float("nan")] * n

    for i in range(period, n):
        if sm_tr[i] == 0:
            continue
        dp = 100 * sm_plus[i] / sm_tr[i]
        dm = 100 * sm_min[i]  / sm_tr[i]
        di_plus[i]  = dp
        di_minus[i] = dm
        dsum = dp + dm
        dx_arr[i] = 100 * abs(dp - dm) / dsum if dsum != 0 else 0

    first = next((i for i in range(period, n) if not math.isnan(dx_arr[i])), None)
    if first is None:
        return di_plus, di_minus, adx_arr
    seed_end = first + period
    if seed_end > n:
        return di_plus, di_minus, adx_arr
    valid = [dx_arr[i] for i in range(first, seed_end) if not math.isnan(dx_arr[i])]
    if len(valid) < period:
        return di_plus, di_minus, adx_arr
    adx_arr[seed_end - 1] = sum(valid) / period
    for i in range(seed_end, n):
        if not math.isnan(adx_arr[i - 1]) and not math.isnan(dx_arr[i]):
            adx_arr[i] = (adx_arr[i - 1] * (period - 1) + dx_arr[i]) / period

    return di_plus, di_minus, adx_arr


def obv(closes, volumes) -> list[float]:
    result = [0.0] * len(closes)
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            result[i] = result[i - 1] + volumes[i]
        elif closes[i] < closes[i - 1]:
            result[i] = result[i - 1] - volumes[i]
        else:
            result[i] = result[i - 1]
    return result


def sma(values, period: int) -> list[float]:
    result = [float("nan")] * len(values)
    for i in range(period - 1, len(values)):
        result[i] = sum(values[i - period + 1: i + 1]) / period
    return result


def rolling_vwap(closes, volumes, period: int) -> list[float]:
    pv  = [c * v for c, v in zip(closes, volumes)]
    spv = sma(pv,      period)
    sv  = sma(volumes, period)
    result = [float("nan")] * len(closes)
    for i in range(len(closes)):
        if not math.isnan(spv[i]) and sv[i] != 0:
            result[i] = spv[i] / sv[i]
    return result


def safe(v, fallback=0.0):
    return fallback if (v is None or math.isnan(v)) else v


# ═══════════════════════════════════════════════════════════════
# SIGNAL LOGIC
# ═══════════════════════════════════════════════════════════════

class SignalResult:
    def __init__(self):
        self.fire_long   = False
        self.fire_short  = False
        self.signal_type = ""
        self.score       = 0          # base score from indicator stack
        self.final_score = 0          # score after all adjustments
        self.entry = self.tp1 = self.tp2 = self.sl = 0.0
        self.entry_low = self.entry_high = 0.0
        self.atr_pct = 0.0
        self.atr_val  = 0.0
        self.breakdown = ""
        self.v10_gates = ""
        self.funding_rate:  float | None = None
        self.open_interest: float | None = None
        self.supports:    list[float] = []
        self.resistances: list[float] = []
        # v3 fields
        self.oi_trend_data:    dict = {}
        self.btc_regime_label: str  = ""
        self.breadth_label:    str  = ""
        self.rs_data:          dict = {}
        self.win_rate_data:    dict = {}
        self.macro_data:       dict = {}
        self.vol_ratio:        float | None = None
        self.score_adjustments: list[tuple[str, int]] = []   # (label, adj) trail


def compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d,
                    state: dict) -> SignalResult:
    res = SignalResult()

    def arrays(candles):
        o  = [c["o"] for c in candles]
        h  = [c["h"] for c in candles]
        l  = [c["l"] for c in candles]
        cl = [c["c"] for c in candles]
        v  = [c["v"] for c in candles]
        return o, h, l, cl, v

    # ── 15m ──────────────────────────────────────────────────
    o15, h15, l15, c15, v15 = arrays(candles_15m)
    ema_f15 = ema(c15, FAST_LEN);  ef15 = safe(ema_f15[-1])
    ema_s15 = ema(c15, SLOW_LEN);  es15 = safe(ema_s15[-1])
    rsi15   = rsi(c15, RSI_LEN);   r15  = safe(rsi15[-1])
    atr15   = atr(h15, l15, c15, ATR_LEN); a15 = safe(atr15[-1])
    _, _, adx15_arr = adx_dmi(h15, l15, c15, ADX_LEN)
    adx15    = safe(adx15_arr[-1], 25.0)
    bb_b15, _, _ = bollinger(c15, BB_LEN, BB_MULT)
    bb_basis = safe(bb_b15[-1])
    vol_ma15 = sma(v15, VOL_LEN);  vm15 = safe(vol_ma15[-1])
    obv15    = obv(c15, v15)
    rvwap15  = rolling_vwap(c15, v15, ROLLING_VWAP_LEN)

    cur_c = c15[-1]; cur_o = o15[-1]
    cur_h = h15[-1]; cur_l = l15[-1]
    cur_v = v15[-1]
    prev_l = l15[-2]; prev_h = h15[-2]
    ef15_prev = safe(ema_f15[-2])

    atr_val  = a15 if a15 > 0 else (cur_c * 0.03 / 100)
    atr_pct  = atr_val / cur_c * 100
    market_ok = MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT

    rv = safe(rvwap15[-1])
    vwap_long  = cur_c > rv if USE_ROLLING_VWAP else True
    vwap_short = cur_c < rv if USE_ROLLING_VWAP else True

    rsi_long  = RSI_LONG_MIN  <= r15 <= RSI_LONG_MAX
    rsi_short = RSI_SHORT_MIN <= r15 <= RSI_SHORT_MAX

    bb_long  = cur_c > bb_basis
    bb_short = cur_c < bb_basis

    obv_slope_long  = obv15[-1] > obv15[-(1 + OBV_LEN)]
    obv_slope_short = obv15[-1] < obv15[-(1 + OBV_LEN)]

    adx_score_ok = adx15 >= ADX_SCORE_MIN
    vol_score_ok = True if vm15 == 0 else (cur_v >= vm15 * VOL_SCORE_MULT)

    # ── P6: Upgraded BREAK volume confirmation ────────────────
    vol_break_ok  = True if vm15 == 0 else (cur_v >= vm15 * BREAK_VOL_MULT)
    vol_ratio     = (cur_v / vm15) if vm15 > 0 else None

    # ── 1H ───────────────────────────────────────────────────
    o1h, h1h, l1h, c1h, v1h = arrays(candles_1h)
    ema_f1h = ema(c1h, FAST_LEN)
    ema_s1h = ema(c1h, SLOW_LEN)
    ef1h = safe(ema_f1h[-2])
    es1h = safe(ema_s1h[-2])
    h1_bull = ef1h > es1h
    h1_bear = ef1h < es1h

    # ── 4H ───────────────────────────────────────────────────
    o4h, h4h, l4h, c4h, v4h = arrays(candles_4h)
    ema_f4h = ema(c4h, FAST_LEN)
    ema_s4h = ema(c4h, SLOW_LEN)
    ef4h = safe(ema_f4h[-2]); es4h = safe(ema_s4h[-2])
    _, _, adx4h_arr = adx_dmi(h4h, l4h, c4h, ADX_LEN)
    adx4h  = safe(adx4h_arr[-2], 25.0)
    h4_bull = ef4h > es4h
    h4_bear = ef4h < es4h

    # ── P7: Record breadth data (reuses already-computed 4H EMA50) ──
    price_above_ema50_4h = c4h[-1] > safe(ema_s4h[-1])
    record_breadth_result(symbol, price_above_ema50_4h)

    # ── P5: Record 7-day return for RS calculation ────────────
    if len(c4h) >= 42:   # 42 × 4H bars ≈ 7 days
        ret_7d = (c4h[-1] - c4h[-42]) / c4h[-42] * 100.0 if c4h[-42] != 0 else 0.0
    elif len(c15) >= 672:  # 672 × 15m bars = 7 days fallback
        ret_7d = (c15[-1] - c15[-672]) / c15[-672] * 100.0 if c15[-672] != 0 else 0.0
    else:
        ret_7d = 0.0
    record_rs_return(symbol, ret_7d)

    def trend_held(ef_arr, es_arr, bull: bool) -> bool:
        for offset in range(1, TREND_HOLD_BARS + 1):
            idx = -(offset + 1)
            if len(ef_arr) < offset + 2:
                return False
            if bull:
                if safe(ef_arr[idx]) <= safe(es_arr[idx]):
                    return False
            else:
                if safe(ef_arr[idx]) >= safe(es_arr[idx]):
                    return False
        return True

    h4_trend_held_bull = trend_held(ema_f4h, ema_s4h, True)
    h4_trend_held_bear = trend_held(ema_f4h, ema_s4h, False)

    # ── Daily 200 EMA + Daily ADX ────────────────────────────
    if candles_d and len(candles_d) >= TREND_LEN:
        o_d, h_d, l_d, c_d, v_d = arrays(candles_d)
        ema_d200 = ema(c_d, TREND_LEN)
        d200 = safe(ema_d200[-2])
        _, _, adx_d_arr = adx_dmi(h_d, l_d, c_d, ADX_LEN)
        adx_daily = safe(adx_d_arr[-2], 25.0)
    else:
        d200 = cur_c
        adx_daily = 25.0
    macro_long  = cur_c > d200 if USE_D200_FILTER else True
    macro_short = cur_c < d200 if USE_D200_FILTER else True

    daily_adx_ok = adx_daily >= MIN_DAILY_ADX if USE_DAILY_ADX else True

    full_long_align  = h4_bull and h4_trend_held_bull and h1_bull
    full_short_align = h4_bear and h4_trend_held_bear and h1_bear

    pull_long_align  = (h4_bull and h4_trend_held_bull and h1_bull) \
                        if PULL_REQUIRES_4H else \
                       (h4_trend_held_bull and h1_bull)
    pull_short_align = (h4_bear and h4_trend_held_bear and h1_bear) \
                        if PULL_REQUIRES_4H else \
                       (h4_trend_held_bear and h1_bear)

    rng = cur_h - cur_l + 1e-10

    def clean_bull_bar():
        return (cur_h - max(cur_o, cur_c)) / rng <= WICK_FILTER

    def clean_bear_bar():
        return (min(cur_o, cur_c) - cur_l) / rng <= WICK_FILTER

    def close_in_top_range():
        return (cur_c - cur_l) / rng >= (1.0 - RANGE_PCT_BREAK)

    def close_in_bot_range():
        return (cur_c - cur_l) / rng <= RANGE_PCT_BREAK

    adx_break_ok   = adx15 >= ADX_BREAK_GATE
    break_bull_bar = cur_c > cur_o and clean_bull_bar() and close_in_top_range()
    break_bear_bar = cur_c < cur_o and clean_bear_bar() and close_in_bot_range()

    pull_zone          = atr_val * PULL_ZONE_MULT
    pull_touched_long  = prev_l <= ef15_prev + pull_zone
    pull_touched_short = prev_h >= ef15_prev - pull_zone
    pull_recover_long  = cur_c > ef15 and cur_c > cur_o
    pull_recover_short = cur_c < ef15 and cur_c < cur_o
    pull_bull_bar      = cur_c > cur_o and clean_bull_bar()
    pull_bear_bar      = cur_c < cur_o and clean_bear_bar()

    # ── Base score (OI scoring slot replaced by real OI adjustment below) ──
    # NOTE: oi_expanding boolean removed from base score; OI is now handled
    # via compute_oi_trend() score adjustment in the final scoring stack.
    long_score = sum([
        bb_long,
        h4_bull and h4_trend_held_bull,
        adx_score_ok,
        obv_slope_long,
        vol_score_ok,
    ])
    short_score = sum([
        bb_short,
        h4_bear and h4_trend_held_bear,
        adx_score_ok,
        obv_slope_short,
        vol_score_ok,
    ])

    # ── P6: BREAK signals require upgraded volume confirmation ──
    # PULL signals retain current logic (vol_score_ok).
    long_break  = (macro_long  and daily_adx_ok and full_long_align  and break_bull_bar
                   and adx_break_ok and vwap_long  and rsi_long
                   and long_score  >= (MIN_SCORE - 1)   # one less since OI adj applied later
                   and market_ok
                   and vol_break_ok)    # P6: upgraded threshold for BREAK
    short_break = (macro_short and daily_adx_ok and full_short_align and break_bear_bar
                   and adx_break_ok and vwap_short and rsi_short
                   and short_score >= (MIN_SCORE - 1)
                   and market_ok
                   and vol_break_ok)    # P6: upgraded threshold for BREAK

    long_pull  = (macro_long  and daily_adx_ok and pull_long_align  and pull_touched_long
                  and pull_recover_long  and pull_bull_bar and vwap_long  and rsi_long
                  and long_score  >= (MIN_SCORE - 1) and market_ok)
    short_pull = (macro_short and daily_adx_ok and pull_short_align and pull_touched_short
                  and pull_recover_short and pull_bear_bar and vwap_short and rsi_short
                  and short_score >= (MIN_SCORE - 1) and market_ok)

    long_sig  = long_break  or long_pull
    short_sig = short_break or short_pull

    def make_breakdown(is_long, oi_tag: str = "OI?", vol_ratio_val: float | None = None):
        bb_s  = "BB✓"  if (bb_long  if is_long else bb_short)  else "BB✗"
        d_s   = "4H✓"  if (h4_bull and h4_trend_held_bull if is_long else h4_bear and h4_trend_held_bear) else "4H✗"
        adx_s = "ADX✓" if adx_score_ok  else "ADX✗"
        obv_s = "OBV✓" if (obv_slope_long if is_long else obv_slope_short) else "OBV✗"
        vol_s = "VOL✓" if vol_score_ok   else "VOL✗"
        return f"{bb_s} {d_s} {adx_s} {obv_s} {vol_s} {oi_tag}"

    def make_gates(is_long):
        macro = macro_long if is_long else macro_short
        h1    = h1_bull    if is_long else h1_bear
        return (f"D200:{'✓' if macro else '✗'} "
                f"4HADX:{'✓' if daily_adx_ok else '✗'} "
                f"1H:{'✓' if h1 else '✗'}")

    if long_sig:
        res.fire_long    = True
        res.signal_type  = "BREAK" if long_break else "PULL"
        res.score        = long_score
        res.entry        = cur_c
        zone_half_width  = atr_val * PULL_ZONE_MULT
        res.entry_low    = cur_c - zone_half_width
        res.entry_high   = cur_c + zone_half_width
        res.tp1          = cur_c + atr_val * TP1_MULT
        res.tp2          = cur_c + atr_val * TP2_MULT
        res.sl           = cur_c - atr_val * SL_MULT
        res.atr_val      = atr_val
        res.atr_pct      = atr_pct
        res.v10_gates    = make_gates(True)
        res.vol_ratio    = vol_ratio
    elif short_sig:
        res.fire_short   = True
        res.signal_type  = "BREAK" if short_break else "PULL"
        res.score        = short_score
        res.entry        = cur_c
        zone_half_width  = atr_val * PULL_ZONE_MULT
        res.entry_low    = cur_c - zone_half_width
        res.entry_high   = cur_c + zone_half_width
        res.tp1          = cur_c - atr_val * TP1_MULT
        res.tp2          = cur_c - atr_val * TP2_MULT
        res.sl           = cur_c + atr_val * SL_MULT
        res.atr_val      = atr_val
        res.atr_pct      = atr_pct
        res.v10_gates    = make_gates(False)
        res.vol_ratio    = vol_ratio

    if not (res.fire_long or res.fire_short):
        return res

    direction = "long" if res.fire_long else "short"
    price_dir = "up" if cur_c > cur_o else "down"

    # ════════════════════════════════════════════════════════
    # FINAL SCORING STACK — applied in order, threshold checked at end
    # ════════════════════════════════════════════════════════
    adjusted_score = res.score
    adjs = res.score_adjustments

    # ── Step 1: OI Confirmation ───────────────────────────────
    oi_data = compute_oi_trend(state, symbol, cur_c, price_dir)
    res.oi_trend_data = oi_data
    adjusted_score += oi_data["score_adj"]
    if oi_data["score_adj"] != 0:
        adjs.append((f"OI ({oi_data['breakdown_tag']})", oi_data["score_adj"]))

    # ── Step 2a: BTC Regime ───────────────────────────────────
    btc_adj, btc_label = apply_btc_regime_adjustment(direction, symbol)
    res.btc_regime_label = btc_label
    adjusted_score += btc_adj
    if btc_adj != 0:
        adjs.append((btc_label, btc_adj))

    # ── Step 2b: Market Breadth ───────────────────────────────
    breadth_adj, breadth_label = apply_breadth_adjustment(direction)
    res.breadth_label = breadth_label
    adjusted_score += breadth_adj
    if breadth_adj != 0:
        adjs.append((breadth_label, breadth_adj))

    # ── Step 3: Relative Strength ─────────────────────────────
    rs_data = compute_relative_strength(symbol)
    res.rs_data = rs_data
    adjusted_score += rs_data["score_adj"]
    if rs_data["score_adj"] != 0:
        adjs.append((rs_data["label"], rs_data["score_adj"]))

    # ── Step 4: Historical Win Rate ───────────────────────────
    wr_data = compute_win_rate_analytics(state, symbol, direction,
                                          res.signal_type, res.score)
    res.win_rate_data = wr_data
    adjusted_score += wr_data["score_adj"]
    if wr_data["score_adj"] != 0:
        adjs.append((wr_data["label"], wr_data["score_adj"]))

    # ── Step 5: Macro Filter ──────────────────────────────────
    macro_data = apply_macro_filter(state, atr_pct)
    res.macro_data = macro_data
    if macro_data["hard_suppress"]:
        print(f"  [MACRO FILTER] {symbol} hard suppressed — elevated ATR + macro risk window")
        res.fire_long  = False
        res.fire_short = False
        return res
    adjusted_score += macro_data["score_adj"]
    if macro_data["score_adj"] != 0:
        adjs.append(("Macro Risk Window", macro_data["score_adj"]))

    # ── Step 6: Volume Confirmation (BREAK only) ──────────────
    if res.signal_type == "BREAK":
        vol_confirmed = vol_break_ok
        if not vol_confirmed:
            adjusted_score -= 1
            adjs.append(("Vol (BREAK threshold not met)", -1))

    # ── Final suppression check ───────────────────────────────
    res.final_score = adjusted_score
    if adjusted_score < MIN_SCORE:
        print(f"  [SCORE FILTER] {symbol} {direction.upper()} suppressed: "
              f"base={res.score} → final={adjusted_score} < {MIN_SCORE}")
        res.fire_long  = False
        res.fire_short = False
        return res

    # Update breakdown with real OI tag
    res.breakdown = make_breakdown(res.fire_long, oi_data["breakdown_tag"], vol_ratio)

    # ── Support / Resistance ──────────────────────────────────
    if res.fire_long or res.fire_short:
        res.supports, res.resistances = find_sr_levels(candles_15m)

    # ── S/R clearance filter ──────────────────────────────────
    if SR_CLEARANCE_ATR_MULT > 0 and (res.fire_long or res.fire_short):
        min_clearance = atr_val * SR_CLEARANCE_ATR_MULT
        if res.fire_long and res.resistances:
            nearest_res = res.resistances[0]
            if nearest_res - res.entry < min_clearance:
                print(f"  [SR FILTER] {symbol} LONG suppressed — resistance {nearest_res:.4f} "
                      f"too close to entry {res.entry:.4f} (need {min_clearance:.4f} clearance)")
                res.fire_long = False
        if res.fire_short and res.supports:
            nearest_sup = res.supports[0]
            if res.entry - nearest_sup < min_clearance:
                print(f"  [SR FILTER] {symbol} SHORT suppressed — support {nearest_sup:.4f} "
                      f"too close to entry {res.entry:.4f} (need {min_clearance:.4f} clearance)")
                res.fire_short = False

    return res


# ═══════════════════════════════════════════════════════════════
# COOLDOWN STATE
# ═══════════════════════════════════════════════════════════════

def load_state() -> dict:
    if Path(STATE_FILE).exists():
        try:
            s = json.loads(Path(STATE_FILE).read_text())
            # Backward compat: initialize missing v3 keys safely
            s.setdefault("oi_history", {})
            s.setdefault("signal_analytics", [])
            s.setdefault("macro_calendar_cache", {})
            # Dead fields from previous versions — leave untouched, don't read/write
            # news_score, sentiment_score, final_news_score remain as-is if present
            return s
        except Exception:
            pass
    return {
        "oi_history": {},
        "signal_analytics": [],
        "macro_calendar_cache": {},
    }


def save_state(state: dict):
    import tempfile, os as _os
    tmp_path = STATE_FILE + ".tmp"
    Path(tmp_path).write_text(json.dumps(state, indent=2))
    _os.replace(tmp_path, STATE_FILE)


def check_cooldown(state, coin, direction, bar_index) -> bool:
    last_dir    = state.get(f"{coin}_{direction}", -9999)
    last_global = state.get(f"{coin}_any",         -9999)
    return (bar_index - last_dir    >= COOLDOWN_BARS and
            bar_index - last_global >= GLOBAL_COOLDOWN)


def update_cooldown(state, coin, direction, bar_index):
    state[f"{coin}_{direction}"] = bar_index
    state[f"{coin}_any"]         = bar_index


# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def stars(score: int) -> str:
    capped = max(0, min(score, 8))   # v3: score can exceed 6 with bonuses
    filled = min(capped, 6)
    return "★" * filled + "☆" * max(0, 6 - filled)


def send_telegram(text: str) -> int | None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(url, json={
                "chat_id":    TG_CHAT_ID,
                "text":       text,
                "parse_mode": "HTML",
            }, timeout=10)
            r.raise_for_status()
            return r.json()["result"]["message_id"]
        except Exception as e:
            if attempt == 2:
                print(f"[TG ERROR] {e}")
            time.sleep(2)
    return None


def react_to_message(message_id: int, emoji: str):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    try:
        r = requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "message_id": message_id,
            "reaction":   [{"type": "emoji", "emoji": emoji}],
            "is_big":     False,
        }, timeout=10)
        r.raise_for_status()
        print(f"  [REACT] {emoji} → msg_id {message_id}")
    except Exception as e:
        print(f"  [REACT ERROR] msg_id {message_id}: {e}")


RANK_MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}


def priority_score(sig: SignalResult) -> tuple:
    """
    Sort key — uses final_score (post-adjustment) as primary key.
    """
    direction    = "long" if sig.fire_long else "short"
    rate         = sig.funding_rate or 0.0
    tailwind     = (rate < 0 and direction == "long") or (rate > 0 and direction == "short")
    is_break     = sig.signal_type == "BREAK"
    oi_confirm   = sig.oi_trend_data.get("score_adj", 0) > 0
    return (sig.final_score, int(is_break), int(tailwind), int(oi_confirm))


def format_signal(symbol: str, sig: SignalResult, engine_tag: str = "V5", rank: int = 0) -> str:
    direction = "▲ LONG" if sig.fire_long else "▼ SHORT"
    dir_str   = "long" if sig.fire_long else "short"
    emoji     = "🟢" if sig.fire_long else "🔴"
    ts        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def fmt(v):
        if v >= 1000: return f"{v:,.2f}"
        if v >= 1:    return f"{v:.4f}"
        return f"{v:.6f}"

    def recommended_leverage(atr_pct: float, score: int) -> str:
        if atr_pct <= 0.60:
            low, high = 8, 12
        elif atr_pct <= 1.20:
            low, high = 5, 10
        elif atr_pct <= 2.00:
            low, high = 3, 7
        else:
            low, high = 2, 5
        if score <= 4:
            high = max(low, high - 2)
        return f"{low}x - {high}x"

    lev_range = recommended_leverage(sig.atr_pct, sig.final_score)

    sr_lines = ""
    if sig.resistances:
        sr_lines += "🔴 Resistance: " + "  |  ".join(f"<code>{fmt(r)}</code>" for r in sig.resistances) + "\n"
    if sig.supports:
        sr_lines += "🟢 Support:    " + "  |  ".join(f"<code>{fmt(s)}</code>" for s in sig.supports) + "\n"

    chk_trend   = "✅" if sig.v10_gates else "☐"
    chk_sr      = "✅" if (sig.supports or sig.resistances) else "☐"
    chk_entry   = "✅"
    chk_lev     = "✅"
    funding_str = format_funding(sig.funding_rate, dir_str)
    rate        = sig.funding_rate
    headwind    = rate is not None and (
        (rate > 0 and dir_str == "long") or (rate < 0 and dir_str == "short")
    )
    chk_funding = "⚠️" if (headwind and rate is not None and abs(rate) >= FUNDING_WARN_EXTREME) else "✅"

    # ── v3 extra fields (Final Scoring Stack order) ───────────
    oi_data     = sig.oi_trend_data
    oi_line     = oi_data.get("label", "OI Trend: Unknown")
    btc_line    = sig.btc_regime_label or "BTC Regime: Unknown"
    breadth_line = sig.breadth_label   or "Market Breadth: Unknown"
    rs_data     = sig.rs_data
    rs_line     = rs_data.get("label", "Relative Strength: N/A")
    wr_data     = sig.win_rate_data
    wr_line     = wr_data.get("label", "Win Rate: N/A")
    macro_line  = sig.macro_data.get("label", "Macro Risk: None") if sig.macro_data else "Macro Risk: None"
    vol_ratio_line = (f"Volume Ratio: {sig.vol_ratio:.1f}x" if sig.vol_ratio is not None
                      else "Volume Ratio: N/A")

    score_trail = ""
    if sig.score_adjustments:
        parts = []
        for lbl, adj in sig.score_adjustments:
            sign = "+" if adj > 0 else ""
            parts.append(f"{lbl}: {sign}{adj}")
        score_trail = "\n<i>Adjustments: " + "  |  ".join(parts) + "</i>"

    checklist = (
        f"\n<b>Pre-Trade Checklist</b>\n"
        f"{chk_trend} Trend identified (4H/1H/15m aligned)\n"
        f"{'✅' if sig.supports or sig.resistances else '☐'} Key S/R marked\n"
        f"{chk_entry} Clear entry signal ({sig.signal_type}, score {sig.final_score}/{sig.score}+adj)\n"
        f"{chk_lev} Leverage appropriate ({lev_range})\n"
        f"{chk_funding} {funding_str}\n"
        f"📊 {format_oi(sig.open_interest)}"
    )

    sr_block = f"\n{sr_lines}" if sr_lines else ""

    medal    = RANK_MEDALS.get(rank, "")
    rank_tag = f"{medal} <b>Priority #{rank}</b>\n" if rank else ""

    return (
        f"{rank_tag}{emoji} <b>{direction} [{sig.signal_type}]</b>  {stars(sig.final_score)}\n"
        f"<b>Pair:</b>  {symbol}\n\n"
        f"<b>Entry:</b> <code>{fmt(sig.entry)}</code>\n"
        f"<b>Entry Zone:</b> <code>{fmt(sig.entry_low)}</code> – <code>{fmt(sig.entry_high)}</code>\n\n"
        f"<b>TP1:</b>   <code>{fmt(sig.tp1)}</code>\n"
        f"<b>TP2:</b>   <code>{fmt(sig.tp2)}</code>\n"
        f"<b>SL:</b>    <code>{fmt(sig.sl)}</code>\n\n"
        f"<b>Leverage:</b> {lev_range}\n"
        f"<b>Score:</b> {sig.final_score}/{sig.score}+adj  |  {sig.breakdown}\n"
        f"<b>Gates:</b> {sig.v10_gates}\n"
        f"\n<b>Signal Context</b>\n"
        f"{oi_line}\n"
        f"{btc_line}\n"
        f"{breadth_line}\n"
        f"{rs_line}\n"
        f"{wr_line}\n"
        f"{macro_line}\n"
        f"{vol_ratio_line}"
        f"{score_trail}"
        f"{sr_block}"
        f"{checklist}\n\n"
        f"<i>Scalp Swing v10 [4H/15m] • Hyperliquid Perps • {ts}</i>"
    )


# ═══════════════════════════════════════════════════════════════
# ACTIVE SIGNAL TRACKING  (TP / SL reactions)
# ═══════════════════════════════════════════════════════════════

def track_signal(state: dict, symbol: str, direction: str,
                 msg_id: int, sig: SignalResult, bar_index: int):
    """
    Persist a newly fired signal so subsequent runs can poll its TP/SL.
    v3: also stores signal_type, score, atr_pct, oi_change_pct for analytics.
    """
    active = state.setdefault("active_signals", [])
    active.append({
        "symbol":          symbol,
        "direction":       direction,
        "msg_id":          msg_id,
        "bar_index":       bar_index,
        "signal_bar_time": (int(time.time() * 1000) // 900_000) * 900_000,
        "tp1":             sig.tp1,
        "tp2":             sig.tp2,
        "sl":              sig.sl,
        "tp1_hit":         False,
        "resolved":        False,
        # v3 analytics metadata
        "signal_type":     sig.signal_type,
        "score":           sig.final_score,
        "atr_pct":         sig.atr_pct,
        "funding_rate":    sig.funding_rate,
        "oi_change_pct":   sig.oi_trend_data.get("oi_change_pct"),
    })


def check_active_signals(state: dict, bar_index_now: int):
    """
    Walk active signals oldest→newest per candle to resolve TP/SL correctly.
    v3: writes resolved entries with analytics metadata for win-rate tracking.
    """
    signals = state.get("active_signals", [])
    if not signals:
        return

    still_active = []

    for sig in signals:
        age = bar_index_now - sig.get("bar_index", bar_index_now)
        if age > SIGNAL_MAX_AGE_BARS:
            print(f"  [TRACK] {sig['symbol']} expired after {age} bars — dropping")
            continue

        if sig.get("resolved", False):
            continue

        symbol    = sig["symbol"]
        direction = sig["direction"]
        msg_id    = sig["msg_id"]
        tp1       = sig["tp1"]
        tp2       = sig["tp2"]
        sl        = sig["sl"]
        tp1_hit   = sig.get("tp1_hit", False)
        signal_bar_time_ms: int | None = sig.get("signal_bar_time")

        try:
            candles = get_candles(symbol, "15m", N_15M, start_time_ms=signal_bar_time_ms)
        except Exception as e:
            print(f"  [TRACK] candle fetch failed for {symbol}: {e}")
            still_active.append(sig)
            continue

        if not candles:
            still_active.append(sig)
            continue

        def resolve_signal(outcome: str):
            # Build resolved entry with analytics metadata
            entry = {
                "symbol":       symbol,
                "direction":    direction,
                "outcome":      outcome,
                "resolved_at":  int(time.time()),
                # analytics fields (from original signal metadata)
                "signal_type":  sig.get("signal_type", "UNKNOWN"),
                "score":        sig.get("score", 0),
                "atr_pct":      sig.get("atr_pct"),
                "funding_rate": sig.get("funding_rate"),
                "oi_change_pct": sig.get("oi_change_pct"),
            }
            state.setdefault("resolved_signals", []).append(entry)
            sig["resolved"] = True

        for candle in candles:
            c_high = candle["h"]
            c_low  = candle["l"]

            if direction == "long":
                if not tp1_hit and c_low <= sl:
                    react_to_message(msg_id, REACT_SL)
                    print(f"  [TRACK] {symbol} SL hit → {REACT_SL}")
                    resolve_signal("sl")
                    break
                if not tp1_hit and c_high >= tp1:
                    react_to_message(msg_id, REACT_TP1)
                    print(f"  [TRACK] {symbol} TP1 hit → {REACT_TP1}")
                    tp1_hit = True
                    sig["tp1_hit"] = True
                if tp1_hit and c_high >= tp2:
                    react_to_message(msg_id, REACT_TP2)
                    print(f"  [TRACK] {symbol} TP2 hit → {REACT_TP2}")
                    resolve_signal("tp2")
                    break
                if tp1_hit and not sig.get("resolved", False) and c_low <= sl:
                    print(f"  [TRACK] {symbol} SL hit after TP1 — resolving silently")
                    resolve_signal("tp1")
                    break
            else:
                if not tp1_hit and c_high >= sl:
                    react_to_message(msg_id, REACT_SL)
                    print(f"  [TRACK] {symbol} SL hit → {REACT_SL}")
                    resolve_signal("sl")
                    break
                if not tp1_hit and c_low <= tp1:
                    react_to_message(msg_id, REACT_TP1)
                    print(f"  [TRACK] {symbol} TP1 hit → {REACT_TP1}")
                    tp1_hit = True
                    sig["tp1_hit"] = True
                if tp1_hit and c_low <= tp2:
                    react_to_message(msg_id, REACT_TP2)
                    print(f"  [TRACK] {symbol} TP2 hit → {REACT_TP2}")
                    resolve_signal("tp2")
                    break
                if tp1_hit and not sig.get("resolved", False) and c_high >= sl:
                    print(f"  [TRACK] {symbol} SL hit after TP1 — resolving silently")
                    resolve_signal("tp1")
                    break

        if not sig.get("resolved", False):
            still_active.append(sig)

    state["active_signals"] = still_active


# ═══════════════════════════════════════════════════════════════
# DAILY SUMMARY
# ═══════════════════════════════════════════════════════════════

def should_send_summary() -> bool:
    now = datetime.now(timezone.utc)
    return now.hour == 8 and now.minute < 15


def send_summary(state: dict):
    cutoff_24h = int(time.time()) - 86400
    cutoff_48h = int(time.time()) - 172800

    recent = [
        e for e in state.get("resolved_signals", [])
        if e["resolved_at"] >= cutoff_24h
    ]

    tp1_count = sum(1 for e in recent if e["outcome"] == "tp1")
    tp2_count = sum(1 for e in recent if e["outcome"] == "tp2")
    sl_count  = sum(1 for e in recent if e["outcome"] == "sl")
    winners   = tp1_count + tp2_count
    losers    = sl_count

    if winners == 0 and losers == 0:
        return

    # ── v3: Win-rate breakdown by category ───────────────────
    all_analytics = state.get("signal_analytics", [])
    total = len(all_analytics)
    overall_wr = None
    if total >= WIN_RATE_MIN_SAMPLE:
        wins = sum(1 for r in all_analytics if r.get("result") in ("tp1", "tp2"))
        overall_wr = wins / total

    line1 = "📊 Signal Summary (last 24h)"
    line2 = f"✅ Winners: {winners} (🔥×{tp1_count}  🏆×{tp2_count})"
    line3 = f"❌ Losers:  {losers} (😭×{sl_count})"
    lines = [line1, line2, line3]

    if overall_wr is not None:
        lines.append(f"📈 Overall Win Rate: {overall_wr*100:.0f}% ({total} trades)")

    send_telegram("\n".join(lines))

    state["resolved_signals"] = [
        e for e in state.get("resolved_signals", [])
        if e["resolved_at"] >= cutoff_48h
    ]


# ═══════════════════════════════════════════════════════════════
# SCAN
# ═══════════════════════════════════════════════════════════════

def scan_symbol(symbol: str, state: dict, bar_index_now: int) -> list[tuple[str, str, str, SignalResult]]:
    """
    Scan a single symbol. Returns list of (symbol, formatted_msg, direction, sig).
    """
    coin = hl_coin(symbol)
    data = fetch_all_candles(symbol)
    if data is None:
        print(f"    Skipping {coin}: insufficient candles")
        return []

    candles_15m, candles_1h, candles_4h, candles_d = data

    # Fetch and cache OI from the shared meta call, then update history
    ctx = get_market_context(symbol)
    if ctx:
        oi_coins = ctx.get("open_interest_coins")
        update_oi_history(state, symbol, oi_coins)

    sig = compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d, state)
    if not (sig.fire_long or sig.fire_short):
        print(f"    {coin}: no signal")
        return []

    direction = "long" if sig.fire_long else "short"
    if not check_cooldown(state, symbol, direction, bar_index_now):
        print(f"    {coin} signal suppressed by cooldown")
        return []

    print(f"    🚀 SIGNAL: {coin} {direction.upper()} [{sig.signal_type}] "
          f"base={sig.score} final={sig.final_score}")

    # Attach market context to signal (OI fetched earlier; reuse)
    if ctx:
        sig.funding_rate  = ctx.get("funding")
        sig.open_interest = ctx.get("open_interest")
        pct = (sig.funding_rate * 100) if sig.funding_rate is not None else float("nan")
        print(f"    [MARKET CTX] {coin}: funding={pct:+.4f}%/8h  {format_oi(sig.open_interest)}")

    # ── Funding suppression filter ────────────────────────────
    if FUNDING_SUPPRESS_EXTREME is not None and sig.funding_rate is not None:
        rate     = sig.funding_rate
        headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
        if headwind and abs(rate) >= FUNDING_SUPPRESS_EXTREME:
            pct = rate * 100
            print(f"    [FUNDING FILTER] {coin} {direction.upper()} suppressed — "
                  f"funding {pct:+.4f}%/8h is extreme headwind")
            return []

    return [(symbol, format_signal(symbol, sig, "CORE"), direction, sig)]


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Scanner starting…")
    print(f"Watchlist ({len(WATCHLIST)} pairs): {[hl_coin(s) for s in WATCHLIST]}")

    bar_index_now = int(time.time() // (15 * 60))
    state         = load_state()

    # ── Step 0: Prime the shared metaAndAssetCtxs cache ──────
    # One API call fetches OI + funding for the entire watchlist.
    print("[INIT] Fetching market context (metaAndAssetCtxs)…")
    get_meta_and_asset_ctxs()

    # ── Step 0b: Prime BTC regime — fetch BTC candles now ────
    print("[INIT] Computing BTC regime…")
    try:
        btc_1h = get_candles("BTCUSDT", "1h", N_1H)
        btc_4h = get_candles("BTCUSDT", "4h", N_4H)
        regime  = compute_btc_regime(btc_1h, btc_4h)
        set_btc_regime(regime)
        print(f"  {regime['label']}")
    except Exception as e:
        print(f"  [BTC REGIME] failed to compute: {e}")

    # ── Step 1: Check pending signals for TP/SL reactions ────
    print("[TRACK] Checking active signals…")
    check_active_signals(state, bar_index_now)

    # ── Step 1a: Sync resolved signals to analytics ───────────
    append_resolved_to_analytics(state)

    save_state(state)

    # ── Step 1.5: Send daily summary if 08:00 UTC window ─────
    if should_send_summary():
        send_summary(state)
        save_state(state)

    # ── Step 2: Scan all symbols in parallel ──────────────────
    # Note: breadth and RS data are populated as a side-effect of
    # compute_signals() for each symbol. Because symbols are scanned
    # in parallel, the breadth/RS caches will be partially filled
    # when early symbols complete. This is acceptable — the cached
    # values will still represent the majority of the watchlist by
    # the time any signal is fully evaluated.
    pending_signals: list[tuple[str, str, str, SignalResult]] = []
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {
            ex.submit(scan_symbol, sym, state, bar_index_now): sym
            for sym in WATCHLIST
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                result = fut.result()
                if result:
                    pending_signals.extend(result)
            except Exception as e:
                print(f"    ERROR processing {sym}: {e}")

    # Update OI history for any symbols not yet captured during scan
    # (scan_symbol already calls update_oi_history — this is a no-op guard)
    save_state(state)   # persist OI history after scan loop

    # ── Step 3: Rank, cap, and send alerts ───────────────────
    pending_signals.sort(key=lambda t: priority_score(t[3]), reverse=True)

    top_signals     = pending_signals[:MAX_SIGNALS_PER_SCAN]
    dropped_signals = pending_signals[MAX_SIGNALS_PER_SCAN:]

    if dropped_signals:
        dropped_names = [f"{hl_coin(s)} {d.upper()}" for s, _, d, _ in dropped_signals]
        print(f"  [RANK] Dropped {len(dropped_signals)} lower-priority signal(s): {', '.join(dropped_names)}")

    signals_fired = 0
    for rank, (symbol, _msg, direction, sig) in enumerate(top_signals, start=1):
        msg    = format_signal(symbol, sig, "CORE", rank=rank)
        msg_id = send_telegram(msg)
        update_cooldown(state, symbol, direction, bar_index_now)
        if msg_id:
            track_signal(state, symbol, direction, msg_id, sig, bar_index_now)
            print(f"  [TRACK] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"TP1={sig.tp1:.4f} TP2={sig.tp2:.4f} SL={sig.sl:.4f}")
        signals_fired += 1
        time.sleep(0.5)

    for symbol, _msg, direction, sig in dropped_signals:
        update_cooldown(state, symbol, direction, bar_index_now)
        print(f"  [RANK] cooldown applied to dropped signal: {hl_coin(symbol)} {direction.upper()}")

    save_state(state)
    print(f"Scan complete. {signals_fired} signal(s) fired.")


if __name__ == "__main__":
    main()
