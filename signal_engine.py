"""
Scalp Swing Bot v11 – Python Signal Engine
Hyperliquid Perpetuals edition
Timeframe map: 4H bias / 1H middle / 15m execution
Runs on GitHub Actions every 15 min, sends Telegram alerts.
No orders are placed — signal only.

v11 UPGRADES
────────────────────────────────────────────
[P1] Real Open Interest Trend:
     Replaced 4H-volume OI proxy with actual HL OI from metaAndAssetCtxs.
     OI history persisted in state.json per symbol. oi_change_pct,
     oi_trend, oi_acceleration calculated. Signals scored on OI/price
     confirmation vs divergence.

[P2] Historical Performance Analytics:
     Every resolved signal stored in state["signal_history"] with full
     metadata. Win rates computed per symbol, signal type, score, and
     direction. Confidence adjustments applied to score (+1/>65% WR,
     -1/<45% WR). Minimum sample size: 20 trades.

[P3] BTC Market Regime Filter:
     4H and 1H EMA21/50 computed for BTC. Altcoin longs suppressed when
     BTC regime is bearish; shorts suppressed when bullish. BTCUSDT
     bypasses filter. Regime displayed in Telegram.

[P4] Economic Calendar Filter:
     Fetches upcoming FOMC/CPI/PPI/NFP/GDP events from
     financialmodelingprep.com free API (cached locally for 4 hours).
     Signals within 60-min pre / 30-min post risk window have score
     reduced by 1. Suppressed entirely when ATR is elevated.

[P5] News Risk Filter:
     RSS feeds (CoinDesk, Cointelegraph, CryptoSlate) parsed once per
     scan and cached. Keyword matching per symbol. Scored events:
     hacks/exploits/delistings suppress; delayed ETF reduces; approvals
     boost. news_score <= -70 suppresses signal; <= -40 reduces by 1;
     >= +50 increases by 1.

[P6] Advanced Sentiment Layer:
     Sentiment keyword scoring applied to same RSS headlines. Output:
     sentiment_score (−100 to +100). Combined:
     final_news_score = news_score*0.7 + sentiment_score*0.3.
     final_news_score used for all news-based gating.

[P7] Relative Strength Ranking:
     7-day return computed for each coin vs BTC. Top-20% RS → +1 score;
     Bottom-20% RS → −1 score. Displayed in Telegram.

[P8] Breakout Volume Upgrade:
     BREAK signals now require cur_v >= vm15 * 1.5 (was 1.0x).
     Volume ratio shown in Telegram. PULL signals retain original logic.

PRESERVED FROM v10
────────────────────────────────────────────
• TP1 / TP2 / SL tracking and reaction emojis
• Reaction system (🔥 🏆 😭)
• Symbol + global cooldowns
• Ranking / cap system (MAX_SIGNALS_PER_SCAN)
• Funding rate filter + suppression
• Support / Resistance filter
• All Telegram formatting
• Backward-compatible state.json (missing fields initialised safely)
"""

import os, json, time, math, random, threading, requests
import xml.etree.ElementTree as ET
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

# Economic calendar API (free, no key required for basic endpoint)
# Swap for your own key if using premium endpoint
ECON_CALENDAR_API_KEY = os.getenv("FMP_API_KEY", "")
ECON_CALENDAR_CACHE_SECS = 4 * 3600   # refresh every 4 hours
MACRO_RISK_WINDOW_BEFORE = 60          # minutes before event
MACRO_RISK_WINDOW_AFTER  = 30          # minutes after event
MACRO_ATR_SUPPRESS_PCT   = 3.0         # suppress if ATR% > this during macro window

# News / sentiment
NEWS_CACHE_SECS  = 900   # refresh RSS every 15 min
NEWS_SUPPRESS_THRESHOLD = -70
NEWS_REDUCE_THRESHOLD   = -40
NEWS_BOOST_THRESHOLD    = +50

# OI history
OI_HISTORY_LEN = 12   # keep last 12 snapshots per symbol (~12 hrs at 1 snap/hr)

# Relative strength
RS_LOOKBACK_BARS_1D = 7   # 7 daily bars for 7-day return
RS_TOP_PERCENTILE   = 0.20
RS_BOT_PERCENTILE   = 0.20

# Historical performance
HIST_MIN_SAMPLE = 20
HIST_BOOST_WR   = 0.65
HIST_PENALISE_WR = 0.45

_hl_request_lock    = threading.Lock()
_hl_last_request_ts = 0.0
_hl_min_interval_s  = HL_MIN_INTERVAL_S

_hl_session = requests.Session()

# Module-level caches (refreshed each process lifetime / scan run)
_news_cache: dict = {}          # {"ts": float, "items": list[dict]}
_econ_cache: dict = {}          # {"ts": float, "events": list[dict]}
_btc_regime: dict = {}          # {"bull": bool, "bear": bool, "ts": float}
_rs_scores:  dict = {}          # {symbol: float}  populated once per scan

# ── HARDCODED WATCHLIST ───────────────────────────────────────
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
MAX_SIGNALS_PER_SCAN = 3

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

# ── ACCURACY FILTERS ─────────────────────────────────────────
FUNDING_SUPPRESS_EXTREME: float = 0.0010
SR_CLEARANCE_ATR_MULT:    float = 0.3
OI_TREND_BARS:            int   = 3

# [P8] Breakout volume multiplier (PULL signals use 1.0×)
BREAK_VOL_MULT: float = 1.5

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

# ── NEWS KEYWORD MAPPING ─────────────────────────────────────
# Maps coin name fragments → score events to watch for
COIN_KEYWORDS: dict[str, list[str]] = {
    "BTC":   ["bitcoin", "btc"],
    "ETH":   ["ethereum", "eth"],
    "SOL":   ["solana", "sol"],
    "XRP":   ["ripple", "xrp"],
    "BNB":   ["binance", "bnb"],
    "DOGE":  ["dogecoin", "doge"],
    "ADA":   ["cardano", "ada"],
    "AVAX":  ["avalanche", "avax"],
    "LINK":  ["chainlink", "link"],
    "DOT":   ["polkadot", "dot"],
    "UNI":   ["uniswap", "uni"],
    "AAVE":  ["aave"],
    "NEAR":  ["near protocol", "near"],
    "HYPE":  ["hyperliquid", "hype"],
    "SUI":   ["sui"],
    "TON":   ["toncoin", "ton"],
    "TRX":   ["tron", "trx"],
    "LTC":   ["litecoin", "ltc"],
    "APT":   ["aptos", "apt"],
    "TAO":   ["bittensor", "tao"],
    "XLM":   ["stellar", "xlm"],
    "ZEC":   ["zcash", "zec"],
    "ONDO":  ["ondo"],
    "PENGU": ["pudgy penguins", "pengu"],
    "PENDLE":["pendle"],
}

# Event → score mapping for news keyword matching
NEWS_EVENT_SCORES: list[tuple[list[str], int]] = [
    (["etf approved", "etf launched", "spot etf", "sec approves"],      +70),
    (["etf delayed", "etf rejected", "sec rejects", "sec delays"],      -50),
    (["hack", "exploit", "hacked", "exploited", "drained", "breach"],   -90),
    (["delisting", "delisted", "removed from"],                         -100),
    (["partnership", "partners with", "collaborates"],                  +20),
    (["funding round", "raised", "series a", "series b", "investment"], +15),
    (["lawsuit", "sec charges", "cftc", "regulatory action"],           -40),
    (["mainnet launch", "mainnet upgrade", "protocol upgrade"],         +25),
    (["airdrop", "token launch"],                                        +10),
    (["scam", "rug pull", "fraud"],                                      -80),
    (["ban", "banned", "prohibited"],                                    -60),
    (["institutional", "adoption", "enterprise"],                        +15),
]

# Sentiment keywords (−100 to +100 contribution per headline)
SENTIMENT_KEYWORDS: list[tuple[list[str], int]] = [
    (["surge", "soar", "rally", "pump", "breakout", "bullish"],         +20),
    (["crash", "plunge", "dump", "collapse", "bearish"],                -20),
    (["all-time high", "ath", "record high"],                           +30),
    (["correction", "pullback", "dip"],                                  -5),
    (["accumulation", "whale buying", "buy the dip"],                   +15),
    (["sell-off", "distribution", "whale selling"],                     -15),
    (["fear", "panic", "uncertainty"],                                   -10),
    (["greed", "euphoria", "mania"],                                     -5),
    (["support", "bounce", "recovery"],                                  +10),
    (["resistance", "rejection", "failed breakout"],                    -10),
    (["adoption", "integration", "partnership"],                         +12),
    (["regulatory clarity", "approval", "legitimate"],                  +18),
    (["regulation", "ban", "crackdown"],                                -15),
    (["innovation", "upgrade", "improvement"],                          +10),
]

# RSS feeds to monitor
RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://cryptoslate.com/feed/",
]


# ═══════════════════════════════════════════════════════════════
# HYPERLIQUID DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def hl_coin(symbol: str) -> str:
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
    Returns (candles_15m, candles_1h, candles_4h, candles_d) or None.
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
# [P1] REAL OPEN INTEREST TREND
# ═══════════════════════════════════════════════════════════════

def fetch_all_oi() -> dict[str, float]:
    """
    Fetch current OI (USD notional) for ALL assets in a single API call.
    Returns {coin_name: oi_usd}.
    Batches the entire watchlist in ONE call — no per-symbol requests.
    """
    try:
        payload = {"type": "metaAndAssetCtxs"}
        data    = hl_post(payload)
        meta       = data[0]
        asset_ctxs = data[1]
        universe   = meta.get("universe", [])
        result: dict[str, float] = {}
        for i, asset in enumerate(universe):
            if i >= len(asset_ctxs):
                break
            ctx  = asset_ctxs[i]
            coin = asset.get("name", "")
            oi   = ctx.get("openInterest")
            mark = ctx.get("markPx")
            if oi is not None and mark is not None:
                result[coin] = float(oi) * float(mark)
        return result
    except Exception as e:
        print(f"  [OI FETCH] failed: {e}")
        return {}


def update_oi_history(state: dict, oi_snapshot: dict[str, float]) -> None:
    """
    Append current OI snapshot to state["oi_history"][coin].
    Trims to OI_HISTORY_LEN entries.
    """
    now = int(time.time())
    hist = state.setdefault("oi_history", {})
    for coin, oi_usd in oi_snapshot.items():
        entries = hist.setdefault(coin, [])
        entries.append({"ts": now, "oi": oi_usd})
        # Trim to max history length
        if len(entries) > OI_HISTORY_LEN:
            entries[:] = entries[-OI_HISTORY_LEN:]


def get_oi_trend(state: dict, symbol: str) -> dict:
    """
    Compute OI trend metrics for a symbol from stored history.

    Returns dict:
      {
        "oi_change_pct":  float,   # change vs oldest snapshot
        "oi_trend":       str,     # "Rising" | "Falling" | "Flat"
        "oi_acceleration":float,   # second-derivative acceleration
        "score_delta":    int,     # +1, 0, or -1
        "label":          str,     # "OI↑" | "OI↓" | "OI Divergence"
        "current_oi":     float | None,
      }
    """
    default = {
        "oi_change_pct":  0.0,
        "oi_trend":       "Flat",
        "oi_acceleration": 0.0,
        "score_delta":     0,
        "label":           "OI?",
        "current_oi":      None,
    }
    coin    = hl_coin(symbol)
    hist    = state.get("oi_history", {}).get(coin, [])

    if len(hist) < 2:
        return default

    current = hist[-1]["oi"]
    oldest  = hist[0]["oi"]
    prev    = hist[-2]["oi"]

    if oldest == 0:
        return {**default, "current_oi": current}

    oi_change_pct = (current - oldest) / oldest * 100

    # Trend direction
    if oi_change_pct > 1.0:
        oi_trend = "Rising"
    elif oi_change_pct < -1.0:
        oi_trend = "Falling"
    else:
        oi_trend = "Flat"

    # Acceleration: rate of change of rate of change
    if len(hist) >= 3 and prev != 0 and oldest != 0:
        first_half  = (prev     - oldest)  / oldest  * 100
        second_half = (current  - prev)    / prev    * 100
        oi_accel    = second_half - first_half
    else:
        oi_accel = 0.0

    return {
        "oi_change_pct":   oi_change_pct,
        "oi_trend":        oi_trend,
        "oi_acceleration": oi_accel,
        "score_delta":     0,   # resolved in score_oi() with price context
        "label":           "OI↑" if oi_trend == "Rising" else ("OI↓" if oi_trend == "Falling" else "OI~"),
        "current_oi":      current,
    }


def score_oi_confirmation(oi_trend: str, price_rising: bool) -> int:
    """
    +1 = Strong Confirmation (OI up + price up, or OI up + price down)
     0 = Neutral
    -1 = Divergence (OI down while price moves — weak)

    Bullish Confirmation:  price↑ + OI↑  → +1
    Bearish Confirmation:  price↓ + OI↑  → +1
    Weak Breakout:         price↑ + OI↓  → -1
    Weak Breakdown:        price↓ + OI↓  → -1
    """
    oi_rising = oi_trend == "Rising"
    oi_falling = oi_trend == "Falling"
    if oi_rising:
        return +1
    if oi_falling:
        return -1
    return 0


# ═══════════════════════════════════════════════════════════════
# FUNDING RATE + MARKET CONTEXT
# ═══════════════════════════════════════════════════════════════

FUNDING_WARN_EXTREME = 0.0010
FUNDING_WARN_HIGH    = 0.0005

def fetch_all_funding() -> dict[str, float]:
    """
    Returns {coin: funding_rate} for all assets in one call.
    Reuses the same metaAndAssetCtxs batch as OI fetch.
    """
    try:
        payload = {"type": "metaAndAssetCtxs"}
        data    = hl_post(payload)
        meta       = data[0]
        asset_ctxs = data[1]
        universe   = meta.get("universe", [])
        result: dict[str, float] = {}
        for i, asset in enumerate(universe):
            if i >= len(asset_ctxs):
                break
            ctx  = asset_ctxs[i]
            coin = asset.get("name", "")
            f    = ctx.get("funding")
            if f is not None:
                result[coin] = float(f)
        return result
    except Exception as e:
        print(f"  [FUNDING FETCH] failed: {e}")
        return {}


def fetch_market_context_batch() -> dict[str, dict]:
    """
    Single metaAndAssetCtxs call returning funding + OI for ALL coins.
    Returns {coin: {"funding": float, "open_interest": float}}
    """
    try:
        payload = {"type": "metaAndAssetCtxs"}
        data    = hl_post(payload)
        meta       = data[0]
        asset_ctxs = data[1]
        universe   = meta.get("universe", [])
        result: dict[str, dict] = {}
        for i, asset in enumerate(universe):
            if i >= len(asset_ctxs):
                break
            ctx  = asset_ctxs[i]
            coin = asset.get("name", "")
            f    = ctx.get("funding")
            oi   = ctx.get("openInterest")
            mark = ctx.get("markPx")
            oi_usd = float(oi) * float(mark) if (oi is not None and mark is not None) else None
            result[coin] = {
                "funding":       float(f) if f is not None else None,
                "open_interest": oi_usd,
            }
        return result
    except Exception as e:
        print(f"  [MARKET CTX BATCH] failed: {e}")
        return {}


def get_market_context(symbol: str) -> dict | None:
    """Legacy single-symbol wrapper — used for backward compatibility."""
    coin = hl_coin(symbol)
    try:
        ctx_batch = fetch_market_context_batch()
        return ctx_batch.get(coin)
    except Exception as e:
        print(f"  [MARKET CTX] fetch failed for {coin}: {e}")
    return None


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


# ═══════════════════════════════════════════════════════════════
# SUPPORT / RESISTANCE
# ═══════════════════════════════════════════════════════════════

def find_sr_levels(candles_15m: list[dict], n_levels: int = 2) -> tuple[list[float], list[float]]:
    highs = [c["h"] for c in candles_15m]
    lows  = [c["l"] for c in candles_15m]
    cur   = candles_15m[-1]["c"]

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
# [P3] BTC MARKET REGIME FILTER
# ═══════════════════════════════════════════════════════════════

def compute_btc_regime() -> dict:
    """
    Compute BTC regime using 4H and 1H EMA21/50.
    Returns {"bull": bool, "bear": bool, "label": str}.
    Cached in _btc_regime for the scan run.
    """
    global _btc_regime
    now = time.time()
    if _btc_regime and now - _btc_regime.get("ts", 0) < 3600:
        return _btc_regime

    try:
        candles_4h = get_candles("BTCUSDT", "4h", N_4H)
        candles_1h = get_candles("BTCUSDT", "1h", N_1H)

        c4h = [c["c"] for c in candles_4h]
        c1h = [c["c"] for c in candles_1h]

        ef4h = safe(ema(c4h, FAST_LEN)[-2])
        es4h = safe(ema(c4h, SLOW_LEN)[-2])
        ef1h = safe(ema(c1h, FAST_LEN)[-2])
        es1h = safe(ema(c1h, SLOW_LEN)[-2])

        btc_4h_bull = ef4h > es4h
        btc_1h_bull = ef1h > es1h

        bull = btc_4h_bull and btc_1h_bull
        bear = (not btc_4h_bull) and (not btc_1h_bull)

        label = "Bullish" if bull else ("Bearish" if bear else "Mixed")
        print(f"  [BTC REGIME] 4H EMA21={ef4h:.2f}/EMA50={es4h:.2f}  "
              f"1H EMA21={ef1h:.2f}/EMA50={es1h:.2f} → {label}")

        _btc_regime = {"bull": bull, "bear": bear, "label": label, "ts": now}
    except Exception as e:
        print(f"  [BTC REGIME] error: {e}")
        _btc_regime = {"bull": True, "bear": False, "label": "Unknown", "ts": now}

    return _btc_regime


def check_btc_regime_filter(symbol: str, direction: str) -> tuple[bool, str]:
    """
    Returns (allowed, reason_string).
    BTCUSDT bypasses this filter entirely.
    """
    if symbol == "BTCUSDT":
        return True, ""

    regime = compute_btc_regime()
    label  = regime.get("label", "Unknown")

    if direction == "long" and regime.get("bear", False):
        return False, f"BTC Regime Bearish — LONG suppressed"
    if direction == "short" and regime.get("bull", False):
        return False, f"BTC Regime Bullish — SHORT suppressed"

    return True, label


# ═══════════════════════════════════════════════════════════════
# [P4] ECONOMIC CALENDAR FILTER
# ═══════════════════════════════════════════════════════════════

MACRO_EVENT_KEYWORDS = ["fomc", "cpi", "ppi", "nfp", "gdp", "interest rate",
                        "non-farm", "federal funds", "inflation", "employment"]

def fetch_econ_calendar() -> list[dict]:
    """
    Fetch upcoming economic events. Uses FMP free endpoint (no key needed
    for basic calendar). Falls back to empty list gracefully.
    Cached for ECON_CALENDAR_CACHE_SECS.
    """
    global _econ_cache
    now = time.time()
    if _econ_cache and now - _econ_cache.get("ts", 0) < ECON_CALENDAR_CACHE_SECS:
        return _econ_cache.get("events", [])

    events = []
    try:
        # FMP free calendar endpoint (no key required for basic data)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ahead = (datetime.now(timezone.utc) + timedelta(days=3)).strftime("%Y-%m-%d")
        url   = f"https://financialmodelingprep.com/api/v3/economic_calendar?from={today}&to={ahead}"
        if ECON_CALENDAR_API_KEY:
            url += f"&apikey={ECON_CALENDAR_API_KEY}"

        r = requests.get(url, timeout=10)
        r.raise_for_status()
        raw = r.json()

        for ev in raw:
            name = (ev.get("event") or "").lower()
            if any(kw in name for kw in MACRO_EVENT_KEYWORDS):
                date_str = ev.get("date", "")
                try:
                    if "T" in date_str:
                        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    else:
                        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    events.append({"name": ev.get("event", ""), "dt": dt})
                except Exception:
                    pass

        print(f"  [ECON CAL] Loaded {len(events)} macro events")
    except Exception as e:
        print(f"  [ECON CAL] fetch failed (non-fatal): {e}")

    _econ_cache = {"ts": now, "events": events}
    return events


def check_macro_risk(atr_pct: float) -> tuple[bool, str]:
    """
    Returns (in_risk_window, description).
    in_risk_window=True means macro risk is active.
    """
    events = fetch_econ_calendar()
    now    = datetime.now(timezone.utc)

    for ev in events:
        dt   = ev["dt"]
        diff = (dt - now).total_seconds() / 60  # minutes until event

        in_before = 0 <= diff <= MACRO_RISK_WINDOW_BEFORE
        in_after  = -MACRO_RISK_WINDOW_AFTER <= diff < 0

        if in_before or in_after:
            if diff >= 0:
                label = f"{ev['name']} in {int(diff)} min"
            else:
                label = f"{ev['name']} {int(-diff)} min ago"
            return True, label

    return False, ""


# ═══════════════════════════════════════════════════════════════
# [P5 + P6] NEWS RISK FILTER + SENTIMENT LAYER
# ═══════════════════════════════════════════════════════════════

def fetch_rss_headlines() -> list[dict]:
    """
    Fetch all RSS feeds in parallel. Returns list of {"title": str, "link": str}.
    Cached for NEWS_CACHE_SECS. Single batch fetch — no per-symbol calls.
    """
    global _news_cache
    now = time.time()
    if _news_cache and now - _news_cache.get("ts", 0) < NEWS_CACHE_SECS:
        return _news_cache.get("items", [])

    items: list[dict] = []

    def fetch_feed(url: str) -> list[dict]:
        try:
            r = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            root  = ET.fromstring(r.content)
            found = []
            for item in root.iter("item"):
                title = item.findtext("title") or ""
                link  = item.findtext("link")  or ""
                if title:
                    found.append({"title": title.lower(), "link": link})
            return found
        except Exception as e:
            print(f"  [RSS] {url[:40]} failed: {e}")
            return []

    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(fetch_feed, url) for url in RSS_FEEDS]
        for f in as_completed(futures):
            items.extend(f.result())

    print(f"  [NEWS] Fetched {len(items)} headlines from {len(RSS_FEEDS)} feeds")
    _news_cache = {"ts": now, "items": items}
    return items


def compute_sentiment_score(headlines: list[str]) -> float:
    """
    Compute sentiment_score from −100 to +100.
    Averages individual headline scores, clamped to range.
    """
    if not headlines:
        return 0.0
    total = 0.0
    for h in headlines:
        hl = h.lower()
        score = 0
        for keywords, val in SENTIMENT_KEYWORDS:
            if any(kw in hl for kw in keywords):
                score += val
        total += max(-100, min(100, score))
    return max(-100.0, min(100.0, total / len(headlines)))


def compute_news_scores(symbol: str, all_headlines: list[dict]) -> tuple[float, float, str]:
    """
    Returns (news_score, final_news_score, top_headline).

    news_score:       raw event-based score
    final_news_score: 0.7*news_score + 0.3*sentiment_score
    """
    coin = hl_coin(symbol)
    coin_kw = COIN_KEYWORDS.get(coin, [coin.lower()])

    # Filter headlines relevant to this coin
    relevant = [
        h["title"] for h in all_headlines
        if any(kw in h["title"] for kw in coin_kw)
    ]

    if not relevant:
        return 0.0, 0.0, ""

    # Event scoring (P5)
    news_score = 0.0
    top_headline = relevant[0] if relevant else ""
    for hl in relevant:
        for keywords, val in NEWS_EVENT_SCORES:
            if any(kw in hl for kw in keywords):
                news_score += val
                break  # one event per headline

    news_score = max(-100.0, min(100.0, news_score))

    # Sentiment scoring (P6)
    sentiment_score   = compute_sentiment_score(relevant)
    final_news_score  = news_score * 0.7 + sentiment_score * 0.3
    final_news_score  = max(-100.0, min(100.0, final_news_score))

    return news_score, final_news_score, top_headline[:80] if top_headline else ""


def apply_news_filter(final_news_score: float, score: int) -> tuple[int, bool, str]:
    """
    Returns (adjusted_score, suppressed, reason).
    """
    if final_news_score <= NEWS_SUPPRESS_THRESHOLD:
        return score, True, f"News suppressed ({final_news_score:.0f})"
    adjusted = score
    reason   = ""
    if final_news_score <= NEWS_REDUCE_THRESHOLD:
        adjusted -= 1
        reason = f"News −1 ({final_news_score:.0f})"
    elif final_news_score >= NEWS_BOOST_THRESHOLD:
        adjusted += 1
        reason = f"News +1 ({final_news_score:.0f})"
    return adjusted, False, reason


# ═══════════════════════════════════════════════════════════════
# [P7] RELATIVE STRENGTH RANKING
# ═══════════════════════════════════════════════════════════════

def compute_rs_scores(state: dict) -> dict[str, float]:
    """
    Compute 7-day return for each watchlist coin vs BTC.
    Cached in _rs_scores for the scan run. Uses daily candles already
    fetched (caller passes cached version; we fetch if not cached).
    Returns {symbol: rs_vs_btc_pct}.
    """
    global _rs_scores
    if _rs_scores:
        return _rs_scores

    returns: dict[str, float] = {}

    def fetch_return(sym: str) -> tuple[str, float]:
        try:
            candles = get_candles(sym, "1d", RS_LOOKBACK_BARS_1D + 2)
            if len(candles) < RS_LOOKBACK_BARS_1D:
                return sym, 0.0
            old   = candles[-(RS_LOOKBACK_BARS_1D + 1)]["c"]
            cur   = candles[-1]["c"]
            if old == 0:
                return sym, 0.0
            return sym, (cur - old) / old * 100
        except Exception:
            return sym, 0.0

    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(fetch_return, sym): sym for sym in WATCHLIST}
        for f in as_completed(futures):
            sym, ret = f.result()
            returns[sym] = ret

    btc_return = returns.get("BTCUSDT", 0.0)
    for sym in WATCHLIST:
        _rs_scores[sym] = returns.get(sym, 0.0) - btc_return

    print(f"  [RS] Computed relative strength for {len(_rs_scores)} symbols "
          f"(BTC 7d: {btc_return:+.2f}%)")
    return _rs_scores


def get_rs_score_delta(symbol: str, rs_scores: dict[str, float]) -> tuple[int, float]:
    """
    Returns (score_delta, rs_pct).
    Top 20% RS → +1, Bottom 20% → −1, otherwise 0.
    """
    all_rs = sorted(rs_scores.values())
    n      = len(all_rs)
    if n < 3:
        return 0, rs_scores.get(symbol, 0.0)

    bot_threshold = all_rs[max(0, int(n * RS_BOT_PERCENTILE))]
    top_threshold = all_rs[min(n - 1, int(n * (1 - RS_TOP_PERCENTILE)))]

    rs = rs_scores.get(symbol, 0.0)

    if rs >= top_threshold:
        return +1, rs
    if rs <= bot_threshold:
        return -1, rs
    return 0, rs


# ═══════════════════════════════════════════════════════════════
# [P2] HISTORICAL PERFORMANCE ANALYTICS
# ═══════════════════════════════════════════════════════════════

def record_signal_history(state: dict, symbol: str, direction: str,
                          signal_type: str, score: int,
                          funding_rate: float | None, atr_pct: float,
                          oi_change_pct: float, news_score: float,
                          final_news_score: float) -> str:
    """
    Store a new signal entry. result populated later by update_signal_result().
    Returns the entry's id.
    """
    hist = state.setdefault("signal_history", [])
    entry_id = f"{symbol}_{int(time.time())}"
    hist.append({
        "id":              entry_id,
        "symbol":          symbol,
        "direction":       direction,
        "signal_type":     signal_type,
        "score":           score,
        "funding_rate":    funding_rate,
        "atr_pct":         atr_pct,
        "oi_change_pct":   oi_change_pct,
        "news_score":      news_score,
        "final_news_score": final_news_score,
        "result":          None,
        "timestamp":       int(time.time()),
    })
    # Keep last 500 entries
    if len(hist) > 500:
        state["signal_history"] = hist[-500:]
    return entry_id


def update_signal_result(state: dict, signal_id: str, result: str) -> None:
    """
    Set result = "tp2" | "tp1" | "sl" for a stored signal history entry.
    """
    for entry in state.get("signal_history", []):
        if entry.get("id") == signal_id:
            entry["result"] = result
            return


def compute_win_rates(state: dict) -> dict:
    """
    Compute win rates by symbol, signal_type, score, direction.
    Returns nested dict of metrics (only for groups with >= HIST_MIN_SAMPLE).
    """
    hist     = [e for e in state.get("signal_history", []) if e.get("result") in ("tp1", "tp2", "sl")]
    wrs: dict = {"by_symbol": {}, "by_type": {}, "by_score": {}, "by_direction": {}}

    def wr_for(entries: list[dict]) -> tuple[float, int]:
        n    = len(entries)
        wins = sum(1 for e in entries if e["result"] in ("tp1", "tp2"))
        return (wins / n if n > 0 else 0.0), n

    # By symbol
    symbols = set(e["symbol"] for e in hist)
    for sym in symbols:
        subset       = [e for e in hist if e["symbol"] == sym]
        wr, n        = wr_for(subset)
        if n >= HIST_MIN_SAMPLE:
            wrs["by_symbol"][sym] = {"wr": wr, "n": n}

    # By signal type
    for st in ("BREAK", "PULL"):
        subset = [e for e in hist if e.get("signal_type") == st]
        wr, n  = wr_for(subset)
        if n >= HIST_MIN_SAMPLE:
            wrs["by_type"][st] = {"wr": wr, "n": n}

    # By score
    for sc in range(4, 8):
        subset = [e for e in hist if e.get("score") == sc]
        wr, n  = wr_for(subset)
        if n >= HIST_MIN_SAMPLE:
            wrs["by_score"][str(sc)] = {"wr": wr, "n": n}

    # By direction
    for d in ("long", "short"):
        subset = [e for e in hist if e.get("direction") == d]
        wr, n  = wr_for(subset)
        if n >= HIST_MIN_SAMPLE:
            wrs["by_direction"][d] = {"wr": wr, "n": n}

    return wrs


def get_historical_score_delta(state: dict, symbol: str,
                               signal_type: str, score: int,
                               direction: str) -> tuple[int, float, int]:
    """
    Returns (delta, win_rate, sample_size).
    Uses most specific available metric (symbol > signal_type > direction > score).
    """
    wrs = compute_win_rates(state)

    # Priority: symbol-level WR if available
    sym_data = wrs["by_symbol"].get(symbol)
    if sym_data:
        wr, n = sym_data["wr"], sym_data["n"]
        delta = +1 if wr >= HIST_BOOST_WR else (-1 if wr <= HIST_PENALISE_WR else 0)
        return delta, wr, n

    # Fallback: direction-level
    dir_data = wrs["by_direction"].get(direction)
    if dir_data:
        wr, n = dir_data["wr"], dir_data["n"]
        delta = +1 if wr >= HIST_BOOST_WR else (-1 if wr <= HIST_PENALISE_WR else 0)
        return delta, wr, n

    return 0, 0.0, 0


# ═══════════════════════════════════════════════════════════════
# INDICATOR MATH  (unchanged from v10)
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
        self.score       = 0
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
        # New v11 fields
        self.oi_trend:       str   = "Flat"
        self.oi_change_pct:  float = 0.0
        self.oi_score_delta: int   = 0
        self.vol_ratio:      float = 0.0
        self.btc_regime_label: str = ""
        self.news_score:     float = 0.0
        self.final_news_score: float = 0.0
        self.top_headline:   str   = ""
        self.sentiment_score: float = 0.0
        self.rs_pct:         float = 0.0
        self.rs_delta:       int   = 0
        self.hist_wr:        float = 0.0
        self.hist_n:         int   = 0
        self.hist_delta:     int   = 0
        self.macro_risk:     str   = ""
        self.macro_in_window: bool = False


def compute_signals(symbol: str, candles_15m, candles_1h, candles_4h, candles_d,
                    state: dict | None = None,
                    oi_state: dict | None = None,
                    rs_scores: dict | None = None,
                    all_headlines: list[dict] | None = None) -> SignalResult:
    """
    Core signal engine — extended with v11 overlays.
    state: full state dict (needed for historical analytics)
    oi_state: pre-computed OI trend data for this symbol
    rs_scores: pre-computed relative strength dict
    all_headlines: pre-fetched news headlines (shared across all symbols)
    """
    res   = SignalResult()
    state = state or {}

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

    # [P8] Volume ratio for display + BREAK check
    vol_ratio = (cur_v / vm15) if vm15 > 0 else 1.0
    vol_break_ok = vol_ratio >= BREAK_VOL_MULT   # only enforced for BREAK signals

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

    # [P1] Real OI trend (replaces 4H volume proxy)
    oi_data    = oi_state or get_oi_trend(state, symbol)
    oi_trend   = oi_data.get("oi_trend", "Flat")
    oi_change  = oi_data.get("oi_change_pct", 0.0)
    price_rising = cur_c > c15[-2] if len(c15) >= 2 else True
    oi_score_d = score_oi_confirmation(oi_trend, price_rising)
    oi_expanding = oi_score_d >= 0   # True = no penalty from OI

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

    # ── OI replaces old vol-proxy in score  ──────────────────
    # +1 for confirmation, 0 neutral, −1 divergence
    oi_score_contrib = oi_score_d > 0  # bool for sum() compat

    long_score = sum([
        bb_long,
        h4_bull and h4_trend_held_bull,
        adx_score_ok,
        obv_slope_long,
        vol_score_ok,
        oi_score_contrib,
    ])
    short_score = sum([
        bb_short,
        h4_bear and h4_trend_held_bear,
        adx_score_ok,
        obv_slope_short,
        vol_score_ok,
        oi_score_contrib,
    ])

    # Apply OI divergence penalty
    if oi_score_d < 0:
        long_score  = max(0, long_score  - 1)
        short_score = max(0, short_score - 1)

    # [P8] BREAK signals require higher volume
    long_break  = (macro_long  and daily_adx_ok and full_long_align  and break_bull_bar
                   and adx_break_ok and vwap_long  and rsi_long  and long_score  >= MIN_SCORE
                   and market_ok and vol_break_ok)
    short_break = (macro_short and daily_adx_ok and full_short_align and break_bear_bar
                   and adx_break_ok and vwap_short and rsi_short and short_score >= MIN_SCORE
                   and market_ok and vol_break_ok)

    # PULL signals retain original volume logic
    long_pull  = (macro_long  and daily_adx_ok and pull_long_align  and pull_touched_long
                  and pull_recover_long  and pull_bull_bar and vwap_long  and rsi_long
                  and long_score  >= MIN_SCORE and market_ok)
    short_pull = (macro_short and daily_adx_ok and pull_short_align and pull_touched_short
                  and pull_recover_short and pull_bear_bar and vwap_short and rsi_short
                  and short_score >= MIN_SCORE and market_ok)

    long_sig  = long_break  or long_pull
    short_sig = short_break or short_pull

    def oi_label() -> str:
        if oi_score_d > 0:  return "OI↑"
        if oi_score_d < 0:  return "OI Divergence"
        return "OI~"

    def make_breakdown(is_long):
        bb_s  = "BB✓"  if (bb_long  if is_long else bb_short)  else "BB✗"
        d_s   = "4H✓"  if (h4_bull and h4_trend_held_bull if is_long else h4_bear and h4_trend_held_bear) else "4H✗"
        adx_s = "ADX✓" if adx_score_ok  else "ADX✗"
        obv_s = "OBV✓" if (obv_slope_long if is_long else obv_slope_short) else "OBV✗"
        vol_s = "VOL✓" if vol_score_ok   else "VOL✗"
        oi_s  = oi_label()
        return f"{bb_s} {d_s} {adx_s} {obv_s} {vol_s} {oi_s}"

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
        res.breakdown    = make_breakdown(True)
        res.v10_gates    = make_gates(True)
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
        res.breakdown    = make_breakdown(False)
        res.v10_gates    = make_gates(False)

    # ── Store OI metadata on result ───────────────────────────
    res.oi_trend      = oi_trend
    res.oi_change_pct = oi_change
    res.oi_score_delta = oi_score_d
    res.vol_ratio     = vol_ratio

    # ── Support / Resistance ──────────────────────────────────
    if res.fire_long or res.fire_short:
        res.supports, res.resistances = find_sr_levels(candles_15m)

    # ── S/R clearance filter ──────────────────────────────────
    if SR_CLEARANCE_ATR_MULT > 0 and (res.fire_long or res.fire_short):
        min_clearance = atr_val * SR_CLEARANCE_ATR_MULT
        if res.fire_long and res.resistances:
            nearest_res = res.resistances[0]
            if nearest_res - res.entry < min_clearance:
                print(f"  [SR FILTER] {symbol} LONG suppressed — resistance too close")
                res.fire_long = False
        if res.fire_short and res.supports:
            nearest_sup = res.supports[0]
            if res.entry - nearest_sup < min_clearance:
                print(f"  [SR FILTER] {symbol} SHORT suppressed — support too close")
                res.fire_short = False

    return res


# ═══════════════════════════════════════════════════════════════
# COOLDOWN STATE
# ═══════════════════════════════════════════════════════════════

def load_state() -> dict:
    if Path(STATE_FILE).exists():
        try:
            s = json.loads(Path(STATE_FILE).read_text())
            # Backward-compatibility: initialise missing v11 fields
            s.setdefault("oi_history",      {})
            s.setdefault("signal_history",  [])
            s.setdefault("resolved_signals", [])
            s.setdefault("active_signals",  [])
            return s
        except Exception:
            pass
    return {
        "oi_history":      {},
        "signal_history":  [],
        "resolved_signals": [],
        "active_signals":  [],
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
    return "★" * score + "☆" * (6 - score)


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
    direction    = "long" if sig.fire_long else "short"
    rate         = sig.funding_rate or 0.0
    tailwind     = (rate < 0 and direction == "long") or (rate > 0 and direction == "short")
    is_break     = sig.signal_type == "BREAK"
    oi_pos       = sig.oi_score_delta >= 0
    rs_pos       = sig.rs_delta >= 0
    return (sig.score, int(is_break), int(tailwind), int(oi_pos), int(rs_pos))


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

    lev_range = recommended_leverage(sig.atr_pct, sig.score)

    # ── S/R lines ───────────────────────────────────────────────
    sr_lines = ""
    if sig.resistances:
        sr_lines += "🔴 Resistance: " + "  |  ".join(f"<code>{fmt(r)}</code>" for r in sig.resistances) + "\n"
    if sig.supports:
        sr_lines += "🟢 Support:    " + "  |  ".join(f"<code>{fmt(s)}</code>" for s in sig.supports) + "\n"

    # ── Pre-trade checklist ─────────────────────────────────────
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

    checklist = (
        f"\n<b>Pre-Trade Checklist</b>\n"
        f"{chk_trend} Trend identified (4H/1H/15m aligned)\n"
        f"{chk_sr} Key S/R marked\n"
        f"{chk_entry} Clear entry signal ({sig.signal_type}, score {sig.score}/6)\n"
        f"{chk_lev} Leverage appropriate ({lev_range})\n"
        f"{chk_funding} {funding_str}\n"
        f"📊 {format_oi(sig.open_interest)}"
    )

    sr_block = f"\n{sr_lines}" if sr_lines else ""

    medal    = RANK_MEDALS.get(rank, "")
    rank_tag = f"{medal} <b>Priority #{rank}</b>\n" if rank else ""

    # ── v11 analytics block ──────────────────────────────────────
    oi_str   = f"OI Trend: {sig.oi_trend} ({sig.oi_change_pct:+.1f}%)"
    vol_conf = "YES" if sig.vol_ratio >= BREAK_VOL_MULT else ("OK" if sig.signal_type == "PULL" else "NO")
    vol_str  = f"Volume Confirmed: {vol_conf}  |  Volume Ratio: {sig.vol_ratio:.1f}x"

    wr_str = ""
    if sig.hist_n >= HIST_MIN_SAMPLE:
        wr_str = f"\nWin Rate: {sig.hist_wr * 100:.0f}%  (n={sig.hist_n})"

    btc_str  = f"BTC Regime: {sig.btc_regime_label}" if sig.btc_regime_label else ""
    rs_str   = f"Relative Strength: {sig.rs_pct:+.1f}%" if sig.rs_pct != 0 else ""

    news_str = ""
    if abs(sig.final_news_score) >= 5:
        news_str  = f"\nNews Score: {sig.news_score:+.0f}  |  Sentiment: {sig.sentiment_score:+.0f}"
        if sig.top_headline:
            news_str += f"\nHeadline: <i>{sig.top_headline}</i>"

    macro_str = ""
    if sig.macro_in_window:
        macro_str = f"\n⚠️ Macro Event Nearby: {sig.macro_risk}"

    analytics = (
        f"\n<b>Analytics</b>\n"
        f"{oi_str}\n"
        f"{vol_str}"
        + (f"\n{wr_str}" if wr_str else "")
        + (f"\n{btc_str}" if btc_str else "")
        + (f"\n{rs_str}" if rs_str else "")
        + news_str
        + macro_str
    )

    return (
        f"{rank_tag}{emoji} <b>{direction} [{sig.signal_type}]</b>  {stars(sig.score)}\n"
        f"<b>Pair:</b>  {symbol}\n\n"
        f"<b>Entry:</b> <code>{fmt(sig.entry)}</code>\n"
        f"<b>Entry Zone:</b> <code>{fmt(sig.entry_low)}</code> – <code>{fmt(sig.entry_high)}</code>\n\n"
        f"<b>TP1:</b>   <code>{fmt(sig.tp1)}</code>\n"
        f"<b>TP2:</b>   <code>{fmt(sig.tp2)}</code>\n"
        f"<b>SL:</b>    <code>{fmt(sig.sl)}</code>\n\n"
        f"<b>Leverage:</b> {lev_range}\n"
        f"<b>Score:</b> {sig.score}/6  |  {sig.breakdown}\n"
        f"<b>Gates:</b> {sig.v10_gates}"
        f"{sr_block}"
        f"{checklist}"
        f"{analytics}\n\n"
        f"<i>Scalp Swing v11 [4H/15m] • Hyperliquid Perps • {ts}</i>"
    )


# ═══════════════════════════════════════════════════════════════
# ACTIVE SIGNAL TRACKING  (TP / SL reactions)
# ═══════════════════════════════════════════════════════════════

def track_signal(state: dict, symbol: str, direction: str,
                 msg_id: int, sig: SignalResult, bar_index: int,
                 hist_id: str | None = None):
    """Persist a newly fired signal for TP/SL tracking."""
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
        "hist_id":         hist_id,   # v11: link to signal_history entry
        "signal_type":     sig.signal_type,
        "score":           sig.score,
        "funding_rate":    sig.funding_rate,
        "atr_pct":         sig.atr_pct,
        "oi_change_pct":   sig.oi_change_pct,
        "news_score":      sig.news_score,
        "final_news_score": sig.final_news_score,
    })


def check_active_signals(state: dict, bar_index_now: int):
    """
    Walk active signals oldest→newest per 15m candle to resolve TP/SL.
    Results written back to signal_history for P2 analytics.
    """
    still_active = []
    for sig in state.get("active_signals", []):
        symbol    = sig["symbol"]
        direction = sig["direction"]
        msg_id    = sig["msg_id"]
        tp1       = sig["tp1"]
        tp2       = sig["tp2"]
        sl        = sig["sl"]
        tp1_hit   = sig.get("tp1_hit", False)
        hist_id   = sig.get("hist_id")

        age_bars  = bar_index_now - sig.get("bar_index", bar_index_now)
        if age_bars > SIGNAL_MAX_AGE_BARS:
            print(f"  [TRACK] {symbol} expired after {age_bars} bars")
            if hist_id:
                update_signal_result(state, hist_id, "sl")
            state.setdefault("resolved_signals", []).append({
                "symbol":      symbol,
                "direction":   direction,
                "outcome":     "sl",
                "resolved_at": int(time.time()),
            })
            continue

        try:
            start_ms  = sig.get("signal_bar_time")
            candles   = get_candles(symbol, "15m", SIGNAL_MAX_AGE_BARS + 4, start_time_ms=start_ms)
        except Exception as e:
            print(f"  [TRACK] candle fetch failed for {symbol}: {e}")
            still_active.append(sig)
            continue

        for c in candles:
            c_low  = c["l"]
            c_high = c["h"]

            if direction == "long":
                if not tp1_hit and c_low <= sl:
                    react_to_message(msg_id, REACT_SL)
                    print(f"  [TRACK] {symbol} SL hit → {REACT_SL}")
                    if hist_id:
                        update_signal_result(state, hist_id, "sl")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "sl",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break
                if not tp1_hit and c_high >= tp1:
                    react_to_message(msg_id, REACT_TP1)
                    print(f"  [TRACK] {symbol} TP1 hit → {REACT_TP1}")
                    sig["tp1_hit"] = True
                    tp1_hit = True
                if tp1_hit and c_high >= tp2:
                    react_to_message(msg_id, REACT_TP2)
                    print(f"  [TRACK] {symbol} TP2 hit → {REACT_TP2}")
                    if hist_id:
                        update_signal_result(state, hist_id, "tp2")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "tp2",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break
                if tp1_hit and not sig.get("resolved", False) and c_low <= sl:
                    print(f"  [TRACK] {symbol} SL after TP1 — resolving silently")
                    if hist_id:
                        update_signal_result(state, hist_id, "tp1")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "tp1",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break

            else:  # short
                if not tp1_hit and c_high >= sl:
                    react_to_message(msg_id, REACT_SL)
                    print(f"  [TRACK] {symbol} SL hit → {REACT_SL}")
                    if hist_id:
                        update_signal_result(state, hist_id, "sl")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "sl",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break
                if not tp1_hit and c_low <= tp1:
                    react_to_message(msg_id, REACT_TP1)
                    print(f"  [TRACK] {symbol} TP1 hit → {REACT_TP1}")
                    sig["tp1_hit"] = True
                    tp1_hit = True
                if tp1_hit and c_low <= tp2:
                    react_to_message(msg_id, REACT_TP2)
                    print(f"  [TRACK] {symbol} TP2 hit → {REACT_TP2}")
                    if hist_id:
                        update_signal_result(state, hist_id, "tp2")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "tp2",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
                    break
                if tp1_hit and not sig.get("resolved", False) and c_high >= sl:
                    print(f"  [TRACK] {symbol} SL after TP1 — resolving silently")
                    if hist_id:
                        update_signal_result(state, hist_id, "tp1")
                    state.setdefault("resolved_signals", []).append({
                        "symbol":      symbol,
                        "direction":   direction,
                        "outcome":     "tp1",
                        "resolved_at": int(time.time()),
                    })
                    sig["resolved"] = True
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

    # [P2] Win rate breakdown
    total = winners + losers
    wr_pct = int(winners / total * 100) if total > 0 else 0

    line1 = "📊 Signal Summary (last 24h)"
    line2 = f"✅ Winners: {winners} (🔥×{tp1_count}  🏆×{tp2_count})"
    line3 = f"❌ Losers:  {losers} (😭×{sl_count})"
    line4 = f"📈 Win Rate: {wr_pct}%  (n={total})"
    text  = "\n".join([line1, line2, line3, line4])

    send_telegram(text)

    state["resolved_signals"] = [
        e for e in state.get("resolved_signals", [])
        if e["resolved_at"] >= cutoff_48h
    ]


# ═══════════════════════════════════════════════════════════════
# SCAN
# ═══════════════════════════════════════════════════════════════

def scan_symbol(symbol: str, state: dict, bar_index_now: int,
                market_ctx_batch: dict,
                oi_snapshot: dict,
                rs_scores: dict,
                all_headlines: list[dict]) -> list[tuple]:
    """
    Scan a single symbol. All batch data pre-fetched in main().
    Returns list of (symbol, formatted_msg, direction, sig).
    """
    coin = hl_coin(symbol)
    data = fetch_all_candles(symbol)
    if data is None:
        print(f"    Skipping {coin}: insufficient candles")
        return []

    candles_15m, candles_1h, candles_4h, candles_d = data

    # Pre-compute OI trend from state for this symbol
    oi_data = get_oi_trend(state, symbol)

    sig = compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d,
                          state=state, oi_state=oi_data,
                          rs_scores=rs_scores, all_headlines=all_headlines)

    if not (sig.fire_long or sig.fire_short):
        print(f"    {coin}: no signal")
        return []

    direction = "long" if sig.fire_long else "short"
    if not check_cooldown(state, symbol, direction, bar_index_now):
        print(f"    {coin} signal suppressed by cooldown")
        return []

    print(f"    🚀 SIGNAL: {coin} {direction.upper()} [{sig.signal_type}] score={sig.score}")

    # ── Attach funding + OI from batch ────────────────────────
    ctx = market_ctx_batch.get(coin, {})
    if ctx:
        sig.funding_rate  = ctx.get("funding")
        sig.open_interest = ctx.get("open_interest")

    # ── [P3] BTC Regime filter ─────────────────────────────────
    allowed, regime_reason = check_btc_regime_filter(symbol, direction)
    regime = compute_btc_regime()
    sig.btc_regime_label = regime.get("label", "")
    if not allowed:
        print(f"    [{coin}] suppressed: {regime_reason}")
        return []

    # ── Funding suppression ────────────────────────────────────
    if FUNDING_SUPPRESS_EXTREME is not None and sig.funding_rate is not None:
        rate     = sig.funding_rate
        headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
        if headwind and abs(rate) >= FUNDING_SUPPRESS_EXTREME:
            pct = rate * 100
            print(f"    [FUNDING FILTER] {coin} {direction.upper()} suppressed — {pct:+.4f}%/8h")
            return []

    # ── [P4] Macro risk ────────────────────────────────────────
    in_macro, macro_desc = check_macro_risk(sig.atr_pct)
    sig.macro_in_window  = in_macro
    sig.macro_risk       = macro_desc
    if in_macro:
        if sig.atr_pct > MACRO_ATR_SUPPRESS_PCT:
            print(f"    [{coin}] suppressed: macro event + elevated ATR ({sig.atr_pct:.1f}%)")
            return []
        # Score reduction
        sig.score = max(0, sig.score - 1)
        print(f"    [{coin}] macro risk: score → {sig.score}  ({macro_desc})")

    # ── [P5 + P6] News + sentiment ──────────────────────────────
    news_score, final_news_score, top_headline = compute_news_scores(symbol, all_headlines)
    sig.news_score        = news_score
    sig.final_news_score  = final_news_score
    sig.top_headline      = top_headline
    # Sentiment score (standalone)
    coin_kw = COIN_KEYWORDS.get(coin, [coin.lower()])
    rel_headlines = [h["title"] for h in all_headlines if any(kw in h["title"] for kw in coin_kw)]
    sig.sentiment_score = compute_sentiment_score(rel_headlines)

    score_adj, suppressed, news_reason = apply_news_filter(final_news_score, sig.score)
    if suppressed:
        print(f"    [{coin}] suppressed by news: {news_reason}")
        return []
    if score_adj != sig.score:
        print(f"    [{coin}] news adjustment: score {sig.score} → {score_adj}  ({news_reason})")
    sig.score = score_adj

    # Final score gate (after all adjustments)
    if sig.score < MIN_SCORE:
        print(f"    [{coin}] score {sig.score} < MIN_SCORE after adjustments")
        return []

    # ── [P7] Relative strength ──────────────────────────────────
    rs_delta, rs_pct = get_rs_score_delta(symbol, rs_scores)
    sig.rs_delta = rs_delta
    sig.rs_pct   = rs_pct
    sig.score    = max(0, sig.score + rs_delta)

    # ── [P2] Historical analytics ───────────────────────────────
    hist_delta, hist_wr, hist_n = get_historical_score_delta(
        state, symbol, sig.signal_type, sig.score, direction
    )
    sig.hist_delta = hist_delta
    sig.hist_wr    = hist_wr
    sig.hist_n     = hist_n
    sig.score      = max(0, sig.score + hist_delta)

    # Final gate after all score adjustments
    if sig.score < MIN_SCORE:
        print(f"    [{coin}] final score {sig.score} < MIN_SCORE (analytics adjustment)")
        return []

    print(f"    ✅ {coin} {direction.upper()} final score={sig.score} "
          f"OI:{sig.oi_trend} RS:{sig.rs_pct:+.1f}% vol:{sig.vol_ratio:.1f}x")

    return [(symbol, format_signal(symbol, sig, "CORE"), direction, sig)]


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Scanner v11 starting…")
    print(f"Watchlist ({len(WATCHLIST)} pairs): {[hl_coin(s) for s in WATCHLIST]}")

    bar_index_now = int(time.time() // (15 * 60))
    state         = load_state()

    # ── Step 1: Check pending signals for TP/SL reactions ────
    print("[TRACK] Checking active signals…")
    check_active_signals(state, bar_index_now)
    save_state(state)

    # ── Step 1.5: Daily summary ───────────────────────────────
    if should_send_summary():
        send_summary(state)
        save_state(state)

    # ── Step 2: Batch pre-fetches (single call each) ──────────
    print("[BATCH] Fetching market context (funding + OI)…")
    market_ctx_batch = fetch_market_context_batch()

    # Update OI history in state from batch snapshot
    oi_snapshot = {coin: ctx["open_interest"]
                   for coin, ctx in market_ctx_batch.items()
                   if ctx.get("open_interest") is not None}
    update_oi_history(state, oi_snapshot)
    print(f"  [OI] Updated history for {len(oi_snapshot)} coins")

    # BTC regime (cached after first call)
    print("[BATCH] Computing BTC regime…")
    compute_btc_regime()

    # News + sentiment (single shared RSS batch)
    print("[BATCH] Fetching news headlines…")
    all_headlines = fetch_rss_headlines()

    # Economic calendar (cached 4h)
    print("[BATCH] Fetching economic calendar…")
    fetch_econ_calendar()

    # Relative strength (single pass, all symbols)
    print("[BATCH] Computing relative strength…")
    rs_scores = compute_rs_scores(state)

    # ── Step 3: Scan all symbols in parallel ──────────────────
    pending_signals: list[tuple] = []
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {
            ex.submit(
                scan_symbol, sym, state, bar_index_now,
                market_ctx_batch, oi_snapshot, rs_scores, all_headlines
            ): sym
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

    # ── Step 4: Rank, cap, send alerts ────────────────────────
    pending_signals.sort(key=lambda t: priority_score(t[3]), reverse=True)

    top_signals     = pending_signals[:MAX_SIGNALS_PER_SCAN]
    dropped_signals = pending_signals[MAX_SIGNALS_PER_SCAN:]

    if dropped_signals:
        dropped_names = [f"{hl_coin(s)} {d.upper()}" for s, _, d, _ in dropped_signals]
        print(f"  [RANK] Dropped {len(dropped_signals)} lower-priority: {', '.join(dropped_names)}")

    signals_fired = 0
    for rank, (symbol, _msg, direction, sig) in enumerate(top_signals, start=1):
        msg    = format_signal(symbol, sig, "CORE", rank=rank)
        msg_id = send_telegram(msg)
        update_cooldown(state, symbol, direction, bar_index_now)

        # [P2] Record to signal history
        hist_id = record_signal_history(
            state, symbol, direction, sig.signal_type, sig.score,
            sig.funding_rate, sig.atr_pct, sig.oi_change_pct,
            sig.news_score, sig.final_news_score
        )

        if msg_id:
            track_signal(state, symbol, direction, msg_id, sig, bar_index_now, hist_id=hist_id)
            print(f"  [TRACK] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"TP1={sig.tp1:.4f} TP2={sig.tp2:.4f} SL={sig.sl:.4f}")
        signals_fired += 1
        time.sleep(0.5)

    for symbol, _msg, direction, sig in dropped_signals:
        update_cooldown(state, symbol, direction, bar_index_now)
        print(f"  [RANK] cooldown applied to dropped: {hl_coin(symbol)} {direction.upper()}")

    save_state(state)
    print(f"Scan complete. {signals_fired} signal(s) fired.")


if __name__ == "__main__":
    main()
