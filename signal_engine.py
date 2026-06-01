"""
Scalp Swing Bot v10 – Python Signal Engine (Pine-aligned rewrite)
Hyperliquid Perpetuals edition

Timeframe map:
- 4H bias
- 1H middle
- 15m execution

Runs every 15 minutes via GitHub Actions
Signal-only (Telegram alerts)
"""

import os, json, time, math, requests
from datetime import datetime, timezone
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
TG_CHAT_ID   = os.environ["TG_CHAT_ID"]
STATE_FILE   = "state.json"

WATCHLIST = [
    "BTCUSDT","ETHUSDT","HYPEUSDT","ZECUSDT","NEARUSDT","ONDOUSDT",
    "SUIUSDT","PUMPUSDT","PENGUUSDT","BNBUSDT","SOLUSDT","TRXUSDT",
    "TONUSDT","DOGEUSDT","ADAUSDT","DOTUSDT","TAOUSDT","AVAXUSDT",
    "LINKUSDT","AAVEUSDT","XRPUSDT","XLMUSDT",
]

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

MIN_SCORE       = 4
TP1_MULT        = 1.0
TP2_MULT        = 1.5
SL_MULT         = 1.0

COOLDOWN_BARS   = 32
GLOBAL_COOLDOWN = 16

ADX_BREAK_GATE  = 20.0
ADX_SCORE_MIN   = 20.0

RSI_LONG_MIN, RSI_LONG_MAX   = 45.0, 65.0
RSI_SHORT_MIN, RSI_SHORT_MAX = 35.0, 55.0

VOL_SCORE_MULT  = 1.0
MAX_ATR_PCT     = 10.0
MIN_ATR_PCT     = 0.2

WICK_FILTER     = 0.45
RANGE_PCT_BREAK = 0.30
PULL_ZONE_MULT  = 0.25

TREND_HOLD_BARS = 2

USE_ROLLING_VWAP = True
ROLLING_VWAP_LEN = 16

USE_D200_FILTER  = True
USE_DAILY_ADX    = True
MIN_DAILY_ADX    = 20.0

PULL_REQUIRES_4H = True

HL_INFO_URL = "https://api.hyperliquid.xyz/info"

INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}

# ─────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────

def hl_coin(symbol):
    return symbol.replace("USDT", "")


def hl_post(payload):
    for i in range(3):
        try:
            r = requests.post(HL_INFO_URL, json=payload, timeout=15)
            r.raise_for_status()
            return r.json()
        except:
            time.sleep(2 ** i)
    raise RuntimeError("Hyperliquid request failed")


def get_candles(symbol, interval, n):
    coin = hl_coin(symbol)
    iv   = INTERVAL_MS[interval]

    end   = int(time.time() * 1000)
    start = end - iv * (n + 20)

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": interval,
            "startTime": start,
            "endTime": end
        }
    }

    raw = hl_post(payload)

    candles = [{
        "t": int(c["t"]),
        "o": float(c["o"]),
        "h": float(c["h"]),
        "l": float(c["l"]),
        "c": float(c["c"]),
        "v": float(c["v"]),
    } for c in raw]

    candles.sort(key=lambda x: x["t"])

    # Pine-equivalent: drop forming candle
    if len(candles) > 2:
        candles = candles[:-1]

    return candles[-n:]


# ─────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────

def ema(values, period):
    k = 2 / (period + 1)
    out = [float("nan")] * len(values)
    if len(values) < period:
        return out
    out[period-1] = sum(values[:period]) / period
    for i in range(period, len(values)):
        out[i] = values[i]*k + out[i-1]*(1-k)
    return out


def sma(values, period):
    out = [float("nan")] * len(values)
    for i in range(period-1, len(values)):
        out[i] = sum(values[i-period+1:i+1]) / period
    return out


def rsi(closes, period):
    out = [float("nan")] * len(closes)
    gains, losses = [], []

    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))

    if len(gains) < period:
        return out

    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_g = (avg_g*(period-1) + gains[i]) / period
        avg_l = (avg_l*(period-1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l else float("inf")
        out[i] = 100 - 100/(1+rs)

    return out


def atr(h, l, c, period):
    tr = [0]*len(c)
    for i in range(1, len(c)):
        tr[i] = max(
            h[i]-l[i],
            abs(h[i]-c[i-1]),
            abs(l[i]-c[i-1])
        )

    out = [float("nan")] * len(c)
    if len(c) < period:
        return out

    out[period] = sum(tr[1:period+1]) / period

    for i in range(period+1, len(c)):
        out[i] = (out[i-1]*(period-1) + tr[i]) / period

    return out


def obv(c, v):
    out = [0]*len(c)
    for i in range(1, len(c)):
        if c[i] > c[i-1]:
            out[i] = out[i-1] + v[i]
        elif c[i] < c[i-1]:
            out[i] = out[i-1] - v[i]
        else:
            out[i] = out[i-1]
    return out


def bollinger(c, period, mult):
    b,u,l = [float("nan")]*len(c), [float("nan")]*len(c), [float("nan")]*len(c)
    for i in range(period-1, len(c)):
        w = c[i-period+1:i+1]
        m = sum(w)/period
        sd = math.sqrt(sum((x-m)**2 for x in w)/period)
        b[i], u[i], l[i] = m, m+mult*sd, m-mult*sd
    return b,u,l


# ─────────────────────────────────────────────
# SIGNAL ENGINE
# ─────────────────────────────────────────────

def compute(symbol, c15, c1h, c4h, cd):
    def unpack(x):
        return [i["o"] for i in x], [i["h"] for i in x], [i["l"] for i in x], [i["c"] for i in x], [i["v"] for i in x]

    o15,h15,l15,c15,v15 = unpack(c15)
    o1h,h1h,l1h,c1h,v1h = unpack(c1h)
    o4h,h4h,l4h,c4h,v4h = unpack(c4h)
    _,_,_,cd_c,_ = unpack(cd)

    # ── 15m
    ef15 = ema(c15, FAST_LEN)[-1]
    es15 = ema(c15, SLOW_LEN)[-1]
    r15  = rsi(c15, RSI_LEN)[-1]
    a15  = atr(h15,l15,c15,ATR_LEN)[-1]
    bb,_u,_l = bollinger(c15, BB_LEN, BB_MULT)
    bb_basis = bb[-1]
    obv15 = obv(c15,v15)

    # ── 1h
    ef1h = ema(c1h, FAST_LEN)[-1]
    es1h = ema(c1h, SLOW_LEN)[-1]
    h1_bull = ef1h > es1h
    h1_bear = ef1h < es1h

    # ── 4h
    ef4h = ema(c4h, FAST_LEN)[-1]
    es4h = ema(c4h, SLOW_LEN)[-1]
    d_bull = ef4h > es4h
    d_bear = ef4h < es4h

    def held(fast, slow):
        return all(fast[-i] > slow[-i] for i in range(1, TREND_HOLD_BARS+1))

    d_hold_b = held(ema(c4h, FAST_LEN), ema(c4h, SLOW_LEN))
    d_hold_s = held(ema(c4h, SLOW_LEN), ema(c4h, FAST_LEN))

    # ── daily
    d200 = ema(cd_c, TREND_LEN)[-1]
    macro_long  = c15[-1] > d200
    macro_short = c15[-1] < d200

    # ── scores
    bb_long  = c15[-1] > bb_basis
    bb_short = c15[-1] < bb_basis

    obv_long  = obv15[-1] > obv15[-OBV_LEN]
    obv_short = obv15[-1] < obv15[-OBV_LEN]

    vol_ok = True

    long_score = sum([bb_long, d_bull and d_hold_b, obv_long, vol_ok])
    short_score = sum([bb_short, d_bear and d_hold_s, obv_short, vol_ok])

    full_long = macro_long and d_bull and h1_bull
    full_short = macro_short and d_bear and h1_bear

    long = full_long and long_score >= MIN_SCORE
    short = full_short and short_score >= MIN_SCORE

    entry = c15[-1]
    atr_v = a15

    if long:
        return True, False, entry, entry+atr_v*TP1_MULT, entry+atr_v*TP2_MULT, entry-atr_v*SL_MULT, long_score
    if short:
        return False, True, entry, entry-atr_v*TP1_MULT, entry-atr_v*TP2_MULT, entry+atr_v*SL_MULT, short_score

    return False, False, 0,0,0,0,0


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("scanner running")

    for symbol in WATCHLIST:
        c15 = get_candles(symbol,"15m",300)
        c1h = get_candles(symbol,"1h",300)
        c4h = get_candles(symbol,"4h",300)
        cd  = get_candles(symbol,"1d",300)

        long, short, entry,tp1,tp2,sl,score = compute(symbol,c15,c1h,c4h,cd)

        if long or short:
            print(symbol, "LONG" if long else "SHORT", score)


if __name__ == "__main__":
    main()
