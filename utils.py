# utils.py
from __future__ import annotations
import math

def safe(v, fallback=0.0):
    try:
        if v is None:
            return fallback
        if isinstance(v, (int, float)) and math.isnan(v):
            return fallback
        return v
    except (TypeError, ValueError):
        return fallback

def sma(values, period):
    out = [float("nan")] * len(values)
    for i in range(period - 1, len(values)):
        out[i] = sum(values[i - period + 1: i + 1]) / period
    return out

def atr(highs, lows, closes, period):
    trs = [float("nan")]
    for i in range(1, len(closes)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
    result = [float("nan")] * len(closes)
    if len(trs) < period + 1:
        return result
    result[period] = sum(trs[1:period + 1]) / period
    for i in range(period + 1, len(trs)):
        result[i] = (result[i - 1] * (period - 1) + trs[i]) / period
    return result

def _cluster_levels(pivots, atr_val, tolerance=0.3):
    if not pivots or atr_val <= 0:
        return pivots
    zones, members = [], []
    for p in sorted(pivots):
        if zones and abs(p - zones[-1]) < atr_val * tolerance:
            members[-1].append(p)
            m = members[-1]
            zones[-1] = sorted(m)[len(m) // 2]
        else:
            zones.append(p)
            members.append([p])
    return zones
