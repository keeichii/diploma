"""
Чистые функции для CAR, BHAR и вспомогательных метрик (тестируемые без API).
"""
from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd


def car_sum(ar: Iterable[float] | pd.Series) -> float:
    """CAR(t1,t2) = сумма AR на окне (уже отфильтрованном)."""
    s = pd.Series(list(ar) if not isinstance(ar, pd.Series) else ar, dtype=float)
    s = s.dropna()
    if s.empty:
        return float("nan")
    return float(s.sum())


def bhar_from_simple_returns(ri: pd.Series, rm: pd.Series) -> float:
    """
    Buy-and-Hold Abnormal Return (BHAR) in "excess wealth" units:

        BHAR_i(T) = Π_{t=1..T}(1 + R_i,t) - Π_{t=1..T}(1 + R_m,t)

    where:
      - ri: simple stock returns (not log returns)
      - rm: simple benchmark returns (not log returns)

    Interpretation:
      - value > 0: strategy "buy stock at t=1 and hold to T" created
        more terminal wealth than benchmark buy-and-hold.
      - value < 0: underperformance vs benchmark wealth path.

    Note: this is NOT (Π(1+Ri)/Π(1+Rm) - 1). We intentionally use
    the difference of terminal wealth multipliers across all modules.
    """
    df = pd.DataFrame({"ri": ri.astype(float), "rm": rm.astype(float)}).dropna()
    if df.empty:
        return float("nan")
    pi_i = float(np.prod(1.0 + df["ri"].to_numpy()))
    pi_m = float(np.prod(1.0 + df["rm"].to_numpy()))
    return pi_i - pi_m


def market_cap_bln(close_rub: float, shares_mln: float) -> float:
    """MarketCap (млрд руб) = Close (руб/акцию) * Shares (млн) / 1000."""
    if close_rub is None or shares_mln is None or (isinstance(close_rub, float) and math.isnan(close_rub)):
        return float("nan")
    return float(close_rub) * float(shares_mln) / 1000.0


def pb_ratio(market_cap_bln_val: float, equity_bln: float) -> float:
    if equity_bln is None or equity_bln == 0 or (isinstance(equity_bln, float) and math.isnan(equity_bln)):
        return float("nan")
    return float(market_cap_bln_val) / float(equity_bln)


def pe_ratio_close_eps(close: float, eps: float) -> float:
    if eps is None or eps == 0 or (isinstance(eps, float) and math.isnan(eps)):
        return float("nan")
    return float(close) / float(eps)


def volume_bln_rub_typical_ohlc_vol(
    o: float, h: float, low: float, c: float, volume_shares: float
) -> float:
    typical = (float(o) + float(h) + float(low) + float(c)) / 4.0
    return typical * float(volume_shares) / 1e9


def bhar_post_window(
    sub: pd.DataFrame,
    *,
    ri_col: str,
    rm_col: str,
    horizon: int,
    min_fraction_valid: float = 0.8,
    min_tail_beyond_horizon: int = 0,
) -> tuple[float, str]:
    """
    BHAR on post-event window t in [1..T] with robust coverage checks.

    The function is intentionally tolerant to small gaps in integer t:
    it does NOT require exact contiguous sequence 1..T. Instead it:
      1) keeps rows with 1 <= t <= T,
      2) de-duplicates by t (last observation wins),
      3) drops rows where either ri or rm is NaN,
      4) computes BHAR on available aligned pairs if enough valid rows.

    Returns:
      (bhar_value, why)
      - bhar_value is NaN on failure
      - why is "" on success, otherwise an explicit failure reason:
        "empty", "short_post_window", "missing_benchmark", "missing_stock_return",
        "no_valid_pairs", "too_few_valid_returns"
    """
    if sub.empty or horizon < 1:
        return float("nan"), "empty"
    g = sub.sort_values("t")
    max_t = pd.to_numeric(g["t"], errors="coerce").max()
    if pd.isna(max_t) or max_t < horizon + min_tail_beyond_horizon:
        return float("nan"), "short_post_window"

    win = g.loc[g["t"].between(1, horizon)].drop_duplicates("t", keep="last").sort_values("t")
    if win.empty:
        return float("nan"), "short_post_window"

    if rm_col not in win.columns or win[rm_col].notna().sum() == 0:
        return float("nan"), "missing_benchmark"
    if ri_col not in win.columns or win[ri_col].notna().sum() == 0:
        return float("nan"), "missing_stock_return"

    ok_mask = win[ri_col].notna() & win[rm_col].notna()
    valid_n = int(ok_mask.sum())
    if valid_n == 0:
        return float("nan"), "no_valid_pairs"

    min_obs = max(1, int(math.ceil(horizon * float(min_fraction_valid))))
    if valid_n < min_obs:
        return float("nan"), "too_few_valid_returns"

    ri = win.loc[ok_mask, ri_col].astype(float)
    rm = win.loc[ok_mask, rm_col].astype(float)
    return bhar_from_simple_returns(ri, rm), ""
