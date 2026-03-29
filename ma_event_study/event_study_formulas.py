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
    BHAR = Π(1+R_i) - Π(1+R_m) на выровненных по индексу рядах (один торговый период на шаг).
    ri, rm — простые доходности (не лог).
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
    min_tail_beyond_horizon: int = 10,
) -> tuple[float, str]:
    """
    BHAR на t=1..T (торговые дни в колонке t).
    Требуется max(t) >= T + min_tail_beyond_horizon и достаточная доля валидных наблюдений.
    Возвращает (значение, причина пропуска: '' если ок).
    """
    if sub.empty or horizon < 1:
        return float("nan"), "empty"
    g = sub.sort_values("t")
    if g["t"].max() < horizon + min_tail_beyond_horizon:
        return float("nan"), "insufficient_bhar_data"
    win = g.loc[g["t"].between(1, horizon)].drop_duplicates("t", keep="last").sort_values("t")
    if len(win) != horizon or not win["t"].tolist() == list(range(1, horizon + 1)):
        return float("nan"), "insufficient_bhar_data"
    ok_mask = win[ri_col].notna() & win[rm_col].notna()
    if ok_mask.sum() < max(1, int(math.ceil(horizon * min_fraction_valid))):
        return float("nan"), "insufficient_bhar_data"
    ri = win[ri_col].astype(float)
    rm = win[rm_col].astype(float)
    if ri.isna().any() or rm.isna().any():
        return float("nan"), "insufficient_bhar_data"
    return bhar_from_simple_returns(ri, rm), ""
