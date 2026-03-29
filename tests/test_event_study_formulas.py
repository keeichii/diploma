from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ma_event_study.event_study_formulas import (
    bhar_from_simple_returns,
    car_sum,
    market_cap_bln,
    bhar_post_window,
    pb_ratio,
    pe_ratio_close_eps,
    volume_bln_rub_typical_ohlc_vol,
)


def test_car_sum():
    assert car_sum([0.01, -0.02, 0.005]) == pytest.approx(-0.005)


def test_bhar_from_simple_returns():
    ri = pd.Series([0.10, -0.05])
    rm = pd.Series([0.02, 0.01])
    # (1.1*0.95) - (1.02*1.01)
    exp = 1.045 - 1.0302
    assert bhar_from_simple_returns(ri, rm) == pytest.approx(exp)


def test_market_cap_bln():
    assert market_cap_bln(100.0, 50.0) == pytest.approx(5.0)


def test_pb_pe():
    assert pb_ratio(10.0, 2.0) == 5.0
    assert np.isnan(pb_ratio(10.0, 0.0))
    assert pe_ratio_close_eps(200.0, 10.0) == 20.0


def test_volume_bln():
    v = volume_bln_rub_typical_ohlc_vol(10, 12, 8, 10, 1_000_000)
    assert v > 0


def test_bhar_post_window_ok():
    rows = [{"t": i, "ri": 0.01, "rm": 0.0} for i in range(0, 80)]
    sub = pd.DataFrame(rows)
    val, why = bhar_post_window(sub, ri_col="ri", rm_col="rm", horizon=5, min_tail_beyond_horizon=10)
    assert why == ""
    assert val > 0


def test_bhar_post_window_short_panel():
    sub = pd.DataFrame([{"t": i, "ri": 0.01, "rm": 0.0} for i in range(0, 10)])
    val, why = bhar_post_window(sub, ri_col="ri", rm_col="rm", horizon=60, min_tail_beyond_horizon=10)
    assert np.isnan(val)
    assert why == "insufficient_bhar_data"
