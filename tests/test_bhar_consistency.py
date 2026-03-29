from __future__ import annotations

import numpy as np
import pandas as pd

from ma_event_study.event_study_formulas import bhar_from_simple_returns
from ma_event_study.ma_thesis_pipeline import bhar_window


def test_bhar_identical_returns_is_zero() -> None:
    ri = pd.Series([0.01, -0.02, 0.03, 0.0, 0.01], dtype=float)
    rm = ri.copy()
    got = bhar_from_simple_returns(ri, rm)
    assert np.isclose(got, 0.0, rtol=1e-12, atol=1e-12)


def test_bhar_known_outperformance_matches_manual() -> None:
    # constant daily spread: stock outperforms benchmark every day
    ri = pd.Series([0.02, 0.02, 0.02, 0.02], dtype=float)
    rm = pd.Series([0.01, 0.01, 0.01, 0.01], dtype=float)
    manual = (1.02**4) - (1.01**4)
    got = bhar_from_simple_returns(ri, rm)
    assert got > 0
    assert np.isclose(got, manual, rtol=1e-12, atol=1e-12)


def test_bhar_window_equals_formula_on_same_horizon() -> None:
    # t from -2..8, BHAR uses only t=1..5
    t = pd.Series([-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8], dtype=float)
    sec = pd.Series([0.0, 0.0, 0.0, 0.03, 0.01, -0.01, 0.02, 0.01, 0.0, 0.0, 0.0], dtype=float)
    mkt = pd.Series([0.0, 0.0, 0.0, 0.01, 0.0, -0.01, 0.01, 0.0, 0.0, 0.0, 0.0], dtype=float)

    panel = pd.DataFrame({"security_return": sec, "benchmark_return": mkt})
    got = bhar_window(panel, t, horizon=5, min_fraction=0.8, min_tail_beyond_horizon=2)

    mask = t.between(1, 5)
    expected = bhar_from_simple_returns(sec[mask], mkt[mask])
    assert np.isclose(got, expected, rtol=1e-12, atol=1e-12)

