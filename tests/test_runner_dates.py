"""Регрессия: parse_date_any и _select_daily_slice (runner)."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd

from ma_event_study.runner import _select_daily_slice, _unique_trade_days, parse_date_any


def test_parse_date_any_dd_mm_yyyy() -> None:
    assert parse_date_any("03.06.2021") == date(2021, 6, 3)


def test_parse_date_any_excel_serial_roundtrip() -> None:
    base = date(1899, 12, 30)
    target = date(2021, 6, 3)
    serial = (target - base).days
    assert parse_date_any(float(serial)) == target


def test_parse_date_any_year_not_excel() -> None:
    """Год как целое (2023) не должен трактоваться как Excel serial."""
    assert parse_date_any(2023) is None


def test_select_daily_slice_pre260_post60_covers_estimation_window() -> None:
    class MockCandle:
        __slots__ = ("time",)

        def __init__(self, t: datetime) -> None:
            self.time = t

    dr = pd.bdate_range("2020-01-01", periods=320, freq="C")
    candles = [
        MockCandle(datetime(d.year, d.month, d.day, 12, 0, tzinfo=timezone.utc)) for d in dr
    ]
    anchor = dr[270].date()
    selected, at = _select_daily_slice(
        candles,
        anchor,
        pre_window=260,
        post_window=60,
        resolved_ticker="TEST",
    )
    assert at == anchor
    assert len(selected) > 0
    td = _unique_trade_days(selected)
    anchor_idx = td.index(at)
    # Первый торговый день в срезе: относительный t = 0 - anchor_idx
    t_min = -anchor_idx
    assert t_min <= -250

