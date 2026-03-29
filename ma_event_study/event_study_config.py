"""Единые параметры оценочного окна market model для ma_thesis_pipeline и car_event_study."""
from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import tomllib
except Exception:  # pragma: no cover
    import tomli as tomllib  # type: ignore


def _model_section() -> dict[str, Any]:
    p = Path(__file__).resolve().parent / "config.toml"
    if not p.is_file():
        return {}
    with open(p, "rb") as f:
        return tomllib.load(f).get("model", {})


def get_estimation_window() -> tuple[int, int]:
    """Торговые дни t: [start, end] включительно (обычно [-250, -30])."""
    m = _model_section()
    lo = int(m.get("estimation_window_start", -250))
    hi = int(m.get("estimation_window_end", -30))
    return lo, hi


def get_estimation_windows_list() -> list[tuple[int, int]]:
    """Список из одного окна (совместимость с циклом в car_event_study.estimate_market_model)."""
    return [get_estimation_window()]
