from __future__ import annotations

import shutil
from pathlib import Path

# Финальные артефакты без логов (.log, .txt) — только .xlsx
FINAL_ROOT_FILES = [
    "ma_deals_AUDIT.xlsx",
    "table_1_intraday.xlsx",
    "table_2_1_first_press_release.xlsx",
    "table_2_2_cbonds_actualization.xlsx",
    "table_2_3_cbonds_create.xlsx",
    "run_summary.xlsx",
    "unified_events_debug.xlsx",
    "car_event_level.xlsx",
    "car_summary.xlsx",
    "car_main_table.xlsx",
    "intraday_unified_debug.xlsx",
    "intraday_event_level_car.xlsx",
    "intraday_car_summary.xlsx",
    "intraday_main_table.xlsx",
]

FINAL_RUONIA_SUBDIR = "ruonia_augmented"


def copy_final_outputs(out_dir: Path) -> Path:
    """
    Копирует итоговые xlsx в out/final (без run.log и прочих текстовых отчётов).
    """
    final_dir = out_dir / "final"
    final_dir.mkdir(parents=True, exist_ok=True)

    for name in FINAL_ROOT_FILES:
        src = out_dir / name
        if src.is_file():
            shutil.copy2(src, final_dir / name)

    ru_src = out_dir / FINAL_RUONIA_SUBDIR
    if ru_src.is_dir():
        ru_dst = final_dir / FINAL_RUONIA_SUBDIR
        ru_dst.mkdir(parents=True, exist_ok=True)
        for p in ru_src.glob("*.xlsx"):
            shutil.copy2(p, ru_dst / p.name)

    return final_dir
