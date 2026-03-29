"""
Сборка ma_deals_enriched.csv и base_deals_standardized.csv в out/clean_data/.
Запускает ma_thesis_pipeline с перенаправлением clean_data, затем merge BHAR и служебные колонки.
"""
from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pandas as pd

from . import bhar_analysis
from . import ma_thesis_pipeline as mtp
from . import cross_sectional_analysis
from . import group_tests
from . import hypotheses_mapper
from .paths import CLEAN_DATA_DIR, OUT_DIR, THESIS_CLEAN_DATA_DIR, ensure_clean_dirs

logger = logging.getLogger("ma_event_study")


def run() -> None:
    ensure_clean_dirs()
    CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Thesis pipeline пишет clean_data в out/clean_data
    mtp.OUTPUT_DIRS["clean_data"] = CLEAN_DATA_DIR
    mtp.OUTPUT_DIRS["tables"] = mtp.OUTPUT_ROOT / "tables"
    mtp.OUTPUT_DIRS["charts"] = mtp.OUTPUT_ROOT / "charts"
    mtp.OUTPUT_DIRS["models"] = mtp.OUTPUT_ROOT / "models"
    for p in mtp.OUTPUT_DIRS.values():
        p.mkdir(parents=True, exist_ok=True)

    try:
        mtp.main()
    except Exception as e:
        logger.exception("ma_thesis_pipeline: %s", e)
        raise

    bhar_analysis.run()
    bh_path = OUT_DIR / "bhar_by_deal_ann.csv"
    enr = CLEAN_DATA_DIR / "ma_deals_enriched.csv"
    if bh_path.is_file() and enr.is_file():
        e = pd.read_csv(enr, encoding="utf-8")
        bh = pd.read_csv(bh_path, encoding="utf-8")
        e = e.merge(bh, on="source_row_excel", how="left", suffixes=("", "_bhar"))
        for c in bh.columns:
            if c == "source_row_excel":
                continue
            if f"{c}_bhar" in e.columns:
                e[c] = e[f"{c}_bhar"].combine_first(e.get(c))
                e.drop(columns=[f"{c}_bhar"], inplace=True, errors="ignore")
        e.to_csv(enr, index=False, encoding="utf-8")

    # Дублировать в thesis/clean_data для старых путей
    for name in ["ma_deals_enriched.csv", "base_deals_standardized.csv", "announcement_daily_panel_clean.csv", "create_daily_panel_clean.csv"]:
        src = CLEAN_DATA_DIR / name
        if src.is_file():
            shutil.copy2(src, THESIS_CLEAN_DATA_DIR / name)

    cross_sectional_analysis.run()
    group_tests.run()
    hypotheses_mapper.run()

    print("enrich_deals: готово ->", CLEAN_DATA_DIR)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
