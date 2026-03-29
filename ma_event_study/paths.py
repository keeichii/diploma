"""Каталоги артефактов проекта (корень репозитория = parent пакета)."""
from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "out"
CLEAN_DATA_DIR = OUT_DIR / "clean_data"
THESIS_DIR = OUT_DIR / "thesis"
THESIS_TABLES_DIR = THESIS_DIR / "tables"
THESIS_MODELS_DIR = THESIS_DIR / "models"
THESIS_CHARTS_DIR = THESIS_DIR / "charts"
# Совместимость: старые прогоны писали в out/thesis/clean_data/
THESIS_CLEAN_DATA_DIR = THESIS_DIR / "clean_data"


def ensure_clean_dirs() -> None:
    CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
    THESIS_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    THESIS_MODELS_DIR.mkdir(parents=True, exist_ok=True)
    THESIS_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    THESIS_CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)


def resolve_clean_data_file(name: str) -> Path:
    """Приоритет: out/clean_data/, затем out/thesis/clean_data/."""
    p = CLEAN_DATA_DIR / name
    if p.is_file():
        return p
    alt = THESIS_CLEAN_DATA_DIR / name
    return alt
