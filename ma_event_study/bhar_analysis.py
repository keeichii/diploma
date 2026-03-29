"""
BHAR на горизонтах 60/120/250 торговых дней после T=0 (без look-ahead).
Записывает предупреждения insufficient_bhar_data и доп. колонки для enrich.

NOTE:
`ma_thesis_pipeline` is the canonical BHAR engine used in enriched outputs.
This module is kept as an auxiliary/diagnostic exporter (`out/bhar_by_deal_ann.csv`)
to audit announcement-only BHAR coverage and failure reasons.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .event_study_formulas import bhar_post_window
from .paths import OUT_DIR, THESIS_TABLES_DIR, ensure_clean_dirs

logger = logging.getLogger("ma_event_study")

BHAR_HORIZONS = (60, 120, 250)
# Smaller tail requirement improves realistic coverage while avoiding look-ahead.
MIN_TAIL = 0
MIN_FRACTION_VALID = 0.75


def _norm_stub(df: pd.DataFrame) -> pd.DataFrame:
    col = next(
        (c for c in df.columns if str(c).strip().lower().replace(" ", "_") == "pipeline_row_note"),
        None,
    )
    if col is None:
        return df
    s = df[col]
    bad = s.notna() & s.astype(str).str.strip().ne("") & ~s.astype(str).str.strip().str.lower().isin(["nan", "<na>"])
    return df.loc[~bad].drop(columns=[col], errors="ignore").reset_index(drop=True)


def _benchmark_col(df: pd.DataFrame) -> str | None:
    cols = [str(c) for c in df.columns]
    # 1) strict IMOEX match
    for c in cols:
        cl = c.lower()
        if "imoex" in cl and "off_market" not in cl:
            return c
    # 2) fallback: index/benchmark-like continuous price
    for c in cols:
        cl = c.lower()
        if any(k in cl for k in ["benchmark", "index", "индекс"]) and "off_market" not in cl:
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().sum() > 50 and s.dropna().nunique() > 20:
                return c
    return None


def _pick_px(df: pd.DataFrame) -> str:
    cols = [str(c) for c in df.columns]
    for c in cols:
        if "adjusted close" in c.lower():
            return c
    for c in cols:
        if c.lower().startswith("close"):
            return c
    for c in cols:
        if "close" in c.lower():
            return c
    raise ValueError("No price col")


def compute_bhars_from_daily_table(announce_path: Path) -> tuple[pd.DataFrame, list[dict]]:
    """Одна строка на source_row_excel: BHAR_ANN_* из table 2.1."""
    warnings: list[dict] = []
    if not announce_path.is_file():
        logger.warning("BHAR: нет файла %s", announce_path)
        return pd.DataFrame(), warnings
    raw = _norm_stub(pd.read_excel(announce_path))
    if raw.empty:
        return pd.DataFrame(), warnings
    raw.columns = [str(c).strip() for c in raw.columns]
    bx = _benchmark_col(raw)
    if not bx:
        logger.warning("BHAR: нет колонки IMOEX")
        return pd.DataFrame(), warnings
    px = _pick_px(raw)
    raw["t"] = pd.to_numeric(raw.get("t"), errors="coerce")
    raw["px"] = pd.to_numeric(raw[px], errors="coerce")
    raw["bm"] = pd.to_numeric(raw[bx], errors="coerce")
    raw["Ri"] = raw.groupby("source_row_excel", sort=False)["px"].pct_change(fill_method=None)
    raw["Rm"] = raw.groupby("source_row_excel", sort=False)["bm"].pct_change(fill_method=None)

    rows = []
    for sid, g in raw.groupby("source_row_excel", sort=True):
        g2 = g.sort_values("t").copy()
        try:
            sid_i = int(sid)
        except (TypeError, ValueError):
            sid_i = sid
        rec: dict = {"source_row_excel": sid_i}
        for T in BHAR_HORIZONS:
            sub = g2[["t", "Ri", "Rm"]].rename(
                columns={"Ri": "stock_return_simple", "Rm": "benchmark_return_simple"}
            )
            # diagnostics for explicit warnings
            win = sub.loc[sub["t"].between(1, T)].drop_duplicates("t", keep="last")
            valid_pairs = int((win["stock_return_simple"].notna() & win["benchmark_return_simple"].notna()).sum())
            max_t = pd.to_numeric(sub["t"], errors="coerce").max()
            val, why = bhar_post_window(
                sub,
                ri_col="stock_return_simple",
                rm_col="benchmark_return_simple",
                horizon=T,
                min_fraction_valid=MIN_FRACTION_VALID,
                min_tail_beyond_horizon=MIN_TAIL,
            )
            rec[f"BHAR_ANN_{T}"] = val
            if why:
                warnings.append(
                    {
                        "category": "insufficient_bhar_data",
                        "message": (
                            f"source_row={sid} T={T}: {why}; "
                            f"valid_pairs={valid_pairs}; max_t={max_t}; "
                            f"min_fraction={MIN_FRACTION_VALID}; min_tail={MIN_TAIL}"
                        ),
                        "table_role": "announcement_daily",
                        "source_row_excel": sid,
                    }
                )
                logger.info(
                    "BHAR_SKIP | source_row=%s | T=%s | why=%s | valid_pairs=%s | max_t=%s",
                    sid,
                    T,
                    why,
                    valid_pairs,
                    max_t,
                )
        rows.append(rec)
    return pd.DataFrame(rows), warnings


def append_warnings_csv(new_rows: list[dict], path: Path) -> None:
    ensure_clean_dirs()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not new_rows:
        return
    add = pd.DataFrame(new_rows)
    if path.is_file():
        old = pd.read_csv(path, encoding="utf-8")
        add = pd.concat([old, add], ignore_index=True)
    add.to_csv(path, index=False, encoding="utf-8")


def run() -> None:
    """Точка входа: дополняет BHAR_ANN_*; закрытие — в enrich при наличии дат."""
    root = OUT_DIR
    ann = root / "table_2_1_first_press_release.xlsx"
    alt = root / "table_2_1_first_press_release-3.xlsx"
    path = ann if ann.is_file() else alt
    bh, warns = compute_bhars_from_daily_table(path)
    out_csv = root / "bhar_by_deal_ann.csv"
    if not bh.empty:
        bh.to_csv(out_csv, index=False, encoding="utf-8")
        logger.info("BHAR: сохранено %s строк в %s", len(bh), out_csv)
    append_warnings_csv(warns, THESIS_TABLES_DIR / "warnings.csv")
    print("bhar_analysis:", out_csv if not bh.empty else "no data")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
