"""
Групповые тесты H10–H13 и др. Выход: out/thesis/tables/group_tests.csv
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from .paths import THESIS_TABLES_DIR, resolve_clean_data_file, ensure_clean_dirs

logger = logging.getLogger("ma_event_study")

ROWSPEC = [
    ("deal_type", "CAR_ANN_1_1"),
    ("deal_type", "CAR_CLOSE_5_5"),
    ("deal_type", "BHAR_ANN_120"),
    ("deal_status_clean", "CAR_ANN_1_1"),
    ("is_off_market_release", "CAR_ANN_INTRADAY_15M"),
]


def _normality_p(x: np.ndarray) -> tuple[str, float]:
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return "none", np.nan
    if len(x) < 50:
        _, p = stats.shapiro(x)
        return "shapiro", float(p)
    _, p = stats.normaltest(x)
    return "dagostino_k2", float(p)


def _test_groups(values: list[np.ndarray], names: list[str]) -> tuple[str, float, float, str, float]:
    """Возвращает test_name, stat, p_value, normality_test, normality_p (объединённая проверка по группам)."""
    norms = []
    for v in values:
        nt, npv = _normality_p(v)
        norms.append(npv)
    norm_ok = all((np.isnan(p) or p > 0.05) for p in norms)
    pooled = np.concatenate(values)
    _, np_all = _normality_p(pooled)

    if len(values) == 2:
        a, b = values
        if norm_ok:
            st, p = stats.ttest_ind(a, b, equal_var=False, nan_policy="omit")
            return "welch_t", float(st), float(p), _normality_p(np.concatenate([a, b]))[0], float(np_all)
        st, p = stats.mannwhitneyu(a, b, alternative="two-sided")
        return "mannwhitney", float(st), float(p), _normality_p(np.concatenate([a, b]))[0], float(np_all)
    if len(values) > 2:
        if norm_ok:
            st, p = stats.f_oneway(*values)
            return "anova", float(st), float(p), "pooled_" + _normality_p(pooled)[0], float(np_all)
        st, p = stats.kruskal(*values)
        return "kruskal", float(st), float(p), "pooled_" + _normality_p(pooled)[0], float(np_all)
    return "none", np.nan, np.nan, "", np.nan


def run(path: Path | None = None) -> pd.DataFrame:
    ensure_clean_dirs()
    p = path or resolve_clean_data_file("ma_deals_enriched.csv")
    out = THESIS_TABLES_DIR / "group_tests.csv"
    empty = pd.DataFrame(
        columns=["group_variable", "metric", "n_groups", "test", "stat", "p_value", "normality_test", "normality_p"]
    )
    if not p.is_file():
        empty.to_csv(out, index=False, encoding="utf-8")
        return empty
    df = pd.read_csv(p, encoding="utf-8")
    rows: list[dict] = []
    for gcol, mcol in ROWSPEC:
        if gcol not in df.columns or mcol not in df.columns:
            continue
        sub = df[[gcol, mcol]].dropna()
        sub = sub[sub[gcol].astype(str).str.len() > 0]
        if sub.empty:
            continue
        parts: list[np.ndarray] = []
        names: list[str] = []
        for gval, gdf in sub.groupby(gcol):
            arr = pd.to_numeric(gdf[mcol], errors="coerce").dropna().to_numpy()
            if len(arr) < 2:
                continue
            parts.append(arr)
            names.append(str(gval))
        if len(parts) < 2:
            continue
        test, st, pv, nt, npp = _test_groups(parts, names)
        rows.append(
            {
                "group_variable": gcol,
                "metric": mcol,
                "n_groups": len(parts),
                "test": test,
                "stat": st,
                "p_value": pv,
                "normality_test": nt,
                "normality_p": npp,
            }
        )
    res = pd.DataFrame(rows) if rows else empty
    res.to_csv(out, index=False, encoding="utf-8")
    print("group_tests ->", out)
    return res


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
