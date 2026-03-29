"""
Итоговая карта гипотез H1–H22 для narrative / CSV.
Читает one_sample_tests, group_tests, regression_coefficients, enriched (для корреляций).
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from .paths import THESIS_TABLES_DIR, resolve_clean_data_file, ensure_clean_dirs

logger = logging.getLogger("ma_event_study")

OUT_CSV_NAME = "hypotheses_significance_map_ru.csv"

# (id, описание, метрика/источник, колонка p-value или спец-ключ)
HYPOTHESES: list[tuple[str, str, str, str]] = [
    ("H1", "Мгновенная внутридневная реакция", "CAR_ANN_INTRADAY_15M", "one_sample:CAR_ANN_INTRADAY_15M"),
    ("H2", "Ранние бары после якоря", "CAR first bars", "manual_na"),
    ("H3", "Доля реакции за 1ч к дню", "ratio_1h_vs_day", "enriched:ratio_1h_vs_day"),
    ("H4", "Предсобытийное окно intraday [-4;0] до якоря", "CAR_INTRADAY_PRE_4_0", "one_sample:CAR_INTRADAY_PRE_4_0"),
    ("H5", "Различие on/off market intraday", "CAR_ANN_INTRADAY_15M × is_off_market", "group:is_off_market_release"),
    ("H6", "Дневные CAR объявления", "CAR_ANN_1_1 / CAR_ANN_3_3", "one_sample:CAR_ANN_1_1"),
    ("H7", "Знак среднего CAR_ANN_1_1", "sign mean CAR", "one_sample:CAR_ANN_1_1"),
    ("H8", "Предсобытийный run-up [-30,-5]", "RUNUP_PRE_30_5", "one_sample:RUNUP_PRE_30_5"),
    ("H9", "Размер сделки (лог)", "coef log_DealSize M1", "reg:M1_H9_H15_CAR_ANN:log_DealSize_bln"),
    ("H10", "Тип сделки (группы)", "deal_type × CAR_ANN_1_1", "group:deal_type"),
    ("H11", "Тип сделки — ecosystem", "subset deal_type", "group:deal_type"),
    ("H12", "Тип сделки — core_banking", "subset deal_type", "group:deal_type"),
    ("H13", "Тип сделки — non_financial / distressed", "subset deal_type", "group:deal_type"),
    ("H14", "ROE fundamentals", "coef ROE M2", "reg:M2_H14_H16_CAR_ANN:ANN_ROE"),
    ("H15", "log Market Cap", "coef log_MarketCap M1", "reg:M1_H9_H15_CAR_ANN:log_MarketCap_bln"),
    ("H16", "ROA, P/B, P/E, активы", "M2 coefficients", "reg:M2_H14_H16_CAR_ANN:any"),
    ("H17", "Ликвидность (объём до события)", "coef log_Volume M4", "reg:M4_H17_CAR_ANN:log_Volume_pre20"),
    ("H18", "Связь CAR короткого окна и BHAR", "Spearman CAR×BHAR", "spearman"),
    ("H19", "Пост-сделочный дрейф BHAR", "BHAR_ANN_120 share<0 + t-test", "bhar_h19"),
    ("H20", "Ecosystem vs non-ecosystem BHAR", "Mann-Whitney BHAR", "group:bhar_ecosystem"),
    ("H21", "Пост-сделочные финансы (ROE)", "требуются данные", "not_available"),
    ("H22", "Пост-сделочные финансы (прочее)", "требуются данные", "not_available"),
]


def _classify_p(p: float) -> str:
    if pd.isna(p):
        return "НЕ ПРОТЕСТИРОВАНО"
    if p < 0.05:
        return "ДА"
    if p < 0.10:
        return "ЧАСТИЧНО"
    return "НЕТ"


def run() -> None:
    ensure_clean_dirs()
    os_path = THESIS_TABLES_DIR / "one_sample_tests.csv"
    grp_path = THESIS_TABLES_DIR / "group_tests.csv"
    reg_path = Path(__file__).resolve().parents[1] / "out" / "thesis" / "models" / "regression_coefficients.csv"

    one_p: dict[str, float] = {}
    if os_path.is_file():
        o = pd.read_csv(os_path, encoding="utf-8")
        if not o.empty and "metric" in o.columns and "p_value" in o.columns:
            one_p = {str(r["metric"]): float(r["p_value"]) for _, r in o.iterrows() if pd.notna(r.get("p_value"))}

    grp_p: dict[str, float] = {}
    if grp_path.is_file():
        g = pd.read_csv(grp_path, encoding="utf-8")
        for _, r in g.iterrows():
            key = f"{r.get('group_variable')}_{r.get('metric')}"
            grp_p[str(key)] = float(r["p_value"])

    reg_p: dict[str, float] = {}
    if reg_path.is_file():
        r = pd.read_csv(reg_path, encoding="utf-8")
        for _, row in r.iterrows():
            k = f"{row['model']}:{row['term']}"
            reg_p[k] = float(row["p_value"])

    enr_path = resolve_clean_data_file("ma_deals_enriched.csv")
    sp_p = np.nan
    if enr_path.is_file():
        e = pd.read_csv(enr_path, encoding="utf-8")
        if "CAR_ANN_1_1" in e.columns and "BHAR_ANN_120" in e.columns:
            a = pd.to_numeric(e["CAR_ANN_1_1"], errors="coerce")
            b = pd.to_numeric(e["BHAR_ANN_120"], errors="coerce")
            m = a.notna() & b.notna()
            if m.sum() > 5:
                _, sp_p = stats.spearmanr(a[m], b[m])

    out_rows = []
    for hid, desc, metric, src in HYPOTHESES:
        p = np.nan
        comment = ""
        if src.startswith("one_sample:"):
            key = src.split(":", 1)[1]
            p = one_p.get(key, np.nan)
        elif src.startswith("enriched:"):
            col = src.split(":", 1)[1]
            if enr_path.is_file():
                e2 = pd.read_csv(enr_path, encoding="utf-8")
                if col in e2.columns:
                    v = pd.to_numeric(e2[col], errors="coerce").dropna()
                    if len(v) > 2:
                        _, p = stats.ttest_1samp(v, 0.0, nan_policy="omit")
        elif src == "group:bhar_ecosystem":
            if enr_path.is_file():
                e2 = pd.read_csv(enr_path, encoding="utf-8")
                if "deal_type" in e2.columns and "BHAR_ANN_120" in e2.columns:
                    eco = e2["deal_type"].astype(str).str.strip().str.lower().eq("ecosystem")
                    v_eco = pd.to_numeric(e2.loc[eco, "BHAR_ANN_120"], errors="coerce").dropna().to_numpy()
                    v_rest = pd.to_numeric(e2.loc[~eco, "BHAR_ANN_120"], errors="coerce").dropna().to_numpy()
                    if len(v_eco) > 1 and len(v_rest) > 1:
                        _, p = stats.mannwhitneyu(v_eco, v_rest, alternative="two-sided")
        elif src.startswith("group:deal_type"):
            p = next((grp_p[k] for k in grp_p if k.startswith("deal_type")), np.nan)
        elif src.startswith("group:is_off_market"):
            p = next((grp_p[k] for k in grp_p if "is_off_market" in k), np.nan)
        elif src.startswith("reg:"):
            parts = src.split(":", 2)
            if len(parts) >= 3:
                p = reg_p.get(f"{parts[1]}:{parts[2]}", np.nan)
            if src.endswith(":any") and np.isnan(p):
                sub = [v for k, v in reg_p.items() if k.startswith(parts[1])]
                p = float(np.nanmin(sub)) if sub else np.nan
        elif src == "spearman":
            p = sp_p
        elif src == "bhar_h19":
            if enr_path.is_file():
                e = pd.read_csv(enr_path, encoding="utf-8")
                if "BHAR_ANN_120" in e.columns:
                    v = pd.to_numeric(e["BHAR_ANN_120"], errors="coerce").dropna()
                    if len(v) > 2:
                        _, p = stats.ttest_1samp(v, 0.0, nan_policy="omit")
        elif src == "not_available" or src == "manual_na":
            p = np.nan
            comment = "Недостаточно данных или ручная метрика"

        verdict = _classify_p(p)
        if src == "not_available":
            verdict = "НЕ ПРОТЕСТИРОВАНО"
        out_rows.append(
            {
                "Гипотеза": hid,
                "Описание": desc,
                "Метрика_теста": metric,
                "Есть статистически значимый эффект": verdict,
                "Комментарий": comment or (f"p={p:.4g}" if np.isfinite(p) else ""),
            }
        )

    df = pd.DataFrame(out_rows)
    pth = THESIS_TABLES_DIR / OUT_CSV_NAME
    df.to_csv(pth, index=False, encoding="utf-8")
    print("hypotheses_mapper ->", pth)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
