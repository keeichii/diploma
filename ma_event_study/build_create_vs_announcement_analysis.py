"""
Сравнение метрик "первое упоминание" vs "официальное объявление".

Генерирует:
  - out/thesis/tables/create_vs_announcement_*.csv/.xlsx
  - out/thesis/charts/*png
  - out/thesis/tables/create_vs_announcement_summary_ru.txt
"""

from __future__ import annotations

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, List, Tuple
from scipy import stats

from .paths import THESIS_CHARTS_DIR, THESIS_TABLES_DIR, THESIS_CLEAN_DATA_DIR


def _export_csv_xlsx(df: pd.DataFrame, stem: str) -> None:
    THESIS_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = THESIS_TABLES_DIR / f"{stem}.csv"
    xlsx_path = THESIS_TABLES_DIR / f"{stem}.xlsx"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="data")


def fmt_pct(x: float, digits: int = 2) -> str:
    if pd.isna(x):
        return "н/д"
    return f"{x * 100:.{digits}f}%"


def paired_stats(df: pd.DataFrame, ann_col: str, create_col: str, label: str) -> Dict[str, object]:
    sub = df[[ann_col, create_col]].dropna().copy()
    if sub.empty:
        return {
            "metric_label": label,
            "announcement_metric": ann_col,
            "create_metric": create_col,
            "n": 0,
            "mean_announcement": np.nan,
            "mean_create": np.nan,
            "mean_diff_create_minus_announcement": np.nan,
            "median_diff_create_minus_announcement": np.nan,
            "mean_abs_diff_create_minus_announcement": np.nan,
            "share_create_greater": np.nan,
            "share_abs_create_greater": np.nan,
            "paired_t_stat": np.nan,
            "paired_t_p_value": np.nan,
            "wilcoxon_p_value": np.nan,
        }

    diff = sub[create_col] - sub[ann_col]
    abs_diff = sub[create_col].abs() - sub[ann_col].abs()

    t_stat, p_val = stats.ttest_rel(sub[create_col], sub[ann_col], nan_policy="omit")
    try:
        nonzero = diff[diff != 0]
        wilcoxon_p = stats.wilcoxon(nonzero).pvalue if len(nonzero) >= 10 else np.nan
    except Exception:
        wilcoxon_p = np.nan

    return {
        "metric_label": label,
        "announcement_metric": ann_col,
        "create_metric": create_col,
        "n": int(len(sub)),
        "mean_announcement": sub[ann_col].mean(),
        "mean_create": sub[create_col].mean(),
        "mean_diff_create_minus_announcement": diff.mean(),
        "median_diff_create_minus_announcement": diff.median(),
        "mean_abs_diff_create_minus_announcement": abs_diff.mean(),
        "share_create_greater": float((diff > 0).mean()),
        "share_abs_create_greater": float((abs_diff > 0).mean()),
        "paired_t_stat": t_stat,
        "paired_t_p_value": p_val,
        "wilcoxon_p_value": wilcoxon_p,
    }


def build_paired_tables(enriched: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    metric_pairs = [
        ("CAR (-1;+1)", "CAR_ANN_1_1", "CAR_CREATE_1_1"),
        ("CAR (-3;+3)", "CAR_ANN_3_3", "CAR_CREATE_3_3"),
        ("CAR (-5;+5)", "CAR_ANN_5_5", "CAR_CREATE_5_5"),
        ("CAR (-10;+10)", "CAR_ANN_10_10", "CAR_CREATE_10_10"),
        ("BHAR (60)", "BHAR_ANN_60", "BHAR_CREATE_60"),
        ("BHAR (120)", "BHAR_ANN_120", "BHAR_CREATE_120"),
        ("BHAR (250)", "BHAR_ANN_250", "BHAR_CREATE_250"),
    ]

    paired_rows = [paired_stats(enriched, ann, create, label) for label, ann, create in metric_pairs]
    paired_df = pd.DataFrame(paired_rows)

    # Deal-level table for histograms/scatter of differences
    deal_level = enriched[
        [
            "source_row_excel",
            "Покупатель",
            "Тикер покупателя",
            "Объект сделки",
            "Дата объявления сделки",
            "Дата создания",
            "DAYS_CREATE_TO_ANNOUNCEMENT",
            "CAR_ANN_1_1",
            "CAR_CREATE_1_1",
            "CAR_ANN_3_3",
            "CAR_CREATE_3_3",
            "CAR_ANN_5_5",
            "CAR_CREATE_5_5",
            "CAR_ANN_10_10",
            "CAR_CREATE_10_10",
            "BHAR_ANN_60",
            "BHAR_CREATE_60",
            "BHAR_ANN_120",
            "BHAR_CREATE_120",
            "BHAR_ANN_250",
            "BHAR_CREATE_250",
            "RUNUP_PRE_30_5",
            "CAR_PRE_ANNOUNCEMENT",
        ]
    ].copy()

    deal_level["DIFF_CREATE_MINUS_ANN_1_1"] = deal_level["CAR_CREATE_1_1"] - deal_level["CAR_ANN_1_1"]
    deal_level["DIFF_CREATE_MINUS_ANN_3_3"] = deal_level["CAR_CREATE_3_3"] - deal_level["CAR_ANN_3_3"]
    deal_level["DIFF_CREATE_MINUS_ANN_5_5"] = deal_level["CAR_CREATE_5_5"] - deal_level["CAR_ANN_5_5"]
    deal_level["DIFF_CREATE_MINUS_ANN_10_10"] = deal_level["CAR_CREATE_10_10"] - deal_level["CAR_ANN_10_10"]
    deal_level["DIFF_BHAR_CREATE_MINUS_ANN_120"] = deal_level["BHAR_CREATE_120"] - deal_level["BHAR_ANN_120"]

    return paired_df, deal_level


def plot_mean_bars(paired_df: pd.DataFrame) -> None:
    sub = paired_df[paired_df["metric_label"].astype(str).str.startswith("CAR")].copy()
    if sub.empty:
        return
    x = np.arange(len(sub))
    width = 0.36
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(x - width / 2, sub["mean_announcement"], width, label="Официальное объявление")
    ax.bar(x + width / 2, sub["mean_create"], width, label="Первое упоминание")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(sub["metric_label"], rotation=0)
    ax.set_ylabel("Средний CAR")
    ax.set_title("Сравнение средних CAR: первое упоминание vs официальное объявление")
    ax.legend()
    fig.tight_layout()
    fig.savefig(THESIS_CHARTS_DIR / "create_vs_announcement_mean_car_bars.png", dpi=200)
    plt.close(fig)


def plot_bhar_bars(paired_df: pd.DataFrame) -> None:
    sub = paired_df[paired_df["metric_label"].astype(str).str.startswith("BHAR")].copy()
    if sub.empty:
        return
    x = np.arange(len(sub))
    width = 0.36
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(x - width / 2, sub["mean_announcement"], width, label="Официальное объявление")
    ax.bar(x + width / 2, sub["mean_create"], width, label="Первое упоминание")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(sub["metric_label"], rotation=0)
    ax.set_ylabel("Средний BHAR")
    ax.set_title("Сравнение средних BHAR: первое упоминание vs официальное объявление")
    ax.legend()
    fig.tight_layout()
    fig.savefig(THESIS_CHARTS_DIR / "create_vs_announcement_mean_bhar_bars.png", dpi=200)
    plt.close(fig)


def plot_diff_hist(series: pd.Series, title: str, filename: str) -> None:
    series = pd.to_numeric(series, errors="coerce").dropna()
    if series.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(series, bins=20, edgecolor="black")
    ax.axvline(0, color="red", linestyle="--", linewidth=1)
    ax.set_title(title)
    ax.set_xlabel("Разность")
    ax.set_ylabel("Частота")
    fig.tight_layout()
    fig.savefig(THESIS_CHARTS_DIR / filename, dpi=200)
    plt.close(fig)


def plot_scatter(enriched: pd.DataFrame, ann_col: str, create_col: str, title: str, filename: str) -> None:
    sub = enriched[[ann_col, create_col]].dropna()
    if sub.empty:
        return
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.scatter(sub[ann_col], sub[create_col], alpha=0.7)
    lo = min(sub[ann_col].min(), sub[create_col].min())
    hi = max(sub[ann_col].max(), sub[create_col].max())
    ax.plot([lo, hi], [lo, hi], color="red", linestyle="--", linewidth=1)
    ax.set_xlabel("Официальное объявление")
    ax.set_ylabel("Первое упоминание")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(THESIS_CHARTS_DIR / filename, dpi=200)
    plt.close(fig)


def plot_overlay_profiles(announcement_panel: pd.DataFrame, create_panel: pd.DataFrame) -> None:
    if "t" not in announcement_panel.columns or "t" not in create_panel.columns:
        return
    ann = announcement_panel.loc[
        announcement_panel["t"].between(-10, 10) & announcement_panel["ar_ann"].notna()
    ].copy()
    cre = create_panel.loc[
        create_panel["t"].between(-10, 10) & create_panel["ar_create"].notna()
    ].copy()
    if ann.empty or cre.empty:
        return

    ann_avg = ann.groupby("t")["ar_ann"].mean().sort_index()
    cre_avg = cre.groupby("t")["ar_create"].mean().sort_index()
    ann_car = ann_avg.cumsum()
    cre_car = cre_avg.cumsum()

    fig, ax = plt.subplots(2, 1, figsize=(8, 7), sharex=True)
    ax[0].plot(ann_avg.index, ann_avg.values, marker="o", label="Официальное объявление")
    ax[0].plot(cre_avg.index, cre_avg.values, marker="o", label="Первое упоминание")
    ax[0].axhline(0, color="black", linewidth=0.8)
    ax[0].set_ylabel("Средний AR")
    ax[0].set_title("Средний профиль сверхнормальной доходности: объявление vs первое упоминание")
    ax[0].legend()

    ax[1].plot(ann_car.index, ann_car.values, marker="o", label="Официальное объявление")
    ax[1].plot(cre_car.index, cre_car.values, marker="o", label="Первое упоминание")
    ax[1].axhline(0, color="black", linewidth=0.8)
    ax[1].set_ylabel("Накопленный CAR")
    ax[1].set_xlabel("Событийный день t")
    ax[1].set_title("Накопленный профиль: объявление vs первое упоминание")
    ax[1].legend()

    fig.tight_layout()
    fig.savefig(THESIS_CHARTS_DIR / "create_vs_announcement_event_profile_overlay.png", dpi=200)
    plt.close(fig)


def plot_lag_hist(enriched: pd.DataFrame) -> None:
    series = pd.to_numeric(enriched["DAYS_CREATE_TO_ANNOUNCEMENT"], errors="coerce").dropna()
    if series.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(series, bins=20, edgecolor="black")
    ax.axvline(series.median(), color="red", linestyle="--", linewidth=1, label=f"Медиана = {series.median():.0f}")
    ax.set_title("Распределение лага между первым упоминанием и официальным объявлением")
    ax.set_xlabel("Число календарных дней")
    ax.set_ylabel("Частота")
    ax.legend()
    fig.tight_layout()
    fig.savefig(THESIS_CHARTS_DIR / "create_to_announcement_lag_hist.png", dpi=200)
    plt.close(fig)


def build_summary_text(paired_df: pd.DataFrame) -> str:
    lines: List[str] = []
    lines.append("Сравнение первого упоминания сделки и официального объявления")
    lines.append("")
    lines.append("Ниже приведены ключевые парные сравнения по одинаковым сделкам.")
    for _, row in paired_df.iterrows():
        line = (
            f"{row['metric_label']}: n={int(row['n'])}, "
            f"среднее по объявлению={fmt_pct(row['mean_announcement'])}, "
            f"среднее по первому упоминанию={fmt_pct(row['mean_create'])}, "
            f"разность (первое упоминание минус объявление)={fmt_pct(row['mean_diff_create_minus_announcement'])}"
        )
        if pd.notna(row.get("paired_t_p_value")):
            line += f", p-value парного t-теста={row['paired_t_p_value']:.4f}"
        lines.append(line)
    return "\n".join([ln for ln in lines if ln is not None])


def main() -> None:
    enriched_path = THESIS_CLEAN_DATA_DIR / "ma_deals_enriched.csv"
    ann_path = THESIS_CLEAN_DATA_DIR / "announcement_daily_panel_clean.csv"
    cre_path = THESIS_CLEAN_DATA_DIR / "create_daily_panel_clean.csv"
    if not enriched_path.is_file() or not ann_path.is_file() or not cre_path.is_file():
        print("build_create_vs_announcement_analysis: required clean_data not found; skip.")
        return

    enriched = pd.read_csv(enriched_path, encoding="utf-8")
    announcement_panel = pd.read_csv(ann_path, encoding="utf-8")
    create_panel = pd.read_csv(cre_path, encoding="utf-8")

    paired_df, deal_level_df = build_paired_tables(enriched)
    _export_csv_xlsx(paired_df, "create_vs_announcement_paired_comparison")
    _export_csv_xlsx(deal_level_df, "create_vs_announcement_deal_level")

    plot_mean_bars(paired_df)
    plot_bhar_bars(paired_df)
    plot_diff_hist(
        deal_level_df["DIFF_CREATE_MINUS_ANN_1_1"],
        "Разность CAR[-1;+1]: первое упоминание минус официальное объявление",
        "create_vs_announcement_diff_hist_car_1_1.png",
    )
    plot_diff_hist(
        deal_level_df["DIFF_BHAR_CREATE_MINUS_ANN_120"],
        "Разность BHAR(120): первое упоминание минус официальное объявление",
        "create_vs_announcement_diff_hist_bhar_120.png",
    )
    plot_scatter(
        deal_level_df,
        "CAR_ANN_1_1",
        "CAR_CREATE_1_1",
        "CAR[-1;+1]: первое упоминание vs официальное объявление",
        "create_vs_announcement_scatter_car_1_1.png",
    )
    plot_scatter(
        deal_level_df,
        "BHAR_ANN_120",
        "BHAR_CREATE_120",
        "BHAR(120): первое упоминание vs официальное объявление",
        "create_vs_announcement_scatter_bhar_120.png",
    )

    plot_overlay_profiles(announcement_panel, create_panel)
    plot_lag_hist(enriched)

    summary_path = THESIS_TABLES_DIR / "create_vs_announcement_summary_ru.txt"
    summary_path.write_text(build_summary_text(paired_df), encoding="utf-8")

    print("build_create_vs_announcement_analysis: done ->", THESIS_TABLES_DIR)


if __name__ == "__main__":
    main()

