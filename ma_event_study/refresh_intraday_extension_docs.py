"""
Обновление документации по расширенным intraday-окнам.

Встраивает недостающие поля в:
  - `out/thesis/tables/ma_deals_enriched_field_dictionary.csv/.xlsx/.md`
  - `out/thesis/tables/ma_deals_enriched_field_dictionary_ru.csv/.xlsx/.md`

Также (опционально и безопасно) дополняет текст в `hypotheses_significance_map_ru.csv`
для гипотез H1 и H3, если для новых метрик есть p-value в `one_sample_tests.csv`.

Скрипт не меняет вычисления метрик; он обновляет именно описания/комментарии.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from .paths import THESIS_TABLES_DIR


def _export_csv_xlsx_md(df: pd.DataFrame, csv_path: Path) -> None:
    df = df.copy()
    df.to_csv(csv_path, index=False, encoding="utf-8")
    xlsx_path = csv_path.with_suffix(".xlsx")
    md_path = csv_path.with_suffix(".md")

    # XSLX
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="data")

    # MD (простая markdown-таблица)
    header = "| " + " | ".join(df.columns.astype(str)) + " |"
    separator = "| " + " | ".join(["---"] * len(df.columns)) + " |"
    rows = []
    for row in df.itertuples(index=False, name=None):
        rows.append("| " + " | ".join("" if pd.isna(v) else str(v) for v in row) + " |")
    md_path.write_text("\n".join([header, separator] + rows), encoding="utf-8")


def _insert_after(df: pd.DataFrame, match_field: str, new_rows: List[Dict[str, Any]], field_col: str = "field") -> pd.DataFrame:
    df2 = df.copy()
    if field_col not in df2.columns:
        return pd.concat([df2, pd.DataFrame(new_rows)], ignore_index=True)

    if (df2[field_col].astype(str) == match_field).any():
        idx = int(df2.index[df2[field_col].astype(str) == match_field][0])
        left = df2.iloc[: idx + 1]
        right = df2.iloc[idx + 1 :]
        return pd.concat([left, pd.DataFrame(new_rows), right], ignore_index=True)

    # fallback: append
    return pd.concat([df2, pd.DataFrame(new_rows)], ignore_index=True)


def refresh_field_dictionary() -> None:
    # EN dictionary
    en_csv = THESIS_TABLES_DIR / "ma_deals_enriched_field_dictionary.csv"
    if en_csv.is_file():
        df = pd.read_csv(en_csv, encoding="utf-8")
        df = df.loc[~df["field"].isin(["CAR_ANN_INTRADAY_M60_P180", "CAR_CLOSE_INTRADAY_M60_P180"])].copy()

        ann_rows = [
            {
                "field": "CAR_ANN_INTRADAY_M60_P180",
                "source": "table_1_intraday.xlsx",
                "formula_or_method": "CAR = sum(AR_tau) over [-60;+180] minutes around anchor_timestamp_msk.",
                "economic_meaning": "Combined abnormal reaction from one hour before the release through the first three hours after it.",
                "interpretation": "Positive and significant values indicate that the market starts repricing before the formal release and continues after it; insignificant values imply no stable combined pre- and post-event effect.",
            }
        ]
        close_rows = [
            {
                "field": "CAR_CLOSE_INTRADAY_M60_P180",
                "source": "Not available from uploaded data",
                "formula_or_method": "Not computed because no dedicated completion intraday quotation table was uploaded; values remain NaN.",
                "economic_meaning": "Placeholder for the combined completion intraday effect from -60 to +180 minutes.",
                "interpretation": "Currently not interpretable because the required completion intraday input data are unavailable.",
            }
        ]

        df = _insert_after(df, "CAR_ANN_INTRADAY_3H", ann_rows, field_col="field")
        df = _insert_after(df, "CAR_CLOSE_INTRADAY_3H", close_rows, field_col="field")
        _export_csv_xlsx_md(df, en_csv)

    # RU dictionary
    ru_csv = THESIS_TABLES_DIR / "ma_deals_enriched_field_dictionary_ru.csv"
    if ru_csv.is_file():
        df = pd.read_csv(ru_csv, encoding="utf-8")
        df = df.loc[~df["Поле"].isin(["CAR_ANN_INTRADAY_M60_P180", "CAR_CLOSE_INTRADAY_M60_P180"])].copy()

        ann_rows = [
            {
                "Поле": "CAR_ANN_INTRADAY_M60_P180",
                "Источник": "table_1_intraday.xlsx",
                "Формула расчета / метод": "CAR = сумма AR_tau в окне [-60;+180] минут вокруг anchor_timestamp_msk.",
                "Экономический смысл": "Совмещённая сверхнормальная реакция рынка от часа до объявления до первых трёх часов после него.",
                "Интерпретация результата": "Положительное и статистически значимое значение означает, что рынок начинает переоценку ещё до формального релиза и продолжает её после него; незначимость означает отсутствие устойчивого суммарного эффекта.",
            }
        ]
        close_rows = [
            {
                "Поле": "CAR_CLOSE_INTRADAY_M60_P180",
                "Источник": "отсутствует среди загруженных данных",
                "Формула расчета / метод": "Не рассчитывается, поскольку среди загруженных файлов отсутствует отдельная intraday-таблица котировок для completion event; значения остаются NaN.",
                "Экономический смысл": "Заглушка для совмещённого intraday-эффекта вокруг закрытия сделки в окне от -60 до +180 минут.",
                "Интерпретация результата": "Сейчас показатель не интерпретируется, поскольку необходимые intraday-данные по закрытию сделки отсутствуют.",
            }
        ]

        df = _insert_after(df, "CAR_ANN_INTRADAY_3H", ann_rows, field_col="Поле")
        df = _insert_after(df, "CAR_CLOSE_INTRADAY_3H", close_rows, field_col="Поле")
        _export_csv_xlsx_md(df, ru_csv)


def refresh_hypotheses_comments() -> None:
    """
    Безопасное обновление только колонки `Комментарий` для H1 и H3.

    Никаких изменений вычислительной части и вердиктов.
    """
    hyp_csv = THESIS_TABLES_DIR / "hypotheses_significance_map_ru.csv"
    one_csv = THESIS_TABLES_DIR / "one_sample_tests.csv"
    if not hyp_csv.is_file() or not one_csv.is_file():
        return

    one = pd.read_csv(one_csv, encoding="utf-8")
    hyp = pd.read_csv(hyp_csv, encoding="utf-8")
    if hyp.empty or one.empty:
        return

    def get_metric_stats(metric: str) -> tuple[float | None, float | None]:
        sub = one.loc[one["metric"].astype(str) == metric]
        if sub.empty:
            return None, None
        row = sub.iloc[0]
        return row.get("mean", np.nan), row.get("p_value", np.nan)

    # H1: windows for immediate intraday response
    h1_mask = hyp["Гипотеза"].astype(str).eq("H1")
    if h1_mask.any():
        metrics = [
            "CAR_ANN_INTRADAY_15M",
            "CAR_ANN_INTRADAY_30M",
            "CAR_ANN_INTRADAY_1H",
            "CAR_ANN_INTRADAY_3H",
            "CAR_ANN_INTRADAY_M60_P180",
        ]
        parts = []
        for m in metrics:
            mean, p = get_metric_stats(m)
            if pd.notna(p):
                parts.append(f"{m} p={float(p):.4f}")
        if parts:
            hyp.loc[h1_mask, "Комментарий"] = "Окна intraday: " + "; ".join(parts) + "."

    # H3: share of reaction within first hour
    h3_mask = hyp["Гипотеза"].astype(str).eq("H3")
    if h3_mask.any():
        metrics = [
            "CAR_ANN_INTRADAY_1H",
            "CAR_ANN_INTRADAY_M60_P180",
            "CAR_ANN_INTRADAY_FULL",
            "CAR_ANN_INTRADAY_TO_CLOSE",
        ]
        parts = []
        for m in metrics:
            mean, p = get_metric_stats(m)
            if pd.notna(p):
                parts.append(f"{m} mean={float(mean):.4f} p={float(p):.4f}")
        if parts:
            hyp.loc[h3_mask, "Комментарий"] = "Сравнение окон: " + "; ".join(parts) + "."

    hyp.to_csv(hyp_csv, index=False, encoding="utf-8")
    xlsx_path = hyp_csv.with_suffix(".xlsx")
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        hyp.to_excel(writer, index=False, sheet_name="data")


def run() -> None:
    refresh_field_dictionary()
    refresh_hypotheses_comments()
    print("refresh_intraday_extension_docs: done ->", THESIS_TABLES_DIR)


if __name__ == "__main__":
    run()

