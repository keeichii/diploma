"""
Трассировка одной сделки сквозь пайплайн: от исходного Excel до ma_deals_enriched.

Перепроверяет даты при переходе ma_deals → audit → table_2_1 → панели → enriched,
пересчитывает ключевые метрики теми же функциями, что ma_thesis_pipeline.

Выход: out/thesis/tables/deal_trace_source_row_<id>.md
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, List

import numpy as np
import pandas as pd

from . import ma_thesis_pipeline as mtp
from .paths import OUT_DIR, THESIS_TABLES_DIR, resolve_clean_data_file
from .runner import parse_date_any


def _hline() -> str:
    return "\n" + "-" * 80 + "\n"


def _fmt(v: float | Any) -> str:
    if pd.isna(v):
        return "NaN"
    if isinstance(v, (float, np.floating)):
        return f"{float(v):.8f}"
    return str(v)


def _find_table21_path() -> Path | None:
    for name in (
        "table_2_1_first_press_release-3.xlsx",
        "table_2_1_first_press_release.xlsx",
        "table_2_1_first_press_release.csv",
    ):
        p = OUT_DIR / name
        if p.is_file():
            return p
    return None


def _load_audit_row(source_row_excel: int) -> pd.Series | None:
    audit_xlsx = OUT_DIR / "ma_deals_AUDIT.xlsx"
    if not audit_xlsx.is_file():
        return None
    try:
        ad = pd.read_excel(audit_xlsx, sheet_name=None)
        sheet = next(iter(ad.values())) if isinstance(ad, dict) else ad
        key = "source_row_excel" if "source_row_excel" in sheet.columns else None
        if key is None:
            for c in sheet.columns:
                if "source" in str(c).lower() and "excel" in str(c).lower():
                    key = c
                    break
        if key is not None:
            m = sheet.loc[sheet[key] == source_row_excel]
            if not m.empty:
                return m.iloc[0]
        # Как в runner: Excel-строка = индекс + 2
        idx = int(source_row_excel) - 2
        if 0 <= idx < len(sheet):
            return sheet.iloc[idx]
        return None
    except Exception:
        return None


def trace_deal(source_row_excel: int) -> Path:
    out_md = THESIS_TABLES_DIR / f"deal_trace_source_row_{source_row_excel}.md"
    out_md.parent.mkdir(parents=True, exist_ok=True)

    enriched = resolve_clean_data_file("ma_deals_enriched.csv")
    enr_df = pd.read_csv(enriched, encoding="utf-8") if enriched.is_file() else pd.DataFrame()
    lines: List[str] = []
    lines.append(f"# Deal trace: source_row_excel={source_row_excel}\n")
    lines.append("## Цепочка данных: Excel → audit → Table 2.1 → панели → enriched\n")

    # --- Audit row (ma_deals_AUDIT) ---
    audit_row = _load_audit_row(source_row_excel)
    lines.append("## 1. Строка audit (ma_deals_AUDIT.xlsx)\n")
    if audit_row is None:
        lines.append("- Файл не найден или строка отсутствует.\n")
    else:
        for col in [
            "audit_first_press_release_date_parsed",
            "audit_release_anchor_trade_date",
            "audit_release_anchor_timestamp_msk",
            "audit_release_anchor_reason",
            "audit_resolved_ticker",
            "audit_notes",
        ]:
            if col in audit_row.index:
                v = audit_row[col]
                p2 = parse_date_any(v) if col != "audit_notes" else None
                extra = f" → parse_date_any={p2}" if p2 is not None else ""
                lines.append(f"- **{col}**: `{v}`{extra}")
        if "audit_first_press_release_date_parsed" in audit_row.index:
            rp = audit_row["audit_first_press_release_date_parsed"]
            at = audit_row.get("audit_release_anchor_trade_date")
            d_rp = parse_date_any(rp) if pd.notna(rp) else None
            d_at = parse_date_any(at) if pd.notna(at) else None
            if d_rp and d_at:
                lines.append(f"- **delta (anchor_trade − release parsed)**: {(d_at - d_rp).days} дней")
    lines.append(_hline())

    # --- Table 2.1 raw export ---
    t21 = _find_table21_path()
    lines.append("## 2. Table 2.1 (runner output: first_press_release)\n")
    if t21 is None:
        lines.append("- Файл table_2_1 не найден в out/.\n")
    else:
        lines.append(f"- Путь: `{t21}`\n")
        try:
            if t21.suffix.lower() == ".csv":
                tdf = pd.read_csv(t21, encoding="utf-8")
            else:
                tdf = pd.read_excel(t21)
            tdf.columns = [str(c).strip() for c in tdf.columns]
            key = "source_row_excel" if "source_row_excel" in tdf.columns else None
            if key:
                sub = tdf.loc[tdf[key] == source_row_excel]
                if sub.empty:
                    lines.append("- Нет строк с этим source_row_excel.\n")
                else:
                    r2 = sub.iloc[0]
                    for col in ["anchor_date", "anchor_date_raw", "anchor_trade_date", "event_name", "t", "Date"]:
                        if col in r2.index:
                            lines.append(f"- **{col}**: `{r2[col]}`")
                    ts = pd.to_numeric(sub.get("t"), errors="coerce")
                    if ts is not None and ts.notna().any():
                        lines.append(f"- **min(t)** в панели: `{ts.min()}` | **max(t)**: `{ts.max()}` | **n_rows**: `{len(sub)}`")
                    ad_col = "anchor_date" if "anchor_date" in r2.index else None
                    dt_col = "Date" if "Date" in r2.index else None
                    t0 = sub.loc[pd.to_numeric(sub.get("t"), errors="coerce") == 0] if "t" in sub.columns else pd.DataFrame()
                    if not t0.empty and ad_col and dt_col:
                        d_anchor = parse_date_any(r2.get("anchor_date"))
                        d_t0 = parse_date_any(t0.iloc[0].get("Date"))
                        lines.append(
                            f"- **Сверка t=0**: Date(t=0)={d_t0} vs anchor_date(колонка)={d_anchor} "
                            f"→ совпадение={'ДА' if d_anchor and d_t0 and d_anchor == d_t0 else 'НЕТ'}"
                        )
        except Exception as e:
            lines.append(f"- Ошибка чтения: `{e}`\n")
    lines.append(_hline())

    # --- Enriched ---
    lines.append("## 3. ma_deals_enriched.csv\n")
    if enr_df.empty or "source_row_excel" not in enr_df.columns:
        lines.append("- Нет enriched.\n")
        out_md.write_text("\n".join(lines), encoding="utf-8")
        return out_md
    row = enr_df.loc[enr_df["source_row_excel"] == source_row_excel]
    if row.empty:
        lines.append("- Сделка отсутствует в enriched.\n")
        out_md.write_text("\n".join(lines), encoding="utf-8")
        return out_md
    r = row.iloc[0]
    for col in [
        "announcement_date_std",
        "buyer_ticker_std",
        "deal_object_std",
        "announcement_date_std",
        "completion_date_std",
    ]:
        if col in r.index:
            lines.append(f"- **{col}**: `{r[col]}`")

    # --- Announcement panel recompute ---
    ann_panel_path = resolve_clean_data_file("announcement_daily_panel_clean.csv")
    if ann_panel_path.is_file():
        panel = pd.read_csv(ann_panel_path, encoding="utf-8")
        sub = panel.loc[panel["source_row_excel"] == source_row_excel].copy()
        lines.append(_hline())
        lines.append("## 4. announcement_daily_panel_clean — пересчёт CAR/BHAR\n")
        if sub.empty:
            lines.append("- Пусто.\n")
        elif "t" in sub.columns and "security_return" in sub.columns:
            sub["t"] = pd.to_numeric(sub["t"], errors="coerce")
            event_t = sub["t"]
            ar, model_type, n_est = mtp.build_daily_model(sub, event_t)
            car_1_1 = mtp.sum_window(ar, event_t, -1, 1)
            car_3_3 = mtp.sum_window(ar, event_t, -3, 3)
            bhar_120 = mtp.bhar_window(sub, event_t, 120)
            lines.append(f"- N rows: **{len(sub)}** | model: **{model_type}** | n_est={n_est}")
            lines.append(f"- min(t)={event_t.min()}, max(t)={event_t.max()}")
            lines.append(f"- CAR_ANN_1_1 recomputed={_fmt(car_1_1)} | stored={_fmt(r.get('CAR_ANN_1_1', np.nan))}")
            lines.append(f"- CAR_ANN_3_3 recomputed={_fmt(car_3_3)} | stored={_fmt(r.get('CAR_ANN_3_3', np.nan))}")
            lines.append(f"- BHAR_ANN_120 recomputed={_fmt(bhar_120)} | stored={_fmt(r.get('BHAR_ANN_120', np.nan))}")

    comp_metrics = mtp.compute_completion_metrics(
        enr_df[["source_row_excel", "completion_date_std"]].copy(),
        {
            "announcement_daily": pd.read_csv(
                resolve_clean_data_file("announcement_daily_panel_clean.csv"), encoding="utf-8"
            )
            if resolve_clean_data_file("announcement_daily_panel_clean.csv").is_file()
            else pd.DataFrame(),
            "actualization_daily": pd.read_csv(
                resolve_clean_data_file("actualization_daily_panel_clean.csv"), encoding="utf-8"
            )
            if resolve_clean_data_file("actualization_daily_panel_clean.csv").is_file()
            else pd.DataFrame(),
            "create_daily": pd.read_csv(
                resolve_clean_data_file("create_daily_panel_clean.csv"), encoding="utf-8"
            )
            if resolve_clean_data_file("create_daily_panel_clean.csv").is_file()
            else pd.DataFrame(),
        },
    )
    comp_row = comp_metrics.loc[comp_metrics["source_row_excel"] == source_row_excel]
    lines.append(_hline())
    lines.append("## 5. Completion metrics (recomputed vs enriched)\n")
    if comp_row.empty:
        lines.append("- Нет completion-строки.\n")
    else:
        c = comp_row.iloc[0]
        for col in [
            "CAR_CLOSE_1_1",
            "CAR_CLOSE_3_3",
            "CAR_CLOSE_5_5",
            "BHAR_CLOSE_120",
        ]:
            lines.append(f"- {col}: recomputed=`{_fmt(c.get(col, np.nan))}` stored=`{_fmt(r.get(col, np.nan))}`")

    intr_path = resolve_clean_data_file("intraday_panel_clean.csv")
    lines.append(_hline())
    lines.append("## 6. Intraday (enriched поля)\n")
    if intr_path.is_file():
        intr = pd.read_csv(intr_path, encoding="utf-8")
        sub_i = intr.loc[intr["source_row_excel"] == source_row_excel]
        lines.append(f"- intraday rows: {len(sub_i)}")
    for col in [
        "CAR_ANN_INTRADAY_15M",
        "CAR_INTRADAY_PRE_4_0",
        "ratio_1h_vs_day",
        "is_off_market_release",
    ]:
        if col in r.index:
            lines.append(f"- **{col}**: `{_fmt(r[col])}`")

    lines.append(_hline())
    lines.append("## 7. Фундаменталы ANN_*\n")
    for col in ["ANN_ROE", "ANN_ROA", "ANN_P_B", "Volume_bln_avg_pre"]:
        if col in r.index:
            lines.append(f"- {col}: `{_fmt(r[col])}`")

    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_md


def main(argv: list[str] | None = None) -> None:
    import sys

    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        print("Usage: python -m ma_event_study.deal_trace <source_row_excel>")
        return
    try:
        sid = int(argv[0])
    except ValueError:
        print("source_row_excel must be int")
        return
    p = trace_deal(sid)
    print("Deal trace written to", p)


if __name__ == "__main__":
    main()
