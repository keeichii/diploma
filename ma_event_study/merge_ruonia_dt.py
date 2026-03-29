from __future__ import annotations

from pathlib import Path

import pandas as pd


REQUIRED = ["DT", "ruo"]
OPTIONAL = ["vol", "T", "C", "MinRate", "Percentile25", "Percentile75", "MaxRate", "StatusXML", "DateUpdate"]


def load_ruonia_table(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Файл RUONIA не найден: {path}")
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)
    df.columns = [str(c).strip() for c in df.columns]
    lower = {c.lower(): c for c in df.columns}
    if "dt" not in lower:
        raise ValueError("В выгрузке ЦБ должна быть колонка DT (дата).")
    df = df.rename(columns={lower["dt"]: "DT"})
    if "ruo" in lower and "RUONIA" not in df.columns:
        df = df.rename(columns={lower["ruo"]: "RUONIA"})
    elif "ruonia" in lower and "RUONIA" not in df.columns:
        df = df.rename(columns={lower["ruonia"]: "RUONIA"})
    if "RUONIA" not in df.columns:
        raise ValueError("RUONIA input must contain 'ruo' or 'RUONIA'")
    df["DT"] = pd.to_datetime(df["DT"], errors="coerce").dt.date
    df["RUONIA"] = pd.to_numeric(df["RUONIA"], errors="coerce")
    df = df.dropna(subset=["DT"]).drop_duplicates(subset=["DT"], keep="last").sort_values("DT")
    return df


def decide_optional_columns(df: pd.DataFrame) -> tuple[list[str], list[tuple[str, str]]]:
    added = ["DT", "RUONIA"]
    rejected: list[tuple[str, str]] = []
    for c in OPTIONAL:
        if c not in df.columns:
            rejected.append((c, "absent in source"))
            continue
        s = df[c]
        non_null = s.notna().mean()
        # Keep only diagnostically useful fields with non-trivial fill.
        if c in {"StatusXML", "DateUpdate"} and non_null > 0.2:
            added.append(c)
        elif c in {"MinRate", "Percentile25", "Percentile75", "MaxRate"} and non_null > 0.5:
            added.append(c)
        elif c in {"vol"} and non_null > 0.5:
            added.append(c)
        elif c in {"T", "C"} and non_null > 0.8:
            added.append(c)
        else:
            rejected.append((c, "not needed for core model/qa under current fill/utility"))
    return added, rejected


RUONIA_DAILY_COL = "RUONIA (daily)"


def merge_into_table(df_table: pd.DataFrame, ru: pd.DataFrame, add_cols: list[str]) -> pd.DataFrame:
    """
    Подтягивает RUONIA с ЦБ в единый столбец «RUONIA (daily)», без дубля «RUONIA» + пустой daily.
    """
    out = df_table.copy()
    if "Date" not in out.columns:
        return out
    out["_DT_merge"] = pd.to_datetime(out["Date"], errors="coerce").dt.date
    ru_sub = ru[["DT", "RUONIA"]].copy()
    ru_sub = ru_sub.drop_duplicates(subset=["DT"], keep="last")
    ru_sub = ru_sub.rename(columns={"RUONIA": "_ruonia_cb"})
    merged = out.merge(ru_sub, left_on="_DT_merge", right_on="DT", how="left")
    merged = merged.drop(columns=["_DT_merge", "DT"], errors="ignore")

    cb = pd.to_numeric(merged["_ruonia_cb"], errors="coerce")
    if RUONIA_DAILY_COL in merged.columns:
        old = pd.to_numeric(merged[RUONIA_DAILY_COL], errors="coerce")
        merged[RUONIA_DAILY_COL] = cb.combine_first(old)
    else:
        merged[RUONIA_DAILY_COL] = cb
    merged = merged.drop(columns=["_ruonia_cb"], errors="ignore")

    # На случай старых файлов с отдельным столбцом «RUONIA» после предыдущих прогонов
    if "RUONIA" in merged.columns and "RUONIA" != RUONIA_DAILY_COL:
        extra = pd.to_numeric(merged["RUONIA"], errors="coerce")
        merged[RUONIA_DAILY_COL] = extra.combine_first(pd.to_numeric(merged[RUONIA_DAILY_COL], errors="coerce"))
        merged = merged.drop(columns=["RUONIA"])

    return merged


def run() -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = root / "out"
    ru_path = root / "data" / "input" / "ruonia and else.xlsx"
    if not ru_path.is_file():
        raise FileNotFoundError(
            f"Ожидается файл ЦБ с RUONIA: {ru_path}\n"
            "Положите выгрузку (например «ruonia and else.xlsx») в data/input/."
        )
    ru = load_ruonia_table(ru_path)
    add_cols, rej = decide_optional_columns(ru)

    targets = [
        out_dir / "table_2_1_first_press_release.xlsx",
        out_dir / "table_2_1_first_press_release-3.xlsx",
        out_dir / "table_2_2_cbonds_actualization.xlsx",
        out_dir / "table_2_3_cbonds_create.xlsx",
    ]
    targets = [p for p in targets if p.exists()]
    save_dir = out_dir / "ruonia_augmented"
    save_dir.mkdir(parents=True, exist_ok=True)

    quality_lines = []
    quality_lines.append("RUONIA MERGE REPORT")
    quality_lines.append("=" * 80)
    quality_lines.append(f"RUONIA source: {ru_path}")
    quality_lines.append(f"Source rows: {len(ru)}; unique DT: {ru['DT'].nunique()}")
    quality_lines.append(f"RUONIA non-null share: {ru['RUONIA'].notna().mean():.2%}")
    quality_lines.append("")
    quality_lines.append("Added columns:")
    for c in add_cols:
        quality_lines.append(f"- {c}")
    quality_lines.append("")
    quality_lines.append("Rejected columns:")
    for c, why in rej:
        quality_lines.append(f"- {c}: {why}")
    quality_lines.append("")
    quality_lines.append("Merge logic:")
    quality_lines.append("- left join on table Date (converted to date) == RUONIA DT")
    quality_lines.append("- original rows preserved (no drops)")
    quality_lines.append("- non-matched dates keep NaN in added RUONIA fields")
    quality_lines.append("")

    if not targets:
        quality_lines.append("WARNING: нет ни одного table_2_*.xlsx в out/ — обработка пропущена.")
        quality_lines.append("Сначала сгенерируйте таблицы: python -m ma_event_study --config ma_event_study/config.toml")
        (out_dir / "ruonia_merge_report.txt").write_text("\n".join(quality_lines), encoding="utf-8")
        from final_outputs import copy_final_outputs

        copy_final_outputs(out_dir)
        print("RUONIA merge: нет целевых файлов.")
        print(out_dir / "ruonia_merge_report.txt")
        return

    for p in targets:
        df = pd.read_excel(p)
        merged = merge_into_table(df, ru, add_cols)
        merged.to_excel(save_dir / p.name, index=False)
        fill = merged[RUONIA_DAILY_COL].notna().mean() if RUONIA_DAILY_COL in merged.columns else 0.0
        quality_lines.append(f"{p.name}: rows={len(merged)}, RUONIA_fill={fill:.2%}")

    (out_dir / "ruonia_merge_report.txt").write_text("\n".join(quality_lines), encoding="utf-8")

    from final_outputs import copy_final_outputs

    fin = copy_final_outputs(out_dir)
    print("Done RUONIA merge.")
    print(save_dir)
    print(out_dir / "ruonia_merge_report.txt")
    print("Final copies:", fin)


if __name__ == "__main__":
    run()
