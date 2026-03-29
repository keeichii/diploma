#!/usr/bin/env python3
"""
Предстартовая проверка путей и ключевых входных файлов (без вызова API).
Запуск: python ma_event_study/check_project.py
"""
from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    errors: list[str] = []
    warnings: list[str] = []

    # Конфиг
    cfg_path = root / "ma_event_study" / "config.toml"
    if not cfg_path.is_file():
        errors.append(f"Нет конфига: {cfg_path}")
    else:
        try:
            import tomllib

            with cfg_path.open("rb") as f:
                raw = tomllib.load(f)
            inp = raw.get("input", {})
            xlsx_rel = inp.get("xlsx", "../data/input/ma_deals.xlsx")
            input_xlsx = (cfg_path.parent / xlsx_rel).resolve()
            if not input_xlsx.is_file():
                errors.append(f"input.xlsx из config.toml не найден: {input_xlsx}")
            else:
                print(f"OK input deals: {input_xlsx}")
        except Exception as e:
            warnings.append(f"Не удалось прочитать config.toml: {e}")

    ruonia = root / "data" / "input" / "ruonia and else.xlsx"
    if not ruonia.is_file():
        warnings.append(f"Нет файла RUONIA для merge_ruonia_dt.py: {ruonia}")
    else:
        print(f"OK RUONIA source: {ruonia}")

    out = root / "out"
    need_tables = [
        "table_2_1_first_press_release.xlsx",
        "table_2_2_cbonds_actualization.xlsx",
        "table_2_3_cbonds_create.xlsx",
    ]
    missing = [n for n in need_tables if not (out / n).is_file()]
    if missing:
        warnings.append(
            "Нет готовых daily-таблиц в out/ (нужен прогон pipeline): " + ", ".join(missing)
        )
    else:
        print("OK out/table_2_*.xlsx присутствуют")

    token = __import__("os").environ.get("INVEST_TOKEN", "").strip()
    if not token:
        warnings.append("INVEST_TOKEN не задан — python -m ma_event_study не сможет вызвать API.")

    for w in warnings:
        print("WARN:", w)
    for e in errors:
        print("ERROR:", e)

    if errors:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
