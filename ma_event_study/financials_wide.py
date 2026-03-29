from __future__ import annotations

import csv
import re
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


def parse_maybe_date(s: str) -> date | None:
    s = (s or "").strip()
    if not s:
        return None
    if s.lower() in {"nan", "none"}:
        return None
    # формат в файлах: dd.mm.YYYY
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()


def parse_number_maybe(x: Any) -> float | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return None
    # убираем кавычки/пробелы тысяч
    s = s.replace("\xa0", " ").replace(" ", "")
    # проценты
    s = s.replace("%", "")
    # десятичная запятая
    s = s.replace(",", ".")
    # оставляем только числооподобные символы
    s = re.sub(r"[^0-9.\-+eE]", "", s)
    if s in {"", ".", "-", "+", "-.", "+."}:
        return None
    try:
        return float(s)
    except Exception:
        return None


@dataclass(frozen=True)
class MetricSeries:
    dates: list[date]
    values: list[float]

    def get_asof(self, d: date) -> float | None:
        if not self.dates:
            return None
        idx = bisect_right(self.dates, d) - 1
        if idx < 0:
            return None
        return self.values[idx]


def _normalize_metric_name(s: str) -> str:
    s = (s or "").strip().upper()
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s


class FinancialsWideParser:
    """
    Парсер wide-format финансовых CSV:
    - разделитель: ';'
    - первая колонка: названия метрик
    - строка "Дата отчета" задаёт report_date по колонкам (period columns)
    - значения каждой метрики лежат по этим колонкам
    """

    def __init__(self, path: Path):
        self.path = path

    def parse(self) -> dict[str, MetricSeries]:
        text = self.path.read_text(encoding="utf-8-sig", errors="replace")
        reader = csv.reader(text.splitlines(), delimiter=";", quotechar='"')
        rows = [row for row in reader if row and any(str(c).strip() for c in row)]

        if len(rows) < 3:
            return {}

        # Ищем строку с "Дата отчета" по первой колонке.
        report_date_row = None
        for row in rows[:10]:
            first = (row[0] or "").strip().strip('"')
            if "Дата отчета" in first:
                report_date_row = row
                break
        if report_date_row is None:
            return {}

        # Карта колонка -> report_date
        # В файле первая колонка — имя метрики, поэтому индексы с 1..N
        col_dates: list[date | None] = [None] * len(report_date_row)
        for j in range(1, len(report_date_row)):
            col_dates[j] = parse_maybe_date(report_date_row[j])

        metric_to_series: dict[str, dict[date, float]] = {}

        for row in rows:
            metric_raw = (row[0] or "").strip()
            if not metric_raw:
                continue
            metric_name = _normalize_metric_name(metric_raw.strip('"'))

            # значения начинаются с 1-й колонки
            for j in range(1, min(len(row), len(col_dates))):
                d = col_dates[j]
                if d is None:
                    continue
                v = parse_number_maybe(row[j])
                if v is None:
                    continue

                key = self._map_metric_key(metric_name)
                if not key:
                    continue

                metric_to_series.setdefault(key, {})[d] = v

        # собираем MetricSeries с сортировкой по датам
        out: dict[str, MetricSeries] = {}
        for key, d2v in metric_to_series.items():
            dates_sorted = sorted(d2v.keys())
            values_sorted = [d2v[d] for d in dates_sorted]
            out[key] = MetricSeries(dates=dates_sorted, values=values_sorted)
        return out

    def _map_metric_key(self, metric_name_upper: str) -> str | None:
        # Ключи, которые нам нужны для ТЗ daily-таблиц
        if "КАПИТАЛИЗАЦ" in metric_name_upper and "МЛРД" in metric_name_upper:
            return "market_cap_bln_rub"
        # "Капитал" (equity/book value). Важно: не путать с "Капитализация"
        if "КАПИТАЛ" in metric_name_upper and "МЛРД" in metric_name_upper and "КАПИТАЛИЗАЦ" not in metric_name_upper:
            return "equity_bln_rub"
        # EPS (прибыль на акцию), руб
        if metric_name_upper.startswith("EPS"):
            return "eps_rub"
        # Чистая прибыль (для PE по net income)
        if "ЧИСТАЯ ПРИБЫЛЬ" in metric_name_upper and "МЛРД" in metric_name_upper:
            return "net_income_bln_rub"
        if "ЧИСЛО АКЦИЙ АО" in metric_name_upper:
            return "shares_ao_mln"
        if "ЧИСЛО АКЦИЙ АП" in metric_name_upper:
            return "shares_ap_mln"
        if metric_name_upper.startswith("ROE"):
            return "roe_pct"
        if metric_name_upper.startswith("ROA"):
            return "roa_pct"
        if metric_name_upper.startswith("P/B") or "P/B" in metric_name_upper:
            return "pb"
        if metric_name_upper.startswith("P/E") or "P/E" in metric_name_upper:
            return "pe"
        if "АКТИВЫ" in metric_name_upper and "МЛРД" in metric_name_upper:
            return "total_assets_bln_rub"
        # Adjusted close: в текущих CSV, которые вы показали, его нет отдельным рядом.
        # Оставляем обработку на будущее.
        if "ADJUSTED" in metric_name_upper or "ADJ CLOSE" in metric_name_upper:
            return "adjusted_close"
        if "АДЖ" in metric_name_upper and "ЗАКР" in metric_name_upper:
            return "adjusted_close"
        return None

