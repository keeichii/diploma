from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .financials_wide import FinancialsWideParser, MetricSeries

logger = logging.getLogger("ma_event_study.financials_provider")


@dataclass(frozen=True)
class FinancialMetricResult:
    value: float | None
    source: str
    reason: str | None = None


class FinancialsProvider:
    """
    Провайдер финансовых метрик из локальных wide-format CSV:
    as-of правило: report_date <= Date.
    """

    def __init__(self, financials_dir: Path | None):
        self.financials_dir = financials_dir
        self._cache_by_file: dict[Path, dict[str, MetricSeries]] = {}
        self._cache_by_ticker: dict[str, Path | None] = {}

        # Эвристические подсказки по имени файлов относительно тикеров.
        self.ticker_hints: dict[str, list[str]] = {
            "SBER": ["Сбербанк", "Сбербанк-МСФО", "Сбер", "Сбербанк"],
            "SVCB": ["Совкомбанк", "Совкомбанк-МСФО", "Совком"],
            "T": ["Т_Технологии", "Тинькофф", "Т Банк", "Т_Технологии_Тинькофф_ТКС_МСФО"],
            "YDEX": ["Яндекс", "Яндекс-МСФО", "Yandex"],
            "OZON": ["ОЗОН", "OZON"],
            "MBNK": ["МТС Банк", "МТС", "МТС-Банк", "MTS", "MTS Банк"],
            "RENI": ["Ренессанс", "RENI"],
            "ROSB": ["Росбанк", "РОСБАНК", "ROSB"],
            "ACBR": ["Альфа", "Alfa", "ACBR"],
        }

    def _find_financial_file_for_ticker(self, ticker: str) -> Path | None:
        if not self.financials_dir or not self.financials_dir.exists():
            return None

        if ticker in self._cache_by_ticker:
            return self._cache_by_ticker[ticker]

        hints = self.ticker_hints.get(ticker, [ticker])
        candidates = [p for p in self.financials_dir.iterdir() if p.is_file() and p.suffix.lower() in {".csv"}]
        scored: list[tuple[int, Path]] = []
        for p in candidates:
            name = p.name.upper()
            score = sum(1 for h in hints if str(h).upper() in name)
            if score:
                scored.append((score, p))
        if not scored:
            self._cache_by_ticker[ticker] = None
            return None
        scored.sort(key=lambda x: (-x[0], x[1].name))
        chosen = scored[0][1]
        self._cache_by_ticker[ticker] = chosen
        return chosen

    def _load_metric_map(self, fpath: Path) -> dict[str, MetricSeries]:
        if fpath in self._cache_by_file:
            return self._cache_by_file[fpath]
        parser = FinancialsWideParser(fpath)
        metric_map = parser.parse()
        self._cache_by_file[fpath] = metric_map
        return metric_map

    def _get_series(self, ticker: str, metric_key: str) -> MetricSeries | None:
        fpath = self._find_financial_file_for_ticker(ticker)
        if not fpath:
            return None
        metric_map = self._load_metric_map(fpath)
        return metric_map.get(metric_key)

    def get_asof(
        self,
        *,
        ticker: str,
        metric_key: str,
        d: date,
    ) -> FinancialMetricResult:
        series = self._get_series(ticker, metric_key)
        if series is None:
            return FinancialMetricResult(value=None, source="unavailable", reason="financials_missing_metric")

        value = series.get_asof(d)
        if value is None:
            return FinancialMetricResult(value=None, source="unavailable", reason="financials_no_asof_value")

        return FinancialMetricResult(value=value, source="financials_ready_metric")

