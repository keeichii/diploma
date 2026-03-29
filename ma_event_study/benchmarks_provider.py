from __future__ import annotations

import logging
import pickle
from dataclasses import dataclass
from datetime import date, timezone
from typing import Any

from zoneinfo import ZoneInfo

import pandas as pd

from t_tech.invest import CandleInterval
from t_tech.invest.schemas import IndicativesRequest
from t_tech.invest.utils import quotation_to_decimal

from t_tech.invest import Client

from .financials_wide import MetricSeries, FinancialsWideParser

logger = logging.getLogger("ma_event_study.benchmarks_provider")

MSK = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class BenchmarkMetricResult:
    value: float | None
    source: str
    reason: str | None = None


def _candle_date_msk(candle: Any) -> date:
    t = getattr(candle, "time", None)
    if t is None:
        raise ValueError("candle.time is None")
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.astimezone(MSK).date()


def _q_to_float(q: Any) -> float | None:
    if q is None:
        return None
    try:
        return float(quotation_to_decimal(q))
    except Exception:
        return None


def _start_of_day_msk(d: date):
    from datetime import datetime

    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=MSK)


def _end_of_day_msk(d: date):
    from datetime import datetime

    return datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=MSK)


class BenchmarksProvider:
    """
    Провайдер бенчмарков:
    - IMOEX daily close: исторические свечи (api_candles) из t_tech.invest
    - RUONIA daily: только локальные financials (benchmark_file). Если файла/метрики нет, None.
    """

    def __init__(self, financials_dir: Any, *, cache_dir: Any = None):
        self.financials_dir = financials_dir
        self.cache_dir = cache_dir
        self._imoex_series: MetricSeries | None = None
        self._ruonia_series: MetricSeries | None = None
        self._ruonia_loaded = False
        self._imoex_loaded = False

    def preload_imoex(self, client: Client, from_d: date, to_d: date) -> None:
        """
        Загружает исторический ряд IMOEX daily close одним диапазоном.
        """
        safe_cache_path = None
        if self.cache_dir:
            from pathlib import Path

            safe_cache_path = Path(self.cache_dir) / f"imoex_daily_close_{from_d.isoformat()}_{to_d.isoformat()}.pkl"
            if safe_cache_path.exists():
                try:
                    with safe_cache_path.open("rb") as f:
                        self._imoex_series = pickle.load(f)
                        self._imoex_loaded = True
                        return
                except Exception as e:
                    logger.warning("IMOEX_CACHE_READ_FAILED | path=%s | error=%s", safe_cache_path, e)

        self._load_imoex_series(client=client, from_d=from_d, to_d=to_d)

        if safe_cache_path:
            try:
                with safe_cache_path.open("wb") as f:
                    pickle.dump(self._imoex_series, f)
            except Exception as e:
                logger.warning("IMOEX_CACHE_WRITE_FAILED | path=%s | error=%s", safe_cache_path, e)

    def _find_imoex_instrument(self, client: Client) -> str | None:
        resp = client.instruments.indicatives(request=IndicativesRequest())
        # Ищем по названию/тикеру
        candidates = []
        for inst in getattr(resp, "instruments", []) or []:
            name = (getattr(inst, "name", "") or "").upper()
            ticker = (getattr(inst, "ticker", "") or "").upper()
            if "IMOEX" in name or "ИМОВЕ" in name or "МОСБИРЖ" in name or "IMOEX" in ticker:
                candidates.append(inst)
        if not candidates:
            return None
        # предпочтение: явное вхождение IMOEX
        candidates.sort(key=lambda x: (0 if "IMOEX" in (getattr(x, "name", "") or "").upper() else 1))
        inst = candidates[0]
        return str(getattr(inst, "figi", None) or getattr(inst, "uid", None) or "")

    def _load_imoex_series(self, client: Client, from_d: date, to_d: date, cache_dir: Any = None) -> None:
        if self._imoex_loaded:
            return
        instrument_id = self._find_imoex_instrument(client)
        if not instrument_id:
            self._imoex_series = None
            self._imoex_loaded = True
            return

        # Запрашиваем дневные свечи одним диапазоном. Индикативные инструменты обычно торгуются как индекс.
        try:
            iterator = client.get_all_candles(
                instrument_id=instrument_id,
                from_=_start_of_day_msk(from_d),
                to=_end_of_day_msk(to_d),
                interval=CandleInterval.CANDLE_INTERVAL_DAY,
            )
            candles = list(iterator)
        except Exception as e:
            # Fallback на market_data endpoint — используется аналогично runner'у.
            # Это должно быть более совместимо с вариантами SDK.
            logger.warning("IMOEX_GET_ALL_CANDLES_FAILED | error=%s", e)
            resp = client.market_data.get_candles(  # type: ignore[attr-defined]
                instrument_id=instrument_id,
                from_=_start_of_day_msk(from_d),
                to=_end_of_day_msk(to_d),
                interval=CandleInterval.CANDLE_INTERVAL_DAY,
            )
            candles = list(getattr(resp, "candles", []) or [])

        d2v: dict[date, float] = {}
        for c in candles:
            d = _candle_date_msk(c)
            close = _q_to_float(getattr(c, "close", None))
            if close is None:
                continue
            d2v[d] = close

        dates_sorted = sorted(d2v.keys())
        values_sorted = [d2v[d] for d in dates_sorted]
        self._imoex_series = MetricSeries(dates=dates_sorted, values=values_sorted)
        self._imoex_loaded = True

    def _load_ruonia_series_from_financials(self) -> None:
        if self._ruonia_loaded:
            return
        self._ruonia_loaded = True
        if not self.financials_dir or not self.financials_dir.exists():
            return
        # Ищем файл, где в имени RUONIA
        file = None
        for p in self.financials_dir.iterdir():
            if p.is_file() and p.suffix.lower() == ".csv" and "RUONIA" in p.name.upper():
                file = p
                break
        if not file:
            self._ruonia_series = None
            return
        parser = FinancialsWideParser(file)
        metric_map = parser.parse()
        # Ищем любые ключи, которые могут соответствовать RUONIA.
        # На текущий момент у нас нет примера RUONIA-файла, поэтому ищем эвристически.
        for key in ["ruonia", "ruonia_daily", "ruonia_rate", "RUONIA", "RUONIA_DAILY"]:
            if key in metric_map:
                self._ruonia_series = metric_map[key]  # type: ignore[assignment]
                return
        self._ruonia_series = None

    def get_imoex_asof(self, d: date) -> BenchmarkMetricResult:
        if not self._imoex_series:
            return BenchmarkMetricResult(value=None, source="unavailable", reason="benchmark_series_missing")
        v = self._imoex_series.get_asof(d)
        if v is None:
            return BenchmarkMetricResult(value=None, source="unavailable", reason="benchmark_no_asof_value")
        return BenchmarkMetricResult(value=float(v), source="api_benchmark")

    def get_ruonia_asof(self, d: date) -> BenchmarkMetricResult:
        if not self._ruonia_loaded:
            self._load_ruonia_series_from_financials()
        if not self._ruonia_series:
            return BenchmarkMetricResult(value=None, source="unavailable", reason="benchmark_series_missing")
        v = self._ruonia_series.get_asof(d)
        if v is None:
            return BenchmarkMetricResult(value=None, source="unavailable", reason="benchmark_no_asof_value")
        return BenchmarkMetricResult(value=float(v), source="benchmark_file")

