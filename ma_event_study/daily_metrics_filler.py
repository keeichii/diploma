from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any

from t_tech.invest.utils import quotation_to_decimal

from .financials_provider import FinancialsProvider
from .benchmarks_provider import BenchmarksProvider, BenchmarkMetricResult

logger = logging.getLogger("ma_event_study.daily_metrics_filler")


@dataclass(frozen=True)
class FilledDailyMetrics:
    adjusted_close: float | None
    close: float | None
    volume_bln_rub: float | None
    market_cap_bln_rub: float | None
    imoex_daily_close: float | None
    ruonia_daily: float | None
    roe: float | None
    roa: float | None
    pb: float | None
    pe: float | None
    total_assets: float | None


class DailyMetricsFiller:
    """
    Централизованное заполнение daily-таблиц (ТЗ) строго из:
    - Close/Volume: t_tech.invest daily candles
    - IMOEX: t_tech.invest (benchmarks via candles)
    - RUONIA: локальный financials (benchmark file) либо None
    - ROE/ROA/PB/PE/Total Assets/Market Cap/Adjusted Close: локальные financials
    """

    def __init__(
        self,
        *,
        financials: FinancialsProvider,
        benchmarks: BenchmarksProvider,
        adjusted_close_allow_close_fallback: bool,
        market_cap_shares_variant: str,
    ):
        self.financials = financials
        self.benchmarks = benchmarks
        self.adjusted_close_allow_close_fallback = adjusted_close_allow_close_fallback
        self.market_cap_shares_variant = market_cap_shares_variant

        self._missing_counter: dict[str, Counter[str]] = defaultdict(Counter)
        self._source_counter: dict[str, Counter[str]] = defaultdict(Counter)

    def _note(self, metric_col: str, reason: str, source: str) -> None:
        self._missing_counter[metric_col][reason] += 1
        self._source_counter[metric_col][source] += 1

    def _q_to_float(self, q: Any) -> float | None:
        if q is None:
            return None
        try:
            return float(quotation_to_decimal(q))
        except Exception:
            return None

    def _candle_turnover_rub(self, candle: Any) -> float | None:
        volume_shares = getattr(candle, "volume", None)
        if volume_shares is None:
            return None
        o = self._q_to_float(getattr(candle, "open", None))
        h = self._q_to_float(getattr(candle, "high", None))
        l = self._q_to_float(getattr(candle, "low", None))
        c = self._q_to_float(getattr(candle, "close", None))
        vals = [v for v in [o, h, l, c] if v is not None]
        if not vals:
            return None
        typical = sum(vals) / len(vals)
        return typical * float(volume_shares)

    def _select_shares_variant(self, instrument_ticker: str) -> str | None:
        if self.market_cap_shares_variant in {"ao", "ap"}:
            return self.market_cap_shares_variant
        if self.market_cap_shares_variant == "auto":
            t = (instrument_ticker or "").upper()
            # В ваших financials для Сбербанка есть AO/AP.
            # Обычные акции: "SBER..." обычно соответствуют AO; привилегированные — с суффиксом "P".
            if t.startswith("SB"):
                if t.endswith("P"):
                    return "ap"
                return "ao"
            return "ao"
        return None

    def fill_for_date(
        self,
        *,
        ticker: str,
        d: date,
        close_px: float | None,
        candle: Any,
    ) -> FilledDailyMetrics:
        # Close / Volume from API candles
        volume_rub = self._candle_turnover_rub(candle)
        volume_bln = None if volume_rub is None else float(volume_rub) / 1e9

        # Adjusted Close: from financials only (optional fallback)
        adjusted = self.financials.get_asof(ticker=ticker, metric_key="adjusted_close", d=d)
        if adjusted.value is not None:
            adjusted_close = adjusted.value
            adjusted_source = adjusted.source
        else:
            adjusted_close = None
            adjusted_source = adjusted.source
            if self.adjusted_close_allow_close_fallback and close_px is not None:
                adjusted_close = close_px
                adjusted_source = "financials_rebuilt"

        # Market Cap (methodologically honest):
        # 1) Try rebuild from Close * shares outstanding on each Date (no freezing).
        # 2) Fallback to ready market cap from financials if rebuild is impossible.
        mc_ready = self.financials.get_asof(ticker=ticker, metric_key="market_cap_bln_rub", d=d)
        market_cap_bln: float | None = None
        mc_source = mc_ready.source
        mc_reason: str = mc_ready.reason or "market_cap_unavailable"

        shares_variant = self._select_shares_variant(ticker)
        if shares_variant and close_px is not None:
            shares_key = "shares_ao_mln" if shares_variant == "ao" else "shares_ap_mln"
            shares = self.financials.get_asof(ticker=ticker, metric_key=shares_key, d=d)
            if shares.value is not None:
                # shares.value is in mln -> number of shares = mln * 1e6
                # market_cap_bln = close_rub * shares_count / 1e9
                #               = close_rub * (mln * 1e6) / 1e9
                #               = close_rub * mln / 1000
                market_cap_bln = float(close_px) * float(shares.value) / 1000.0
                mc_source = "financials_rebuilt"
                mc_reason = "financials_rebuilt"
            else:
                mc_reason = shares.reason or "market_cap_shares_unavailable"

        if market_cap_bln is None and mc_ready.value is not None:
            market_cap_bln = mc_ready.value
            mc_source = mc_ready.source
            mc_reason = mc_ready.reason or "market_cap_ready_metric_used"

        # Benchmarks
        imoex_res = self.benchmarks.get_imoex_asof(d)
        ruonia_res = self.benchmarks.get_ruonia_asof(d)

        # Fundamental metrics from financials
        roe = self.financials.get_asof(ticker=ticker, metric_key="roe_pct", d=d)
        roa = self.financials.get_asof(ticker=ticker, metric_key="roa_pct", d=d)
        pb_ready = self.financials.get_asof(ticker=ticker, metric_key="pb", d=d)
        pe_ready = self.financials.get_asof(ticker=ticker, metric_key="pe", d=d)
        total_assets = self.financials.get_asof(ticker=ticker, metric_key="total_assets_bln_rub", d=d)

        # Rebuild valuation multiples when components are available.
        # PB = Market Cap / Equity (both in bln rub -> dimensionless)
        equity = self.financials.get_asof(ticker=ticker, metric_key="equity_bln_rub", d=d)
        pb: float | None = None
        pb_source = pb_ready.source
        pb_reason = pb_ready.reason or "pb_unavailable"
        if market_cap_bln is not None and equity.value is not None and equity.value != 0:
            pb = market_cap_bln / equity.value
            pb_source = "financials_rebuilt"
            pb_reason = "financials_rebuilt"
        elif pb_ready.value is not None:
            pb = pb_ready.value
            pb_source = pb_ready.source
            pb_reason = pb_ready.reason or "pb_ready_metric_used"

        # PE:
        # Prefer Close / EPS (EPS in rub/share).
        # Fallback to Market Cap / Net Income (both in bln rub).
        eps = self.financials.get_asof(ticker=ticker, metric_key="eps_rub", d=d)
        net_income = self.financials.get_asof(ticker=ticker, metric_key="net_income_bln_rub", d=d)
        pe: float | None = None
        pe_source = pe_ready.source
        pe_reason = pe_ready.reason or "pe_unavailable"
        if close_px is not None and eps.value is not None and eps.value != 0:
            pe = float(close_px) / float(eps.value)
            pe_source = "financials_rebuilt"
            pe_reason = "financials_rebuilt"
        elif market_cap_bln is not None and net_income.value is not None and net_income.value != 0:
            pe = market_cap_bln / float(net_income.value)
            pe_source = "financials_rebuilt"
            pe_reason = "financials_rebuilt"
        elif pe_ready.value is not None:
            pe = pe_ready.value
            pe_source = pe_ready.source
            pe_reason = pe_ready.reason or "pe_ready_metric_used"

        # Source tracking + reason for None
        if close_px is None:
            self._note("Close", "close_missing_in_candles", "api_candles")
        if volume_bln is None:
            self._note("Volume, bln RUB", "volume_turnover_missing_in_candles", "api_candles")

        if adjusted_close is None:
            self._note("Adjusted Close", "adjusted_close_unavailable", adjusted_source)
        if market_cap_bln is None:
            self._note("Market Capitalization", mc_reason or "market_cap_unavailable", mc_source)
        if imoex_res.value is None:
            self._note("IMOEX daily close", imoex_res.reason or "benchmark_unavailable", imoex_res.source)
        if ruonia_res.value is None:
            self._note("RUONIA daily", ruonia_res.reason or "benchmark_unavailable", ruonia_res.source)
        if roe.value is None:
            self._note("ROE", roe.reason or "roe_unavailable", roe.source)
        if roa.value is None:
            self._note("ROA", roa.reason or "roa_unavailable", roa.source)
        if pb is None:
            self._note("P/B", pb_reason or "pb_unavailable", pb_source)
        if pe is None:
            self._note("P/E", pe_reason or "pe_unavailable", pe_source)
        if total_assets.value is None:
            self._note("Total Assets", total_assets.reason or "assets_unavailable", total_assets.source)

        return FilledDailyMetrics(
            adjusted_close=adjusted_close,
            close=close_px,
            volume_bln_rub=volume_bln,
            market_cap_bln_rub=market_cap_bln,
            imoex_daily_close=float(imoex_res.value) if imoex_res.value is not None else None,
            ruonia_daily=float(ruonia_res.value) if ruonia_res.value is not None else None,
            roe=roe.value,
            roa=roa.value,
            pb=pb,
            pe=pe,
            total_assets=total_assets.value,
        )

    def log_summary(self) -> None:
        if not self._missing_counter:
            return
        logger.info("FIELD_MISSING_SUMMARY (daily tables)")
        for metric, cnt in self._missing_counter.items():
            sources = self._source_counter.get(metric, Counter())
            logger.info(
                "%s | missing=%s | top_reasons=%s | top_sources=%s",
                metric,
                sum(cnt.values()),
                dict(cnt.most_common(6)),
                dict(sources.most_common(6)),
            )

