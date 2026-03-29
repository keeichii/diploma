"""
README
======

This script builds a thesis-ready empirical pipeline for M&A deals in the
Russian banking sector using the uploaded Excel files in the project root.

Files and roles
---------------
- `ma_deals_AUDIT.xlsx`: base deal-level dataset, one row per deal.
- `table_1_intraday.xlsx`: 15-minute intraday prices around the first press
  release; used for announcement intraday event study.
- `table_2_1_first_press_release.xlsx`: daily prices and fundamentals around
  the announcement date; used for announcement daily CAR/BHAR and controls.
- `table_2_2_cbonds_actualization.xlsx`: daily prices and fundamentals around
  the actualization date; used for robustness checks.
- `table_2_3_cbonds_create.xlsx`: daily prices and fundamentals around the
  creation / earliest mention date; used for leakage analysis.

Matching logic
--------------
- Matching priority is `source_row_excel`.
- Each panel is additionally checked against `buyer_ticker`, `buyer_company`,
  and `deal_object`.
- If a source row has multiple key combinations or keys disagree with the base
  dataset, the row is marked as `ambiguous` and excluded from calculations.

Missing dates and times
-----------------------
- Intraday announcement CAR is anchored on `anchor_timestamp_msk` from the
  intraday panel. If base announcement time is missing but the intraday anchor
  exists, the deal is still used.
- Missing completion time -> deal is excluded from intraday completion CAR.
- Missing completion date -> deal is still used for announcement analysis.
- There is no dedicated intraday completion panel among uploaded files, so all
  completion intraday metrics remain missing and are explicitly warned about.
- For daily completion analysis, the script re-anchors the available daily
  quote panel that contains the completion date. Priority:
  announcement panel -> actualization panel -> creation panel.

Methodology summary
-------------------
- Daily event study:
  estimation window = (-250, -30) trading days.
- Daily abnormal returns:
  market model when enough estimation observations and benchmark exist,
  otherwise market-adjusted, otherwise mean-adjusted.
- BHAR:
  compounded stock return minus compounded benchmark return over +1..+H days.
- Intraday abnormal returns:
  15-minute stock return minus the average 15-minute return on the pre-event
  trading day because no intraday benchmark series is available in the upload.

How to run
----------
- Установите зависимости (`pip install -r ma_event_study/requirements.txt`).
- Сначала соберите таблицы в `out/` (модуль `ma_event_study` + при необходимости merge RUONIA и CAR-скрипты).
- Затем:
  `python ma_event_study/ma_thesis_pipeline.py`

Outputs
-------
- `out/thesis/clean_data`: очищенные панели и обогащённый датасет сделок.
- `out/thesis/mapping_audit.csv` и прочие таблицы в `out/thesis/tables`.
- `out/thesis/charts`, `out/thesis/models`.
"""

from __future__ import annotations

import os
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("MPLCONFIGDIR", str(PROJECT_ROOT / ".mplconfig"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy import stats
try:
    from ma_event_study.event_study_formulas import bhar_post_window
except ImportError:
    from event_study_formulas import bhar_post_window

try:
    from ma_event_study.event_study_config import get_estimation_window
except ImportError:
    from event_study_config import get_estimation_window


OUT_INPUT_DIR = PROJECT_ROOT / "out"
OUTPUT_ROOT = PROJECT_ROOT / "out" / "thesis"
OUTPUT_DIRS = {
    "clean_data": OUTPUT_ROOT / "clean_data",
    "tables": OUTPUT_ROOT / "tables",
    "charts": OUTPUT_ROOT / "charts",
    "models": OUTPUT_ROOT / "models",
}


def print_header(title: str) -> None:
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def normalize_label(value: object) -> str:
    if pd.isna(value):
        return ""
    value = str(value).strip().lower().replace("ё", "е")
    value = re.sub(r"[^0-9a-zа-я]+", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_text(value: object) -> str:
    return normalize_label(value)


def normalize_ticker(value: object) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", "", str(value).strip().upper())


def parse_numeric_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    cleaned = (
        series.astype(str)
        .str.replace("\xa0", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    cleaned = cleaned.replace({"nan": np.nan, "None": np.nan, "": np.nan})
    return pd.to_numeric(cleaned, errors="coerce")


def parse_date_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series, errors="coerce")
    as_text = series.astype(str)
    iso_like = as_text.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False).mean() > 0.8
    return pd.to_datetime(series, errors="coerce", dayfirst=not iso_like)


def parse_time_value(value: object) -> Optional[str]:
    if pd.isna(value):
        return None
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%H:%M")
        except Exception:
            pass
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    match = re.search(r"(\d{1,2}):(\d{2})", text)
    if match:
        hh = int(match.group(1))
        mm = int(match.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f"{hh:02d}:{mm:02d}"
    return None


def detect_column(columns: Iterable[str], aliases: Iterable[str], required: bool = True) -> Optional[str]:
    columns = list(columns)
    norm_map = {normalize_label(col): col for col in columns}
    for alias in aliases:
        alias_norm = normalize_label(alias)
        if alias_norm in norm_map:
            return norm_map[alias_norm]
    for alias in aliases:
        tokens = set(normalize_label(alias).split())
        if not tokens:
            continue
        matches = [col for col in columns if tokens.issubset(set(normalize_label(col).split()))]
        if len(matches) > 1:
            raise KeyError(
                f"Ambiguous column for aliases={list(aliases)}: multiple matches {matches}. Specify column name explicitly."
            )
        if len(matches) == 1:
            return matches[0]
    if required:
        raise KeyError(f"Could not detect column for aliases={list(aliases)}")
    return None


def ensure_output_dirs() -> None:
    for path in OUTPUT_DIRS.values():
        path.mkdir(parents=True, exist_ok=True)


def _drop_pipeline_stub_rows(raw: pd.DataFrame) -> pd.DataFrame:
    """Убирает строки-заглушки (непустой pipeline_row_note), оставшиеся в xlsx для полноты выгрузки."""
    col = next(
        (c for c in raw.columns if str(c).strip().lower().replace(" ", "_") == "pipeline_row_note"),
        None,
    )
    if col is None:
        return raw
    s = raw[col]
    bad = s.notna() & s.astype(str).str.strip().ne("") & ~s.astype(str).str.strip().str.lower().isin(["nan", "<na>"])
    return raw.loc[~bad].drop(columns=[col], errors="ignore").reset_index(drop=True)


@dataclass
class WarningRecord:
    category: str
    message: str
    table_role: Optional[str] = None
    source_row_excel: Optional[int] = None


WARNING_LOG: List[WarningRecord] = []


def log_warning(category: str, message: str, table_role: Optional[str] = None, source_row_excel: Optional[int] = None) -> None:
    WARNING_LOG.append(
        WarningRecord(
            category=category,
            message=message,
            table_role=table_role,
            source_row_excel=source_row_excel,
        )
    )


def warning_frame() -> pd.DataFrame:
    return pd.DataFrame([record.__dict__ for record in WARNING_LOG])


def find_input_files(out_dir: Path) -> Dict[str, Path]:
    files = {path.name: path for path in out_dir.glob("*.xlsx")}
    # После merge RUONIA файл может называться table_2_1_first_press_release-3.xlsx
    if files.get("table_2_1_first_press_release.xlsx") is None and files.get("table_2_1_first_press_release-3.xlsx"):
        files["table_2_1_first_press_release.xlsx"] = files["table_2_1_first_press_release-3.xlsx"]
    roles = {
        "base": files.get("ma_deals_AUDIT.xlsx"),
        "intraday": files.get("table_1_intraday.xlsx"),
        "announcement_daily": files.get("table_2_1_first_press_release.xlsx"),
        "actualization_daily": files.get("table_2_2_cbonds_actualization.xlsx"),
        "create_daily": files.get("table_2_3_cbonds_create.xlsx"),
    }
    missing = [role for role, path in roles.items() if path is None]
    if missing:
        raise FileNotFoundError(f"Missing required input files for roles: {missing}")
    return roles


def load_base_dataset(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path)
    mapping = {
        "buyer_company": detect_column(raw.columns, ["Покупатель"]),
        "buyer_ticker": detect_column(raw.columns, ["Тикер покупателя", "Тикер Покупателя"]),
        "deal_object": detect_column(raw.columns, ["Объект сделки", "Объект Сделки"]),
        "announcement_date": detect_column(raw.columns, ["Дата объявления сделки"]),
        "announcement_time": detect_column(raw.columns, ["Время объявления сделки"]),
        "completion_date": detect_column(raw.columns, ["Дата закрытия сделки"]),
        "completion_time": detect_column(raw.columns, ["Время закрытия сделки"]),
        "deal_status": detect_column(raw.columns, ["Статус сделки"]),
        "deal_value_usd_mn": detect_column(raw.columns, ["Цена сделки, млн USD"]),
        "stake_pct": detect_column(raw.columns, ["Доля участия, %"]),
        "actualization_date": detect_column(raw.columns, ["Дата актуализации"]),
        "creation_date": detect_column(raw.columns, ["Дата создания"]),
        "deal_kind": detect_column(raw.columns, ["Вид сделки"], required=False),
        "deal_type": detect_column(raw.columns, ["Тип сделки"], required=False),
        "integration_direction": detect_column(raw.columns, ["Направление интеграции"], required=False),
    }

    base = raw.copy()
    base["source_row_excel"] = base.index + 2
    base["buyer_company_std"] = base[mapping["buyer_company"]].astype(str).str.strip()
    base["buyer_company_norm"] = base["buyer_company_std"].map(normalize_text)
    base["buyer_ticker_std"] = base[mapping["buyer_ticker"]].astype(str).str.strip().str.upper()
    base["buyer_ticker_norm"] = base["buyer_ticker_std"].map(normalize_ticker)
    base["deal_object_std"] = base[mapping["deal_object"]].astype(str).str.strip()
    base["deal_object_norm"] = base["deal_object_std"].map(normalize_text)
    base["announcement_date_std"] = parse_date_series(base[mapping["announcement_date"]])
    base["completion_date_std"] = parse_date_series(base[mapping["completion_date"]])
    base["actualization_date_std"] = parse_date_series(base[mapping["actualization_date"]])
    base["creation_date_std"] = parse_date_series(base[mapping["creation_date"]])
    base["announcement_time_std"] = base[mapping["announcement_time"]].map(parse_time_value)
    base["completion_time_std"] = base[mapping["completion_time"]].map(parse_time_value)
    base["deal_value_usd_mn_std"] = parse_numeric_series(base[mapping["deal_value_usd_mn"]])
    base["stake_pct_std"] = parse_numeric_series(base[mapping["stake_pct"]])
    base["deal_status_clean"] = base[mapping["deal_status"]].astype(str).str.strip()
    base["deal_kind_clean"] = base[mapping["deal_kind"]].astype(str).str.strip() if mapping["deal_kind"] else np.nan
    base["deal_type_clean"] = base[mapping["deal_type"]].astype(str).str.strip() if mapping["deal_type"] else np.nan
    base["integration_direction_clean"] = (
        base[mapping["integration_direction"]].astype(str).str.strip() if mapping["integration_direction"] else np.nan
    )
    return base


def standardize_panel_keys(df: pd.DataFrame) -> pd.DataFrame:
    df["buyer_ticker_norm"] = df["buyer_ticker"].map(normalize_ticker)
    df["buyer_company_norm"] = df["buyer_company"].map(normalize_text)
    df["deal_object_norm"] = df["deal_object"].map(normalize_text)
    return df


def load_intraday_panel(path: Path) -> pd.DataFrame:
    raw = _drop_pipeline_stub_rows(pd.read_excel(path))
    mapping = {
        "source_row_excel": detect_column(raw.columns, ["source_row_excel"]),
        "buyer_ticker": detect_column(raw.columns, ["buyer_ticker"]),
        "buyer_company": detect_column(raw.columns, ["buyer_company"]),
        "deal_object": detect_column(raw.columns, ["deal_object"]),
        "event_name": detect_column(raw.columns, ["event_name"], required=False),
        "release_date": detect_column(raw.columns, ["release_date"], required=False),
        "release_time": detect_column(raw.columns, ["release_time"], required=False),
        "anchor_trade_date": detect_column(raw.columns, ["anchor_trade_date"], required=False),
        "anchor_timestamp_msk": detect_column(raw.columns, ["anchor_timestamp_msk"], required=False),
        "trade_day_offset": detect_column(raw.columns, ["trade_day_offset"]),
        "timestamp_msk": detect_column(raw.columns, ["timestamp_msk"]),
        "price": detect_column(raw.columns, ["price_at_timestamp_rub"]),
        "volume": detect_column(raw.columns, ["volume_during_timestamp_plus_15m_mn_rub"], required=False),
        "is_off_market_release": detect_column(raw.columns, ["is_off_market_release"], required=False),
    }
    df = pd.DataFrame(
        {
            "source_row_excel": pd.to_numeric(raw[mapping["source_row_excel"]], errors="coerce").astype("Int64"),
            "buyer_ticker": raw[mapping["buyer_ticker"]].astype(str).str.strip(),
            "buyer_company": raw[mapping["buyer_company"]].astype(str).str.strip(),
            "deal_object": raw[mapping["deal_object"]].astype(str).str.strip(),
            "event_name": raw[mapping["event_name"]].astype(str).str.strip() if mapping["event_name"] else np.nan,
            "release_date": parse_date_series(raw[mapping["release_date"]]) if mapping["release_date"] else pd.NaT,
            "release_time": raw[mapping["release_time"]].map(parse_time_value) if mapping["release_time"] else None,
            "anchor_trade_date": parse_date_series(raw[mapping["anchor_trade_date"]]) if mapping["anchor_trade_date"] else pd.NaT,
            "anchor_timestamp_msk": pd.to_datetime(raw[mapping["anchor_timestamp_msk"]], errors="coerce"),
            "trade_day_offset": pd.to_numeric(raw[mapping["trade_day_offset"]], errors="coerce"),
            "timestamp_msk": pd.to_datetime(raw[mapping["timestamp_msk"]], errors="coerce"),
            "price": parse_numeric_series(raw[mapping["price"]]),
            "volume_mn_rub": parse_numeric_series(raw[mapping["volume"]]) if mapping["volume"] else np.nan,
            "is_off_market_release": (
                pd.to_numeric(raw[mapping["is_off_market_release"]], errors="coerce").fillna(0).astype(int)
                if mapping["is_off_market_release"]
                else 0
            ),
        }
    )
    df = standardize_panel_keys(df)
    df = df.sort_values(["source_row_excel", "timestamp_msk"]).reset_index(drop=True)
    df["interval_return"] = df.groupby(["source_row_excel", "trade_day_offset"], sort=False)["price"].pct_change(
        fill_method=None
    )
    return df


def load_daily_panel(path: Path, role: str) -> pd.DataFrame:
    raw = _drop_pipeline_stub_rows(pd.read_excel(path))
    mapping = {
        "source_row_excel": detect_column(raw.columns, ["source_row_excel"]),
        "buyer_ticker": detect_column(raw.columns, ["buyer_ticker"]),
        "buyer_company": detect_column(raw.columns, ["buyer_company"]),
        "deal_object": detect_column(raw.columns, ["deal_object"]),
        "event_name": detect_column(raw.columns, ["event_name"], required=False),
        "anchor_date": detect_column(raw.columns, ["anchor_date"]),
        "anchor_trade_date": detect_column(raw.columns, ["anchor_trade_date"], required=False),
        "t": detect_column(raw.columns, ["t"]),
        "date": detect_column(raw.columns, ["Date"]),
        "adjusted_close": detect_column(raw.columns, ["Adjusted Close, руб.", "Adjusted Close"], required=False),
        "close": detect_column(raw.columns, ["Close, руб.", "Close"]),
        "volume": detect_column(raw.columns, ["Volume, млрд. руб.", "Volume"], required=False),
        "market_cap": detect_column(raw.columns, ["Market Capitalization, млрд. руб.", "Market Capitalization"], required=False),
        "benchmark_close": detect_column(raw.columns, ["IMOEX daily close", "IMOEX"], required=False),
        "ruonia": detect_column(raw.columns, ["RUONIA (daily)", "RUONIA"], required=False),
        "roe": detect_column(raw.columns, ["ROE"], required=False),
        "roa": detect_column(raw.columns, ["ROA"], required=False),
        "p_b": detect_column(raw.columns, ["P/B", "P B"], required=False),
        "p_e": detect_column(raw.columns, ["P/E", "P E"], required=False),
        "total_assets": detect_column(raw.columns, ["Total Assets"], required=False),
    }
    adjusted = parse_numeric_series(raw[mapping["adjusted_close"]]) if mapping["adjusted_close"] else pd.Series(np.nan, index=raw.index)
    close = parse_numeric_series(raw[mapping["close"]])
    df = pd.DataFrame(
        {
            "source_row_excel": pd.to_numeric(raw[mapping["source_row_excel"]], errors="coerce").astype("Int64"),
            "buyer_ticker": raw[mapping["buyer_ticker"]].astype(str).str.strip(),
            "buyer_company": raw[mapping["buyer_company"]].astype(str).str.strip(),
            "deal_object": raw[mapping["deal_object"]].astype(str).str.strip(),
            "event_name": raw[mapping["event_name"]].astype(str).str.strip() if mapping["event_name"] else role,
            "anchor_date": parse_date_series(raw[mapping["anchor_date"]]),
            "anchor_trade_date": parse_date_series(raw[mapping["anchor_trade_date"]]) if mapping["anchor_trade_date"] else pd.NaT,
            "t": pd.to_numeric(raw[mapping["t"]], errors="coerce"),
            "date": parse_date_series(raw[mapping["date"]]),
            "adjusted_close": adjusted,
            "close": close,
            "volume_bnrub": parse_numeric_series(raw[mapping["volume"]]) if mapping["volume"] else np.nan,
            "market_cap_bnrub": parse_numeric_series(raw[mapping["market_cap"]]) if mapping["market_cap"] else np.nan,
            "benchmark_close": parse_numeric_series(raw[mapping["benchmark_close"]]) if mapping["benchmark_close"] else np.nan,
            "ruonia": parse_numeric_series(raw[mapping["ruonia"]]) if mapping["ruonia"] else np.nan,
            "roe": parse_numeric_series(raw[mapping["roe"]]) if mapping["roe"] else np.nan,
            "roa": parse_numeric_series(raw[mapping["roa"]]) if mapping["roa"] else np.nan,
            "p_b": parse_numeric_series(raw[mapping["p_b"]]) if mapping["p_b"] else np.nan,
            "p_e": parse_numeric_series(raw[mapping["p_e"]]) if mapping["p_e"] else np.nan,
            "total_assets": parse_numeric_series(raw[mapping["total_assets"]]) if mapping["total_assets"] else np.nan,
            "panel_role": role,
        }
    )
    df = standardize_panel_keys(df)
    df = df.sort_values(["source_row_excel", "date"]).reset_index(drop=True)
    deal_stats = (
        df.groupby("source_row_excel", as_index=False)
        .agg(
            n_rows=("close", "size"),
            adj_n=("adjusted_close", lambda s: int(s.notna().sum())),
        )
    )
    deal_stats["adj_share"] = deal_stats["adj_n"] / deal_stats["n_rows"].replace(0, np.nan)
    df = df.merge(deal_stats[["source_row_excel", "adj_share"]], on="source_row_excel", how="left")
    use_adj = df["adj_share"] >= 0.8
    df["price"] = np.where(use_adj, df["adjusted_close"], df["close"])
    both_parts = (df["adj_share"] > 0) & (df["adj_share"] < 0.8) & df["adjusted_close"].notna() & df["close"].notna()
    df["price_source"] = np.where(use_adj, "adjusted", np.where(both_parts, "mixed_WARNING", "close"))
    for sr, sub in df.loc[both_parts].groupby("source_row_excel"):
        as_share = float(sub["adj_share"].iloc[0])
        log_warning(
            category="mixed_price_series",
            message=f"adjusted_close covers {as_share:.1%} of rows; using close-only (no hybrid with adjusted)",
            table_role=role,
            source_row_excel=int(sr),
        )
    df = df.drop(columns=["adj_share"])
    df["security_return"] = df.groupby("source_row_excel")["price"].pct_change(fill_method=None)
    df["benchmark_return"] = df.groupby("source_row_excel")["benchmark_close"].pct_change(fill_method=None)
    df["trading_seq"] = df.groupby("source_row_excel").cumcount()
    return df


def evaluate_matching(base: pd.DataFrame, panel: pd.DataFrame, role: str) -> pd.DataFrame:
    panel_keys = (
        panel[["source_row_excel", "buyer_ticker_norm", "buyer_company_norm", "deal_object_norm"]]
        .dropna(subset=["source_row_excel"])
        .drop_duplicates()
    )
    dup_counts = panel_keys.groupby("source_row_excel").size().rename("panel_key_count").reset_index()
    merged = panel_keys.merge(
        base[
            [
                "source_row_excel",
                "buyer_ticker_norm",
                "buyer_company_norm",
                "deal_object_norm",
            ]
        ],
        on="source_row_excel",
        how="left",
        suffixes=("_panel", "_base"),
    )
    status_rows = []
    for row in merged.itertuples(index=False):
        source_row = int(row.source_row_excel)
        reason = "certain"
        if pd.isna(row.buyer_ticker_norm_base):
            reason = "missing_base_row"
        elif (
            row.buyer_ticker_norm_panel != row.buyer_ticker_norm_base
            or row.buyer_company_norm_panel != row.buyer_company_norm_base
            or row.deal_object_norm_panel != row.deal_object_norm_base
        ):
            reason = "key_mismatch"
        status_rows.append({"source_row_excel": source_row, "table_role": role, "mapping_status": reason})
    status = pd.DataFrame(status_rows).merge(dup_counts, on="source_row_excel", how="left")
    status["panel_key_count"] = status["panel_key_count"].fillna(0)
    status.loc[status["panel_key_count"] > 1, "mapping_status"] = "ambiguous_multiple_key_sets"
    if not status.empty:
        ambiguous = status[status["mapping_status"] != "certain"]
        for row in ambiguous.itertuples(index=False):
            log_warning(
                category="ambiguous_match",
                message=f"{role}: {row.mapping_status}",
                table_role=role,
                source_row_excel=int(row.source_row_excel),
            )
    return status


def filter_certain_rows(panel: pd.DataFrame, mapping_status: pd.DataFrame) -> pd.DataFrame:
    certain_ids = set(mapping_status.loc[mapping_status["mapping_status"] == "certain", "source_row_excel"].tolist())
    return panel[panel["source_row_excel"].isin(certain_ids)].copy()


def build_daily_model(
    sub: pd.DataFrame, event_t: pd.Series, source_row_excel: Optional[int] = None
) -> Tuple[pd.Series, str, int]:
    ew_lo, ew_hi = get_estimation_window()
    est_mask = event_t.between(ew_lo, ew_hi) & sub["security_return"].notna()
    est = sub.loc[est_mask, ["security_return", "benchmark_return"]].dropna()
    if len(est) >= 60 and est["benchmark_return"].std() > 0:
        model = sm.OLS(est["security_return"], sm.add_constant(est["benchmark_return"])).fit()
        benchmark_filled = sub["benchmark_return"].fillna(0.0)
        expected = pd.Series(
            model.params["const"] + model.params["benchmark_return"] * benchmark_filled,
            index=sub.index,
        )
        return sub["security_return"] - expected, "market_model", len(est)
    if sub["benchmark_return"].notna().sum() >= 20:
        if source_row_excel is not None:
            log_warning(
                category="model_fallback",
                message="Using market_adjusted (OLS market model not estimated: insufficient obs or zero benchmark variance)",
                source_row_excel=source_row_excel,
            )
        expected = sub["benchmark_return"].fillna(0.0)
        return sub["security_return"] - expected, "market_adjusted", int(sub["benchmark_return"].notna().sum())
    est_mean = est["security_return"].mean() if len(est) >= 20 else 0.0
    expected = pd.Series(est_mean, index=sub.index)
    if source_row_excel is not None:
        log_warning(
            category="model_fallback",
            message="Using mean_adjusted (benchmark sparse or estimation window short)",
            source_row_excel=source_row_excel,
        )
    return sub["security_return"] - expected, "mean_adjusted", len(est)


def sum_window(values: pd.Series, event_t: pd.Series, start: int, end: int, min_fraction: float = 0.8) -> float:
    mask = event_t.between(start, end)
    window = values.loc[mask].dropna()
    expected_obs = end - start + 1
    min_obs = max(1, int(math.ceil(expected_obs * min_fraction)))
    if len(window) < min_obs:
        return np.nan
    return float(window.sum())


def compounded_return(values: pd.Series) -> float:
    if values.empty:
        return np.nan
    return float(np.prod(1.0 + values) - 1.0)


def bhar_window(
    sub: pd.DataFrame,
    event_t: pd.Series,
    horizon: int,
    min_fraction: float = 0.75,
    min_tail_beyond_horizon: int = 0,
    source_row_excel: Optional[int] = None,
) -> float:
    """
    BHAR_i(T) in excess-wealth units:
      Π(1+Ri) - Π(1+Rm), using simple returns on t in [1..T].

    Uses the same robust validator/definition as event_study_formulas.bhar_post_window
    so BHAR is mathematically consistent across modules.
    """
    frame = pd.DataFrame(
        {
            "t": pd.to_numeric(event_t, errors="coerce"),
            "security_return": pd.to_numeric(sub["security_return"], errors="coerce"),
            "benchmark_return": pd.to_numeric(sub["benchmark_return"], errors="coerce"),
        }
    )
    val, why = bhar_post_window(
        frame,
        ri_col="security_return",
        rm_col="benchmark_return",
        horizon=horizon,
        min_fraction_valid=min_fraction,
        min_tail_beyond_horizon=min_tail_beyond_horizon,
    )
    if why and source_row_excel is not None:
        log_warning(
            category="insufficient_bhar_data",
            message=f"T={horizon}: {why}",
            table_role="daily_bhar",
            source_row_excel=source_row_excel,
        )
    return val


def extract_controls(sub: pd.DataFrame, event_t: pd.Series, prefix: str) -> Dict[str, float]:
    event_rows = sub.loc[event_t == 0]
    if event_rows.empty:
        event_rows = sub.loc[event_t >= 0].head(1)
    pre60 = sub.loc[event_t.between(-60, -1)]
    pre20 = sub.loc[event_t.between(-20, -1)]
    out = {
        f"{prefix}_ROE": np.nan,
        f"{prefix}_ROA": np.nan,
        f"{prefix}_P_B": np.nan,
        f"{prefix}_P_E": np.nan,
        f"{prefix}_TOTAL_ASSETS": np.nan,
        f"{prefix}_MARKET_CAP": np.nan,
        f"{prefix}_LIQUIDITY_60D": np.nan,
        f"{prefix}_VOLATILITY_60D": np.nan,
    }
    if not event_rows.empty:
        row = event_rows.iloc[0]
        out.update(
            {
                f"{prefix}_ROE": row.get("roe", np.nan),
                f"{prefix}_ROA": row.get("roa", np.nan),
                f"{prefix}_P_B": row.get("p_b", np.nan),
                f"{prefix}_P_E": row.get("p_e", np.nan),
                f"{prefix}_TOTAL_ASSETS": row.get("total_assets", np.nan),
                f"{prefix}_MARKET_CAP": row.get("market_cap_bnrub", np.nan),
            }
        )
    out[f"{prefix}_LIQUIDITY_60D"] = pre60["volume_bnrub"].mean() if "volume_bnrub" in pre60 else np.nan
    out[f"{prefix}_VOLATILITY_60D"] = pre60["security_return"].std() * np.sqrt(252) if not pre60.empty else np.nan
    if prefix == "ANN":
        out["Volume_bln_avg_pre"] = (
            float(pre20["volume_bnrub"].mean())
            if "volume_bnrub" in pre20.columns and not pre20.empty
            else np.nan
        )
    return out


def compute_anchored_daily_metrics(panel: pd.DataFrame, prefix: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    metric_rows: List[Dict[str, object]] = []
    annotated_groups: List[pd.DataFrame] = []
    for source_row, sub in panel.groupby("source_row_excel", sort=True):
        sub = sub.sort_values("t").copy()
        event_t = sub["t"]
        abnormal, model_type, n_est = build_daily_model(sub, event_t, source_row_excel=int(source_row))
        sub[f"ar_{prefix.lower()}"] = abnormal
        sub[f"expected_model_{prefix.lower()}"] = model_type
        sub[f"n_est_{prefix.lower()}"] = n_est
        metrics = {
            "source_row_excel": int(source_row),
            f"CAR_{prefix}_1_1": sum_window(abnormal, event_t, -1, 1),
            f"CAR_{prefix}_3_3": sum_window(abnormal, event_t, -3, 3),
            f"CAR_{prefix}_5_5": sum_window(abnormal, event_t, -5, 5),
            f"CAR_{prefix}_10_10": sum_window(abnormal, event_t, -10, 10),
            f"CAR_{prefix}_30_30": sum_window(abnormal, event_t, -30, 30),
            f"CAR_{prefix}_50_50": sum_window(abnormal, event_t, -50, 50),
            f"CAR_{prefix}_30_5": sum_window(abnormal, event_t, -30, -5),
            f"BHAR_{prefix}_60": bhar_window(sub, event_t, 60, source_row_excel=int(source_row)),
            f"BHAR_{prefix}_120": bhar_window(sub, event_t, 120, source_row_excel=int(source_row)),
            f"BHAR_{prefix}_250": bhar_window(sub, event_t, 250, source_row_excel=int(source_row)),
            f"{prefix}_MODEL_TYPE": model_type,
            f"{prefix}_ESTIMATION_OBS": n_est,
        }
        metrics.update(extract_controls(sub, event_t, prefix))
        metric_rows.append(metrics)
        annotated_groups.append(sub)
    return pd.DataFrame(metric_rows), pd.concat(annotated_groups, ignore_index=True)


def compute_intraday_metrics(base: pd.DataFrame, intraday: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    available_ids = set(intraday["source_row_excel"].dropna().astype(int).tolist())
    for deal in base.itertuples(index=False):
        record = {
            "source_row_excel": int(deal.source_row_excel),
            "is_off_market_release": 0,
            "CAR_ANN_INTRADAY_15M": np.nan,
            "CAR_ANN_INTRADAY_30M": np.nan,
            "CAR_ANN_INTRADAY_M60_P180": np.nan,
            "CAR_ANN_INTRADAY_1H": np.nan,
            "CAR_ANN_INTRADAY_3H": np.nan,
            "CAR_ANN_INTRADAY_FULL": np.nan,
            "CAR_ANN_INTRADAY_TO_CLOSE": np.nan,
            "CAR_ANN_NEXT_OPEN_1H": np.nan,
            "CAR_ANN_NEXT_DAY": np.nan,
            "CAR_INTRADAY_PRE_4_0": np.nan,
            "CAR_INTRADAY_PRE_4_0_prevday": np.nan,
            "ratio_1h_vs_day": np.nan,
            "CAR_CLOSE_INTRADAY_15M": np.nan,
            "CAR_CLOSE_INTRADAY_30M": np.nan,
            "CAR_CLOSE_INTRADAY_M60_P180": np.nan,
            "CAR_CLOSE_INTRADAY_1H": np.nan,
            "CAR_CLOSE_INTRADAY_3H": np.nan,
            "CAR_CLOSE_INTRADAY_FULL": np.nan,
            "CAR_CLOSE_INTRADAY_TO_CLOSE": np.nan,
            "CAR_CLOSE_NEXT_OPEN_1H": np.nan,
            "CAR_CLOSE_NEXT_DAY": np.nan,
        }
        if int(deal.source_row_excel) not in available_ids:
            rows.append(record)
            continue
        sub = intraday.loc[intraday["source_row_excel"] == deal.source_row_excel].copy()
        if sub.empty:
            rows.append(record)
            continue
        sub = sub.sort_values("timestamp_msk").reset_index(drop=True)
        if "is_off_market_release" in sub.columns and sub["is_off_market_release"].notna().any():
            record["is_off_market_release"] = int(sub["is_off_market_release"].fillna(0).astype(int).max())
        anchor_ts = sub["anchor_timestamp_msk"].dropna().iloc[0] if sub["anchor_timestamp_msk"].notna().any() else pd.NaT
        if pd.isna(anchor_ts):
            log_warning(
                category="missing_intraday_anchor",
                message="Intraday anchor timestamp is missing",
                table_role="intraday",
                source_row_excel=int(deal.source_row_excel),
            )
            rows.append(record)
            continue
        pre_day_returns = sub.loc[sub["trade_day_offset"] == -1, "interval_return"].dropna()
        expected_mean = float(pre_day_returns.mean()) if len(pre_day_returns) >= 4 else 0.0
        anchor_pos_arr = np.where(sub["anchor_timestamp_msk"].eq(anchor_ts).to_numpy())[0]
        k0 = int(anchor_pos_arr[0]) if len(anchor_pos_arr) else 0
        sub["bar_k"] = np.arange(len(sub), dtype=int) - k0
        prior_est = sub.loc[sub["trade_day_offset"].between(-20, -1), ["bar_k", "interval_return"]].dropna()
        mean_by_k = prior_est.groupby("bar_k")["interval_return"].mean()
        abnormal_est = sub["interval_return"] - sub["bar_k"].map(mean_by_k).fillna(0.0)
        abnormal = sub["interval_return"] - expected_mean
        event_trade_date = sub["anchor_trade_date"].dropna().iloc[0] if sub["anchor_trade_date"].notna().any() else anchor_ts.normalize()
        event_day = sub.loc[sub["timestamp_msk"].dt.tz_localize(None).dt.normalize() == pd.Timestamp(event_trade_date).normalize()]
        next_day = sub.loc[sub["trade_day_offset"] == 1]

        def intraday_car(start_minutes: int, end_minutes: int) -> float:
            start_ts = anchor_ts + pd.Timedelta(minutes=start_minutes)
            end_ts = anchor_ts + pd.Timedelta(minutes=end_minutes)
            window = abnormal.loc[(sub["timestamp_msk"] > start_ts) & (sub["timestamp_msk"] <= end_ts)].dropna()
            return float(window.sum()) if not window.empty else np.nan

        record["CAR_ANN_INTRADAY_15M"] = intraday_car(-15, 15)
        record["CAR_ANN_INTRADAY_30M"] = intraday_car(-30, 30)
        record["CAR_ANN_INTRADAY_M60_P180"] = intraday_car(-60, 180)
        record["CAR_ANN_INTRADAY_1H"] = intraday_car(0, 60)
        record["CAR_ANN_INTRADAY_3H"] = intraday_car(0, 180)
        record["CAR_ANN_INTRADAY_FULL"] = float(abnormal.dropna().sum()) if abnormal.notna().any() else np.nan
        if not event_day.empty:
            close_ts = event_day["timestamp_msk"].max()
            same_day_window = abnormal.loc[(sub["timestamp_msk"] > anchor_ts) & (sub["timestamp_msk"] <= close_ts)].dropna()
            record["CAR_ANN_INTRADAY_TO_CLOSE"] = float(same_day_window.sum()) if not same_day_window.empty else np.nan
            d0_only = event_day.sort_values("timestamp_msk")
            day_tot = float(abnormal.loc[d0_only.index].sum())
            end1h = anchor_ts + pd.Timedelta(hours=1)
            m1 = (d0_only["timestamp_msk"] > anchor_ts) & (d0_only["timestamp_msk"] <= end1h)
            if m1.any() and day_tot and not math.isclose(day_tot, 0.0):
                h1 = float(abnormal.loc[d0_only.loc[m1].index].sum())
                record["ratio_1h_vs_day"] = h1 / day_tot * 100.0
        pre_mask = sub["bar_k"].between(-4, -1)
        pre_est = abnormal_est.loc[pre_mask].dropna()
        pre_prev = abnormal.loc[pre_mask].dropna()
        if pre_mask.sum() == 4 and len(pre_est) == 4:
            record["CAR_INTRADAY_PRE_4_0"] = float(pre_est.sum())
        if pre_mask.sum() == 4 and len(pre_prev) == 4:
            record["CAR_INTRADAY_PRE_4_0_prevday"] = float(pre_prev.sum())
        if not next_day.empty:
            next_open_ts = next_day["timestamp_msk"].min()
            next_close_ts = next_day["timestamp_msk"].max()
            next_1h = abnormal.loc[(sub["timestamp_msk"] > next_open_ts) & (sub["timestamp_msk"] <= next_open_ts + pd.Timedelta(minutes=60))].dropna()
            next_full = abnormal.loc[(sub["timestamp_msk"] > next_open_ts) & (sub["timestamp_msk"] <= next_close_ts)].dropna()
            record["CAR_ANN_NEXT_OPEN_1H"] = float(next_1h.sum()) if not next_1h.empty else np.nan
            record["CAR_ANN_NEXT_DAY"] = float(next_full.sum()) if not next_full.empty else np.nan
        rows.append(record)

    for deal in base.itertuples(index=False):
        if deal.completion_time_std and pd.notna(deal.completion_date_std):
            log_warning(
                category="completion_intraday_unavailable",
                message="Completion intraday metrics left missing because no dedicated completion intraday panel was uploaded",
                table_role="intraday",
                source_row_excel=int(deal.source_row_excel),
            )
            break
    return pd.DataFrame(rows)


def choose_completion_panel(source_row_excel: int, completion_date: pd.Timestamp, panels: Dict[str, pd.DataFrame]) -> Tuple[Optional[str], Optional[pd.DataFrame], Optional[pd.Timestamp]]:
    priority = ["announcement_daily", "actualization_daily", "create_daily"]
    for role in priority:
        panel = panels[role]
        sub = panel.loc[panel["source_row_excel"] == source_row_excel].sort_values("date").copy()
        if sub.empty:
            continue
        if completion_date < sub["date"].min() or completion_date > sub["date"].max():
            continue
        event_candidates = sub.loc[sub["date"] >= completion_date, "date"]
        if event_candidates.empty:
            continue
        event_trade_date = event_candidates.min()
        return role, sub, event_trade_date
    return None, None, None


def compute_completion_metrics(base: pd.DataFrame, panels: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for deal in base.itertuples(index=False):
        metrics = {
            "source_row_excel": int(deal.source_row_excel),
            "CAR_CLOSE_1_1": np.nan,
            "CAR_CLOSE_3_3": np.nan,
            "CAR_CLOSE_5_5": np.nan,
            "CAR_CLOSE_10_10": np.nan,
            "BHAR_CLOSE_60": np.nan,
            "BHAR_CLOSE_120": np.nan,
            "BHAR_CLOSE_250": np.nan,
            "CLOSE_MODEL_TYPE": np.nan,
            "CLOSE_ESTIMATION_OBS": np.nan,
            "CLOSE_PANEL_ROLE": np.nan,
            "CLOSE_EVENT_TRADE_DATE": pd.NaT,
        }
        metrics.update(extract_controls(pd.DataFrame(), pd.Series(dtype=float), "CLOSE"))
        if pd.isna(deal.completion_date_std):
            rows.append(metrics)
            continue
        role, sub, event_trade_date = choose_completion_panel(int(deal.source_row_excel), deal.completion_date_std, panels)
        if sub is None or event_trade_date is None:
            log_warning(
                category="missing_completion_coverage",
                message="No daily panel contains the completion date for re-anchored completion analysis",
                table_role="completion_daily",
                source_row_excel=int(deal.source_row_excel),
            )
            rows.append(metrics)
            continue
        pos = sub.reset_index(drop=True)
        event_idx = pos.index[pos["date"] == event_trade_date]
        if len(event_idx) == 0:
            rows.append(metrics)
            continue
        event_pos = int(event_idx[0])
        event_t = pd.Series(np.arange(len(pos)) - event_pos, index=pos.index)
        abnormal, model_type, n_est = build_daily_model(pos, event_t, source_row_excel=int(deal.source_row_excel))
        metrics.update(
            {
                "CAR_CLOSE_1_1": sum_window(abnormal, event_t, -1, 1),
                "CAR_CLOSE_3_3": sum_window(abnormal, event_t, -3, 3),
                "CAR_CLOSE_5_5": sum_window(abnormal, event_t, -5, 5),
                "CAR_CLOSE_10_10": sum_window(abnormal, event_t, -10, 10),
                "BHAR_CLOSE_60": bhar_window(pos, event_t, 60, source_row_excel=int(deal.source_row_excel)),
                "BHAR_CLOSE_120": bhar_window(pos, event_t, 120, source_row_excel=int(deal.source_row_excel)),
                "BHAR_CLOSE_250": bhar_window(pos, event_t, 250, source_row_excel=int(deal.source_row_excel)),
                "CLOSE_MODEL_TYPE": model_type,
                "CLOSE_ESTIMATION_OBS": n_est,
                "CLOSE_PANEL_ROLE": role,
                "CLOSE_EVENT_TRADE_DATE": event_trade_date,
            }
        )
        metrics.update(extract_controls(pos, event_t, "CLOSE"))
        rows.append(metrics)
    return pd.DataFrame(rows)


def compute_leakage_metrics(base: pd.DataFrame, ann_metrics: pd.DataFrame, create_metrics: pd.DataFrame) -> pd.DataFrame:
    merged = (
        base[["source_row_excel", "announcement_date_std", "creation_date_std"]]
        .merge(ann_metrics[["source_row_excel", "CAR_ANN_1_1"]], on="source_row_excel", how="left")
        .merge(create_metrics[["source_row_excel", "CAR_CREATE_1_1", "CAR_CREATE_3_3"]], on="source_row_excel", how="left")
    )
    merged["RUNUP_PRE_30_5"] = np.nan
    merged["CAR_PRE_ANNOUNCEMENT"] = np.nan
    merged["DAYS_CREATE_TO_ANNOUNCEMENT"] = (merged["announcement_date_std"] - merged["creation_date_std"]).dt.days
    merged["DIFF_CAR_ANN_MINUS_CREATE_1_1"] = merged["CAR_ANN_1_1"] - merged["CAR_CREATE_1_1"]
    return merged[
        [
            "source_row_excel",
            "RUNUP_PRE_30_5",
            "CAR_PRE_ANNOUNCEMENT",
            "DAYS_CREATE_TO_ANNOUNCEMENT",
            "DIFF_CAR_ANN_MINUS_CREATE_1_1",
        ]
    ]


def add_pre_announcement_runups(leakage_df: pd.DataFrame, announcement_panel_annotated: pd.DataFrame) -> pd.DataFrame:
    values = []
    for source_row, sub in announcement_panel_annotated.groupby("source_row_excel"):
        event_t = sub["t"]
        ar = sub["ar_ann"]
        values.append(
            {
                "source_row_excel": int(source_row),
                "RUNUP_PRE_30_5": sum_window(ar, event_t, -30, -5),
                "CAR_PRE_ANNOUNCEMENT": sum_window(ar, event_t, -30, -1),
            }
        )
    return leakage_df.drop(columns=["RUNUP_PRE_30_5", "CAR_PRE_ANNOUNCEMENT"]).merge(
        pd.DataFrame(values), on="source_row_excel", how="left"
    )


def winsorize_series(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    if series.dropna().nunique() <= 2:
        return series
    lo = series.quantile(lower)
    hi = series.quantile(upper)
    return series.clip(lo, hi)


def run_summary_stats(enriched: pd.DataFrame, metric_cols: List[str]) -> pd.DataFrame:
    rows = []
    for col in metric_cols:
        series = enriched[col].dropna()
        if series.empty:
            continue
        rows.append(
            {
                "metric": col,
                "n": int(series.shape[0]),
                "mean": series.mean(),
                "median": series.median(),
                "std": series.std(),
                "min": series.min(),
                "max": series.max(),
            }
        )
    return pd.DataFrame(rows)


def run_one_sample_tests(enriched: pd.DataFrame, metric_cols: List[str]) -> pd.DataFrame:
    rows = []
    for col in metric_cols:
        series = enriched[col].dropna()
        if len(series) < 3:
            continue
        t_stat, p_value = stats.ttest_1samp(series, popmean=0.0, nan_policy="omit")
        rows.append(
            {
                "metric": col,
                "n": int(series.shape[0]),
                "mean": series.mean(),
                "t_stat": t_stat,
                "p_value": p_value,
            }
        )
    return pd.DataFrame(rows)


def build_metric_focus_table(
    enriched: pd.DataFrame,
    one_sample_tests: pd.DataFrame,
    metrics: List[str],
    *,
    block_label: str = "",
) -> pd.DataFrame:
    """Small thesis-ready summary table for a selected metric block."""
    if one_sample_tests.empty:
        one_map = pd.DataFrame(columns=["metric", "t_stat", "p_value"]).set_index("metric")
    else:
        one_map = one_sample_tests.set_index("metric")

    rows: List[Dict[str, object]] = []
    for metric in metrics:
        if metric not in enriched.columns:
            continue
        s = pd.to_numeric(enriched[metric], errors="coerce").dropna()
        t_stat = np.nan
        p_value = np.nan
        if metric in one_map.index:
            t_stat = one_map.loc[metric, "t_stat"]
            p_value = one_map.loc[metric, "p_value"]
        rows.append(
            {
                "block_label": block_label,
                "metric_name": metric,
                "n": int(s.shape[0]),
                "mean": float(s.mean()) if not s.empty else np.nan,
                "median": float(s.median()) if not s.empty else np.nan,
                "std": float(s.std()) if not s.empty else np.nan,
                "t_stat": t_stat,
                "p_value": p_value,
            }
        )
    return pd.DataFrame(rows)


def run_group_tests(enriched: pd.DataFrame, metrics: List[str]) -> pd.DataFrame:
    candidate_groups = ["deal_status_clean", "deal_kind_clean", "deal_type_clean", "integration_direction_clean"]
    rows = []
    for group_col in candidate_groups:
        if group_col not in enriched.columns:
            continue
        group_counts = enriched[group_col].replace({"nan": np.nan}).dropna().value_counts()
        valid_groups = group_counts[group_counts >= 5].index.tolist()
        if len(valid_groups) < 2:
            continue
        for metric in metrics:
            sub = enriched[[group_col, metric]].dropna()
            sub = sub[sub[group_col].isin(valid_groups)]
            grouped = [g[metric].values for _, g in sub.groupby(group_col)]
            if len(grouped) == 2:
                stat, p_value = stats.ttest_ind(grouped[0], grouped[1], equal_var=False, nan_policy="omit")
                rows.append(
                    {
                        "group_variable": group_col,
                        "metric": metric,
                        "test": "welch_t",
                        "statistic": stat,
                        "p_value": p_value,
                        "groups_used": len(grouped),
                    }
                )
            elif len(grouped) > 2:
                kw_stat, kw_p = stats.kruskal(*grouped)
                rows.append(
                    {
                        "group_variable": group_col,
                        "metric": metric,
                        "test": "kruskal_wallis",
                        "statistic": kw_stat,
                        "p_value": kw_p,
                        "groups_used": len(grouped),
                    }
                )
                f_stat, f_p = stats.f_oneway(*grouped)
                rows.append(
                    {
                        "group_variable": group_col,
                        "metric": metric,
                        "test": "anova_f",
                        "statistic": f_stat,
                        "p_value": f_p,
                        "groups_used": len(grouped),
                    }
                )
            else:
                continue
    return pd.DataFrame(rows)


def run_regressions(enriched: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str]]:
    specs = [
        ("announcement_car", "CAR_ANN_1_1", ["deal_value_usd_mn_std", "stake_pct_std", "ANN_ROE", "ANN_ROA", "ANN_P_B", "ANN_P_E", "ANN_TOTAL_ASSETS", "ANN_MARKET_CAP", "ANN_LIQUIDITY_60D", "ANN_VOLATILITY_60D"]),
        ("completion_car", "CAR_CLOSE_1_1", ["deal_value_usd_mn_std", "stake_pct_std", "CLOSE_ROE", "CLOSE_ROA", "CLOSE_P_B", "CLOSE_P_E", "CLOSE_TOTAL_ASSETS", "CLOSE_MARKET_CAP", "CLOSE_LIQUIDITY_60D", "CLOSE_VOLATILITY_60D"]),
        ("announcement_bhar", "BHAR_ANN_120", ["deal_value_usd_mn_std", "stake_pct_std", "ANN_ROE", "ANN_ROA", "ANN_P_B", "ANN_P_E", "ANN_TOTAL_ASSETS", "ANN_MARKET_CAP", "ANN_LIQUIDITY_60D", "ANN_VOLATILITY_60D"]),
        ("completion_bhar", "BHAR_CLOSE_120", ["deal_value_usd_mn_std", "stake_pct_std", "CLOSE_ROE", "CLOSE_ROA", "CLOSE_P_B", "CLOSE_P_E", "CLOSE_TOTAL_ASSETS", "CLOSE_MARKET_CAP", "CLOSE_LIQUIDITY_60D", "CLOSE_VOLATILITY_60D"]),
    ]
    coef_rows = []
    diagnostic_rows = []
    summaries: Dict[str, str] = {}
    for model_name, dep, candidates in specs:
        cols = [dep] + [col for col in candidates if col in enriched.columns]
        reg_df = enriched[cols + ["deal_status_clean"]].copy()
        reg_df = reg_df.dropna(subset=[dep])
        available_predictors = []
        for col in candidates:
            if col not in reg_df.columns:
                continue
            if reg_df[col].notna().sum() < 30:
                continue
            if reg_df[col].dropna().nunique() <= 1:
                continue
            reg_df[col] = winsorize_series(reg_df[col])
            available_predictors.append(col)
        reg_df[dep] = winsorize_series(reg_df[dep])

        selected_predictors: List[str] = []
        sorted_predictors = sorted(available_predictors, key=lambda x: reg_df[x].notna().sum(), reverse=True)
        for col in sorted_predictors:
            trial = selected_predictors + [col]
            trial_frame = reg_df[[dep] + trial].dropna()
            if len(trial_frame) >= max(25, 6 * len(trial)):
                selected_predictors = trial

        formula_terms = selected_predictors.copy()
        status_counts = reg_df["deal_status_clean"].dropna().value_counts()
        include_status = False
        if len(status_counts) >= 2 and (status_counts >= 5).sum() >= 2:
            status_trial = reg_df[[dep] + selected_predictors + ["deal_status_clean"]].dropna()
            if len(status_trial) >= max(25, 6 * max(1, len(selected_predictors))):
                include_status = True
        if include_status:
            formula_terms.append("C(deal_status_clean)")
        if not formula_terms:
            log_warning("regression_skipped", f"{model_name}: no usable predictors")
            continue
        model_cols = [dep] + selected_predictors + (["deal_status_clean"] if include_status else [])
        model_frame = reg_df[model_cols].dropna()
        if len(model_frame) < max(25, 6 * max(1, len(selected_predictors))):
            log_warning("regression_skipped", f"{model_name}: insufficient observations after NA filtering")
            continue
        formula = f"{dep} ~ " + " + ".join(formula_terms)
        fitted = smf.ols(formula, data=model_frame).fit(cov_type="HC1")
        conf = fitted.conf_int()
        for term in fitted.params.index:
            coef_rows.append(
                {
                    "model": model_name,
                    "dependent_variable": dep,
                    "term": term,
                    "coef": fitted.params[term],
                    "std_err": fitted.bse[term],
                    "t_value": fitted.tvalues[term],
                    "p_value": fitted.pvalues[term],
                    "ci_low": conf.loc[term, 0],
                    "ci_high": conf.loc[term, 1],
                    "nobs": fitted.nobs,
                    "adj_r2": fitted.rsquared_adj,
                }
            )
        jb_stat, jb_p, _, _ = sm.stats.jarque_bera(fitted.resid)
        diagnostic_rows.append(
            {
                "model": model_name,
                "dependent_variable": dep,
                "nobs": fitted.nobs,
                "adj_r2": fitted.rsquared_adj,
                "aic": fitted.aic,
                "bic": fitted.bic,
                "f_pvalue": fitted.f_pvalue,
                "durbin_watson": sm.stats.stattools.durbin_watson(fitted.resid),
                "jarque_bera_pvalue": jb_p,
                "predictors": ", ".join(formula_terms),
            }
        )
        summaries[model_name] = fitted.summary().as_text()
    return pd.DataFrame(coef_rows), pd.DataFrame(diagnostic_rows), summaries


def plot_histogram(series: pd.Series, title: str, path: Path) -> None:
    series = series.dropna()
    if series.empty:
        plt.figure(figsize=(8, 5))
        plt.title(title)
        plt.text(0.5, 0.5, "No non-NaN observations", ha="center", va="center")
        plt.xlabel("Value")
        plt.ylabel("Frequency")
        plt.tight_layout()
        plt.savefig(path, dpi=200)
        plt.close()
        return
    plt.figure(figsize=(8, 5))
    plt.hist(series, bins=20, edgecolor="black")
    plt.title(title)
    plt.xlabel("Value")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()


def plot_scatter(
    x: pd.Series,
    y: pd.Series,
    title: str,
    xlabel: str,
    ylabel: str,
    path: Path,
) -> None:
    frame = pd.DataFrame({"x": x, "y": y}).dropna()
    if frame.empty:
        plt.figure(figsize=(7, 6))
        plt.title(title)
        plt.text(0.5, 0.5, "No paired non-NaN observations", ha="center", va="center")
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.tight_layout()
        plt.savefig(path, dpi=200)
        plt.close()
        return
    plt.figure(figsize=(7, 6))
    plt.scatter(frame["x"], frame["y"], alpha=0.7)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()


def plot_event_profile(panel: pd.DataFrame, ar_col: str, t_col: str, title: str, path: Path, low: int = -10, high: int = 10) -> None:
    sub = panel.loc[panel[t_col].between(low, high) & panel[ar_col].notna()].copy()
    if sub.empty:
        return
    g = sub.groupby(t_col)[ar_col]
    avg = g.mean().sort_index()
    n_per_t = g.size().reindex(avg.index, fill_value=0)
    car = avg.cumsum()
    fig, ax = plt.subplots(2, 1, figsize=(8, 7), sharex=True)
    ax[0].bar(avg.index, avg.values, color="steelblue")
    ax0r = ax[0].twinx()
    ax0r.step(n_per_t.index, n_per_t.values, where="mid", color="tab:red", alpha=0.85, linewidth=1.2)
    ax0r.set_ylabel("N(t)", color="tab:red")
    ax0r.tick_params(axis="y", labelcolor="tab:red")
    ax[0].set_title(f"{title}: Average AR and event count N(t)")
    ax[0].set_ylabel("AR")
    ax[1].plot(car.index, car.values, marker="o")
    ax[1].set_title(f"{title}: Cumulative Abnormal Return")
    ax[1].set_xlabel("Event day t")
    ax[1].set_ylabel("CAR")
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def export_dataframe(df: pd.DataFrame, csv_path: Path, xlsx_path: Optional[Path] = None) -> None:
    df.to_csv(csv_path, index=False, encoding="utf-8")
    if xlsx_path:
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="data")


def main() -> None:
    WARNING_LOG.clear()
    ensure_output_dirs()

    print_header("FOUND FILES AND ASSIGNED ROLES")
    roles = find_input_files(OUT_INPUT_DIR)
    for role, path in roles.items():
        print(f"{role:>20}: {path.name}")

    print_header("LOADING DATA")
    base = load_base_dataset(roles["base"])
    intraday = load_intraday_panel(roles["intraday"])
    announcement_daily = load_daily_panel(roles["announcement_daily"], role="announcement_daily")
    actualization_daily = load_daily_panel(roles["actualization_daily"], role="actualization_daily")
    create_daily = load_daily_panel(roles["create_daily"], role="create_daily")

    print(f"Base deals: {base.shape}")
    print(f"Intraday panel: {intraday.shape}")
    print(f"Announcement daily panel: {announcement_daily.shape}")
    print(f"Actualization daily panel: {actualization_daily.shape}")
    print(f"Create daily panel: {create_daily.shape}")

    print_header("SCHEMA INSPECTION")
    print("Base columns sample:", list(base.columns[:20]))
    print("Intraday columns:", intraday.columns.tolist())
    print("Daily columns:", announcement_daily.columns.tolist())

    print_header("MATCHING AUDIT")
    mapping_frames = []
    for role, panel in [
        ("intraday", intraday),
        ("announcement_daily", announcement_daily),
        ("actualization_daily", actualization_daily),
        ("create_daily", create_daily),
    ]:
        status = evaluate_matching(base, panel, role)
        mapping_frames.append(status)
        counts = status["mapping_status"].value_counts(dropna=False).to_dict()
        print(f"{role}: {counts}")
    mapping_audit = pd.concat(mapping_frames, ignore_index=True)

    intraday = filter_certain_rows(intraday, mapping_audit.loc[mapping_audit["table_role"] == "intraday"])
    announcement_daily = filter_certain_rows(announcement_daily, mapping_audit.loc[mapping_audit["table_role"] == "announcement_daily"])
    actualization_daily = filter_certain_rows(actualization_daily, mapping_audit.loc[mapping_audit["table_role"] == "actualization_daily"])
    create_daily = filter_certain_rows(create_daily, mapping_audit.loc[mapping_audit["table_role"] == "create_daily"])

    print_header("EVENT STUDY CALCULATIONS")
    intraday_metrics = compute_intraday_metrics(base, intraday)
    ann_metrics, ann_panel_annotated = compute_anchored_daily_metrics(announcement_daily, prefix="ANN")
    act_metrics, act_panel_annotated = compute_anchored_daily_metrics(actualization_daily, prefix="ACT")
    create_metrics, create_panel_annotated = compute_anchored_daily_metrics(create_daily, prefix="CREATE")
    completion_metrics = compute_completion_metrics(
        base,
        {
            "announcement_daily": announcement_daily,
            "actualization_daily": actualization_daily,
            "create_daily": create_daily,
        },
    )
    leakage_metrics = compute_leakage_metrics(base, ann_metrics, create_metrics)
    leakage_metrics = add_pre_announcement_runups(leakage_metrics, ann_panel_annotated)
    ann_create_diff = (
        ann_metrics[["source_row_excel", "CAR_ANN_1_1", "CAR_ANN_30_5"]]
        .merge(
            create_metrics[["source_row_excel", "CAR_CREATE_1_1", "CAR_CREATE_30_5"]],
            on="source_row_excel",
            how="outer",
        )
    )
    ann_create_diff["DIFF_CAR_ANN_CREATE_1_1"] = ann_create_diff["CAR_ANN_1_1"] - ann_create_diff["CAR_CREATE_1_1"]
    ann_create_diff["DIFF_CAR_ANN_CREATE_30_5"] = ann_create_diff["CAR_ANN_30_5"] - ann_create_diff["CAR_CREATE_30_5"]
    ann_create_diff = ann_create_diff[
        ["source_row_excel", "DIFF_CAR_ANN_CREATE_1_1", "DIFF_CAR_ANN_CREATE_30_5"]
    ]

    print("Announcement intraday deals with metrics:", int(intraday_metrics["CAR_ANN_INTRADAY_15M"].notna().sum()))
    print("Announcement daily deals with CAR:", int(ann_metrics["CAR_ANN_1_1"].notna().sum()))
    print("Completion daily deals with CAR:", int(completion_metrics["CAR_CLOSE_1_1"].notna().sum()))
    print("Create-date deals with CAR:", int(create_metrics["CAR_CREATE_1_1"].notna().sum()))

    print_header("BUILDING ENRICHED DEAL DATASET")
    enriched = (
        base.merge(intraday_metrics, on="source_row_excel", how="left")
        .merge(ann_metrics, on="source_row_excel", how="left")
        .merge(completion_metrics, on="source_row_excel", how="left")
        .merge(leakage_metrics, on="source_row_excel", how="left")
        .merge(ann_create_diff, on="source_row_excel", how="left")
        .merge(act_metrics, on="source_row_excel", how="left")
        .merge(create_metrics, on="source_row_excel", how="left")
    )
    if "deal_type_clean" in enriched.columns:
        enriched["deal_type"] = enriched["deal_type_clean"].fillna("unknown").astype(str).str.strip()
        enriched.loc[enriched["deal_type"].eq("") | enriched["deal_type"].eq("nan"), "deal_type"] = "unknown"
    else:
        enriched["deal_type"] = "unknown"
    if "RUNUP_PRE_30_5_x" in enriched.columns:
        enriched["RUNUP_PRE_30_5"] = enriched["RUNUP_PRE_30_5_y"].combine_first(enriched["RUNUP_PRE_30_5_x"])
        enriched["CAR_PRE_ANNOUNCEMENT"] = enriched["CAR_PRE_ANNOUNCEMENT_y"].combine_first(enriched["CAR_PRE_ANNOUNCEMENT_x"])
        enriched = enriched.drop(
            columns=[
                "RUNUP_PRE_30_5_x",
                "RUNUP_PRE_30_5_y",
                "CAR_PRE_ANNOUNCEMENT_x",
                "CAR_PRE_ANNOUNCEMENT_y",
            ]
        )

    metric_cols = [
        "CAR_ANN_INTRADAY_15M",
        "CAR_ANN_INTRADAY_30M",
        "CAR_ANN_INTRADAY_M60_P180",
        "CAR_ANN_INTRADAY_1H",
        "CAR_ANN_INTRADAY_3H",
        "CAR_ANN_INTRADAY_FULL",
        "CAR_ANN_INTRADAY_TO_CLOSE",
        "CAR_ANN_NEXT_OPEN_1H",
        "CAR_ANN_NEXT_DAY",
        "CAR_INTRADAY_PRE_4_0",
        "CAR_INTRADAY_PRE_4_0_prevday",
        "ratio_1h_vs_day",
        "CAR_ANN_1_1",
        "CAR_ANN_3_3",
        "CAR_ANN_5_5",
        "CAR_ANN_10_10",
        "CAR_ANN_30_30",
        "CAR_ANN_50_50",
        "CAR_ANN_30_5",
        "BHAR_ANN_60",
        "BHAR_ANN_120",
        "BHAR_ANN_250",
        "CAR_CLOSE_1_1",
        "CAR_CLOSE_3_3",
        "CAR_CLOSE_5_5",
        "CAR_CLOSE_10_10",
        "BHAR_CLOSE_60",
        "BHAR_CLOSE_120",
        "BHAR_CLOSE_250",
        "RUNUP_PRE_30_5",
        "CAR_PRE_ANNOUNCEMENT",
        "DIFF_CAR_ANN_MINUS_CREATE_1_1",
        "CAR_CREATE_1_1",
        "CAR_CREATE_3_3",
        "CAR_CREATE_30_5",
        "CAR_ACT_30_30",
        "CAR_ACT_50_50",
        "CAR_ACT_30_5",
        "CAR_CREATE_30_30",
        "CAR_CREATE_50_50",
        "CAR_ACT_1_1",
        "CAR_ACT_3_3",
        "DIFF_CAR_ANN_CREATE_1_1",
        "DIFF_CAR_ANN_CREATE_30_5",
    ]

    print_header("HYPOTHESIS TESTS AND REGRESSIONS")
    summary_stats = run_summary_stats(enriched, metric_cols)
    one_sample_tests = run_one_sample_tests(enriched, metric_cols)
    fast_reaction_intraday_summary = build_metric_focus_table(
        enriched,
        one_sample_tests,
        [
            "CAR_ANN_INTRADAY_15M",
            "CAR_ANN_INTRADAY_30M",
            "CAR_ANN_INTRADAY_1H",
            "CAR_INTRADAY_PRE_4_0",
            "ratio_1h_vs_day",
        ],
        block_label="fast_reaction_intraday",
    )
    daily_leakage_summary = build_metric_focus_table(
        enriched,
        one_sample_tests,
        [
            "CAR_ANN_1_1",
            "CAR_CREATE_1_1",
            "RUNUP_PRE_30_5",
            "CAR_PRE_ANNOUNCEMENT",
            "DIFF_CAR_ANN_MINUS_CREATE_1_1",
        ],
        block_label="leakage_daily",
    )
    paired_rows: List[Dict[str, object]] = []
    for label, ann_col, create_col in [
        ("CAR_ANN_vs_CREATE_1_1", "CAR_ANN_1_1", "CAR_CREATE_1_1"),
        ("CAR_ANN_vs_CREATE_30_5", "CAR_ANN_30_5", "CAR_CREATE_30_5"),
    ]:
        if ann_col not in enriched.columns or create_col not in enriched.columns:
            continue
        pair = enriched[[ann_col, create_col]].dropna()
        if len(pair) < 3:
            paired_rows.append(
                {
                    "test_label": label,
                    "announcement_metric": ann_col,
                    "create_metric": create_col,
                    "n_pairs": int(len(pair)),
                    "mean_diff_ann_minus_create": np.nan,
                    "t_stat": np.nan,
                    "p_value": np.nan,
                }
            )
            continue
        diff = pair[ann_col] - pair[create_col]
        t_stat, p_value = stats.ttest_1samp(diff, popmean=0.0, nan_policy="omit")
        paired_rows.append(
            {
                "test_label": label,
                "announcement_metric": ann_col,
                "create_metric": create_col,
                "n_pairs": int(len(pair)),
                "mean_diff_ann_minus_create": float(diff.mean()),
                "t_stat": float(t_stat),
                "p_value": float(p_value),
            }
        )
    paired_tests = pd.DataFrame(paired_rows)
    group_tests = run_group_tests(enriched, ["CAR_ANN_1_1", "CAR_CLOSE_1_1", "RUNUP_PRE_30_5", "BHAR_ANN_120"])
    reg_coefs, reg_diag, reg_summaries = run_regressions(enriched)

    print("Summary metrics:", len(summary_stats))
    print("One-sample tests:", len(one_sample_tests))
    print("Group tests:", len(group_tests))
    print("Regression coefficient rows:", len(reg_coefs))

    print_header("EXPORTING OUTPUTS")
    export_dataframe(base, OUTPUT_DIRS["clean_data"] / "base_deals_standardized.csv")
    export_dataframe(intraday, OUTPUT_DIRS["clean_data"] / "intraday_panel_clean.csv")
    export_dataframe(ann_panel_annotated, OUTPUT_DIRS["clean_data"] / "announcement_daily_panel_clean.csv")
    export_dataframe(act_panel_annotated, OUTPUT_DIRS["clean_data"] / "actualization_daily_panel_clean.csv")
    export_dataframe(create_panel_annotated, OUTPUT_DIRS["clean_data"] / "create_daily_panel_clean.csv")
    export_dataframe(
        enriched,
        OUTPUT_DIRS["clean_data"] / "ma_deals_enriched.csv",
        OUTPUT_DIRS["clean_data"] / "ma_deals_enriched.xlsx",
    )
    export_dataframe(mapping_audit, OUTPUT_DIRS["tables"] / "mapping_audit.csv")
    export_dataframe(summary_stats, OUTPUT_DIRS["tables"] / "summary_statistics.csv", OUTPUT_DIRS["tables"] / "summary_statistics.xlsx")
    export_dataframe(one_sample_tests, OUTPUT_DIRS["tables"] / "one_sample_tests.csv", OUTPUT_DIRS["tables"] / "one_sample_tests.xlsx")
    export_dataframe(
        fast_reaction_intraday_summary,
        OUTPUT_DIRS["tables"] / "fast_reaction_intraday_summary.csv",
        OUTPUT_DIRS["tables"] / "fast_reaction_intraday_summary.xlsx",
    )
    export_dataframe(
        daily_leakage_summary,
        OUTPUT_DIRS["tables"] / "daily_leakage_summary.csv",
        OUTPUT_DIRS["tables"] / "daily_leakage_summary.xlsx",
    )
    export_dataframe(paired_tests, OUTPUT_DIRS["tables"] / "paired_tests.csv", OUTPUT_DIRS["tables"] / "paired_tests.xlsx")
    export_dataframe(group_tests, OUTPUT_DIRS["tables"] / "group_tests.csv", OUTPUT_DIRS["tables"] / "group_tests.xlsx")
    model_count_rows: List[Dict[str, object]] = []
    for label, mframe in [
        ("announcement_daily", ann_metrics),
        ("actualization_daily", act_metrics),
        ("create_daily", create_metrics),
    ]:
        col = next((c for c in mframe.columns if c.endswith("_MODEL_TYPE")), None)
        if col is None:
            continue
        for mt, cnt in mframe[col].value_counts(dropna=False).items():
            model_count_rows.append({"panel": label, "model_type": str(mt), "n_deals": int(cnt)})
    export_dataframe(
        pd.DataFrame(model_count_rows),
        OUTPUT_DIRS["tables"] / "model_type_counts.csv",
        OUTPUT_DIRS["tables"] / "model_type_counts.xlsx",
    )
    export_dataframe(reg_coefs, OUTPUT_DIRS["models"] / "regression_coefficients.csv", OUTPUT_DIRS["models"] / "regression_coefficients.xlsx")
    export_dataframe(reg_diag, OUTPUT_DIRS["models"] / "regression_diagnostics.csv", OUTPUT_DIRS["models"] / "regression_diagnostics.xlsx")
    warning_df = warning_frame()
    if warning_df.empty:
        warning_df = pd.DataFrame(columns=["category", "message", "table_role", "source_row_excel"])
    export_dataframe(warning_df, OUTPUT_DIRS["tables"] / "warnings.csv", OUTPUT_DIRS["tables"] / "warnings.xlsx")

    for model_name, summary_text in reg_summaries.items():
        (OUTPUT_DIRS["models"] / f"{model_name}_summary.txt").write_text(summary_text, encoding="utf-8")

    plot_histogram(enriched["CAR_ANN_1_1"], "Distribution of CAR_ANN_1_1", OUTPUT_DIRS["charts"] / "car_ann_1_1_hist.png")
    plot_histogram(
        enriched["CAR_ANN_INTRADAY_15M"],
        "Distribution of CAR_ANN_INTRADAY_15M (fast market reaction)",
        OUTPUT_DIRS["charts"] / "car_ann_intraday_15m_hist.png",
    )
    plot_histogram(
        enriched["RUNUP_PRE_30_5"],
        "Distribution of pre-announcement run-up CAR [-30; -5]",
        OUTPUT_DIRS["charts"] / "runup_pre_30_5_hist.png",
    )
    plot_histogram(
        enriched["DIFF_CAR_ANN_MINUS_CREATE_1_1"],
        "Distribution of CAR_ANN_1_1 - CAR_CREATE_1_1",
        OUTPUT_DIRS["charts"] / "diff_car_ann_create_1_1_hist.png",
    )
    plot_histogram(enriched["BHAR_ANN_120"], "Distribution of BHAR_ANN_120", OUTPUT_DIRS["charts"] / "bhar_ann_120_hist.png")
    plot_histogram(enriched["BHAR_ANN_250"], "Distribution of BHAR_ANN_250", OUTPUT_DIRS["charts"] / "bhar_ann_250_hist.png")
    plot_histogram(enriched["CAR_CLOSE_1_1"], "Distribution of CAR_CLOSE_1_1", OUTPUT_DIRS["charts"] / "car_close_1_1_hist.png")
    plot_scatter(
        enriched["CAR_ANN_1_1"],
        enriched["BHAR_ANN_120"],
        "BHAR_ANN_120 vs CAR_ANN_1_1",
        "CAR_ANN_1_1",
        "BHAR_ANN_120",
        OUTPUT_DIRS["charts"] / "bhar_ann_120_vs_car_ann_1_1_scatter.png",
    )
    plot_event_profile(ann_panel_annotated, "ar_ann", "t", "Announcement Event Profile", OUTPUT_DIRS["charts"] / "announcement_event_profile.png")
    plot_event_profile(create_panel_annotated, "ar_create", "t", "Create-Date Event Profile", OUTPUT_DIRS["charts"] / "create_event_profile.png")

    print_header("WARNINGS SUMMARY")
    if warning_df.empty:
        print("No warnings logged.")
    else:
        print(warning_df["category"].value_counts().to_string())
        print("\nSample warnings:")
        print(warning_df.head(10).to_string(index=False))

    print_header("DONE")
    print("Main output:", OUTPUT_DIRS["clean_data"] / "ma_deals_enriched.xlsx")
    print("Warnings log:", OUTPUT_DIRS["tables"] / "warnings.csv")


if __name__ == "__main__":
    main()
