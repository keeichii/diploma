from __future__ import annotations

import logging
import re
import sys
import time
from dataclasses import dataclass
import os
import pickle
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from t_tech.invest import CandleInterval, Client, InstrumentStatus
from t_tech.invest.exceptions import UnauthenticatedError
from t_tech.invest.utils import quotation_to_decimal

from .benchmarks_provider import BenchmarksProvider
from .daily_metrics_filler import DailyMetricsFiller
from .financials_provider import FinancialsProvider
from .config import Config


MSK = ZoneInfo("Europe/Moscow")
UTC = timezone.utc

DEFAULT_EXCHANGE_OPEN_TIME = dt_time(10, 0)
PREFERRED_CLASS_CODES = {"TQBR", "FQBR"}

RATE_LIMIT_RETRIES = 7
RATE_LIMIT_SLEEP_BASE = 3
RATE_LIMIT_HITS_TOTAL = 0

MIN_15M_FETCH_PADDING_DAYS = 20
DAILY_FETCH_CALENDAR_PADDING = 900

SHARES_CACHE: dict[str, list[dict[str, Any]]] | None = None
SHARES_BY_NAME: list[dict[str, Any]] | None = None

INTRADAY_15M_CACHE: dict[tuple[str, str], list[Any]] = {}
DAILY_CANDLE_CACHE: dict[tuple[str, str, str], list[Any]] = {}
FILE_TABLE_CACHE: dict[str, pd.DataFrame] = {}
CACHE_DIR: Path | None = None

logger = logging.getLogger("ma_event_study")


@dataclass(frozen=True)
class ResolvedInstrument:
    ok: bool
    ticker: str | None = None
    name: str | None = None
    class_code: str | None = None
    uid: str | None = None
    figi: str | None = None
    instrument_id: str | None = None
    reason: str | None = None
    resolve_method: str | None = None


@dataclass(frozen=True)
class ReleaseAnchor:
    ok: bool
    release_date: date | None = None
    release_time: dt_time | None = None
    anchor_trade_date: date | None = None
    anchor_timestamp_msk: datetime | None = None
    is_off_market_release: int = 0
    reason: str | None = None


SHEET_CANDIDATES = ["Deals_sorted", "Dealssorted", "Deals sorted", "DealsSorted", "Sheet1"]

COL_COMPANY_ALIASES = ["покупатель", "buyer", "buyer name", "company", "компания", "acquirer", "acquirer name"]
COL_TICKER_ALIASES = ["тикер покупателя", "buyer ticker", "buyer_ticker", "ticker", "тикер"]
COL_OBJECT_ALIASES = ["объект сделки", "deal object", "target", "target name", "объект", "цель сделки"]
COL_FIRST_PRESS_RELEASE_DATE_ALIASES = [
    "first_press_release_date",
    "first press release date",
    "firstpressreleasedate",
    "дата первого пресс релиза",
    "дата первого пресс-релиза",
    "дата объявления сделки",
    "дата объявления",
]
COL_FIRST_PRESS_RELEASE_TIME_ALIASES = [
    "first_press_release_time",
    "first press release time",
    "firstpressreleasetime",
    "время первого пресс релиза",
    "время первого пресс-релиза",
    "время объявления сделки",
    "время объявления",
]
COL_CBONDS_ACTUALIZATION_DATE_ALIASES = [
    "cbonds_actualization_date",
    "cbonds actualization date",
    "cbondsactualizationdate",
    "дата актуализации",
    "дата актуализации cbonds",
    "дата актуализации информации о сделке в cbonds",
]
COL_CBONDS_CREATE_DATE_ALIASES = [
    "cbonds_create_date",
    "cbonds create date",
    "cbondscreatedate",
    "дата создания",
    "дата первой записи cbonds",
    "дата первой записи о сделке в cbonds",
]

AUDIT_COLS = [
    "audit_row_status",
    "audit_skip_reason",
    "audit_notes",
    "audit_exception",
    "audit_resolved_flag",
    "audit_resolved_ticker",
    "audit_resolved_name",
    "audit_resolved_class_code",
    "audit_resolved_uid",
    "audit_resolved_figi",
    "audit_rate_limit_hits",
    "audit_first_press_release_date_parsed",
    "audit_first_press_release_time_parsed",
    "audit_cbonds_actualization_date_parsed",
    "audit_cbonds_create_date_parsed",
    "audit_is_off_market_release",
    "audit_release_anchor_trade_date",
    "audit_release_anchor_timestamp_msk",
    "audit_release_anchor_reason",
    "audit_python_version",
    "audit_run_ts",
]

TABLE1_COLS = [
    "source_row_excel",
    "buyer_ticker",
    "buyer_company",
    "deal_object",
    "event_name",
    "release_date",
    "release_time",
    "is_off_market_release",
    "anchor_trade_date",
    "anchor_timestamp_msk",
    "trade_day_offset",
    "timestamp_msk",
    "price_at_timestamp_rub",
    "volume_during_timestamp_plus_15m_mn_rub",
    "pipeline_row_note",
]

TABLE2_COLS = [
    "source_row_excel",
    "buyer_ticker",
    "buyer_company",
    "deal_object",
    "event_name",
    "anchor_date",
    "anchor_date_raw",
    "anchor_trade_date",
    "is_off_market_release",
    "t",
    "Date",
    "Adjusted Close, руб.",
    "Close, руб.",
    "Volume, млрд. руб.",
    "Market Capitalization, млрд. руб.",
    "IMOEX daily close",
    "RUONIA (daily)",
    "ROE",
    "ROA",
    "P/B",
    "P/E",
    "Total Assets",
    "pipeline_row_note",
]


def _raw_cell(row: pd.Series, col_map: dict[str, str | None], key: str) -> Any:
    c = col_map.get(key)
    if not c:
        return None
    return row.get(c)


def _display_buyer_ticker(row: pd.Series, col_map: dict[str, str | None], source_df: pd.DataFrame, i: int) -> Any:
    if int(source_df.at[i, "audit_resolved_flag"] or 0) == 1:
        t = source_df.at[i, "audit_resolved_ticker"]
        return t if pd.notna(t) else None
    raw = _raw_cell(row, col_map, "buyer_ticker")
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    return s if s else None


def _table2_stub_row(
    *,
    source_row_excel: int,
    buyer_ticker: Any,
    buyer_company: Any,
    deal_object: Any,
    event_name: str,
    anchor_date: date | None,
    anchor_date_raw: str | None = None,
    anchor_trade_date: date | None,
    is_off_market_release: int,
    t: int,
    date_val: date | None,
    note: str,
) -> dict[str, Any]:
    return {
        "source_row_excel": source_row_excel,
        "buyer_ticker": buyer_ticker,
        "buyer_company": buyer_company,
        "deal_object": deal_object,
        "event_name": event_name,
        "anchor_date": anchor_date.isoformat() if anchor_date else None,
        "anchor_date_raw": anchor_date_raw,
        "anchor_trade_date": anchor_trade_date.isoformat() if anchor_trade_date else None,
        "is_off_market_release": is_off_market_release,
        "t": t,
        "Date": date_val.isoformat() if date_val else None,
        "Adjusted Close, руб.": None,
        "Close, руб.": None,
        "Volume, млрд. руб.": None,
        "Market Capitalization, млрд. руб.": None,
        "IMOEX daily close": None,
        "RUONIA (daily)": None,
        "ROE": None,
        "ROA": None,
        "P/B": None,
        "P/E": None,
        "Total Assets": None,
        "pipeline_row_note": note,
    }


def _table1_stub_row(
    *,
    source_row_excel: int,
    buyer_ticker: Any,
    buyer_company: Any,
    deal_object: Any,
    release_date_raw: Any,
    release_time_raw: Any,
    anchor: ReleaseAnchor | None,
    note: str,
) -> dict[str, Any]:
    rd = parse_date_any(release_date_raw)
    rt = parse_time_any(release_time_raw)
    disp_date = anchor.release_date if anchor and anchor.release_date else rd
    disp_time = anchor.release_time if anchor and anchor.release_time else rt
    return {
        "source_row_excel": source_row_excel,
        "buyer_ticker": buyer_ticker,
        "buyer_company": buyer_company,
        "deal_object": deal_object,
        "event_name": "first_press_release",
        "release_date": disp_date.isoformat() if disp_date else None,
        "release_time": disp_time.strftime("%H:%M") if disp_time else None,
        "is_off_market_release": anchor.is_off_market_release if anchor else None,
        "anchor_trade_date": anchor.anchor_trade_date.isoformat() if anchor and anchor.anchor_trade_date else None,
        "anchor_timestamp_msk": anchor.anchor_timestamp_msk.isoformat() if anchor and anchor.anchor_timestamp_msk else None,
        "trade_day_offset": None,
        "timestamp_msk": None,
        "price_at_timestamp_rub": None,
        "volume_during_timestamp_plus_15m_mn_rub": None,
        "pipeline_row_note": note,
    }


def setup_logging(log_path: Path) -> None:
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    log_path.parent.mkdir(parents=True, exist_ok=True)

    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)


def _normalize_header(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower().replace("ё", "е")
    s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = "".join(ch if (ch.isalnum() or ch.isspace() or ch in "_-") else " " for ch in s)
    s = s.replace("_", " ").replace("-", " ")
    s = " ".join(s.split())
    return s


def _norm_text(x: Any) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return " ".join(str(x).replace("\xa0", " ").strip().upper().replace("Ё", "Е").split())


def _norm_name(x: Any) -> str:
    s = _norm_text(x)
    cleaned = []
    for ch in s:
        if ch.isalnum() or ch.isspace():
            cleaned.append(ch)
        else:
            cleaned.append(" ")
    return " ".join("".join(cleaned).split())


def _norm_ticker(x: Any) -> str:
    s = _norm_text(x)
    return "".join(ch for ch in s if (ch.isalnum() or ch == "."))


def _validate_parsed_date(result: date, raw: Any) -> date | None:
    if result.year < 1990 or result.year > 2035:
        logger.warning("SUSPICIOUS_DATE | raw=%r | parsed=%s", raw, result)
        return None
    return result


def parse_date_any(x: Any) -> date | None:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, bool):
        return None
    if isinstance(x, pd.Timestamp):
        return _validate_parsed_date(x.date(), x)
    if isinstance(x, datetime):
        return _validate_parsed_date(x.date(), x)
    if isinstance(x, date):
        return _validate_parsed_date(x, x)
    # numpy scalar / Excel serial
    if hasattr(x, "item") and not isinstance(x, (bytes, str)):
        try:
            x = x.item()
        except Exception:
            pass
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        xf = float(x)
        # Excel serial: безопасный диапазон; не путать с календарным годом (1900–2100)
        if 1000 < xf < 100_000:
            is_int_like = abs(xf - round(xf)) < 1e-9
            if is_int_like and 1900 <= int(round(xf)) <= 2100:
                return None
            try:
                d = date(1899, 12, 30) + timedelta(days=int(round(xf)))
                return _validate_parsed_date(d, x)
            except Exception:
                pass
        return None
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return None
    # DD.MM.YYYY — явно (первое число = день); не полагаться на dayfirst в pandas
    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", s)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return _validate_parsed_date(date(year, month, day), x)
        except ValueError:
            return None
    # ISO YYYY-MM-DD
    m_iso = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m_iso:
        y, mo, d = int(m_iso.group(1)), int(m_iso.group(2)), int(m_iso.group(3))
        try:
            return _validate_parsed_date(date(y, mo, d), x)
        except ValueError:
            return None
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if pd.isna(dt):
        return None
    return _validate_parsed_date(dt.date(), x)


def parse_time_any(x: Any) -> dt_time | None:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, pd.Timestamp):
        return x.time().replace(second=0, microsecond=0)
    if isinstance(x, datetime):
        return x.time().replace(second=0, microsecond=0)
    if isinstance(x, dt_time):
        return x.replace(second=0, microsecond=0)
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) in {3, 4}:
        hh = int(digits[:-2])
        mm = int(digits[-2:])
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return dt_time(hh, mm)
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.to_pydatetime().time().replace(second=0, microsecond=0)


def _combine_msk(d: date, t: dt_time | None) -> datetime:
    t = t or DEFAULT_EXCHANGE_OPEN_TIME
    return datetime(d.year, d.month, d.day, t.hour, t.minute, tzinfo=MSK)


def _start_of_day_msk(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=MSK)


def _end_of_day_msk(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=MSK)


def _candle_dt_msk(candle: Any) -> datetime:
    t = getattr(candle, "time", None)
    if t is None:
        raise ValueError("candle.time is None")
    if t.tzinfo is None:
        return t.replace(tzinfo=UTC).astimezone(MSK)
    return t.astimezone(MSK)


def _q_to_float(q: Any) -> float | None:
    if q is None:
        return None
    try:
        return float(quotation_to_decimal(q))
    except Exception:
        pass
    units = getattr(q, "units", None)
    nano = getattr(q, "nano", None)
    if units is not None or nano is not None:
        return float(units or 0) + float(nano or 0) / 1_000_000_000
    if isinstance(q, (int, float)):
        return float(q)
    return None


def _to_float(x: Any) -> float | None:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(" ", "").replace(",", ".")
    s = "".join(ch for ch in s if (ch.isdigit() or ch in ".-"))
    if s in {"", "-", ".", "-."}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _avg_ignore_none(values: list[float | None]) -> float | None:
    vals = [v for v in values if v is not None]
    return (sum(vals) / len(vals)) if vals else None


def _scale_to_bln_if_needed(v: float | None) -> float | None:
    if v is None:
        return None
    return (v / 1e9) if abs(v) >= 1e8 else v


def _msg_is_rate_limit(msg: str) -> bool:
    m = msg.upper()
    return "RESOURCE_EXHAUSTED" in m or "RATE_LIMIT" in m or "429" in m


def _call_with_retry(fn, *args, **kwargs):
    global RATE_LIMIT_HITS_TOTAL
    last_exc: Exception | None = None
    for attempt in range(RATE_LIMIT_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if _msg_is_rate_limit(str(e)):
                RATE_LIMIT_HITS_TOTAL += 1
                sleep_s = min(RATE_LIMIT_SLEEP_BASE * (attempt + 1), 20)
                logger.warning(
                    "RATE_LIMIT | fn=%s | attempt=%s | sleep=%ss | error=%s",
                    getattr(fn, "__name__", str(fn)),
                    attempt + 1,
                    sleep_s,
                    e,
                )
                time.sleep(sleep_s)
                continue
            raise
    assert last_exc is not None
    raise last_exc


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for c in columns:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def _normalize_sheet_name(x: str) -> str:
    s = str(x).strip().lower()
    for ch in " _-":
        s = s.replace(ch, "")
    return s


def _resolve_sheet_name(xlsx_path: Path, requested_sheet: str | None) -> str:
    xls = pd.ExcelFile(xlsx_path)
    available = list(xls.sheet_names)
    logger.info("AVAILABLE_SHEETS | %s", available)
    if requested_sheet and requested_sheet in available:
        return requested_sheet
    targets = {_normalize_sheet_name(x) for x in SHEET_CANDIDATES}
    for s in available:
        if _normalize_sheet_name(s) in targets:
            return s
    if not available:
        raise ValueError("В Excel-файле не найдено ни одного листа.")
    logger.warning("SHEET_FALLBACK | using first sheet=%s", available[0])
    return available[0]


def read_input_excel(xlsx_path: Path, requested_sheet: str | None) -> tuple[pd.DataFrame, str]:
    sheet_name = _resolve_sheet_name(xlsx_path, requested_sheet)
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    df.columns = [str(c).strip() for c in df.columns]
    return df, sheet_name


def _pick_col(df: pd.DataFrame, aliases: list[str], fallback_index: int | None, label: str) -> str | None:
    cols = [c for c in df.columns if not str(c).strip().lower().startswith("audit_")]
    norm_map = {c: _normalize_header(c) for c in cols}
    alias_norm = {_normalize_header(a) for a in aliases}
    exact = [c for c in cols if norm_map[c] in alias_norm]
    if exact:
        logger.info("COLUMN_RESOLVED | %s -> %r", label, exact[0])
        return exact[0]
    contains: list[str] = []
    for c in cols:
        n = norm_map[c]
        if any(a and (a in n or n in a) for a in alias_norm):
            contains.append(c)
    if contains:
        logger.info("COLUMN_RESOLVED_CONTAINS | %s -> %r", label, contains[0])
        return contains[0]
    if fallback_index is not None and 0 <= fallback_index < len(cols):
        fallback = cols[fallback_index]
        logger.warning("COLUMN_FALLBACK_INDEX | %s -> %r", label, fallback)
        return fallback
    logger.warning("COLUMN_NOT_FOUND | %s", label)
    return None


def build_column_map(df: pd.DataFrame) -> dict[str, str | None]:
    logger.info("DETECTED_COLUMNS | %s", list(df.columns))
    return {
        "buyer_company": _pick_col(df, COL_COMPANY_ALIASES, 0, "buyer_company"),
        "buyer_ticker": _pick_col(df, COL_TICKER_ALIASES, 1, "buyer_ticker"),
        "deal_object": _pick_col(df, COL_OBJECT_ALIASES, 2, "deal_object"),
        "first_press_release_date": _pick_col(df, COL_FIRST_PRESS_RELEASE_DATE_ALIASES, 3, "first_press_release_date"),
        "first_press_release_time": _pick_col(df, COL_FIRST_PRESS_RELEASE_TIME_ALIASES, 4, "first_press_release_time"),
        "cbonds_actualization_date": _pick_col(df, COL_CBONDS_ACTUALIZATION_DATE_ALIASES, 17, "cbonds_actualization_date"),
        "cbonds_create_date": _pick_col(df, COL_CBONDS_CREATE_DATE_ALIASES, 18, "cbonds_create_date"),
    }


def _read_any_table(path: Path) -> pd.DataFrame:
    key = str(path.resolve())
    if key in FILE_TABLE_CACHE:
        return FILE_TABLE_CACHE[key].copy()
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, sep=None, engine="python")
    elif path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported file format: {path}")
    FILE_TABLE_CACHE[key] = df.copy()
    return df


def _first_existing_col(df: pd.DataFrame, variants: list[str]) -> str | None:
    norm_cols = {_normalize_header(c): c for c in df.columns}
    for v in variants:
        k = _normalize_header(v)
        if k in norm_cols:
            return norm_cols[k]
    for c in df.columns:
        n = _normalize_header(c)
        if any(_normalize_header(v) in n or n in _normalize_header(v) for v in variants):
            return c
    return None


def _extract_date_column(df: pd.DataFrame) -> str | None:
    return _first_existing_col(df, ["date", "дата", "reportdate", "tradedate", "datetime", "period"])


def _find_matching_file_by_name(base_dir: Path | None, hints: list[str]) -> Path | None:
    if base_dir is None or not base_dir.exists():
        return None
    candidates = [p for p in base_dir.iterdir() if p.is_file() and p.suffix.lower() in {".csv", ".xlsx", ".xls"}]
    scored: list[tuple[int, Path]] = []
    for p in candidates:
        name = _norm_text(p.name)
        score = sum(1 for h in hints if _norm_text(h) in name)
        if score:
            scored.append((score, p))
    if not scored:
        return None
    scored.sort(key=lambda x: (-x[0], x[1].name))
    return scored[0][1]


def _load_time_series_table(path: Path, value_hints: list[str]) -> pd.DataFrame | None:
    try:
        df = _read_any_table(path)
    except Exception as e:
        logger.warning("SERIES_READ_FAILED | file=%s | error=%s", path, e)
        return None
    date_col = _extract_date_column(df)
    value_col = _first_existing_col(df, value_hints)
    if not date_col or not value_col:
        return None
    out = pd.DataFrame(
        {
            "__date__": pd.to_datetime(df[date_col].map(parse_date_any)),
            "__value__": df[value_col].map(_to_float),
        }
    )
    out = out[out["__date__"].notna()].copy()
    out = out.sort_values("__date__").drop_duplicates("__date__", keep="last").reset_index(drop=True)
    return None if out.empty else out


def _lookup_series_exact_or_asof(series_df: pd.DataFrame | None, d: date) -> float | None:
    if series_df is None or series_df.empty:
        return None
    ts = pd.Timestamp(d)
    exact = series_df[series_df["__date__"] == ts]
    if not exact.empty:
        v = exact.iloc[-1]["__value__"]
        return None if pd.isna(v) else float(v)
    eligible = series_df[series_df["__date__"] <= ts]
    if eligible.empty:
        return None
    v = eligible.iloc[-1]["__value__"]
    return None if pd.isna(v) else float(v)


def _load_financial_frame(financials_dir: Path | None, ticker: str) -> pd.DataFrame | None:
    if financials_dir is None or not financials_dir.exists():
        return None
    # Лёгкая эвристика по имени файла: ищем тикер в имени.
    fpath = _find_matching_file_by_name(financials_dir, [ticker])
    if not fpath:
        return None
    try:
        df = _read_any_table(fpath)
    except Exception as e:
        logger.warning("FINANCIAL_READ_FAILED | ticker=%s | file=%s | error=%s", ticker, fpath, e)
        return None
    date_col = _extract_date_column(df)
    if not date_col:
        return None
    metric_map = {
        "adjusted_close": _first_existing_col(df, ["adjusted close", "adjustedclose", "adj close", "adjclose"]),
        "market_cap": _first_existing_col(df, ["market capitalization", "marketcapitalization", "marketcap", "капитализация"]),
        "roe": _first_existing_col(df, ["roe"]),
        "roa": _first_existing_col(df, ["roa"]),
        "pb": _first_existing_col(df, ["p/b", "pb"]),
        "pe": _first_existing_col(df, ["p/e", "pe"]),
        "assets": _first_existing_col(df, ["total assets", "totalassets", "assets", "активы"]),
    }
    keep = [date_col] + [c for c in metric_map.values() if c]
    local = df.loc[:, list(dict.fromkeys(keep))].copy()
    local["__date__"] = pd.to_datetime(local[date_col].map(parse_date_any))
    local = local[local["__date__"].notna()].copy()
    if local.empty:
        return None
    out = pd.DataFrame({"__date__": local["__date__"]})
    for k, col in metric_map.items():
        out[k] = local[col].map(_to_float) if col and col in local.columns else pd.NA
    out = out.sort_values("__date__").drop_duplicates("__date__", keep="last").reset_index(drop=True)
    return out


def _asof_financial_value(fin_df: pd.DataFrame | None, d: date, metric: str) -> float | None:
    if fin_df is None or fin_df.empty or metric not in fin_df.columns:
        return None
    ts = pd.Timestamp(d)
    eligible = fin_df[fin_df["__date__"] <= ts]
    if eligible.empty:
        return None
    v = eligible.iloc[-1][metric]
    return None if pd.isna(v) else float(v)


def _build_shares_cache(client: Client) -> None:
    global SHARES_CACHE, SHARES_BY_NAME
    if SHARES_CACHE is not None and SHARES_BY_NAME is not None:
        return
    SHARES_CACHE = {}
    SHARES_BY_NAME = []
    try:
        resp = _call_with_retry(
            client.instruments.shares,
            instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL,
        )
    except TypeError:
        resp = _call_with_retry(client.instruments.shares)
    for inst in getattr(resp, "instruments", []) or []:
        ticker = _norm_ticker(getattr(inst, "ticker", ""))
        name = (getattr(inst, "name", "") or "").strip()
        class_code = (getattr(inst, "class_code", "") or "").strip()
        figi = getattr(inst, "figi", None)
        uid = getattr(inst, "uid", None)
        currency = (getattr(inst, "currency", "") or "").strip().upper()
        api_trade_available_flag = bool(getattr(inst, "api_trade_available_flag", True))
        if not ticker:
            continue
        if class_code and class_code not in PREFERRED_CLASS_CODES:
            continue
        if currency and currency != "RUB":
            continue
        item = {
            "ticker": ticker,
            "name": name,
            "name_norm": _norm_name(name),
            "class_code": class_code,
            "uid": uid,
            "figi": figi,
            "api_trade_available_flag": api_trade_available_flag,
        }
        SHARES_CACHE.setdefault(ticker, []).append(item)
        SHARES_BY_NAME.append(item)

    def rank_key(x: dict[str, Any]):
        return (
            0 if x["class_code"] in PREFERRED_CLASS_CODES else 1,
            0 if x["api_trade_available_flag"] else 1,
            x["class_code"] or "",
            x["name"] or "",
        )

    for t, items in SHARES_CACHE.items():
        items.sort(key=rank_key)
    SHARES_BY_NAME.sort(key=rank_key)


def _resolve_instrument(client: Client, raw_ticker: Any, raw_company: Any) -> ResolvedInstrument:
    _build_shares_cache(client)
    ticker = _norm_ticker(raw_ticker)
    company_norm = _norm_name(raw_company)
    if not ticker and not company_norm:
        return ResolvedInstrument(ok=False, reason="empty_company_and_ticker", resolve_method="none")

    if ticker and SHARES_CACHE and ticker in SHARES_CACHE and SHARES_CACHE[ticker]:
        best = SHARES_CACHE[ticker][0]
        return ResolvedInstrument(
            ok=True,
            ticker=best["ticker"],
            name=best["name"],
            class_code=best["class_code"],
            uid=best["uid"],
            figi=best["figi"],
            instrument_id=best["figi"] or best["uid"],
            resolve_method="raw_ticker_exact",
        )

    if company_norm and SHARES_BY_NAME:
        tokens = set(company_norm.split())
        best: dict[str, Any] | None = None
        best_score = -1
        for it in SHARES_BY_NAME:
            name_norm = it["name_norm"]
            score = 0
            if company_norm == name_norm:
                score += 100
            if company_norm in name_norm or name_norm in company_norm:
                score += 20
            score += len(tokens & set(name_norm.split())) * 5
            if score > best_score:
                best_score = score
                best = it
        if best is not None and best_score >= 10:
            return ResolvedInstrument(
                ok=True,
                ticker=best["ticker"],
                name=best["name"],
                class_code=best["class_code"],
                uid=best["uid"],
                figi=best["figi"],
                instrument_id=best["figi"] or best["uid"],
                resolve_method="fuzzy_company_name",
            )

    return ResolvedInstrument(ok=False, ticker=ticker or None, reason="unresolved", resolve_method="none")


def _api_get_candles_compat(client: Client, instrument_id: str, from_dt: datetime, to_dt: datetime, interval: CandleInterval):
    attempts = [
        {"instrument_id": instrument_id, "from_": from_dt.astimezone(UTC), "to": to_dt.astimezone(UTC), "interval": interval},
        {"instrument_id": instrument_id, "from_": from_dt, "to": to_dt, "interval": interval},
        {"figi": instrument_id, "from_": from_dt.astimezone(UTC), "to": to_dt.astimezone(UTC), "interval": interval},
        {"figi": instrument_id, "from_": from_dt, "to": to_dt, "interval": interval},
    ]
    last_exc: Exception | None = None
    for kwargs in attempts:
        try:
            return _call_with_retry(client.market_data.get_candles, **kwargs)
        except Exception as e:
            last_exc = e
    assert last_exc is not None
    raise last_exc


def _get_intraday_15m_for_day(client: Client, instrument_id: str, day: date) -> list[Any]:
    key = (instrument_id, day.isoformat())
    if key in INTRADAY_15M_CACHE:
        return INTRADAY_15M_CACHE[key]
    # Файловый кеш на уровне дня и инструмента
    cache_path: Path | None = None
    if CACHE_DIR is not None:
        safe_inst = instrument_id.replace(":", "_")
        cache_path = CACHE_DIR / f"candles_15m_{safe_inst}_{day.isoformat()}.pkl"
        if cache_path.exists():
            try:
                with cache_path.open("rb") as f:
                    candles = pickle.load(f)
                INTRADAY_15M_CACHE[key] = candles
                return candles
            except Exception as e:
                logger.warning("CACHE_READ_FAILED | path=%s | error=%s", cache_path, e)
    try:
        resp = _api_get_candles_compat(
            client=client,
            instrument_id=instrument_id,
            from_dt=_start_of_day_msk(day),
            to_dt=_end_of_day_msk(day),
            interval=CandleInterval.CANDLE_INTERVAL_15_MIN,
        )
        candles = [c for c in getattr(resp, "candles", []) or [] if _candle_dt_msk(c).date() == day]
    except Exception as e:
        logger.debug("FETCH_15M_FAILED | instrument_id=%s | day=%s | error=%s", instrument_id, day, e)
        candles = []
    if cache_path is not None:
        try:
            with cache_path.open("wb") as f:
                pickle.dump(candles, f)
        except Exception as e:
            logger.warning("CACHE_WRITE_FAILED | path=%s | error=%s", cache_path, e)
    candles.sort(key=_candle_dt_msk)
    INTRADAY_15M_CACHE[key] = candles
    return candles


def _get_daily_window(client: Client, instrument_id: str, from_day: date, to_day: date) -> list[Any]:
    key = (instrument_id, from_day.isoformat(), to_day.isoformat())
    if key in DAILY_CANDLE_CACHE:
        return DAILY_CANDLE_CACHE[key]
    candles: list[Any] = []
    # Файловый кеш по окну дат
    cache_path: Path | None = None
    if CACHE_DIR is not None:
        safe_inst = instrument_id.replace(":", "_")
        cache_path = CACHE_DIR / f"candles_1d_{safe_inst}_{from_day.isoformat()}_{to_day.isoformat()}.pkl"
        if cache_path.exists():
            try:
                with cache_path.open("rb") as f:
                    candles = pickle.load(f)
                DAILY_CANDLE_CACHE[key] = candles
                return candles
            except Exception as e:
                logger.warning("CACHE_READ_FAILED | path=%s | error=%s", cache_path, e)
    try:
        try:
            candles = list(
                client.get_all_candles(
                    instrument_id=instrument_id,
                    from_=_start_of_day_msk(from_day),
                    to=_end_of_day_msk(to_day),
                    interval=CandleInterval.CANDLE_INTERVAL_DAY,
                )
            )
        except Exception:
            resp = _api_get_candles_compat(
                client=client,
                instrument_id=instrument_id,
                from_dt=_start_of_day_msk(from_day),
                to_dt=_end_of_day_msk(to_day),
                interval=CandleInterval.CANDLE_INTERVAL_DAY,
            )
            candles = list(getattr(resp, "candles", []) or [])
    except Exception as e:
        logger.debug("FETCH_DAILY_FAILED | instrument_id=%s | from=%s | to=%s | error=%s", instrument_id, from_day, to_day, e)
        candles = []
    candles.sort(key=_candle_dt_msk)
    DAILY_CANDLE_CACHE[key] = candles
    if cache_path is not None:
        try:
            with cache_path.open("wb") as f:
                pickle.dump(candles, f)
        except Exception as e:
            logger.warning("CACHE_WRITE_FAILED | path=%s | error=%s", cache_path, e)
    return candles


def _unique_trade_days(candles: list[Any]) -> list[date]:
    seen: set[date] = set()
    out: list[date] = []
    for c in candles:
        d = _candle_dt_msk(c).date()
        if d not in seen:
            seen.add(d)
            out.append(d)
    out.sort()
    return out


def _find_next_or_same_trade_day(trade_days: list[date], target: date) -> date | None:
    for d in trade_days:
        if d >= target:
            return d
    return None


def _find_neighbor_trade_days(trade_days: list[date], anchor_trade_date: date) -> tuple[date | None, date | None]:
    if anchor_trade_date not in trade_days:
        return None, None
    idx = trade_days.index(anchor_trade_date)
    prev_day = trade_days[idx - 1] if idx - 1 >= 0 else None
    next_day = trade_days[idx + 1] if idx + 1 < len(trade_days) else None
    return prev_day, next_day


def _first_candle_ts(candles_15m: list[Any]) -> datetime | None:
    return _candle_dt_msk(candles_15m[0]) if candles_15m else None


def _candle_session_open_dt(candles_15m: list[Any]) -> datetime | None:
    return _candle_dt_msk(candles_15m[0]) if candles_15m else None


def _candle_session_close_dt(candles_15m: list[Any]) -> datetime | None:
    return (_candle_dt_msk(candles_15m[-1]) + timedelta(minutes=15)) if candles_15m else None


def _floor_to_existing_15m_ts(candles_15m: list[Any], event_dt: datetime) -> datetime | None:
    if not candles_15m:
        return None
    eligible = [(_candle_dt_msk(c)) for c in candles_15m if _candle_dt_msk(c) <= event_dt]
    return eligible[-1] if eligible else _candle_dt_msk(candles_15m[0])


def resolve_release_anchor(client: Client, instrument_id: str, release_date_raw: Any, release_time_raw: Any) -> ReleaseAnchor:
    """
    Якорь события T по ТЗ:
    - если времени нет: якорь = open торгового дня (либо next open, если release_date неторговая)
    - если событие вне торговой сессии: якорь = next open, is_off_market_release=1
    - если в сессии: якорь floored к ближайшей существующей 15m-свече <= T
    """
    release_date = parse_date_any(release_date_raw)
    release_time = parse_time_any(release_time_raw)
    if release_date is None:
        return ReleaseAnchor(ok=False, reason="empty_first_press_release_date")

    daily = _get_daily_window(
        client,
        instrument_id,
        release_date - timedelta(days=MIN_15M_FETCH_PADDING_DAYS),
        release_date + timedelta(days=MIN_15M_FETCH_PADDING_DAYS),
    )
    trade_days = _unique_trade_days(daily)
    if not trade_days:
        return ReleaseAnchor(ok=False, release_date=release_date, release_time=release_time, reason="no_trade_days_near_release")

    same_trade_day = release_date if release_date in trade_days else None
    next_trade_day = _find_next_or_same_trade_day(trade_days, release_date)

    if release_time is None:
        if same_trade_day is not None:
            intraday = _get_intraday_15m_for_day(client, instrument_id, same_trade_day)
            anchor_ts = _first_candle_ts(intraday) or _combine_msk(same_trade_day, DEFAULT_EXCHANGE_OPEN_TIME)
            return ReleaseAnchor(
                ok=True,
                release_date=release_date,
                release_time=None,
                anchor_trade_date=same_trade_day,
                anchor_timestamp_msk=anchor_ts,
                is_off_market_release=0,
                reason="time_missing_same_day_open",
            )
        if next_trade_day is None:
            return ReleaseAnchor(ok=False, release_date=release_date, release_time=None, reason="no_next_trade_day_for_missing_time")
        intraday = _get_intraday_15m_for_day(client, instrument_id, next_trade_day)
        anchor_ts = _first_candle_ts(intraday) or _combine_msk(next_trade_day, DEFAULT_EXCHANGE_OPEN_TIME)
        return ReleaseAnchor(
            ok=True,
            release_date=release_date,
            release_time=None,
            anchor_trade_date=next_trade_day,
            anchor_timestamp_msk=anchor_ts,
            is_off_market_release=1,
            reason="time_missing_non_trade_day_next_open",
        )

    if same_trade_day is None:
    # релиз на неторговую дату -> next open
        if next_trade_day is None:
            return ReleaseAnchor(ok=False, release_date=release_date, release_time=release_time, reason="release_on_non_trade_day_no_next")
        intraday = _get_intraday_15m_for_day(client, instrument_id, next_trade_day)
        anchor_ts = _first_candle_ts(intraday) or _combine_msk(next_trade_day, DEFAULT_EXCHANGE_OPEN_TIME)
        return ReleaseAnchor(
            ok=True,
            release_date=release_date,
            release_time=release_time,
            anchor_trade_date=next_trade_day,
            anchor_timestamp_msk=anchor_ts,
            is_off_market_release=1,
            reason="release_on_non_trade_day_next_open",
        )

    intraday_same = _get_intraday_15m_for_day(client, instrument_id, same_trade_day)
    if not intraday_same:
        return ReleaseAnchor(ok=False, release_date=release_date, release_time=release_time, reason="no_intraday_on_release_day")

    event_dt = _combine_msk(release_date, release_time)
    session_open = _candle_session_open_dt(intraday_same)
    session_close = _candle_session_close_dt(intraday_same)
    if session_open is None or session_close is None:
        return ReleaseAnchor(ok=False, release_date=release_date, release_time=release_time, reason="unable_to_detect_session")

    if event_dt < session_open:
        return ReleaseAnchor(
            ok=True,
            release_date=release_date,
            release_time=release_time,
            anchor_trade_date=same_trade_day,
            anchor_timestamp_msk=session_open,
            is_off_market_release=1,
            reason="release_before_open_same_day_open",
        )
    if event_dt >= session_close:
        next_td = _find_next_or_same_trade_day(trade_days, release_date + timedelta(days=1))
        if next_td is None:
            return ReleaseAnchor(
                ok=True,
                release_date=release_date,
                release_time=release_time,
                anchor_trade_date=same_trade_day,
                anchor_timestamp_msk=session_open,
                is_off_market_release=1,
                reason="release_after_close_no_next_fallback_open",
            )
        intraday_next = _get_intraday_15m_for_day(client, instrument_id, next_td)
        anchor_ts = _first_candle_ts(intraday_next) or _combine_msk(next_td, DEFAULT_EXCHANGE_OPEN_TIME)
        return ReleaseAnchor(
            ok=True,
            release_date=release_date,
            release_time=release_time,
            anchor_trade_date=next_td,
            anchor_timestamp_msk=anchor_ts,
            is_off_market_release=1,
            reason="release_after_close_next_open",
        )

    floored = _floor_to_existing_15m_ts(intraday_same, event_dt) or session_open
    return ReleaseAnchor(
        ok=True,
        release_date=release_date,
        release_time=release_time,
        anchor_trade_date=same_trade_day,
        anchor_timestamp_msk=floored,
        is_off_market_release=0,
        reason="release_in_market",
    )


def _candle_turnover_rub(candle: Any) -> float | None:
    vol_shares = getattr(candle, "volume", None)
    if vol_shares is None:
        return None
    o = _q_to_float(getattr(candle, "open", None))
    h = _q_to_float(getattr(candle, "high", None))
    l = _q_to_float(getattr(candle, "low", None))
    c = _q_to_float(getattr(candle, "close", None))
    typical = _avg_ignore_none([o, h, l, c])
    if typical is None:
        return None
    return typical * float(vol_shares)


def _daily_turnover_bln_rub(candle: Any) -> float | None:
    rub = _candle_turnover_rub(candle)
    return None if rub is None else (rub / 1e9)


def _price_at_timestamp_rub(candle: Any) -> float | None:
    o = _q_to_float(getattr(candle, "open", None))
    return o if o is not None else _q_to_float(getattr(candle, "close", None))


def _select_daily_slice(
    candles: list[Any],
    anchor_calendar_date: date,
    pre_window: int,
    post_window: int,
    *,
    resolved_ticker: str | None = None,
) -> tuple[list[Any], date | None]:
    if not candles:
        return [], None
    trade_days = _unique_trade_days(candles)
    anchor_trade_date = _find_next_or_same_trade_day(trade_days, anchor_calendar_date)
    if anchor_trade_date is None:
        return [], None
    idx_map = {d: i for i, d in enumerate(trade_days)}
    anchor_idx = idx_map[anchor_trade_date]
    if anchor_idx < pre_window:
        logger.warning(
            "INSUFFICIENT_HISTORY | ticker=%s | anchor=%s | anchor_trade_idx=%s | trade_days_in_fetch=%s | required_pre=%s",
            resolved_ticker,
            anchor_calendar_date,
            anchor_idx,
            len(trade_days),
            pre_window,
        )
    left = max(0, anchor_idx - pre_window)
    right = min(len(trade_days) - 1, anchor_idx + post_window)
    selected_days = set(trade_days[left : right + 1])
    selected = [c for c in candles if _candle_dt_msk(c).date() in selected_days]
    selected.sort(key=_candle_dt_msk)
    return selected, anchor_trade_date


def _build_global_series(cfg: Config) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    imoex = None
    ruonia = None
    if cfg.benchmarks_imoex and cfg.benchmarks_imoex.exists():
        imoex = _load_time_series_table(cfg.benchmarks_imoex, ["imoex daily close", "imoex", "close", "value", "индекс мосбиржи"])
    elif cfg.financials_dir:
        auto = _find_matching_file_by_name(cfg.financials_dir, ["IMOEX", "MOEX", "INDEX"])
        if auto:
            imoex = _load_time_series_table(auto, ["imoex daily close", "imoex", "close", "value", "индекс мосбиржи"])
    if cfg.benchmarks_ruonia and cfg.benchmarks_ruonia.exists():
        ruonia = _load_time_series_table(cfg.benchmarks_ruonia, ["ruonia", "ставка ruonia", "value", "close"])
    elif cfg.financials_dir:
        auto = _find_matching_file_by_name(cfg.financials_dir, ["RUONIA"])
        if auto:
            ruonia = _load_time_series_table(auto, ["ruonia", "ставка ruonia", "value", "close"])
    return imoex, ruonia


def process_source_rows(client: Client, df: pd.DataFrame, col_map: dict[str, str | None], cfg: Config) -> pd.DataFrame:
    _ensure_columns(df, AUDIT_COLS)
    total = len(df)
    for i in range(total):
        row_excel = i + 2
        before_hits = RATE_LIMIT_HITS_TOTAL
        row_t0 = time.perf_counter()
        logger.info("ROW_START | row_excel=%s/%s", row_excel, total + 1)
        try:
            row = df.iloc[i]
            buyer_company = row.get(col_map["buyer_company"]) if col_map["buyer_company"] else None
            buyer_ticker = row.get(col_map["buyer_ticker"]) if col_map["buyer_ticker"] else None
            deal_object = row.get(col_map["deal_object"]) if col_map["deal_object"] else None
            release_date_raw = row.get(col_map["first_press_release_date"]) if col_map["first_press_release_date"] else None
            release_time_raw = row.get(col_map["first_press_release_time"]) if col_map["first_press_release_time"] else None
            cbonds_act_raw = row.get(col_map["cbonds_actualization_date"]) if col_map["cbonds_actualization_date"] else None
            cbonds_create_raw = row.get(col_map["cbonds_create_date"]) if col_map["cbonds_create_date"] else None

            resolved = _resolve_instrument(client, buyer_ticker, buyer_company)

            df.at[i, "audit_run_ts"] = datetime.now(MSK).isoformat()
            df.at[i, "audit_python_version"] = sys.version.split()[0]
            df.at[i, "audit_first_press_release_date_parsed"] = parse_date_any(release_date_raw)
            rt = parse_time_any(release_time_raw)
            df.at[i, "audit_first_press_release_time_parsed"] = rt.strftime("%H:%M") if rt else None
            df.at[i, "audit_cbonds_actualization_date_parsed"] = parse_date_any(cbonds_act_raw)
            df.at[i, "audit_cbonds_create_date_parsed"] = parse_date_any(cbonds_create_raw)
            df.at[i, "audit_resolved_flag"] = 1 if resolved.ok else 0
            df.at[i, "audit_resolved_ticker"] = resolved.ticker
            df.at[i, "audit_resolved_name"] = resolved.name
            df.at[i, "audit_resolved_class_code"] = resolved.class_code
            df.at[i, "audit_resolved_uid"] = resolved.uid
            df.at[i, "audit_resolved_figi"] = resolved.figi

            if not resolved.ok:
                if cfg.ignore_unresolved_rows:
                    df.at[i, "audit_skip_reason"] = "ignored_unresolved_row"
                    df.at[i, "audit_row_status"] = "skipped"
                else:
                    df.at[i, "audit_skip_reason"] = resolved.reason
                    df.at[i, "audit_row_status"] = "unresolved"
                df.at[i, "audit_is_off_market_release"] = None
                df.at[i, "audit_release_anchor_trade_date"] = None
                df.at[i, "audit_release_anchor_timestamp_msk"] = None
                df.at[i, "audit_release_anchor_reason"] = None
                df.at[i, "audit_notes"] = f"resolve_method={resolved.resolve_method}; reason={resolved.reason}"
                df.at[i, "audit_rate_limit_hits"] = RATE_LIMIT_HITS_TOTAL - before_hits
                continue

            anchor = resolve_release_anchor(client, resolved.instrument_id or "", release_date_raw, release_time_raw)
            df.at[i, "audit_is_off_market_release"] = anchor.is_off_market_release
            df.at[i, "audit_release_anchor_trade_date"] = anchor.anchor_trade_date
            df.at[i, "audit_release_anchor_timestamp_msk"] = anchor.anchor_timestamp_msk.isoformat() if anchor.anchor_timestamp_msk else None
            df.at[i, "audit_release_anchor_reason"] = anchor.reason
            df.at[i, "audit_row_status"] = "ready" if anchor.ok else "ready_without_release_anchor"
            df.at[i, "audit_skip_reason"] = None
            base_notes = f"resolved={resolved.resolve_method}; release_anchor={anchor.reason}"
            release_date_parsed = parse_date_any(release_date_raw)
            if (
                anchor.ok
                and anchor.anchor_trade_date
                and release_date_parsed
                and abs((anchor.anchor_trade_date - release_date_parsed).days) > 30
            ):
                logger.warning(
                    "ANCHOR_DATE_MISMATCH | row=%s | release_date=%s | anchor_trade_date=%s | delta_days=%s",
                    row_excel,
                    release_date_parsed,
                    anchor.anchor_trade_date,
                    (anchor.anchor_trade_date - release_date_parsed).days,
                )
                base_notes += (
                    f"; WARNING: anchor_trade_date is {abs((anchor.anchor_trade_date - release_date_parsed).days)}d from release_date"
                )
            df.at[i, "audit_notes"] = base_notes
            df.at[i, "audit_rate_limit_hits"] = RATE_LIMIT_HITS_TOTAL - before_hits
        except Exception as e:
            df.at[i, "audit_row_status"] = "row_error"
            df.at[i, "audit_exception"] = f"{type(e).__name__}: {e}"
            df.at[i, "audit_rate_limit_hits"] = RATE_LIMIT_HITS_TOTAL - before_hits
            logger.exception("ROW_ERROR | row=%s | error=%s", row_excel, e)
        finally:
            elapsed = time.perf_counter() - row_t0
            status = df.at[i, "audit_row_status"]
            logger.info(
                "ROW_DONE | row_excel=%s/%s | status=%s | resolved_ticker=%s | elapsed_s=%.2f | rate_limit_delta=%s",
                row_excel,
                total + 1,
                status,
                df.at[i, "audit_resolved_ticker"],
                elapsed,
                df.at[i, "audit_rate_limit_hits"],
            )
        if cfg.autosave_every > 0 and (i + 1) % cfg.autosave_every == 0:
            logger.info("SOURCE_CHECKPOINT | rows_done=%s", i + 1)
    return df


def build_table_1_intraday(client: Client, source_df: pd.DataFrame, col_map: dict[str, str | None]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for i in source_df.index:
        row = source_df.loc[i]
        buyer_company = row.get(col_map["buyer_company"]) if col_map.get("buyer_company") else None
        deal_object = row.get(col_map["deal_object"]) if col_map.get("deal_object") else None
        release_date_raw = row.get(col_map["first_press_release_date"]) if col_map.get("first_press_release_date") else None
        release_time_raw = row.get(col_map["first_press_release_time"]) if col_map.get("first_press_release_time") else None
        buyer_ticker = _display_buyer_ticker(row, col_map, source_df, i)
        src_excel = i + 2

        if int(source_df.at[i, "audit_resolved_flag"] or 0) != 1:
            rows.append(
                _table1_stub_row(
                    source_row_excel=src_excel,
                    buyer_ticker=buyer_ticker,
                    buyer_company=buyer_company,
                    deal_object=deal_object,
                    release_date_raw=release_date_raw,
                    release_time_raw=release_time_raw,
                    anchor=None,
                    note="instrument_unresolved",
                )
            )
            continue

        instrument_id = source_df.at[i, "audit_resolved_figi"] or source_df.at[i, "audit_resolved_uid"]
        resolved_ticker = source_df.at[i, "audit_resolved_ticker"]
        if not instrument_id:
            rows.append(
                _table1_stub_row(
                    source_row_excel=src_excel,
                    buyer_ticker=buyer_ticker,
                    buyer_company=buyer_company,
                    deal_object=deal_object,
                    release_date_raw=release_date_raw,
                    release_time_raw=release_time_raw,
                    anchor=None,
                    note="no_instrument_id",
                )
            )
            continue

        anchor = resolve_release_anchor(client, instrument_id, release_date_raw, release_time_raw)
        if not anchor.ok or anchor.anchor_trade_date is None:
            rows.append(
                _table1_stub_row(
                    source_row_excel=src_excel,
                    buyer_ticker=buyer_ticker,
                    buyer_company=buyer_company,
                    deal_object=deal_object,
                    release_date_raw=release_date_raw,
                    release_time_raw=release_time_raw,
                    anchor=anchor,
                    note="release_anchor_unavailable",
                )
            )
            continue

        daily_near = _get_daily_window(
            client,
            instrument_id,
            anchor.anchor_trade_date - timedelta(days=MIN_15M_FETCH_PADDING_DAYS),
            anchor.anchor_trade_date + timedelta(days=MIN_15M_FETCH_PADDING_DAYS),
        )
        trade_days = _unique_trade_days(daily_near)
        prev_td, next_td = _find_neighbor_trade_days(trade_days, anchor.anchor_trade_date)
        offsets: list[tuple[int, date]] = []
        if prev_td is not None:
            offsets.append((-1, prev_td))
        offsets.append((0, anchor.anchor_trade_date))
        if next_td is not None:
            offsets.append((1, next_td))
        appended = False
        for off, trade_day in offsets:
            candles = _get_intraday_15m_for_day(client, instrument_id, trade_day)
            for c in candles:
                ts = _candle_dt_msk(c)
                turnover = _candle_turnover_rub(c)
                rows.append(
                    {
                        "source_row_excel": src_excel,
                        "buyer_ticker": resolved_ticker,
                        "buyer_company": buyer_company,
                        "deal_object": deal_object,
                        "event_name": "first_press_release",
                        "release_date": anchor.release_date.isoformat() if anchor.release_date else None,
                        "release_time": anchor.release_time.strftime("%H:%M") if anchor.release_time else None,
                        "is_off_market_release": anchor.is_off_market_release,
                        "anchor_trade_date": anchor.anchor_trade_date.isoformat() if anchor.anchor_trade_date else None,
                        "anchor_timestamp_msk": anchor.anchor_timestamp_msk.isoformat() if anchor.anchor_timestamp_msk else None,
                        "trade_day_offset": off,
                        "timestamp_msk": ts.isoformat(),
                        "price_at_timestamp_rub": _price_at_timestamp_rub(c),
                        "volume_during_timestamp_plus_15m_mn_rub": None if turnover is None else (turnover / 1e6),
                        "pipeline_row_note": None,
                    }
                )
                appended = True
        if not appended:
            rows.append(
                _table1_stub_row(
                    source_row_excel=src_excel,
                    buyer_ticker=resolved_ticker,
                    buyer_company=buyer_company,
                    deal_object=deal_object,
                    release_date_raw=release_date_raw,
                    release_time_raw=release_time_raw,
                    anchor=anchor,
                    note="no_intraday_candles",
                )
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=TABLE1_COLS)
    for c in TABLE1_COLS:
        if c not in out.columns:
            out[c] = pd.NA
    return out[TABLE1_COLS].sort_values(["source_row_excel", "timestamp_msk"], na_position="last").reset_index(drop=True)


def build_table_2_generic(
    client: Client,
    source_df: pd.DataFrame,
    col_map: dict[str, str | None],
    cfg: Config,
    anchor_col_key: str,
    event_name: str,
    metrics_filler: DailyMetricsFiller,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for i in source_df.index:
        row = source_df.loc[i]
        buyer_company = row.get(col_map["buyer_company"]) if col_map.get("buyer_company") else None
        deal_object = row.get(col_map["deal_object"]) if col_map.get("deal_object") else None
        buyer_ticker = _display_buyer_ticker(row, col_map, source_df, i)
        anchor_raw = row.get(col_map[anchor_col_key]) if col_map.get(anchor_col_key) else None
        anchor_date_raw_str = None if anchor_raw is None else str(anchor_raw).strip()
        anchor_date_raw_parsed = parse_date_any(anchor_raw)
        if event_name == "first_press_release" and int(source_df.at[i, "audit_resolved_flag"] or 0) == 1:
            audit_anchor = source_df.at[i, "audit_release_anchor_trade_date"]
            if pd.notna(audit_anchor):
                ad_from_audit = parse_date_any(audit_anchor)
                anchor_date = ad_from_audit if ad_from_audit is not None else anchor_date_raw_parsed
            else:
                anchor_date = anchor_date_raw_parsed
        else:
            anchor_date = anchor_date_raw_parsed

        if anchor_date and anchor_date_raw_parsed and abs((anchor_date - anchor_date_raw_parsed).days) > 5:
            logger.warning(
                "ANCHOR_DATE_OVERRIDE | row=%s | raw_parsed=%s | effective_anchor=%s",
                i + 2,
                anchor_date_raw_parsed,
                anchor_date,
            )

        src_excel = i + 2

        def _iom_resolved() -> int:
            if event_name != "first_press_release":
                return 0
            return int(source_df.at[i, "audit_is_off_market_release"] or 0)

        if int(source_df.at[i, "audit_resolved_flag"] or 0) != 1:
            rows.append(
                _table2_stub_row(
                    source_row_excel=src_excel,
                    buyer_ticker=buyer_ticker,
                    buyer_company=buyer_company,
                    deal_object=deal_object,
                    event_name=event_name,
                    anchor_date=anchor_date,
                    anchor_date_raw=anchor_date_raw_str,
                    anchor_trade_date=None,
                    is_off_market_release=0,
                    t=0,
                    date_val=anchor_date,
                    note="instrument_unresolved",
                )
            )
            continue

        instrument_id = source_df.at[i, "audit_resolved_figi"] or source_df.at[i, "audit_resolved_uid"]
        resolved_ticker = source_df.at[i, "audit_resolved_ticker"]
        if not instrument_id or not resolved_ticker:
            rows.append(
                _table2_stub_row(
                    source_row_excel=src_excel,
                    buyer_ticker=buyer_ticker,
                    buyer_company=buyer_company,
                    deal_object=deal_object,
                    event_name=event_name,
                    anchor_date=anchor_date,
                    anchor_date_raw=anchor_date_raw_str,
                    anchor_trade_date=None,
                    is_off_market_release=_iom_resolved() if event_name == "first_press_release" else 0,
                    t=0,
                    date_val=anchor_date,
                    note="no_instrument_or_ticker",
                )
            )
            continue

        if anchor_date is None:
            rows.append(
                _table2_stub_row(
                    source_row_excel=src_excel,
                    buyer_ticker=resolved_ticker,
                    buyer_company=buyer_company,
                    deal_object=deal_object,
                    event_name=event_name,
                    anchor_date=None,
                    anchor_date_raw=anchor_date_raw_str,
                    anchor_trade_date=None,
                    is_off_market_release=_iom_resolved() if event_name == "first_press_release" else 0,
                    t=0,
                    date_val=None,
                    note="no_anchor_date",
                )
            )
            continue

        is_off_market_release = _iom_resolved() if event_name == "first_press_release" else 0

        daily = _get_daily_window(
            client,
            instrument_id,
            anchor_date - timedelta(days=DAILY_FETCH_CALENDAR_PADDING),
            anchor_date + timedelta(days=DAILY_FETCH_CALENDAR_PADDING),
        )
        selected, anchor_trade_date = _select_daily_slice(
            daily,
            anchor_date,
            cfg.daily_window_pre,
            cfg.daily_window_post,
            resolved_ticker=str(resolved_ticker).strip(),
        )
        if not selected or anchor_trade_date is None:
            rows.append(
                _table2_stub_row(
                    source_row_excel=src_excel,
                    buyer_ticker=str(resolved_ticker).strip(),
                    buyer_company=buyer_company,
                    deal_object=deal_object,
                    event_name=event_name,
                    anchor_date=anchor_date,
                    anchor_date_raw=anchor_date_raw_str,
                    anchor_trade_date=None,
                    is_off_market_release=is_off_market_release,
                    t=0,
                    date_val=anchor_date,
                    note="no_daily_candles",
                )
            )
            continue

        trade_days = _unique_trade_days(selected)
        idx_map = {d: idx for idx, d in enumerate(trade_days)}
        anchor_idx = idx_map.get(anchor_trade_date)
        if anchor_idx is None:
            rows.append(
                _table2_stub_row(
                    source_row_excel=src_excel,
                    buyer_ticker=str(resolved_ticker).strip(),
                    buyer_company=buyer_company,
                    deal_object=deal_object,
                    event_name=event_name,
                    anchor_date=anchor_date,
                    anchor_date_raw=anchor_date_raw_str,
                    anchor_trade_date=anchor_trade_date,
                    is_off_market_release=is_off_market_release,
                    t=0,
                    date_val=anchor_date,
                    note="anchor_not_in_calendar",
                )
            )
            continue

        rt = str(resolved_ticker).strip()
        for c in selected:
            d = _candle_dt_msk(c).date()
            t_rel = idx_map[d] - anchor_idx
            close_px = _q_to_float(getattr(c, "close", None))
            filled = metrics_filler.fill_for_date(
                ticker=rt,
                d=d,
                close_px=close_px,
                candle=c,
            )
            rows.append(
                {
                    "source_row_excel": src_excel,
                    "buyer_ticker": rt,
                    "buyer_company": buyer_company,
                    "deal_object": deal_object,
                    "event_name": event_name,
                    "anchor_date": anchor_date.isoformat(),
                    "anchor_date_raw": anchor_date_raw_str,
                    "anchor_trade_date": anchor_trade_date.isoformat(),
                    "is_off_market_release": is_off_market_release,
                    "t": t_rel,
                    "Date": d.isoformat(),
                    "Adjusted Close, руб.": filled.adjusted_close,
                    "Close, руб.": filled.close,
                    "Volume, млрд. руб.": filled.volume_bln_rub,
                    "Market Capitalization, млрд. руб.": filled.market_cap_bln_rub,
                    "IMOEX daily close": filled.imoex_daily_close,
                    "RUONIA (daily)": filled.ruonia_daily,
                    "ROE": filled.roe,
                    "ROA": filled.roa,
                    "P/B": filled.pb,
                    "P/E": filled.pe,
                    "Total Assets": filled.total_assets,
                    "pipeline_row_note": None,
                }
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=TABLE2_COLS)
    for c in TABLE2_COLS:
        if c not in out.columns:
            out[c] = pd.NA
    return out[TABLE2_COLS].sort_values(["source_row_excel", "t", "Date"], na_position="last").reset_index(drop=True)


def _save_xlsx(df: pd.DataFrame, xlsx_path: Path, sheet_name: str) -> None:
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(xlsx_path, sheet_name=sheet_name[:31], index=False)


def run_build(cfg: Config) -> None:
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    global CACHE_DIR
    CACHE_DIR = cfg.cache_dir
    setup_logging(cfg.output_dir / "run.log")
    if not cfg.input_xlsx.exists():
        raise FileNotFoundError(f"Не найден input xlsx: {cfg.input_xlsx}")

    source_df, source_sheet = read_input_excel(cfg.input_xlsx, cfg.sheet_name)
    col_map = build_column_map(source_df)
    source_df = _ensure_columns(source_df, AUDIT_COLS)
    # Диапазон для IMOEX historical preload (минимизируем количество API вызовов).
    anchor_date_cols = [k for k in ["first_press_release_date", "cbonds_actualization_date", "cbonds_create_date"] if col_map.get(k)]
    anchor_dates: list[date] = []
    for k in anchor_date_cols:
        col = col_map[k]
        if not col:
            continue
        for x in source_df[col].tolist():
            d = parse_date_any(x)
            if d is not None:
                anchor_dates.append(d)
    if anchor_dates:
        min_anchor = min(anchor_dates)
        max_anchor = max(anchor_dates)
    else:
        # честный дефолт: без дат бенчмарк всё равно не заполнится
        min_anchor = date.today()
        max_anchor = date.today()
    bench_from = min_anchor - timedelta(days=DAILY_FETCH_CALENDAR_PADDING)
    bench_to = max_anchor + timedelta(days=DAILY_FETCH_CALENDAR_PADDING)

    try:
        with Client(cfg.token) as client:
            _build_shares_cache(client)
            source_df = process_source_rows(client, source_df, col_map, cfg)

            financials_provider = FinancialsProvider(cfg.financials_dir)
            benchmarks_provider = BenchmarksProvider(
                cfg.financials_dir,
                cache_dir=cfg.cache_dir,
            )
            benchmarks_provider.preload_imoex(client=client, from_d=bench_from, to_d=bench_to)

            metrics_filler = DailyMetricsFiller(
                financials=financials_provider,
                benchmarks=benchmarks_provider,
                adjusted_close_allow_close_fallback=cfg.adjusted_close_allow_close_fallback,
                market_cap_shares_variant=cfg.market_cap_shares_variant,
            )

            _save_xlsx(
                source_df,
                cfg.output_dir / "ma_deals_AUDIT.xlsx",
                "audit",
            )

            table1 = build_table_1_intraday(client, source_df, col_map)
            _save_xlsx(
                table1,
                cfg.output_dir / "table_1_intraday.xlsx",
                "table_1_intraday",
            )

            table21 = build_table_2_generic(
                client=client,
                source_df=source_df,
                col_map=col_map,
                cfg=cfg,
                anchor_col_key="first_press_release_date",
                event_name="first_press_release",
                metrics_filler=metrics_filler,
            )
            _save_xlsx(
                table21,
                cfg.output_dir / "table_2_1_first_press_release.xlsx",
                "table_2_1",
            )

            table22 = build_table_2_generic(
                client=client,
                source_df=source_df,
                col_map=col_map,
                cfg=cfg,
                anchor_col_key="cbonds_actualization_date",
                event_name="cbonds_actualization",
                metrics_filler=metrics_filler,
            )
            _save_xlsx(
                table22,
                cfg.output_dir / "table_2_2_cbonds_actualization.xlsx",
                "table_2_2",
            )

            table23 = build_table_2_generic(
                client=client,
                source_df=source_df,
                col_map=col_map,
                cfg=cfg,
                anchor_col_key="cbonds_create_date",
                event_name="cbonds_create",
                metrics_filler=metrics_filler,
            )
            _save_xlsx(
                table23,
                cfg.output_dir / "table_2_3_cbonds_create.xlsx",
                "table_2_3",
            )

            metrics_filler.log_summary()
    except UnauthenticatedError as e:
        msg = (
            "INVEST API вернул UNAUTHENTICATED (40003): отсутствует или неверен токен.\n"
            "Проверь, что:\n"
            f"- переменная окружения {os.environ.get('INVEST_TOKEN') and 'INVEST_TOKEN'} задана корректно;\n"
            "- токен имеет доступ к инвест‑API и не отозван.\n"
        )
        logger.error(msg)
        raise

    summary = pd.DataFrame(
        [
            {
                "rows_total": len(source_df),
                "rows_resolved": int(pd.to_numeric(source_df["audit_resolved_flag"], errors="coerce").fillna(0).sum()),
                "off_market_releases": int(pd.to_numeric(source_df["audit_is_off_market_release"], errors="coerce").fillna(0).sum()),
                "table_1_rows": len(table1),
                "table_2_1_rows": len(table21),
                "table_2_2_rows": len(table22),
                "table_2_3_rows": len(table23),
                "rate_limit_hits_total": RATE_LIMIT_HITS_TOTAL,
                "generated_at_msk": datetime.now(MSK).isoformat(),
                "source_sheet": source_sheet,
            }
        ]
    )
    summary.to_excel(cfg.output_dir / "run_summary.xlsx", index=False)

    from .final_outputs import copy_final_outputs

    copy_final_outputs(cfg.output_dir)

