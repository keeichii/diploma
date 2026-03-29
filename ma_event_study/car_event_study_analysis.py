from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from scipy import stats

try:
    from .event_study_config import get_estimation_windows_list
except ImportError:
    from event_study_config import get_estimation_windows_list


INPUT_CANDIDATES = [
    "table_2_1_first_press_release-3.xlsx",
    "table_2_2_cbonds_actualization.xlsx",
    "table_2_3_cbonds_create.xlsx",
]

# Альтернативные имена только xlsx (без CSV)
INPUT_ALIASES = {
    "table_2_1_first_press_release-3.xlsx": ["table_2_1_first_press_release.xlsx"],
}

WINDOWS = [(-1, 1), (-3, 3), (-5, 5), (0, 1), (0, 3), (0, 5), (-10, 10), (-20, 20)]
# H8: pre-event run-up (асимметричные окна)
RUNUP_WINDOWS = [(-30, -5), (-20, -5), (-10, -5), (-5, -1)]
CAR_WINDOWS = list(WINDOWS) + list(RUNUP_WINDOWS)
ESTIMATION_WINDOWS = get_estimation_windows_list()


@dataclass
class FileAudit:
    file_label: str
    source_path: str
    n_rows: int
    n_events: int
    price_field: str
    adjusted_vs_close_note: str
    benchmark_col: str | None
    benchmark_non_null_share: float
    duplicate_event_keys: int
    duplicate_event_t_rows: int
    missing_t_rows: int
    missing_price_rows: int
    non_sequential_events: int
    incomplete_t0_events: int
    t0_anchor_mismatch_events: int
    weekend_rows: int
    weekend_share: float
    suspicious_calendar_days: bool
    anomalous_return_rows: int
    events_without_positive_t: int
    max_available_t: int | float
    ruonia_note: str
    pipeline_stub_rows_excluded: int


def _norm_col(s: str) -> str:
    return (
        s.strip()
        .replace('"', "")
        .replace("'", "")
        .replace("руб.", "RUB")
        .replace("млрд.", "bln")
    )


def _drop_pipeline_stub_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Исключает строки с непустым pipeline_row_note (заглушки пайплайна); в расчётах остаются полные строки."""
    col = next(
        (c for c in df.columns if str(c).strip().lower().replace(" ", "_") == "pipeline_row_note"),
        None,
    )
    if col is None:
        return df, 0
    s = df[col]
    bad = s.notna() & s.astype(str).str.strip().ne("") & ~s.astype(str).str.strip().str.lower().isin(["nan", "<na>"])
    n = int(bad.sum())
    out = df.loc[~bad].drop(columns=[col]).reset_index(drop=True)
    return out, n


def load_file(input_dir: Path, file_name: str) -> tuple[pd.DataFrame, str]:
    candidates = [input_dir / file_name]
    for alt in INPUT_ALIASES.get(file_name, []):
        candidates.append(input_dir / alt)
    for p in candidates:
        if p.exists():
            return pd.read_excel(p), str(p)
    raise FileNotFoundError(f"File not found (xlsx): {file_name} | tried: {candidates}")


def choose_price_field(df: pd.DataFrame) -> tuple[str, str]:
    adj_candidates = [c for c in df.columns if "Adjusted Close" in c]
    close_candidates = [c for c in df.columns if c.startswith("Close")]
    adj_col = adj_candidates[0] if adj_candidates else None
    close_col = close_candidates[0] if close_candidates else None

    if adj_col is None and close_col is None:
        raise ValueError("Neither Adjusted Close nor Close columns found.")

    if adj_col is not None:
        adj = pd.to_numeric(df[adj_col], errors="coerce")
        if adj.notna().sum() == 0:
            if close_col is None:
                return adj_col, "Adjusted Close exists but empty; Close missing."
            return close_col, "Adjusted Close fully empty; using Close."
        if close_col is not None:
            close = pd.to_numeric(df[close_col], errors="coerce")
            mask = adj.notna() & close.notna()
            if mask.any():
                same = np.isclose(adj[mask], close[mask], rtol=0, atol=1e-12).all()
                if same:
                    return close_col, "Adjusted Close equals Close for all overlapping rows; using Close."
        return adj_col, "Using Adjusted Close."
    return close_col, "Adjusted Close absent; using Close."


def choose_close_col(df: pd.DataFrame) -> str:
    close_candidates = [c for c in df.columns if c.startswith("Close")]
    if not close_candidates:
        raise ValueError("Close column is required for RUONIA-adjusted close calculation.")
    return close_candidates[0]


def _normalize_to_date(s: pd.Series) -> pd.Series:
    ts = pd.to_datetime(s, errors="coerce", utc=True)
    return ts.dt.tz_convert(None).dt.normalize()


def get_adjusted_close_tinvest_ruonia(
    prices_df: pd.DataFrame,
    *,
    close_col: str,
    date_col: str = "Date",
    ruonia_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, str]:
    if prices_df is None or prices_df.empty:
        raise ValueError("prices_df is empty.")
    if close_col not in prices_df.columns:
        raise ValueError(f"Missing required close column: {close_col}")
    if date_col not in prices_df.columns:
        raise ValueError(f"Missing required date column: {date_col}")

    out = prices_df.copy()
    out["date"] = _normalize_to_date(out[date_col])
    out["close"] = pd.to_numeric(out[close_col], errors="coerce")

    if out["date"].isna().all():
        raise ValueError("All price dates are NaT after normalization.")

    ru_note = ""
    ru = ruonia_df.copy() if ruonia_df is not None else out.copy()
    ru.columns = [str(c) for c in ru.columns]
    ru_date_col = "date" if "date" in ru.columns else ("DT" if "DT" in ru.columns else date_col)
    ru["date"] = _normalize_to_date(ru[ru_date_col])

    if "ruonia_index" in ru.columns:
        ru["ruonia_index"] = pd.to_numeric(ru["ruonia_index"], errors="coerce")
        ru_note = "Using provided ruonia_index."
    else:
        rate_cols = [
            c
            for c in ru.columns
            if c.lower() in {"ruonia", "ruo", "ruonia (daily)", "ruonia_daily", "ruonia_rate"}
            or "ruonia" in c.lower()
        ]
        if not rate_cols:
            raise ValueError("Missing ruonia_index and no RUONIA rate column available.")
        daily_pref = [c for c in rate_cols if "daily" in c.lower()]
        rate_col = (daily_pref[0] if daily_pref else rate_cols[0])
        tmp = ru[["date", rate_col]].copy()
        tmp[rate_col] = pd.to_numeric(tmp[rate_col], errors="coerce")
        tmp = tmp.dropna(subset=["date"]).sort_values("date")
        daily_rate = tmp[rate_col].copy()
        daily_rate = np.where(daily_rate > 1.0, daily_rate / 100.0, daily_rate)
        tmp["ruonia_index"] = pd.Series(1.0 + daily_rate / 365.0, index=tmp.index).cumprod()
        ru = tmp
        ru_note = f"ruonia_index derived from daily rate column '{rate_col}'."

    ru = ru[["date", "ruonia_index"]].drop_duplicates("date").sort_values("date")
    merged = out.merge(ru, on="date", how="left")
    sort_cols = ["event_id", "date"] if "event_id" in merged.columns else ["date"]
    merged = merged.sort_values(sort_cols)
    merged["ruonia_index"] = pd.to_numeric(merged["ruonia_index"], errors="coerce").ffill()

    if merged["ruonia_index"].isna().all():
        raise ValueError("ruonia_index is fully empty after merge/ffill.")

    def _deflate_per_event(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        base_cand = g["ruonia_index"].replace(0, np.nan).dropna()
        if base_cand.empty:
            bi = np.nan
        else:
            bi = float(base_cand.iloc[0])
        safe_idx = g["ruonia_index"].replace(0, np.nan)
        g["adjusted_close_deflated"] = g["close"] * bi / safe_idx
        g["adjusted_close_carry"] = g["close"] * safe_idx / bi
        return g

    if "event_id" in merged.columns:
        merged = merged.groupby("event_id", group_keys=False).apply(_deflate_per_event)
    else:
        base_candidates = merged["ruonia_index"].replace(0, np.nan).dropna()
        if base_candidates.empty:
            raise ValueError("No non-zero ruonia_index values for base_index.")
        base_index = float(base_candidates.iloc[0])
        safe_idx = merged["ruonia_index"].replace(0, np.nan)
        merged["adjusted_close_deflated"] = merged["close"] * base_index / safe_idx
        merged["adjusted_close_carry"] = merged["close"] * safe_idx / base_index

    if merged["adjusted_close_deflated"].isna().all() or merged["adjusted_close_carry"].isna().all():
        raise ValueError("Adjusted close columns are empty (check close/ruonia_index and date alignment).")

    return merged, ru_note


def choose_benchmark_col(df: pd.DataFrame) -> str | None:
    preferred = []
    secondary = []
    for c in df.columns:
        cl = c.lower()
        if "off_market" in cl:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        uniq = s.dropna().nunique()
        if uniq <= 10:
            continue
        if "imoex" in cl:
            preferred.append(c)
        elif "index" in cl or "benchmark" in cl:
            secondary.append(c)
    candidates = preferred + secondary
    if not candidates:
        return None
    ranked = sorted(candidates, key=lambda c: pd.to_numeric(df[c], errors="coerce").notna().mean(), reverse=True)
    return ranked[0]


def build_event_id(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    base_key = ["buyer_ticker", "deal_object", "event_name", "anchor_date"]
    key_cols = [c for c in base_key if c in df.columns]
    for extra in ["anchor_trade_date", "source_row_excel", "buyer_company", "is_off_market_release"]:
        if extra in df.columns:
            key_cols.append(extra)
            if not df.duplicated(key_cols + ["t"]).any():
                break
    df["event_id"] = df[key_cols].astype(str).agg(" | ".join, axis=1)
    return df, key_cols


def is_sequential_t(ts: pd.Series) -> bool:
    vals = ts.dropna().sort_values().astype(int).to_numpy()
    if len(vals) <= 1:
        return True
    return np.all(np.diff(vals) == 1)


def estimate_market_model(group: pd.DataFrame, stock_col: str, mkt_col: str) -> tuple[pd.Series, str | None]:
    """OLS market model на окне оценки; при n<30 или сбое — fallback market-adjusted (как B_mkt_adj)."""
    result = pd.Series(np.nan, index=group.index, dtype=float)
    for left, right in ESTIMATION_WINDOWS:
        est = group[(group["t"] >= left) & (group["t"] <= right)]
        est = est[[stock_col, mkt_col]].dropna()
        if len(est) < 30:
            continue
        x = est[mkt_col].to_numpy(dtype=float)
        y = est[stock_col].to_numpy(dtype=float)
        finite_mask = np.isfinite(x) & np.isfinite(y)
        x = x[finite_mask]
        y = y[finite_mask]
        if len(x) < 30 or np.isclose(np.nanstd(x), 0.0):
            continue
        try:
            beta, alpha = np.polyfit(x, y, deg=1)
        except np.linalg.LinAlgError:
            continue
        pred = alpha + beta * group[mkt_col].to_numpy(dtype=float)
        result.loc[group.index] = group[stock_col].to_numpy(dtype=float) - pred
        return result, f"[{left};{right}]"
    # Fallback: не применяем C с пустым AR — переключаемся на market-adjusted
    m = group[mkt_col].replace(0, np.nan).fillna(0.0)
    result.loc[group.index] = group[stock_col].to_numpy(dtype=float) - m.to_numpy(dtype=float)
    return result, "fallback_B_mkt_adj"


def summarize_cars(cars: pd.Series) -> dict[str, float]:
    vals = cars.dropna().astype(float)
    n = len(vals)
    if n == 0:
        return {
            "n_events_in_window": 0,
            "mean_car": np.nan,
            "median_car": np.nan,
            "std_car": np.nan,
            "t_stat": np.nan,
            "t_pvalue": np.nan,
            "wilcoxon_stat": np.nan,
            "wilcoxon_pvalue": np.nan,
            "sign_test_pvalue": np.nan,
            "share_positive": np.nan,
            "min": np.nan,
            "p25": np.nan,
            "p50": np.nan,
            "p75": np.nan,
            "max": np.nan,
        }
    t_stat, t_pvalue = stats.ttest_1samp(vals, popmean=0.0, nan_policy="omit")
    if np.allclose(vals, 0):
        w_stat, w_p = np.nan, np.nan
    else:
        try:
            w_stat, w_p = stats.wilcoxon(vals)
        except ValueError:
            w_stat, w_p = np.nan, np.nan
    non_zero = vals[vals != 0]
    if len(non_zero) > 0:
        pos = int((non_zero > 0).sum())
        sign_p = stats.binomtest(pos, len(non_zero), p=0.5, alternative="two-sided").pvalue
    else:
        sign_p = np.nan
    return {
        "n_events_in_window": n,
        "mean_car": vals.mean(),
        "median_car": vals.median(),
        "std_car": vals.std(ddof=1) if n > 1 else np.nan,
        "t_stat": t_stat,
        "t_pvalue": t_pvalue,
        "wilcoxon_stat": w_stat,
        "wilcoxon_pvalue": w_p,
        "sign_test_pvalue": sign_p,
        "share_positive": (vals > 0).mean(),
        "min": vals.min(),
        "p25": vals.quantile(0.25),
        "p50": vals.quantile(0.5),
        "p75": vals.quantile(0.75),
        "max": vals.max(),
    }


def car_by_window(df: pd.DataFrame, ar_col: str, windows: Iterable[tuple[int, int]]) -> pd.DataFrame:
    rows = []
    grouped = df.groupby("event_id", dropna=False)
    for event_id, g in grouped:
        for left, right in windows:
            window_len = right - left + 1
            x = g[(g["t"] >= left) & (g["t"] <= right)]
            if len(x) == window_len and x[ar_col].notna().sum() == window_len:
                car = x[ar_col].sum()
                in_window = 1
            else:
                car = np.nan
                in_window = 0
            rows.append(
                {
                    "event_id": event_id,
                    "window": f"[{left};{right}]",
                    "car": car,
                    "event_qualifies": in_window,
                }
            )
    return pd.DataFrame(rows)


def run() -> None:
    root = Path(__file__).resolve().parents[1]
    input_dir = root / "out"
    out_dir = root / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    unified_parts = []
    audits: list[FileAudit] = []
    all_event_rows = []
    summary_rows = []
    audit_detail_blocks: list[str] = []

    for file_name in INPUT_CANDIDATES:
        raw, source_path = load_file(input_dir, file_name)
        raw.columns = [_norm_col(c) for c in raw.columns]
        raw, n_stub_excl = _drop_pipeline_stub_rows(raw)
        if len(raw) == 0:
            print(f"SKIP {file_name}: все строки были заглушками pipeline_row_note ({n_stub_excl} исключено).")
            continue
        raw["anchor_date"] = pd.to_datetime(raw.get("anchor_date"), errors="coerce")
        raw["anchor_trade_date"] = pd.to_datetime(raw.get("anchor_trade_date"), errors="coerce")
        raw["Date"] = pd.to_datetime(raw.get("Date"), errors="coerce")
        raw["t"] = pd.to_numeric(raw.get("t"), errors="coerce")
        raw["source_file"] = Path(source_path).name

        raw, event_id_cols = build_event_id(raw)

        price_col, price_note = choose_price_field(raw)
        close_col = choose_close_col(raw)
        try:
            raw, ruonia_note = get_adjusted_close_tinvest_ruonia(
                raw,
                close_col=close_col,
                date_col="Date",
                ruonia_df=None,
            )
        except ValueError as e:
            ruonia_note = f"RUONIA-adjusted close unavailable: {e}"
            raw["date"] = _normalize_to_date(raw["Date"])
            raw["close"] = pd.to_numeric(raw[close_col], errors="coerce")
            raw["ruonia_index"] = np.nan
            raw["adjusted_close_deflated"] = np.nan
            raw["adjusted_close_carry"] = np.nan
        raw["price"] = pd.to_numeric(raw[price_col], errors="coerce")
        bench_col = choose_benchmark_col(raw)
        if bench_col is not None:
            raw["benchmark_price"] = pd.to_numeric(raw[bench_col], errors="coerce")
        else:
            raw["benchmark_price"] = np.nan
        raw = raw.sort_values(["event_id", "t", "Date"], kind="mergesort").reset_index(drop=True)

        event_table = raw[event_id_cols].drop_duplicates()
        dupl_event_keys = int(event_table.duplicated(event_id_cols).sum())
        dupl_event_t = int(raw.duplicated(["event_id", "t"]).sum())
        missing_t = int(raw["t"].isna().sum())
        missing_price = int(raw["price"].isna().sum())
        seq_viol = int(raw.groupby("event_id")["t"].apply(is_sequential_t).eq(False).sum())
        t0_missing_events = int(raw.groupby("event_id").apply(lambda g: (g["t"] == 0).sum() == 0).sum())

        t0 = raw[raw["t"] == 0][["event_id", "Date", "anchor_date"]].drop_duplicates("event_id")
        t0_mismatch = int((t0["Date"].dt.normalize() != t0["anchor_date"].dt.normalize()).sum())

        block_lines: list[str] = []
        if not t0.empty:
            bad_t0 = t0[t0["Date"].dt.normalize() != t0["anchor_date"].dt.normalize()]
            if not bad_t0.empty:
                block_lines.append("EVENTS WITH t=0 Date != anchor_date:")
                for _, rr in bad_t0.iterrows():
                    ad = rr["anchor_date"]
                    ad_str = pd.Timestamp(ad).date() if pd.notna(ad) else "NaT"
                    dt_str = pd.Timestamp(rr["Date"]).date() if pd.notna(rr["Date"]) else "NaT"
                    block_lines.append(
                        f"  event_id={rr['event_id']} | Date(t=0)={dt_str} | anchor_date={ad_str}"
                    )

        max_t_per_event = raw.groupby("event_id")["t"].max()
        short_events = max_t_per_event[max_t_per_event < 20].index.tolist()
        if short_events:
            block_lines.append(
                f"EVENTS WITH max(t) < 20 (n={len(short_events)}): insufficient for [-20;20] window"
            )
            for eid in short_events[:10]:
                block_lines.append(f"  {eid}")

        min_t_per_event = raw.groupby("event_id")["t"].min()
        no_estimation = min_t_per_event[min_t_per_event > -30].index.tolist()
        if no_estimation:
            block_lines.append(
                f"EVENTS WITH min(t) > -30 (n={len(no_estimation)}): market model estimation window IMPOSSIBLE"
            )
            for eid in no_estimation[:10]:
                block_lines.append(f"  {eid}")

        if block_lines:
            audit_detail_blocks.append(f"\n--- Detail: {file_name} ---\n" + "\n".join(block_lines))

        raw["weekday"] = raw["Date"].dt.weekday
        weekend_rows = int(raw["weekday"].isin([5, 6]).sum())
        weekend_share = weekend_rows / len(raw) if len(raw) else 0.0

        md = (
            raw.sort_values(["event_id", "t"])
            .groupby("event_id")["Date"]
            .apply(lambda s: s.diff().dt.days.dropna().median() if s.notna().sum() > 1 else np.nan)
        )
        suspicious_calendar = bool((md == 1).mean() > 0.5)

        raw["stock_return_simple"] = raw.groupby("event_id")["price"].pct_change()
        raw["stock_return_log"] = raw.groupby("event_id")["price"].transform(lambda s: np.log(s / s.shift(1)))
        raw["stock_return_adj_deflated_simple"] = raw.groupby("event_id")["adjusted_close_deflated"].pct_change()
        raw["stock_return_adj_deflated_log"] = raw.groupby("event_id")["adjusted_close_deflated"].transform(
            lambda s: np.log(s / s.shift(1))
        )
        raw["stock_return_adj_carry_simple"] = raw.groupby("event_id")["adjusted_close_carry"].pct_change()
        raw["stock_return_adj_carry_log"] = raw.groupby("event_id")["adjusted_close_carry"].transform(
            lambda s: np.log(s / s.shift(1))
        )
        raw["benchmark_return_simple"] = raw.groupby("event_id")["benchmark_price"].pct_change()
        raw["benchmark_return_log"] = raw.groupby("event_id")["benchmark_price"].transform(
            lambda s: np.log(s / s.shift(1))
        )

        anomalous = int((raw["stock_return_simple"].abs() > 0.5).sum())
        max_t = raw["t"].max() if len(raw) else np.nan
        ev_no_pos_t = int(raw.groupby("event_id")["t"].max().le(0).sum())

        n_events = raw["event_id"].nunique()
        bench_non_null_share = float(raw["benchmark_price"].notna().mean())
        audits.append(
            FileAudit(
                file_label=file_name,
                source_path=source_path,
                n_rows=len(raw),
                n_events=n_events,
                price_field=price_col,
                adjusted_vs_close_note=price_note,
                benchmark_col=bench_col,
                benchmark_non_null_share=bench_non_null_share,
                duplicate_event_keys=dupl_event_keys,
                duplicate_event_t_rows=dupl_event_t,
                missing_t_rows=missing_t,
                missing_price_rows=missing_price,
                non_sequential_events=seq_viol,
                incomplete_t0_events=t0_missing_events,
                t0_anchor_mismatch_events=t0_mismatch,
                weekend_rows=weekend_rows,
                weekend_share=weekend_share,
                suspicious_calendar_days=suspicious_calendar,
                anomalous_return_rows=anomalous,
                events_without_positive_t=ev_no_pos_t,
                max_available_t=max_t,
                ruonia_note=ruonia_note,
                pipeline_stub_rows_excluded=n_stub_excl,
            )
        )

        unified_parts.append(raw.copy())

        model_defs = [
            ("A_raw", "stock_return_simple", None),
            ("A_raw_log", "stock_return_log", None),
        ]
        has_adj = raw["adjusted_close_deflated"].notna().any() and raw["adjusted_close_carry"].notna().any()
        if has_adj:
            model_defs += [
                ("A_raw_adj_deflated", "stock_return_adj_deflated_simple", None),
                ("A_raw_adj_deflated_log", "stock_return_adj_deflated_log", None),
                ("A_raw_adj_carry", "stock_return_adj_carry_simple", None),
                ("A_raw_adj_carry_log", "stock_return_adj_carry_log", None),
            ]
        if bench_non_null_share > 0.2:
            model_defs += [
                ("B_mkt_adj", "stock_return_simple", "benchmark_return_simple"),
                ("B_mkt_adj_log", "stock_return_log", "benchmark_return_log"),
            ]
            if has_adj:
                model_defs += [
                    ("B_mkt_adj_adj_deflated", "stock_return_adj_deflated_simple", "benchmark_return_simple"),
                    ("B_mkt_adj_adj_deflated_log", "stock_return_adj_deflated_log", "benchmark_return_log"),
                    ("B_mkt_adj_adj_carry", "stock_return_adj_carry_simple", "benchmark_return_simple"),
                    ("B_mkt_adj_adj_carry_log", "stock_return_adj_carry_log", "benchmark_return_log"),
                ]

        # Model C (market model) per return type if benchmark exists.
        for model_name, stock_col, mkt_col in model_defs:
            work = raw.copy()
            if mkt_col is None:
                work["ar"] = work[stock_col]
            else:
                work["ar"] = work[stock_col] - work[mkt_col]
            cars = car_by_window(work, "ar", CAR_WINDOWS)
            meta = work[["event_id", "source_file", "event_name"]].drop_duplicates("event_id")
            cars = cars.merge(meta, on="event_id", how="left")
            cars["model"] = model_name
            all_event_rows.append(cars)

            for (source_file, event_name, window), g in cars.groupby(["source_file", "event_name", "window"]):
                metrics = summarize_cars(g["car"])
                summary_rows.append(
                    {
                        "source_file": source_file,
                        "event_name": event_name,
                        "model": model_name,
                        "window": window,
                        **metrics,
                    }
                )

        if bench_non_null_share > 0.2:
            market_pairs = [
                ("stock_return_simple", "benchmark_return_simple", "C_market_model"),
                ("stock_return_log", "benchmark_return_log", "C_market_model_log"),
            ]
            if has_adj:
                market_pairs += [
                    ("stock_return_adj_deflated_simple", "benchmark_return_simple", "C_market_model_adj_deflated"),
                    ("stock_return_adj_deflated_log", "benchmark_return_log", "C_market_model_adj_deflated_log"),
                    ("stock_return_adj_carry_simple", "benchmark_return_simple", "C_market_model_adj_carry"),
                    ("stock_return_adj_carry_log", "benchmark_return_log", "C_market_model_adj_carry_log"),
                ]
            for ret_pair in market_pairs:
                stock_col, mkt_col, label = ret_pair
                work = raw.copy().reset_index(drop=False).rename(columns={"index": "idx"})
                ar_parts = []
                mm_windows: dict[str, str | None] = {}
                for event_id, grp in work.groupby("event_id", sort=False):
                    series, win = estimate_market_model(grp, stock_col, mkt_col)
                    mm_windows[str(event_id)] = win
                    ar_parts.append(pd.DataFrame({"event_id": event_id, "idx": series.index, "ar": series.values}))
                if ar_parts:
                    ar_df = pd.concat(ar_parts, ignore_index=True)
                    work = work.merge(ar_df, on=["event_id", "idx"], how="left")
                    work["mm_estimation_window"] = work["event_id"].astype(str).map(mm_windows)
                else:
                    work["ar"] = np.nan
                    work["mm_estimation_window"] = None

                cars = car_by_window(work, "ar", CAR_WINDOWS)
                meta = work[["event_id", "source_file", "event_name", "mm_estimation_window"]].drop_duplicates("event_id")
                cars = cars.merge(meta, on="event_id", how="left")
                cars["model"] = label
                all_event_rows.append(cars)

                for (source_file, event_name, window), g in cars.groupby(["source_file", "event_name", "window"]):
                    metrics = summarize_cars(g["car"])
                    summary_rows.append(
                        {
                            "source_file": source_file,
                            "event_name": event_name,
                            "model": label,
                            "window": window,
                            **metrics,
                        }
                    )

    unified = pd.concat(unified_parts, ignore_index=True) if unified_parts else pd.DataFrame()
    event_level = pd.concat(all_event_rows, ignore_index=True) if all_event_rows else pd.DataFrame()
    summary = pd.DataFrame(summary_rows)

    # Wide event-level table: one row per event with all CAR specs
    if event_level.empty:
        pivot = pd.DataFrame(columns=["event_id", "source_file", "event_name"])
    else:
        pivot = (
            event_level.assign(col=lambda d: d["model"] + "_" + d["window"].str.replace(";", "_", regex=False))
            .pivot_table(index=["event_id", "source_file", "event_name"], columns="col", values="car", aggfunc="first")
            .reset_index()
        )
        if "mm_estimation_window" in event_level.columns:
            mm_used = event_level[["event_id", "mm_estimation_window"]].dropna().drop_duplicates("event_id")
            pivot = pivot.merge(mm_used, on="event_id", how="left")

    # Main compact table for dissertation
    main_windows = ["[-1;1]", "[-3;3]", "[-5;5]", "[0;1]", "[0;3]", "[0;5]"]
    main_models = ["A_raw", "B_mkt_adj", "C_market_model"]
    main_table = summary[summary["window"].isin(main_windows) & summary["model"].isin(main_models)].copy()
    main_table = main_table.sort_values(["source_file", "event_name", "model", "window"])

    unified.to_excel(out_dir / "unified_events_debug.xlsx", index=False)
    pivot.to_excel(out_dir / "car_event_level.xlsx", index=False)
    summary.to_excel(out_dir / "car_summary.xlsx", index=False)
    main_table.to_excel(out_dir / "car_main_table.xlsx", index=False)

    audit_lines = []
    audit_lines.append("CAR DATA QUALITY AUDIT")
    audit_lines.append("=" * 80)
    audit_lines.append("")
    audit_lines.append("Event ID columns (final):")
    audit_lines.append("buyer_ticker + deal_object + event_name + anchor_date + anchor_trade_date + source_row_excel")
    audit_lines.append("")
    for a in audits:
        audit_lines.append(f"File: {a.file_label}")
        audit_lines.append(f"Source: {a.source_path}")
        audit_lines.append(f"Rows: {a.n_rows}, Unique events: {a.n_events}")
        audit_lines.append(f"Excluded pipeline stub rows (pipeline_row_note): {a.pipeline_stub_rows_excluded}")
        audit_lines.append(f"Price field: {a.price_field} ({a.adjusted_vs_close_note})")
        audit_lines.append(f"RUONIA note: {a.ruonia_note}")
        audit_lines.append(f"Benchmark column: {a.benchmark_col}, non-null share={a.benchmark_non_null_share:.2%}")
        audit_lines.append(f"Duplicate event keys: {a.duplicate_event_keys}")
        audit_lines.append(f"Duplicate (event_id,t) rows: {a.duplicate_event_t_rows}")
        audit_lines.append(f"Missing t rows: {a.missing_t_rows}, Missing price rows: {a.missing_price_rows}")
        audit_lines.append(f"Non-sequential t events: {a.non_sequential_events}")
        audit_lines.append(f"Events with missing t=0 row: {a.incomplete_t0_events}")
        audit_lines.append(f"Events with t=0 Date != anchor_date: {a.t0_anchor_mismatch_events}")
        audit_lines.append(f"Weekend rows: {a.weekend_rows} ({a.weekend_share:.2%})")
        audit_lines.append(f"Likely calendar-day series (median date diff=1 for majority events): {a.suspicious_calendar_days}")
        audit_lines.append(f"Anomalous |return| > 50% rows: {a.anomalous_return_rows}")
        audit_lines.append(f"Events with max(t)<=0 (no post-event tail): {a.events_without_positive_t}")
        audit_lines.append(f"Max available t in file: {a.max_available_t}")
        audit_lines.append("-" * 80)
    if audit_detail_blocks:
        audit_lines.append("")
        audit_lines.append("EVENT-LEVEL DETAILS")
        audit_lines.extend(audit_detail_blocks)
    (out_dir / "car_audit.txt").write_text("\n".join(audit_lines), encoding="utf-8")

    print("Done.")
    print(f"Saved: {out_dir / 'unified_events_debug.xlsx'}")
    print(f"Saved: {out_dir / 'car_event_level.xlsx'}")
    print(f"Saved: {out_dir / 'car_summary.xlsx'}")
    print(f"Saved: {out_dir / 'car_audit.txt'}")
    print(f"Saved: {out_dir / 'car_main_table.xlsx'}")

    # Копия итоговых файлов в thesis-директорию (относительный импорт внутри пакета).
    try:
        from .final_outputs import copy_final_outputs  # type: ignore[import-not-found]

        print("Final:", copy_final_outputs(out_dir))
    except Exception:
        # Не критично для расчёта CAR; CLI/пользователь могут отдельно собрать thesis-outputs.
        pass


if __name__ == "__main__":
    run()
