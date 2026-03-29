from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats


CORE_WINDOWS = [(0, 1), (-1, 1), (0, 2), (-2, 2), (0, 4)]
ROBUST_WINDOWS = [(-4, 4)]
# H4: только бары строго до якоря (bar_k < 0), окно [-4,-1]
PRE_EVENT_WINDOWS = [(-4, -1)]


def _drop_pipeline_stub_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Исключает строки с непустым pipeline_row_note (заглушки пайплайна)."""
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


@dataclass
class IntradayAudit:
    source_path: str
    n_rows: int
    n_events: int
    main_events: int
    supplementary_events: int
    empty_release_time_share: float
    duplicate_event_ts: int
    missing_price_rows: int
    missing_anchor_rows: int
    missing_trade_day_offset_rows: int
    events_missing_day_minus1: int
    events_missing_day0: int
    events_missing_day_plus1: int
    events_anchor_not_in_ts: int
    events_missing_k0: int
    bad_15m_step_rows: int
    anomalous_return_rows: int
    off_market_events: int
    pipeline_stub_rows_excluded: int


def summarize_vals(vals: pd.Series) -> dict[str, float]:
    v = vals.dropna().astype(float)
    n = len(v)
    if n == 0:
        return {
            "N_events": 0,
            "mean_CAR": np.nan,
            "median_CAR": np.nan,
            "std_CAR": np.nan,
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
    t_stat, t_p = stats.ttest_1samp(v, 0.0, nan_policy="omit")
    try:
        w_stat, w_p = stats.wilcoxon(v) if not np.allclose(v, 0.0) else (np.nan, np.nan)
    except ValueError:
        w_stat, w_p = np.nan, np.nan
    nz = v[v != 0]
    if len(nz) > 0:
        sign_p = stats.binomtest(int((nz > 0).sum()), len(nz), p=0.5).pvalue
    else:
        sign_p = np.nan
    return {
        "N_events": n,
        "mean_CAR": v.mean(),
        "median_CAR": v.median(),
        "std_CAR": v.std(ddof=1) if n > 1 else np.nan,
        "t_stat": t_stat,
        "t_pvalue": t_p,
        "wilcoxon_stat": w_stat,
        "wilcoxon_pvalue": w_p,
        "sign_test_pvalue": sign_p,
        "share_positive": (v > 0).mean(),
        "min": v.min(),
        "p25": v.quantile(0.25),
        "p50": v.quantile(0.5),
        "p75": v.quantile(0.75),
        "max": v.max(),
    }


# H4: бары k∈[-4,-1] (строго до якоря k=0); в отчётах обозначается как окно до границы события «[-4;0]»
PRE_EVENT_WINDOW_LABELS: dict[tuple[int, int], str] = {(-4, -1): "[-4;0]_pre"}


def event_cars(
    df: pd.DataFrame,
    ret_col: str,
    sample: str,
    label: str,
    windows: list[tuple[int, int]],
    window_labels: dict[tuple[int, int], str] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    labels = window_labels or {}
    for event_id, g in df.groupby("event_id", dropna=False):
        for l, r in windows:
            x = g[(g["bar_k"] >= l) & (g["bar_k"] <= r)].sort_values("bar_k")
            exp_len = r - l + 1
            win_str = labels.get((l, r), f"[{l};{r}]")
            if len(x) == exp_len and x[ret_col].notna().sum() == exp_len:
                car = float(x[ret_col].sum())
                ok = 1
            else:
                car = np.nan
                ok = 0
            rows.append({"event_id": event_id, "sample": sample, "model": label, "window": win_str, "CAR": car, "event_qualifies": ok})
    return pd.DataFrame(rows)


def run() -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = root / "out"
    src_xlsx = out_dir / "table_1_intraday.xlsx"
    if not src_xlsx.exists():
        raise FileNotFoundError("table_1_intraday.xlsx not found in out/")
    raw = pd.read_excel(src_xlsx)
    raw, n_stub_excl = _drop_pipeline_stub_rows(raw)
    if raw.empty:
        raise ValueError(
            f"После исключения строк с pipeline_row_note не осталось строк для intraday "
            f"(исключено {n_stub_excl}). Проверьте table_1_intraday.xlsx."
        )
    src = str(src_xlsx)

    rename_map = {
        "source_row_excel": "sourcerowexcel",
        "buyer_ticker": "buyerticker",
        "buyer_company": "buyercompany",
        "deal_object": "dealobject",
        "event_name": "eventname",
        "release_date": "releasedate",
        "release_time": "releasetime",
        "is_off_market_release": "isoffmarketrelease",
        "anchor_trade_date": "anchortradedate",
        "anchor_timestamp_msk": "anchortimestampmsk",
        "trade_day_offset": "tradedayoffset",
        "timestamp_msk": "timestampmsk",
        "price_at_timestamp_rub": "priceattimestamprub",
        "volume_during_timestamp_plus_15m_mn_rub": "volumeduringtimestampplus15mmnrub",
    }
    raw = raw.rename(columns=rename_map)

    raw["releasedate"] = pd.to_datetime(raw.get("releasedate"), errors="coerce").dt.date
    raw["releasetime"] = raw.get("releasetime").astype(str).replace({"nan": "", "NaT": ""}).str.strip()
    raw["timestampmsk"] = pd.to_datetime(raw.get("timestampmsk"), errors="coerce", utc=True).dt.tz_convert("Europe/Moscow")
    raw["anchortimestampmsk"] = pd.to_datetime(raw.get("anchortimestampmsk"), errors="coerce", utc=True).dt.tz_convert("Europe/Moscow")
    raw["anchortimestampmsk_str"] = raw["anchortimestampmsk"].astype(str)
    raw["tradedayoffset"] = pd.to_numeric(raw.get("tradedayoffset"), errors="coerce")
    raw["priceattimestamprub"] = pd.to_numeric(raw.get("priceattimestamprub"), errors="coerce")
    raw["isoffmarketrelease"] = pd.to_numeric(raw.get("isoffmarketrelease"), errors="coerce").fillna(0).astype(int)

    key_cols = ["buyerticker", "dealobject", "eventname", "releasedate", "anchortimestampmsk_str"]
    for extra in ["sourcerowexcel", "anchortradedate", "buyercompany"]:
        key_cols2 = key_cols + [extra]
        if not raw.duplicated(key_cols2 + ["timestampmsk"]).any():
            key_cols = key_cols2
            break
    raw["event_id"] = raw[key_cols].astype(str).agg(" | ".join, axis=1)
    raw = raw.sort_values(["event_id", "timestampmsk"]).reset_index(drop=True)

    raw["step_min"] = raw.groupby("event_id")["timestampmsk"].diff().dt.total_seconds().div(60)
    raw["is_15m_step"] = raw["step_min"].isna() | raw["step_min"].isin([15.0])
    bad_15m = int((~raw["is_15m_step"]).sum())

    dup_event_ts = int(raw.duplicated(["event_id", "timestampmsk"]).sum())
    miss_price = int(raw["priceattimestamprub"].isna().sum())
    miss_anchor = int(raw["anchortimestampmsk"].isna().sum())
    miss_tdo = int(raw["tradedayoffset"].isna().sum())

    per_event = raw.groupby("event_id")
    has_m1 = per_event["tradedayoffset"].apply(lambda s: (-1 in set(s.dropna().astype(int)))).astype(int)
    has_0 = per_event["tradedayoffset"].apply(lambda s: (0 in set(s.dropna().astype(int)))).astype(int)
    has_p1 = per_event["tradedayoffset"].apply(lambda s: (1 in set(s.dropna().astype(int)))).astype(int)
    events_miss_m1 = int((has_m1 == 0).sum())
    events_miss_0 = int((has_0 == 0).sum())
    events_miss_p1 = int((has_p1 == 0).sum())

    anchor_in_ts = per_event.apply(lambda g: g["timestampmsk"].eq(g["anchortimestampmsk"].iloc[0]).any() if g["anchortimestampmsk"].notna().any() else False)
    events_anchor_absent = int((~anchor_in_ts).sum())

    raw["bar_k"] = per_event.cumcount() - per_event.apply(lambda g: int(np.where(g["timestampmsk"].eq(g["anchortimestampmsk"].iloc[0]))[0][0]) if g["timestampmsk"].eq(g["anchortimestampmsk"].iloc[0]).any() else 0).reindex(raw["event_id"]).to_numpy()
    k0_miss = int(per_event["bar_k"].apply(lambda s: (0 in set(s))).eq(False).sum())

    empty_rt_event = per_event["releasetime"].apply(lambda s: s.fillna("").astype(str).str.strip().eq("").all())
    valid_anchor_event = per_event["anchortimestampmsk"].apply(lambda s: s.notna().all())
    main_events = (empty_rt_event == False) & valid_anchor_event
    raw["sample"] = raw["event_id"].map(lambda e: "main_sample" if bool(main_events.get(e, False)) else "supplementary_sample")

    raw["simple_return"] = raw.groupby("event_id")["priceattimestamprub"].pct_change()
    raw["log_return"] = raw.groupby("event_id")["priceattimestamprub"].transform(lambda s: np.log(s / s.shift(1)))

    # baseline_estimation_window + prev-day baseline (один проход; см. pandas 2.2+ include_groups)
    def _add_abn_intraday_baselines(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("timestampmsk")
        prior = g[g["tradedayoffset"].between(-20, -1)]
        mean_k = prior.groupby("bar_k")["simple_return"].mean()
        out = g.copy()
        out["abn_est"] = out["simple_return"] - out["bar_k"].map(mean_k).fillna(0.0)
        out["abn_est_log"] = out["log_return"]  # лог-вариант: без adjust (как baseline prev-day для лога)
        mu = pd.to_numeric(g.loc[g["tradedayoffset"] == -1, "simple_return"], errors="coerce").mean()
        mu = 0.0 if pd.isna(mu) else float(mu)
        out["abn_prev_day"] = pd.to_numeric(out["simple_return"], errors="coerce") - mu
        return out

    # pandas>=2.2: по умолчанию apply не передаёт колонки группировки → в результате пропадал event_id
    try:
        raw = raw.groupby("event_id", group_keys=False).apply(_add_abn_intraday_baselines, include_groups=True)
    except TypeError:
        parts: list[pd.DataFrame] = []
        for _, g in raw.groupby("event_id", sort=False):
            parts.append(_add_abn_intraday_baselines(g))
        raw = pd.concat(parts, ignore_index=True)

    raw["anomalous_abs_gt_10pct"] = raw["simple_return"].abs() > 0.10
    anomalous_rows = int(raw["anomalous_abs_gt_10pct"].sum())

    # extra windows
    overnight = []
    close_d0 = []
    next4 = []
    for event_id, g in raw.groupby("event_id"):
        g = g.sort_values("timestampmsk")
        d0 = g[g["tradedayoffset"] == 0]
        d1 = g[g["tradedayoffset"] == 1]
        sample = g["sample"].iloc[0]
        if len(d0) > 0 and d0["bar_k"].min() <= 0:
            x = d0[d0["bar_k"] >= 0]
            close_d0.append({"event_id": event_id, "sample": sample, "model": "raw_simple", "window": "[0;close_D0]", "CAR": x["simple_return"].sum() if x["simple_return"].notna().all() else np.nan})
            close_d0.append({"event_id": event_id, "sample": sample, "model": "raw_log", "window": "[0;close_D0]", "CAR": x["log_return"].sum() if x["log_return"].notna().all() else np.nan})
        else:
            close_d0.append({"event_id": event_id, "sample": sample, "model": "raw_simple", "window": "[0;close_D0]", "CAR": np.nan})
            close_d0.append({"event_id": event_id, "sample": sample, "model": "raw_log", "window": "[0;close_D0]", "CAR": np.nan})
        if len(d1) >= 4:
            y = d1.sort_values("timestampmsk").head(4)
            next4.append({"event_id": event_id, "sample": sample, "model": "raw_simple", "window": "[next_day_first_4_bars]", "CAR": y["simple_return"].sum() if y["simple_return"].notna().all() else np.nan})
            next4.append({"event_id": event_id, "sample": sample, "model": "raw_log", "window": "[next_day_first_4_bars]", "CAR": y["log_return"].sum() if y["log_return"].notna().all() else np.nan})
        else:
            next4.append({"event_id": event_id, "sample": sample, "model": "raw_simple", "window": "[next_day_first_4_bars]", "CAR": np.nan})
            next4.append({"event_id": event_id, "sample": sample, "model": "raw_log", "window": "[next_day_first_4_bars]", "CAR": np.nan})
        if len(d0) > 0 and len(d1) > 0:
            p0 = d0.sort_values("timestampmsk")["priceattimestamprub"].iloc[-1]
            p1 = d1.sort_values("timestampmsk")["priceattimestamprub"].iloc[0]
            ov = p1 / p0 - 1 if pd.notna(p0) and pd.notna(p1) and p0 != 0 else np.nan
        else:
            ov = np.nan
        overnight.append({"event_id": event_id, "sample": sample, "model": "raw_simple", "window": "[overnight_gap_D0_to_Dplus1_open]", "CAR": ov})

    ratio_rows: list[dict[str, object]] = []
    for event_id, g in raw.groupby("event_id"):
        g = g.sort_values("timestampmsk")
        pre = g.loc[g["tradedayoffset"] == -1, "simple_return"].dropna()
        exp = float(pre.mean()) if len(pre) >= 1 else 0.0
        d0 = g.loc[g["tradedayoffset"] == 0].sort_values("timestampmsk")
        if d0.empty or g["anchortimestampmsk"].isna().all():
            ratio_rows.append({"event_id": event_id, "ratio_1h_vs_day_pct": np.nan})
            continue
        anchor = g["anchortimestampmsk"].dropna().iloc[0]
        ab = d0["simple_return"].astype(float) - exp
        day_tot = float(ab.sum())
        end1h = anchor + pd.Timedelta(hours=1)
        m1 = (d0["timestampmsk"] > anchor) & (d0["timestampmsk"] <= end1h)
        h1 = float(ab.loc[m1].sum()) if m1.any() else np.nan
        ratio_rows.append(
            {
                "event_id": event_id,
                "ratio_1h_vs_day_pct": (h1 / day_tot * 100.0) if day_tot and not np.isclose(day_tot, 0.0) else np.nan,
            }
        )
    ratio_df = pd.DataFrame(ratio_rows)

    all_event: list[pd.DataFrame] = []
    for sample_name in ["main_sample", "supplementary_sample"]:
        sub = raw[raw["sample"] == sample_name].copy()
        all_event.append(
            event_cars(
                sub,
                "simple_return",
                sample_name,
                "raw_simple",
                CORE_WINDOWS + ROBUST_WINDOWS + PRE_EVENT_WINDOWS,
                window_labels=PRE_EVENT_WINDOW_LABELS,
            )
        )
        all_event.append(
            event_cars(
                sub,
                "log_return",
                sample_name,
                "raw_log",
                CORE_WINDOWS + ROBUST_WINDOWS + PRE_EVENT_WINDOWS,
                window_labels=PRE_EVENT_WINDOW_LABELS,
            )
        )
        all_event.append(
            event_cars(
                sub,
                "abn_est",
                sample_name,
                "baseline_estimation_window",
                PRE_EVENT_WINDOWS + CORE_WINDOWS + ROBUST_WINDOWS,
                window_labels=PRE_EVENT_WINDOW_LABELS,
            )
        )
        all_event.append(
            event_cars(
                sub,
                "abn_prev_day",
                sample_name,
                "baseline_prev_day",
                PRE_EVENT_WINDOWS + CORE_WINDOWS + ROBUST_WINDOWS,
                window_labels=PRE_EVENT_WINDOW_LABELS,
            )
        )
    all_event.append(pd.DataFrame(close_d0))
    all_event.append(pd.DataFrame(next4))
    all_event.append(pd.DataFrame(overnight))
    event_level = pd.concat(all_event, ignore_index=True)
    event_level["event_qualifies"] = event_level["CAR"].notna().astype(int)
    event_level = event_level.merge(ratio_df, on="event_id", how="left")

    summary_rows = []
    for (sample, model, window), g in event_level.groupby(["sample", "model", "window"]):
        stats_row = summarize_vals(g["CAR"])
        summary_rows.append({"sample": sample, "model": model, "window": window, **stats_row})
    summary = pd.DataFrame(summary_rows).sort_values(["sample", "model", "window"])

    # On-market vs off-market split
    off_map = raw.groupby("event_id")["isoffmarketrelease"].max()
    first_off = raw.groupby("event_id")["isoffmarketrelease"].first()
    event_level["offmarket_group"] = event_level["event_id"].map(lambda e: "off_market" if off_map.get(e, 0) == 1 else "on_market")
    split_rows = []
    for (sample, model, window, grp), g in event_level.groupby(["sample", "model", "window", "offmarket_group"]):
        x = summarize_vals(g["CAR"])
        split_rows.append({"sample": sample, "model": model, "window": window, "offmarket_group": grp, **x})
    split_df = pd.DataFrame(split_rows)
    if not split_df.empty:
        split_df["is_off_market_release"] = (split_df["offmarket_group"] == "off_market").astype(int)

    # H5: одновыборочный t-test CAR (mean=0) отдельно для on-market и off-market; окно [-1;1], raw_simple
    off_tt_rows: list[dict] = []
    h5 = event_level[(event_level["model"] == "raw_simple") & (event_level["window"] == "[-1;1]")].copy()
    if not h5.empty:
        h5["is_off_market_release"] = h5["event_id"].map(lambda e: int(first_off.get(e, 0)))
        for ioff, sub in h5.groupby("is_off_market_release", sort=True):
            vals = sub["CAR"].dropna().astype(float)
            if len(vals) < 2:
                continue
            t_stat, p_val = stats.ttest_1samp(vals, 0.0, nan_policy="omit")
            off_tt_rows.append(
                {
                    "is_off_market_release": int(ioff),
                    "model": "raw_simple",
                    "window": "[-1;1]",
                    "n": len(vals),
                    "t_stat": float(t_stat),
                    "p_value": float(p_val),
                }
            )
    baseline_cmp: list[dict] = []
    for (sample, window), g in event_level[event_level["window"].isin(["[-1;1]", "[-4;0]_pre"])].groupby(["sample", "window"]):
        for model in ("raw_simple", "baseline_prev_day", "baseline_estimation_window"):
            gm = g[g["model"] == model]
            if gm.empty:
                continue
            baseline_cmp.append({"sample": sample, "window": window, "model": model, **summarize_vals(gm["CAR"])})
    pd.DataFrame(off_tt_rows).to_csv(out_dir / "intraday_offmarket_ttests.csv", index=False, encoding="utf-8")
    pd.DataFrame(baseline_cmp).to_csv(out_dir / "intraday_baseline_method_compare.csv", index=False, encoding="utf-8")

    # Files
    unified = raw[
        [
            "sourcerowexcel",
            "buyerticker",
            "buyercompany",
            "dealobject",
            "eventname",
            "releasedate",
            "releasetime",
            "isoffmarketrelease",
            "anchortradedate",
            "anchortimestampmsk",
            "tradedayoffset",
            "timestampmsk",
            "priceattimestamprub",
            "volumeduringtimestampplus15mmnrub",
            "event_id",
            "sample",
            "bar_k",
            "simple_return",
            "log_return",
            "abn_est",
            "abn_prev_day",
            "is_15m_step",
            "anomalous_abs_gt_10pct",
        ]
    ].copy()
    for c in ["timestampmsk", "anchortimestampmsk"]:
        if c in unified.columns:
            unified[c] = pd.to_datetime(unified[c], errors="coerce").dt.tz_localize(None)
    unified.to_excel(out_dir / "intraday_unified_debug.xlsx", index=False)
    event_level.to_excel(out_dir / "intraday_event_level_car.xlsx", index=False)
    with pd.ExcelWriter(out_dir / "intraday_car_summary.xlsx") as xw:
        summary.to_excel(xw, sheet_name="summary", index=False)
        split_df.to_excel(xw, sheet_name="on_off_market_split", index=False)

    core_main = summary[
        (summary["sample"] == "main_sample")
        & (summary["model"] == "raw_simple")
        & (
            summary["window"].isin(
                ["[-4;0]_pre", "[-1;1]", "[0;2]", "[-2;2]", "[0;4]", "[0;close_D0]"]
            )
        )
    ]
    core_main.to_excel(out_dir / "intraday_main_table.xlsx", index=False)

    aud = IntradayAudit(
        source_path=src,
        n_rows=len(raw),
        n_events=raw["event_id"].nunique(),
        main_events=int(main_events.sum()),
        supplementary_events=int((~main_events).sum()),
        empty_release_time_share=float(empty_rt_event.mean()),
        duplicate_event_ts=dup_event_ts,
        missing_price_rows=miss_price,
        missing_anchor_rows=miss_anchor,
        missing_trade_day_offset_rows=miss_tdo,
        events_missing_day_minus1=events_miss_m1,
        events_missing_day0=events_miss_0,
        events_missing_day_plus1=events_miss_p1,
        events_anchor_not_in_ts=events_anchor_absent,
        events_missing_k0=k0_miss,
        bad_15m_step_rows=bad_15m,
        anomalous_return_rows=anomalous_rows,
        off_market_events=int((off_map == 1).sum()),
        pipeline_stub_rows_excluded=n_stub_excl,
    )
    lines = [
        "INTRADAY AUDIT",
        "=" * 80,
        f"Source: {aud.source_path}",
        f"Rows: {aud.n_rows}",
        f"Unique events: {aud.n_events}",
        f"Main sample events: {aud.main_events}",
        f"Supplementary sample events: {aud.supplementary_events}",
        f"Empty releasetime event share: {aud.empty_release_time_share:.2%}",
        f"Duplicate (event_id,timestampmsk): {aud.duplicate_event_ts}",
        f"Missing price rows: {aud.missing_price_rows}",
        f"Missing anchor timestamp rows: {aud.missing_anchor_rows}",
        f"Missing tradedayoffset rows: {aud.missing_trade_day_offset_rows}",
        f"Events missing tradedayoffset=-1: {aud.events_missing_day_minus1}",
        f"Events missing tradedayoffset=0: {aud.events_missing_day0}",
        f"Events missing tradedayoffset=+1: {aud.events_missing_day_plus1}",
        f"Events where anchor timestamp absent in series: {aud.events_anchor_not_in_ts}",
        f"Events with missing k=0: {aud.events_missing_k0}",
        f"Rows with non-15m step: {aud.bad_15m_step_rows}",
        f"Rows with |intraday return| > 10%: {aud.anomalous_return_rows}",
        f"Off-market events: {aud.off_market_events}",
        f"Excluded pipeline stub rows (pipeline_row_note): {aud.pipeline_stub_rows_excluded}",
    ]
    (out_dir / "intraday_audit.txt").write_text("\n".join(lines), encoding="utf-8")

    print("Done intraday analysis.")
    print(out_dir / "intraday_unified_debug.xlsx")
    print(out_dir / "intraday_event_level_car.xlsx")
    print(out_dir / "intraday_car_summary.xlsx")
    print(out_dir / "intraday_audit.txt")
    print(out_dir / "intraday_main_table.xlsx")

    from final_outputs import copy_final_outputs

    print("Final:", copy_final_outputs(out_dir))


if __name__ == "__main__":
    run()
