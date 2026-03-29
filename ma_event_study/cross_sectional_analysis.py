"""
OLS с робастной ковариацией HC3 для H9–H17.
Вход: out/clean_data/ma_deals_enriched.csv (или out/thesis/clean_data/...).
Выход: out/thesis/models/regression_*.csv
"""
from __future__ import annotations

import logging
import warnings as py_warnings
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.regression.linear_model import OLS

from .paths import THESIS_MODELS_DIR, resolve_clean_data_file, ensure_clean_dirs

logger = logging.getLogger("ma_event_study")

MIN_N = 20


def _log1p_safe(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    return np.log(np.maximum(x.replace(0, np.nan), np.finfo(float).tiny))


def _run_one_ols(
    df: pd.DataFrame,
    y_col: str,
    x_cols: list[str],
    model_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    coef_rows: list[dict] = []
    diag_rows: list[dict] = []
    warns: list[str] = []
    use = [y_col] + [c for c in x_cols if c in df.columns]
    d = df[use].copy()
    d = d.replace([np.inf, -np.inf], np.nan).dropna()
    if len(d) < MIN_N:
        warns.append(f"{model_name}: n={len(d)} < {MIN_N}, пропуск")
        return pd.DataFrame(coef_rows), pd.DataFrame(diag_rows), warns
    y = d[y_col].astype(float)
    X = d[[c for c in x_cols if c in d.columns]].astype(float)
    X = sm.add_constant(X, has_constant="add")
    with py_warnings.catch_warnings():
        py_warnings.simplefilter("ignore")
        try:
            res = OLS(y, X).fit(cov_type="HC3")
        except Exception as e:
            warns.append(f"{model_name}: OLS error {e}")
            return pd.DataFrame(coef_rows), pd.DataFrame(diag_rows), warns
    ci = res.conf_int()
    for term in res.params.index:
        coef_rows.append(
            {
                "model": model_name,
                "term": term,
                "coef": float(res.params[term]),
                "std_err": float(res.bse[term]),
                "t_stat": float(res.tvalues[term]),
                "p_value": float(res.pvalues[term]),
                "conf_int_low": float(ci.loc[term, 0]),
                "conf_int_high": float(ci.loc[term, 1]),
            }
        )
    diag_rows.append(
        {
            "model": model_name,
            "n_obs": int(res.nobs),
            "r_squared": float(res.rsquared),
            "adj_r_squared": float(res.rsquared_adj),
            "f_stat": float(res.fvalue) if res.fvalue is not None and np.isfinite(res.fvalue) else np.nan,
            "f_p_value": float(res.f_pvalue) if res.f_pvalue is not None and np.isfinite(res.f_pvalue) else np.nan,
            "AIC": float(res.aic),
            "BIC": float(res.bic),
        }
    )
    return pd.DataFrame(coef_rows), pd.DataFrame(diag_rows), warns


def run(enriched_path: Path | None = None) -> None:
    ensure_clean_dirs()
    path = enriched_path or resolve_clean_data_file("ma_deals_enriched.csv")
    if not path.is_file():
        logger.warning("cross_sectional: нет %s — пустые CSV", path)
        empty_c = pd.DataFrame(columns=["model", "term", "coef", "std_err", "t_stat", "p_value", "conf_int_low", "conf_int_high"])
        empty_d = pd.DataFrame(columns=["model", "n_obs", "r_squared", "adj_r_squared", "f_stat", "f_p_value", "AIC", "BIC"])
        empty_c.to_csv(THESIS_MODELS_DIR / "regression_coefficients.csv", index=False, encoding="utf-8")
        empty_d.to_csv(THESIS_MODELS_DIR / "regression_diagnostics.csv", index=False, encoding="utf-8")
        return
    df = pd.read_csv(path, encoding="utf-8")
    all_c: list[pd.DataFrame] = []
    all_d: list[pd.DataFrame] = []

    # Подготовка регрессоров (имена как в обогащённой таблице / ma_thesis)
    if "deal_value_usd_mn_std" in df.columns:
        df["log_DealSize_bln"] = _log1p_safe(df["deal_value_usd_mn_std"] / 1000.0)  # грубый UX: USD млн → bln
    else:
        df["log_DealSize_bln"] = np.nan
    for c_base, name in [
        ("ANN_MARKET_CAP", "log_MarketCap_bln"),
        ("ANN_TOTAL_ASSETS", "log_TotalAssets"),
    ]:
        if c_base in df.columns:
            df[name] = _log1p_safe(df[c_base])
        elif name not in df.columns:
            df[name] = np.nan
    if "log_MarketCap_bln" not in df.columns and "ANN_MARKET_CAP" in df.columns:
        df["log_MarketCap_bln"] = _log1p_safe(df["ANN_MARKET_CAP"])

    deal_dum = pd.get_dummies(df.get("deal_type", pd.Series("unknown", index=df.index)), prefix="dt", dtype=float)
    df = pd.concat([df, deal_dum], axis=1)
    dum_cols = [c for c in df.columns if str(c).startswith("dt_")]

    specs: list[tuple[str, str, list[str]]] = [
        ("M1_H9_H15_CAR_ANN", "CAR_ANN_1_1", ["log_DealSize_bln", "log_MarketCap_bln"]),
        ("M2_H14_H16_CAR_ANN", "CAR_ANN_1_1", ["ANN_ROE", "ANN_ROA", "ANN_P_B", "ANN_P_E", "log_TotalAssets"]),
        ("M3_H10_13_CAR_ANN", "CAR_ANN_1_1", dum_cols),
        ("M4_H17_CAR_ANN", "CAR_ANN_1_1", ["log_Volume_pre20"]),
        ("M1_CLOSE", "CAR_CLOSE_5_5", ["log_DealSize_bln", "log_MarketCap_bln"]),
        ("M2_CLOSE", "CAR_CLOSE_5_5", ["ANN_ROE", "ANN_ROA", "ANN_P_B", "ANN_P_E", "log_TotalAssets"]),
    ]

    if "Volume_bln_avg_pre" in df.columns:
        df["log_Volume_pre20"] = _log1p_safe(df["Volume_bln_avg_pre"])
    else:
        df["log_Volume_pre20"] = np.nan

    all_warns: list[str] = []
    for mname, yc, xc in specs:
        xc2 = [c for c in xc if c in df.columns]
        if yc not in df.columns:
            all_warns.append(f"{mname}: нет зависимой {yc}")
            continue
        if not xc2:
            all_warns.append(f"{mname}: нет регрессоров")
            continue
        c_df, d_df, w = _run_one_ols(df, yc, xc2, mname)
        all_c.append(c_df)
        all_d.append(d_df)
        all_warns.extend(w)

    coef_out = pd.concat(all_c, ignore_index=True) if all_c else pd.DataFrame()
    diag_out = pd.concat(all_d, ignore_index=True) if all_d else pd.DataFrame()
    coef_out.to_csv(THESIS_MODELS_DIR / "regression_coefficients.csv", index=False, encoding="utf-8")
    diag_out.to_csv(THESIS_MODELS_DIR / "regression_diagnostics.csv", index=False, encoding="utf-8")
    for w in all_warns:
        logger.warning("%s", w)
    print("cross_sectional_analysis: ->", THESIS_MODELS_DIR)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
