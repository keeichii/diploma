# Deal trace: source_row_excel=28

## Цепочка данных: Excel → audit → Table 2.1 → панели → enriched

## 1. Строка audit (ma_deals_AUDIT.xlsx)

- **audit_first_press_release_date_parsed**: `2023-09-07 00:00:00` → parse_date_any=2023-09-07
- **audit_release_anchor_trade_date**: `2023-12-15 00:00:00` → parse_date_any=2023-12-15
- **audit_release_anchor_timestamp_msk**: `2023-12-15T17:00:00+03:00` → parse_date_any=2023-12-15
- **audit_release_anchor_reason**: `release_on_non_trade_day_next_open`
- **audit_resolved_ticker**: `SVCB`
- **audit_notes**: `resolved=raw_ticker_exact; release_anchor=release_on_non_trade_day_next_open; WARNING: anchor_trade_date is 99d from release_date`
- **delta (anchor_trade − release parsed)**: 99 дней

--------------------------------------------------------------------------------

## 2. Table 2.1 (runner output: first_press_release)

- Путь: `/home/keeichi/Work/diplom/out/table_2_1_first_press_release.xlsx`

- **anchor_date**: `2023-12-15`
- **anchor_date_raw**: `07.09.2023`
- **anchor_trade_date**: `2023-12-15`
- **event_name**: `first_press_release`
- **t**: `0`
- **Date**: `2023-12-15`
- **min(t)** в панели: `0` | **max(t)**: `18` | **n_rows**: `19`
- **Сверка t=0**: Date(t=0)=2023-12-15 vs anchor_date(колонка)=2023-12-15 → совпадение=ДА

--------------------------------------------------------------------------------

## 3. ma_deals_enriched.csv

- **announcement_date_std**: `2023-09-07`
- **buyer_ticker_std**: `SVCB`
- **deal_object_std**: `Инлайф Страхование (Страховая компания Уралсиб)`
- **announcement_date_std**: `2023-09-07`
- **completion_date_std**: `2023-08-31`

--------------------------------------------------------------------------------

## 4. announcement_daily_panel_clean — пересчёт CAR/BHAR

- N rows: **19** | model: **mean_adjusted** | n_est=0
- min(t)=0, max(t)=18
- CAR_ANN_1_1 recomputed=NaN | stored=NaN
- CAR_ANN_3_3 recomputed=NaN | stored=NaN
- BHAR_ANN_120 recomputed=NaN | stored=NaN

--------------------------------------------------------------------------------

## 5. Completion metrics (recomputed vs enriched)

- CAR_CLOSE_1_1: recomputed=`NaN` stored=`NaN`
- CAR_CLOSE_3_3: recomputed=`NaN` stored=`NaN`
- CAR_CLOSE_5_5: recomputed=`NaN` stored=`NaN`
- BHAR_CLOSE_120: recomputed=`NaN` stored=`NaN`

--------------------------------------------------------------------------------

## 6. Intraday (enriched поля)

- intraday rows: 45
- **CAR_ANN_INTRADAY_15M**: `-0.10205479`
- **CAR_INTRADAY_PRE_4_0**: `NaN`
- **ratio_1h_vs_day**: `81.31831377`
- **is_off_market_release**: `1`

--------------------------------------------------------------------------------

## 7. Фундаменталы ANN_*

- ANN_ROE: `NaN`
- ANN_ROA: `NaN`
- ANN_P_B: `1.28468164`
- Volume_bln_avg_pre: `NaN`
