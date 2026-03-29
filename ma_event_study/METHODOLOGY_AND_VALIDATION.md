# Методология расчета полей daily-таблиц и проверок `out/*`

Этот документ фиксирует, **какие источники данных** используются и **какие формулы/правила** применяются в текущей реализации проекта `ma_event_study` для заполнения daily-полей в `out/table_2_*.csv`.

## 1) Источники данных (строго)

1. **Рыночная часть (Close, Volume, интервалы, IMOEX)**
   - через `t_tech.invest` (класс `Client`, candles).
   - код: `ma_event_study/runner.py`, `ma_event_study/benchmarks_provider.py`.
2. **Финансовая часть (ROE, ROA, Total Assets, Equity, EPS, акции, Net Income, а также PB/PE-компоненты)**
   - только локальные wide-format CSV из папки `financials` (as-of без look-ahead).
   - код: `ma_event_study/financials_wide.py`, `ma_event_study/financials_provider.py`.
3. **RUONIA**
   - только из локальных финансовых файлов (`financials`) по имени/эвристике (или `None`).
   - код: `ma_event_study/benchmarks_provider.py`.

## 2) Как устроен daily-табличный расчет (поток данных)

1. `runner.py` формирует daily-окно для каждой сделки и каждой `Date`:
   - получает набор дневных свечей эмитента (в этом наборе есть `candle.close` и `candle.volume`),
   - для каждой выбранной дневной свечи вызывает:
     - `metrics_filler.fill_for_date(ticker, d, close_px, candle)`,
   - результат записывается в `out/table_2_1_first_press_release.csv` / `table_2_2_*` / `table_2_3_*`.
   - точка сборки строки таблицы: `ma_event_study/runner.py` внутри `build_table_2_generic(...)`.

2. Все daily-метрики вычисляет `DailyMetricsFiller`:
   - файл: `ma_event_study/daily_metrics_filler.py`.

## 3) Расчет рыночных полей (t_tech.invest candles)

### 3.1 Close, руб.
- Берется напрямую из дневной свечи эмитента:
  - `close_px = _q_to_float(candle.close)` в `build_table_2_generic(...)` (`runner.py`),
  - `filled.close = close_px` в `DailyMetricsFiller.fill_for_date(...)` (`daily_metrics_filler.py`).

**Кодовые функции:**
- `_q_to_float` использует `t_tech.invest.utils.quotation_to_decimal` и затем `float(...)`.

### 3.2 Volume, млрд. руб.
В `daily_metrics_filler.py`:

1. Сначала оценивается “оборот в рублях” по candle:
   - берется `volume_shares = candle.volume`
   - берутся `open, high, low, close`, переводятся в float,
   - вычисляется `typical = среднее(open, high, low, close)` по доступным значениям,
   - `turnover_rub = typical * volume_shares`.
2. Затем:
   - `volume_bln_rub = turnover_rub / 1e9`.

Функция-реализация:
- `DailyMetricsFiller._candle_turnover_rub(candle)`

### 3.3 IMOEX daily close
1. `benchmarks_provider.preload_imoex(...)` загружает исторические дневные свечи IMOEX одним диапазоном дат.
2. IMOEX инструмент подбирается через `client.instruments.indicatives(request=IndicativesRequest())`.
3. Далее:
   - `client.get_all_candles(... interval=CANDLE_INTERVAL_DAY ...)`,
   - `close` извлекается и складывается в `MetricSeries`.

Ключевые функции:
- `BenchmarksProvider._find_imoex_instrument(...)`
- `BenchmarksProvider._load_imoex_series(...)`
- `BenchmarksProvider.get_imoex_asof(d)` (as-of из `MetricSeries`)

> Примечание: если в SDK не работает `get_all_candles`, используется fallback на `client.market_data.get_candles` внутри `BenchmarksProvider._load_imoex_series(...)`.

## 4) Wide-format финансовые ряды и as-of правило

### 4.1 Формат финансового CSV (wide)
Описан в `financials_wide.py`:
- разделитель `;`
- первая колонка: название метрики
- строки: метрики
- колонки после первой: периоды/даты
- строка, содержащая `Дата отчета`, задает `report_date` для каждой периодной колонки.

### 4.2 Как превращаем wide в time-series (MetricSeries)
В `FinancialsWideParser.parse()`:

1. Находим строку заголовка периодов: `report_date_row` (та, где в первой колонке есть `Дата отчета`).
2. Для каждой периодной колонки `j` парсим дату:
   - `col_dates[j] = parse_maybe_date(report_date_row[j])`
   - формат в файле: `dd.mm.YYYY`.
3. Для каждой строки-метрики:
   - нормализуем имя метрики (`_normalize_metric_name`)
   - маппим на internal metric_key через `_map_metric_key(metric_name_upper)`
   - значения парсим как число (`parse_number_maybe`)
   - формируем `d2v[report_date] = value`.
4. Для каждого metric_key создаем `MetricSeries(dates_sorted, values_sorted)`.

### 4.3 As-of правило (anti look-ahead)
В `MetricSeries.get_asof(d)`:
- используется `bisect_right(self.dates, d) - 1`,
- возвращается значение на последней дате раскрытия `report_date <= d`,
- если подходящей даты нет — `None`.

Эта логика — основа корректного event study без look-ahead bias.

## 5) Финансовые поля daily-таблицы (что куда берется)

Все фундаментальные ряды берутся через `FinancialsProvider.get_asof(...)`:
- `FinancialsProvider` находит файл по тикеру (эвристики по имени),
- парсит wide CSV через `FinancialsWideParser`,
- возвращает `FinancialMetricResult(value, source, reason)`.

### 5.1 Total Assets
1. В `financials_wide.py` метрика `Total Assets` маппится на:
   - `total_assets_bln_rub`,
   - условие маппинга: строка содержит `АКТИВЫ` и `МЛРД` (и не совпадает с другими паттернами).
2. В `DailyMetricsFiller.fill_for_date()`:
   - `total_assets = financials.get_asof(ticker, metric_key="total_assets_bln_rub", d=d)`
3. В таблицу записывается `FilledDailyMetrics.total_assets = total_assets.value`.

### 5.2 ROE, ROA
- `roe_pct` и `roa_pct` берутся из:
  - строк, начинающихся с `ROE` и `ROA` (мэппинг в `_map_metric_key`),
- затем as-of по дате `d`.

### 5.3 Рыночная оценка: Market Capitalization (главный методологический фикс)

#### 5.3.1 Shares outstanding берутся только из financials
В `financials_wide.py`:
- `shares_ao_mln` маппится по строке `ЧИСЛО АКЦИЙ АО`
- `shares_ap_mln` маппится по строке `ЧИСЛО АКЦИЙ АП`

#### 5.3.2 Формула Market Capitalization на каждую `Date`
После фикса в `daily_metrics_filler.py`:

1. Выбирается “какой класс акций” использовать:
   - флаг `market_cap_shares_variant` в конфиге.
   - при `auto` для `SBER...P` используется `ap`, иначе `ao`.
2. На дату `d` (для конкретной строки daily-окна) считаем:

**Обозначения**
- `P(d)` = `Close, rub` на дату `d` (берется из дневной свечи эмитента)
- `S_class(d)` = число акций выбранного класса, в **mln** (из financials)
- `S_count(d)` = `S_class(d) * 1e6` (перевод mln -> штуки)

Тогда:

`MarketCap_bln(d) = P(d) * S_count(d) / 1e9`

Подставляя `S_count(d) = S_class(d) * 1e6`:

`MarketCap_bln(d) = P(d) * S_class(d) * 1e6 / 1e9 = P(d) * S_class(d) / 1000`

Реальная строка кода:
- `market_cap_bln = float(close_px) * float(shares.value) / 1000.0`

#### 5.3.3 Fallback
Если rebuild невозможен (например, нет shares метрики или нет `Close`), тогда используется ready-ряд:
- `market_cap_bln_rub` из `financials` как fallback.

### 5.4 P/B

Цель: устранить методологическую “заморозку” мультипликатора.

В `daily_metrics_filler.py`:

1. Компонент `equity_bln_rub` парсится из wide-format:
   - `Капитал, млрд руб` -> `equity_bln_rub`.
2. Формула:

`P/B(d) = MarketCap_bln(d) / Equity_bln_rub(d)`

Если rebuild компонентов возможен — используется rebuilt `P/B`.
Если нет — берется готовый `pb` из `financials` (fallback).

### 5.5 P/E

Приоритет: `Close / EPS` (наиболее “рыночный” вид).

1. `EPS`:
   - `EPS, руб` -> `eps_rub`.
2. Формула:

`P/E(d) = Close(d) / EPS(d)`

Если EPS недоступен — fallback:
- `P/E(d) = MarketCap_bln(d) / NetIncome_bln_rub(d)`

Где:
- `NetIncome_bln_rub` извлекается из строки `Чистая прибыль, млрд руб` -> `net_income_bln_rub`.

## 6) Adjusted Close и RUONIA (почему они None)

### 6.1 Adjusted Close
В текущих financials CSV, которые лежат в `data/financials`, отдельного ряда `adjusted_close` обычно нет.

Поэтому:
- `DailyMetricsFiller` сначала пытается взять `adjusted_close` из `financials`,
- fallback `Adjusted Close = Close` разрешен **только** если включен явный флаг:
  - `adjusted_close_allow_close_fallback = true` в `config.toml`.

### 6.2 RUONIA
`BenchmarksProvider.get_ruonia_asof(d)` пытается найти RUONIA:
 - файл по имени содержит `RUONIA`,
 - затем через `_map_metric_key` ищет ключи из списка эвристик.
Если файл/метрика не находятся — `RUONIA (daily) = None`.

## 7) Проверки “что меняется” в out-файлах (как я это считал)

Чтобы проверить подозрение “в Total Assets берется одно и то же значение”, я использовал структуру таблицы:
- `source_row_excel` — номер строки сделки во входном Excel (в таблицу попадает как `i + 2`),
- `Date` — конкретная дата в daily-окне,
- каждая строка таблицы — одна (deal, Date).

Логика проверки:

1. Берем конкретную таблицу, например `out/table_2_1_first_press_release.csv`.
2. Оставляем только строки, где значение метрики не-null.
3. Группируем по `source_row_excel`.
4. Считаем `nunique()` по метрике.

Пример проверки (как в моих скриптах):

```python
df = pd.read_csv('out/table_2_1_first_press_release.csv')
col = 'Total Assets'
nonnull = df[df[col].notna()]
by_deal = nonnull.groupby('source_row_excel')[col].nunique()
deals_constant = (by_deal == 1).sum()
deals_varying = (by_deal > 1).sum()
```

Поскольку `Total Assets` — as-of ряда `financials`, внутри сделки оно может выглядеть “ступеньками”:
- значения меняются только тогда, когда `report_date <= Date` переходит к следующему раскрытию.

## 8) Вывод по методологическим рискам из проверки

1. `Adjusted Close` и `RUONIA` действительно могут быть полностью `None`, если соответствующие локальные ряды не найдены.
2. `Market Capitalization` после фикс-правки **точно пересчитывается по `Date`** как:
   - `Close(d) * Shares_outstanding_asof(d) / 1000`.
   Поэтому внутри сделки теперь появляется вариативность по датам (а не “плоский” кусок).
3. `Total Assets`:
   - **не** “одинаковое на все сделки всегда”,
   - внутри некоторых сделок может быть константой (часто из-за as-of ступенчатости, а не потому что код берет одно значение на весь deal).

