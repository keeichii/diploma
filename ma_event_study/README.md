# MA event study выгрузка (Таблица 1 и Таблицы 2.1–2.3)

Полная цепочка до эмпирики и PDF-отчёта описана в **[README в корне репозитория](../README.md)** (`run_full_pipeline.sh`).

Этот проект строит таблицы по ТЗ “выгрузка котировки + фин показатели” на основе сделок из `ma_deals.xlsx` и данных T‑Invest Public API (SDK: `t-tech-investments`, репозиторий `invest-python`).

## Что делает

- **Таблица 1 (intraday, 15 минут)**: для события `first_press_release` строит свечи 15m для \(T^\*\) в окне **T−1 торговый день … T+1 торговый день**, где \(T\) определяется как:
  - если `first_press_release_time` заполнено: \(T =\) `first_press_release_date + time` (MSK), с обработкой релиза вне сессии;
  - если `first_press_release_time` пусто: используется `first_press_release_date` (без времени), а якорь берётся на **open** торговой сессии;
  - если релиз **вне торгов**: `is_off_market_release=1`, якорь переносится на **следующее открытие**.
- **Таблицы 2.1–2.3 (daily, 501 строка на сделку)**: окно **−250 … +250 торговых дней** (настраивается), столбцы: Close/Volume/IMOEX/RUONIA и финансовые метрики **as-of** ближайшей раскрытой отчётности не позже `Date`.

## Входные файлы

- **`ma_deals.xlsx`** (обязателен): таблица сделок. В заголовках допускаются разные варианты названий; скрипт пытается сопоставить колонки по алиасам.
- **Финансовые показатели** (опционально): папка с файлами `.csv/.xlsx` по компаниям (ROE/ROA/PB/PE/Assets/MarketCap/Adjusted Close), плюс при желании отдельные файлы с IMOEX и RUONIA.

## Установка

Рекомендуется Python **3.11–3.13** (Python 3.14 может быть несовместим с protobuf/grpc стеком SDK).

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r ma_event_study/requirements.txt
# Python 3.14: при ошибке импорта protobuf/UPB выполните:
# pip install "protobuf>=5.28,<8" --upgrade
```

## Настройка

Скопируйте пример конфига и поправьте пути:

```bash
cp ma_event_study/config.example.toml ma_event_study/config.toml
```

В переменной окружения должен быть токен:

```bash
export INVEST_TOKEN="t.***"
```

## Проверка перед запуском (без API)

```bash
python ma_event_study/check_project.py
```

Проверяет наличие `config.toml`, входного `ma_deals.xlsx`, файла RUONIA для merge и готовых `out/table_2_*.xlsx`.

## Запуск

```bash
python -m ma_event_study --config ma_event_study/config.toml
```

## Выходные файлы

В `output.dir` будут созданы (основные таблицы — только **.xlsx**):

- `ma_deals_AUDIT.xlsx` — аудит (как распознаны тикеры/даты/якоря, флаг off‑market).
- `table_1_intraday.xlsx`
- `table_2_1_first_press_release.xlsx`
- `table_2_2_cbonds_actualization.xlsx`
- `table_2_3_cbonds_create.xlsx`
- `run_summary.xlsx`, `run.log`

Итоговые копии без логов: папка **`final/`** (см. `ma_event_study/final_outputs.py`).

