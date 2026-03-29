# M&A event study и эмпирический пайплайн диплома

Сбор таблиц по API, merge RUONIA, event-study CAR (дневной и intraday), затем обогащение выборки и отчёты (бывшая папка **Codex**).

## Быстрый старт (всё подряд)

Рекомендуется Python **3.11–3.13**. На **3.14** пакет **protobuf 4.x** (подтягивается с `t-tech-investments`) при импорте UPB падает с `TypeError: Metaclasses with custom tp_new are not supported`. Скрипт **`run_full_pipeline.sh`** для 3.14+ сам выполняет `pip install "protobuf>=5.28,<8" --upgrade` (в метаданных t-tech указано `protobuf<5`, но на практике SDK с protobuf 5–7 работает).

При ручном запуске без скрипта на 3.14 после `pip install -r ...` выполните:

```bash
pip install "protobuf>=5.28,<8" --upgrade
```

Нужны переменная **`INVEST_TOKEN`** и входные данные в **`data/input/`** (см. ниже).

```bash
cd /path/to/diplom
python3 -m venv .venv
source .venv/bin/activate   # или: .venv\Scripts\activate на Windows
pip install -r ma_event_study/requirements.txt
export INVEST_TOKEN="t.***"
cp ma_event_study/config.example.toml ma_event_study/config.toml   # при необходимости поправьте пути
./run_full_pipeline.sh
```

Скрипт **`run_full_pipeline.sh`** в первую очередь использует **`.venv312/bin/python`**, при отсутствии — `.venv/bin/python`, иначе `python3`.

Перед первым запуском для выбранного venv установите зависимости (в том числе `matplotlib`, `statsmodels` для этапов 5–6):

```bash
./.venv312/bin/pip install -r ma_event_study/requirements.txt
```

### Продолжить с этапа 5 (thesis и нарратив)

Нужны уже собранные xlsx в **`out/`**: `ma_deals_AUDIT.xlsx`, `table_1_intraday.xlsx`, `table_2_1_*.xlsx`, `table_2_2_*.xlsx`, `table_2_3_*.xlsx`. Токен API не требуется.

```bash
cd /path/to/diplom   # корень репозитория
./.venv312/bin/python ma_event_study/ma_thesis_pipeline.py
./.venv312/bin/python ma_event_study/build_research_story_report.py
```

Результаты: **`out/thesis/`** (панели, таблицы, графики) и **`out/thesis/tables/narrative_research_report_ru.pdf`** (и `.docx`).

### Что делает `run_full_pipeline.sh`

1. **`python -m ma_event_study --config ma_event_study/config.toml`** — выгрузка `out/ma_deals_AUDIT.xlsx`, `table_1_*.xlsx`, `table_2_*.xlsx`, логи.
2. **`python ma_event_study/merge_ruonia_dt.py`** — подстановка RUONIA в дневные таблицы (`data/input/ruonia and else.xlsx`), копии в `out/ruonia_augmented/`, `out/final/`.
3. **`python ma_event_study/car_event_study_analysis.py`** — дневной CAR/BHAR, файлы `out/car_*.xlsx`, аудиты.
4. **`python ma_event_study/intraday_event_study_analysis.py`** — intraday CAR, `out/intraday_*.xlsx`.
5. **`python ma_event_study/ma_thesis_pipeline.py`** — панели, регрессии, графики в **`out/thesis/`** (clean_data, tables, charts, models).
6. **`python ma_event_study/build_research_story_report.py`** — Word/PDF нарратив в **`out/thesis/tables/`** (`narrative_research_report_ru.docx` / `.pdf`).

Проверка без входа в API (только файлы и конфиг):

```bash
python ma_event_study/check_project.py
```

Подробности по шагу 1 — в **[ma_event_study/README.md](ma_event_study/README.md)**.

## Структура каталогов

| Путь | Назначение |
|------|------------|
| `data/input/` | `ma_deals.xlsx`, выгрузка ЦБ с RUONIA и др. |
| `data/cache/`, `data/financials/` | кеш свечей и финансовые ряды (опционально) |
| `out/` | основные xlsx, CAR/intraday, `final/`, `ruonia_augmented/` |
| `out/thesis/` | эмпирический пайплайн и отчёты (аналог бывшего `Codex/outputs/`) |
| `ma_event_study/` | код: runner, merge, анализы, `ma_thesis_pipeline.py`, `build_research_story_report.py` |

## Примечания

- Если шаг 5/6 не нужны, запускайте только шаги 1–4 вручную.
- Файлы **`hypotheses_significance_map_ru.csv`** и **`significant_deals_case_analysis_summary_ru.csv`** в `out/thesis/tables/` опциональны: отчёт построится, таблицу гипотез при отсутствии файла можно дополнить вручную.
- Каталог **`Codex`** удалён: дублирующие xlsx в корне Codex не нужны; артефакты перенесены в **`out/thesis/`**, скрипты — в **`ma_event_study/`**.
