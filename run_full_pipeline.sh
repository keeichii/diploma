#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ -z "${INVEST_TOKEN:-}" ]]; then
  echo "Ошибка: задайте переменную окружения INVEST_TOKEN (токен T-Invest)." >&2
  exit 1
fi

PY="python3"
if [[ -x "$ROOT/.venv312/bin/python" ]]; then
  PY="$ROOT/.venv312/bin/python"
elif [[ -x "$ROOT/.venv/bin/python" ]]; then
  PY="$ROOT/.venv/bin/python"
fi
echo "Интерпретатор: $PY"

# Python 3.14: protobuf 4.x (зависимость t-tech-investments) падает при загрузке UPB.
# pip не может одновременно зафиксировать protobuf>=5 в requirements.txt с t-tech, поэтому обновляем здесь.
if "$PY" -c "import sys; raise SystemExit(0 if sys.version_info[:2] >= (3, 14) else 1)" 2>/dev/null; then
  echo "Python 3.14+: обновляю protobuf (совместимость UPB/gRPC)…"
  "$PY" -m pip install "protobuf>=5.28,<8" --upgrade
fi

export MPLCONFIGDIR="${MPLCONFIGDIR:-$ROOT/.mplconfig}"
mkdir -p "$MPLCONFIGDIR"

echo "== 1/6: ma_event_study (API, таблицы 1–2) =="
$PY -m ma_event_study --config ma_event_study/config.toml

echo "== 2/6: merge RUONIA =="
$PY ma_event_study/merge_ruonia_dt.py

echo "== 3/6: CAR (дневной) =="
$PY ma_event_study/car_event_study_analysis.py

echo "== 4/6: intraday CAR =="
$PY ma_event_study/intraday_event_study_analysis.py

echo "== 5/6: thesis pipeline (панели, регрессии, графики) =="
$PY ma_event_study/ma_thesis_pipeline.py

echo "== 6/6: narrative DOCX/PDF =="
$PY ma_event_study/build_research_story_report.py

echo "Готово. Основные артефакты: out/, итоговые копии: out/final/, эмпирика: out/thesis/."
