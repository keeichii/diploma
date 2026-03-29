from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tomllib  # py311+
except Exception:  # pragma: no cover
    import tomli as tomllib  # type: ignore


@dataclass(frozen=True)
class Config:
    input_xlsx: Path
    output_dir: Path
    financials_dir: Path | None
    benchmarks_imoex: Path | None
    benchmarks_ruonia: Path | None
    token: str
    sheet_name: str | None
    autosave_every: int
    daily_window: int
    daily_window_pre: int
    daily_window_post: int
    ignore_unresolved_rows: bool
    cache_dir: Path
    adjusted_close_allow_close_fallback: bool
    market_cap_shares_variant: str


def _load_toml(path: Path) -> dict[str, Any]:
    with open(path, "rb") as f:
        return tomllib.load(f)


def _abs_path(base_dir: Path, raw: str | None) -> Path | None:
    if not raw:
        return None
    p = Path(str(raw)).expanduser()
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return p


def load_config(path: Path) -> Config:
    path = path.expanduser().resolve()
    raw = _load_toml(path)

    run = raw.get("run", {})
    token_env_or_value = str(run.get("token_env", "INVEST_TOKEN")).strip()
    if token_env_or_value.startswith("t."):
        token = token_env_or_value
    else:
        token = os.environ.get(token_env_or_value, "").strip()
        if not token:
            raise EnvironmentError(f"Не найдена переменная окружения {token_env_or_value}")

    input_xlsx = _abs_path(path.parent, raw.get("input", {}).get("xlsx"))
    if input_xlsx is None:
        raise ValueError("Не задан input.xlsx в config")

    # Если пользователь дал путь как ../data/input/ma_deals.xlsx, а config лежит в ma_event_study/,
    # это нормально. Дополнительно поддержим удобный короткий путь data/input/ma_deals.xlsx.
    if not input_xlsx.exists():
        alt = _abs_path(path.parent.parent, raw.get("input", {}).get("xlsx"))
        if alt and alt.exists():
            input_xlsx = alt

    output_dir = _abs_path(path.parent, raw.get("output", {}).get("dir", "out"))
    if output_dir is None:
        raise ValueError("Не задан output.dir в config")

    financials_dir = _abs_path(path.parent, raw.get("financials", {}).get("dir"))
    benchmarks_imoex = _abs_path(path.parent, raw.get("benchmarks", {}).get("imoex"))
    benchmarks_ruonia = _abs_path(path.parent, raw.get("benchmarks", {}).get("ruonia"))

    if financials_dir and not financials_dir.exists():
        alt = _abs_path(path.parent.parent, raw.get("financials", {}).get("dir"))
        if alt and alt.exists():
            financials_dir = alt

    if benchmarks_imoex and not benchmarks_imoex.exists():
        alt = _abs_path(path.parent.parent, raw.get("benchmarks", {}).get("imoex"))
        if alt and alt.exists():
            benchmarks_imoex = alt

    if benchmarks_ruonia and not benchmarks_ruonia.exists():
        alt = _abs_path(path.parent.parent, raw.get("benchmarks", {}).get("ruonia"))
        if alt and alt.exists():
            benchmarks_ruonia = alt

    raw_cache_dir = raw.get("cache", {}).get("dir", "../data/cache")
    cache_dir = _abs_path(path.parent, raw_cache_dir) or (path.parent / "../data/cache").resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    def _parse_bool(x: Any) -> bool:
        if isinstance(x, bool):
            return x
        if x is None:
            return False
        s = str(x).strip().lower()
        return s in {"1", "true", "yes", "y", "on"}

    adjusted_close_allow_close_fallback = _parse_bool(
        raw.get("cache", {}).get("adjusted_close_allow_close_fallback", False)
    )
    market_cap_shares_variant = str(raw.get("cache", {}).get("market_cap_shares_variant", "auto")).strip()


    dw = int(run.get("daily_window", 250))
    dwp = int(run.get("daily_window_pre", dw))
    dwpost = int(run.get("daily_window_post", 60))

    return Config(
        input_xlsx=input_xlsx,
        output_dir=output_dir,
        financials_dir=financials_dir,
        benchmarks_imoex=benchmarks_imoex,
        benchmarks_ruonia=benchmarks_ruonia,
        token=token,
        sheet_name=raw.get("input", {}).get("sheet_name"),
        autosave_every=int(run.get("autosave_every", 25)),
        daily_window=dw,
        daily_window_pre=dwp,
        daily_window_post=dwpost,
        ignore_unresolved_rows=_parse_bool(run.get("ignore_unresolved_rows", False)),
        cache_dir=cache_dir,
        adjusted_close_allow_close_fallback=adjusted_close_allow_close_fallback,
        market_cap_shares_variant=market_cap_shares_variant,
    )

