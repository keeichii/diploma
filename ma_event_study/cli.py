from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

from .config import Config, load_config
from .runner import run_build


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build MA event-study tables from ma_deals.xlsx")
    p.add_argument("--config", default=str(Path(__file__).with_name("config.toml")))
    return p


def main(argv: list[str]) -> int:
    args = _build_arg_parser().parse_args(argv)
    try:
        cfg: Config = load_config(Path(args.config))
        run_build(cfg)
        return 0
    except Exception as e:
        print(f"[FATAL] {type(e).__name__}: {e}", file=sys.stderr)
        traceback.print_exc()
        return 1

