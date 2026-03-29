from __future__ import annotations

import argparse
import logging
import sys
import traceback
from pathlib import Path

from .config import Config, load_config
from .runner import run_build

logger = logging.getLogger("ma_event_study")


def _post_enrich_analysis() -> None:
    from . import cross_sectional_analysis
    from . import group_tests
    from . import hypotheses_mapper

    cross_sectional_analysis.run()
    group_tests.run()
    hypotheses_mapper.run()


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="M&A event-study: full build (full) or partial steps (car, intraday, enrich, analyze, report)."
    )
    p.add_argument(
        "command",
        nargs="?",
        default="full",
        choices=(
            "full",
            "build",
            "car",
            "intraday",
            "enrich",
            "analyze",
            "thesis",
            "post_enrich",
            "report",
            "trace",
        ),
        help="full = build + car + intraday + thesis(enrich_deals) + HC3/group_tests/hypotheses; "
        "enrich = ma_thesis_pipeline + BHAR merge + post_enrich; "
        "thesis same as enrich; "
        "post_enrich = cross_sectional + group_tests + hypotheses only",
    )
    p.add_argument("--config", default=str(Path(__file__).with_name("config.toml")))
    p.add_argument("--skip-build", action="store_true", help="For 'full': не вызывать run_build (только аналитика по уже собранным out/).")
    p.add_argument("--no-report", action="store_true", help="For 'full': не собирать narrative docx/pdf.")
    p.add_argument("--deal", type=int, default=None, metavar="N", help="Для trace: source_row_excel (номер строки в Excel).")
    return p


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _build_arg_parser().parse_args(argv)
    try:
        if args.command == "build":
            cfg: Config = load_config(Path(args.config))
            run_build(cfg)
            return 0

        if args.command == "car":
            from . import car_event_study_analysis

            car_event_study_analysis.run()
            return 0

        if args.command == "intraday":
            from . import intraday_event_study_analysis

            intraday_event_study_analysis.run()
            return 0

        if args.command in ("enrich", "thesis"):
            from . import enrich_deals

            enrich_deals.run()
            return 0

        if args.command == "post_enrich":
            _post_enrich_analysis()
            return 0

        if args.command == "analyze":
            _post_enrich_analysis()
            return 0

        if args.command == "report":
            from . import build_research_story_report

            build_research_story_report.main()
            return 0

        if args.command == "trace":
            from .deal_trace import trace_deal

            if args.deal is None:
                print("trace: укажите --deal <source_row_excel>", file=sys.stderr)
                return 2
            p = trace_deal(args.deal)
            print(p)
            return 0

        # full
        if not args.skip_build:
            cfg = load_config(Path(args.config))
            run_build(cfg)

        from . import car_event_study_analysis
        from . import enrich_deals
        from . import intraday_event_study_analysis

        car_event_study_analysis.run()
        intraday_event_study_analysis.run()
        enrich_deals.run()
        # Доп. артефакты из Codex2 (документация полей + сравнение create vs announcement)
        try:
            from . import refresh_intraday_extension_docs

            refresh_intraday_extension_docs.run()
        except Exception as e:
            print(f"WARNING: refresh_intraday_extension_docs failed: {type(e).__name__}: {e}")
        try:
            from . import build_create_vs_announcement_analysis

            build_create_vs_announcement_analysis.main()
        except Exception as e:
            print(f"WARNING: build_create_vs_announcement_analysis failed: {type(e).__name__}: {e}")
        if not args.no_report:
            from . import build_research_story_report

            build_research_story_report.main()
        return 0
    except Exception as e:
        logger.exception("%s: %s", type(e).__name__, e)
        print(f"[FATAL] {type(e).__name__}: {e}", file=sys.stderr)
        traceback.print_exc()
        return 1

