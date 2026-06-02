#!/usr/bin/env python3
"""Run locked model-validation grids and summarize production-readiness signals.

The runner intentionally shells out to ``model_validation_report.py`` so each
case uses the same read-only validation path as manual model iteration.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence


@dataclass(frozen=True)
class ValidationCase:
    """One locked validation scenario in the iteration grid."""

    name: str
    start_date: str
    end_date: str
    training_window_days: int
    evaluation_window_days: int
    step_days: int
    horizon_days: int
    positive_threshold_pct: float
    min_training_rows: int = 500
    min_class_count: int = 80
    iterations: int = 300
    top_k: int = 10


CORE_CASES: tuple[ValidationCase, ...] = (
    ValidationCase("recent_h5_direction", "2024-01-01", "2026-02-28", 720, 90, 90, 5, 0.0),
    ValidationCase("recent_h5_hurdle_1p", "2024-01-01", "2026-02-28", 720, 90, 90, 5, 1.0),
    ValidationCase("recent_h10_direction", "2024-01-01", "2026-02-28", 720, 90, 90, 10, 0.0),
    ValidationCase("recent_h10_hurdle_2p", "2024-01-01", "2026-02-28", 720, 90, 90, 10, 2.0),
    ValidationCase("recent_h20_direction", "2024-01-01", "2026-02-28", 720, 90, 90, 20, 0.0),
    ValidationCase("recent_h20_hurdle_3p", "2024-01-01", "2026-02-28", 720, 90, 90, 20, 3.0),
    ValidationCase("recent_h60_direction", "2024-01-01", "2026-02-28", 720, 90, 90, 60, 0.0),
    ValidationCase("recent_h60_hurdle_5p", "2024-01-01", "2026-02-28", 720, 90, 90, 60, 5.0),
)


REGIME_CASES: tuple[ValidationCase, ...] = (
    ValidationCase("early_h5_hurdle_1p", "2018-01-01", "2020-12-31", 720, 120, 120, 5, 1.0),
    ValidationCase("early_h10_hurdle_2p", "2018-01-01", "2020-12-31", 720, 120, 120, 10, 2.0),
    ValidationCase("early_h20_hurdle_3p", "2018-01-01", "2020-12-31", 720, 120, 120, 20, 3.0),
    ValidationCase("mid_h5_hurdle_1p", "2021-01-01", "2023-12-31", 720, 120, 120, 5, 1.0),
    ValidationCase("mid_h10_hurdle_2p", "2021-01-01", "2023-12-31", 720, 120, 120, 10, 2.0),
    ValidationCase("mid_h20_hurdle_3p", "2021-01-01", "2023-12-31", 720, 120, 120, 20, 3.0),
)


PRESETS = {
    "core": CORE_CASES,
    "regime": REGIME_CASES,
    "all": CORE_CASES + REGIME_CASES,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run fixed model-validation grids and summarize results."
    )
    parser.add_argument(
        "--preset",
        choices=sorted(PRESETS),
        default="core",
        help="Locked validation case set to run.",
    )
    parser.add_argument(
        "--output-dir",
        default="/tmp/model_iteration_grid",
        help="Directory for per-case reports and summary JSON.",
    )
    parser.add_argument(
        "--stocks",
        default="",
        help="Optional comma-separated stock codes for a faster subset run.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Reuse existing case JSON files instead of rerunning them.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    case_summaries = []
    for case in PRESETS[args.preset]:
        report_path = output_dir / f"{case.name}.json"
        if not args.skip_existing or not report_path.exists():
            _run_case(case, report_path=report_path, stocks=args.stocks)
        report = _load_json(report_path)
        case_summaries.append(_summarize_case(case, report_path, report))

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "preset": args.preset,
        "stocks": args.stocks or "ALL",
        "case_count": len(case_summaries),
        "readiness": _build_readiness_summary(case_summaries),
        "cases": case_summaries,
    }
    summary_path = output_dir / f"{args.preset}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary["readiness"], indent=2, sort_keys=True))
    print(f"summary_path={summary_path}")


def _run_case(case: ValidationCase, *, report_path: Path, stocks: str) -> None:
    script_path = Path(__file__).with_name("model_validation_report.py")
    cmd = [
        sys.executable,
        str(script_path),
        "--start-date",
        case.start_date,
        "--end-date",
        case.end_date,
        "--training-window-days",
        str(case.training_window_days),
        "--evaluation-window-days",
        str(case.evaluation_window_days),
        "--step-days",
        str(case.step_days),
        "--horizon-days",
        str(case.horizon_days),
        "--positive-threshold-pct",
        str(case.positive_threshold_pct),
        "--min-training-rows",
        str(case.min_training_rows),
        "--min-class-count",
        str(case.min_class_count),
        "--iterations",
        str(case.iterations),
        "--top-k",
        str(case.top_k),
        "--output",
        str(report_path),
    ]
    if stocks:
        cmd.extend(["--stocks", stocks])
    subprocess.run(cmd, check=True)


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _summarize_case(case: ValidationCase, report_path: Path, report: dict) -> dict:
    walk_forward = report["walk_forward"]
    thresholds = walk_forward["threshold_candidates"]
    p50 = _threshold(thresholds, 0.5)
    best_net = max(
        thresholds,
        key=lambda item: (item.get("average_net_return_pct", 0.0), item["row_count"]),
    )
    baseline = walk_forward["baseline"]
    folds = walk_forward["fold_count"]
    skipped = walk_forward["skipped_folds"]
    top_k_return = walk_forward["overall_top_k_average_forward_return_10d"]
    baseline_top_k_return = baseline["top_k_average_forward_return_10d"]
    p50_net_return = p50.get("average_net_return_pct", 0.0)

    return {
        "case": asdict(case),
        "report_path": str(report_path),
        "fold_count": folds,
        "skipped_folds": skipped,
        "total_evaluated_rows": walk_forward["total_evaluated_rows"],
        "overall_hit_rate": walk_forward["overall_hit_rate"],
        "overall_brier_score": walk_forward["overall_brier_score"],
        "overall_average_return_pct": walk_forward["overall_average_forward_return_10d"],
        "top_k_average_return_pct": top_k_return,
        "baseline_top_k_average_return_pct": baseline_top_k_return,
        "top_k_beats_baseline": top_k_return > baseline_top_k_return,
        "p50": p50,
        "best_net_threshold": best_net,
        "passes_minimum_data": folds >= 6 and skipped == 0 and walk_forward["total_evaluated_rows"] >= 5000,
        "passes_probability_gate": p50["row_count"] >= 100 and p50_net_return > 0.0,
        "passes_ranking": top_k_return > 0.0 and top_k_return >= baseline_top_k_return,
    }


def _threshold(thresholds: Sequence[dict], target: float) -> dict:
    for item in thresholds:
        if float(item["threshold"]) == float(target):
            return item
    raise ValueError(f"Missing probability threshold {target}")


def _build_readiness_summary(case_summaries: Iterable[dict]) -> dict:
    cases = list(case_summaries)
    if not cases:
        return {"status": "BLOCKED", "reason": "no_cases"}

    minimum_data_passes = sum(1 for case in cases if case["passes_minimum_data"])
    probability_gate_passes = sum(1 for case in cases if case["passes_probability_gate"])
    ranking_passes = sum(1 for case in cases if case["passes_ranking"])
    robust_case_count = sum(
        1
        for case in cases
        if case["passes_minimum_data"]
        and case["passes_probability_gate"]
        and case["passes_ranking"]
    )
    status = "PASS" if robust_case_count >= max(2, len(cases) // 2) else "FAIL"
    return {
        "status": status,
        "case_count": len(cases),
        "minimum_data_passes": minimum_data_passes,
        "probability_gate_passes": probability_gate_passes,
        "ranking_passes": ranking_passes,
        "robust_case_count": robust_case_count,
        "required_robust_cases": max(2, len(cases) // 2),
    }


if __name__ == "__main__":
    main()
