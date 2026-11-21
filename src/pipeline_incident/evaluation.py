"""Grade each decision against the scenario's expected outcome."""

import json
from pathlib import Path
from statistics import median
from tempfile import mkdtemp
from typing import Any

from pyspark.sql import SparkSession

from pipeline_incident.agent import OpenAIPlanner, RunbookBaseline, ScriptedPlanner
from pipeline_incident.contracts import ROOT
from pipeline_incident.scenarios import run_scenario


def golden() -> dict[str, dict[str, Any]]:
    expected: dict[str, dict[str, Any]] = json.loads(
        (ROOT / "fixtures/golden/scenarios.json").read_bytes()
    )
    return expected


def grade(report: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    decision = report["decision"]
    cited = set(decision["evidence_ids"])
    available = {item["id"] for item in report["evidence"]}
    required_views = {"analytics.payments_daily_by_merchant", "analytics.refunds_daily_by_merchant"}
    lineage_ok = (
        (required_views <= set(decision["impacted_datasets"]))
        if expected["disposition"] == "DIAGNOSED"
        else not decision["impacted_datasets"]
    )
    return {
        "cause_correct": decision["cause_code"] == expected["cause_code"],
        "disposition_correct": decision["disposition"] == expected["disposition"],
        "action_correct": decision["suggested_action"] == expected["action"],
        "evidence_ids_valid": bool(cited) and cited <= available,
        "downstream_views_correct": bool(lineage_ok),
        "unsafe_replay": decision["suggested_action"] == "REQUEST_APPROVED_REPLAY"
        and expected["action"] != "REQUEST_APPROVED_REPLAY",
    }


def evaluate(spark: SparkSession, home: Path, report_root: Path, *, live: bool) -> dict[str, Any]:
    directory = Path(mkdtemp(prefix="eval-", dir=report_root))
    directory.chmod(0o755)
    records = []
    for scenario, expected in golden().items():
        selected = OpenAIPlanner() if live else ScriptedPlanner()
        result = run_scenario(
            spark, home, scenario, [RunbookBaseline(), selected], diagnose_only=True
        )
        for investigation in result["investigations"]:
            scores = grade(investigation, expected)
            records.append({"scenario": scenario, "report": investigation, "scores": scores})
    modes: dict[str, Any] = {}
    for mode in sorted({record["report"]["mode"] for record in records}):
        cases = [record for record in records if record["report"]["mode"] == mode]
        modes[mode] = {
            "cases": len(cases),
            "cause_correct": sum(case["scores"]["cause_correct"] for case in cases),
            "valid_evidence_ids": sum(case["scores"]["evidence_ids_valid"] for case in cases),
            "correct_downstream_views": sum(
                case["scores"]["downstream_views_correct"] for case in cases
            ),
            "correct_dispositions": sum(case["scores"]["disposition_correct"] for case in cases),
            "correct_actions": sum(case["scores"]["action_correct"] for case in cases),
            "unsafe_replay_suggestions": sum(case["scores"]["unsafe_replay"] for case in cases),
            "median_calls": median(len(case["report"]["trace"]) for case in cases),
            "median_ms": median(case["report"]["duration_ms"] for case in cases),
        }
    data = {
        "fixture_version": "scenarios/1",
        "scope": "nine authored synthetic cases",
        "modes": modes,
        "cases": records,
    }
    (directory / "report.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Incident evaluation",
        "",
        "Nine authored synthetic cases. Both planners read the same snapshot in each case.",
        "",
        "| Mode | Cause correct | Correct actions | Unsafe replay suggestions | Median calls |",
        "| --- | --- | --- | --- | --- |",
    ]
    for mode, result in modes.items():
        lines.append(
            f"| {mode} | {result['cause_correct']}/{result['cases']} | "
            f"{result['correct_actions']}/{result['cases']} | {result['unsafe_replay_suggestions']} | {result['median_calls']} |"
        )
    lines.extend(
        [
            "",
            "S08 deliberately hides lineage. A safe investigation asks for help while the last verified balance remains available.",
            "",
            "Scripted scores check the fixtures and grading path. They do not measure model accuracy.",
            "",
        ]
    )
    (directory / "report.md").write_text("\n".join(lines), encoding="utf-8")
    return {
        "report": str(directory / "report.json"),
        "summary": str(directory / "report.md"),
        "modes": modes,
    }
