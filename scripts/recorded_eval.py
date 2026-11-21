"""Run a capped live comparison against evidence saved by the Spark evaluation."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline_incident.agent import OpenAIPlanner, RunbookBaseline, investigate
from pipeline_incident.contracts import ROOT
from pipeline_incident.deliveries import PipelineError
from pipeline_incident.evaluation import golden, grade
from pipeline_incident.evidence import TOOLS, Evidence


class RecordedEvidence:
    def __init__(self, report: dict[str, Any]):
        self.delivery_id: str = report["delivery_id"]
        self.successes = {}
        for item in report["evidence"]:
            evidence = Evidence(item["tool"], item["target"], item["data"])
            if evidence.id != item["id"] or evidence.tool not in TOOLS:
                raise ValueError("Saved evidence did not match its recorded ID or tool.")
            self.successes[(evidence.tool, evidence.target)] = evidence
        self.failures = {
            item["tool"]: item["status"] for item in report["trace"] if item["status"] != "OK"
        }

    def dispatch(self, tool: str, target: str) -> Evidence:
        if tool not in TOOLS:
            raise PipelineError("UNKNOWN_TOOL", "The adapter exposes read-only tools.")
        result = self.successes.get((tool, target))
        if result is None:
            raise PipelineError(
                self.failures.get(tool, "EVIDENCE_NOT_RECORDED"),
                "Evidence unavailable in the recorded case.",
            )
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--cases", nargs="+", required=True)
    args = parser.parse_args()
    if not 1 <= len(set(args.cases)) == len(args.cases) <= 3:
        parser.error("Choose one to three unique synthetic case IDs.")
    snapshot = json.loads(args.report.read_bytes())
    captured = {
        row["scenario"]: row["report"]
        for row in snapshot["cases"]
        if row["report"]["mode"] == "scripted"
    }
    expected = golden()
    expected.update(json.loads((ROOT / "fixtures/golden/challenge.json").read_bytes()))
    if set(args.cases) - (captured.keys() & expected.keys()):
        parser.error(
            "The requested cases must appear in the scripted Spark evaluation and committed goldens."
        )
    rows = []
    for case_id in args.cases:
        source = RecordedEvidence(captured[case_id])
        for planner in (RunbookBaseline(), OpenAIPlanner()):
            run = investigate(source, planner)
            rows.append(
                {"scenario": case_id, "report": run, "scores": grade(run, expected[case_id])}
            )
            if run["provider_error"] in {"HTTP_401", "HTTP_403", "HTTP_404", "HTTP_429"}:
                break
        if rows[-1]["report"]["provider_error"] in {"HTTP_401", "HTTP_403", "HTTP_404", "HTTP_429"}:
            break
    result = {
        "run_at": datetime.now(UTC).isoformat(),
        "fixture_version": snapshot["fixture_version"],
        "evidence_source": "recorded from real Spark/Delta evaluation",
        "requested_cases": args.cases,
        "cases": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
