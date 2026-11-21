"""Expose fixture setup, read-only investigation, and operator commands."""

import argparse
import json
import os
import re
import sys
import time
from collections.abc import Callable
from pathlib import Path
from tempfile import mkdtemp
from typing import Any

from pyspark.sql import SparkSession

from pipeline_incident.agent import OpenAIPlanner, ScriptedPlanner, investigate
from pipeline_incident.deliveries import PipelineError, Workspace, stage
from pipeline_incident.evaluation import evaluate, golden, grade
from pipeline_incident.evidence import EvidenceReader
from pipeline_incident.fixtures import BASELINE, REPLAY, broken_contract
from pipeline_incident.operations import Operator, no_failure
from pipeline_incident.scenarios import run_scenario
from pipeline_incident.spark_engine import Engine, session, smoke

SCENARIOS = tuple(f"S{number:02}" for number in range(1, 10))


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Investigate and recover a fictional payment delivery."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("smoke")
    integration = commands.add_parser("integration")
    integration.add_argument("--scenario", choices=SCENARIOS)
    for name in ("demo", "eval", "investigate"):
        command = commands.add_parser(name)
        command.add_argument("--mode", choices=("scripted", "live"), default="scripted")
        if name == "demo":
            command.add_argument("--scenario", choices=SCENARIOS, default="S04")
    for name in (
        "stage",
        "project",
        "status",
        "investigate",
        "plan",
        "approve",
        "apply",
        "reconcile",
        "totals",
    ):
        command = commands.choices.get(name) or commands.add_parser(name)
        command.add_argument("--namespace", required=True)
        if name in {"stage", "project", "status", "investigate", "plan"}:
            command.add_argument("--delivery", required=True)
        if name == "stage":
            command.add_argument(
                "--fixture", choices=("baseline", "replay", "broken"), required=True
            )
        if name in {"approve", "apply"}:
            command.add_argument("--plan", required=True)
            command.add_argument("--digest", required=True)
        if name == "approve":
            command.add_argument("--actor", required=True)
        if name == "apply":
            command.add_argument("--pause-at", choices=("before-merge", "after-commit"))
        if name in {"apply", "project"}:
            command.add_argument("--fixture-mode", action="store_true")
        if name == "project":
            command.add_argument("--fail-before-merge", action="store_true")
    for command in commands.choices.values():
        command.add_argument(
            "--result-file", help="Write the JSON result to this filename under artifacts/."
        )
    args = parser.parse_args()
    if args.result_file and not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,95}\.json", args.result_file):
        parser.error("--result-file must be a lowercase JSON filename without a directory")
    if hasattr(args, "namespace") and not re.fullmatch(r"[a-z][a-z0-9-]{0,47}", args.namespace):
        parser.error(
            "--namespace must start with a lowercase letter and use lowercase letters, digits or hyphens"
        )
    injecting = getattr(args, "fail_before_merge", False) or getattr(args, "pause_at", None)
    if injecting and (not args.fixture_mode or not args.namespace.startswith("crash-")):
        parser.error("failure injection requires --fixture-mode and a crash- namespace")
    return args


def test_failpoint(args: argparse.Namespace) -> Callable[[str], None]:
    def failpoint(position: str) -> None:
        if getattr(args, "fail_before_merge", False) and position == "before-merge":
            raise PipelineError(
                "FIXTURE_PRE_COMMIT_FAILURE", "Stopped at the requested fixture boundary."
            )
        if position == getattr(args, "pause_at", None):
            print(f"PAUSED: {position}", file=sys.stderr, flush=True)
            time.sleep(90)
            raise PipelineError(
                "FIXTURE_PAUSE_EXPIRED", "The test did not kill the paused process."
            )

    return failpoint


def operator_command(args: argparse.Namespace, operator: Operator) -> dict[str, Any]:
    match args.command:
        case "stage":
            payloads = {"baseline": BASELINE, "replay": REPLAY, "broken": broken_contract()}
            stage(operator.workspace, args.delivery, payloads[args.fixture])
            manifest = operator.capture_delivery(args.delivery)
            return {
                "status": operator.delivery(args.delivery)["state"],
                "delivery_id": args.delivery,
                "manifest_digest": manifest.digest,
            }
        case "project":
            return operator.project(
                args.delivery, test_failpoint(args) if args.fixture_mode else no_failure
            )
        case "status":
            return operator.delivery(args.delivery)
        case "investigate":
            planner = OpenAIPlanner() if args.mode == "live" else ScriptedPlanner()
            reader = EvidenceReader(operator.workspace, operator.engine, args.delivery)
            return investigate(reader, planner)
        case "plan":
            return operator.create_plan(args.delivery)
        case "approve":
            return operator.approve(args.plan, args.digest, args.actor)
        case "apply":
            return operator.apply(
                args.plan, args.digest, test_failpoint(args) if args.fixture_mode else no_failure
            )
        case "reconcile":
            return {"operations": operator.reconcile()}
        case "totals":
            return operator.engine.totals()
    raise AssertionError(f"Unknown command {args.command}")


def run(args: argparse.Namespace, spark: SparkSession, home: Path, reports: Path) -> dict[str, Any]:
    if args.command == "smoke":
        return smoke(spark, Path(mkdtemp(prefix="smoke-", dir=home)) / "silver")
    if args.command == "integration":
        expected = golden()
        results = []
        for scenario in (args.scenario,) if args.scenario else SCENARIOS:
            result = run_scenario(spark, home, scenario, [ScriptedPlanner()])
            actual = result["after"]
            assert actual == {key: expected[scenario][key] for key in actual}, (scenario, actual)
            scores = grade(result["investigations"][0], expected[scenario])
            assert not scores.pop("unsafe_replay") and all(scores.values()), (scenario, scores)
            results.append({"scenario": scenario, "totals": actual, "status": "PASS"})
            print(f"{scenario} passed", file=sys.stderr, flush=True)
        return {"scenarios": results}
    if args.command in {"demo", "eval"}:
        reports.mkdir(parents=True, exist_ok=True)
        if args.command == "eval":
            return evaluate(spark, home, reports, live=args.mode == "live")
        planner = OpenAIPlanner() if args.mode == "live" else ScriptedPlanner()
        result = run_scenario(spark, home, args.scenario, [planner])
        directory = Path(mkdtemp(prefix=f"demo-{args.scenario.lower()}-", dir=reports))
        directory.chmod(0o755)
        report = directory / "report.json"
        report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return {**result, "report": str(report)}
    workspace = Workspace(home / args.namespace)
    return operator_command(args, Operator(workspace, Engine(spark, workspace)))


def main() -> int:
    args = arguments()
    home = Path(os.environ.get("PIPELINE_HOME", "/data")).resolve()
    reports = Path(os.environ.get("PIPELINE_REPORTS", "/reports")).resolve()
    home.mkdir(parents=True, exist_ok=True)
    spark = session()
    try:
        result = run(args, spark, home, reports)
    except PipelineError as error:
        print(f"{error.code}: {error}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    rendered = json.dumps(result, indent=2) + "\n"
    if args.result_file:
        reports.mkdir(parents=True, exist_ok=True)
        (reports / args.result_file).write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0
