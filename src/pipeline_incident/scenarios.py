"""Run the authored incidents against real local Bronze and Silver tables."""

from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from tempfile import mkdtemp
from typing import Any

from pyspark.sql import SparkSession

from pipeline_incident.agent import Planner, investigate
from pipeline_incident.deliveries import PipelineError, Workspace, stage
from pipeline_incident.evidence import EvidenceReader
from pipeline_incident.fixtures import BASELINE, REPLAY, broken_contract, jsonl, payment
from pipeline_incident.operations import Operator
from pipeline_incident.spark_engine import Engine

FIXTURE_TIME = datetime(2026, 1, 7, 10, 0, tzinfo=UTC)


def fixed_clock() -> datetime:
    return FIXTURE_TIME


class SimulatedFailure(RuntimeError):
    pass


def inject_failure(target: str, position: str) -> None:
    if position == target:
        raise SimulatedFailure(position)


def deliver(operator: Operator, delivery_id: str, payload: bytes) -> dict[str, Any]:
    stage(operator.workspace, delivery_id, payload)
    operator.capture()
    return operator.project(delivery_id)


def expect_pipeline_error(code: str, call: Any, *args: Any) -> dict[str, str]:
    try:
        call(*args)
    except PipelineError as error:
        if error.code != code:
            raise
        return {"status": "REFUSED", "reason": code}
    raise AssertionError(f"Expected {code}")


def replay_fixture(operator: Operator, delivery_id: str = "RECOVERY") -> dict[str, Any]:
    stage(operator.workspace, delivery_id, REPLAY)
    operator.capture()
    try:
        operator.project(delivery_id, lambda phase: inject_failure("before-merge", phase))
    except SimulatedFailure:
        pass
    else:
        raise AssertionError("The pre-commit fixture did not stop the write.")
    return operator.reconcile()[0]


def run_scenario(
    spark: SparkSession,
    home: Path,
    scenario: str,
    planners: list[Planner],
    *,
    diagnose_only: bool = False,
) -> dict[str, Any]:
    workspace = Workspace(Path(mkdtemp(prefix=f"{scenario.lower()}-", dir=home)))
    engine = Engine(spark, workspace)
    operator = Operator(workspace, engine, fixed_clock)
    baseline = deliver(operator, "BASELINE", BASELINE)
    delivery_id = "BASELINE"
    steps: list[dict[str, Any]] = [{"step": "baseline", **baseline}]

    if scenario == "S01":
        operator.capture()
        assert len(engine.bronze_lines("BASELINE")) == 3
        assert operator.project("BASELINE")["repeated"]
        steps.append({"step": "checkpoint restart", "bronze_rows": 3})
    elif scenario == "S02":
        updates = {
            "DUPLICATE": jsonl([payment("P100", "10.00")]),
            "REFUND": jsonl([payment("P200", "25.50", revision=2, status="REFUNDED")]),
            "NEWER": jsonl([payment("P400", "12.75", revision=2)]),
            "LATE": jsonl([payment("P400", "11.75")]),
        }
        for delivery_id, payload in updates.items():
            stage(workspace, delivery_id, payload)
        operator.capture()
        for delivery_id in updates:
            operator.project(delivery_id)
        assert sum(len(engine.bronze_lines(name)) for name in ["BASELINE", *updates]) == 7
    elif scenario == "S03":
        delivery_id = "BROKEN"
        steps.append(
            expect_pipeline_error(
                "CONTRACT_MISMATCH", deliver, operator, delivery_id, broken_contract()
            )
        )
        assert len(engine.bronze_lines(delivery_id)) == 2
    elif scenario in {"S04", "S05", "S06", "S08"}:
        delivery_id = "RECOVERY"
        steps.append({"step": "reconcile failed write", **replay_fixture(operator, delivery_id)})
    elif scenario == "S07":
        delivery_id = "CONFLICT"
        conflicted = jsonl([payment("P200", "99.00"), payment("P900", "3.00")])
        steps.append(
            expect_pipeline_error("REVISION_CONFLICT", deliver, operator, delivery_id, conflicted)
        )
    elif scenario == "S09":
        steps.append(
            deliver(
                operator, "ADDITIVE", jsonl([payment("P950", "5.00", version=2, channel="APP")])
            )
        )
        delivery_id = "UNSUPPORTED"
        steps.append(
            expect_pipeline_error(
                "UNSUPPORTED_VERSION",
                deliver,
                operator,
                delivery_id,
                jsonl([payment("P960", "1.00", version=3)]),
            )
        )
    else:
        raise ValueError(f"Unknown scenario {scenario}")

    before = engine.totals()
    if scenario in {"S03", "S04", "S05", "S06", "S07", "S08"}:
        assert before == {
            "history_rows": 3,
            "current_payments": 3,
            "captured": "39.75",
            "refunded": "0.00",
        }
    reports = [
        investigate(
            EvidenceReader(workspace, engine, delivery_id, lineage_available=scenario != "S08"),
            planner,
        )
        for planner in planners
    ]
    if scenario == "S03" and not diagnose_only:
        steps.append(
            deliver(
                operator, "CORRECTED", jsonl([payment("P500", "90.00"), payment("P510", "2.00")])
            )
        )
    if scenario in {"S04", "S05", "S06"} and not diagnose_only:
        plan = operator.create_plan(delivery_id)
        steps.append({"step": "plan", **plan})
        if scenario == "S04":
            steps.append(
                expect_pipeline_error(
                    "APPROVAL_REQUIRED", operator.apply, plan["plan_id"], plan["digest"]
                )
            )
        steps.append(
            {
                "step": "fixture approval",
                **operator.approve(plan["plan_id"], plan["digest"], "fixture-operator"),
            }
        )
        if scenario == "S05":
            steps.append(deliver(operator, "ADDITIONAL", jsonl([payment("P800", "1.25")])))
            steps.append(
                expect_pipeline_error("STALE_PLAN", operator.apply, plan["plan_id"], plan["digest"])
            )
            assert engine.totals()["captured"] == "41.00"
            plan = operator.create_plan(delivery_id)
            steps.append(
                {
                    "step": "fixture reapproval",
                    **operator.approve(plan["plan_id"], plan["digest"], "fixture-operator"),
                }
            )
        version_before = engine.version
        if scenario == "S06":
            # The separate container test kills the writer at these same boundaries.
            for point in ("before-merge", "after-commit"):
                try:
                    operator.apply(plan["plan_id"], plan["digest"], partial(inject_failure, point))
                except SimulatedFailure:
                    steps.extend(operator.reconcile())
                else:
                    raise AssertionError(f"{point} did not stop the fixture")
        else:
            steps.append(operator.apply(plan["plan_id"], plan["digest"]))
        repeat = operator.apply(plan["plan_id"], plan["digest"])
        assert repeat["repeated"] and engine.version == version_before + 1
        steps.append(repeat)
    after = engine.totals()
    if scenario == "S02":
        latest = (
            spark.table("payment_current").where("payment_id = 'P400'").select("revision").first()
        )
        assert latest is not None and latest[0] == 2
    return {
        "scenario": scenario,
        "workspace": str(workspace.root),
        "before": before,
        "after": after,
        "steps": steps,
        "investigations": reports,
    }
