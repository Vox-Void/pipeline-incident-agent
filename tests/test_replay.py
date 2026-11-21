from datetime import UTC, datetime, timedelta

import pytest

from pipeline_incident.deliveries import PipelineError, Workspace, delivery_path, stage
from pipeline_incident.fixtures import BASELINE, REPLAY, jsonl, payment
from pipeline_incident.ledger import ledger
from pipeline_incident.operations import Operator
from pipeline_incident.projection import Blocked, validate_delivery


class ReadSnapshot:
    version = 8

    def __init__(self, workspace):
        self.workspace = workspace
        self.accepted = []

    def bronze_lines(self, delivery_id):
        return delivery_path(self.workspace, delivery_id).read_text(encoding="utf-8").splitlines()

    def history(self, ids):
        return [record for record in self.accepted if record["payment_id"] in ids]

    def totals(self, additional):
        return {"preview_rows": len(additional)}


@pytest.fixture
def replay(tmp_path):
    workspace = Workspace(tmp_path)
    current_time = [datetime(2026, 1, 7, 10, tzinfo=UTC)]
    stage(workspace, "RECOVERY", REPLAY)
    with ledger(workspace.ledger) as connection, connection:
        connection.execute(
            "UPDATE deliveries SET state = 'FAILED_REPLAYABLE' WHERE id = 'RECOVERY'"
        )
    engine = ReadSnapshot(workspace)
    operator = Operator(workspace, engine, lambda: current_time[0])
    return workspace, engine, operator, current_time


def test_wrong_plan_and_approval_digests_cannot_write(replay):
    workspace, _, operator, _ = replay
    plan = operator.create_plan("RECOVERY")
    with pytest.raises(PipelineError) as different_plan:
        operator.approve(plan["plan_id"], "invalid", "test-operator")
    assert different_plan.value.code == "BAD_PLAN_DIGEST"

    operator.approve(plan["plan_id"], plan["digest"], "test-operator")
    with ledger(workspace.ledger) as connection, connection:
        connection.execute("UPDATE approvals SET digest = 'invalid'")
    with pytest.raises(PipelineError) as different_approval:
        operator.apply(plan["plan_id"], plan["digest"])
    assert different_approval.value.code == "APPROVAL_REQUIRED"


def test_apply_checks_approval_expiry_at_the_boundary(replay):
    _, _, operator, current_time = replay
    plan = operator.create_plan("RECOVERY")
    operator.approve(plan["plan_id"], plan["digest"], "test-operator")
    current_time[0] += timedelta(minutes=15)

    with pytest.raises(PipelineError) as expired:
        operator.apply(plan["plan_id"], plan["digest"])
    assert expired.value.code == "EXPIRED_PLAN"


def test_edited_sealed_file_cannot_be_applied(replay):
    workspace, _, operator, _ = replay
    plan = operator.create_plan("RECOVERY")
    operator.approve(plan["plan_id"], plan["digest"], "test-operator")
    delivery_path(workspace, "RECOVERY").write_bytes(BASELINE)

    with pytest.raises(PipelineError) as changed:
        operator.apply(plan["plan_id"], plan["digest"])
    assert changed.value.code == "STALE_PLAN"


def test_superseded_plan_cannot_be_reapproved(replay):
    _, _, operator, _ = replay
    first = operator.create_plan("RECOVERY")
    operator.create_plan("RECOVERY")

    with pytest.raises(PipelineError) as superseded:
        operator.approve(first["plan_id"], first["digest"], "test-operator")
    assert superseded.value.code == "STALE_PLAN"


def test_approved_recovery_with_only_duplicates_records_no_op(replay):
    workspace, engine, operator, _ = replay
    candidate = validate_delivery(workspace, engine, "RECOVERY")
    engine.accepted = [
        {
            "payment_id": item.payment_id,
            "revision": item.revision,
            "merchant_id": item.merchant_id,
            "payment_date": item.payment_date,
            "payload_hash": item.digest,
        }
        for item in candidate.rows
    ]
    plan = operator.create_plan("RECOVERY")
    assert plan["insert_count"] == 0
    assert plan["duplicate_count"] == 2
    operator.approve(plan["plan_id"], plan["digest"], "test-operator")
    outcome = operator.apply(plan["plan_id"], plan["digest"])
    assert outcome["status"] == "APPLIED_NO_OP"
    assert outcome["commit_version"] is None
    repeated = operator.apply(plan["plan_id"], plan["digest"])
    assert repeated["operation_id"] == outcome["operation_id"]


def test_conflicting_revision_within_a_delivery_blocks_the_whole_batch(tmp_path):
    workspace = Workspace(tmp_path)
    stage(workspace, "CONFLICT", jsonl([payment("P200", "10.00"), payment("P200", "9.00")]))

    with pytest.raises(Blocked) as conflict:
        validate_delivery(workspace, ReadSnapshot(workspace), "CONFLICT")
    assert conflict.value.code == "REVISION_CONFLICT"
