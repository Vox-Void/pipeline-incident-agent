"""Keep approval and execution in the operator process."""

import json
import re
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from pipeline_incident.contracts import CONTRACT_VERSION, LINEAGE_VERSION, canonical, sha256
from pipeline_incident.deliveries import PipelineError, Workspace
from pipeline_incident.ledger import as_json, ledger, row, rows
from pipeline_incident.projection import Blocked, Candidate, validate_delivery
from pipeline_incident.spark_engine import Engine

Clock = Callable[[], datetime]
Failpoint = Callable[[str], None]


def now() -> datetime:
    return datetime.now(UTC)


def no_failure(name: str) -> None:
    pass


class Operator:
    def __init__(self, workspace: Workspace, engine: Engine, clock: Clock = now):
        self.workspace = workspace
        self.engine = engine
        self.clock = clock

    def delivery(self, delivery_id: str) -> dict[str, Any]:
        with ledger(self.workspace.ledger, read_only=True) as connection:
            result = row(connection, "SELECT * FROM deliveries WHERE id = ?", (delivery_id,))
        if result is None:
            raise PipelineError("UNKNOWN_DELIVERY", "No sealed delivery with that ID.")
        return result

    def capture(self) -> None:
        self.workspace.prepare()
        self.engine.capture()
        self.engine.ensure_silver()

    def project(self, delivery_id: str, failpoint: Failpoint = no_failure) -> dict[str, Any]:
        self.reconcile()
        delivery = self.delivery(delivery_id)
        if delivery["state"] in {"FAILED_REPLAYABLE", "NEEDS_RECONCILIATION", "BLOCKED"}:
            raise PipelineError(
                "DELIVERY_INELIGIBLE", "Inspect the delivery before taking another action."
            )
        if delivery["state"] in {"PROJECTED", "APPLIED", "APPLIED_NO_OP"}:
            with ledger(self.workspace.ledger, read_only=True) as connection:
                recorded = row(
                    connection,
                    "SELECT * FROM attempts WHERE delivery_id = ? ORDER BY rowid DESC LIMIT 1",
                    (delivery_id,),
                )
            return {
                "status": delivery["state"],
                "delivery_id": delivery_id,
                "operation_id": recorded and recorded["id"],
                "repeated": True,
            }
        try:
            candidate = validate_delivery(self.workspace, self.engine, delivery_id)
        except Blocked as error:
            self.block(delivery_id, error)
            raise
        with ledger(self.workspace.ledger) as connection, connection:
            connection.execute(
                "UPDATE deliveries SET state = 'CAPTURED' WHERE id = ?", (delivery_id,)
            )
        return self.execute(candidate, None, failpoint)

    def block(self, delivery_id: str, error: Blocked) -> None:
        with ledger(self.workspace.ledger) as connection, connection:
            connection.execute(
                "UPDATE deliveries SET state = 'BLOCKED', reason = ?, findings = ? WHERE id = ?",
                (error.code, as_json([asdict(finding) for finding in error.findings]), delivery_id),
            )
            connection.execute(
                """INSERT INTO attempts
                   (id, delivery_id, state, base_version, expected, metadata, started_at, ended_at, reason)
                   VALUES (?, ?, 'BLOCKED', ?, '[]', '', ?, ?, ?)""",
                (
                    uuid4().hex,
                    delivery_id,
                    self.engine.version,
                    self.clock().isoformat(),
                    self.clock().isoformat(),
                    error.code,
                ),
            )

    def create_plan(self, delivery_id: str) -> dict[str, Any]:
        self.reconcile()
        if self.delivery(delivery_id)["state"] != "FAILED_REPLAYABLE":
            raise PipelineError(
                "DELIVERY_INELIGIBLE", "Only confirmed replayable deliveries can be planned."
            )
        candidate = validate_delivery(self.workspace, self.engine, delivery_id)
        body = {
            "schema_version": 1,
            "plan_id": uuid4().hex,
            "delivery_id": delivery_id,
            "manifest_digest": candidate.manifest.digest,
            "contract_version": CONTRACT_VERSION,
            "lineage_version": LINEAGE_VERSION,
            "silver_version": self.engine.version,
            "candidate_digest": candidate.digest,
            "insert_count": len(candidate.rows),
            "duplicate_count": candidate.duplicate_count,
            "metrics_preview": self.engine.totals(candidate.rows),
            "expires_at": (self.clock() + timedelta(minutes=15)).isoformat(),
        }
        digest = sha256(canonical(body))
        with ledger(self.workspace.ledger) as connection, connection:
            connection.execute(
                "UPDATE plans SET superseded = 1 WHERE delivery_id = ? AND outcome IS NULL",
                (delivery_id,),
            )
            connection.execute(
                "INSERT INTO plans (id, delivery_id, digest, body) VALUES (?, ?, ?, ?)",
                (body["plan_id"], delivery_id, digest, as_json(body)),
            )
        return {**body, "digest": digest}

    def load_plan(self, plan_id: str, digest: str) -> tuple[dict[str, Any], dict[str, Any]]:
        with ledger(self.workspace.ledger, read_only=True) as connection:
            record = row(connection, "SELECT * FROM plans WHERE id = ?", (plan_id,))
        if record is None:
            raise PipelineError("UNKNOWN_PLAN", "No replay plan with that ID.")
        body: dict[str, Any] = json.loads(record["body"])
        if digest != record["digest"] or digest != sha256(canonical(body)):
            raise PipelineError(
                "BAD_PLAN_DIGEST", "The supplied digest must match the stored plan."
            )
        if record["superseded"]:
            raise PipelineError("STALE_PLAN", "A newer plan superseded this plan.")
        return record, body

    def require_unexpired(self, body: dict[str, Any]) -> None:
        if self.clock() >= datetime.fromisoformat(body["expires_at"]):
            raise PipelineError("EXPIRED_PLAN", "The replay plan has expired.")

    def approve(self, plan_id: str, digest: str, actor: str) -> dict[str, str]:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,39}", actor):
            raise PipelineError("INVALID_ACTOR", "Use a short local operator ID.")
        record, body = self.load_plan(plan_id, digest)
        self.require_unexpired(body)
        if record["outcome"]:
            raise PipelineError("COMPLETED_PLAN", "The replay plan already finished.")
        with ledger(self.workspace.ledger) as connection, connection:
            connection.execute(
                "INSERT OR IGNORE INTO approvals (plan_id, digest, actor, approved_at) VALUES (?, ?, ?, ?)",
                (plan_id, digest, actor, self.clock().isoformat()),
            )
            result = row(
                connection, "SELECT actor, digest FROM approvals WHERE plan_id = ?", (plan_id,)
            )
        if result is None or result["digest"] != digest:
            raise PipelineError("BAD_APPROVAL", "The stored approval does not match this plan.")
        return {
            "status": "APPROVED",
            "plan_id": plan_id,
            "actor": str(result["actor"]),
            "digest": digest,
        }

    def apply(self, plan_id: str, digest: str, failpoint: Failpoint = no_failure) -> dict[str, Any]:
        self.reconcile()
        record, body = self.load_plan(plan_id, digest)
        if record["outcome"]:
            return {
                "status": record["outcome"],
                "plan_id": plan_id,
                "operation_id": record["operation_id"],
                "repeated": True,
            }
        with ledger(self.workspace.ledger, read_only=True) as connection:
            approval = row(connection, "SELECT digest FROM approvals WHERE plan_id = ?", (plan_id,))
        if approval is None or approval["digest"] != digest:
            raise PipelineError(
                "APPROVAL_REQUIRED", "Approve this exact replay plan before applying it."
            )
        self.require_unexpired(body)
        if (
            self.delivery(body["delivery_id"])["state"] != "FAILED_REPLAYABLE"
            or self.engine.version != body["silver_version"]
            or body["contract_version"] != CONTRACT_VERSION
            or body["lineage_version"] != LINEAGE_VERSION
        ):
            raise PipelineError(
                "STALE_PLAN", "The Silver snapshot or contract no longer matches the plan."
            )
        try:
            candidate = validate_delivery(self.workspace, self.engine, body["delivery_id"])
        except Blocked as error:
            raise PipelineError("STALE_PLAN", "The sealed input no longer validates.") from error
        if (
            candidate.manifest.digest != body["manifest_digest"]
            or candidate.digest != body["candidate_digest"]
            or len(candidate.rows) != body["insert_count"]
            or candidate.duplicate_count != body["duplicate_count"]
        ):
            raise PipelineError(
                "STALE_PLAN", "The validated replay input no longer matches the plan."
            )
        return self.execute(candidate, record, failpoint)

    def execute(
        self, candidate: Candidate, plan: dict[str, Any] | None, failpoint: Failpoint
    ) -> dict[str, Any]:
        operation_id = uuid4().hex
        delivery_id = candidate.manifest.delivery_id
        plan_id = plan["id"] if plan else None
        metadata = as_json(
            {"operation_id": operation_id, "plan_digest": plan["digest"] if plan else None}
        )
        version = self.engine.version
        with ledger(self.workspace.ledger) as connection, connection:
            connection.execute(
                """INSERT INTO attempts (id, delivery_id, plan_id, state, base_version, expected, metadata, started_at)
                   VALUES (?, ?, ?, 'EXECUTING', ?, ?, ?, ?)""",
                (
                    operation_id,
                    delivery_id,
                    plan_id,
                    version,
                    as_json(candidate.expected),
                    metadata,
                    self.clock().isoformat(),
                ),
            )
            connection.execute(
                "UPDATE deliveries SET state = 'EXECUTING', reason = NULL WHERE id = ?",
                (delivery_id,),
            )
        failpoint("before-merge")
        if candidate.rows:
            self.engine.insert(candidate.rows, delivery_id, metadata)
            failpoint("after-commit")
            return self.reconcile()[-1]
        state = "APPLIED_NO_OP" if plan else "PROJECTED"
        return self.finish(operation_id, delivery_id, plan_id, state, None)

    def finish(
        self,
        operation_id: str,
        delivery_id: str,
        plan_id: str | None,
        state: str,
        version: int | None,
    ) -> dict[str, Any]:
        with ledger(self.workspace.ledger) as connection, connection:
            connection.execute(
                "UPDATE attempts SET state = ?, ended_at = ?, commit_version = ? WHERE id = ?",
                (state, self.clock().isoformat(), version, operation_id),
            )
            connection.execute(
                "UPDATE deliveries SET state = ?, reason = NULL WHERE id = ?", (state, delivery_id)
            )
            if plan_id:
                connection.execute(
                    "UPDATE plans SET outcome = ?, operation_id = ? WHERE id = ?",
                    (state, operation_id, plan_id),
                )
        return {
            "status": state,
            "delivery_id": delivery_id,
            "operation_id": operation_id,
            "plan_id": plan_id,
            "commit_version": version,
        }

    def reconcile(self) -> list[dict[str, Any]]:
        with ledger(self.workspace.ledger) as connection:
            if row(
                connection,
                "SELECT id FROM attempts WHERE state = 'NEEDS_RECONCILIATION' LIMIT 1",
                (),
            ):
                raise PipelineError(
                    "NEEDS_RECONCILIATION", "Inspect the unresolved Silver attempt before writing."
                )
            pending = rows(
                connection, "SELECT * FROM attempts WHERE state = 'EXECUTING' ORDER BY rowid"
            )
        results = []
        for attempt in pending:
            versions = self.engine.commit_versions(attempt["metadata"])
            expected: list[list[Any]] = json.loads(attempt["expected"])
            if versions == [attempt["base_version"] + 1] and self.matches_commit(attempt, expected):
                state = "APPLIED" if attempt["plan_id"] else "PROJECTED"
                results.append(
                    self.finish(
                        attempt["id"],
                        attempt["delivery_id"],
                        attempt["plan_id"],
                        state,
                        versions[0],
                    )
                )
            elif not versions and self.engine.version == attempt["base_version"]:
                with ledger(self.workspace.ledger) as connection, connection:
                    connection.execute(
                        """UPDATE attempts SET state = 'FAILED_REPLAYABLE', reason = 'PRE_COMMIT_FAILURE',
                           ended_at = ? WHERE id = ?""",
                        (self.clock().isoformat(), attempt["id"]),
                    )
                    connection.execute(
                        "UPDATE deliveries SET state = 'FAILED_REPLAYABLE', reason = 'PRE_COMMIT_FAILURE' WHERE id = ?",
                        (attempt["delivery_id"],),
                    )
                results.append(
                    {
                        "status": "FAILED_REPLAYABLE",
                        "delivery_id": attempt["delivery_id"],
                        "operation_id": attempt["id"],
                    }
                )
            else:
                with ledger(self.workspace.ledger) as connection, connection:
                    connection.execute(
                        "UPDATE attempts SET state = 'NEEDS_RECONCILIATION', reason = 'AMBIGUOUS_COMMIT' WHERE id = ?",
                        (attempt["id"],),
                    )
                    connection.execute(
                        "UPDATE deliveries SET state = 'NEEDS_RECONCILIATION', reason = 'AMBIGUOUS_COMMIT' WHERE id = ?",
                        (attempt["delivery_id"],),
                    )
                raise PipelineError(
                    "NEEDS_RECONCILIATION", "Delta history did not match the recorded insert set."
                )
        return results

    def matches_commit(self, attempt: dict[str, Any], expected: list[list[Any]]) -> bool:
        keys = {(entry[0], entry[1]) for entry in expected}
        history = self.engine.history({entry[0] for entry in expected})
        committed = sorted(
            [record["payment_id"], record["revision"], record["payload_hash"]]
            for record in history
            if (record["payment_id"], record["revision"]) in keys
            and record["delivery_id"] == attempt["delivery_id"]
        )
        return bool(expected) and committed == expected
