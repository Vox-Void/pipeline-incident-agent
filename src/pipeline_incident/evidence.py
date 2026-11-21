"""The investigator can inspect four types of evidence."""

import json
from dataclasses import dataclass
from typing import Any

from pipeline_incident.contracts import LINEAGE_VERSION, ROOT, canonical, sha256
from pipeline_incident.deliveries import PipelineError, Workspace, delivery_path, load_manifest
from pipeline_incident.ledger import ledger, row
from pipeline_incident.projection import validate_delivery
from pipeline_incident.spark_engine import Engine

ROOT_NODE = "bronze.raw_payments"
TOOLS = (
    "get_delivery_status",
    "get_contract_findings",
    "get_downstream_impact",
    "preview_recovery",
)


@dataclass(frozen=True)
class Evidence:
    tool: str
    target: str
    data: dict[str, Any]

    @property
    def id(self) -> str:
        return "e-" + sha256(canonical([self.tool, self.target, self.data]))[:16]

    def document(self) -> dict[str, Any]:
        return {"id": self.id, "tool": self.tool, "target": self.target, "data": self.data}


class EvidenceReader:
    def __init__(
        self,
        workspace: Workspace,
        engine: Engine,
        delivery_id: str,
        *,
        lineage_available: bool = True,
    ):
        delivery_path(workspace, delivery_id)
        self.workspace = workspace
        self.engine = engine
        self.delivery_id = delivery_id
        self.lineage_available = lineage_available

    def dispatch(self, tool: str, target: str) -> Evidence:
        if tool not in TOOLS:
            raise PipelineError("UNKNOWN_TOOL", "Choose a read-only evidence tool.")
        if tool == "get_downstream_impact":
            data = self.impact(target)
        else:
            if target != self.delivery_id:
                raise PipelineError("WRONG_DELIVERY", "This investigation covers one delivery.")
            if tool == "get_delivery_status":
                data = self.status()
            elif tool == "get_contract_findings":
                data = self.findings()
            else:
                data = self.preview()
        return Evidence(tool, target, data)

    def status(self) -> dict[str, Any]:
        with ledger(self.workspace.ledger, read_only=True) as connection:
            delivery = row(
                connection, "SELECT state, reason FROM deliveries WHERE id = ?", (self.delivery_id,)
            )
            last_attempt = row(
                connection,
                """SELECT id, started_at, ended_at, commit_version
                   FROM attempts WHERE delivery_id = ? ORDER BY rowid DESC LIMIT 1""",
                (self.delivery_id,),
            )
        if delivery is None:
            raise PipelineError("UNKNOWN_DELIVERY", "No sealed delivery with that ID.")
        manifest = load_manifest(self.workspace, self.delivery_id)
        return {
            **delivery,
            "attempt": last_attempt,
            "sealed_rows": manifest.line_count,
            "silver_version": self.engine.version,
            "data_status": "DELIVERY_DELAYED"
            if delivery["state"] in {"FAILED_REPLAYABLE", "BLOCKED"}
            else "NO_DELAY_RECORDED",
        }

    def findings(self) -> dict[str, Any]:
        with ledger(self.workspace.ledger, read_only=True) as connection:
            data = row(
                connection,
                "SELECT findings, reason FROM deliveries WHERE id = ?",
                (self.delivery_id,),
            )
        if data is None:
            raise PipelineError("UNKNOWN_DELIVERY", "No sealed delivery with that ID.")
        manifest = load_manifest(self.workspace, self.delivery_id)
        return {
            "reason": data["reason"],
            "manifest_digest": manifest.digest,
            "findings": json.loads(data["findings"])[:20],
        }

    def impact(self, start: str) -> dict[str, Any]:
        if not self.lineage_available:
            raise PipelineError("LINEAGE_UNAVAILABLE", "The lineage registry is unavailable.")
        registry: dict[str, Any] = json.loads((ROOT / "contracts" / "lineage.json").read_bytes())
        edges: dict[str, list[str]] = registry["edges"]
        if registry["version"] != LINEAGE_VERSION or start not in edges:
            raise PipelineError(
                "INVALID_LINEAGE_NODE", "Use a dataset in the current lineage registry."
            )
        seen = {start}
        pending = list(edges[start])
        while pending:
            node = pending.pop()
            if node not in seen:
                seen.add(node)
                pending.extend(edges[node])
        return {"lineage_version": LINEAGE_VERSION, "potential_dependents": sorted(seen - {start})}

    def preview(self) -> dict[str, Any]:
        if self.status()["state"] != "FAILED_REPLAYABLE":
            raise PipelineError(
                "DELIVERY_INELIGIBLE", "Only replayable deliveries can be previewed."
            )
        candidate = validate_delivery(self.workspace, self.engine, self.delivery_id)
        return {
            "manifest_matches": True,
            "candidate_digest": candidate.digest,
            "silver_version": self.engine.version,
            "insert_count": len(candidate.rows),
            "duplicate_count": candidate.duplicate_count,
        }
