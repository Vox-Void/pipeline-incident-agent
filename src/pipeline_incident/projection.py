"""Calculate the complete insert set for a sealed delivery."""

from dataclasses import dataclass

from pipeline_incident.contracts import (
    Finding,
    InvalidEvent,
    Payment,
    canonical,
    line_digest,
    parse_payment,
    sha256,
)
from pipeline_incident.deliveries import (
    MAX_BYTES,
    Manifest,
    PipelineError,
    Workspace,
    delivery_path,
    load_manifest,
)
from pipeline_incident.spark_engine import Engine


class Blocked(PipelineError):
    def __init__(self, code: str, findings: list[Finding]):
        super().__init__(code, "The delivery was blocked before any Silver rows were written.")
        self.findings = findings[:20]


@dataclass(frozen=True)
class Candidate:
    manifest: Manifest
    rows: list[Payment]
    duplicate_count: int

    @property
    def expected(self) -> list[tuple[str, int, str]]:
        return [(payment.payment_id, payment.revision, payment.digest) for payment in self.rows]

    @property
    def digest(self) -> str:
        return sha256(canonical(self.expected))


def sealed_lines(
    workspace: Workspace, engine: Engine, delivery_id: str
) -> tuple[Manifest, list[str]]:
    manifest = load_manifest(workspace, delivery_id)
    file = delivery_path(workspace, delivery_id)
    if not file.is_file():
        raise Blocked(
            "MISSING_DELIVERY", [Finding("MISSING_DELIVERY", "", "The sealed file is missing.")]
        )
    if file.stat().st_size > MAX_BYTES or sha256(file.read_bytes()) != manifest.file_sha256:
        raise Blocked(
            "MANIFEST_MISMATCH",
            [Finding("MANIFEST_MISMATCH", "", "The sealed file checksum changed.")],
        )
    lines = engine.bronze_lines(delivery_id)
    if len(lines) != manifest.line_count or line_digest(lines) != manifest.lines_sha256:
        raise Blocked(
            "BRONZE_MISMATCH",
            [Finding("BRONZE_MISMATCH", "", "Bronze count or checksum differs from the seal.")],
        )
    return manifest, lines


def validate_delivery(workspace: Workspace, engine: Engine, delivery_id: str) -> Candidate:
    manifest, lines = sealed_lines(workspace, engine, delivery_id)
    snapshots: dict[tuple[str, int], Payment] = {}
    identities: dict[str, tuple[str, str]] = {}
    invalid: list[InvalidEvent] = []
    for line in lines:
        try:
            event = parse_payment(line)
        except InvalidEvent as error:
            invalid.append(error)
            continue
        previous = snapshots.get(event.key)
        if previous and previous.digest != event.digest:
            raise conflict(previous.digest, event)
        snapshots[event.key] = event
        identity = (event.merchant_id, event.payment_date)
        if identities.setdefault(event.payment_id, identity) != identity:
            raise identity_conflict()
    if invalid:
        raise Blocked(invalid[0].code, [finding for error in invalid for finding in error.findings])

    history = engine.history(set(identities))
    accepted = {}
    for payment in history:
        payment_id = str(payment["payment_id"])
        identity = (payment["merchant_id"], str(payment["payment_date"]))
        if identities[payment_id] != identity:
            raise identity_conflict()
        accepted[(payment_id, int(payment["revision"]))] = str(payment["payload_hash"])
    inserts = []
    for key, event in sorted(snapshots.items()):
        stored = accepted.get(key)
        if stored is None:
            inserts.append(event)
        elif stored != event.digest:
            raise conflict(stored, event)
    return Candidate(manifest, inserts, len(lines) - len(inserts))


def conflict(previous_hash: str, event: Payment) -> Blocked:
    return Blocked(
        "REVISION_CONFLICT",
        [
            Finding(
                "REVISION_CONFLICT",
                "revision",
                f"{event.event_id}: {previous_hash} differs from {event.digest}.",
            )
        ],
    )


def identity_conflict() -> Blocked:
    return Blocked(
        "IDENTITY_CONFLICT",
        [
            Finding(
                "IDENTITY_CONFLICT",
                "merchant_id/payment_date",
                "Accepted payment revisions must keep both fields unchanged.",
            )
        ],
    )
