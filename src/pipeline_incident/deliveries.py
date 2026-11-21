"""Atomically publish sealed, small JSONL deliveries."""

import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path

from pipeline_incident.contracts import canonical, line_digest, sha256
from pipeline_incident.ledger import as_json, ledger, row

MAX_ROWS = 10_000
MAX_BYTES = 10 * 1024 * 1024
DELIVERY_ID = re.compile(r"[A-Z][A-Z0-9_-]{0,39}")


class PipelineError(RuntimeError):
    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code


@dataclass(frozen=True)
class Workspace:
    root: Path

    def prepare(self) -> None:
        self.inbox.mkdir(parents=True, exist_ok=True)

    @property
    def inbox(self) -> Path:
        return self.root / "inbox"

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "silver"

    @property
    def checkpoint(self) -> Path:
        return self.root / "checkpoint"

    @property
    def ledger(self) -> Path:
        return self.root / "operations.sqlite3"


@dataclass(frozen=True)
class Manifest:
    delivery_id: str
    line_count: int
    file_sha256: str
    lines_sha256: str

    @property
    def digest(self) -> str:
        return sha256(canonical(asdict(self)))


def delivery_path(workspace: Workspace, delivery_id: str) -> Path:
    if not DELIVERY_ID.fullmatch(delivery_id):
        raise PipelineError("INVALID_DELIVERY_ID", "Use an uppercase delivery ID.")
    return workspace.inbox / f"{delivery_id}.jsonl"


def describe(delivery_id: str, payload: bytes) -> Manifest:
    if not payload or len(payload) > MAX_BYTES or not payload.endswith(b"\n") or b"\r" in payload:
        raise PipelineError(
            "INVALID_DELIVERY", "Expected a non-empty UTF-8 file with LF and a trailing LF."
        )
    try:
        lines = payload[:-1].decode("utf-8", errors="strict").split("\n")
    except UnicodeDecodeError as error:
        raise PipelineError("INVALID_DELIVERY", "Expected UTF-8.") from error
    if len(lines) > MAX_ROWS or any(not line.strip() for line in lines):
        raise PipelineError("INVALID_DELIVERY", "The delivery is too large or has blank lines.")
    return Manifest(delivery_id, len(lines), sha256(payload), line_digest(lines))


def stage(workspace: Workspace, delivery_id: str, payload: bytes) -> Manifest:
    destination = delivery_path(workspace, delivery_id)
    manifest = describe(delivery_id, payload)
    workspace.prepare()
    manifest_json = as_json(asdict(manifest))
    with ledger(workspace.ledger) as connection, connection:
        existing = row(connection, "SELECT manifest FROM deliveries WHERE id = ?", (delivery_id,))
        if existing is None:
            connection.execute(
                "INSERT INTO deliveries (id, manifest, state) VALUES (?, ?, 'SEALED')",
                (delivery_id, manifest_json),
            )
        elif existing["manifest"] != manifest_json:
            raise PipelineError(
                "DELIVERY_ID_CONFLICT", "Use a new ID for different delivery bytes."
            )
    if destination.exists():
        if sha256(destination.read_bytes()) != manifest.file_sha256:
            raise PipelineError("MANIFEST_MISMATCH", "The watched file no longer matches its seal.")
        return manifest
    temporary = workspace.root / f".{delivery_id}.staged"
    with temporary.open("wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, destination)
    return manifest


def load_manifest(workspace: Workspace, delivery_id: str) -> Manifest:
    delivery_path(workspace, delivery_id)
    with ledger(workspace.ledger, read_only=True) as connection:
        record = row(connection, "SELECT manifest FROM deliveries WHERE id = ?", (delivery_id,))
    if record is None:
        raise PipelineError("UNKNOWN_DELIVERY", "No sealed delivery with that ID.")
    return Manifest(**json.loads(record["manifest"]))
