"""Store delivery, plan, approval and execution state in SQLite."""

import json
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS deliveries (
  id TEXT PRIMARY KEY, manifest TEXT NOT NULL, state TEXT NOT NULL,
  reason TEXT, findings TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS attempts (
  id TEXT PRIMARY KEY, delivery_id TEXT NOT NULL REFERENCES deliveries(id),
  plan_id TEXT, state TEXT NOT NULL, base_version INTEGER NOT NULL,
  expected TEXT NOT NULL, metadata TEXT NOT NULL, started_at TEXT NOT NULL,
  ended_at TEXT, commit_version INTEGER, reason TEXT
);
CREATE TABLE IF NOT EXISTS plans (
  id TEXT PRIMARY KEY, delivery_id TEXT NOT NULL REFERENCES deliveries(id),
  digest TEXT NOT NULL, body TEXT NOT NULL, outcome TEXT, operation_id TEXT,
  superseded INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS approvals (
  plan_id TEXT PRIMARY KEY REFERENCES plans(id), digest TEXT NOT NULL,
  actor TEXT NOT NULL, approved_at TEXT NOT NULL
);
"""


def open_ledger(path: Path, *, read_only: bool = False) -> sqlite3.Connection:
    target = f"{path.resolve().as_uri()}?mode=ro" if read_only else str(path)
    connection = sqlite3.connect(target, uri=read_only, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    if read_only:
        connection.execute("PRAGMA query_only = ON")
    else:
        connection.executescript(SCHEMA)
    return connection


@contextmanager
def ledger(path: Path, *, read_only: bool = False) -> Iterator[sqlite3.Connection]:
    with closing(open_ledger(path, read_only=read_only)) as connection:
        yield connection


def rows(
    connection: sqlite3.Connection, sql: str, parameters: Sequence[object] = ()
) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(sql, parameters)]


def row(
    connection: sqlite3.Connection, sql: str, parameters: Sequence[object]
) -> dict[str, Any] | None:
    result = connection.execute(sql, parameters).fetchone()
    return dict(result) if result is not None else None


def as_json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
