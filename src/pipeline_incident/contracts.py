"""Validate producer snapshots before they reach typed Spark columns."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, NoReturn, cast

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_VERSION = "payments/1+2"
LINEAGE_VERSION = "payments-lineage/1"


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def line_digest(lines: list[str]) -> str:
    """Hash the sorted line hashes, including repeated lines."""
    return sha256("".join(sorted(sha256(line.encode("utf-8")) for line in lines)).encode("ascii"))


@dataclass(frozen=True)
class Finding:
    code: str
    field: str
    detail: str


class InvalidEvent(ValueError):
    def __init__(self, code: str, findings: list[Finding]):
        super().__init__(code)
        self.code = code
        self.findings = findings


@dataclass(frozen=True)
class Payment:
    event_id: str
    payment_id: str
    merchant_id: str
    revision: int
    occurred_at: str
    payment_date: str
    status: str
    currency: str
    amount: str
    channel: str | None

    @property
    def key(self) -> tuple[str, int]:
        return self.payment_id, self.revision

    @property
    def digest(self) -> str:
        return sha256(canonical(asdict(self)))


def no_duplicate_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field, value in pairs:
        if field in result:
            raise ValueError("duplicate field name")
        result[field] = value
    return result


def reject_constant(value: str) -> NoReturn:
    raise ValueError("unsupported JSON number")


@lru_cache(maxsize=2)
def validator(version: int) -> Draft202012Validator:
    path = ROOT / "contracts" / f"payment-v{version}.json"
    schema = json.loads(path.read_bytes())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def parse_payment(line: str) -> Payment:
    try:
        data: Any = json.loads(
            line, object_pairs_hook=no_duplicate_fields, parse_constant=reject_constant
        )
    except (ValueError, RecursionError) as error:
        raise InvalidEvent(
            "MALFORMED_JSON",
            [Finding("MALFORMED_JSON", "", "Invalid JSON or duplicate field name.")],
        ) from error
    if not isinstance(data, dict):
        raise InvalidEvent(
            "MALFORMED_JSON", [Finding("MALFORMED_JSON", "", "Expected a JSON object.")]
        )
    version = data.get("schema_version")
    if type(version) is not int or version not in {1, 2}:
        raise InvalidEvent(
            "UNSUPPORTED_VERSION",
            [Finding("UNSUPPORTED_VERSION", "schema_version", "Expected version 1 or 2.")],
        )
    schema = validator(version)
    if isinstance(schema.schema, bool):
        raise RuntimeError("Payment contracts must be schema objects.")
    # Collect missing fields, unexpected field names and invalid values as separate findings.
    findings: list[Finding] = []
    for name in schema.schema["required"]:
        if name not in data:
            findings.append(Finding("MISSING_FIELD", name, "Required field missing."))
    for name in sorted(data.keys() - schema.schema["properties"].keys()):
        safe_name = name if re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]{0,63}", name) else "unknown"
        findings.append(Finding("UNEXPECTED_FIELD", safe_name, "Field is outside this contract."))
    for violation in schema.iter_errors(data):
        if violation.path:
            findings.append(
                Finding(
                    "INVALID_FIELD", str(violation.path[0]), "Value does not match its contract."
                )
            )
    if findings:
        raise InvalidEvent("CONTRACT_MISMATCH", findings[:20])
    # Check the event ID, calendar values and amount before normalizing the payment.
    accepted = cast(dict[str, Any], data)
    if accepted["event_id"] != f"{accepted['payment_id']}:{accepted['revision']}":
        raise InvalidEvent(
            "CONTRACT_MISMATCH",
            [Finding("INVALID_FIELD", "event_id", "Expected payment ID and revision.")],
        )
    try:
        datetime.fromisoformat(accepted["occurred_at"])
        date.fromisoformat(accepted["payment_date"])
    except ValueError as error:
        raise InvalidEvent(
            "CONTRACT_MISMATCH",
            [Finding("INVALID_FIELD", "date", "Invalid calendar date or time.")],
        ) from error
    if accepted["amount"] == "0.00":
        raise InvalidEvent(
            "CONTRACT_MISMATCH", [Finding("INVALID_FIELD", "amount", "Amount must be positive.")]
        )
    # Map both producer versions to the same Payment shape with a nullable channel.
    accepted.pop("schema_version")
    accepted.setdefault("channel", None)
    return Payment(**accepted)
