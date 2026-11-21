"""Use fixed inputs for the demo. The expected outcomes live in a separate file."""

from typing import Any

from pipeline_incident.contracts import canonical


def payment(
    payment_id: str,
    amount: str,
    *,
    merchant: str = "M1",
    revision: int = 1,
    status: str = "CAPTURED",
    version: int = 1,
    channel: str | None = None,
) -> dict[str, Any]:
    event = {
        "schema_version": version,
        "event_id": f"{payment_id}:{revision}",
        "payment_id": payment_id,
        "merchant_id": merchant,
        "revision": revision,
        "occurred_at": "2026-01-07T09:15:00Z",
        "payment_date": "2026-01-07",
        "status": status,
        "currency": "ZAR",
        "amount": amount,
    }
    if channel is not None:
        event["channel"] = channel
    return event


def jsonl(events: list[dict[str, Any]]) -> bytes:
    return b"".join(canonical(event) + b"\n" for event in events)


BASELINE = jsonl(
    [payment("P100", "10.00"), payment("P200", "25.50"), payment("P300", "4.25", merchant="M2")]
)
REPLAY = jsonl([payment("P600", "15.00"), payment("P700", "22.00")])


def broken_contract() -> bytes:
    renamed = payment("P500", "90.00")
    renamed.pop("amount")
    renamed["amount_cents"] = 9000
    return jsonl([renamed, payment("P510", "2.00")])
