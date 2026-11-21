"""Review the supported producer schemas before building the runtime."""

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

CONTRACTS = ("contracts/payment-v1.json", "contracts/payment-v2.json")
DOWNSTREAM_FIELDS = {"payment_id", "merchant_id", "payment_date", "amount", "revision", "status"}


def compare(old: dict[str, Any], new: dict[str, Any], *, new_version: bool) -> list[dict[str, str]]:
    old_fields = old["properties"]
    new_fields = new["properties"]
    required = set(new["required"])
    changes = []
    for name in sorted(old_fields.keys() | new_fields.keys()):
        if name not in new_fields:
            change, result = "removed", "blocked"
        elif name not in old_fields:
            change = "required addition" if name in required else "optional addition"
            result = "compatible" if name not in required and new_version else "blocked"
        elif old_fields[name] != new_fields[name] or (name in old["required"]) != (
            name in required
        ):
            change = "definition changed"
            result = "compatible" if name == "schema_version" and new_version else "blocked"
        else:
            continue
        changes.append({"field": name, "change": change, "result": result})
    for name in sorted(DOWNSTREAM_FIELDS - required):
        changes.append(
            {"field": name, "change": "missing downstream requirement", "result": "blocked"}
        )
    policy_fields = ("$schema", "type", "additionalProperties")
    for name in policy_fields:
        if old.get(name) != new.get(name):
            changes.append({"field": name, "change": "schema policy changed", "result": "blocked"})
    return changes


def check(base: str | None) -> dict[str, Any]:
    schemas = {path: json.loads(Path(path).read_bytes()) for path in CONTRACTS}
    _, v2 = CONTRACTS
    differences = [
        {"comparison": "producer v1 to v2", "changes": compare(*schemas.values(), new_version=True)}
    ]
    supported_channel = schemas[v2]["properties"].get("channel") == {"enum": ["WEB", "APP"]}
    if not supported_channel or "channel" in schemas[v2]["required"]:
        differences[0]["changes"].append(
            {"field": "channel", "change": "version mapping mismatch", "result": "blocked"}
        )
    if base:
        for path, schema in schemas.items():
            previous = subprocess.run(
                ["git", "show", f"{base}:{path}"], check=True, capture_output=True
            )
            differences.append(
                {
                    "comparison": f"committed {path}",
                    "changes": compare(json.loads(previous.stdout), schema, new_version=False),
                }
            )
    passed = all(
        change["result"] == "compatible" for entry in differences for change in entry["changes"]
    )
    return {"passed": passed, "comparisons": differences}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="Compare each supported schema with this Git commit.")
    args = parser.parse_args()
    report = check(args.base)
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report["passed"] else 1)
