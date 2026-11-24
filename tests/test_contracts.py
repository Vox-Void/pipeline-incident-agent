import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from pipeline_incident import deliveries
from pipeline_incident.contracts import ROOT, InvalidEvent, line_digest, parse_payment
from pipeline_incident.deliveries import PipelineError, Workspace, describe, load_manifest, stage
from pipeline_incident.fixtures import BASELINE, broken_contract, payment
from pipeline_incident.ledger import ledger


def test_sealed_fixture_is_stable(tmp_path):
    workspace = Workspace(tmp_path)
    manifest = stage(workspace, "BASELINE", BASELINE)

    assert stage(workspace, "BASELINE", BASELINE) == manifest
    assert load_manifest(workspace, "BASELINE") == manifest
    assert manifest.line_count == 3
    assert line_digest(["a", "b", "a"]) == line_digest(["b", "a", "a"])
    assert line_digest(["a", "b", "a"]) != line_digest(["a", "b"])
    with pytest.raises(PipelineError, match="different delivery bytes"):
        stage(workspace, "BASELINE", broken_contract())
    with (
        ledger(workspace.ledger, read_only=True) as connection,
        pytest.raises(sqlite3.OperationalError),
    ):
        connection.execute("DELETE FROM deliveries")


def test_concurrent_retries_publish_one_complete_delivery(tmp_path, monkeypatch):
    workspace = Workspace(tmp_path)
    ready_to_link = Barrier(4)
    link = deliveries.os.link

    def publish_together(source, destination):
        ready_to_link.wait(timeout=10)
        return link(source, destination)

    monkeypatch.setattr(deliveries.os, "link", publish_together)
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = [pool.submit(stage, workspace, "BASELINE", BASELINE) for _ in range(4)]
        manifests = [job.result(timeout=15) for job in jobs]

    assert manifests == [load_manifest(workspace, "BASELINE")] * 4
    assert list(workspace.inbox.iterdir()) == [workspace.inbox / "BASELINE.jsonl"]
    assert (workspace.inbox / "BASELINE.jsonl").read_bytes() == BASELINE
    assert not list(tmp_path.glob(".delivery-*"))


@pytest.mark.parametrize("payload", [b"{}", b"{}\r\n", b"\n", b"\xff\n"])
def test_rejects_unsealed_input(payload):
    with pytest.raises(PipelineError) as error:
        describe("BASELINE", payload)
    assert error.value.code == "INVALID_DELIVERY"


@pytest.mark.parametrize(
    "amount", [0, 0.1, "0.00", "-1.00", "01.00", "1.0", "10000000000000000.00", "1.00\n"]
)
def test_rejects_invalid_decimal(amount):
    with pytest.raises(InvalidEvent) as error:
        parse_payment(json.dumps(payment("P100", amount)))
    assert "amount" in {finding.field for finding in error.value.findings}


def test_version_two_without_channel_is_an_exact_business_duplicate():
    v1 = parse_payment(json.dumps(payment("P100", "10.00")))
    v2 = parse_payment(json.dumps(payment("P100", "10.00", version=2)))
    with_channel = parse_payment(json.dumps(payment("P100", "10.00", version=2, channel="APP")))

    assert v1.digest == v2.digest
    assert with_channel.digest != v2.digest


def test_rejects_duplicate_json_keys():
    with pytest.raises(InvalidEvent) as error:
        parse_payment('{"amount":"1.00","amount":"2.00"}')
    assert error.value.code == "MALFORMED_JSON"


def test_renamed_amount_is_a_field_level_diagnostic():
    with pytest.raises(InvalidEvent) as error:
        parse_payment(broken_contract().decode().splitlines()[0])
    assert {(finding.code, finding.field) for finding in error.value.findings} == {
        ("MISSING_FIELD", "amount"),
        ("UNEXPECTED_FIELD", "amount_cents"),
    }


FROZEN_CASES = json.loads((ROOT / "fixtures/golden/contract-cases.json").read_bytes())


@pytest.mark.parametrize("case", FROZEN_CASES, ids=[case["case"] for case in FROZEN_CASES])
def test_frozen_contract_example(case):
    line = json.dumps(case["event"])
    if "error" not in case:
        assert parse_payment(line).payment_id == "P101"
    else:
        with pytest.raises(InvalidEvent) as error:
            parse_payment(line)
        assert error.value.code == case["error"]
