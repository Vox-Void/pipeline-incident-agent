import os
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

from pipeline_incident import cli
from pipeline_incident.contracts import ROOT


@pytest.mark.parametrize("command", ["status", "reconcile"])
def test_unused_namespace_has_a_readable_cli_error(command, tmp_path, monkeypatch, capsys):
    args = ["driver.py", command, "--namespace", "unused"]
    if command == "status":
        args += ["--delivery", "MISSING"]
    monkeypatch.setattr(sys, "argv", args)
    monkeypatch.setenv("PIPELINE_HOME", str(tmp_path))
    monkeypatch.setenv("PIPELINE_REPORTS", str(tmp_path / "reports"))
    monkeypatch.setattr(cli, "session", lambda: SimpleNamespace(stop=lambda: None))

    assert cli.main() == 1
    assert capsys.readouterr().err.startswith("UNKNOWN_NAMESPACE:")
    assert not (tmp_path / "unused").exists()


@pytest.mark.skipif(shutil.which("bash") is None, reason="requires the Bash launcher")
@pytest.mark.parametrize(
    ("options", "live"),
    [
        (["--mode", "live"], True),
        (["--mode=live"], True),
        (["--mo=live"], True),
        (["--mode", "live", "--mode=scripted"], False),
        (["--result-file", "value --mode live value"], False),
        ([], False),
    ],
)
def test_launcher_selects_network_settings_from_mode(options, live, tmp_path, monkeypatch):
    docker = tmp_path / "docker"
    docker.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$@\"\n", encoding="utf-8")
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")

    result = subprocess.run(
        ["bash", "scripts/dev", "demo", *options],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    compose = ["-f", "compose.yaml", "-f", "compose.live.yaml"] if live else []
    assert result.stdout.splitlines() == [
        "compose",
        *compose,
        "run",
        "--rm",
        "-T",
        "pipeline",
        "demo",
        *options,
    ]
