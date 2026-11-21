"""Fetch the two Delta JVM artifacts when building the container."""

import hashlib
from pathlib import Path
from urllib.request import urlopen

VERSION = "3.3.2"
ARTIFACTS = {
    "delta-spark_2.12": "4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85",
    "delta-storage": "c096975a2ca97ccc9b4005aa5cce16c9ac3903139d340da6665b66ace67d7c36",
}

directory = Path("/opt/delta")
directory.mkdir(exist_ok=True)
for artifact, expected_sha in ARTIFACTS.items():
    filename = f"{artifact}-{VERSION}.jar"
    url = f"https://repo.maven.apache.org/maven2/io/delta/{artifact}/{VERSION}/{filename}"
    with urlopen(url, timeout=60) as response:
        jar = response.read()
    if hashlib.sha256(jar).hexdigest() != expected_sha:
        raise RuntimeError(f"Checksum mismatch for {filename}")
    (directory / filename).write_bytes(jar)
