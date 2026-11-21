"""Commands for the demo and the operator workflow."""

import argparse
import json
import os
import sys
from pathlib import Path
from tempfile import mkdtemp

from pipeline_incident.spark_engine import session, smoke


def main() -> int:
    parser = argparse.ArgumentParser(prog="pipeline")
    parser.add_argument("command", choices=["smoke"])
    args = parser.parse_args()
    home = Path(os.environ.get("PIPELINE_HOME", "./data"))
    home.mkdir(exist_ok=True, parents=True)
    spark = session()
    try:
        if args.command == "smoke":
            directory = Path(mkdtemp(prefix="smoke-", dir=home))
            print(json.dumps(smoke(spark, directory / "payments")))
    except Exception as error:
        print(json.dumps({"status": "error", "operation": args.command, "error": str(error)}), file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0
