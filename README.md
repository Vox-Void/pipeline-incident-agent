# Data Pipeline Incident Agent

A local payments pipeline for practising incident investigation and recovery with synthetic data.

## Start the runtime

Use Bash on Linux or WSL2 with Docker and the Compose plugin. From the repository root, run:

```sh
./scripts/dev bootstrap
```

Building the image downloads the pinned runtime and dependencies. The smoke test runs without network access. It writes a real Delta table, reads it back and checks event `P100:1` with amount `10.00`. Spark runs on two local threads. Docker stores the test table in the project’s named volume.
