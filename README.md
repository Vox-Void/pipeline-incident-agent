# Data Pipeline Incident Agent

A local payments pipeline with an incident investigator and an operator-approved replay. It uses fictional ZAR payments to show how an on-call engineer can diagnose a failed delivery, check which reports are affected and restore the correct balance.

The [project guide](docs/project-guide.md) explains how the project was built and follows a failed delivery through the code.

## Run it

Use Bash on Linux or WSL2 with Docker and the Docker Compose plugin. Clone the repository, enter its directory and run these commands.

```bash
./scripts/dev bootstrap
./scripts/dev test
./scripts/dev demo --scenario S04
./scripts/dev demo --scenario S03
./scripts/dev eval
```

Bootstrap needs internet access to build the pinned image and verify a Delta write and read. The remaining commands run with the container network disabled and use no API key. The named Docker volume holds the inbox, checkpoint, Delta tables and SQLite ledger. Demo and evaluation reports appear under `artifacts/` in your checkout. Each scenario uses a fresh storage directory.

Commands return a nonzero status when they fail. You can run one integration scenario with `./scripts/dev test --scenario S04`. Add `--result-file result.json` to save just the JSON output under `artifacts/` for use in scripts.

## What the demos show

Watch the [2:29 captioned walkthrough](docs/demo.mp4), which uses output from the actual Spark runs.

| Scenario | What happens | Captured before | Captured after |
| --- | --- | ---: | ---: |
| `S04` | A valid delivery fails before its commit. The investigator gathers evidence. The fixture shows the refusal without approval, records approval of the exact plan and applies the replay. Reapplying returns the existing operation. | 39.75 | 76.75 |
| `S03` | A producer sends `amount_cents` where the contract requires `amount`. The whole delivery stays in Bronze. A corrected delivery under a new ID reaches Silver. | 39.75 | 131.75 |

The default investigator is scripted and the report labels it `scripted`. The demos use real Spark jobs, Delta commits and SQLite records. The S04 demo uses a fixture actor to show the interaction in one command. Follow the [operator runbook](docs/runbook.md) to issue each command yourself.

## How it works

1. The producer seals a JSON Lines delivery with a file checksum, a line count and a digest of every raw line. Spark Structured Streaming appends those lines to Bronze and tracks its progress in a checkpoint.
2. The projector validates the whole delivery against the declared contract and accepted revisions. An insert-only Delta `MERGE` writes accepted revisions to Silver. The serving views use each payment's highest revision to calculate daily captured and refunded amounts by merchant.
3. The investigator reads delivery status, findings, lineage and a recovery preview. It cites evidence returned during the investigation and asks for human help when required evidence is missing. The investigator can suggest a replay.
4. The operator creates a plan, checks the preview and approves its digest. Apply revalidates the sealed input and Silver version. After a crash, the operator checks Delta commit metadata and row hashes before retrying or finalizing the ledger.

## Verify the results

- The [Linux CI run](https://github.com/Vox-Void/pipeline-incident-agent/actions/runs/44915449551) passed all nine Spark scenarios and both writer crash tests. It checked checkpoint reuse, duplicates, out-of-order revisions, contract rejection, stale approval and missing lineage. The [independent expected results](fixtures/golden/scenarios.json) give exact row counts and totals.
- The [recovery and contract PR](https://github.com/Vox-Void/pipeline-incident-agent/pull/1) shows how the Bronze seal check was added and how CI runs the real writer crash tests.
- `./scripts/dev eval` grades the scripted investigator and a fixed runbook on the same snapshots. Both produce per-case decisions, evidence IDs, tool calls and elapsed time. Use `--split challenge` for the three seeded variants that were added after the investigation prompt was frozen.
- The live benchmark includes a correct contract diagnosis and two investigations the verifier stopped because evidence was missing. See the [live scores and failure analysis](docs/example-results/README.md#live-openai-sample).

## Read further

- [Architecture and data contracts](docs/architecture.md)
- [Operator runbook and live mode](docs/runbook.md)
- [Example results and testing notes](docs/example-results/README.md)
- [Revision ordering decision](docs/decisions/001-payment-revisions.md)
- [Replay and crash recovery decision](docs/decisions/002-recovery-boundary.md)

Version 1 uses one writer, synthetic payments and a local Docker volume. A shared ledger and coordinated writer ownership would be needed before moving the design to a multi-worker deployment.
