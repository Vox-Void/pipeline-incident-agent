# Operator runbook

## Start with a known fixture

Build the image from the repository root with `./scripts/dev bootstrap`. Choose a new namespace for each run so that the checkpoint and ledger belong to that run.

```bash
./scripts/dev stage --namespace crash-walkthrough --delivery BASELINE --fixture baseline
./scripts/dev project --namespace crash-walkthrough --delivery BASELINE
./scripts/dev stage --namespace crash-walkthrough --delivery RECOVERY --fixture replay
./scripts/dev project --namespace crash-walkthrough --delivery RECOVERY \
  --fixture-mode --fail-before-merge
./scripts/dev reconcile --namespace crash-walkthrough
./scripts/dev investigate --namespace crash-walkthrough --delivery RECOVERY
```

The flagged projection intentionally exits with `FIXTURE_PRE_COMMIT_FAILURE`. The next reconcile records `FAILED_REPLAYABLE`. The investigation should give the reason, evidence IDs and the possible downstream views. The existing captured balance is `39.75` ZAR.

## Approve and apply

```bash
./scripts/dev plan --namespace crash-walkthrough --delivery RECOVERY
```

Review the proposed insert count and decimal balances, the Silver version and the expiry time. Copy `plan_id` and `digest` from the output. Substitute those values for `PLAN_ID` and `DIGEST` below.

```bash
./scripts/dev approve --namespace crash-walkthrough --plan PLAN_ID --digest DIGEST --actor local-operator
./scripts/dev apply --namespace crash-walkthrough --plan PLAN_ID --digest DIGEST
./scripts/dev totals --namespace crash-walkthrough
```

The successful replay has five current payments and 76.75 ZAR captured. Run the same apply command again to confirm it returns the same operation without another commit.

## Handle failures

| Result | Operator action |
| --- | --- |
| `CONTRACT_MISMATCH` or `UNSUPPORTED_VERSION` | Check the field findings. Ask the producer to correct the payload and use a new delivery ID. |
| `REVISION_CONFLICT` | Compare the revision and hashes. Ask the producer which snapshot is correct and whether a new revision is required. |
| `STALE_PLAN` or `EXPIRED_PLAN` | Inspect the current state, create a fresh plan and approve its digest. |
| `NEEDS_HUMAN` | Look up the evidence named in `missing_evidence`. The agent has withheld a replay suggestion. |
| `NEEDS_RECONCILIATION` | Preserve the ledger and Delta history. Compare the execution metadata, expected hashes and table version before continuing. |

Use `bash scripts/crash-test` to exercise isolated writer failures with real containers. It creates unique namespaces, stops the test containers and leaves logs under `artifacts/`.

## Opt into live investigation

Set `OPENAI_API_KEY` in the environment and choose a Responses API model available to that API project with `PIPELINE_MODEL`. Use `./scripts/dev demo --scenario S04 --mode live` for one synthetic case or `./scripts/dev eval --mode live` for the evaluation. These commands enable container networking for the provider and may incur API charges. The trace records model and token usage when the provider returns them. The default demo and CI run without a key.
