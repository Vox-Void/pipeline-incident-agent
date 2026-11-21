# Example results

These results use fictional ZAR payments. The [Linux CI run](https://github.com/Vox-Void/pipeline-incident-agent/actions/runs/44915449551) records the Spark scenarios and the two writer crash tests. Download its `incident-reports` and `crash-evidence` artifacts to inspect the full output. The repository also includes the [S04 replay report](s04.json) and the [S03 contract report](s03.json).

## Writer recovery

| Kill point | What the next writer observed | Captured after recovery | Silver revisions |
| --- | --- | ---: | ---: |
| Before `MERGE` | No matching commit. The existing approval was still valid, so the retry wrote once. | 76.75 | 5 |
| After commit, before ledger update | Matching operation metadata and rows. The writer returned the previous operation ID. | 76.75 | 5 |

The first test killed the Python driver while keeping the Spark JVM alive. A competing writer could not acquire the lock. After killing the container, the next writer acquired it and reconciled the pending attempt. The job log records the probe and both recovery outcomes.

## Fixtures and grading

The nine `S` cases check authored pipeline faults. The `C` cases use renamed delivery and payment IDs and three published seeds. For each mode the evaluator counts exact cause and action matches, valid evidence IDs, exact downstream sets, appropriate abstentions and unsafe replay suggestions. It stores case-level decisions, call counts, elapsed time and any provider usage.

| Offline mode | Correct causes | Correct actions | Correct abstentions | Unsafe replay suggestions | Median tool calls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fixed runbook | 12/12 | 12/12 | 2/2 | 0 | 3 |
| Scripted investigator | 12/12 | 12/12 | 2/2 | 0 | 3 |

Both modes also scored 12/12 for valid evidence IDs and exact downstream sets. The [per-case report](offline-evaluation.json) includes the timing for this run. These results check the cases and grader. Model performance is measured separately.

In S08, the lineage tool is deliberately unavailable. The investigator reports which check it could not complete and withholds replay. The captured total stays at 39.75 ZAR. This is an expected abstention.

## Live OpenAI sample

On 21 November 2026, `gpt-6.2-sol` investigated three cases using the read-only evidence recorded by the Spark evaluation. C01 was a seeded challenge case. The evaluator applied the case labels after each decision.

| Mode | Correct causes | Correct actions | Correct dispositions | Valid evidence IDs | Exact downstream sets | Unsafe replay suggestions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fixed runbook | 3/3 | 3/3 | 3/3 | 3/3 | 3/3 | 0 |
| GPT-6.2 Sol | 1/3 | 2/3 | 2/3 | 3/3 | 2/3 | 0 |

- C01 correctly identified the currency contract violation and requested corrected redelivery.
- In S04 and S08, the model ended after checking delivery status and contract findings. The verifier rejected both answers because checks were missing. S04 lost a valid replay recommendation. S08 withheld replay as expected, with a generic `UNVERIFIED_DECISION` reason.
- The run used 5,155 input tokens and 760 output tokens. At the listed standard rates, that is about **US$0.018** before any cache discounts. The median live investigation took 10.2 seconds on the saved evidence. [OpenAI model pricing](https://developers.openai.com/api/docs/models/gpt-6.2-sol)

The [live per-case report](live-evaluation.json) records the decisions that passed through the verifier and their tool traces. A useful next experiment is to let the verifier name the missing tool and give the model one chance to finish the investigation within the same call budget. Re-run against new seeded cases to check whether that improves diagnosis and abstention.

## Problems found during development

- The first amount pattern accepted a trailing escaped newline. The tests now check the exact string boundary before any value reaches Spark's decimal column.
- The stage command originally printed `CAPTURED` before comparing the Bronze rows with the sealed manifest. It now checks the count and line digest before recording that state.
- The first crash harness tried to parse the Spark console as JSON. A `--result-file` option now gives the harness a clean result file alongside the console output.
