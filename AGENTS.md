# Repository guide

- Keep demo data synthetic. Store credentials in the local environment and keep run artifacts with real credentials outside the repository.
- Preserve confirmed delivery checks, revision ordering, exact decimal amounts and the separate approval path when making changes.
- Run `./scripts/dev test` for a code change. Use the real Spark scenarios for changes to checkpointing, contracts, Silver or reconciliation.
- Record changes to monetary expectations in the fixture golden file after checking the expected results independently.
