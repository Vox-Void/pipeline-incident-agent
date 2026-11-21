# 002. Reconcile replay at the Delta commit boundary

**Status:** Accepted

The payment history lives in one Delta table and the small operational ledger lives in SQLite. A process can die between a Delta commit and the ledger update.

The writer records an execution ID, the expected rows and the prior Silver version before writing. The Delta commit includes the execution ID and the plan digest. A subsequent writer takes the same `flock` lock, inspects previous executions and uses commit metadata and row hashes to confirm whether a commit exists. An unchanged head with no matching commit leaves the delivery eligible for approved replay. Exact matching evidence finalizes the existing execution. Ambiguous evidence stops further Silver writes.

The container passes the lock descriptor to the Spark JVM. The JVM still owns the lock if its Python child exits before the JVM. Once the JVM exits, the next writer can reconcile the ledger and take over.

Moving to object storage and several machines would require a shared ledger, writer ownership with fencing and a recovery design aligned with the catalog or storage commit protocol. A recovery that spans multiple data tables would also require an explicit publishing boundary for readers.
