---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Troubleshooting

Use this page to route a production symptom to observable evidence, typed
diagnostics, corrective action, and the boundary where the incident should move
from operations to engineering. The authoritative failure identities and
per-code corrective actions are in the
[error and refusal reference](../reference/errors-and-refusals.md).

## Diagnostic protocol

Work in this order and change one admission boundary at a time:

1. Establish role liveness and readiness. Do not diagnose execution from a
   process that has not emitted its readiness marker.
2. Read the typed result or thrown family first. For a coded family, branch on
   `code()` or enum identity; message text is diagnostic and not a stable key.
3. Open the job DAG and instance table in
   [monitoring](monitoring.md), then compare source input, stage state, sink
   output, Worker placement, and exceptions.
4. Reproduce with the smallest supported graph and the same artifact identity,
   schema, resource profile, and provider set.
5. Preserve evidence before restart or cleanup. Recovery and storage failures
   fail closed; cleanup can destroy the only authority evidence.

The failure tree below uses three escalation levels:

* **Operate**: a role, dependency, or input can be repaired without changing
  the graph or release.
* **Recover**: use the applicable recovery, store, or rolling-upgrade runbook.
* **Escalate**: preserve the run and request engineering review. The normal
  escalation boundary is a P0/P1 reproducible failure, ambiguous durable state,
  checksum/schema drift, a dual-generation sink, or a typed refusal that
  requires a contract change.

## Failure tree

| First observation | Route first | Do not |
|---|---|---|
| No `MASTER_PORT=`, `WORKER_READY=`, `STREAM_MASTER_READY=`, or `STREAM_WORKER_READY=` | [Startup failures](#startup-failures) | Submit work to an unready role. |
| Submission throws or returns a typed refusal | [Submission failures](#submission-failures) | Retry through an admission failure. |
| Job starts but a stage fails | [Execution failures](#execution-failures) | Sum intermediate rows to infer business progress. |
| Memory, disk, thread, descriptor, or budget pressure | [Resource pressure](#resource-pressure) | Disable a limit to keep the job running. |
| Provider open, read, write, transaction, or conformance failure | [Connector failures](#connector-failures) | Treat connector-local retries as engine exactly-once recovery. |
| HTTP missing, stale, CORS-blocked, or standby redirect fails | [Monitor failures](#monitor-failures) | Classify job failure from a missed scrape. |
| Election, discovery, fencing, or authority conflict | [Registry and HA failures](#registry-and-ha-failures) | Clear registry state without an authority snapshot. |
| Checkpoint, savepoint, restore, commit, or migration failure | [Checkpoint and recovery failures](#checkpoint-and-recovery-failures) | Delete or overwrite a recovery point. |
| Control RPC, shuffle, TLS, advertised host, or firewall failure | [Cluster network failures](#cluster-network-failures) | Use loopback success as cross-host qualification. |

## Startup failures

| Symptom | Evidence and typed diagnostic | Corrective action | Escalation boundary |
|---|---|---|---|
| Master does not become ready | Capture the process exit and stderr; require `MASTER_PORT=` for Batch or `STREAM_MASTER_READY=` for Stream. | Fix the explicit role property, port, memory envelope, artifact directory, and classpath shown in [deployment](deployment.md). | Stop if startup mutates durable state or fails after a store transition. |
| Worker does not become ready | Require `WORKER_READY=` or `STREAM_WORKER_READY=`; inspect the Master role log for placement or admission errors. | Correct Worker identity, capacity, spill/temp directory, artifact identity, and Master reachability. | Stop if the same Worker identity returns with a different incarnation or artifact. |
| Monitor starts but the engine cannot bind its port | `GET /api/config` is unavailable; the process log contains the bind failure. | Choose another monitor port or close the conflicting listener. Monitoring remains advisory. | No engine escalation unless the same error is present in a required role. |
| Process exits before readiness | Compare JVM limits, `rowweave.process.memoryLimitBytes`, active processors, and OS quotas with [configuration](../reference/configuration.md). | Repair the deployment envelope and restart one role at a time. | Escalate if a typed capacity family appears at every supported envelope. |

## Submission failures

| Symptom | Evidence and typed diagnostic | Corrective action | Escalation boundary |
|---|---|---|---|
| Plan is rejected before execution | Read the exception family and `code()` in the [error and refusal reference](../reference/errors-and-refusals.md). | Correct schema, opcode, graph shape, capability, or descriptor; do not bypass admission. | Escalate only when a documented supported capability is refused. |
| Artifact or deployment validation fails | Follow `PlanArtifactValidationException` or `DeploymentValidationException`; preserve the artifact digest and manifest index. | Rebuild and verify the complete signed deployment set; never copy a partial set. | Stop admission on signer, checksum, count, index, or identity mismatch. |
| Durable submission cannot reach a terminal state | Capture the run ID, command ID, authority state, and `DurableRunSubmitException` category. | Re-read authority and replay the identical idempotent command where its contract allows it. | Stop and recover when state is ambiguous, fenced, or has a pending publication. |
| Stream contract or change shape is refused | Follow `StreamContractException`, `StreamChangeException`, or `StreamUpsertException`; preserve the portable node and opcode evidence. | Correct the declared topology, changelog kind, key, or stream capability. | Escalate if two independently supported shapes cannot compose. |

## Execution failures

| Symptom | Evidence and typed diagnostic | Corrective action | Escalation boundary |
|---|---|---|---|
| One stage fails and downstream stages stop | Open `/api/jobs/{jobId}`, find the earliest failed stage and instance, and read `ChannelPoisonedException` for the propagated boundary. | Correct the earliest cause, not the downstream poison; preserve the job ID and stage index. | Escalate when the earliest failure is internal or irreproducible. |
| Result is not equivalent across modes | Run the matching example and compare typed result sets; capture effective parallelism, placement, and artifact identity. | Correct nondeterminism, source ordering assumptions, or sink idempotency. | Escalate any reproducible exactness or decimal-semantics difference. |
| Stream stops, restarts, or loses an attempt | Read the Stream projection, attempt and lifetime counters, restart count, and `StreamControlException` or runtime family. | Repair the input, resource condition, or retry policy; restore only from a completed recovery point. | Stop on duplicate/gap evidence or dual-generation output. |
| Incremental frontier does not advance | Call `queryIncrementalFrontier(feedId)` and compare `activeRunId`, source/sink progress, and `/api/retry-recovery`. | Treat an unexplained gap as a connector or recovery failure and preserve the attempt. | Do not advance or reset the frontier from monitor evidence. |

## Resource pressure

| Symptom | Evidence and typed diagnostic | Corrective action | Escalation boundary |
|---|---|---|---|
| Heap, direct memory, or managed memory rises toward its bound | Compare `/api/workers` `managedMemoryUsedBytes` and `managedMemoryMaxBytes` with JVM logs and `ProcessCapacityException`. | Reduce state/cardinality or parallelism, or raise the bound only after capacity review. | Stop when growth persists at the reviewed envelope. |
| Breaker spills or spill cleanup fails | Follow the `SpillFailureCode` and inspect disk free space, inode capacity, ownership, and preserved cause. | Provide a dedicated writable disk and rerun only recoverable work. | Escalate data loss, checksum failure, or `SPILL_CLEANUP_FAILED`. |
| Threads, file descriptors, or channel queues saturate | Capture Worker counters, thread names, descriptor limits, and stage instance rows from `/api/jobs/{jobId}`. | Reduce parallelism or producer/channel pressure and inspect for leaked resources. | Escalate if a bounded resource continues rising after the job reaches a terminal state. |
| Disk or shared-state capacity is low | Check spill, artifact, checkpoint, and state roots separately; read `/api/retention` and `/api/artifact-retention`. | Expand storage or run the documented retention/migration path. | Do not delete recovery, artifact, or authority state under pressure. |

## Connector failures

| Symptom | Evidence and typed diagnostic | Corrective action | Escalation boundary |
|---|---|---|---|
| Source cannot open, split, seek, or replay | Preserve provider class, descriptor digest, split count, source-position evidence, and the typed connector failure. | Verify credentials, permissions, format constants, bounds, and replay contract. | Escalate gap/duplicate replay or a conformance failure on supported input. |
| Sink cannot commit or reports an ambiguous transaction | Preserve sink generation, transaction identity, first decision, completion, and pending-commit evidence. | Reconcile forward or roll back according to the sink contract; make retries idempotent. | Stop on an undecided publication or dual writable generation. |
| Format bytes are malformed or exceed a limit | Use the frozen `StoreFormatHeader`, magic, version, checksum, and frame limits in [format constants](../reference/format-constants.md). | Repair the producer or use a supported format revision; preserve raw bytes. | Stop on checksum, schema, capability, or non-canonical drift. |
| Stream connector is nonconformant | Run the connector conformance example and preserve the `StreamConnectorConformanceException` code. | Correct the provider and rerun the full conformance kit. | Do not qualify the connector from one happy-path job. |

## Monitor failures

| Symptom | Evidence and typed diagnostic | Corrective action | Escalation boundary |
|---|---|---|---|
| HTTP is absent | Check that `rowweave-monitor` is on the classpath and `rowweave.monitor.enable=true`; inspect the startup warning. | Add or enable the optional module. The engine continues on its no-operation registry. | No engine escalation for monitor absence alone. |
| A view is stale | Poll `/api/overview`, `/api/jobs/{jobId}`, and `/api/workers`; compare the browser request with the exact `Origin` in `/api/config`. | Repair the observer network or exact CORS allow-list; refresh the dashboard. | Correlate execution only with runtime status, never with scrape freshness. |
| Retention values are `{}` | Query `/api/retention`, `/api/artifact-retention`, and `/api/retry-recovery`. | `{}` means no snapshot has been published; check Master role logs and retention configuration. | Escalate a persistently unhealthy or blocked recovery snapshot. |
| Standby does not redirect | A known leader must return `302 Found`; no leader returns `503 Service Unavailable`. | Repair election/discovery with the registry and cluster runbook. | Stop admission while leadership or registry identity is ambiguous. |

## Registry and HA failures

| Symptom | Evidence and typed diagnostic | Corrective action | Escalation boundary |
|---|---|---|---|
| Election cannot elect a leader | Capture ZooKeeper connectivity, quorum state, configured registry identity, role properties, and Master logs. | Restore quorum and unique registry identities using [deployment](deployment.md). | Stop admission until exactly one valid leader exists. |
| Worker discovery is incomplete | Compare configured/fixed endpoints with live role registration and `/api/workers`; inspect session and lease expiry. | Repair membership, session TTL, advertised host, and network reachability. | Stop if two live Workers claim one identity or incarnation. |
| Incremental authority reports a conflict | Follow `IncrementalAuthorityDecision`; preserve command ID, feed ID, fence, owner, revision, and active attempt. | Retry only the idempotent command after resolving the stale owner or fence. | Stop on `ACTIVE_EXISTS`, stale ownership, or unexplained durable conflict. |
| Store upgrade admission remains frozen | Require `writesAllowed=false` and the `STORE_UPGRADE_ADMISSION_FROZEN` refusal; read the bound upgrade command. | Resume only through the [store migration](../../../docs/runbooks/store-migration.md) state machine. | Do not clear state or bypass freeze/resume ordering. |

## Checkpoint and recovery failures

| Symptom | Evidence and typed diagnostic | Corrective action | Escalation boundary |
|---|---|---|---|
| Checkpoint times out or stalls | Compare `StreamCheckpointException`, placement/report task counts, watermark, backlog, and last completed checkpoint. | Reduce state growth or timeout pressure, repair slow storage, and inspect the stalled task. | Stop on repeated replacement, missing chunks, or checksum failure. |
| Restore cannot load durable state | Follow `StreamStateException` or the store upgrade family; preserve store ID, generation, digest, and recovery-point identity. | Restore the exact qualified state/artifact set; do not edit bytes. | Stop on corrupt, capability-mismatched, or partially copied state. |
| Savepoint or stop attempt remains undecided | Capture attempt ID, checkpoint ID, sink decisions, and terminal-operation status from logs and typed evidence. | Reconcile from the authoritative manifest, first decision, and completion state. | Do not treat an ACK as completion or republish ambiguous output. |
| Migration verification fails | Compare raw and semantic digests, object/evidence counts, and the bound command identity in the store runbook. | Stop before cutover and repair from a known-good source or target. | Preserve both namespaces; do not delete either side. |

## Cluster network failures

| Symptom | Evidence and typed diagnostic | Corrective action | Escalation boundary |
|---|---|---|---|
| Control RPC refuses or times out | Capture `MASTER_PORT=`, Worker endpoint, advertised host, firewall result, and `ProtocolNegotiationException`. | Make every possible Master/Worker endpoint mutually reachable and use explicit stable ports. | Stop on certificate, SAN, role, term, Worker ID, or incarnation mismatch. |
| Shuffle succeeds locally but fails cross-host | Compare local, mixed, and remote edge evidence in `/api/jobs/{jobId}` with Worker addresses and packet/firewall logs. | Correct advertised hosts, peer admission, MTLS, and credit/flow rules. | Do not qualify cross-host from loopback success. |
| TLS negotiation or identity fails | Follow `ProtocolNegotiationException`; preserve certificate role, SAN, fingerprint, trust store, and negotiated protocol evidence. | Rotate to the intended role-specific certificate and reject plaintext. | Stop on any plaintext fallback cross-host or unsupported downgrade. |
| A Worker connection changes during execution | Correlate Worker incarnation, channel epoch, stage instance, and channel-poisoned cause. | Fence the old incarnation and recover assigned work from a valid point. | Stop if output generations overlap or channel identities cannot be distinguished. |

## Evidence to preserve

Preserve evidence before restart, resubmission, retention, or cleanup:

* Job ID, run ID, attempt ID, command ID, stage index, instance index, worker
  identity, and incarnation.
* Exact exception family and `code()`/enum identity; include the message and
  caused-by chain.
* Artifact digest, deployment-set ID, schema/schema digest, resource profile,
  release identity, and source/sink provider versions.
* `/api/overview`, `/api/jobs/{jobId}`, `/api/workers`, `/api/config`,
  `/api/retention`, and `/api/retry-recovery` snapshots, with timestamps.
* Readiness lines, Master/Worker logs, typed terminal status, checkpoint or
  savepoint manifest, and sink decision/completion evidence.
* Reproduction graph, workload seed, input descriptor, expected digest or
  changelog oracle, and the exact command timeline.

Keep credentials, row payloads, private keys, and secret endpoint material out
of defect reports. Redact them without changing identities, lengths, digests,
status codes, or timing evidence needed to reproduce the failure.
