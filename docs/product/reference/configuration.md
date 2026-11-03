---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Configuration reference

Configuration uses explicit constructors, session setters, and system
properties. Every property below is read through a strict parser that rejects
unknown or invalid combinations before execution. All process-level properties
are read once at startup and frozen; no runtime mutation is supported unless
the section explicitly states otherwise.

Properties are read from `System.getProperties()` unless the owning class
documents a different source. Unknown properties in the `rowweave.*` namespace
are not read by the engine and have no effect.

## 1. Configurable property reference

The tables below are the normative set of supported properties. Each row
names the owning parser class so that the behavior source can be traced. A
property is supported only if it appears in one of these tables and its
parser class reads it before the first side effect.

### 1.1 Batch process roles

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.master.enable` | boolean | `false` | Start the Batch Master role (DAG scheduling, job admission). |
| `rowweave.worker.enable` | boolean | `false` | Start the Batch Worker role (stage execution, shuffle). |
| `rowweave.master.port` | integer | `0` | Master RPC bind port; `0` selects an ephemeral port. |
| `rowweave.worker.host` | string | local address | Worker public host for control-plane reconnection. |
| `rowweave.worker.master.host` | string | `127.0.0.1` | Worker → Master RPC endpoint. |
| `rowweave.worker.master.port` | integer | `0` | Worker → Master RPC port; `0` fails Worker startup. |
| `rowweave.registry` | string | `inprocess` | Registry provider: `inprocess` or `zk` (requires `rowweave-registry-zk`). |
| `rowweave.zk.connectString` | string | *none* | ZooKeeper connect string; required when `rowweave.registry=zk`. |
| `rowweave.master.artifact.dir` | path | *none* | Shared artifact directory; required for durable Batch submission. |

Invalid-value behavior: role booleans must be `true` or `false`; an unknown
registry value fails before either role allocates resources; `zk` without
`rowweave.zk.connectString` is refused.

### 1.2 Worker capacity, memory, and spill

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.worker.memory` | long (bytes) | heap / 2 | Worker managed-memory pool; canonical form. |
| `rowweave.memory.bytes` | long (bytes) | *none* | Deprecated alias; must equal `rowweave.worker.memory` when both are set. |
| `rowweave.worker.slots` | integer | `128` | Concurrent stage-instance admission slots; also the memory denominator. |
| `rowweave.worker.maxStageThreads` | integer | `16` | Certified per-assignment internal-thread ceiling. |
| `rowweave.worker.operator.minMemory` | long (bytes) | 512 KiB | Minimum per-breaker operator budget. |
| `rowweave.worker.operator.maxMemory` | long (bytes) | 16 MiB | Maximum per-breaker operator budget. |
| `rowweave.worker.merge.fanIn` | integer | `128` | External-sort merge concurrency. |
| `rowweave.worker.spill.dir` | path | `java.io.tmpdir` | Spill workspace root; must be absolute. |
| `rowweave.worker.spill.maxBytes` | long (bytes) | 64 GiB | Worker-level spill ceiling. |
| `rowweave.worker.spill.jobMaxBytes` | long (bytes) | 16 GiB | Per-job spill ceiling; must not exceed `maxBytes`. |
| `rowweave.worker.spill.minFreeBytes` | long (bytes) | 1 GiB | Minimum free disk bytes; spill fails closed below this. |
| `rowweave.worker.spill.orphanGraceMs` | long (ms) | 3,600,000 | Orphan cleanup grace; production minimum 60,000. |
| `rowweave.worker.spill.scanMaxEntries` | integer | `4096` | Maintenance scan page size; range 4–100000. |
| `rowweave.worker.spill.deleteMaxEntries` | integer | `1024` | Maintenance delete page size; range 3–scanMaxEntries. |
| `rowweave.worker.spill.maintenanceIntervalMs` | long (ms) | 60000 | Maintenance interval; range 1000–3600000 (production). |
| `rowweave.worker.heartbeatMs` | long (ms) | 1000 | Worker heartbeat interval; must be positive. |
| `rowweave.cluster.heartbeatTrace` | boolean | `false` | Diagnostic heartbeat event trace (Master and Worker). |
| `rowweave.worker.closeDrainMs` | long (ms) | 5000 | Grace period to drain stage output on shutdown. |
| `rowweave.worker.cancelDrainMs` | long (ms) | 5000 | Grace period to drain stage output on cancel. |
| `rowweave.worker.assignmentRetentionMs` | long (ms) | 300000 | Worker assignment-ledger retention. |
| `rowweave.worker.assignmentMaxEntries` | integer | 100000 | Worker assignment-ledger entry cap. |
| `rowweave.worker.rendezvousRetentionMs` | long (ms) | 300000 | Local shuffle rendezvous retention. |
| `rowweave.worker.rendezvousMaxEntries` | integer | 100000 | Local shuffle rendezvous entry cap. |
| `rowweave.worker.preparedOutputLimit` | integer | `max(64, slots × 4)` | Prepared-output entries kept before the oldest is dropped. |
| `rowweave.worker.preparedOutputRetentionMs` | long (ms) | 600000 | Prepared-output retention window. |

Invalid-value behavior: memory and slot values must be positive; setting both
`rowweave.worker.memory` and `rowweave.memory.bytes` requires byte-exact
equality; spill quotas and maintenance windows are range-checked at startup
and the process refuses to start on violation.

### 1.3 Control plane (RPC admission and transport)

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.control.securityProfile` | enum | `LOOPBACK` | `LOOPBACK` (single host) or `MTLS` (cross-host). |
| `rowweave.control.host` | string | `127.0.0.1` | Control bind/advertised host; `LOOPBACK` forces a loopback address. |
| `rowweave.control.maxConnections` | integer | `256` | Concurrent control-plane connections. |
| `rowweave.control.listenBacklog` | integer | `128` | TCP listen backlog. |
| `rowweave.control.frameDeadlineMs` | long (ms) | 30000 | Maximum time for one control RPC frame. |
| `rowweave.control.inFlightFrameBytes` | long (bytes) | 128 MiB | Per-frame admission bound. |
| `rowweave.control.residentBytes` | long (bytes) | 256 MiB | Control-plane resident-memory budget. |
| `rowweave.control.protocolHelloHex` | hex string | *none* | Overrides the transport hello payload for protocol negotiation. |

Invalid-value behavior: an unknown security profile or an MTLS profile without
the TLS material below is rejected before any listener opens.

### 1.4 Control-plane TLS material

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.control.tls.keyStore` | path | *none* | Keystore path; required under `MTLS`. |
| `rowweave.control.tls.keyStoreType` | string | `PKCS12` | Keystore type. |
| `rowweave.control.tls.keyStorePassword` | string | *none* | Keystore password; required under `MTLS`. |
| `rowweave.control.tls.trustStore` | path | *none* | Truststore path; required under `MTLS`. |
| `rowweave.control.tls.trustStoreType` | string | `PKCS12` | Truststore type. |
| `rowweave.control.tls.trustStorePassword` | string | *none* | Truststore password; required under `MTLS`. |
| `rowweave.control.tls.authzFile` | path | *none* | Role authorization map; required under `MTLS`. |

### 1.5 Shuffle and data-plane transport

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.shuffle.mux` | boolean | `true` | Enable the multiplexed shuffle transport; `false` rolls back to per-stage channels. |
| `rowweave.shuffle.securityProfile` | enum | *none* | Explicit shuffle security profile; must not be set — the value is rejected as an independent security authority. |
| `rowweave.shuffle.maxConcurrentHandshakes` | integer | `4` | Concurrent TLS handshakes on the data plane. |
| `rowweave.shuffle.handshakeTimeoutMs` | long (ms) | 10000 | Data-plane handshake deadline. |
| `rowweave.shuffle.connectTimeoutMs` | long (ms) | 10000 | Data-plane connect deadline. |
| `rowweave.shuffle.handshakeResidentBytes` | long (bytes) | 24 MiB | Handshake resident-memory budget. |
| `rowweave.shuffle.authFrameBytes` | integer | 1024 | Authentication frame byte bound; frozen. |
| `rowweave.shuffle.ioThreads` | integer | `max(2, cpus / 2)` | Data-plane event-loop thread count. |
| `rowweave.shuffle.maxConnections` | integer | `256` | Ingress connection admission cap. |
| `rowweave.shuffle.maxIngressHandlers` | integer | `256` | Ingress handler admission cap. |
| `rowweave.shuffle.maxTargetsPerConnection` | integer | `4096` | Remote targets multiplexed per connection. |
| `rowweave.shuffle.maxPendingFramesPerTarget` | integer | `65536` | Pending frames per target before backpressure. |
| `rowweave.shuffle.ingressResidentBytes` | long (bytes) | 256 MiB | Ingress resident-memory admission. |
| `rowweave.shuffle.idleReadTimeoutMs` | long (ms) | 60000 | Ingress idle-read deadline; production minimum 1000. |
| `rowweave.shuffle.keepaliveMs` | integer | 10000 | Shuffle TCP keepalive interval. |
| `rowweave.shuffle.soTimeoutMs` | integer | 60000 | Shuffle TCP read timeout. |
| `rowweave.shuffle.acceptTimeoutMs` | integer | `max(60000, stageTimeoutMs)` | Reverse-topology accept deadline. |
| `rowweave.shuffle.flushIntervalMs` | long (ms) | 100 | Data-plane socket flush interval. |

Invalid-value behavior: every admission dimension is range-checked; a value
outside its bound or a cross-budget conflict fails the role before any socket
is opened. Setting `rowweave.shuffle.securityProfile` is always rejected.

### 1.6 Scheduler and pipeline timing

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.cluster.admission` | boolean | `true` | Enable Master admission gating (slot availability, job limits). |
| `rowweave.cluster.admitTimeoutMs` | long (ms) | 60000 | Maximum wait for admission before the submit fails. |
| `rowweave.cluster.downgrade` | boolean | `true` | Allow parallelism downgrade when slots are exhausted. |
| `rowweave.cluster.downgradeGraceMs` | long (ms) | `min(admitTimeoutMs / 4, 5000)` | Grace window before downgrade. |
| `rowweave.cluster.assign.parallelism` | integer | `4` | Stage-assignment parallelism hint. |
| `rowweave.cluster.stageTimeoutMs` | long (ms) | 60000 | Stage execution deadline. |
| `rowweave.cluster.outputCommitTimeoutMs` | long (ms) | 30000 | Output-commit deadline. |
| `rowweave.cluster.submitTimeoutMs` | long (ms) | 600000 | Client-side ephemeral-submit execution budget. |
| `rowweave.cluster.submitCleanupGraceMs` | long (ms) | 60000 | Client-side cleanup grace. |
| `rowweave.cluster.ephemeralJobTimeoutMs` | long (ms) | 600000 | Master-side ephemeral-job execution budget. |
| `rowweave.cluster.ephemeralCleanupTimeoutMs` | long (ms) | 60000 | Master-side cleanup budget. |
| `rowweave.cluster.jobHardLimitMs` | long (ms) | `0` (disabled) | Legacy admission hard limit; caps the accepted execution budget. |
| `rowweave.heartbeat.staleness.ms` | long (ms) | 5000 | Master declares a Worker lost after this gap. |
| `rowweave.process.memoryLimitBytes` | long (bytes) | *none* | Declared process memory limit for the capacity preflight. |
| `rowweave.process.heapHeadroomBytes` | long (bytes) | *none* | Declared heap headroom for the capacity preflight. |
| `rowweave.process.nativeHeadroomBytes` | long (bytes) | *none* | Declared native headroom for the capacity preflight. |
| `rowweave.master.heartbeatCheckMs` | long (ms) | `clamp(staleness / 3, 100, 1000)` | Master heartbeat-scan interval. |
| `rowweave.master.closeDrainMs` | long (ms) | 5000 | Master shutdown drain grace. |
| `rowweave.netty.shutdownTimeoutMs` | long (ms) | 30000 | Netty transport shutdown deadline. |
| `rowweave.pipeline.deadlockTimeoutMs` | long (ms) | 300000 | Pipeline deadlock watchdog; `<= 0` disables it. |
| `rowweave.portable.actionTimeoutMs` | long (ms) | 30000 | Portable-provider action deadline. |

Invalid-value behavior: timeout values must be positive; an out-of-range
value or a submit budget smaller than its cleanup grace fails before any role
starts.

### 1.7 Master maintenance and recovery

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.master.artifactRetention.maxEntriesPerShard` | integer | 256 | Artifact shard entry capacity. |
| `rowweave.master.artifactRetention.maxBytesPerShard` | long (bytes) | 1 GiB | Artifact shard byte capacity. |
| `rowweave.master.artifactRetention.orphanGraceMs` | long (ms) | 86,400,000 (24 h) | Orphan artifact grace; production minimum 300,000. |
| `rowweave.master.artifactRetention.scanPageSize` | integer | `128` | Artifact scan page size; range 1–1000. |
| `rowweave.master.artifactRetention.maxDeletesPerPass` | integer | `16` | Artifact delete budget per pass; range 1–1000. |
| `rowweave.master.artifactRetention.maintenanceIntervalMs` | long (ms) | 60000 | Artifact maintenance interval. |
| `rowweave.master.runRetention.terminalMs` | long (ms) | 2,592,000,000 (30 days) | Terminal-run retention; production minimum 300,000. |
| `rowweave.master.runRetention.maxEntriesPerShard` | integer | `4096` | Run shard entry capacity; range 1–4096. |
| `rowweave.master.runRetention.scanPageSize` | integer | `128` | Run scan page size; range 1–1000. |
| `rowweave.master.runRetention.maxDeletesPerPass` | integer | `32` | Run delete budget per pass; range 1–1000. |
| `rowweave.master.runRetention.maintenanceIntervalMs` | long (ms) | 60000 | Run maintenance interval. |
| `rowweave.master.recoveryInventory.pageSize` | integer | `128` | Recovery inventory page size. |
| `rowweave.master.recoveryInventory.initialBackoffMs` | long (ms) | 50 | Recovery retry initial backoff. |
| `rowweave.master.recoveryInventory.maxBackoffMs` | long (ms) | 2000 | Recovery retry maximum backoff. |
| `rowweave.master.recoveryInventory.sampleLimit` | integer | `16` | Recovery inventory sample limit. |
| `rowweave.master.recoveryInventory.awaitMs` | long (ms) | 30000 | Recovery inventory wait before giving up. |

### 1.8 Deployment identity and signing

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.deployment.count` | integer | `0` | Number of deployment archives; range 0–4096. |
| `rowweave.deployment.memoryBytes` | long (bytes) | *none* | Deployment runtime memory budget. |
| `rowweave.deployment.trustRoot` | path | *none* | Trusted root for deployment signatures. |
| `rowweave.deployment.probeIntervalMs` | long (ms) | *none* | Deployment probe interval. |
| `rowweave.deployment.signer.count` | integer | `0` | Number of trusted signers; must match the number of `signer.id.<n>` and `signer.path.<n>` entries. |
| `rowweave.deployment.manifest.<n>` | path | *none* | Manifest path for deployment index `n`; indexes 0…`count-1` must be exact. |
| `rowweave.deployment.signer.id.<n>` | string | *none* | Signer identity for index `n`. |
| `rowweave.deployment.signer.path.<n>` | path | *none* | Signer certificate path for index `n`; must be absolute. |

Invalid-value behavior: a manifest or signer index gap, duplicate signer ID,
relative signer path, or count mismatch fails deployment loading before any
provider is created.

### 1.9 Monitor

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.monitor.enable` | boolean | `false` | Start the read-only monitor HTTP service. |
| `rowweave.monitor.bindAddress` | string | `127.0.0.1` | Listener address; `0.0.0.0` is required for non-loopback exposure. |
| `rowweave.monitor.port` | integer | `0` | `0` selects a random port; use an explicit port for a stable scrape target. |
| `rowweave.monitor.ioThreads` | integer | `max(2, cpus / 2)` | Netty event-loop thread count. |
| `rowweave.monitor.history` | integer | `200` | Maximum retained jobs in bounded memory. |
| `rowweave.monitor.allowedOrigins` | string | *none* | Comma-separated exact origins; wildcard origins are rejected. |

See [monitoring](../operations/monitoring.md) and the
[monitor contract](monitor-contract.md) for the route and metric surface.

### 1.10 Stream cluster and worker

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.stream.master.enable` | boolean | `false` | Start the Stream Master role. |
| `rowweave.stream.worker.enable` | boolean | `false` | Start the Stream Worker role. |
| `rowweave.stream.master.port` | integer | `0` | Stream Master RPC bind port; `0` selects ephemeral. |
| `rowweave.stream.worker.port` | integer | `0` | Stream Worker RPC bind port; `0` selects ephemeral. |
| `rowweave.stream.master.workers` | string | *none* | Fixed `host:port` list for direct Master-only mode. |
| `rowweave.stream.state.dir` | path | *none* | Stream checkpoint/savepoint/state root; required for any Stream role. |
| `rowweave.stream.master.artifact.dir` | path | *none* | Shared artifact directory; required for Stream ZooKeeper mode. |
| `rowweave.stream.worker.id` | string | *none* | Stable Worker identity; required when `rowweave.stream.worker.enable=true`. |
| `rowweave.stream.worker.taskSlots` | integer | `64` | Concurrent Stream task slots. |
| `rowweave.stream.worker.channelCapacity` | integer | `64` | Per-channel record buffer bound. |
| `rowweave.stream.worker.maximumFrameBytes` | integer | 4 MiB | Stream frame byte bound. |
| `rowweave.stream.worker.maximumResidentBytes` | long (bytes) | 1 GiB | Stream resident-memory admission. |
| `rowweave.stream.worker.maximumThreads` | integer | `128` | Stream worker thread ceiling. |
| `rowweave.stream.worker.maximumTimerCount` | integer | 1,000,000 | Stream timer ceiling. |
| `rowweave.stream.worker.maximumWindowCount` | integer | 1,000,000 | Stream window ceiling. |
| `rowweave.stream.worker.maximumCheckpointBytes` | long (bytes) | 1 GiB | Checkpoint state admission. |
| `rowweave.stream.worker.maximumSpillDiskBytes` | long (bytes) | 8 GiB | Stream spill disk budget. |
| `rowweave.stream.worker.maximumFileDescriptorCount` | integer | `4096` | Stream FD budget. |

Invalid-value behavior: at least one Stream role must be enabled; a Worker
without an identity, a Master-only mode without fixed Worker endpoints, or a
ZooKeeper mode without a shared artifact directory is refused before any
runtime resource is allocated.

### 1.11 Engine behavior switches

| Property | Type | Default | Effect |
|---|---|---|---|
| `rowweave.expr.codegen` | boolean | `true` | Select the compiled expression engine; `false` forces the interpreted engine. Both engines are bit-identical. |
| `rowweave.key.serialization.projected` | boolean | `true` | Serialize only projected key fields. `false` serializes the full row. |
| `rowweave.local.shuffle.batch` | boolean | `true` | Batch local shuffle channel writes; `false` disables batching. |
| `rowweave.local.runtime.parallelismHint` | integer | `min(16, availableProcessors)` | In-process Batch parallelism hint; range 1–16. |
| `rowweave.join.probeRows` | integer | `4096` | Hash-join probe-row admission threshold. |
| `rowweave.provider.action.parallelism` | integer | `min(graph P, supervisor cap)` | Provider action concurrent-execution capacity. |
| `rowweave.binary.concurrentDrain` | boolean | `true` | Concurrent binary-join drain. |
| `rowweave.binary.drain.maxThreads` | integer | supervisor capacity | Concurrent binary-drain thread cap; negative values use the default. |
| `rowweave.profile.timing` | boolean | `false` | Enable stage-assignment timing diagnostics. |
| `rowweave.cluster.objectReuse` | boolean | `true` | Reuse serialized row objects in Batch pipelines. |
| `rowweave.cluster.materializeFanout` | boolean | `true` | Materialize fan-out references in Batch pipelines. |

## 2. Relationship to monitor metrics

Most properties bound a resource or a deadline that the monitor surfaces as a
gauge or a counter. The mapping below is not exhaustive; see the
[monitor contract](monitor-contract.md) for the route authority.

| Property | Monitor view |
|---|---|
| `rowweave.monitor.*` | Service availability and dashboard routes. |
| `rowweave.worker.slots` / `rowweave.worker.memory` | Capacity gauges and admission state. |
| `rowweave.worker.spill.*` | Spill event counters and disk-pressure diagnostics. |
| `rowweave.control.*` / `rowweave.shuffle.*` | Connection, frame, and handshake counters. |
| `rowweave.master.runRetention.*` / `rowweave.master.artifactRetention.*` | Retention progress in the retention API. |
| `rowweave.cluster.*` timeouts | Job/stage failure reasons and admission counters. |
| `rowweave.stream.worker.*` | Stream worker gauges and checkpoint/state metrics. |

## 3. Scope and invalid-value contract

All properties are read at process startup from system properties unless the
owning component documents an explicit constructor source. A property is
frozen once read; no runtime mutation is supported. An invalid value never
falls back to a default silently — it fails before any resource is allocated,
so the process cannot enter a partially configured state. Diagnostic-only
hooks (`rowweave.test.*`) are not product configuration and are excluded from
this reference.
