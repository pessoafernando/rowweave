---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Run modes

Choose a mode by the process and recovery boundary you need to test. Local mode
is the application embedding path. InProcess mode exercises cluster placement
without sockets where a public runtime is available. Separate-process cluster modes
split Master, Worker, and Client roles and are the production deployment path.

Batch and Stream use separate clients and lifecycles. A Batch job submitted on
a timer is not Stream execution, and no mode changes plan semantics, operator
contracts, decimal precision, or stable refusals.

## Batch modes

| Mode | Public entry point | Process model | Choose when |
|---|---|---|---|
| Local | `RowWeave.createEnvironment` with the default client | One JVM | Embedding a bounded job in an application, evaluating the API, or running a small deterministic job. |
| InProcess cluster | `RowWeave.startInProcessCluster(parallelism, stateDirectory)` | Master and Worker product roles in one JVM | Testing cluster admission, placement, and whole-graph behavior without deploying separate processes. |
| Cluster | `RowWeave.createClusterJobClient(...)` with `io.rowweave.cluster.ClusterMain` | Separate Client, Master, and Worker JVMs | Running jobs across processes and hosts, scaling workers, or using production deployment and monitoring. |

`InProcessCluster` is `AutoCloseable`. Give its constructor a clean state
directory and a worker-slot hint at least as large as the job parallelism. The
runtime expands that hint to a safe local slot budget; insufficient placement
capacity is admitted or refused by the same cluster rules as a separate-process cluster.

`RunModesExample` runs the same Local and InProcess graph and compares the
result files. Its cluster method is explicitly a sketch: it does not start
Master or Worker processes.

## Stream: Local / InProcess / separate-process cluster

| Mode | Public entry point | Process model | Choose when |
|---|---|---|---|
| Local | `RowWeave.createLocalStreamJobClient` | Master, Worker, and authority roles in one JVM with no required listener | Embedding a Stream graph, exercising checkpoint or restore behavior, or testing a provider before cluster deployment. |
| InProcess parity | No public application SDK type | Master and multiple Worker product roles in one JVM | Deterministic runtime parity and qualification tests. It is not an application entry point. |
| Cluster | `RowWeave.createClusterStreamJobClient`, `RowWeave.startStreamMaster`, `RowWeave.startStreamWorker`, and `io.rowweave.StreamClusterMain` | Separate Master-only and Worker-only JVMs | Production Stream deployment, independent workers, durable state, savepoint, rescale, and resident CDC. |

Local Stream still assembles the same portable DAG, operator task factory,
input gates, output router, and state/time/checkpoint lifecycle as cluster
mode. It is a single-JVM deployment of the product roles, not a separate
interpreter.

Separate-process Stream supports Master-only and Worker-only processes. A Master-only
process never executes tasks; a Worker-only process does not schedule jobs.
Fixed-endpoint and ZooKeeper discovery are deployment choices. An external
process role requires the mTLS security profile; see
[deployment](../operations/deployment.md) and the
[Stream runbook](../../runbooks/stream-runtime.md).

## Dependencies

All modes require `rowweave-core`. `rowweave-registry-zk` is required only for
ZooKeeper-backed discovery and Master election. `rowweave-monitor` is optional
in every mode. Connectors are added only when the graph uses their providers;
connector POMs mark core `provided`, so the application must also declare core.

## Guarantees and boundaries

* A mode never converts an unsupported graph into a supported one. Admission,
  capability checks, resource checks, and stable refusals remain authoritative.
* `execute()` and Stream submission do not return rows to the Batch or Stream
  client. Sinks run at the graph boundary; in a cluster the Worker writes the
  sink.
* For Batch jobs that run outside the submitting JVM, use a worker-writable
  sink such as `FileSink` and read the result through a source or external
  system. Do not rely on an in-process collector.
* Stream recovery requires a completed checkpoint or savepoint. Do not infer a
  recovery point from submission, command acceptance, or a running state.
* Mode parity applies to accepted plan behavior and result contracts. Timing,
  worker placement, retries, and resource pressure differ by process topology.

## Diagnostics

Enable the optional monitor on the application, Master, or embedded-runtime
classpath with `rowweave.monitor.enable=true`. It provides the read-only job
DAG, stage and instance progress, workers, Stream progress, retention and
recovery views, health, and Prometheus surface described in
[monitoring](../operations/monitoring.md). Monitoring is advisory: it never
blocks, retries, or completes a job.

Without a monitor, diagnose from the submitted result, typed failure,
checkpoint/savepoint status, and worker logs. For Batch InProcess,
`InProcessCluster.Diagnostics` exposes listener, handler, and data-listener
counts without opening a network surface. For cluster operations, verify
Master/Worker readiness, resource envelopes, durable state availability, and
retained job exceptions using the deployment and runbook procedures.

## Mode selection checks

### Mode-to-property routing

Each mode's production knobs and the pages that own them. Property names are
defined in [configuration](../reference/configuration.md); no mode introduces
a property outside that reference.

| Mode | Key configuration | Authoritative pages |
|---|---|---|
| Batch Local | None required; the default in-process runtime needs only the application classpath | [First job](../quickstart/first-job.md), [configuration](../reference/configuration.md) |
| Batch InProcess cluster | Constructor parallelism hint and a clean state directory; these are builder arguments, not properties | [Deployment](../operations/deployment.md) |
| Batch cluster | `rowweave.registry`, `rowweave.zk.connectString`, `rowweave.process.memoryLimitBytes`, `rowweave.worker.spill.dir` | [Deployment](../operations/deployment.md), [ZooKeeper registry](../reference/registry-zookeeper.md), [Batch process roles](../reference/configuration.md#11-batch-process-roles) |
| Stream Local | `rowweave.monitor.enable` is optional; no listener configuration | [Stream API reference](../reference/stream-api.md), [Stream first job](../quickstart/stream-first-job.md) |
| Stream cross-host cluster | `rowweave.stream.master.enable`, `rowweave.stream.worker.enable`, `rowweave.control.securityProfile` set to `MTLS`, registry discovery | [Deployment](../operations/deployment.md), [security](../operations/security.md), [Stream cluster and worker](../reference/configuration.md#110-stream-cluster-and-worker) |

Before promotion from Local or InProcess to a separate-process cluster, run the graph with
the production sink, resource envelope, parallelism, connector deployment set,
security profile, and recovery policy. A successful Local run does not certify
cross-host transport, Worker capacity, durable-state ownership, or mTLS
identity. Re-run admission and recovery checks in the target mode.

## Examples

See [example index](examples.md) for tested Batch, Stream, pipeline, connector,
and run-mode entry points.
