---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Monitor contract

This page defines the public monitoring contract: what data the engine
exposes, how the HTTP surface is structured, what the dashboard renders, and
what security defaults apply. It is the authority for implementing and
extending the `rowweave-monitor` module.

## Design invariants

- Monitoring is **advisory**: the engine pushes observations to a registry;
  failures in collection never block, fail, or delay a job.
- `rowweave-core` depends on the `JobMonitorRegistry` interface and
  `InMemoryJobMonitor`. HTTP, dashboard, and Prometheus exposition live in the
  optional `rowweave-monitor` module discovered through `ServiceLoader`.
- Without the monitor JAR on the classpath, the engine runs identically with
  zero HTTP overhead.
- All monitor Java source, dashboard text, configuration keys, and this
  document are English-only.
- Metric and endpoint names use generation-neutral vocabulary. Predecessor
  labels, generation suffixes, or internal task identifiers must not appear
  in monitor code, wire values, or user-facing text.

## Metric model

### Kinds

| Kind | Description | Prometheus type |
|---|---|---|
| `GAUGE` | Point-in-time value that can increase or decrease. | gauge |
| `COUNTER` | Monotonically non-decreasing value since job start. | counter |

### Metric value

Each metric value carries:

| Field | Type | Description |
|---|---|---|
| `name` | `String` | Dot-separated metric name. |
| `kind` | `GAUGE` or `COUNTER` | How the value should be interpreted and exported. |
| `value` | `double` | The observation. |
| `unit` | `String` | One of `bytes`, `rows`, `events`, `milliseconds`, `count`, or `ratio`. |
| `timestamp` | `long` | Epoch milliseconds when the observation was taken. |

### Scopes and naming

A metric name is lowercase, dot-separated, and uses snake_case segments. The
first segment identifies its scope. A unit suffix is explicit. The shipped
scope is `stream`; the model also permits `job`, `stage`, `worker`, `cluster`,
and `source` as producers are wired. Canonical examples include
`stream.checkpoints_completed`, `stream.operator_watermark_millis`, and
`stream.managed_state_bytes`.

### Storage

`InMemoryJobMonitor` stores the latest value for each
`(scope, tags, metric name)` triple in a bounded map. It does not accumulate
time-series history. Long-term history is delegated to an external Prometheus
or compatible scraper that pulls `/api/metrics`.

The job history has a hard capacity. It retains admitted active jobs and evicts
the oldest terminal observation to make room for a new job. If every slot is
active, new observations are skipped until a slot becomes available; execution
continues. Size `rowweave.monitor.history` for the expected concurrent jobs.
Updates preserve the first submission time and first terminal time.

## HTTP API surface

All endpoints are read-only `GET`. Non-GET methods return
`405 Method Not Allowed`.

| Endpoint | Content type | Description |
|---|---|---|
| `GET /`, `GET /index.html` | `text/html` | Dashboard single-page application. |
| `GET /api/overview` | `application/json` | Job and worker count summary by state and liveness. |
| `GET /api/jobs` | `application/json` | Bounded job list. |
| `GET /api/jobs/{jobId}` | `application/json` | Job detail: stages, instances, channels, errors, execution telemetry, and Stream projection. |
| `GET /api/jobs/{jobId}/exceptions` | `application/json` | Failed-job entries retained in the bounded history; the path must name a retained job. |
| `GET /api/workers` | `application/json` | Worker list with slots, memory, and liveness. |
| `GET /api/metrics` | `text/plain; version=0.0.4` | Prometheus text exposition. |
| `GET /api/config` | `application/json` | Sanitized public listener configuration. |
| `GET /api/health` | `application/json` | Liveness response. |
| `GET /api/retention` | `application/json` | Run-retention snapshot. |
| `GET /api/artifact-retention` | `application/json` | Artifact-retention snapshot. |
| `GET /api/retry-recovery` | `application/json` | Retry and recovery snapshot. |

The monitor does not accept job submission, configuration mutation, or any
write operation.

### Job detail

A job detail response contains the common job identity and state, a stage
projection, optional Batch sink and execution telemetry, optional Stream
progress, and a structured `dag`. Each stage carries role, state, operator
chain, input/output rows, times, worker assignment, channel descriptions,
parallelism, and parallel-instance detail. Each instance carries index,
worker, state, input/output rows, times, and error text.

Stream evidence is projected into an `attempt` object for the current attempt
and a `lifetime` object for cumulative progress. Batch jobs serialize
`stream` as `null`.

### Prometheus exposition

`/api/metrics` emits the current registry snapshot in Prometheus text format.
Metric names are prefixed with `rowweave_`; dots become underscores. Tag names
and values are escaped, and only gauge or counter types are emitted. An empty
registry produces an empty body.

## Security defaults

| Setting | Default | Purpose |
|---|---|---|
| `rowweave.monitor.enable` | `false` | Monitor HTTP is opt-in. |
| `rowweave.monitor.bindAddress` | `127.0.0.1` | Loopback by default; only explicit `0.0.0.0` may expose the listener beyond loopback. |
| `rowweave.monitor.port` | `0` | Random port; set an explicit port for a scrape target. |
| `rowweave.monitor.ioThreads` | `max(2, availableProcessors / 2)` | Netty event-loop threads. |
| `rowweave.monitor.history` | `200` | Bounded job history; least-recently-used jobs are evicted. |
| `rowweave.monitor.allowedOrigins` | empty | Comma-separated exact CORS allow-list. Wildcard origins are rejected. |
| HTTP methods | `GET` only | Other methods return `405` with `Allow: GET`. |

A standby monitor redirects to the active leader. If the leader address is not
available, it returns `503 Service Unavailable`.

## Metric producers

The current shipped producer is the Stream monitor bridge. It converts public
`StreamJobEvidence` into job snapshots and a typed `stream` metric set. The
Batch runtime publishes job, stage, instance, sink, retention, recovery, and
worker snapshots directly; its dashboard progress is derived from those typed
snapshots and the terminal execution report, not from a separate periodic
metric sampler.

An enabled Stream client owns one daemon sampler, created on first observation
and closed with the client. It samples at 250 ms intervals and tracks at most
`min(4096, historyLimit)` nonterminal handles (with a minimum of one). The
no-operation registry creates no sampler. Terminal handles are removed from
sampling; the registry retains their bounded snapshots.
Closing a client stops future samples without interrupting a lifecycle or
file-authority call already in progress; that call follows its normal transport
deadline and owner shutdown.

The Stream Master also publishes accepted, fenced task samples from its normal
runtime observation cycle. Its dashboard contains the admitted DAG, physical
worker placement, per-node counters and parallel instances. Task completion is
derived from lifecycle evidence, independently of telemetry completeness.
Node counters describe the current run attempt; job metrics retain lifetime
totals across restarts. Stage start/end times are observation times, rather than
worker-clock timestamps. On job termination, unfinished instances project the
job outcome while instances already known to have finished retain that state.

Run the detailed Stream dashboard on the Master. A client in another JVM
receives job-level evidence through the public control protocol; it does not
receive per-task placement or counters through that protocol.

Canonical Stream metrics are:

| Metric | Kind | Unit |
|---|---|---|
| `stream.checkpoints_completed` | COUNTER | count |
| `stream.attempt_checkpoints_completed` | COUNTER | count |
| `stream.operator_watermark_millis` | GAUGE | milliseconds |
| `stream.cdc_phase` | GAUGE | count |
| `stream.restarts` | COUNTER | count |
| `stream.records_read` | COUNTER | events |
| `stream.records_written` | COUNTER | events |
| `stream.bytes_read` | COUNTER | bytes |
| `stream.bytes_written` | COUNTER | bytes |
| `stream.backpressured_millis` | COUNTER | milliseconds |
| `stream.backlog_events` | GAUGE | events |
| `stream.backlog_bytes` | GAUGE | bytes |
| `stream.mailbox_high_water_events` | GAUGE | events |
| `stream.channel_high_water_bytes` | GAUGE | bytes |
| `stream.managed_state_bytes` | GAUGE | bytes |
| `stream.timer_high_water_count` | GAUGE | count |
| `stream.window_high_water_count` | GAUGE | count |
| `stream.last_checkpoint_duration_millis` | GAUGE | milliseconds |
| `stream.placement_task_count` | GAUGE | count |
| `stream.reported_task_count` | GAUGE | count |

Metric collection is exception-isolated. A producer failure is logged or
ignored by the caller and never reaches the data or control path that changes
job execution.

## Dashboard information architecture

The dashboard is a single-file HTML application with client-side hash routing.
No CDN, build tool, or external framework is used.

| Tab | Content |
|---|---|
| Overview | Job/worker counters, job-state totals, workers, memory, and latest failures. |
| Jobs | Bounded job list and the live DAG workspace for the selected job. |
| Workers | Worker ID, address, slots, free slots, managed memory, and ALIVE/OFFLINE status. |
| Retention | Run retention, artifact retention, and retry/recovery snapshots. |
| Config | Sanitized configuration from `/api/config`. |

### Live DAG rendering

The Jobs view is the primary operational surface. It renders the job DAG as an
interactive SVG graph and polls the selected job every 1.5 seconds.

| Element | Live binding |
|---|---|
| Node color | Stage state: running, finished, failed, or pending. |
| Node title | Operator chain, stage index, and effective parallelism. |
| Node duration | Elapsed wall time, frozen on terminal state. |
| Node footer | Processed-to-output row progress. |
| Edge style | `local`, `mixed`, or `remote` channel. |
| Breaker marker | A stage whose implementation materializes a bounded working set. |

Polling compares a runtime signature and redraws only when state, rows, times,
parallelism, channels, instances, or outcome evidence changes. Pan, zoom, and
manual node positions are preserved.

### Node detail

Clicking a node opens the Stage details tab with:

- Stage state, role, operator chain, and upstream indexes.
- Processed and output rows, including a soaking indicator for a running
  breaker that has consumed rows but not yet emitted.
- Start time and elapsed duration.
- Source split count, replay mode, and descriptor digest prefix when present.
- Error text when the stage fails.
- One row for every parallel instance with instance index, state, worker,
  input/output rows, start time, and duration.

The Stage table tab shows the same core runtime fields for all stages. The
Metrics tab summarizes source input, sink output, stage states, Batch
execution telemetry, and Stream progress. The Exceptions tab collects job,
stage, and instance errors.

## Vocabulary

| Term | Definition |
|---|---|
| Job | A submitted Batch or Stream execution tracked in the bounded registry. |
| Stage | A pipeline stage or task group within the job DAG. |
| Instance | A parallel instance of a stage running on a worker. |
| Worker | A JVM process that executes stages. |
| Attempt | A retry or re-execution of a Stream job. |
| Checkpoint | A consistent state snapshot taken by the Stream runtime. |
| Watermark | Event-time progress indicator in the Stream runtime. |
| Backpressure | Upstream slowness reflected by Stream duration or high-water evidence. |
| Spill | Writing bounded breaker state to disk when a memory budget is exceeded. |

Generation labels, predecessor product names, internal task identifiers, and
non-English text must not appear in this surface.
