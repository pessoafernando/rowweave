---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Monitoring

RowWeave exposes monitoring through the optional `rowweave-monitor` module.
The monitor is read-only: it accepts no job submission and performs no
configuration mutation. Monitoring failures are advisory and never delay,
fail, or block execution.

Enable it with `rowweave.monitor.enable=true` and place the module on the
application, Master, or embedded-runtime classpath. Without the module, the
engine runs with the no-operation registry and has no HTTP monitor overhead.
The route, field, and metric definitions in this page are operation shortcuts
to the authoritative [monitor contract](../reference/monitor-contract.md).

## Start and locate the monitor

Set the listener before constructing or submitting work:

```bash
-Drowweave.monitor.enable=true
-Drowweave.monitor.bindAddress=127.0.0.1
-Drowweave.monitor.port=18080
-Drowweave.monitor.ioThreads=2
-Drowweave.monitor.history=200
```

With port `0`, Netty assigns a port. The server logs
`monitor HTTP started port=<port>`, and `GET /api/config` returns the effective
port without exposing credentials or filesystem paths. For a stable scrape
target, assign an explicit port. Record it with the role's deployment manifest.

A monitor module that is absent, fails to load, or fails to bind leaves the
engine on the no-operation registry. Diagnose the process with the ordinary
readiness markers and logs; do not infer execution health from monitor
availability.

## Dashboard

The dashboard uses hash routing and includes Overview, Jobs, Workers,
Retention, and Config views. It has no external framework, CDN, or build step.
The browser polls the job and worker APIs every 1.5 seconds, with at most one
refresh cycle in flight. Overview counts open the corresponding job filter;
recent jobs open the execution graph directly. Search jobs by name or ID and
filter by state without changing the selected job.

The header shows connection state and the time of the last successful browser
refresh. That time is receipt time, not a source watermark or proof of worker
health. Pause freezes automatic dashboard updates; it does not pause jobs.
Refresh fetches one snapshot even while updates are paused. Resume restarts
automatic observation. Failed requests retain the last successful graph and
show a disconnected notice with Retry now. Requests time out after eight
seconds. An expired job is shown as unavailable rather than as a stale running
job.

Stream observations update independently of application status queries. The
Master publishes the admitted DAG, task placement and accepted node samples;
open its dashboard for node and parallel-instance details. A remote client's
local monitor exposes job-level evidence. Enabled clients also own one bounded
250 ms sampler; close the client to release that daemon thread. A no-operation
registry creates no sampling thread.

The Jobs view is DAG-first. Selecting a job opens a live SVG execution graph:

* Each stage node shows operator names, effective parallelism, state,
  elapsed duration, and processed-to-output row progress.
* Node colors distinguish running, finished, failed, and pending stages.
  A colored stripe on the left edge of each node repeats its state, and
  hovering a node highlights its connected edges while dimming the rest.
* Edge styles show local, mixed, or remote channels; materialized breakers
  are marked explicitly. Edges attached to a running stage animate a flowing
  dash pattern; the animation is disabled when the browser prefers reduced
  motion.
* Polling updates the graph when runtime values change while preserving
  pan, zoom, and manually moved node positions.

Select a node or a row in Stages to open Node details. Selecting a stage from
the table also centers its node in the graph. Fit graph shows the complete
DAG; the zoom percentage, zoom buttons, and pointer-centered wheel zoom help
inspect large graphs. Reset restores node positions and a readable scale.
The details panel can be collapsed or resized. On narrow screens it starts
collapsed so the graph remains available.

Tab reaches the graph nodes and detail tabs. Enter or Space selects a node;
arrow keys move between nodes. Within the detail tab bar, Left/Right and
Home/End select tabs. On the graph background, arrows pan, plus/minus zoom,
and Home fits the graph. The details separator supports Up/Down resizing.
Selection, keyboard focus, moved nodes, and the viewport survive refreshes.

Node details shows operator state, rows, timing, upstream stages, and errors.
It also lists each parallel instance with its
state, worker, rows, start time, and duration. The stage table provides the
same runtime summary for every stage. Separate tabs show job metrics and
exceptions. Failed jobs offer a View failures shortcut; stage exceptions
provide a Locate stage action. Job info keeps identifiers, timing, parallelism,
Batch sink evidence, and worker observations available without narrowing the
graph. The live DAG is the primary operational view; counters and
tabular views explain the graph, they do not replace it.

## HTTP API and JSON contract

All monitor endpoints are read-only `GET`; other methods return
`405 Method Not Allowed` with `Allow: GET`. Unknown paths return
`404 Not Found` as JSON. A standby monitor redirects to the active leader with
`302 Found`; when the leader is unknown it returns
`503 Service Unavailable`. JSON responses use `application/json; charset=UTF-8`.

| Endpoint | Purpose and key fields |
|---|---|
| `/` and `/index.html` | Single-file dashboard. |
| `/api/overview` | `jobsTotal`, `jobsRunning`, `jobsFinished`, `jobsFailed`, `workersTotal`, and `workersAlive`. |
| `/api/jobs` | Bounded summaries: `jobId`, `state`, `submitTime`, `endTime`, `stageCount`, `desiredP`, `effectiveP`, `downgraded`, and `error`. |
| `/api/jobs/{jobId}` | Full detail plus `dag`, stage `instances`, channel evidence, execution telemetry, and a typed `stream` projection. |
| `/api/jobs/{jobId}/exceptions` | Failed jobs retained in bounded history: `jobId`, `state`, `endTime`, and `message`. |
| `/api/workers` | Worker snapshots described in the next section. |
| `/api/metrics` | Prometheus text exposition, `text/plain; version=0.0.4; charset=utf-8`. |
| `/api/config` | Effective `bindAddress`, `port`, `history`, and exact CORS `allowedOrigins`. |
| `/api/health` | `status`, `jobs`, and `workers`; liveness of the monitor process, not job success. |
| `/api/retention` | Run-retention snapshot; `{}` means no snapshot has been published. |
| `/api/artifact-retention` | Artifact-retention snapshot; `{}` means no snapshot has been published. |
| `/api/retry-recovery` | Retry and recovery snapshot; `{}` means no snapshot has been published. |

Job detail contains a structured `dag` with `nodes` and `edges`. Each stage
carries role, state, operator chain, input/output rows, times, parallelism,
worker assignment, channels, and parallel `instances`. Batch jobs serialize
the typed `stream` projection as `null`; Stream jobs project current-attempt
and lifetime progress there.

The current Stream metric producer reports checkpoints, watermarks, restarts,
records and bytes read/written, backpressure duration, backlog, mailbox and
channel high-water marks, managed state, timer and window high-water marks,
and CDC phase. The full catalog and field model are in the
[monitor contract](../reference/monitor-contract.md#metric-producers).

Batch progress is derived from typed job, stage, instance, sink, worker,
retention, recovery, and terminal-report snapshots. Business throughput comes
from source input and sink output, not from summing intermediate-stage traffic.

## Worker observations

`/api/workers` and the dashboard Workers view show the latest registry
snapshot:

* `workerId`, `host`, and `port` locate the advertised control/shuffle
  endpoint.
* `slots` and `freeSlots` show capacity at snapshot time.
* `managedMemoryUsedBytes` and `managedMemoryMaxBytes` show managed-memory
  pressure; compare both rather than reasoning from heap use alone.
* `startTimeMillis` distinguishes a restarted process from a long-lived one.
* `alive` reflects the registry's latest heartbeat determination.

A worker may disappear from the list before the runtime admits a failure, or
remain listed as alive while a stage is failing for another reason. Correlate
the worker snapshot with job stage instances, exceptions, Master/Worker logs,
and typed execution outcomes before taking action.

## Retention and recovery observations

`/api/retention` reports the latest bounded run-retention pass: `backlog`,
`lifecycleCapacityPerShard`, `scanPageCapacity`,
`oldestTerminalAgeMillis`, `claimedRuns`, `purgedRuns`, `errorRuns`,
`cycleComplete`, and up to 16 `diagnosticSamples`. `backlog` is a page-scoped
observation, not a globally exact total. Investigate repeated incomplete
cycles or rising `errorRuns`, then compare with Master logs and store health.

`/api/artifact-retention` reports artifact backlog and inventory pressure,
including inventory entries and byte capacity, retained referenced artifacts,
stale owners, errors, not-ready passes, `referenceStatus`, `inventoryStatus`,
and bounded diagnostic samples. Treat a reference or inventory status that is
not healthy as evidence for diagnosis; do not manually delete artifacts based
on the dashboard value alone.

`/api/retry-recovery` reports `state`, `activeRuns`, `waitingRetries`,
`nextDueDelayBucket`, `scheduledRetries`, `exhaustedRetries`,
`notRetryableClosures`, `maintenanceCursorHealth`, and bounded diagnostic
samples. A blocked state or exhausted retry count should route to the recovery
runbook. Monitoring does not trigger recovery, cancel work, or reset retry
state.

## Security boundaries

The monitor has no built-in authentication or authorization. Treat it as a
read-only data surface and place it on a private observer network or behind an
authenticated proxy before exposing it beyond a single operator workstation.

| Setting | Default and boundary |
|---|---|
| `rowweave.monitor.enable` | `false`; HTTP is opt-in. |
| `rowweave.monitor.bindAddress` | `127.0.0.1`; only explicit `0.0.0.0` exposes the socket beyond loopback. |
| `rowweave.monitor.port` | `0` selects a random port; use an explicit port for stable admission control. |
| `rowweave.monitor.ioThreads` | `max(2, availableProcessors / 2)` Netty event-loop threads. |
| `rowweave.monitor.history` | `200` jobs; oldest terminal observations are evicted first. Active observations remain retained. |
| `rowweave.monitor.allowedOrigins` | Empty; comma-separated exact origins only, with wildcards rejected. |

CORS reflects an allowed origin only when the request carries that exact
`Origin`. `/api/config` exposes only sanitized listener settings. Keep
credentials, job payloads, and local filesystem paths out of this surface.

If every history slot contains an active job, new observations are skipped;
job execution continues. Increase the capacity for the expected concurrency.
Submission and completion times remain stable as new observations arrive.

## Prometheus operations

`/api/metrics` emits the current bounded registry snapshot in Prometheus text
format, version 0.0.4. Metric names are prefixed with `rowweave_`, dots become
underscores, tags become labels, and gauge/counter kinds are preserved. The
registry stores latest values only; it does not provide an internal
time-series history or histogram buckets.

Use an external Prometheus or compatible scraper for history and alerting:

```yaml
scrape_configs:
  - job_name: rowweave
    static_configs:
      - targets: ["127.0.0.1:18080"]
    scrape_interval: 15s
```

In service discovery, export the resolved value as `rowweave_monitor_port`
when the listener uses an assigned port. Scrape every monitor process that can
be active; a standby normally redirects, so configure the scraper to follow
redirects or scrape a known active address. Alert on the absence of fresh
monitor data separately from job failure alerts.

The shipped canonical metric names are:

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

Metric collection is exception-isolated. A producer failure must not change
the data or control path that decides job execution.

### Bounded incremental frontier

Call `queryIncrementalFrontier(feedId)` to inspect the published frontier and
`activeRunId`. A nonempty active attempt means the frontier is provisional for
operations still converging. Use this field to diagnose liveness; do not use it to advance or reset the frontier.

For incident triage, compare the published frontier with source and sink
progress. A job that ends with an unexplained frontier gap is not successful;
treat it as a recovery or connector failure and preserve the attempt evidence.

## Troubleshoot absent or stale data

| Symptom | Check | Likely action |
|---|---|---|
| No HTTP listener | Is `rowweave.monitor.enable=true` set, and is `rowweave-monitor` on the classpath? | Correct the setting or classpath and restart the role; do not treat prior jobs as failed solely because monitoring was absent. |
| Configured URL refuses connection | Does `GET /api/config` or the startup log show the effective port? Did the bind address change? | Correct the scrape target or bind address; reserve an explicit port for stable targets. |
| Standby returns `503 Service Unavailable` | Is election healthy, and is a leader address published? | Repair election or discovery with the registry/cluster runbook; monitor standby behavior remains advisory. |
| Dashboard is stale | Open the Network inspector, confirm `/api/jobs` and `/api/workers`, and check exact CORS origins. | Refresh from the resolved origin, add the browser origin to the exact allow-list, and restart only if the client state remains unusable. |
| A job disappears | Does `/api/health` show a falling job count, and was `rowweave.monitor.history` exceeded? | Increase bounded history if needed; export snapshots externally for post-incident analysis. |
| Metrics are absent | Does `/api/jobs/{jobId}` show the typed projection while `/api/metrics` is empty? | Confirm that the relevant producer is wired for that runtime; do not invent metrics from absent names. |
| Prometheus history stops | Check scrape-target health, redirects, network policy, and the last `up` timestamp. | Repair the observer network or scrape configuration; use runtime readiness and job status, not monitor freshness, to classify execution. |
| Worker seems missing | Compare `/api/workers` with stage instances and heartbeat logs. | Treat a snapshot mismatch as one signal; decide only after correlating typed execution evidence. |

When evidence is incomplete, continue with runtime logs, typed outcomes, and
recovery state. Monitoring failures are advisory and never change job
admission, execution, retry, or terminal status; do not treat a missed scrape as job failure evidence.
