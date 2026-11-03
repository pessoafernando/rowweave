---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Capability matrix

Lifecycle and reporting routes are outside this matrix. See the
[Support policy](../../../SUPPORT.md) and
[Security policy](../../../SECURITY.md). This page remains the technical
authority for supported behavior and stable refusals.

This page states what the public release supports today. A plan outside these
contracts fails admission or decoding; it is not silently approximated.

## Batch and pipeline capability map

| Need | Supported contract |
|---|---|
| Map, filter, projection, calculation | expression, lambda, and typed function operators |
| Grouping and aggregation | hash aggregation with budget/spill; whole-table aggregate |
| Join | inner, left, right, full; HASH and SORT_MERGE physical paths |
| Sort, TopN, distinct | bounded breaker operators with null policy |
| Multiple inputs/outputs | one whole-job graph; shared subtrees execute once |
| Schema conversion | explicit validated column mapping and widening |
| Validation | typed row validation with fail-fast error codes |
| Pipeline composition | explicit versioned boundary in `rowweave-pipeline` |

Batch deployment modes are Local, InProcess, and real cluster. The plan graph,
operator implementations, join strategy, and decimal semantics remain the
same across those modes.

Unbounded global sort, unbounded join, non-converging aggregate, and unlimited
group materialization are rejected.

## Connector capabilities

| Contract | Supported boundary |
|---|---|
| JDBC snapshot source | typed snapshot query and source-provider admission |
| PostgreSQL logical-replication CDC source | replication position decoding and bounded incremental state |
| S3-compatible object storage snapshot source | manifest-driven exact-snapshot reads only |
| Custom source and sink providers | contract-checked Batch and Stream provider SPI with ServiceLoader discovery |
| HTTP monitoring | optional dashboard and read-only API with live DAG, stage/instance progress, workers, retention/recovery snapshots, and Prometheus exposition |

Object storage remains a read-only source boundary: exact-snapshot reads only.
A sink, byte-range reads, dynamic enumeration, or an incremental frontier is a
stable refusal, not a partially enabled write path.

## Capability boundaries

The matrix is the authority for today's public contract. The controls below are
not part of the current public contract. They require a separately scoped API,
compatibility, test and release review; this list is not a promise of support.
* Typed ordering and ranked-top convenience methods.
* Per-node single-parallelism controls and public diagnostic/naming APIs.
* Explicit public hash-join key declaration.
* Session-level memory-budget setters.
* Public cleanup-status query APIs.
* Embedded Batch session timeout, asynchronous submission, progress and
  cancellation parity with remote run handles.

## Stream product capabilities

Stream has a closed operator set and one universal task lifecycle. It
supports `CALC`, `FLAT_MAP`, `UNION`, `ASSIGN_EVENT_TIME`, `KEYED_PROCESS`,
`KEYED_CO_PROCESS`, `GENERIC_WINDOW`, and `ASYNC_WAIT`, with bounded keyed
state, timers, event-time windows, bounded temporal join, and bounded async
lookup. Changeshapes include `APPEND`, `UPSERT`, and `RETRACT`.

Deployment modes are Local, InProcess, and cross-host M+NW. Stream supports
dynamic split discovery, aligned and unaligned checkpoints, stop-with-savepoint,
checkpoint-driven live rescale, and resident CDC.

A representative process→window→co-process graph uses the same generic task
factory as every other accepted topology. Savepoint recovery resumes from
recorded state without replaying acknowledged output. An unbounded global
sort/join/aggregate is rejected rather than executed with unbounded resources.

A common requirement mapping is: source process operator, event-time window,
then keyed co-process. Operators compose in one DAG; the runtime does not
substitute a different graph executor based on shape.

## Bounded incremental CDC support matrix

| Contract | State |
|---|---|
| Snapshot descriptor | `SNAPSHOT_V1` |
| Incremental descriptor | `INCREMENTAL_V1` |
| Backfill descriptor | `BACKFILL_V1` |
| Recovery scope | whole-attempt |
| Frontier with incomplete intent | frontier does not advance |
| Visible ordering | after intent |
| Connector qualification | same-build |
| Relationship to Stream | not the Stream Runtime |

Incremental CDC is a bounded Batch source contract with explicit frontier and
failure semantics. It is not a way to submit Batch loops as Stream jobs.

## Stable refusals

* Unbounded global sort, join, aggregate and distinct are absent from the closed
  Stream operator set, so a plan naming one fails decoding with
  `UNKNOWN_OPCODE` and cannot be approximated by accident.
* `RETRACT` changelog output is available only for operators that declare a
  local compensation rule; anything without a stable key, before-image and
  retract-aware identity is refused at the node or edge.
* Object storage providers outside the conformance-tested set are refused.
* The object-storage surface is read-only: a sink, byte-range reads, dynamic
  enumeration and an incremental frontier are not implemented, and SPI v1 covers
  exact-snapshot reads only.
