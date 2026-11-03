---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Stream first job

This Stream job reads two ordered event-time records, keys them by
`categoryKey`, keeps a one-second tumbling window pending across a manual
checkpoint, restores that checkpoint after cancellation, and emits one
aggregated row when the final watermark arrives. It is the fastest way to see
RowWeave's separate Stream API, typed window binding, checkpoint authority, and
safe cancellation.

The checked-in implementation is
[`StreamLocalEventTimeRecoveryExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamLocalEventTimeRecoveryExample.java).
Its test asserts the exact result shown below.

## Prerequisites

Complete [installation and modules](installation.md) and build the source
reactor. This example uses only core Stream APIs and provider SPIs. It does not
need an optional connector, registry, or monitor.

## Run the tested example

Run the focused test:

```bash
mvn -q -pl rowweave-examples test -Dtest=StreamLocalEventTimeRecoveryExampleTest
```

The example class exposes a Java API entry point rather than a command-line
`main`. Copy from it when you add Stream execution to an application.

## Create the local client and session

A local Stream client runs Master, Worker, and authority roles in one JVM. The
first argument is the durable state root; the next arguments install the
example source and sink providers. The try-with-resources block closes the
client after the restored job stops.

```java
StreamJobClient client = RowWeave.createLocalStreamJobClient(
        stateRoot,
        Collections.singletonList(source),
        Collections.singletonList(sink),
        Collections.emptyList());
StreamSession session = RowWeave.createStreamSession(
        "local-event-time-recovery",
        "local-event-time-recovery",
        client);
```

This Java fragment continues the preceding example and omits imports. The
complete file shows every import and lifecycle call.

The session enables event time and manual aligned checkpoints only. A manual
checkpoint waits up to five seconds and retains two completed checkpoints:

```java
session.setTimeMode(StreamTimeMode.EVENT_TIME);
session.setCheckpointConfig(StreamCheckpointConfig.manualOnly(5_000L, 2));
```

The explicit resource profile bounds processors, memory, network windows, and
transfers for the example. Keep an equivalent envelope in production; admission
fails closed when a graph cannot satisfy its declared resource contract.

## Define the event schema

The input schema has a non-null category, a non-null timestamp, and a nullable
decimal quantity:

```java
StreamSchema inputSchema = new StreamSchema(Arrays.asList(
        new StreamField("categoryKey", "categoryKey", "categoryKey",
                DataType.StringType, false),
        new StreamField("event-time", "eventTime", "eventTime",
                DataType.TimestampType, false),
        new StreamField("quantity", "quantity", "quantity",
                DataType.BigDecimalType, true)));
```

Field IDs, logical names, and stable names are explicit. The window binds to
`event-time` and `quantity`; the sink reads output by its declared position.

## Compose the graph

The exact ordered source emits a position and timestamp with each record and
uses provider events for watermarks. The graph keys by `categoryKey`, applies a
one-second tumbling decimal window, and writes to the named sink:

```java
DataStream keyed = session.fromSource(
                StreamSourceSpec.exactOrderedEventTime(
                        inputSchema, sourceId, sourceContract,
                        new byte[] {1},
                        Collections.singletonList(splitSpec),
                        StreamWatermarkPolicy.providerEvents(5_000L)))
        .uid("source")
        .keyBy("categoryKey")
        .uid("category-key");
keyed.eventTimeDecimalWindow(
                StreamWindowSpec.tumblingDecimal(
                        "event-time", "quantity", 1_000L,
                        "window-start", "window-end", "total"),
                outputSchema,
                StreamStateSpec.decimalWindow("category-quantity-window"))
        .uid("category-quantity-window")
        .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                sinkId, sinkContract, new byte[] {3}));
```

This fragment continues the preceding examples. `uid` values identify nodes in
the portable graph and recovery state. The window output schema declares two
epoch-millisecond bounds and the decimal total. Every field in
`StreamWindowSpec` must resolve by field ID and type; the compiler rejects a
missing or mistyped binding before submission.

## Checkpoint, cancel, and restore

Submit the graph, wait until the job is running, and wait until the example
source signals that both records have created a pending window. Then trigger a
checkpoint and require its completion before cancellation:

```java
StreamJobHandle initial = session.submit(
        StreamSubmissionId.of("example-event-initial"));
requireRunning(initial.awaitRunning(5_000L), "initial job");
source.awaitPendingWindow(5_000L);

StreamCheckpointHandle checkpoint = initial.triggerCheckpoint(
        StreamCommandId.of("example-event-window-cut"));
requireSucceeded(checkpoint.awaitCompletion(5_000L), "window checkpoint");
requireSucceeded(initial.cancel(
        StreamCommandId.of("example-event-initial-cancel"))
        .awaitCompletion(5_000L), "initial cancel");
```

Restore from that completed checkpoint by naming the original job, recovery
point, target parallelism, and maximum parallelism. After the restored job is
running, release the final watermark and wait for the window result:

```java
StreamJobHandle restored = client.restore(
        StreamSubmissionId.of("example-event-restored"),
        session.toPlanArtifact(),
        new StreamRestoreRequest(
                initial.id(),
                StreamRecoveryPointId.checkpoint(checkpoint.checkpointId()),
                1,
                128));
requireRunning(restored.awaitRunning(5_000L), "restored job");
source.releaseFinalWatermark();
sink.awaitRows(1, 5_000L);
requireSucceeded(restored.cancel(
        StreamCommandId.of("example-event-restored-cancel"))
        .awaitCompletion(5_000L), "restored cancel");
```

The completed checkpoint manifest is the recovery authority. Do not infer
completion from the `triggerCheckpoint` return value alone; use
`awaitCompletion` and require `SUCCEEDED`. Canceling before that point can
discard the pending window state that the example intends to restore.

## Expected result

The test observes exactly one row:

```text
category-a:0:1000:3.0000000001
```

The fields are category, window start, window end, and total. The source
quantities are `1.00000000006` and `2.00000000005`. Their exact sum is
`3.00000000011`; the built-in final decimal window rounds once at the scale
stated in [decimal and null
semantics](../reference/decimal-null-semantics.md), producing `3.0000000001`.

## Common Stream first-job problems

* **No prior key.** An event-time window requires a preceding `keyBy`. The
  compiler fails with `MISSING_STABLE_KEY`.
* **Wrong window binding.** Event-time, value, boundary, and result fields must
  exist with the declared types. A mismatch fails with
  `INVALID_WINDOW_FIELD_BINDING`.
* **Cancel during a checkpoint.** Wait for `checkpoint.awaitCompletion` to
  return `SUCCEEDED` before canceling. A canceled or failed checkpoint is not a
  restore authority.
* **Restore before running.** Use the original `StreamJobHandle.id()` and the
  completed `StreamCheckpointHandle.checkpointId()`. Do not restore from a
  request identifier.
* **No window output.** The example deliberately withholds the final watermark
  until after restore. Releasing it too early makes the result a normal run
  rather than a recovery demonstration.

## Next steps

* Review the production recovery boundary in the
  [Stream runbook](../../runbooks/stream-runtime.md).
* Compare supported Stream shapes and stable refusals in the
  [capability matrix](../reference/capability-matrix.md).
* Start with the simpler [Batch first job](first-job.md) if you need bounded
  file-style processing first.
