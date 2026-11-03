---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Resident change-data-capture source contract

A resident change-data-capture source is a Stream source that never terminates.
It reads a changing external feed through two ordered phases — a snapshot of the
current table contents, then the continuing log of changes — and hands off from
one to the other at a recorded cut. This page states the phases, the enumerator
state that makes the hand-off durable, the reader event vocabulary, the
checkpoint acknowledgement, the restart behaviour and the published ceilings.
The connector that supplies the feed is qualified through the provider contract
in [Stream behavior](stream-behavior.md); the configuration surface for each
supplied connector is on [S3 snapshot connector](connector-s3-snapshot.md) and
[PostgreSQL CDC connector](connector-postgres-cdc.md).

A resident source is not a bounded source with a retry loop. The two are
different declarations, and the boundary between them is enforced at
construction rather than at runtime.

## Declaring a resident source

[`StreamSourceSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamSourceSpec.java)
is the only public declaration of a source. `residentCdc(...)` at
`io/rowweave/stream/api/StreamSourceSpec.java:169` builds a resident source whose
enumerator state is the engine-owned form, and `residentCdcV2(...)` at
`io/rowweave/stream/api/StreamSourceSpec.java:193` builds one whose positions are
supplied by the connector and whose state carries transaction boundaries.

| Declaration | Enumerator state version | Position ownership | Changelog |
|---|---|---|---|
| `residentCdc(...)` | `1` (`io/rowweave/stream/api/StreamSourceSpec.java:186`) | engine-owned split descriptor | `StreamChangelogMode.UPSERT` |
| `residentCdcV2(...)` | `2` (`io/rowweave/stream/api/StreamSourceSpec.java:210`) | opaque external positions plus commit boundaries | `StreamChangelogMode.UPSERT` |

Both factories mark the source resident, and the resulting spec reports it
through `residentCdc()` at
`io/rowweave/stream/api/StreamSourceSpec.java:231`. `residentCdcStateVersion()`
at `io/rowweave/stream/api/StreamSourceSpec.java:232` is the value a Worker
uses to choose the state decoder, so a spec created by one release and read by
another is resolved by that number rather than by a class version.

The engine-owned split descriptor is tagged `STREAM_CDC_ENUMERATOR_V1` at
`io/rowweave/stream/api/StreamSourceSpec.java:181`. That tag is a persisted
identity: a spec written by a released engine keeps it, and the tag names the
state layout rather than the release that introduced it.

### What a resident source may not also be

The constructor rejects every combination that would make termination ambiguous:

* `boundedTermination` and `residentCdc` are mutually exclusive, and
  `boundedTermination` and dynamic split discovery are mutually exclusive
  (`io/rowweave/stream/api/StreamSourceSpec.java:41`). A source that stops is
  not a resident feed, and a feed that discovers splits on its own is not a
  frozen snapshot.
* A spec carrying a snapshot id, cut sequence or split list must also carry the
  replayable identity fields the recovery path needs; their absence fails with
  `replayable source fields are required` at
  `io/rowweave/stream/api/StreamSourceSpec.java:49`.
* A state version outside the declared range is refused rather than defaulted
  (`io/rowweave/stream/api/StreamSourceSpec.java:45`).

Read back through `snapshotId()` at
`io/rowweave/stream/api/StreamSourceSpec.java:237`, `snapshotCutSequence()` at
`io/rowweave/stream/api/StreamSourceSpec.java:238`, `snapshotSplitIds()` at
`io/rowweave/stream/api/StreamSourceSpec.java:239` and `dynamicDiscovery()` at
`io/rowweave/stream/api/StreamSourceSpec.java:221`, these fields are the whole
public evidence of where a resident source stands.

## Phases

[`StreamCdcPhase`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamCdcPhase.java)
is the closed runtime phase set at `io/rowweave/stream/api/StreamCdcPhase.java:5-7`:

| Value | Wire id | Meaning |
|---|---:|---|
| `NOT_APPLICABLE` | `0` (`io/rowweave/stream/api/StreamCdcPhase.java:5`) | the source is not a resident CDC source |
| `SNAPSHOT` | `1` (`io/rowweave/stream/api/StreamCdcPhase.java:6`) | frozen table contents are still being read |
| `LOG` | `2` (`io/rowweave/stream/api/StreamCdcPhase.java:7`) | the continuing change log is being read |

`wireId()` at `io/rowweave/stream/api/StreamCdcPhase.java:15` is the value that
crosses the process boundary, so a phase read from a Worker record is resolved
by that integer. There is no other phase, and there is no partial-overlap phase:
the snapshot is finished before the log begins, from the reader's point of view,
even though the two runs are executed by one continuously running task.

## Enumerator state

[`StreamCdcEnumeratorState`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamCdcEnumeratorState.java)
is the durable record of how far the snapshot got and which splits are already
complete. It is declared at
`io/rowweave/stream/contract/StreamCdcEnumeratorState.java:10` and carries two
version identities: `LEGACY_VERSION` at `:11` is the engine-owned split form,
and `CURRENT_VERSION` at `:12` is the form that additionally carries opaque
external positions and transaction boundaries. A state written under one version
is read under the same version; the engine never reinterprets an old frame as
the new layout.

The nested [`Phase`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamCdcEnumeratorState.java)
enum at `io/rowweave/stream/contract/StreamCdcEnumeratorState.java:16-17` is the
state-level view of the same two-run sequence, and it deliberately has no
`NOT_APPLICABLE` member: a state object only exists for a resident source.

`CompletedSplit` at `io/rowweave/stream/contract/StreamCdcEnumeratorState.java:33`
records the split identity, the record count and the content digest. The digest
is what makes a re-run checkable: a split that reports the same identity but a
different content digest is not the same work, and the recovery path treats it as
evidence of an inconsistency rather than as a completed split.

| Ceiling | Constant site | Value |
|---|---|---|
| Snapshot splits per source | `io/rowweave/stream/contract/StreamCdcEnumeratorState.java:13` | 1024 |
| Opaque external position bytes | `io/rowweave/stream/contract/StreamCdcEnumeratorState.java:14` | 64 KiB |

Both are enforced before allocation. A provider that tries to enumerate more
splits than the ceiling is refused at the state boundary, which is what keeps a
misbehaving feed from growing durable state without bound.

## Reader event vocabulary

A resident reader is bound through
[`StreamCdcSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamCdcSourceProvider.java),
declared at `io/rowweave/stream/spi/StreamCdcSourceProvider.java:4` as an
extension of the ordinary source provider. `openCdc(StreamCdcSourceContext)` at
`io/rowweave/stream/spi/StreamCdcSourceProvider.java:5` is the resident entry
point. Its default implementation at
`io/rowweave/stream/spi/StreamCdcSourceProvider.java:10` throws
`UnsupportedOperationException`, which is how a provider that was only
qualified for bounded reads refuses a resident assignment instead of silently
returning a reader that cannot produce a hand-off.

[`StreamCdcSourceReader`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamCdcSourceReader.java)
is the pull side of the contract at
`io/rowweave/stream/spi/StreamCdcSourceReader.java:4`:

| Member | Declaration | Contract |
|---|---|---|
| `next()` | `io/rowweave/stream/spi/StreamCdcSourceReader.java:5` | returns the next event, or `null` when none is currently available |
| `notifyCheckpointComplete(...)` | `io/rowweave/stream/spi/StreamCdcSourceReader.java:6` | tells the connector that a checkpoint durably committed, so it may release retained log data |
| `wakeUp()` | `io/rowweave/stream/spi/StreamCdcSourceReader.java:8` | unblocks a reader parked inside `next()` so a cancel or snapshot barrier is honoured promptly |
| `cancel()` | `io/rowweave/stream/spi/StreamCdcSourceReader.java:9` | stops producing without a further checkpoint |
| `close()` | `io/rowweave/stream/spi/StreamCdcSourceReader.java:10` | releases the connection; must be safe after `cancel()` |

[`StreamCdcSourceEvent`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamCdcSourceEvent.java)
closes the vocabulary at `io/rowweave/stream/spi/StreamCdcSourceEvent.java:5-12`:

| Kind | Emitted when | Factory |
|---|---|---|
| `SNAPSHOT_AFTER_IMAGE` | one snapshot row, as an upsert frame | `io/rowweave/stream/spi/StreamCdcSourceEvent.java:54` |
| `SNAPSHOT_SPLIT_COMPLETE` | one split fully read | `io/rowweave/stream/spi/StreamCdcSourceEvent.java:60` |
| `SNAPSHOT_HANDOFF_COMPLETE` | one split's snapshot position reaches the cut | `io/rowweave/stream/spi/StreamCdcSourceEvent.java:65` |
| `HANDOFF_COMPLETE` | every split handed off; the log may begin | `io/rowweave/stream/spi/StreamCdcSourceEvent.java:71` |
| `LOG_RECORD` | one change from the continuing log | `io/rowweave/stream/spi/StreamCdcSourceEvent.java:76` |
| `LOG_POSITION` | an advanced external position with no record | *constructor path* |

The constructor runs the cross-field checks at
`io/rowweave/stream/spi/StreamCdcSourceEvent.java:24`: a snapshot event carries a
split id and no sequence, a log event carries a sequence and no split id, a frame
is present exactly when the kind carries a payload, and a position frame is
present only for the position-carrying kinds. Anything else fails as
`invalid resident CDC event` at
`io/rowweave/stream/spi/StreamCdcSourceEvent.java:44`. The frame ceiling for one
event is at `io/rowweave/stream/spi/StreamCdcSourceEvent.java:14`: 4 MiB, the
same bound as a single encoded Stream row
(`io/rowweave/stream/api/StreamDataRowCodec.java:8`), so one event cannot become
a channel-sized buffer.

## Cut, frontier and acknowledgement

The hand-off point is the `snapshotCutSequence`: the log sequence at which the
snapshot was taken. Because the cut is recorded rather than inferred, a restart
after the snapshot half-finishes resumes from the recorded splits and the same
cut instead of re-reading the table. The engine's view of that progress is
[`StreamCdcStatusView`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCdcStatusView.java),
whose accessors report `phase()` at
`io/rowweave/stream/runtime/StreamCdcStatusView.java:21`, `totalSnapshotSplits()`
at `:22`, `completedSnapshotSplits()` at `:23`, `snapshotCutSequence()` at
`io/rowweave/stream/runtime/StreamCdcStatusView.java:24` and
`lastLogSequence()` at `:25`. The view is internal and redacted: it exposes
progress, never a credential or an external connection string.

[`StreamCdcCheckpointCompletion`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamCdcCheckpointCompletion.java)
at `io/rowweave/stream/spi/StreamCdcCheckpointCompletion.java:6` is what a
completed checkpoint hands back to the connector: the checkpoint sequence, the
last log sequence covered, and the opaque external position. Its validation at
`io/rowweave/stream/spi/StreamCdcCheckpointCompletion.java:13` requires a
checkpoint sequence of at least one and a non-negative log sequence, so a
completion can never claim to have committed a record the engine had not
accepted. `checkpointSequence()` at
`io/rowweave/stream/spi/StreamCdcCheckpointCompletion.java:26` is the identity a
connector uses to decide that retained log data may be released.

Acknowledge-and-release is therefore strictly ordered: retention may only be
reduced for a sequence already inside a completed checkpoint. Restart replays
from the last completed checkpoint, so the window between the acknowledged
sequence and the replay point is delivered twice rather than lost. That ordering
is why the release of external log data is a consequence of checkpoint
completion and never of a reader simply having consumed a record.

## Provider capability flags

A connector declares residency rather than having it inferred. In
[`StreamConnectorProviderContract`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamConnectorProviderContract.java),
`residentCdc()` at `io/rowweave/stream/contract/StreamConnectorProviderContract.java:132`
says the provider serves a never-terminating feed, `dynamicDiscovery()` at
`:131` says splits may appear after planning, `boundedEndOfInput()` at `:130`
says a run may finish, and `externalCheckpointAcknowledgement()` at `:133` says
the connector needs the completion callback. The contract ABI is
`ABI_VERSION` at `io/rowweave/stream/contract/StreamConnectorProviderContract.java:8`
and the configuration ceiling is `MAX_CONFIGURATION_BYTES` at `:9`.

A graph that asks a provider for a capability the signed contract did not
declare is refused at admission. `StreamPublicResidentCdcCompilerContractTest`
is the qualifying evidence that residency is decided from the contract rather
than from the operator mix.

## Qualification

Resident CDC is qualified at three independent levels, and each is named so the
next can be checked against it:

* `LocalStreamPublicResidentCdcTest` — the public declaration, the phase view and
  the local execution of a two-run feed.
* `ResidentStreamCdcEnumeratorTest` — split enumeration, state versions, the
  ceilings, and resume after a torn split.
* `StreamPublicResidentCdcCompilerContractTest` — admission and refusal from the
  signed contract.
* `StreamCdcSourceEventV2Test` — the cross-field rules for every event kind and
  the external-position frame.
* `StreamCdcCheckpointCompletionTest` — the acknowledgement ordering.
* `StreamProductResidentCdcRealWorkerDispatchTest` — the same contract executed
  by a real Worker process rather than the local runtime.
* `StreamClusterCdcExample` — the runnable end-to-end path, qualified by
  `StreamClusterCdcExampleTest`.

The durable state authorities that keep the enumerator across a Master change
are described in [ZooKeeper registry](registry-zookeeper.md).

## Boundaries

* A resident source never reports completion. An empty log poll returns `null`
  from `next()` and the task stays alive.
* A snapshot is taken at one cut. Records changed after the cut arrive as log
  records; the engine does not attempt to reconcile the two, and a consumer
  applying upserts sees the final state.
* Split assignment changes are not applied to a running snapshot: the split set
  is the one recorded in the durable enumerator state.
* Global ordering across splits is not promised. Per-key ordering inside the log
  is the connector's contract; see the connector page for what it declares.

Related: [Stream behavior](stream-behavior.md),
[capability matrix](capability-matrix.md),
[PostgreSQL CDC connector](connector-postgres-cdc.md),
[configuration](configuration.md),
[Stream runbook](../../runbooks/stream-runtime.md),
[errors and refusals](errors-and-refusals.md).
