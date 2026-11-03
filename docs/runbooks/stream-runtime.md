---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# RowWeave Stream operations and recovery runbook

This runbook is the production boundary for the Stream Artifact
contract. It applies to a verified
Stream Artifact after capability, identity, resource and deployment
admission and before any provider, state, Sink or network side effect.

## Public deployment modes

All modes execute the same portable DAG through the same node-local task
factory, input gate, output router and state/time/checkpoint lifecycle:

- Local: `RowWeave.createLocalStreamJobClient(...)`; all product roles share one
  JVM and no listening socket is created.
- InProcess: the product Master/Worker/authority composition in one JVM for
  deterministic integration and parity tests.
- cross-host M+NW: independent Master-only and Worker-only
  `io.rowweave.StreamClusterMain` processes. Production roles use mTLS for
  control, record and checkpoint/rescale payload lanes. Worker and leader
  discovery may use fixed endpoints or ZooKeeper.

An external process role includes ZooKeeper, Master-only, Worker-only or a
fixed Worker endpoint. It must use `rowweave.control.securityProfile=MTLS`; a
missing or invalid identity fails with `external Stream process roles require
MTLS` before registry, listener or provider access. Combined loopback remains
an explicit local-development profile, not a production network profile.

## Supported production profiles

Stream supports bounded-resource resident execution for:

- APPEND, UPSERT and RETRACT changelog, subject to per-node/per-edge stable-key,
  before-image and Sink capability admission;
- fixed and dynamic split discovery with checkpointed cursor, known/pending/
  assigned/finished split state, assignment generation and exact position;
- event-time processing, keyed process/co-process, broadcast state, bounded
  generic/final windows, finite temporal join and replay-aware async wait;
- aligned checkpoint by default and explicitly configured, resource-bounded
  unaligned checkpoint with checksummed channel frame/sequence/credit/epoch;
- savepoint/restore, offline rescale and checkpoint-driven live rescale with
  fenced generation cutover;
- eager at-least-once and checkpoint-transactional Sinks, and resident CDC.

The following remain unsupported and fail before side effects: unbounded
global sort/top/distinct, join without a time or size bound, non-convergent
aggregate, arbitrary group materialization, unbounded state/timers, dynamic
input union, arbitrary Java object state and un-managed external side effects.

## Cluster bootstrap

Every product JVM must declare heap, direct memory, Metaspace, code cache,
thread stack, active processors and a process/container limit. Relevant Stream
properties are:

- `rowweave.stream.master.enable`, `rowweave.stream.worker.enable`;
- `rowweave.stream.state.dir`, `rowweave.stream.master.port`,
  `rowweave.stream.worker.port`, `rowweave.stream.worker.id`;
- `rowweave.stream.master.workers` for fixed endpoints;
- `rowweave.registry=zk`, `rowweave.zk.connectString` and the shared
  `rowweave.stream.master.artifact.dir` for ZooKeeper HA;
- the normal `rowweave.control.*` mTLS identity, trust and authorization settings.

Providers are installed on Workers with `ServiceLoader`. The Artifact contains
only inert configuration and exact contract/deployment digests. Wait for
`STREAM_WORKER_READY=` and `STREAM_MASTER_READY=` readiness lines. Never infer
readiness from sleep or an open but unauthenticated port.

## Admission checklist

1. Verify Artifact checksum/version, exact deployment/authz set, operator UIDs,
   schemas, stable keys, state serializers and required capabilities.
2. Negotiate control/data/checkpoint protocol and the full plan capability set
   before opening providers. A current Master may execute a format-v1 plan on an
   N-1 Worker; a format-v2 plan must reject that Worker
   without downgrade.
3. Reserve node-local mailbox/frame/channel/state/timer/window/checkpoint/
   transfer/thread/FD/disk resources. Check limit-1, limit and limit+1 behavior.
4. For exactly-once require exact source replay, complete state members and a
   transactional or idempotent Sink decision protocol. Missing evidence is a
   rejection, never a weaker delivery label.
5. Run `StreamConnectorConformance` for every Source, eager Sink and
   transactional Sink build before allowlisting it.

## Checkpoint and dynamic split recovery

The completed checkpoint decision and its exact manifest members are the
restore authority. For unaligned mode restore operator/source/Sink state first,
then replay only the task-local channel members in lane sequence. A checksum,
epoch, sequence, credit or ownership mismatch fails closed; it is never
silently converted to aligned mode.

The barrier snapshot freezes the dynamic split cursor and split table together
with positions. Duplicate discovery and ACK loss are idempotent. A remove/add
ABA requires a larger split generation. Worker replacement revokes the old
incarnation assignment before reassignment; Master replacement restores the
cursor and positions before another discover call.

## Failure and recovery procedure

1. Fence the old coordinator term, Worker incarnation, attempt, assignment,
   channel epoch and checkpoint writers before replacement work starts.
2. Re-read the durable first decision. If it is `COMMIT_DECIDED`, restore the
   exact source/state members and run Sink query/commit/query reconciliation
   for that checkpoint before allowing consumption or another checkpoint.
3. If there is no committed decision, restart the whole graph from the latest
   exact `COMPLETED` checkpoint. Do not continue an old socket/channel attempt.
4. Verify every state chunk, serializer/schema identity, source position and
   Sink receipt before source reopen or Sink mutation. Corruption keeps the
   prior completed restore point intact and fails closed.
5. For resident CDC in snapshot phase, restore only a completed split-boundary
   enumerator state and resume the next canonical split. At handoff, require
   all splits complete and the first log sequence to equal snapshot cut plus
   one. In log phase require every sequence to equal the previous sequence plus
   one; a duplicate or gap produces no output and requires a new snapshot.
6. Compare output count/digest, authority generation, resource high-water
   marks and redacted CDC status with the pre-failure record before reopening
   admission.

Late records for finalized windows and invalid temporal-join sequences are
fatal for the attempt. Worker loss, socket loss or Master replacement triggers
fenced whole-graph recovery rather than partial channel replay.

## Live rescale procedure

1. Call `StreamJobHandle.rescale(commandId, targetParallelism)` with a stable
   command identity. Reattach the same identity when the response is unknown.
2. The Master persists intent, takes the rescale checkpoint and derives target
   key groups, split ownership, task-local state and channel state.
3. Target generation tasks restore before durable cutover. The old generation
   remains fenced; two generations may never both publish records.
4. Before cutover failure returns to the source generation. After cutover,
   replacement can restore only the target generation checkpoint.
5. Verify `RUNNING`, assignment generation, checkpoint completion, Sink
   transaction freshness and source continuity after every scale up/down.

Checkpoint, savepoint, cancel and rescale operations are serialized by the
same durable operation authority. Do not emulate live rescale with a client
resubmit or Batch loop.

Stop with a completed savepoint before binary migration or rollback. Offline
rescale reads a verified immutable source savepoint, writes a shadow
generation, validates every key-group count/digest, and atomically binds the
result provenance before scheduling it. Keep the previous release and restore
points until the N-1/N reader/writer and required-capability matrix pass. Never
downgrade or delete newer state to make rollback appear possible; when the
revision intersection is empty, perform an explicitly designed forward
migration.

## RETRACT operations

RETRACT is an edge contract, not a whole-graph executor. CALC/MAP/FLAT_MAP/
FILTER transform before/after deterministically; key changes become old-key
delete plus new-key insert. Reversible aggregate and bounded window/join state
emit deterministic compensation. Legacy keyed-process/co-process/async or
decimal-window ABIs that lack a retract contract reject locally. Transactional
Sinks commit the exact change sequence with the checkpoint.

For incident analysis compare canonical change kind, stable-key digest,
sequence and checkpoint identity. Never log row images, keys or state bytes.

## Master and Worker replacement

ZooKeeper sessions for leader election, leader discovery and Worker discovery
are independent. Each control call resolves the active leader. A replacement
Master first converges pending Sink/checkpoint decisions, split assignment and
rescale cutover, then launches or restores tasks. Old term, Worker incarnation,
attempt, assignment generation and channel epoch writes fail closed.

Keep shared Artifact and checkpoint payload storage available to every eligible
Master/Worker. ZooKeeper metadata is not a substitute for payload durability.
Retain the previous completed checkpoint/savepoint and release until N-1/N
readers, control negotiation and rollback capability have been proven.

## Security and observability

Use a distinct certificate per Client, Master and Worker, a common trust store,
deny-by-default role authorization and an exact deployment set. Verify SAN,
endpoint, fingerprint, role, Worker ID/incarnation and Master term together.
There is no plaintext fallback, opportunistic TLS or legacy-wire downgrade.

Query durable state by stable run/attempt/checkpoint identities. Never infer a
decision from a request ACK. The authoritative sequence is checkpoint manifest,
first decision and completion. `COMMIT_DECIDED` means publication must be
reconciled forward; only `COMPLETED` is a restore point.

For resident CDC, expose only `StreamCdcStatusView`: phase, total/completed
snapshot split counts, snapshot cut sequence and last log sequence. Source
names, split IDs, row keys, payloads, credentials and state/digest identities
must not enter status text, metrics labels, logs or exception messages.

Expose bounded, data-free counters only: attempts/restarts, records/bytes,
completed checkpoints, watermark, mailbox/channel high-water, managed state,
timer/window counts and backpressure time. Alert at 80% of any configured
resource ceiling and stop admission or apply backpressure at 95%. Page
immediately for a stalled checkpoint, non-advancing watermark, repeated
replacement, checksum/schema drift, missing state chunk, log sequence gap or
duplicate, durable authority conflict, split gap, dual-generation output or an
undecided Sink transaction.

Reject unknown required capabilities before class loading or provider access.
Do not place control messages in row frames. Checkpoint, savepoint, spill and
deployment paths require least-privilege ownership and must reject symlink or
path escape.

## Long-stability qualification

CI fault matrices are the deterministic release gate, but they do not replace
a release soak. Before production, run at least 24 hours with two Masters and
multiple cross-host Workers using dynamic split, APPEND/UPSERT/RETRACT,
aligned/unaligned checkpoint, window/join/async state and repeated 1→N→1 live
rescale. Inject leader replacement, Worker kill/restart, lost ACK, socket loss,
disk pressure and checkpoint/cutover failures. Acceptance requires exact
count/digest or changelog-oracle equality, no duplicate/gap/dual-generation
publication, bounded threads, FDs, direct memory, state and disk, and
successful convergence of any `COMMIT_DECIDED` publication. Archive the
workload seed, Artifact digest, release ID, fault timeline, resource profiles
and output digests with the release evidence.

## Stop conditions

Stop admission when any required capability or identity differs, mTLS/ZooKeeper
authority is unavailable, payload or checksum evidence is incomplete, a Sink
decision is ambiguous, a split/channel sequence has a gap or duplicate, a
rescale has two writable generations, or a resource profile exceeds its bound.
Preserve the latest completed recovery point and all stable identities. Never
clear authority state, weaken the Artifact or bypass a failed gate to resume.
