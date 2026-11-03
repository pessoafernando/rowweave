---
doc-contract: rowweave-1.0.0
doc-channel: frozen-release
---
# Rolling upgrade and rollback runbook

This runbook applies to releases in protocol epoch 1 with an explicitly proven
N-1 compatibility window. It does not authorize an epoch change, a decimal
-semantics change, or a store-format migration. A storage cutover must follow
the [store migration](store-migration.md) state machine.

## Preconditions

1. Verify `release-manifest.hex` with the pinned public key and detached
   signature. Record the exact manifest digest, public API digest, release ID,
   and signer-key digest.
2. Run the frozen N-1 runner against the candidate N binary in both directions.
   Required capabilities, stable message IDs, resource ceilings, and the
   writer-revision intersection must match.
3. Confirm all active Runs and durable publications are queryable by stable
   `runId`, `attemptId`, and `publicationId`. Keep the old binary and old
   namespace intact.
4. For a storage cutover, require zero active Runs/intervals and an
   upgrade-authority owner token. Never copy into or mutate the active
   namespace.

## Compatibility and connector qualification

Qualify the complete execution unit before it enters the rollout:

* Confirm the graph only uses capabilities listed in the
  [capability matrix](../product/reference/capability-matrix.md). An optional
  extension must be negotiated on every peer that can receive the Artifact.
* Run source and sink conformance with the exact provider version, descriptor,
  credentials class, format revision, and resource profile intended for
  production. A connector qualification is `same-build`: do not transfer a
  result from another provider build or descriptor revision.
* For transactional sinks, exercise commit, restart, failure, rollback, and
  replay paths. Prove that a replay uses the declared sink generation and does
  not publish two writable generations.
* Keep the deployment-set count, manifest indexes, signer identities, and
  checksums identical on every Worker that can load provider code.
* Record the required-capability intersection. An empty intersection, unknown
  required capability, or mismatched resource ceiling must fail before Run
  creation or class loading.

## Checkpoint and savepoint compatibility

Check durable recovery compatibility before replacing a stateful role:

1. Inventory completed checkpoints and savepoints by stable store, Run,
   attempt, checkpoint, state, and schema identities. Only a state the runtime
   reports as completed is a restore point.
2. Verify that both the old and candidate readers accept the checkpoint or
   savepoint format, capability set, writer revision, checksum, and state
   schema declared in the release packet.
3. For a savepoint, stop admission, reach a typed stable boundary, complete the
   savepoint, and verify its manifest before removing the old process. An ACK
   is not completion.
4. For a rolling checkpoint change, ensure a replacement Worker can read the
   latest completed checkpoint while the old writer remains active. If both
   generations cannot be proven readable, drain before the change.
5. Do not open state with an unqualified binary, rewrite bytes in place, or
   delete the previous completed checkpoint while either generation is live.

The detailed Stream lifecycle is in the
[Stream operations runbook](stream-runtime.md).

## Rolling sequence

Keep one Master, at least two Workers, and a Client probe visible throughout
the exercise.

1. Start N peers beside N-1 peers. Registration must negotiate protocol/data
   versions, runtime capabilities, optional extensions, resident limits, and
   writer revision before any assignment.
2. Replace one Worker at a time. After each replacement, submit the portable
   probe Artifact, query its Run/events/outcome, and verify that a lost Worker
   is replaced only by a peer whose negotiated intersection admits that
   Artifact.
3. Replace the remaining Worker, then replace the Master. Re-read all active
   authority records after leadership acquisition; never infer success from an
   earlier ACK.
4. Upgrade the Client last. A Client does not own data and may be rolled back
   while the current epoch/revision remains readable.

For a store migration, issue the stable commands in this order:
`FREEZE_ADMISSION`, build and verify the shadow namespace, `CUTOVER`, then
`RESUME_WRITES`. Bind every command to release/store/generation/source/target/
owner identity. Duplicate commands and a lost ACK are handled by exact reread
and idempotent replay, not by inventing a new command identity.

## Validation after each step

* The hello/manifest digest and required-capability intersection are accepted
  before Run creation or Worker allocation.
* Existing `runId` and `attemptId` remain unchanged; events continue from the
  last sequence without duplication.
* `queryBatchSinkOutcome`, `queryQualityGateDecision`, and incremental frontier
  queries agree with the durable authority.
* Old leaders, stale Workers, stale writer revisions, term/incarnation drift,
  and duplicate completions are rejected with zero mutation.
* Process resident, RPC frame, thread, action-lane, and store-transaction
  ceilings remain inside the signed release profile.
* A stateful probe restores only from a completed checkpoint or savepoint and
  produces the expected result digest or changelog oracle.

## Rollback decision

Binary rollback is allowed only while the active state remains within the older
reader/writer intersection. Stop admission, drain or cancel bounded work,
replace Client/Master/Workers in reverse order, then re-read authority under
the restored leader.

Data rollback is separate. Before `CUTOVER`, abandon the shadow namespace and
keep the old active pointer. After cutover, `DATA_ROLLBACK` is allowed only
when no N-only write or externally visible publication exists. If new semantics
have been committed, record `MARK_NEW_SEMANTICS` and fail closed: keep the new
active writer and data until an explicitly designed inverse migration exists.

If any command ACK is lost, query by the original command identity. A complete
matching state means success; an old complete state means safe replay; a
partial or digest-drifted state means stop and investigate. Never repeat
`CUTOVER`, `DATA_ROLLBACK`, or `RESUME_WRITES` with guessed identities.

## Stop conditions

Stop the rollout and retain the current authority when there is an epoch
mismatch, manifest/signature mismatch, empty required-capability intersection,
resource-limit mismatch, non-zero active interval at cutover, stale ownership
token, publication/frontier disagreement, missing or corrupt state evidence,
ambiguous sink decision, dual-generation output, or an N-1 request after
N-only state. Preserve logs and the stable command identity; do not bypass the
gate.

## Required evidence

Preserve the rollout evidence as one release packet:

* Signed manifest digest, release ID, signer-key digest, API digest, and
  N-1/N matrix result.
* Artifact and deployment-set digests, connector provider versions, conformance
  results, and the required-capability intersection.
* Job, run, attempt, checkpoint, savepoint, publication, and command IDs with
  their completion or reconciliation evidence.
* Readiness lines, role logs, replacement timeline, monitor snapshots, and
  query results for every Worker and Master replacement.
* Source and sink result digests or changelog-oracle evidence before and after
  the rollout and after any rollback.
* The stop-condition timeline, authority rereads, and exact replayed command
  identities if an ACK was lost.
