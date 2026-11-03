---
doc-contract: rowweave-1.0.0
doc-channel: frozen-release
---
# Store migration and rollback runbook

This runbook is the operating procedure for the
[recovery authority model](../product/architecture/recovery.md). It applies to
one bounded File or ZooKeeper authority root and one upgrade generation. Never
run two generations against the same store concurrently.

## Preconditions

- The target release manifest, source/target `StoreFormatHeader`, exact store
  ID, generation, owner term/incarnation/fence, namespaces and rollback
  deadline are fixed before bootstrap.
- The source namespace contains the complete Run, frontier, assignment,
  intent, receipt and manifest state. External Sink data is outside this
  migration and must not be copied or republished.
- Non-durable ephemeral submissions are drained before the maintenance window;
  they are not durable Runs and are not discoverable after leader loss.
- File deployments construct their state stores below
  `FileUpgradeAuthority.activeNamespacePath()`. ZooKeeper deployments use the
  explicit `ZkServiceRegistry(connectString, upgradeRoot)` constructor. The
  one-argument registry constructor deliberately has no upgrade authority.
- Bootstrap is performed while the source namespace is stable. For the initial
  migration, prepare the source namespace offline, bootstrap the authority,
  then start the leader with the configured authority and keep the load
  balancer closed until admission freeze succeeds.
- The active release declares its persistent Run/frontier/manifest store layout
  byte-stable: canonical migration copies each object without rewriting its
  bytes. A release that declares a storage rewrite needs its own reviewed
  migrator and semantic projector before `BEGIN_COPY`; this runbook does not
  authorize an ad-hoc transform.

## Forward migration

1. Read back the authority and both headers. Verify store ID, protocol epoch,
   source/target revisions, generation, owner fence, namespace names, raw
   digests, active pointer version and rollback deadline against the signed
   release packet.
2. On the active `MasterServer`, apply one fully bound `FREEZE_ADMISSION`
   command through `applyStoreUpgrade`. Do not call a File/ZK authority
   transition directly while a Master can admit work. Confirm
   `writesAllowed=false`; new submit, claim and assignment admission now fails
   with `STORE_UPGRADE_ADMISSION_FROZEN`, while terminal reconciliation remains
   allowed.
3. Reapply the same bound `QUIESCE` command until it returns `ACCEPTED`.
   `ACTIVE_WORK_REMAINS` is expected while an in-process lease or a durable
   non-terminal Run exists. A replacement leader performs a bounded full-cycle
   durable Run census, so leader loss cannot erase the drain obligation.
4. Apply `BEGIN_COPY`, then call `copyAndVerifyShadow` with that exact command.
   Copy is idempotent: after JVM/session loss, reread authority and continue
   with the same command identity. Any source drift, object/child/byte bound,
   symlink, corrupt frame or target digest mismatch fails closed; do not
   advance to verification. Confirm that `evidenceCount == objectCount` and
   retain the per-object evidence frames. Each frame binds generation,
   canonical relative path, source digest, target digest, command ID and
   command digest; File and ZK backends shard them outside both namespaces.
5. Build `VERIFY` from the returned object/shard counts and the independently
   computed semantic projections. Raw source/target namespace digests bind the
   copy; semantic source/target digests must be equal. Apply `VERIFY` and read
   back phase `VERIFIED`.
6. Apply `CUTOVER` once. On timeout, connection loss or lost ACK, do not issue
   a new command ID: reread authority and replay the identical command. The
   only valid observations are the complete old pointer/epoch or the complete
   new pointer/epoch.
7. Leave writes frozen. Stop old readers, construct a fresh reader from the
   active pointer, and perform read-only checks of Run/frontier/intent/receipt
   references plus a no-side-effect plan smoke test. A configured ZooKeeper
   registry mounts all persistent Run/frontier/manifest authorities beneath
   the selected namespace; worker lease discovery stays outside the migrated
   namespace.
8. Only after cold recovery and smoke checks pass, apply `RESUME_WRITES` and
   reopen traffic. Before the first target-only mutation, either apply
   `MARK_NEW_SEMANTICS` explicitly or have the target writer do so as part of
   its first such admission. Never claim data rollback is possible after that
   evidence exists.

## Failure and rollback

- Before cutover, the source pointer remains authoritative. Fix the cause and
  replay the same command, or apply `FREEZE_FAILURE`; never expose the shadow
  namespace to readers.
- After cutover but before writes resume and before the rollback deadline,
  apply the same fully bound `DATA_ROLLBACK` command through the active leader.
  It atomically restores the old namespace and increments both pointer version
  and writer epoch. Restart readers against the restored active pointer before
  deciding whether to resume the old release.
- Once writes resume, target-only semantics are recorded, or the deadline has
  passed, direct data rollback is forbidden. Preserve both namespaces and
  either forward-fix or run a separately designed inverse migration.
- Binary rollback is independent: first lower the negotiated writer revision
  under the rolling-cluster authority, then replace binaries. Visible
  manifests and Sink intents must always reconcile under their original Run
  authority.
- `MARK_CLEANUP_ELIGIBLE` is allowed only at or after the rollback deadline.
  Namespace deletion is a separate operator action and additionally requires
  retention/reference evidence. This implementation never deletes the old
  namespace automatically.

## Required evidence

Archive the initial and final header/authority frames, all command IDs and
digests, raw and semantic digests, verified counts, leader owner fence,
pointer/writer epochs, cold-reader smoke result, and the outcome of every
uncertain replay. Redact credentials, SQL, object keys and user payloads.
