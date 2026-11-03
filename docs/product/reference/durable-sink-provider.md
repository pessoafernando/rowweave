---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Durable sink provider boundary

A durable sink provider is the extension point that lets an external store take
part in the Batch commit protocol instead of the engine writing the result
itself. This page states the provider contract, the two handles it returns, the
fence and budget the engine supplies to every action, the closed semantic
declarations a provider must make, and the admission checks that keep an
unqualified provider out of a deployment. For the sink side the engine drives,
see [Batch sinks and output boundaries](batch-sinks.md).

## Why the boundary exists

A Batch result becomes externally visible in one step, and the engine cannot
observe an external store's intermediate state. So the protocol is split: the
provider writes a private attempt, solidifies it into an opaque locator, and only
publishes when the whole group is decided. A failed or retried attempt must leave
nothing visible, and an outcome that the engine cannot determine must never be
answered by a guess. Every type below exists to enforce exactly that.

## Roles

| Type | Responsibility | Declared |
|---|---|---|
| `DurableBatchSinkProvider` | Worker-side runtime capability for one provider family | `io/rowweave/DurableBatchSinkProvider.java:9` |
| `DurableBatchSinkProviderContract` | the versioned self-description admission reads | `io/rowweave/DurableBatchSinkProviderContract.java:10` |
| `DurableBatchSinkWriteAttempt` | one bounded write handle for the current attempt | `io/rowweave/DurableBatchSinkWriteAttempt.java:6` |
| `DurableBatchSinkPreparedGroup` | rebuildable handle for one prepared publication group | `io/rowweave/DurableBatchSinkPreparedGroup.java:6` |
| `DurableSinkActionContext` | engine-owned fence, deadline and resource view | `io/rowweave/DurableSinkActionContext.java:6` |
| `DurableSinkProviderKey` | the three-part identity a plan names | `io/rowweave/DurableSinkProviderKey.java:8` |
| `ProviderBatchSink` | the inert description that travels in the plan | `io/rowweave/ProviderBatchSink.java:10` |

## The provider interface

`contract()` at `io/rowweave/DurableBatchSinkProvider.java:11` returns the
self-description the Worker compares with the signed contract it discovered, so a
provider cannot describe itself differently than the deployment recorded.
`canonicalizeConfiguration(ProviderBatchSink)` at
`io/rowweave/DurableBatchSinkProvider.java:17` is the Worker-only decode and
re-encode boundary, and its result must be byte-exact canonical configuration for
the same frozen proxy. `canonicalizeLocator(PreparedSinkLocator)` at `:20` is the
same rule applied to the opaque locator.

Two factory methods complete the interface. `openWriteAttempt` at
`io/rowweave/DurableBatchSinkProvider.java:22` takes the sink description, the
attempt identity and the action context; `openPreparedGroup` at `:26` takes the
whole output list, the locator list and the context, because publication is
decided for the group and never per sink. There are no other members, so a
provider cannot omit the canonicalization that makes a retry comparable.

## The write attempt handle

[`DurableBatchSinkWriteAttempt`](../../../rowweave-core/src/main/java/io/rowweave/DurableBatchSinkWriteAttempt.java)
at `io/rowweave/DurableBatchSinkWriteAttempt.java:6` is Worker-local and bounded:
rows enter synchronously through `write(DataRow)` at `:8`, `prepare()` at `:10`
solidifies the attempt and returns its locator, and `close()` at `:14` is
resource release only. That last rule is explicit in the declaration: closing a
handle must neither publish nor abort external state, so releasing a handle after
a crash cannot change what is visible.

## The prepared group handle

[`DurableBatchSinkPreparedGroup`](../../../rowweave-core/src/main/java/io/rowweave/DurableBatchSinkPreparedGroup.java)
at `io/rowweave/DurableBatchSinkPreparedGroup.java:6` is rebuildable: the engine
can reconstruct it from the locators after a Worker restarts, which is what makes
an uncertain outcome recoverable rather than terminal. `publish()` at `:8` makes
the group visible atomically, `abort()` at `:10` discards it,
`finalizeAttempt()` at `:12` releases compensating state once the decision is
durable, and `queryOutcome()` at `:14` reports the recorded outcome. `close()` at
`:18` again releases resources only and must not alter publication state.

## The context the engine supplies

Every provider action receives
[`DurableSinkActionContext`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkActionContext.java)
so the provider never invents its own identity or limits. `actionId()` at
`io/rowweave/DurableSinkActionContext.java:8`, `providerKey()` at `:10`,
`attemptIdentity()` at `:12`, `publicationId()` at `:14` and
`publicationGeneration()` at `:16` are the fence: a provider that acts on a
superseded attempt is detectable from the identity alone.
`authorityDecisionDigest()` at `:19` is empty before a durable decision exists and
otherwise carries the exact authority digest, which is how a provider refuses to
publish against a decision it did not see.

`deadlineNanos()` at `io/rowweave/DurableSinkActionContext.java:22` is an absolute
monotonic deadline rather than a duration, and `isCancellationRequested()` at
`:24` is the cooperative cancel signal. The four limits are `maxResidentBytes()`
at `:26`, `maxOpenHandles()` at `:28`, `maxRequestBytes()` at `:30` and
`maxResponseBytes()` at `:32`, and the two leases make them enforceable instead of
advisory: `acquireResidentLease(long)` at `:34` and `acquireHandleLease()` at `:36`
return a [`DurableSinkResourceLease`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkResourceLease.java)
declared at `io/rowweave/DurableSinkResourceLease.java:4`, whose `residentBytes()`
`:6`, `openHandles()` `:8`, `isOpen()` `:10` and `close()` `:13` let the engine
observe what a provider is actually holding.

## Closed semantic declarations

[`DurableBatchSinkProviderContract`](../../../rowweave-core/src/main/java/io/rowweave/DurableBatchSinkProviderContract.java)
is the frozen, serializable declaration admission reads. Its SPI version is
`io/rowweave/DurableBatchSinkProviderContract.java:13`, with the configuration and
outcome byte ceilings at `:14` and `:15`. Each semantic dimension is a closed enum
with exactly one shipped value, so a provider states its behaviour rather than
implying it:

| Declaration | Value | Asserts |
|---|---|---|
| `IdempotencyScope` at `:17` | `ATTEMPT_AND_PUBLICATION_V1` | both an attempt and a publication are idempotent |
| `OrphanOwnership` at `:30` | `ATTEMPT_PRIVATE_V1` | orphaned state belongs to the attempt that made it |
| `QuarantinePolicy` at `:43` | `PRESERVE_ON_UNKNOWN_OR_CONFLICT` | unknown or conflicting state is preserved, not deleted |
| `SafeDeleteProof` at `:56` | `ABORT_DECISION_OR_UNREFERENCED_RETENTION_V1` | deletion needs a recorded abort decision or an unreferenced retention floor |
| `DeploymentCapability` at `:69` | `CORE_BUILTIN_V1` at `:70`, `SIGNED_TEST_CONFORMANCE_V1` at `:71` | either built into the engine or qualified by a signed conformance run |

The constructor at `io/rowweave/DurableBatchSinkProviderContract.java:93` is the
enforcement point. A contract declaring an SPI version other than the current one
fails at `:104`; a capability profile that does not match the declaration fails at
`:114` and `:116`; and any codec whose declared payload exceeds the published
ceiling fails at `:120` through `:124`. Accessors follow at `:140` through `:148`.

[`DurableSinkCleanupContract`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkCleanupContract.java)
at `io/rowweave/DurableSinkCleanupContract.java:8` bounds the janitor's work:
retention up to a year at `:11`, batch size capped at `:12`, and the constructor at
`:22` rejects retention out of range `:28`, an over-large claim batch `:32`, a
delete batch larger than the claim batch `:36`, and any missing semantic `:41`. A
provider that could delete without a proof cannot be constructed.

[`DurableSinkAttemptBudget`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkAttemptBudget.java)
at `io/rowweave/DurableSinkAttemptBudget.java:8` publishes the hard ceilings a
provider may choose within: resident bytes at `:11`, open handles at `:12`,
locator bytes at `:13`, control bytes at `:14` and action timeout at `:15`. The
constructor rejects a value above each of them, at `:27`, `:31`, `:35`, `:39` and
`:44`, and `readResolve` at `:60` rebuilds a deserialized budget through those same
checks, so a budget cannot be forged onto a Worker.

Codec identity is declared the same way.
[`DurableSinkCodecDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkCodecDescriptor.java)
at `io/rowweave/DurableSinkCodecDescriptor.java:8` caps codec identifier length at
`:11` and payload size at `:12`, and a descriptor above either bound fails at `:23`
and `:27`.
[`DurableSinkProviderKey`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkProviderKey.java)
at `io/rowweave/DurableSinkProviderKey.java:8` is the identity a plan names:
connector type, version and capability profile, and `matches(BatchSinkDescriptor)`
at `:39` requires all three to agree, so a plan cannot silently be served by a
different provider version.

## Identity, locator and outcome

[`SinkAttemptIdentity`](../../../rowweave-core/src/main/java/io/rowweave/run/SinkAttemptIdentity.java)
at `io/rowweave/run/SinkAttemptIdentity.java:16` fixes what one attempt is:
artifact id, run-attempt fence, assignment generation, stage execution id, logical
stage, instance index, sink operator id, capability profile and commit domain, with
the descriptor digest carried beside them, as its accessors at `:175` through `:187`
show. `forAssignment` at `:83` is how the scheduler builds one, and
`MAX_OPERATOR_ID_BYTES` at `:19` bounds the identifier it embeds.

[`PreparedSinkLocator`](../../../rowweave-core/src/main/java/io/rowweave/run/PreparedSinkLocator.java)
at `io/rowweave/run/PreparedSinkLocator.java:12` is the opaque handle a prepared
attempt hands back. Its version is `:14` with the pre-protocol form at `:15`, the
payload ceiling at `:16`, and the two constructors at `:37` and `:47` select
between the current and the legacy manifest provider declared at `:18` and `:21`.
The locator is deliberately opaque: the engine stores and transports it, and only
the provider that made it can read meaning from it, which is exactly why
`canonicalizeLocator` is part of the provider contract.

[`BatchSinkOutcome`](../../../rowweave-core/src/main/java/io/rowweave/commit/BatchSinkOutcome.java)
at `io/rowweave/commit/BatchSinkOutcome.java:7` is what
`queryOutcome()` returns, and its `State` at `:9` is the closed answer space:

| State | Means |
|---|---|
| `NOT_FOUND` | no record of this attempt exists |
| `PREPARING` | prepared but undecided |
| `COMMIT_DECIDED` | commit decided, publication not yet confirmed |
| `COMMITTED` | visible |
| `ABORT_DECIDED` | abort decided, cleanup not yet confirmed |
| `ABORTED` | discarded and confirmed |
| `CORRUPT_OR_CONFLICT` | the record contradicts its own identity |

The two states between a decision and a confirmation exist because the engine must
be able to say that a decision is recorded while its effect is not yet observed,
instead of collapsing that into either a success or a failure. `of` at `:54` and
`notFound` at `:59` are the constructors, and a detail code is length-bounded at
`:20`.

## The manifest commit authority

The engine's own durable publication is a built-in provider, and its authority
interfaces are the boundary an external deployment can replace.
[`CommitGenerationAuthority`](../../../rowweave-core/src/main/java/io/rowweave/commit/CommitGenerationAuthority.java)
at `io/rowweave/commit/CommitGenerationAuthority.java:6` allocates a strictly
monotonic, never-reused generation per commit domain: `reserve` at `:9` must
idempotently return the same generation for the same attempt id, and `find` at
`:12` is read-only and never allocates.
[`ManifestCommitAuthority`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitAuthority.java)
at `io/rowweave/commit/ManifestCommitAuthority.java:6` adds active-writer fencing,
with non-blocking `tryAcquire` at `:9` returning null when the domain is held, and
the `Lease` shape at `:11`.
[`ManifestCommitAuthorityProvider`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitAuthorityProvider.java)
at `io/rowweave/commit/ManifestCommitAuthorityProvider.java:4` is how a registry or
deployment exposes one.

The file-system implementation is
[`FileCommitGenerationAuthority`](../../../rowweave-core/src/main/java/io/rowweave/commit/FileCommitGenerationAuthority.java)
at `io/rowweave/commit/FileCommitGenerationAuthority.java:28` with
[`FileManifestCommitAuthority`](../../../rowweave-core/src/main/java/io/rowweave/commit/FileManifestCommitAuthority.java)
at `io/rowweave/commit/FileManifestCommitAuthority.java:6`, and the store, recovery
and cleanup machinery beside them: `ManifestFileStore` at
`io/rowweave/commit/ManifestFileStore.java:17`, `ManifestFileRecovery` at
`io/rowweave/commit/ManifestFileRecovery.java:12`, `ManifestFileJanitor` at
`io/rowweave/commit/ManifestFileJanitor.java:11`. The read-back answer is
[`ManifestCommitOutcome`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitOutcome.java)
at `io/rowweave/commit/ManifestCommitOutcome.java:4`, whose four states at `:6` are
the manifest-specific projection of the seven-value sink outcome above. Fingerprints
that bind a member to a header come from
[`SchemaFingerprint`](../../../rowweave-core/src/main/java/io/rowweave/commit/SchemaFingerprint.java)
at `io/rowweave/commit/SchemaFingerprint.java:12`. On the cluster side,
`DurableSinkCommitCoordinator` at
`io/rowweave/cluster/DurableSinkCommitCoordinator.java:8` drives the group through
its phases, and a submission the coordinator refuses is reported by
[`DurableRunSubmitException`](../../../rowweave-core/src/main/java/io/rowweave/cluster/DurableRunSubmitException.java)
at `io/rowweave/cluster/DurableRunSubmitException.java:6`, whose `Category` at `:7`
separates a clean rejection from a terminal failure and from an unknown state.

## What travels in the plan

A plan that names a provider carries
[`ProviderBatchSink`](../../../rowweave-core/src/main/java/io/rowweave/ProviderBatchSink.java)
at `io/rowweave/ProviderBatchSink.java:10` rather than a user object, and its
`RUNTIME_REQUIRED` refusal at `:13` is the message every lifecycle method throws:
`collect` at `:64`, `close` at `:67`, `abort` at `:70`, and the four commit methods
at `:76` through `:85`. It reports itself unwritable from a Worker at `:88` and
exposes its destination identity at `:91`.

Its constructor is the admission gate for the description itself: all four fields
required at `io/rowweave/ProviderBatchSink.java:26`, schema fingerprint agreeing
with the descriptor at `:30`, the atomic-group profile from
[`BatchSinkDescriptor.CapabilityProfile`](../../../rowweave-core/src/main/java/io/rowweave/BatchSinkDescriptor.java)
at `:35`, and configuration bytes inside both the codec bound and the SPI ceiling at
`:41`. `DurableSinkConfigurationEnvelope` at
`io/rowweave/DurableSinkConfigurationEnvelope.java:7` is the pair of codec
descriptor and canonical bytes that a provider later re-derives, constructed at
`:12`.

## Admission and qualification

Providers are discovered from a service resource and bounded exactly like source
providers:
[`DurableBatchSinkProviderRegistry`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DurableBatchSinkProviderRegistry.java)
at `io/rowweave/deployment/DurableBatchSinkProviderRegistry.java:25` names the
resource at `:27` and caps service size, provider count, candidate count and name
length at `:29` through `:32`, with the two roles at `:34`. `scan` at `:52` refuses
a malformed set at `:68`, and a size check at `:143`. `requireContract` at `:111`
and `loadProvider` at `:122` refuse an unknown key with `PROVIDER_RUNTIME_REQUIRED`
at `:127`. The built-in contracts registered by the engine itself are declared in
`DurableBatchSinkBuiltinContracts` at
`io/rowweave/deployment/DurableBatchSinkBuiltinContracts.java:14`, and
`DurableBatchSinkProviderAdmissionPolicy` at
`io/rowweave/deployment/DurableBatchSinkProviderAdmissionPolicy.java:11` decides
which discovered keys a role may serve, refusing an oversized set at `:25`, a null
entry at `:31` and a conflicting entry at `:36`.

[`DurableSinkProviderPlanPreflight`](../../../rowweave-core/src/main/java/io/rowweave/plan/DurableSinkProviderPlanPreflight.java)
at `io/rowweave/plan/DurableSinkProviderPlanPreflight.java:16` is where a plan meets
the deployment that will run it. `validate` at `:20` requires both inputs at `:24`,
routes a plan that carries envelopes through durable validation at `:28`, rejects a
provider that is not in the scanned set at `:44`, rejects an out-of-budget attempt
with `PROVIDER_BUDGET_INVALID` at `:49`, and checks locator and control bounds at
`:56`. `validateClientGraph` at `:126` applies the same rules before submission,
refusing an incomplete binding at `:129`, a descriptor mismatch at `:140` and a
capability disagreement at `:159`, and `validateProviderConfiguration` at `:179`
checks the configuration against the codec the contract declares.

`DeploymentCapability` closes the last gap. A provider is either built into the
engine or carries a signed conformance result, and
`SIGNED_TEST_CONFORMANCE_V1` at `io/rowweave/DurableBatchSinkProviderContract.java:71`
exists so an external provider is admitted only after its conformance evidence is
part of the deployment, not because a caller asserted it.

## Tested by

- [`DurableBatchSinkProviderContractCodecTest`](../../../rowweave-core/src/test/java/io/rowweave/sink/DurableBatchSinkProviderContractCodecTest.java)
  and [`DurableBatchSinkProviderConformanceRedTest`](../../../rowweave-core/src/test/java/io/rowweave/sink/DurableBatchSinkProviderConformanceRedTest.java):
  contract wire form, ceiling enforcement, and the refusals an unqualified provider must hit.
- [`DurableBatchSinkProviderRegistryTest`](../../../rowweave-core/src/test/java/io/rowweave/deployment/DurableBatchSinkProviderRegistryTest.java)
  and [`MasterDurableRunPreflightTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/MasterDurableRunPreflightTest.java):
  discovery, role policy, and preflight enforced on the Master.
- [`DurableSinkCommitCoordinatorTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/DurableSinkCommitCoordinatorTest.java)
  and [`DurableCommitBarrierFailureTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/DurableCommitBarrierFailureTest.java):
  group publish and abort driven through every failure interleaving.
- [`ClusterManifestCommitTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/ClusterManifestCommitTest.java)
  and [`RealClusterBatchSinkOutcomeProtocolTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/RealClusterBatchSinkOutcomeProtocolTest.java):
  atomic-group visibility and the outcome protocol across processes.
- [`ManifestProcessCrashRecoveryTest`](../../../rowweave-core/src/test/java/io/rowweave/commit/ManifestProcessCrashRecoveryTest.java)
  and [`ManifestCommitOutcomeTest`](../../../rowweave-core/src/test/java/io/rowweave/commit/ManifestCommitOutcomeTest.java):
  recovery after a Worker dies mid-commit, and outcome states.
- [`ManifestFileJanitorTest`](../../../rowweave-core/src/test/java/io/rowweave/commit/ManifestFileJanitorTest.java)
  and [`ManifestAbortCleanupAuthorityTest`](../../../rowweave-core/src/test/java/io/rowweave/commit/ManifestAbortCleanupAuthorityTest.java):
  retention, batch bounds and safe-delete proof.
- [`FileManifestCommitAuthorityTest`](../../../rowweave-core/src/test/java/io/rowweave/commit/FileManifestCommitAuthorityTest.java)
  and [`ManifestVisibilityReferenceModelTest`](../../../rowweave-core/src/test/java/io/rowweave/commit/ManifestVisibilityReferenceModelTest.java):
  fencing and the visibility model it must satisfy.
- [`SinkAttemptIdentityCodecTest`](../../../rowweave-core/src/test/java/io/rowweave/run/SinkAttemptIdentityCodecTest.java)
  and [`ManifestCommitJournalCodecTest`](../../../rowweave-core/src/test/java/io/rowweave/run/ManifestCommitJournalCodecTest.java):
  identity and journal wire stability.
- [`PreparedSinkAuthoritySurfaceTest`](../../../rowweave-core/src/test/java/io/rowweave/run/PreparedSinkAuthoritySurfaceTest.java),
  [`InMemoryPreparedSinkAuthorityTest`](../../../rowweave-core/src/test/java/io/rowweave/run/InMemoryPreparedSinkAuthorityTest.java),
  [`BatchSinkOutcomeTest`](../../../rowweave-core/src/test/java/io/rowweave/commit/BatchSinkOutcomeTest.java)
  and [`BatchSinkAbortRecordTest`](../../../rowweave-core/src/test/java/io/rowweave/run/BatchSinkAbortRecordTest.java):
  prepared-attempt authority and recorded outcomes.
- [`ProviderSinkWriteSessionTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/ProviderSinkWriteSessionTest.java)
  and [`ProviderSinkConfigurationEnvelopeTest`](../../../rowweave-core/src/test/java/io/rowweave/plan/ProviderSinkConfigurationEnvelopeTest.java):
  provider sinks driven only by the runtime, with canonical configuration.
- [`ZkBatchSinkJournalScaleProfileTest`](../../../rowweave-registry-zk/src/test/java/io/rowweave/cluster/registry/zk/ZkBatchSinkJournalScaleProfileTest.java):
  journal behaviour with an external registry authority.

Related: [Batch sinks and output boundaries](batch-sinks.md),
[source executor and provider SPI](source-executor-spi.md),
[built-in Batch sources and source replay](batch-sources.md),
[bounded incremental Batch source](incremental-batch-source.md),
[schema conversion and row validation](schema-and-validation.md),
[recovery and state authority](../architecture/recovery.md).
