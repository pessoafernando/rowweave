---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Pipeline composition boundary

What a multi-stage Pipeline may contain, what it refuses, and which of those
behaviours a test pins today. Every claim below points at a source line or a
named test; the refusals were measured by calling the public API, not read off
the implementation.

## 1. Pipeline combines metadata; it never executes

`rowweave-pipeline` composes the **metadata** of compiled plan artifacts:
identities, operator endpoints, schema fingerprints and the materialized
boundaries between stages. A Batch step takes an artifact whose payload
format is `PORTABLE_BATCH_PLAN_V1`, a Stream step one whose payload format is
`PORTABLE_STREAM_JOB_V1`; the public API accepts exactly these two payload
formats, and [integrating an external platform](../guides/external-platform-integration.md)
shows one being built. A carrier of any other payload format is refused at the
step seam, as §3.1 records. The compiled result is an inspection plan, not a
running job
(`io/rowweave/pipeline/PipelineExecutionPlan.java:7`,
`io/rowweave/pipeline/PipelineValidator.java:9`). Every main source of the
module imports only `java.*`, `io.rowweave.artifact.*` and their own package; no
engine operator, cluster or runtime class is reachable from here. The suite
enforces that boundary instead of assuming it: one test walks every source file,
rejects any import of `io.rowweave.internal`, `io.rowweave.cluster`,
`io.rowweave.stream.cluster` or `io.rowweave.ops`, and fails when it cannot read
the module's sources rather than passing on an empty walk
(`PipelinePublicBoundaryTest.productionModuleDoesNotImportEngineInternalsOrRuntimeImplementations`).
The runnable example builds both carriers, portable Batch plan included
(`rowweave-examples/src/main/java/io/rowweave/examples/PipelineCompositionExample.java`).

Submitting and executing a job stays on the documented Batch and Stream entry
points; nothing in this module takes a payload, decodes it, or schedules a task
(`PipelineInspectionTest.publicAdaptersValidateMetadataWithoutDecodingPayload`).

## 2. Composition shapes

The only inter-stage boundary in this release is a versioned dataset:
`PipelineEdge.BoundaryKind` has exactly one value
(`io/rowweave/pipeline/PipelineEdge.java:7-9`). A materialized edge
always runs producer `SINK` to consumer `SOURCE`, the edge schema must
equal both endpoint schemas, and one consumer `SOURCE` operator takes
exactly one input (`validateEdges`, `PipelineDefinition.java:155-194`), so a step that
consumes two datasets declares two `SOURCE` operators inside its own
artifact.

Whether a carrier may declare that fan-in is decided in the portable model, not
here: a portable plan puts no bound on how many `SOURCE` nodes `validateNodes`
accepts (`io/rowweave/portable/PortableBatchPlan.java:70-112`), opcode `JOIN` takes
exactly two inputs (`io/rowweave/portable/PortableOperatorCode.java:33`), and
the Batch compiler projects it onto a `BINARY` operator by `inputCount()`
(`io/rowweave/portable/PortablePlanCompiler.java:185-187`) and `kind()`
(`io/rowweave/artifact/PortablePlanArtifact.java:167-176`). Whether such a plan
then executes is the Batch runtime's contract; what this section judges is that
a carrier of that shape composes. The fan-in row below is therefore pinned both
ways: once from hand-written operator metadata, once from a `PortableBatchPlan`
that passes `PortablePlanCompiler.preflight` and is wrapped by
`PortablePlanArtifact.wrap`.

| Shape | Behaviour | Pinned by a named test |
|---|---|---|
| Batch-only or Stream-only single step | accepted, unit keeps its runtime target | `PipelineSdkTest.builderCoversBatchOnlyAndStreamOnlyDefinitions` |
| One producer materialized into one batch and one stream consumer, two requested outputs | accepted, two materialized edges, canonical unit order `shared, a-batch, b-stream` | `PipelineSdkTest.builderCoversMaterializedMixedRuntimeSharedUpstreamAndMultiOutput` |
| Materialized fan-in: two producers into one step that declares one `SOURCE` operator per input, feeding a `BINARY` operator | accepted, three units and two materialized edges | `PipelineCompositionShapesTest.materializedFanInComposesTwoInputsIntoOneStep`, `materializedFanInOrderIsCanonicalNotCallerOrdered`, `fanInComposesFromPortableArtifactsTheBatchModelProduces` |
| Chained boundary across three steps (`batch → dataset → batch → dataset → stream`) | accepted, three ordered units | `PipelineCompositionShapesTest.chainedBoundariesKeepUnitOrderAcrossThreeSteps` |
| Stream producer materialized into a batch consumer | accepted, two units | `PipelineCompositionShapesTest.streamProducerMaterializesIntoABatchConsumer` |
| Two disconnected roots in one definition, one output each | accepted, two units | `PipelineCompositionShapesTest.disconnectedRootsComposeIntoIndependentUnits` |
| One producer fanned out to three consumers | accepted, three materialized edges | `PipelineCompositionShapesTest.oneProducerFansOutToThreeConsumers` |
| Requested-output pruning of unreachable steps | accepted, artifacts stay atomic | `PipelineCompilerTest.requestedOutputPrunesUnreachableUnitsAndKeepsArtifactsAtomic` |
| Caller collection order | cannot change topology, edges or digest | `PipelineCompilerTest.callerOrderDoesNotChangeCanonicalTopoEdgesOrOutputs`, `PipelineDefinitionCodecTest.canonicalEncodingAndDigestIgnoreCallerCollectionOrder` |
| Revision identity | different revision, different canonical digest | `PipelineSdkTest.revisionIdentityIsVisibleInCanonicalDigest` |

## 3. Refused shapes, and the code each one returns

A definition or compilation refusal is a `PipelineValidationException`, which
extends `IllegalArgumentException` and carries a stable `code()` plus the
offending path (`io/rowweave/pipeline/PipelineValidationException.java:4`,
`:14`, `:15`). A hand-off authority refusal is a
`VersionedDatasetHandoffException`, which extends `IllegalStateException` and
carries the same `code()`
(`io/rowweave/pipeline/VersionedDatasetHandoffException.java:4`, `:12`), so
branching on codes across the whole module means catching both. Code strings
are part of the contract. The module's sources can return 73 distinct codes; 72
are asserted by name today, 71 of them reachable from a public entry point. The
two internal defences in §3.2 account for the difference.

| Attempt | Code | Asserted by |
|---|---|---|
| Two boundaries bound to one consumer source operator (fan-in across a dataset) | `DUPLICATE_SOURCE_BINDING` (`PipelineDefinition.java:191`) | `PipelineDefinitionTest` |
| Fan-in attempted through a `SINK` operator, which declares one input | `INVALID_SEMANTICS` with `SINK requires 1 inputs` (`io/rowweave/artifact/OperatorDescriptor.java:57-61`) | `PipelineCompositionShapesTest.fanInThroughASinkEndpointIsRefusedByTheArtifactContract` |
| Output bound to an operator that is not a sink | `INVALID_OUTPUT_ENDPOINT_KIND` (`PipelineDefinition.java:286`) | `PipelineRefusalContractTest.requestedOutputMustBindASinkOperator` |
| Edge naming an operator the artifact does not declare | `UNKNOWN_OPERATOR` | `PipelineDefinitionTest` |
| Cycle, or an edge that binds a step to itself | `CYCLIC_PIPELINE` and `SELF_EDGE`, raised while the public `PipelineDefinition` constructor checks the graph — `SELF_EDGE` in edge validation (`PipelineDefinition.java:168-170`), cycles in `requireAcyclic` (`:216`) | `PipelineDefinitionTest.cyclesAndSelfEdgesFailClosed` |
| One materialization id naming two producers or schemas | `MATERIALIZATION_CONFLICT` (`PipelineDefinition.java:195-202`) | `PipelineDefinitionTest` |
| Same edge or output id twice | `DUPLICATE_EDGE_ID`, `DUPLICATE_OUTPUT_ID` | `PipelineDefinitionTest` |
| SDK given a plan compiled from a different definition | `PLAN_DEFINITION_MISMATCH` | `VersionedDatasetHandoffSdkTest` |
| SDK given an edge the compiled plan pruned | `EDGE_NOT_SELECTED` (`VersionedDatasetHandoffSdk.java:14-16`) | `VersionedDatasetHandoffSdkTest` |
| Missing, duplicated or unsupported required feature | `MISSING_FEATURE` (`PipelineDefinition.java:64`), `DUPLICATE_FEATURE` (`:60`, `:95`), `UNSUPPORTED_REQUIRED_FEATURE` (`:69-72`) | `PipelineRefusalContractTest.publicDefinitionConstructorRequiresTheCompositionFeature`, `PipelineDefinitionTest`, `PipelineDefinitionCodecTest` |
| Definition compiled with no requested output, unknown or repeated | `MISSING_REQUESTED_OUTPUT`, `UNKNOWN_REQUESTED_OUTPUT`, `DUPLICATE_REQUESTED_OUTPUT` (`PipelineCompiler.java:26`, `:56`, `:51`) | `PipelineCompilerTest` |
| Any count or byte limit exceeded | `LIMIT_EXCEEDED` | `PipelineResourceBoundaryTest`, `PipelineDefinitionTest`, `PipelineDefinitionCodecTest`, `VersionedDatasetHandoffCodecTest` |
| A frame that fails its own integrity check | `CHECKSUM_MISMATCH`, `INVALID_MAGIC`, `UNSUPPORTED_ENVELOPE_VERSION`, `FRAME_LENGTH_MISMATCH` | `VersionedDatasetHandoffCodecTest`, `PipelineDefinitionCodecTest`, and `FileVersionedDatasetHandoffAuthorityTest` for a stored record |

### 3.1 Publicly reachable refusals and their anchors

Each code below is returned by the listed public entry point and asserted by
name, so it cannot be renamed, reordered or dropped without failing a test.

| Code | Returned by | Emitted at | Asserted by |
|---|---|---|---|
| `INVALID_ARTIFACT` | `new PipelineStep(id, null)` | `PipelineStep.java:16` | `PipelineRefusalContractTest.definitionPartsRefuseAMissingOrMalformedComponent` |
| `INVALID_STEP` | `PipelineDefinition.Builder.addStep(null)` | `PipelineDefinitionBuilder.java:33` | same method |
| `INVALID_EDGE` | `PipelineDefinition.Builder.addEdge(null)` | `PipelineDefinitionBuilder.java:52` | same method |
| `INVALID_OUTPUT` | `PipelineDefinition.Builder.addOutput((PipelineOutput) null)` | `PipelineDefinitionBuilder.java:71` | same method |
| `INVALID_BOUNDARY` | `new PipelineEdge(..., boundaryKind = null)` | `PipelineEdge.java:36` | same method |
| `INVALID_SCHEMA_FINGERPRINT` | `new PipelineEdge(..., schemaFingerprint = "not-a-fingerprint")` | `PipelineText.java:40` | same method |
| `INVALID_DEFINITION` | `PipelineCompiler.compile(null, outputs)` | `PipelineCompiler.java:22` | `PipelineRefusalContractTest.compilerAndExplainerEntriesRefuseAMissingPlanOrDefinition` |
| `INVALID_PLAN` | `PipelineExplainer.explain(null)` | `PipelineExplainer.java:9` | same method |
| `INVALID_PIPELINE_PLAN` | `VersionedDatasetHandoffSdk.handoffKey(null, null, …)` | `VersionedDatasetHandoffSdk.java:67` | same method |
| `INVALID_BINDING_INPUT` | `VersionedDatasetHandoffSdk.consumerRequest(…)` given a null plan input | `VersionedDatasetHandoffSdk.java:41` | same method |
| `MISSING_FEATURE` | public `PipelineDefinition` constructor whose required features omit the composition feature | `PipelineDefinition.java:64` | `PipelineRefusalContractTest.publicDefinitionConstructorRequiresTheCompositionFeature` |
| `INVALID_OUTPUT_ENDPOINT_KIND` | `Builder.build()` with an output bound to a non-`SINK` operator | `PipelineDefinition.java:286` | `PipelineRefusalContractTest.requestedOutputMustBindASinkOperator` |
| `INVALID_HANDOFF_KEY` | `new VersionedDatasetProducerAttempt(null, …)` | `VersionedDatasetProducerAttempt.java:17` | `PipelineRefusalContractTest.handOffValuesRefuseAMissingOrOutOfRangePart` |
| `INVALID_ATTEMPT` | `new VersionedDatasetCommitEvidence(null, …)` | `VersionedDatasetCommitEvidence.java:27` | same method |
| `INVALID_EVIDENCE` | `new VersionedDatasetVisibleWinner(null, time)` | `VersionedDatasetVisibleWinner.java:13` | same method |
| `INVALID_TIMESTAMP` | `new VersionedDatasetVisibleWinner(evidence, 0)` fails the `positive` guard | `VersionedDatasetHandoffSupport.java:30-32` | same method |
| `INVALID_VERSION` | `new VersionedDatasetCommitEvidence(…, storageContractVersion = 0, …)` | `VersionedDatasetHandoffSupport.java:24` | same method |
| `INVALID_BYTES` | `new VersionedDatasetAuthorityRecord(null)`, or a null `readerBinding` | `VersionedDatasetHandoffSupport.java:47` | same method |
| `INVALID_ARTIFACT_METADATA` | `PipelineArtifactAdapter.forBatch(…)` given a carrier that is not a portable Batch plan, the trusted in-process graph an `ExecutionEnvironment` compiles among them (`LegacyPlanArtifactAdapter.java:53`, which yields `TRUSTED_LEGACY_PLAN_GRAPH`; refused as `LEGACY_NOT_PUBLIC_V1` at `PlanArtifactValidator.java:210`) | `PipelineArtifactAdapter.java:29` | `PipelineInspectionTest.publicAdaptersValidateMetadataWithoutDecodingPayload`, `PipelineCompositionShapesTest.trustedLegacyBatchArtifactIsRefusedAtTheStepSeam` |
| `RUNTIME_TARGET_MISMATCH` | `PipelineArtifactAdapter.forStream(…)` given a Batch artifact | `PipelineArtifactAdapter.java:38` | `PipelineInspectionTest.publicAdaptersValidateMetadataWithoutDecodingPayload` |
| `INVALID_BINDING` | `new VersionedDatasetConsumerBinding(null, null, time)` | `VersionedDatasetConsumerBinding.java:16` | same method. The same code name is also asserted for a different class in the core plan codec (`rowweave-core/src/test/java/io/rowweave/portable/PortableBindingTest.java:71`); this constructor is a separate public entry with its own assertion |
| `BINDING_MISMATCH` | a binding whose request names another hand-off key, or another expected winner digest, than the supplied winner; both branches | `VersionedDatasetConsumerBinding.java:21` | `PipelineRefusalContractTest.consumerBindingMustIdentifyTheWinnerItBinds`, which also binds a matching triple successfully so the assertion is not vacuous |
| `AUTHORITY_IO_FAILURE` | `new FileVersionedDatasetAuthorityRecordStore(root)` where `root` is a regular file | `FileVersionedDatasetAuthorityRecordStore.java:163` | `PipelineRefusalContractTest.durableAuthorityStorageReportsItsOwnFailureCode` |
| `INVALID_RECORD` | `VersionedDatasetHandoffCodec.encodeKey(null)` and the five sibling encoders | `VersionedDatasetHandoffCodec.java:116` | `VersionedDatasetHandoffFaultContractTest.publicEncodersRefuseAMissingValue` |
| `NON_CANONICAL_FRAME` | `decodeEvidence(frame)` given a body extended with trailing bytes and re-signed so the checksum passes | `VersionedDatasetHandoffCodec.java:197` | `VersionedDatasetHandoffFaultContractTest.frameWithExtraBytesIsRefusedAsNonCanonical` |
| `AUTHORITY_RECORD_MISMATCH` | an authority record pairing one claim with a winner from another key | `VersionedDatasetAuthorityRecordCodec.java:114` | `VersionedDatasetHandoffFaultContractTest.authorityRecordWithAWinnerFromAnotherKeyIsRefused` |
| `AUTHORITY_CONTENTION` | a durable store whose compare-and-set never succeeds | `StoreBackedVersionedDatasetHandoffAuthority.java:232` | `VersionedDatasetHandoffFaultContractTest.exhaustedCompareAndSetBudgetIsReportedAsContention` |
| `KEY_DIGEST_CONFLICT` | a store that answers a claim with a record owned by another key | `StoreBackedVersionedDatasetHandoffAuthority.java:226` | `VersionedDatasetHandoffFaultContractTest.aRecordOwnedByAnotherKeyIsReportedAsDigestConflict` |

The codec and store rows are decided by components a caller supplies or
targets: the public hand-off codec (`VersionedDatasetHandoffCodec.java:19`) and
the durable-store service provider interface
(`VersionedDatasetAuthorityRecordStore.java:10`), whose record bytes come from
the caller's own store (`VersionedDatasetAuthorityRecord.java:8`). A store that
never wins a compare-and-set, or that answers with a record belonging to another
key, therefore reaches `AUTHORITY_CONTENTION` and `KEY_DIGEST_CONFLICT` without
a test fixture, by handing the public
`StoreBackedVersionedDatasetHandoffAuthority` constructor
(`StoreBackedVersionedDatasetHandoffAuthority.java:14`) that store. The
authority clock is not in that group and no store can imitate it; §3.2 says
why.

### 3.2 The two codes that are not a caller-facing contract

`STEP_NOT_SELECTED` (`VersionedDatasetHandoffSdk.java:109`) fires when an edge
endpoint is missing from an already compiled plan. Both public producers of
`PipelineExecutionPlan` — `PipelineValidator.validateAndCompile`
(`PipelineValidator.java:9`) and the public `PipelineCompiler.compile`
(`PipelineCompiler.java:19`) — prune all-or-nothing, which
`PipelineCompilerTest.requestedOutputPrunesUnreachableUnitsAndKeepsArtifactsAtomic`
pins, and the plan class has no public constructor
(`PipelineExecutionPlan.java:16`). No public call can build a plan whose
endpoint is absent, so the check stays as defence in depth and is deliberately
not asserted through the public API. It is the only one of the 73 codes with no
named assertion.

`INVALID_AUTHORITY_TIME` (`StoreBackedVersionedDatasetHandoffAuthority.java:201`)
fires when the authority clock hands back a non-positive epoch timestamp. Every
public constructor binds the host clock
(`StoreBackedVersionedDatasetHandoffAuthority.java:14`,
`FileVersionedDatasetHandoffAuthority.java:12`,
`InMemoryVersionedDatasetHandoffAuthority.java:14`) and a clock can only be
installed through a package-private one
(`StoreBackedVersionedDatasetHandoffAuthority.java:19`,
`FileVersionedDatasetHandoffAuthority.java:17`,
`InMemoryVersionedDatasetHandoffAuthority.java:18`). The public
`VersionedDatasetAuthorityRecordStore` exposes only `read` and `compareAndSet`
(`VersionedDatasetAuthorityRecordStore.java:16`, `:20`) and carries no
timestamp, so no store can imitate a broken clock.
`VersionedDatasetHandoffFaultContractTest.aNonPositiveAuthorityClockStopsThePublish`
asserts the code by name through that internal seam, which keeps the guard from
being renamed or dropped: it defends against a host whose clock is not set, and
is not a value a caller passes in.

## 4. Resource bounds are declared before allocation

`PipelineLimits` fixes the contract surface: 256 bytes for an id or feature,
256 features, 4096 steps, 16384 edges, 256 outputs and a 96 MiB body ceiling
(`io/rowweave/pipeline/PipelineLimits.java:5-11`). Limits are enforced as
boundaries, not after-effects: a collection one element over the cap fails
before any element is read — the test hands the definition a recording list and
requires the read count to stay at zero
(`PipelineResourceBoundaryTest.collectionLimitPlusOneFailsBeforeElementTraversal`) —
and the codec reports every reservation it makes so a caller can release it
(`PipelineResourceBoundaryTest`, four cases, including
`aggregateArtifactBytesRespectExactBodyBoundaryBeforeReservation`).

Encoding reserves twice the body length via `multiplyExact` before allocating
it (`PipelineDefinitionCodec.java:53-55`), so an oversized definition cannot
consume memory on the way to its own rejection.

### 4.1 Published ceilings

| Limit | Value | Declared at |
|---|---:|---|
| `MAX_ID_BYTES` | 256 | `io/rowweave/pipeline/PipelineLimits.java:5` |
| `MAX_FEATURE_BYTES` | 256 | `io/rowweave/pipeline/PipelineLimits.java:6` |
| `MAX_FEATURES` | 256 | `io/rowweave/pipeline/PipelineLimits.java:7` |
| `MAX_STEPS` | 4096 | `io/rowweave/pipeline/PipelineLimits.java:8` |
| `MAX_EDGES` | 16384 | `io/rowweave/pipeline/PipelineLimits.java:9` |
| `MAX_OUTPUTS` | 256 | `io/rowweave/pipeline/PipelineLimits.java:10` |
| `MAX_BODY_BYTES` | 96 MiB | `io/rowweave/pipeline/PipelineLimits.java:11` |
| `SHA256_BYTES` | 32 | `io/rowweave/pipeline/PipelineLimits.java:12` |

Hand-off records carry their own ceilings, enforced on the frame before any
store write:

| Limit | Value | Declared at |
|---|---:|---|
| `MAX_ID_BYTES` | 256 | `io/rowweave/pipeline/VersionedDatasetHandoffLimits.java:5` |
| `MAX_READER_BINDING_BYTES` | 64 KiB | `io/rowweave/pipeline/VersionedDatasetHandoffLimits.java:6` |
| `MAX_RECORD_BYTES` | 256 KiB | `io/rowweave/pipeline/VersionedDatasetHandoffLimits.java:7` |
| `SHA256_BYTES` | 32 | `io/rowweave/pipeline/VersionedDatasetHandoffLimits.java:8` |

Numeric ceilings for every other published family, including the plan-artifact
limits a Pipeline step carries, are indexed in
[formats-limits-spi](formats-limits-spi.md).

## 5. Hand-off durability

The versioned dataset hand-off authority decides, durably, which producer
attempt won. Evidence in this release:

* a write whose acknowledgement is lost is resolved by querying the durable
  record instead of assuming failure
  (`FileVersionedDatasetHandoffAuthorityTest.durableWriteThenAckLossIsRecoveredByQuery`);
* concurrent instances of the durable authority produce exactly one winner
  (`FileVersionedDatasetHandoffAuthorityTest.concurrentInstancesStillChooseOneEvidence`),
  and a late old attempt is fenced before it becomes visible
  (`InMemoryVersionedDatasetHandoffAuthorityTest.retryFencesLateOldAttemptBeforeVisibility`),
  which drives the same `StoreBacked` state machine the file authority uses;
* a corrupt record fails closed and an unreferenced temporary file is ignored
  rather than adopted (`FileVersionedDatasetHandoffAuthorityTest.corruptRecordFailsClosedAndUnreferencedTempIsIgnored`).
  The durable write is a private `temporary` file that is forced and then renamed
  atomically (`io/rowweave/pipeline/FileVersionedDatasetAuthorityRecordStore.java:102-110`).
* producer and independent consumers recover across three separate JVMs
  (`FileVersionedDatasetHandoffProcessTest.producerAndConsumerRecoverAcrossIndependentJvms`,
  which forks real JVMs at `:32-35`);
* every durable frame has a canonical round trip and a stable identity, and a
  frame that does not match its own canonical form is refused
  (`VersionedDatasetHandoffCodecTest`, four cases).
* a publish that died before the atomic rename leaves only an orphaned
  temporary file, and neither a reader nor a fresh authority treats it as
  durable
  (`FileVersionedDatasetHandoffAuthorityTest.aPublishTornBeforeTheRenameIsNeverAdoptedAndOneRetryWins`,
  `anOrphanedTemporaryRecordIsNotDurableForAFreshAuthority`). The second case
  places a complete winner record under the file name the writer would have
  used, so a store that adopted a temporary file instead of a renamed record
  fails the assertion rather than passing it.

### 5.1 Producer-to-consumer hand-off walkthrough

The published sequence, as a task an integrator can follow. The runnable form
is `PipelineCompositionExample` in `rowweave-examples`; each step names the
public method that performs it.

1. Compile the definition with the requested outputs:
   `PipelineValidator.validateAndCompile(definition, outputs)` produces the
   execution plan the hand-off identities are derived from.
2. Derive the hand-off key with
   `VersionedDatasetHandoffSdk.handoffKey(definition, plan, edgeId,
   logicalRunId)` (`io/rowweave/pipeline/VersionedDatasetHandoffSdk.java:7`).
   It folds the pipeline, revision, definition digest, edge, materialization,
   schema fingerprint, producer step and sink operator, producer artifact id,
   and logical run into one identity, and refuses an edge the compiled plan
   did not retain.
3. Bind the attempt with `VersionedDatasetHandoffSdk.producerAttempt(key,
   producerRunId, attemptId, attemptNumber, fenceToken)`
   (`io/rowweave/pipeline/VersionedDatasetHandoffSdk.java:28`). A retried
   producer publishes a new attempt number and fence token over the same key.
4. Claim, then publish: the authority reserves the key for that attempt with
   `claim(producerAttempt)`, and `publish(commitEvidence)` commits the durable
   winner
   (`rowweave-examples/src/main/java/io/rowweave/examples/PipelineCompositionExample.java:94`).
5. The consumer reads the visible winner and builds its bind request with
   `VersionedDatasetHandoffSdk.consumerRequest(definition, plan, key, winner,
   consumerRunId, consumerAttemptId)`
   (`io/rowweave/pipeline/VersionedDatasetHandoffSdk.java:35`), re-deriving
   the key from its own definition and plan, so a consumer cannot bind a
   materialization from a different pipeline generation.
6. `bind(consumerRequest)` succeeds only when the request matches the durable
   visible winner
   (`rowweave-examples/src/main/java/io/rowweave/examples/PipelineCompositionExample.java:100`).
   A successful bind — not the publish alone — is the consumer's acceptance
   point.

Failures in this sequence are typed: every hand-off refusal raises
`VersionedDatasetHandoffException` with a stable code, the same code-string
contract recorded in section 3, so one routing table covers definition,
compilation, and authority refusals.

Not certified, and not claimed:

* no Pipeline case runs in the multi-process cluster suite, so pipeline
  behaviour is proven by the tests named here rather than by cluster qualification;
* cancellation is not part of this contract — the module defines no cancel,
  interrupt or timeout surface at all, so a caller that wants cancellation
  composes it from the Batch and Stream job APIs;
* the credit-style accounting of a running job is out of scope here: nothing in
  this module executes, see section 1.

## 6. Where this is documented elsewhere

* Module list and role: [Installation](../quickstart/installation.md).
* Example index: [Examples](../guides/examples.md) routes every tested entry
  point; the Pipeline family is `PipelineCompositionExample`, which builds a
  shared Batch upstream, materializes one versioned dataset into a Stream
  consumer, and walks the full hand-off sequence of section 5.1.
* The task-oriented composition journey starts at
  [external platform integration](../guides/external-platform-integration.md),
  which builds one portable carrier end to end.
* Method-level API contracts on both sides of a materialized edge:
  [Batch API reference](batch-api.md) for the producer run and durable sink,
  [Stream API reference](stream-api.md) for the consumer source replay.
* Numeric ceilings owned elsewhere: [formats-limits-spi](formats-limits-spi.md).
* Open work tracked in [Roadmap](../../../ROADMAP.md); the data-plane behaviour
  of a running job is in [Data plane limits](data-plane-limits.md).
