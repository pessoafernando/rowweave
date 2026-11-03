---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Formats, limits, and SPI reference

This page is the authority for externally visible numeric limits, record
shapes, provider qualification, and the route to each frozen wire format. It
does not reinterpret opaque magic identifiers: those are explained only in
[format constants](format-constants.md). The [capability
matrix](capability-matrix.md) states whether a boundary is supported; the
[error reference](errors-and-refusals.md) explains typed failures.

The authoritative route for every boundary is fixed:

| Boundary | Authoritative page |
|---|---|
| Opaque format tags and magic families | [Format constants](format-constants.md) |
| Shuffle socket behavior and certified transport limits | [Data-plane limits](data-plane-limits.md) |
| Portable artifact and Pipeline numeric limits | this page |
| Stream checkpoint state shapes and bounds | this page |
| Rejected-row record shape | this page |
| Object-storage manifest contract | this page |
| PostgreSQL CDC frontier and connector limits | this page |
| Provider and conformance SPI | this page |
| Pipeline versioned composition and handoff refusals | [Pipeline boundary](pipeline-boundary.md) |
| Release compatibility and frozen protocol policy | [Release contract](versioned-release-v1.md) |

Shuffle transport evidence is represented by `MuxConnectionPoolFanoutTest` and
the source anchors in [data-plane limits](data-plane-limits.md).

## Portable artifact and Pipeline limits

A portable artifact is decoded only within these bounds. A larger graph is
refused before payload interpretation rather than truncated.

| Limit | Value | Effect |
|---|---:|---|
| Plan text, identifier, and feature text | 256 bytes | Text fields and identifiers must fit the canonical descriptor. |
| Schema fingerprint | 128 bytes | Schema identity is a bounded canonical fingerprint. |
| Required or optional features | 1,024 | The feature set is finite and explicitly declared. |
| Operators | 100,000 | Graph admission is bounded independently of memory. |
| Inputs per operator | 1,024 | Operator arity is bounded and checked during decode. |
| Artifact payload | 64 MiB | Payload frames above this size are refused. |
| Artifact body | 72 MiB | The full envelope body, including descriptor overhead, is bounded. |

Source authority: [`PlanArtifactLimits`](../../../rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifactLimits.java).
Tested by `PlanArtifactModelTest`.

A Pipeline definition has separate composition bounds because it composes
already compiled artifacts:

| Limit | Value | Effect |
|---|---:|---|
| Identifier and feature text | 256 bytes | IDs and capability tokens remain bounded. |
| Features | 256 | Pipeline capability negotiation is finite. |
| Steps | 4,096 | The composed plan has an explicit step ceiling. |
| Materialized edges | 16,384 | Dataset hand-off edges are bounded. |
| Requested outputs | 256 | One definition can expose a bounded output set. |
| Canonical body | 96 MiB | Definition frames are refused above this bound. |
| Digest length | 32 bytes | Canonical integrity uses SHA-256. |

Source authority: [`PipelineLimits`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineLimits.java).
Tested by `PipelineResourceBoundaryTest` and `PipelineDefinitionCodecTest`.

A versioned dataset record has these allocation bounds: IDs are at most
256 bytes, the reader binding is at most 64 KiB, and a record is at most
256 KiB. Source authority:
[`VersionedDatasetHandoffLimits`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffLimits.java).
Tested by `VersionedDatasetHandoffCodecTest`.

## Application data limits

These limits protect embedded and compatibility execution paths before a job
is admitted.

| Surface | Default | Hard maximum | Effect |
|---|---:|---:|---|
| Compatibility resident rows | 100,000 | 1,000,000 | An exact embedded collection cannot materialize beyond this row fence. |
| Compatibility resident bytes | 64 MiB | 512 MiB | Row count and byte count are checked together; crossing either fails. |
| Map-partition output rows | configured | 1,000,000,000 | One physical invocation cannot emit more than its declared row quota. |
| Map-partition output bytes | configured | 1 TiB | Byte output is checked independently of row count. |

Source authority:
[`CompatibilityDataLimits`](../../../rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimits.java)
and
[`MapPartitionEmissionLimit`](../../../rowweave-core/src/main/java/io/rowweave/MapPartitionEmissionLimit.java).
Tested by `CompatibilityDataLimitsTest`, `DataSetOutputLimitsTest`, and
`MapPartitionEmissionLimitTest`.

## Stream checkpoint state formats

Checkpoint state is split into four independently bounded formats: source
position snapshots, the canonical checkpoint manifest, checkpoint decisions,
and chunked payload storage. Every format is canonical and fail-closed on
length, digest, version, or trailing-byte mismatch. The opaque frame tags are
described only by [format constants](format-constants.md).

A source position snapshot records the artifact identity, source operator,
subtask, provider key and contract/configuration digests, split identity,
position codec identity, split descriptor digest, logical position, and the
provider-owned position frame. Text identity fields are at most 1,024 bytes;
a provider position frame is at most 4 MiB; the logical position is
non-negative. Restore accepts the snapshot only when the artifact, provider,
configuration, split, codec, and descriptor digest all match the live task.
Source authority:
[`StreamSourcePositionCheckpoint`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourcePositionCheckpoint.java).
Tested by `StreamSourcePositionCheckpointTest`.

The checkpoint manifest is a bounded 16 MiB body, a 16-byte canonical header,
and a 32-byte SHA-256 digest. It contains 1 to `MAX_TASK_RECEIPTS` task
receipts. Each receipt carries a task ID of at most 512 bytes, assignment
generation, worker incarnation of at most 256 bytes, and 1 to 256 evidence
items; each evidence item has a typed kind, member ID of at most 512 bytes,
digest of at most 64 bytes, and byte count. The physical-task receipt ceiling
is `MAX_BODY_BYTES / 107`, the smallest valid receipt size. Source authority:
[`CheckpointManifestCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointManifestCodec.java).
Manifest membership and coordinator behavior are tested by
`CheckpointMemberContractTest` and `StreamCheckpointCoordinatorTest`.

A checkpoint decision reuses the common bounded envelope, has a 4,096-byte
body limit, and records checkpoint identity, status wire ID, and an optional
64-byte manifest digest. Source authority:
[`CheckpointDecisionCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointDecisionCodec.java).
Tested by `CheckpointDecisionContractTest`.

The file payload store chunks staged bytes, records a SHA-256 content digest,
and refuses payload, chunk count, or manifest bounds before writing. A chunk is
at most 16 MiB, a payload may have at most 65,536 chunks, and the chunk
manifest body is bounded to 16 MiB. A stored checkpoint member frame is at most
8 KiB. Source authority:
[`FileCheckpointChunkStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileCheckpointChunkStore.java)
and
[`FileCheckpointMemberStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileCheckpointMemberStore.java).
Tested by `FileCheckpointChunkStoreTest` and `FileCheckpointMemberStoreTest`.

## Rejected-row record

The object-storage quality route preserves the original fields and appends
exactly eight classification fields. A valid row uses tag `0`; a rejected row
uses tag `1`. The appended fields are, in order:

| Index | Field | Rejected-row content |
|---:|---|---|
| 0 | classification tag | `0` for valid, `1` for rejected |
| 1 | source operator identity | originating source operator UID; empty for valid |
| 2 | split identity digest | SHA-256 identity of the source split; empty for valid |
| 3 | split ordinal | source-local record ordinal; `-1` for valid |
| 4 | quality error code | one closed raw-decode failure code |
| 5 | rule ID | fixed `OBJECT_ROW_DECODE_V1` |
| 6 | field index | `-1`; raw framing precedes field decoding |
| 7 | detail token | one closed token such as `UTF8`, `CR`, `ROW_BYTES`, or `ROWWEAVE_ROW_TEXT_V1` |

The reason identity and detail token are validated at construction; a run
cannot invent an arbitrary error code or detail string. Source authority:
[`ObjectStorageQualityRecordDecoder`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageQualityRecordDecoder.java),
[`QualityReasonKey`](../../../rowweave-core/src/main/java/io/rowweave/QualityReasonKey.java),
and
[`TypedQualityManifest`](../../../rowweave-core/src/main/java/io/rowweave/TypedQualityManifest.java).
Tested by `ObjectStorageQualityBoundsLifecycleTest`,
`TypedQualityManifestModelTest`, and `QualityEvidenceReadGuardTest`.

The typed quality summary has a 128 KiB decision frame and a 32 KiB exemplar
section. Its policy accepts at most 256 reason thresholds and 64 exemplars.
Source authority:
[`QualityGateDecisionCodec`](../../../rowweave-core/src/main/java/io/rowweave/QualityGateDecisionCodec.java)
and
[`QualityGatePolicy`](../../../rowweave-core/src/main/java/io/rowweave/QualityGatePolicy.java).
Tested by `QualityGateDecisionCodecTest` and `QualityGatePolicyTest`.

## Object-storage manifest contract

The S3-compatible source is a read-only, exact-snapshot boundary. It supports
`CSV_RFC4180` and `NDJSON_UTF8` objects. A manifest freezes the current object
set, schema, ETag, optional version ID, content length, SHA-256, format, and
split identity into a deterministic ordered set with a SHA-256 manifest
digest. A later change to a frozen object is detected on read instead of being
silently accepted.

| Limit or rule | Value | Effect |
|---|---:|---|
| Manifest objects | 1–4,096 | Empty manifests and larger exact object sets are refused. |
| Listing page | 1,000 keys | The planner pages deterministically and rejects invalid pagination. |
| Object key | 4,096 UTF-8 bytes | Longer keys cannot enter a manifest. |
| Object size | configuration bound, at most 1 TiB | Planning and reading enforce the configured object ceiling. |
| Row bytes | 1 KiB–16 MiB | The row reader enforces both lower and upper framing bounds. |
| Configuration payload | 16 KiB | Connector configuration is a bounded signed deployment payload. |
| Credential frame | 16 KiB | Access key, secret key, and session token have separate bounds. |

Source authority:
[`S3Manifest`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3Manifest.java),
[`S3ManifestPlanner`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ManifestPlanner.java),
[`S3ConnectorConfiguration`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorConfiguration.java),
and
[`S3CredentialCodec`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3CredentialCodec.java).
Tested by `S3ManifestPlannerTest`, `S3ConnectorProviderConformanceTest`,
`S3CredentialCodecTest`, and the MinIO integration test when its qualification
environment is enabled.

## PostgreSQL CDC frontier and connector limits

A PostgreSQL CDC position is the exact recovery and acknowledgement boundary
for one database. It is either a snapshot handoff or a transaction commit. A
commit position carries a nonzero transaction ID; a snapshot handoff does not.
The position frame is at most 128 bytes and identifies the codec as
`rowweave.postgres.cdc-position.v1`.

The LSN is canonical uppercase `HIGH/LOW`: neither half has a leading zero,
and both halves are hexadecimal. The frame also records the unsigned database
system identifier and database OID. Source authority:
[`PostgresCdcPosition`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCdcPosition.java)
and
[`PostgresCdcPositionCodec`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCdcPositionCodec.java).
Tested by `PostgresCdcPositionCodecTest`.

Configuration and binding limits are:

| Limit | Value | Effect |
|---|---:|---|
| Bound tables | 256 | One CDC configuration cannot exceed this table count. |
| Total bound columns | 4,096 | Column projection has a global ceiling. |
| Configuration frame | 512 KiB | The full connector configuration is bounded. |
| Transaction changes | 1,000,000 | Per-transaction admission is bounded. |
| Transaction bytes | 1 GiB | Oversized transactions are refused rather than unbounded. |
| Retained WAL | 1 TiB | WAL retention has an explicit operator ceiling. |
| Table/column names and field IDs | 63 / 256 bytes | Binding identity fields have fixed widths. |
| Credential frame | 8 KiB | User name is at most 256 bytes; password is at most 4 KiB. |

Source authority:
[`PostgresConnectorConfiguration`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorConfiguration.java),
[`PostgresTableBinding`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresTableBinding.java),
and
[`PostgresCredentialCodec`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCredentialCodec.java).
Tested by `StreamConnectorConfigurationTest`, `PostgresCredentialCodecTest`, and
the PostgreSQL connector integration tests when their qualification environment
is enabled.

## Provider and conformance SPI

The Stream connector SDK exposes source, sink, transactional sink, function,
dynamic source, and CDC source provider contracts. Provider frames use the
common 4 MiB Stream frame ceiling. A source must expose a checkpoint position
frame, restore it exactly, and keep provider positions strictly monotonic. A
bounded source must terminate once, support wakeup and close, and honor time
and control deadlines. A transactional sink must expose prepare/commit/abort
semantics and a queryable transaction status. Source authority:
[`StreamConnectorConformance`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamConnectorConformance.java).
Tested by `StreamBoundedSourceConformanceKitTest` and
`StreamConnectorConformanceKitTest`.

The Batch durable sink provider qualification checks graph admission, no side
effect before Worker admission, Worker admission and fencing, duplicate
submission and query replay, at-most-one publication, all-or-nothing visibility,
terminal receipt and locator closure, safe orphan cleanup, bounded blocking
behavior, and fresh handle rebuild. Source authority:
[`DurableBatchSinkProviderConformance`](../../../rowweave-core/src/test/java/io/rowweave/sink/DurableBatchSinkProviderConformance.java).
Tested by `DurableBatchSinkProviderConformanceRedTest`,
`ProviderClusterConformanceHarness`, and
`SignedProviderClusterTest`.

The pipeline authority-store SPI must implement durable compare-and-set,
canonical authority records, visibility fencing, idempotent replay, and
conflict reporting. Source authority:
[`VersionedDatasetAuthorityRecordStoreConformance`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetAuthorityRecordStoreConformance.java).
Tested by `VersionedDatasetAuthorityRecordStoreConformanceTest`.

Connector deployment remains separate from provider discovery: a provider
class, signed contract digest, deployment set, capability intersection, and
resource budget must all pass before provider access. Route deployment
procedures to [deployment](../operations/deployment.md) and
[security](../operations/security.md).
