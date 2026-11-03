---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# S3-compatible snapshot connector

`rowweave-connector-s3` is a bounded snapshot source over an S3-compatible object
store: it reads a fixed set of objects, decoded as CSV or newline-delimited JSON,
and finishes. It is not a change-data-capture connector and it does not discover
new objects while a run is in progress. The manifest that names the objects, and
the object-format constants, are in
[formats, limits and SPI](formats-limits-spi.md); this page is the connector's
own configuration, contract, packaging and failure surface.

## Roles

| Type | Responsibility | Declared |
|---|---|---|
| `S3ConnectorProvider` | the signed provider a Worker binds | `io/rowweave/connector/s3/S3ConnectorProvider.java:11` |
| `S3ConnectorContract` | the static contract shipped with the provider | `io/rowweave/connector/s3/S3ConnectorContract.java:8` |
| `S3ConnectorConfiguration` | the inert, canonical configuration value | `io/rowweave/connector/s3/S3ConnectorConfiguration.java:15` |
| `S3ObjectFormat` | the closed object-format enum | `io/rowweave/connector/s3/S3ObjectFormat.java:4` |
| `S3SnapshotReader` | bounded split reading with spill to local disk | `io/rowweave/connector/s3/S3SnapshotReader.java:20` |
| `S3ManifestPlanner` | turn a bucket listing into a bounded manifest | `io/rowweave/connector/s3/S3ManifestPlanner.java:14` |
| `S3DeploymentPackager` | build the deployable connector image | `io/rowweave/connector/s3/S3DeploymentPackager.java:22` |
| `S3Credentials` | the secret material, addressed by alias only | `io/rowweave/connector/s3/S3Credentials.java:6` |

`providerKey()` at `io/rowweave/connector/s3/S3ConnectorProvider.java:14` returns
the key a `StreamSourceSpec` must name for this provider to be selected, and
`contractDigest()` at `io/rowweave/connector/s3/S3ConnectorProvider.java:18`
returns the digest the Worker compares against the contract it discovered. A
provider whose digest does not match the one carried in the plan is refused before
any object is listed.

## Declared capabilities

[`S3ConnectorContract`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorContract.java)
is a single static value, built at
`io/rowweave/connector/s3/S3ConnectorContract.java:12-17` from
`StreamConnectorProviderContract.source(...)`
(`io/rowweave/stream/contract/StreamConnectorProviderContract.java:82`):

| Field | Value | Site |
|---|---|---|
| Deployment id | `rowweave-connector-s3` | `io/rowweave/connector/s3/S3ConnectorContract.java:9` |
| Provider key | `rowweave.s3.snapshot` | `io/rowweave/connector/s3/S3ConnectorContract.java:10` |
| Configuration codec | `StreamConnectorConfiguration.CODEC_ID` | `io/rowweave/connector/s3/S3ConnectorContract.java:14` |
| Configuration ceiling | 64 KiB | `io/rowweave/connector/s3/S3ConnectorContract.java:15` |
| Concurrent opens | 64 | `io/rowweave/connector/s3/S3ConnectorContract.java:15` |
| Open deadline | 120000 ms | `io/rowweave/connector/s3/S3ConnectorContract.java:15` |
| `boundedEndOfInput` | `true` | `io/rowweave/connector/s3/S3ConnectorContract.java:16` |
| `dynamicDiscovery` | `false` | `io/rowweave/connector/s3/S3ConnectorContract.java:16` |
| `residentCdc` | `false` | `io/rowweave/connector/s3/S3ConnectorContract.java:16` |
| `externalCheckpointAcknowledgement` | `false` | `io/rowweave/connector/s3/S3ConnectorContract.java:16` |

The four trailing booleans are the whole capability story: this connector may
finish, may not grow its split set mid-run, is not a resident feed
([resident CDC contract](stream-cdc.md)), and does not need a checkpoint callback
because there is no external retention to release. `contract()` at
`io/rowweave/connector/s3/S3ConnectorContract.java:23`, `contractDigest()` at `:27`
and `encodedContract()` at `:29` are the three ways the same value is read.

## Configuration surface

[`S3ConnectorConfiguration`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorConfiguration.java)
is immutable and carries no live client. Its accessors are the documentation of
the surface: `endpoint()` at
`io/rowweave/connector/s3/S3ConnectorConfiguration.java:186`, `region()` at `:187`,
`bucket()` at `:188`, `format()` at `:189`, `credentialsReference()` at `:190`,
`maximumObjectBytes()` at `:191`, `maximumRowBytes()` at `:192`,
`temporaryDiskBudgetBytes()` at `:193`, `connectTimeoutMillis()` at `:196`,
`readTimeoutMillis()` at `:197`, `maximumAttempts()` at `:198`,
`pathStyleAccess()` at `:199`, `allowInsecureEndpoint()` at `:200` and
`nullLiteral()` at `:201`.

| Ceiling or bound | Constant site | Value |
|---|---|---|
| Largest single object | `io/rowweave/connector/s3/S3ConnectorConfiguration.java:17` | 1 TiB |
| Minimum row budget | `io/rowweave/connector/s3/S3ConnectorConfiguration.java:18` | 1 KiB |
| Largest single row | `io/rowweave/connector/s3/S3ConnectorConfiguration.java:19` | 16 MiB |
| Payload bytes | `io/rowweave/connector/s3/S3ConnectorConfiguration.java:25` | 16 KiB |
| Largest manifest | `io/rowweave/connector/s3/S3Manifest.java:20` | 4096 objects |
| Listing page size | `io/rowweave/connector/s3/S3ManifestPlanner.java:15` | 1000 keys |

The constructor range checks at
`io/rowweave/connector/s3/S3ConnectorConfiguration.java:64-73` require the local
temporary-disk budget to be at least the maximum object size and no more than the
object ceiling, a connect timeout in 100 to 120000 ms, a read timeout in 100 to
600000 ms, and at most ten attempts. A configuration that violates any of those is
not constructible, so an unbounded or unreachable setting cannot reach a Worker.

Region and bucket names are pattern-checked rather than free text, at
`io/rowweave/connector/s3/S3ConnectorConfiguration.java:26-29`, and an endpoint
must be a valid absolute URI whose path is empty or `/`
(`io/rowweave/connector/s3/S3ConnectorConfiguration.java:166`). Every violation
fails with the code `S3_CONFIGURATION_INVALID`, declared at
`io/rowweave/connector/s3/S3ConnectorConfiguration.java:21`.

Secrets are never configuration fields. `CREDENTIALS_ALIAS` at
`io/rowweave/connector/s3/S3ConnectorConfiguration.java:16` names where the Worker
looks up credentials in its own secret store, so a plan can travel between processes
without carrying a key. `S3CredentialCodecTest` qualifies the encode-and-decode
round trip of that material, and the frame it produces is bounded by the payload
ceiling above.

## Canonical form

`encode()` at `io/rowweave/connector/s3/S3ConnectorConfiguration.java:80` and
`decode(byte[])` at `:92` are the persisted form. The frame starts with a fixed
magic at `io/rowweave/connector/s3/S3ConnectorConfiguration.java:24`, and decoding
rejects a frame that is not byte-identical to what encoding the decoded value would
produce. That is why a configuration can be re-planned and re-verified without a
provider being loaded: the canonical bytes are the identity. The magic family is
catalogued in [format constants](format-constants.md) rather than explained here.

`builder()` at `io/rowweave/connector/s3/S3ConnectorConfiguration.java:78` returns
the [`Builder`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorConfiguration.java)
declared at `:213`. A builder with a missing required field fails at `build()` with
the same `S3_CONFIGURATION_INVALID` code, not at the first use.

## Object format

[`S3ObjectFormat`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ObjectFormat.java)
is closed at `io/rowweave/connector/s3/S3ObjectFormat.java:5-6`: `CSV_RFC4180` and
`NDJSON_UTF8`. There is no Parquet, Avro or ORC path in this connector. CSV is
parsed with the RFC 4180 quoting rules (`S3CsvRecordReader`), NDJSON with one
document per line (`S3NdjsonDecoder`), and `nullLiteral()` decides which token
becomes a SQL null rather than the string `"null"`. Type coercion lives in
`S3ValueParser` and is bound by the row-validation rules in
[schema conversion and row validation](schema-and-validation.md).

## Reading and spill

`S3SnapshotReader` opens one object per split, reads it with a bounded line reader
(`S3BoundedLineReader`), and stages the decompressed stream into a local temporary
file sized by `temporaryDiskBudgetBytes()`. A row larger than `maximumRowBytes()`
is refused rather than buffered in memory, which is the same discipline the engine
applies to a breaker working set: the budget is declared before allocation. The
lifecycle — open, read, close, and close-after-cancel — is qualified by
`S3SnapshotReaderLifecycleTest`, and the row-level behaviour by `S3SnapshotReaderTest`.

`S3ManifestPlanner` produces the manifest from a listing, bounded by the listing page size at `io/rowweave/connector/s3/S3ManifestPlanner.java:15`
and the object-count ceiling above; `S3ManifestPlannerTest` qualifies the truncation
and page-token behaviour, and the manifest itself is a frozen value that a replay
re-reads rather than re-lists — see
[built-in Batch sources](batch-sources.md) for the replay contract on the Batch side.

## Packaging and admission

`S3DeploymentPackager.pack(shadedJar, outputJar)` at
`io/rowweave/connector/s3/S3DeploymentPackager.java:40` builds the image a Worker
admits. It requires exactly two arguments through `main` at
`io/rowweave/connector/s3/S3DeploymentPackager.java:34-35`, rejects an entry larger
than `MAX_ENTRY_BYTES` at `:28`, and rejects the finished image when it exceeds
`MAX_IMAGE_BYTES` at `io/rowweave/connector/s3/S3DeploymentPackager.java:29` (enforced at `:61`).
It stamps the provider service binding at `SOURCE_SERVICE_RESOURCE` (`:23`) and the
contract bytes at `SOURCE_CONTRACT_RESOURCE` (`:25`), which is how the Worker
discovers the provider with `ServiceLoader` and verifies its digest without loading
job code first. Deployment signing and the signer property family are in
[security](../operations/security.md).

## Qualification

`S3ConnectorModelTest` — configuration ranges, canonical round trip, endpoint and
name rules. `S3ConnectorProviderConformanceTest` — the provider against the
connector conformance harness
(`StreamConnectorConformance`). `S3CredentialCodecTest` — credential framing.
`S3DeploymentPackagerTest` — image bounds and resources. `S3ManifestPlannerTest` —
listing and manifest limits. `S3SnapshotReaderTest` and
`S3SnapshotReaderLifecycleTest` — decoding, row bounds and lifecycle.

Related: [formats, limits and SPI](formats-limits-spi.md),
[format constants](format-constants.md),
[PostgreSQL CDC connector](connector-postgres-cdc.md),
[resident CDC contract](stream-cdc.md),
[connector runbook](../../runbooks/stream-runtime.md),
[errors and refusals](errors-and-refusals.md).
