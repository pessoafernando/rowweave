---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# PostgreSQL logical change-data-capture connector

`rowweave-connector-postgres-cdc` is a resident change-data-capture source over
one PostgreSQL database: it reads an atomic snapshot, hands off at the recorded
consistent point, then continues from the logical decoding output of a replication
slot. Unlike the [S3 snapshot connector](connector-s3-snapshot.md), this connector
never terminates and does require the checkpoint acknowledgement, because the
server retains write-ahead log until the engine says a position is durable. The
phase machine and reader vocabulary are the ones in
[resident CDC contract](stream-cdc.md); this page is the connector's own
configuration, binding, position, packaging and failure surface.

## Roles

| Type | Responsibility | Declared |
|---|---|---|
| `PostgresConnectorProvider` | the signed resident CDC provider | `io/rowweave/connector/postgres/PostgresConnectorProvider.java:10` |
| `PostgresConnectorContract` | the static contract shipped with it | `io/rowweave/connector/postgres/PostgresConnectorContract.java:8` |
| `PostgresConnectorConfiguration` | the inert, canonical configuration | `io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:24` |
| `PostgresTableBinding` | one relation plus its column identity | `io/rowweave/connector/postgres/PostgresTableBinding.java:10` |
| `PostgresCdcPosition` | one external recovery or acknowledgement point | `io/rowweave/connector/postgres/PostgresCdcPosition.java:6` |
| `PostgresCdcReader` | the snapshot-to-log reader for one split | `io/rowweave/connector/postgres/PostgresCdcReader.java:16` |
| `PostgresTypeMapping` | catalog object id to engine type | `io/rowweave/connector/postgres/PostgresTypeMapping.java:15` |
| `PostgresDeploymentPackager` | build the deployable connector image | `io/rowweave/connector/postgres/PostgresDeploymentPackager.java:22` |

`PostgresConnectorProvider` implements `StreamCdcSourceProvider` directly
(`io/rowweave/connector/postgres/PostgresConnectorProvider.java:11`), which is the
resident form of the provider contract rather than the ordinary bounded one.
`providerKey()` at `io/rowweave/connector/postgres/PostgresConnectorProvider.java:26`
and `contractDigest()` at `:30` read from the static contract, so the key a
`StreamSourceSpec` must name and the digest a Worker verifies are the same two
values in every process.

## Declared capabilities

[`PostgresConnectorContract`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorContract.java)
builds its single static value at
`io/rowweave/connector/postgres/PostgresConnectorContract.java:14-20`:

| Field | Value | Site |
|---|---|---|
| Deployment id | `rowweave-connector-postgres-cdc` | `io/rowweave/connector/postgres/PostgresConnectorContract.java:9` |
| Provider key | `rowweave.postgres.logical-cdc` | `io/rowweave/connector/postgres/PostgresConnectorContract.java:11` |
| Configuration ceiling | `MAX_CONFIGURATION_BYTES` | `io/rowweave/connector/postgres/PostgresConnectorContract.java:18` |
| Concurrent opens | 16 | `io/rowweave/connector/postgres/PostgresConnectorContract.java:19` |
| Open deadline | 120000 ms | `io/rowweave/connector/postgres/PostgresConnectorContract.java:19` |
| `boundedEndOfInput` | `false` | `io/rowweave/connector/postgres/PostgresConnectorContract.java:20` |
| `dynamicDiscovery` | `false` | `io/rowweave/connector/postgres/PostgresConnectorContract.java:20` |
| `residentCdc` | `true` | `io/rowweave/connector/postgres/PostgresConnectorContract.java:20` |
| `externalCheckpointAcknowledgement` | `true` | `io/rowweave/connector/postgres/PostgresConnectorContract.java:20` |

The configuration ceiling is the connector's own constant, 512 KiB, declared at
`io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:29`, and it is
below the SPI ceiling in
`io/rowweave/stream/contract/StreamConnectorProviderContract.java:9`: a binding set
that cannot be encoded into the connector limit cannot be declared admissible under
the broader SPI limit. `contract()` at
`io/rowweave/connector/postgres/PostgresConnectorContract.java:26`,
`contractDigest()` at `:30` and `encodedContract()` at `:32` expose that value.

## Configuration surface

[`PostgresConnectorConfiguration`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorConfiguration.java)
is immutable and carries no connection. Its accessors are the surface: `host()` at
`io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:356`, `port()`
at `:357`, `database()` at `:358`, `slot()` at `:359`, `publication()` at `:360`,
`databaseSystemIdentifier()` at `:361`, `databaseOid()` at `:364`, `tlsMode()` at
`:365`, `credentialsReference()` at `:366`, `hasTlsTrustReference()` at `:367`,
`tables()` at `:376`, `maximumTransactionChanges()` at `:377`,
`maximumTransactionBytes()` at `:380`, `maximumRetainedWalBytes()` at `:381`,
`snapshotFetchRows()` at `:384`, `connectTimeoutMillis()` at `:385`,
`socketTimeoutMillis()` at `:386`, `statusIntervalMillis()` at `:387`,
`streamSchema()` at `:388`, `stableKeySchema()` at `:389` and
`stableKeyFieldIds()` at `:390`.

| Ceiling | Constant site | Value |
|---|---|---|
| Bound tables | `io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:27` | 256 |
| Total bound columns | `io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:28` | 4096 |
| Columns per table | `io/rowweave/connector/postgres/PostgresTableBinding.java:11` | 1024 |
| Encoded configuration | `io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:29` | 512 KiB |
| Changes in one transaction | `io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:30` | 1000000 |
| Bytes in one transaction | `io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:31` | 1 GiB |
| Retained write-ahead log | `io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:32` | 1 TiB |
| Snapshot fetch rows | `io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:103` | 100000 maximum |

The constructor checks every one of these before the value exists
(`io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:93-109`) and
throws the same typed invalidity otherwise at `:110`, whose code is
`POSTGRES_CDC_CONFIGURATION_INVALID` declared at `:35`. Three rules in that block
are not mere range checks and are worth reading twice:

* the retained write-ahead budget must be at least the transaction budget
  (`io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:101`),
  because a transaction that cannot fit inside what the server is asked to retain
  could never be acknowledged;
* a TLS trust reference may only be present for a mode that verifies
  (`io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:94-96`), so a
  pinned trust material cannot be configured onto a connection that would ignore it;
* the constructor encodes the value it is building
  (`io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:114`), for the
  reason stated at `:112-113`: a public value must never be constructible if it
  cannot cross the signed deployment boundary it represents.

[`PostgresTlsMode`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresTlsMode.java)
is closed at `io/rowweave/connector/postgres/PostgresTlsMode.java:5-8`: `DISABLED`,
`REQUIRE`, `VERIFY_CA`, `VERIFY_FULL`.
[`PostgresReplicaIdentity`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresReplicaIdentity.java)
is closed at `io/rowweave/connector/postgres/PostgresReplicaIdentity.java:5-7`:
`DEFAULT`, `USING_INDEX`, `FULL`. A table without a stable identity is refused at
binding time rather than producing an upsert that cannot be keyed.

Secrets are not configuration fields. `CREDENTIALS_ALIAS` at
`io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:25` and
`TLS_TRUST_ALIAS` at `:26` name the entries the Worker looks up in its own secret
store; `PostgresCredentialCodec` bounds the material it accepts, with
`MAX_PASSWORD_BYTES` at
`io/rowweave/connector/postgres/PostgresCredentialCodec.java:10` and the code
`POSTGRES_CDC_CREDENTIAL_INVALID` at `:12`, and
[`PostgresCredentials`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCredentials.java)
at `io/rowweave/connector/postgres/PostgresCredentials.java:6` is destroyable rather
than merely unreferenced.

`builder()` at `io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:117`
returns the [`Builder`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorConfiguration.java)
declared at `:469`, whose port default is visible at `:471`. `encode()` at `:119`
is the canonical form; the opaque magic family it starts with is catalogued in
[format constants](format-constants.md) rather than explained here.

## Snapshot identity and the source declaration

`toSourceSpec()` at
`io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:395` is the only
public bridge from configuration to a `StreamSourceSpec`, and it builds a resident
version-2 source (`io/rowweave/stream/api/StreamSourceSpec.java:193`) over one atomic
snapshot split. `snapshotIdentity()` at
`io/rowweave/connector/postgres/PostgresConnectorConfiguration.java:404` is a digest
of the encoded configuration, and it becomes the `snapshotId` of the spec. That is
what ties a run to a binding: `openCdc` recomputes the identity from the frame the
Worker delivered and refuses the assignment when it does not equal the context
(`io/rowweave/connector/postgres/PostgresConnectorProvider.java:52-55`). A plan that
describes a different table set than the provider image actually contains cannot
execute.

`openCdc` at `io/rowweave/connector/postgres/PostgresConnectorProvider.java:34` also
insists on the exact context shape a resident source has, at `:36-47`: a cut sequence
of zero, the single expected split, no external position while still in the snapshot
phase, and a completed snapshot split once in the log phase. Any violation is
`POSTGRES_CDC_SOURCE_CONTEXT_INVALID` at `:47`. The same discipline exists one layer
down in the engine at `io/rowweave/stream/spi/StreamCdcSourceContext.java:49-63`,
which rejects a log sequence below the cut and an external position carried during the
snapshot, failing as `invalid resident CDC source context` at `:62`.

## Position and acknowledgement

[`PostgresCdcPosition`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCdcPosition.java)
is the external point the engine records. Its [`Boundary`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCdcPosition.java)
enum at `io/rowweave/connector/postgres/PostgresCdcPosition.java:7` is closed to
`SNAPSHOT_HANDOFF` and `TRANSACTION_COMMIT`. The two factories are
`snapshotHandoff(...)` at `io/rowweave/connector/postgres/PostgresCdcPosition.java:35`
and `transactionCommit(...)` at `:43`.

A position couples the boundary kind to the transaction id exactly: the constructor
requires a non-zero id for a commit boundary and a zero id otherwise
(`io/rowweave/connector/postgres/PostgresCdcPosition.java:27-30`), so a
`SNAPSHOT_HANDOFF` position cannot smuggle a transaction. Reading the id back on such
a position is an explicit failure rather than a zero
(`io/rowweave/connector/postgres/PostgresCdcPosition.java:71-75`). A log position is
parsed and reformatted under canonical upper-case hex rules
(`io/rowweave/connector/postgres/PostgresCdcPosition.java:81` and `:111`), so the text
form a checkpoint stores and the text form an operator reads are the same characters.
Violations report `POSTGRES_CDC_POSITION_INVALID` at
`io/rowweave/connector/postgres/PostgresCdcPosition.java:9`.

Because the connector declares `externalCheckpointAcknowledgement()` true, the reader
is told when a checkpoint commit happened, which is the only point at which retained
log may be released — the ordering is stated in
[resident CDC contract](stream-cdc.md).

## Reading

`PostgresCdcReader` at `io/rowweave/connector/postgres/PostgresCdcReader.java:16`
implements the resident reader interface. In the snapshot phase it opens the atomic
snapshot immediately and requires the server to hand back a consistent point before it
emits anything (`io/rowweave/connector/postgres/PostgresCdcReader.java:50-57`); a
snapshot that returns no hand-off position fails as `POSTGRES_CDC_SNAPSHOT_FAILED` at
`:54` rather than degrading into a plain table scan. In the log phase it restores the
external position supplied by the durable state
(`io/rowweave/connector/postgres/PostgresCdcReader.java:59`).

Row values are decoded by `PostgresPgOutputDecoder` against the bound column set and
mapped through `PostgresTypeMapping`, whose unsupported-type and invalid-value codes
are at `io/rowweave/connector/postgres/PostgresTypeMapping.java:16-17`. Emitted rows
are encoded as upserts against the stable-key and full schemas
(`io/rowweave/connector/postgres/PostgresCdcReader.java:45-47`), which is the changelog
mode the source declaration carries. Lifecycle — cancel, wake-up, close, and close
after cancel — is qualified by `PostgresCdcReaderLifecycleTest`.

Catalog drift is detected rather than absorbed: a
[`PostgresTableBinding`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresTableBinding.java)
records the relation oid, the replica identity, and every column attribute number,
type oid, type modifier and nullability
(`io/rowweave/connector/postgres/PostgresTableBinding.java:14-19`), and the binding
code `POSTGRES_CDC_BINDING_INVALID` at `:12` is what a mismatch produces.

## Packaging and admission

`PostgresDeploymentPackager.pack(shadedJar, outputJar)` at
`io/rowweave/connector/postgres/PostgresDeploymentPackager.java:41` builds the image a
Worker admits, requiring exactly two arguments through `main` at
`io/rowweave/connector/postgres/PostgresDeploymentPackager.java:35-36`, rejecting an
entry above `MAX_ENTRY_BYTES` at `:29` and an image above `MAX_IMAGE_BYTES` at `:30`.
It stamps the provider service binding at `SOURCE_SERVICE_RESOURCE`
(`io/rowweave/connector/postgres/PostgresDeploymentPackager.java:23`) and the contract
bytes at `SOURCE_CONTRACT_RESOURCE` (`:25`), so discovery and digest verification
happen before job code is loaded. Deployment signing and the signer property family
are in [security](../operations/security.md).

## Qualification

`PostgresConnectorModelTest` — configuration ceilings and the constructibility
self-check. `PostgresCdcPositionCodecTest` — canonical position round trip and the
boundary coupling. `PostgresCdcReaderLifecycleTest` — phase entry, cancel, wake-up and
close. `PostgresCredentialCodecTest` — credential framing and bounds.
`PostgresPgOutputDecoderTest` — decoded logical messages to typed rows.
`PostgresDeploymentPackagerTest` — image bounds and stamped resources.

Related: [resident CDC contract](stream-cdc.md),
[formats, limits and SPI](formats-limits-spi.md),
[format constants](format-constants.md),
[S3 snapshot connector](connector-s3-snapshot.md),
[Stream behavior](stream-behavior.md),
[connector runbook](../../runbooks/stream-runtime.md),
[errors and refusals](errors-and-refusals.md).
