---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Format self-descriptors

RowWeave writes a short tag into almost every envelope it persists or puts on the
wire: a checkpoint frame, a spill-file marker, a signed deployment manifest, a
plan codec envelope, a handshake preamble. The tag exists so a reader can refuse
bytes that were not produced by the writer that understands them, instead of
decoding them as if they were valid.

These tags are part of the format. They are opaque identifiers with no product
meaning, and they are **not renamed** while the format they name stays readable.
Renaming one changes every byte sequence it prefixes: artifacts already on disk
or replaying from a checkpoint become unreadable, and the pinned wire fixtures
that lock those sequences are wrong. A new tag therefore means a new format
version plus a reader that still accepts the old one, which is a format
revision, not a cleanup.

`tools/oss_identity_residue_gate.sh` fails the build on any identity-shaped
string outside the families below, so this page is the authority for what the
whitelist covers.

## Fixed-width magic families

| Shape | Where it lives | Examples |
|---|---|---|
| 8-byte ASCII magic `"RWXX??????"` | envelope preamble, written then checked on decode | `RWXXVSC2` stage command schema 2 [`StageCommandCodecV2.java:44`](../../../rowweave-core/src/main/java/io/rowweave/cluster/StageCommandCodecV2.java); `RWXXRPCS1` secure RPC handshake [`RpcCodec.java:24`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcCodec.java); `RWXXVDH1` versioned dataset hand-off [`VersionedDatasetHandoffCodec.java:20`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffCodec.java); `RWXXDEP1` deployment manifest [`DeploymentManifest.java:13`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentManifest.java) |
| 8-byte ASCII magic `"RWX??????"` | connector-persisted positions and credentials | `RWXS3CR1` S3 credential frame [`S3CredentialCodec.java:14`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3CredentialCodec.java); `RWXPGCR1` PostgreSQL credential frame [`PostgresCredentialCodec.java:14`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCredentialCodec.java) |
| `int` word `0x5257xxxx` (ASCII `RW..`) | four-byte preamble inside `ByteBuffer` frames | `RWRS` run state [`RunStateCodec.java:16`](../../../rowweave-core/src/main/java/io/rowweave/run/RunStateCodec.java); `RWPS` stream source position [`StreamSourcePositionCheckpoint.java:22`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourcePositionCheckpoint.java); `RWSD` batch split descriptor [`BatchSourceSplitDescriptorCodec.java:24`](../../../rowweave-core/src/main/java/io/rowweave/BatchSourceSplitDescriptorCodec.java) |
| domain-separation string in a signature digest | prefixed before hashing, so a signature over one record type cannot be replayed as another | `RWXX-RELEASE-CONTRACT-V1\0` [`ReleaseContractSignature.java:16`](../../../rowweave-core/src/main/java/io/rowweave/release/ReleaseContractSignature.java); `RWXX-DEPLOYMENT-V1\0` [`DeploymentManifest.java:14`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentManifest.java) |

Roughly 180 declarations sit in more than 130 files under `src/main` across
`rowweave-core`, `rowweave-pipeline`, `rowweave-registry-zk`,
`rowweave-connector-s3` and `rowweave-connector-postgres-cdc`; 27 test classes
pin their encoded bytes as golden hex or Base64.

Changing a tag that names a persisted envelope is therefore not free even before
a release: a spill workspace or credential file written under the previous tag is
refused rather than adopted, and the leftover directory has to be removed by hand
because the scavenger will not delete what it cannot authenticate. Treat every
"unreadable envelope" failure as a question about which tag wrote the file.

Some of those goldens are deliberately immutable. The schema-1 frames in
[`StageCommandJoinStrategyWireTest.java:40`](../../../rowweave-core/src/test/java/io/rowweave/cluster/StageCommandJoinStrategyWireTest.java)
were produced once by the accepted writer and are never regenerated: they are
the proof that a current build still reads what an older build wrote. Treat a
failing golden as a real format break, not as a test to update.

## Variable-length text descriptors

Two descriptors are longer ASCII strings that appear in canonical LF or
pipe-delimited records rather than in a fixed-width frame, so they can be read
by a person inspecting an artifact. Each one has exactly one accepted spelling,
compared with `String.equals`; the spill marker keeps it in one constant, the row
format descriptor repeats it inline at its producers and readers:

| Descriptor | Written by | Read by |
|---|---|---|
| `ROWWEAVE_ROW_TEXT_V1` | the canonical row body in [`FileSink.java:256`](../../../rowweave-core/src/main/java/io/rowweave/FileSink.java), and as one input to the object-storage split identity [`ObjectStorageSnapshotSource.java:130`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageSnapshotSource.java) | strict object decoding, and the rejected-row detail column, validated against the closed reason set in [`QualityReasonKey.java:54`](../../../rowweave-core/src/main/java/io/rowweave/QualityReasonKey.java) |
| `ROWWEAVE_SPILL_MARKER_V1` | spill-file ownership marker [`SpillWorkspaceMarkerCodec.java:12`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillWorkspaceMarkerCodec.java) | same codec, on decode, after its SHA-256 checksum |

If you add a descriptor, add it to the closed validation set that reads it. A
detail token that is not in [`QualityReasonKey`](../../../rowweave-core/src/main/java/io/rowweave/QualityReasonKey.java)
cannot be produced — it is rejected from the constructor, not filtered later — so
a rejected row can never carry an arbitrary string into a customer table. Because
the row format descriptor is repeated inline rather than held in one constant, a
misspelling there fails at construction instead of silently diverging.

## File-name conventions

Signed deployment manifests are written by operators as `*.rwdep` and named by
the `rowweave.deployment.manifest.<N>` property (see
[security](../operations/security.md)). The extension is only a convention: the
loader accepts any path and trusts the decoded `RWXXDEP1` first line, its line
count, and the detached signature, not the file name.
