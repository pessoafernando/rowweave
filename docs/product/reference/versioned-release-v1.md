---
doc-contract: rowweave-1.0.0
doc-channel: frozen-release
---
# RowWeave versioned production release v1

This page is the human-readable index for the immutable `rowweave-1.0.0` release packet governed by the public v1 release contract catalog. The machine authority is the signed manifest, not this page or a Maven version string.

## Exact identity

| Field | Frozen value |
|---|---|
| release ID | `rowweave-1.0.0` |
| protocol epoch / release revision | `1 / 0` |
| readable / writable revision | `0..0 / 0..0` |
| manifest digest | `c0280e0bcce6b2d4531275644b552116c488fc465d78ea61554153448dac9901` |
| public API baseline digest | `e80f27a4769a79071d3da55572d2ab610c8e784da9d4d000500db6158422421d` |
| signer public-key SHA-256 | `e1a9ee622953d720b516bb9a3d35da78822e5467b8effc8cd247865ff43a4e88` |
| decimal semantics | `rowweave-decimal-semantics-v1` |
| runtime capability | `rowweave.runtime.batch.v1` |

The detached signature is RSA-3072/SHA-256 over a domain-separated digest and frame length. Admission calls `ReleaseContractSignature.verify` before decoding or accepting the catalog. The offline private key used to produce this first immutable fixture is not stored in the repository.

## Release packet

The immutable machine artifacts live below `rowweave-core/src/test/resources`:

- `release/v1/release-manifest.hex` — canonical release manifest frame;
- `release/v1/release-manifest.sig.hex` — detached signature;
- `release/v1/release-public-key.der.hex` — X.509 DER signer key;
- `release/v1/public-api-v1.sig` — public API/SPI JVM descriptor floor;
- `compat/golden-corpus-v1.tsv` and `compat/v1_0` through `compat/v1_4` — frozen frames, semantic projections and independent compatibility runners.

The signature verifier, catalog constructor and API linkage test make the packet reproducible. A changed API baseline necessarily changes the manifest digest and invalidates the detached signature.

## Contract classification

`PUBLIC_FROZEN` includes the release manifest, portable Artifact/Plan IR, row schema, source split, durable sink and provider contracts, signed function and connector contracts, incremental definition/frontier/position/authority, publication, Run/attempt/event, control/data/stage protocols, store header and upgrade authority. These are cross-build contracts with stable numeric IDs, canonical codecs, bounded frames and explicit reader/writer revision ranges.

`INTERNAL_SAME_BUILD` contains `PlanGraph` and physical store layout. They may cross threads inside one build but may not cross a release, process deployment boundary, persisted Artifact or external platform integration boundary.

`LEGACY_EPHEMERAL` contains the Java-object plan payload and RPC enum ordinal. They remain for same-build source compatibility only and are rejected by durable public-v1 admission. No release packet may contain a `REQUIRED_ABSENT` component.

## Decimal semantics

The v1 result contract retains high precision during accumulation and performs exactly one final `BigDecimal.setScale(10, HALF_UP)`. A null grouping key forms its own group. Numeric null sorting defaults to zero unless explicit NULLS FIRST/LAST is selected; string null sorts first by default. Join keys obey `null != null`, so two null keys never match.

## Compatibility and rejection matrix

| Input or deployment | v1 outcome |
|---|---|
| public epoch 1 revision 0 packet | accept after signature, digest, limit and catalog checks |
| known optional extension from a later revision | preserve/ignore only when negotiated semantics and authority are unchanged |
| unknown required capability | reject before allocation, class loading, connector open or authority mutation |
| epoch mismatch or changed decimal semantics | reject; requires a new reviewed design decision and a stop-write migration |
| legacy Java object payload / RPC enum ordinal | same-build ephemeral only; reject from durable v1 |
| corrupt, truncated, oversized or checksum-drifted frame | deterministic fail-closed rejection |
| signer below RSA-3072 or invalid signature | `SIGNER_KEY_TOO_WEAK` / `SIGNATURE_INVALID` |
| produced N-only state with an N-1 rollback request | preserve active authority and reject false rollback |

The compatibility corpus proves v1.0 manifest bootstrap, v1.1 optional/required behavior, v1.2 portable plan and provider contracts, v1.3 migration/store authority and v1.4 negotiated control peers. Frozen runner source and frames are append-only.

## Version policy

A compatible optional addition increments the release revision. A new required meaning, field deletion, numeric ID change, decimal-semantics change or authority-order change increments the protocol epoch and requires a new reviewed design decision plus a stop-write migration. Deprecated public contracts remain readable for at least one complete N-1 window and may be removed only after proving that no persisted Artifact, state or deployment references them.

Operational rollout and rollback are defined in [rolling-upgrade-v1.md](../../runbooks/rolling-upgrade-v1.md). An external platform may use only the public surface shown in
[external-platform-integration.md](../guides/external-platform-integration.md).
