---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Changelog

All notable changes to RowWeave are documented here. The format follows
Keep a Changelog, and the project uses Semantic Versioning.

## 0.2.1

### Dependencies

* ASM 9.7 → 9.11.1 and Jackson 2.22.3 → 2.23.0.
* Maven surefire, failsafe, jar and shade plugin upgrades.
* GitHub Actions bumps: actions/checkout v4 → v7 and actions/setup-java v4 → v6.

### Tooling

* The identity and content-boundary gates now fail loudly when ripgrep is
  missing, and the release-contract baseline was refreshed for the renamed
  cluster client.

## 0.2.0 (initial public preview)

This is the first public preview. The artifact is not yet published to a Maven
repository, and the API may change before 1.0.0.

### Engine

* Pure push batch pipeline with bounded channels for back pressure; no
  intermediate materialisation of row collections between operators.
* Batch operators: map, filter, flatMap, project, group by and aggregate,
  sort, distinct, top-N, limit, join (broadcast hash join, sort-merge join and
  auto selection), co-group, union, and deduplication.
* Stream runtime over a bounded resource envelope, with keyed state,
  watermarks, windows, temporal join, and checkpoint/restore.
* Managed memory partitioning with per-operator budgets and disk spill for
  sort and aggregation.
* Cluster runtime with job client, master and worker roles; local mode reuses
  the same architecture inside one JVM.
* Expression engine with a compiled bytecode path and an interpreter fallback.

### Connectors

* JDBC snapshot source.
* PostgreSQL CDC source.
* S3 and generic object storage snapshot source, with manifest-driven
  exact-snapshot listing.
* Custom source and sink SPI with service-loader discovery.

### Composition

* Multi-stage Pipeline over the portable artifacts produced by the public
  Batch and Stream compilers (`PORTABLE_BATCH_PLAN_V1` for a Batch step,
  `PORTABLE_STREAM_JOB_V1` for a Stream step): definition, canonical
  compilation, inspection and explanation, with the versioned dataset
  hand-off authority deciding one durable winner per materialized boundary.
* Accepted shapes include chained boundaries across three steps, a stream
  producer feeding a batch consumer, disconnected roots with one output each,
  one producer fanned out to three consumers, and materialized fan-in, where a
  step declares one `SOURCE` operator per input and joins them with a `BINARY`
  operator.
* Every refusal code a public entry point can return is asserted by name — 71
  of the 73 the module can return, the other two being internal defences
  (§3.2 of that page) — and the contract is documented behaviour by behaviour
  in `docs/product/reference/pipeline-boundary.md`.
* Not certified in this release: no Pipeline case runs in the multi-process
  cluster suite, so Pipeline behaviour rests on the module's own tests, and
  the module defines no cancellation surface.

### Operations

* HTTP monitoring endpoint with pipeline, operator, buffer and memory views.
* Rolling upgrade and store migration runbooks.
* Java 8 bytecode target; build and CI on JDK 17, Linux and macOS.

### Release engineering

* Reproducible artifacts: fixed entry timestamps make two builds byte-identical,
  and every published jar plus the CycloneDX SBOM (produced under `-Prelease`)
  carries a `.SHA-256` sidecar.
* CI release job: clean-checkout packaging gate (`tools/oss_release_gate.sh`)
  with explicit JDK 8 consumer home, then `mvn -Prelease verify` including the
  published-jar consumer ITs.
* Opt-in connector validation includes a real MinIO conformance/cluster gate
  and a local PostgreSQL 14 service/cluster gate.
* Documentation anchor gate: every `file:line` claim on the pipeline boundary
  reference must cite a non-blank source line carrying the claimed identifier.
