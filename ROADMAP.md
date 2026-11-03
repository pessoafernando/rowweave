---
doc-contract: preview-0.2.0
doc-channel: default-branch
doc-status: development-planning
---
# Roadmap

This page lists work that is **not finished** in `0.2.0`. Supported behaviour is
described only in the [capability matrix](docs/product/reference/capability-matrix.md);
nothing below is a promise of support, and an item here does not widen what the
matrix certifies.

The public 0.2.0 surface is complete for the Batch and Stream contracts listed in the capability matrix. Open work is in the surrounding engineering: data-plane scale, operating-system coverage, packaging and release mechanics.

## Before 1.0.0

**Shuffle data plane.** Refcounted connection reuse between worker pairs,
per-target wire credit, parallel task assignment behind a claim fence, and the
bounded thread and connection arithmetic are implemented and covered by the
cluster suite; the two-worker multi-process gate enforces at least four logical
streams on one physical connection, and its reference run reports eight streams
carried by two connections job-wide. What is open is validation beyond one
host: connection behaviour across real networks, with loss and asymmetric
bandwidth, has not been measured. Each
behaviour, its source location and its test are listed in
[data plane limits](docs/product/reference/data-plane-limits.md).

**Cross-operating-system socket behaviour.** The data plane depends on
differences that only appear at the socket boundary: `close()` semantics
(`RST` versus `FIN`) and half-close through `shutdownOutput()`, both specified
and tested on the paths in use; `InputStream.available()` appears only in frame
codecs reading a fully materialised byte array, so it carries no operating-system
risk. Continuous integration runs the full unit and multi-process suites on both
Linux and macOS as a guardrail, but there is no versioned behaviour matrix per
operating system yet.

**Pipeline coverage at cluster scale.** Multi-stage composition, the accepted
shapes (including a materialized fan-in, where one step consumes two datasets)
and every refusal code the module can return through a public entry point are
asserted by name, and the hand-off authority is covered for a publish that dies
before its atomic rename. What is not covered is cluster scale: no Pipeline case
runs in the multi-process suite, so pipeline behaviour rests on the module's own
tests rather than on two-worker qualification. Each behaviour, its source
location and its test are listed in
[pipeline composition boundary](docs/product/reference/pipeline-boundary.md).

**Documentation site and language parity.** Canonical Markdown documentation is English. A static documentation site and reviewed translation mirrors remain open.

**Repository identity and publication metadata.** The parent POM already
carries the Central metadata (`url`, `scm`, `developers`,
`distributionManagement`), attaches sources and javadoc jars, and defines the
repository URL once as the `rowweave.repo.url` property that the identity and
URL gates check the templates against. Before publication, the repository owner confirms the canonical URL and release identity; publication itself remains a separate authorized action.

**Release engineering.** Reproducible release artifacts exist: fixed
timestamps make two builds byte-identical, every jar carries a SHA-256 sidecar,
the CycloneDX SBOM is produced under `-Prelease`, and a CI release job runs the
packaging gate (`tools/oss_release_gate.sh`) plus `mvn -Prelease verify` with a
real JDK 8 consumer home. Two external Java 8 consumer gates hold the bytecode
boundary. The opt-in connector conformance suite has also been executed against a real
MinIO server and a local PostgreSQL 14 cluster. Still open: publishing to
Maven Central, promoting that service matrix to a CI service runner, and the
long-running stability evidence required for a production release — all against
one final clean candidate rather than continuously during feature work.

## Deferred API boundaries

The current public contract, explicitly deferred controls and stable refusals
are in the [capability matrix](docs/product/reference/capability-matrix.md).
These pointers are not a promise of support.

## Contributing

Pick up an item by opening an issue first; see
[CONTRIBUTING.md](CONTRIBUTING.md) for the design-note convention that applies
to changes in the execution, resource or recovery model.
