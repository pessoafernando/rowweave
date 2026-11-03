---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# RowWeave documentation

RowWeave is an embedded compute engine for exact, deterministic data
processing. Use this home to reach the authoritative page for each task. The
[capability matrix](reference/capability-matrix.md) is the authority for what
the release supports. Roadmap items do not widen that support boundary.

RowWeave provides dual Runtime support: Batch for bounded graphs and bounded-resource Stream for stateful event-time graphs. Both runtimes use portable DAGs and can run in Local, InProcess, or cross-host M+NW clusters.

## Audience routes

| Audience | Start here | Purpose |
|---|---|---|
| Evaluator | [README](../../README.md) | Understand the execution model, correctness guarantees, maturity, requirements, and evaluation path. |
| Application developer | [Installation](quickstart/installation.md) | Install a module, run a first Batch or Stream job, choose a run mode, and use the supported API. |
| Connector or pipeline author | [External platform integration](guides/external-platform-integration.md) | Learn the versioned integration boundary, deployment artifacts, recovery model, and capability limits. |
| Operator | [Cluster deployment](operations/deployment.md) | Start supported processes, configure security and monitoring, observe jobs, diagnose failures, and recover. |
| Contributor or documentation maintainer | [Contributing](../../CONTRIBUTING.md) | Build, test, update behavior or documentation, review contracts, and prepare release work. |

## Authoritative page map

| Page type | Authoritative route |
|---|---|
| Product overview | [README](../../README.md) |
| Documentation home and navigation | [This page](index.md) |
| Installation and modules | [Installation](quickstart/installation.md) |
| Batch quickstart | [First job](quickstart/first-job.md) |
| Stream quickstart | [Stream first job](quickstart/stream-first-job.md) |
| Run modes | [Run modes](guides/run-modes.md) |
| Example index | [Examples](guides/examples.md) |
| Public API overview | [API overview](reference/api-overview.md) |
| Public API inventory | [API inventory](reference/api-inventory.md) |
| Supported capability and stable refusals | [Capability matrix](reference/capability-matrix.md) |
| Batch behavior contracts | [Batch behavior](reference/batch-behavior.md) |
| Batch environment and DataSet method reference | [Batch API reference](reference/batch-api.md) |
| Custom function interfaces | [Custom function interfaces](reference/custom-functions.md) |
| Expression syntax and evaluation | [Expression evaluation](reference/expression-evaluation.md) |
| Built-in Batch sources and replay | [Batch sources](reference/batch-sources.md) |
| Batch sinks and output boundaries | [Batch sinks](reference/batch-sinks.md) |
| Source executor and provider SPI | [Source executor SPI](reference/source-executor-spi.md) |
| Schema conversion and row validation | [Schema and validation](reference/schema-and-validation.md) |
| Bounded incremental Batch source | [Incremental Batch source](reference/incremental-batch-source.md) |
| Durable sink provider boundary | [Durable sink provider](reference/durable-sink-provider.md) |
| Decimal scale, rounding, and null rules | [Decimal and null semantics](reference/decimal-null-semantics.md) |
| Batch scenario oracle and qualification method | [Scenario oracle specification](reference/scenario-oracle-specification.md) |
| Stream behavior and lifecycle | [Stream behavior](reference/stream-behavior.md) |
| Stream session and DataStream method reference | [Stream API reference](reference/stream-api.md) |
| Resident change-data-capture source contract | [Resident CDC contract](reference/stream-cdc.md) |
| S3-compatible snapshot connector | [S3 snapshot connector](reference/connector-s3-snapshot.md) |
| PostgreSQL logical CDC connector | [PostgreSQL CDC connector](reference/connector-postgres-cdc.md) |
| ZooKeeper service registry and Master election | [ZooKeeper registry](reference/registry-zookeeper.md) |
| Configuration reference | [Configuration](reference/configuration.md) |
| Errors and refusals | [Errors](reference/errors-and-refusals.md) |
| Formats, limits, and SPI | [Formats and limits](reference/formats-limits-spi.md) |
| Execution and dataflow model | [Pure push model](architecture/pure-push-model.md) |
| Architecture routing | [Architecture reading map](architecture/index.md) |
| Cluster model | [Cluster topology and scheduling](architecture/cluster.md) |
| Recovery model | [Recovery and state authority](architecture/recovery.md) |
| Resource model | [Resource and backpressure](architecture/resources.md) |
| Cluster deployment | [Deployment](operations/deployment.md) |
| Monitoring and live DAG | [Monitoring](operations/monitoring.md) |
| Monitor HTTP contract | [Monitor contract](reference/monitor-contract.md) |
| Security model | [Security](operations/security.md) |
| Troubleshooting | [Troubleshooting](operations/troubleshooting.md) |
| Stream operations and recovery | [Stream runbook](../runbooks/stream-runtime.md) |
| Rolling upgrade procedure | [Rolling upgrade runbook](../runbooks/rolling-upgrade-v1.md) |
| State-store migration procedure | [Store migration runbook](../runbooks/store-migration.md) |
| External release compatibility | [Release compatibility](reference/versioned-release-v1.md) |
| Data-plane behavior and certification limits | [Data-plane limits](reference/data-plane-limits.md) |
| External formats and frozen constants | [Format constants](reference/format-constants.md) |
| Versioned pipeline boundary | [Pipeline boundary](reference/pipeline-boundary.md) |
| Release notes | [Changelog](../../CHANGELOG.md) |
| Open work and non-promises | [Roadmap](../../ROADMAP.md) |
| Support and issue intake | [Support policy](../../SUPPORT.md) |
| Vulnerability reporting | [Security policy](../../SECURITY.md) |
| Contribution workflow | [Contributing](../../CONTRIBUTING.md) |
| License and notices | [License](../../LICENSE) and [third-party notices](../../THIRD-PARTY-NOTICES.md) |
| Editorial and code-sample standard | [Documentation style](reference/documentation-style.md) |
| Documentation maintenance and ownership | [Documentation maintenance](reference/documentation-maintenance.md) |
| Release and documentation visibility | [Release visibility](reference/release-visibility.md) |
| Documentation inventory and known gaps | [Documentation audit](reference/documentation-audit.md) |

## Translation policy

The authoritative documentation is English. No translated mirror is shipped in
`0.2.0`. [Translation synchronization](reference/translation-synchronization.md)
defines the locale tree, manifest, content-revision binding, review boundary,
and stale-page gate used when a locale is added.
