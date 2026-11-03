---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# RowWeave

**RowWeave** is a lightweight, embeddable data-processing engine for the JVM.
Write typed Batch or Stream pipelines in plain Java, run them inside an
existing application JVM, or submit the same explicit DAG to a
Master/Worker cluster. The engine focuses on exact decimal arithmetic,
deterministic semantics, bounded-memory execution, and explicit admission
limits.

## Why RowWeave

| Concern | RowWeave approach |
|---|---|
| **Embedding** | A Java library, not a platform to install. Local mode runs in one JVM; cluster mode uses separate Master and Worker JVMs. |
| **Dependency footprint** | Pure-Java core with a small direct runtime dependency set: ASM, Netty, and SLF4J. Embedded execution does not require Hadoop, YARN, Kubernetes, Scala, or an external coordinator. |
| **Exact decimal processing** | `BigDecimal` values accumulate at full precision and receive one defined output rounding. |
| **Deterministic pipelines** | Typed rows, explicit schemas, and stated null rules for grouping, ordering, and joining. |
| **Batch and Stream** | Separate Batch and Stream APIs with one portable graph model. Stream is a distinct lifecycle, not a timed batch loop. |
| **Predictable limits** | Unsupported or resource-exceeding plans are rejected with typed failures instead of silently degrading results. |
| **Operations** | No cluster setup is required for local mode. Optional HTTP dashboard, read-only API, and Prometheus endpoint are available through `rowweave-monitor`. |

RowWeave is a practical choice when Flink or Spark would bring more operational
weight than a problem needs: embedded Java ETL, exact decimal transformations,
repeatable backfills, deterministic integration tests, bounded sort/join/aggregate
workloads, or a Master/Worker deployment without a Hadoop or Kubernetes stack.

It is not a general SQL warehouse, a Python-first analytics stack, or a
drop-in replacement for every distributed-processing workload. Prefer Apache
Flink, Apache Spark, or a database engine when you need their broader connector
ecosystems, SQL optimizers, or unlimited unbounded-stream topologies.

## Core contracts

* **Pure-push execution.** Rows move from source through operators to sink.
  Intermediate results are not collected into a row list. Only a bounded
  breaker working set—such as sort or aggregation state—may materialize rows,
  and it spills according to its memory budget.
* **Deterministic decimal behavior.** Decimal values accumulate at full
  precision and receive one output rounding. Null rules for grouping, ordering,
  and joining are explicit. [Decimal and null
  semantics](docs/product/reference/decimal-null-semantics.md) states the scale,
  rounding, and null rules that every runtime follows.
* **Whole-job graphs.** Multiple sources, sinks, and shared subtrees are part
  of one submitted graph. A shared subtree executes once.
* **Admission instead of approximation.** Plans outside a supported shape or
  resource envelope are rejected with typed failures rather than silently
  degraded.
* **Separate runtimes.** Batch and Stream have distinct clients and lifecycles.
  Stream composes one portable DAG through one task lifecycle.

Read the [capability matrix](docs/product/reference/capability-matrix.md) for
the authoritative support boundary. The [roadmap](ROADMAP.md) is separate and
does not widen that boundary.

## Maturity

Version `0.2.0` is the **initial public preview**. The API can change before
`1.0.0`, and no Maven artifact has been published yet. Evaluate from source.
Support is evaluation-only during the source preview: see
[Support](SUPPORT.md) for issue intake and current-branch fixes.

The Java 8 bytecode and consumer-JAR boundary is tested. This is not the same
as certifying every Java 8 runtime or operating system. The test suite runs on
JDK 17, and the encrypted cross-host data plane currently admits only the
certified JDK 17 build matrix. The project makes no general throughput,
latency, or multi-host scale claim.

## Requirements

| Item | Value |
|---|---|
| Build JDK | 17 |
| Build tool | Maven 3.6 or later |
| Test-suite runtime | JDK 17 |
| Application bytecode | Java 8 |
| Embedded batch host runtime | Java 8 or later |
| Encrypted cross-host data plane | Certified JDK 17 builds only |

Two consumer gates compile and run external Java 8 clients against built JARs.
See
[verify-map-partition-jar-consumer.sh](tools/verify-map-partition-jar-consumer.sh)
and
[verify-stream-connector-jar-consumer.sh](tools/verify-stream-connector-jar-consumer.sh).
The [data-plane limits](docs/product/reference/data-plane-limits.md) page
separates implemented behavior from certified deployment environments.

## Evaluate from source

1. Check the [capability matrix](docs/product/reference/capability-matrix.md)
   against your operators, connectors, state, recovery, and deployment needs.
2. Review the requirements and certification limits above.
3. Build and test from source:

   ```bash
   mvn -q test
   ```

4. Follow [installation and modules](docs/product/quickstart/installation.md)
   to choose dependencies.
5. Run the [Batch first job](docs/product/quickstart/first-job.md), then use
   [run modes](docs/product/guides/run-modes.md) when you need in-process or
   cluster execution.

No internal design document is required to make this first evaluation decision.

## Modules

An application normally depends on `rowweave-core`. The other modules are
opt-in.

| Module | Purpose |
|---|---|
| `rowweave-core` | Public Batch and Stream APIs, push execution, runtimes, planning, and provider SPIs |
| `rowweave-pipeline` | Versioned multi-stage pipeline composition boundary |
| `rowweave-connector-s3` | Manifest-driven, read-only S3-compatible snapshot source |
| `rowweave-connector-postgres-cdc` | PostgreSQL logical-replication CDC source |
| `rowweave-registry-zk` | ZooKeeper registry and Master election |
| `rowweave-monitor` | Optional HTTP dashboard, read-only API, and Prometheus endpoint |
| `rowweave-soak` | Release qualification harness |
| `rowweave-examples` | Runnable examples compiled by CI |

Core does not require the registry or monitor. Optional runtime providers are
discovered through `ServiceLoader` only when their modules are present.

## API orientation

The following fragment shows the Batch API shape. See the first job for a
complete, tested evaluation path.

```java
ExecutionEnvironment environment = RowWeave.createEnvironment("evaluation");
DataSet source = environment.fromSource(new CollectionSource(schema, rows));
DataSet aggregate = source.groupBy("categoryKey").sum("quantity");
aggregate.sinkTo(new PrintSink(aggregate.getSchema()));
environment.execute();
```

The environment compiles every source and sink into one plan graph and submits
it through the configured job client. A sink receives rows during execution;
`execute()` does not return a row collection to the client.

## Stream

Stream is a separate public `StreamSession/DataStream` API with its own job
client and lifecycle. It supports a closed operator set, bounded keyed state,
timers, dynamic split, event-time windows, bounded temporal join, bounded async lookup, and
`APPEND`, `UPSERT`, and `RETRACT` changelog shapes. Local, in-process, and
cross-host M+NW modes use the same portable DAG lifecycle.

The closed operator set rejects unbounded global sort, join, aggregate, and
distinct with `UNKNOWN_OPCODE` rather than approximating them. Stream
deployment, checkpoint, savepoint, and rescale operations are covered by the
[capability matrix](docs/product/reference/capability-matrix.md) and the
[Stream runbook](docs/runbooks/stream-runtime.md). Start with the tested [Stream first job](docs/product/quickstart/stream-first-job.md).

## Documentation

Start at the [documentation home](docs/product/index.md).

* [Installation and modules](docs/product/quickstart/installation.md)
* [First job](docs/product/quickstart/first-job.md)
* [Stream first job](docs/product/quickstart/stream-first-job.md)
* [Run modes](docs/product/guides/run-modes.md)
* [Example index](docs/product/guides/examples.md)
* [API inventory](docs/product/reference/api-inventory.md)
* [Capability matrix](docs/product/reference/capability-matrix.md)
* [Batch behavior](docs/product/reference/batch-behavior.md)
* [Stream behavior](docs/product/reference/stream-behavior.md)
* [Data-plane limits](docs/product/reference/data-plane-limits.md)
* [External formats](docs/product/reference/format-constants.md)
* [Configuration reference](docs/product/reference/configuration.md)
* [Error and refusal reference](docs/product/reference/errors-and-refusals.md)
* [Formats, limits, and SPI reference](docs/product/reference/formats-limits-spi.md)
* [Architecture reading map](docs/product/architecture/index.md)
* [Pure push execution model](docs/product/architecture/pure-push-model.md)
* [Monitoring](docs/product/operations/monitoring.md)
* [Security model](docs/product/operations/security.md)
* [Troubleshooting](docs/product/operations/troubleshooting.md)
* [Roadmap](ROADMAP.md) — work that is open in `0.2.0`, not supported behavior
* [Support](SUPPORT.md)
* [Contributing](CONTRIBUTING.md)

## Build

```bash
mvn -q test
mvn -q compile
```

CI runs the default qualification suite and the multi-process cluster suite on
Linux and macOS with JDK 17.

## License

RowWeave is licensed under the [Apache License 2.0](LICENSE). Third-party
dependency terms are in
[THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md) and [NOTICE](NOTICE).
Security issues are handled privately as described in [SECURITY.md](SECURITY.md).
