---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Installation and modules

RowWeave `0.2.0` is a source preview. Its Maven coordinates define the local
build identity, but no artifact is published to Maven Central. Build and install
the reactor into your local Maven repository, then declare the module in your
application with version `0.2.0`.

## Requirements

| Use | Requirement |
|---|---|
| Build and test suite | JDK 17 and Maven 3.6 or later |
| Application bytecode | Java 8 |
| Embedded Batch application host | Java 8 or later |
| Encrypted cross-host data plane | A certified JDK 17 build |
| Local Maven repository | Writable by the build user |

A Java 8 bytecode target proves that public JARs can be consumed by a Java 8
compiler and JVM. It does not certify every Java 8 distribution, operating
system, or cross-host network environment. See
[data-plane limits](../reference/data-plane-limits.md) for the certification
boundary.

## Build from source

Run the complete suite before relying on a candidate:

```bash
mvn -q test
```

For a faster compile and package while iterating on documentation or one
targeted area:

```bash
mvn -q -DskipTests package
```

To make the preview modules resolvable from another local project:

```bash
mvn -B -DskipTests -Djacoco.skip=true install
```

The command installs the parent POM and all module JARs to your local Maven
repository. Run the test suite first; `install` in the command above does not
replace qualification.

## Select a dependency

Start with core only:

```xml
<dependency>
  <groupId>io.github.rowweave</groupId>
  <artifactId>rowweave-core</artifactId>
  <version>0.2.0</version>
</dependency>
```

Use the same RowWeave version for every RowWeave module in one application. Do
not mix preview versions.

## Module selection

| Module | Add when | Direct runtime dependencies | Selection notes |
|---|---|---|---|
| `rowweave-core` | You embed Batch or Stream execution. | ASM, Netty transport/codec/handler, SLF4J API | This is the required library. The host application selects one compatible logging provider. |
| `rowweave-pipeline` | You use the explicit multi-stage, versioned hand-off boundary. | `rowweave-core` | Add it only for pipeline composition; ordinary Batch and Stream graphs do not require it. |
| `rowweave-connector-s3` | S3-compatible object storage snapshot source provider; you read a manifest-defined snapshot. | `rowweave-core` (provided), AWS SDK v2 S3, URL-connection client, Jackson core | The connector is a read-only source. Add core explicitly in your application because the connector declares core as provided. |
| `rowweave-connector-postgres-cdc` | You use PostgreSQL logical replication as a bounded CDC source. | `rowweave-core` (provided), PostgreSQL JDBC driver | Add core explicitly. Review replication privileges, slot lifetime, and WAL retention with your database operator before deployment. |
| `rowweave-registry-zk` | You need ZooKeeper service registration and Master election. | `rowweave-core`, Curator framework/recipes, SLF4J API | Discovered through `ServiceLoader` when present. Use one registry implementation on a classpath. |
| `rowweave-monitor` | You need the optional HTTP dashboard, read-only API, or Prometheus endpoint. | `rowweave-core`, Netty HTTP codec/handler, SLF4J API | Discovered through `ServiceLoader`; monitoring remains disabled unless explicitly enabled. See [monitoring](../operations/monitoring.md). |
| `rowweave-soak` | You run release qualification harnesses. | Core, registry, both connectors, Curator test support, SLF4J Simple | Do not add to an application. It is release engineering tooling. |
| `rowweave-examples` | You build or inspect runnable examples. | Core, pipeline, SLF4J Simple | Do not add to an application. Examples are compiled by CI to detect API drift. |

Every RowWeave module is optional except the module your application directly
uses; a Batch or Stream application always needs core. Connector modules declare
core as provided so that a deployment can keep one copy of core and select
connector JARs deliberately.

## Optional runtime discovery

Core discovers two optional integrations through `ServiceLoader`:

* `rowweave-monitor` supplies the HTTP monitor launcher.
* `rowweave-registry-zk` supplies the ZooKeeper registry provider.

Adding a JAR makes the provider available. It does not force the integration to
run: the monitor requires explicit enabling, and cluster configuration selects
registry use. Do not place multiple implementations of the same provider
interface on one classpath.

Connector provider selection follows the connector contract rather than the
two ServiceLoader integrations above. Keep provider configuration and deployment
payloads inside the supported contract checked by the connector conformance
boundary.

## Application packaging

For an embedded application, depend on the plain module JARs and let Maven bring
their compile/runtime dependencies. Do not shade RowWeave into your application
unless you own dependency relocation, signing, ServiceLoader metadata, and the
full qualification burden.

Connector modules also produce signed deployment images for the cluster provider
deployment path. Treat those images as deployment artifacts, not as a general
replacement for application dependency management.

## Java 8 consumer verification

After packaging, verify the public JAR boundary with the repository gates:

```bash
bash tools/verify-map-partition-jar-consumer.sh
bash tools/verify-stream-connector-jar-consumer.sh
```

Point `ROWWEAVE_JAVA8_HOME` (or its alias `JAVA8_HOME`) at a JDK 8 to compile and
run the consumer with a Java 8 toolchain:

```bash
ROWWEAVE_JAVA8_HOME=/path/to/jdk8 bash tools/verify-map-partition-jar-consumer.sh
ROWWEAVE_JAVA8_HOME=/path/to/jdk8 bash tools/verify-stream-connector-jar-consumer.sh
```

A consumer home is accepted only when it has both `bin/java` and `bin/javac` and
reports version 1.8, so a Java 8 runtime that cannot compile is rejected rather
than traded for the host JDK. The scripts compile an external consumer with Java 8
semantics, verify the class file version of both the published JAR and the
consumer, and run it against the built core JAR. Without a JDK 8 they fall back to
the host compiler in `--release 8` mode and say so in the PASS line; the class
file version check still holds, while the toolchain claim does not.

## Dependency conflicts

Run Maven's tree when versions are mediated unexpectedly:

```bash
mvn -B dependency:tree -Dscope=runtime
```

Apply these rules:

* **Netty.** Core and monitor are built against Netty 4.1.139.Final. Keep every
  Netty artifact aligned; a mismatched transport, codec, or handler version can
  fail at channel setup or corrupt a transport assumption.
* **SLF4J.** Core exposes SLF4J 2.0.20 without a logging provider or application
  logging configuration. Supply exactly one provider compatible with your host
  JDK. For Java 8, `org.slf4j:slf4j-simple:2.0.20` is an option. Logback 1.5.38
  is confined to tests running on the JDK 17 build host.
* **ASM.** Core uses ASM 9.7 for expression code generation. Avoid introducing
  an older ASM on the same classpath.
* **AWS SDK and Jackson.** Let `rowweave-connector-s3` manage AWS SDK v2 and
  Jackson core versions unless you can rerun connector qualification.
* **PostgreSQL.** Let `rowweave-connector-postgres-cdc` manage the JDBC driver
  version unless you can rerun connector qualification.
* **Curator and ZooKeeper.** The registry module uses Curator 5.9.0 and the
  ZooKeeper 3.9.7 client. Do not mix unrelated Curator generations. Server
  compatibility still requires the deployment-specific qualification matrix.
* **Connectors.** Their POMs mark core as provided. Add the core dependency in
  the application; do not rely on the connector to bring it transitively.

If dependency mediation changes a version, run the module suite and the relevant
connector or consumer qualification again. Version alignment is not a substitute
for behavioral tests.

## Release packaging

Normal application installation uses the commands above. Reproducible release
artifacts, checksums, SBOM generation, and signing checks use the separate
release gate described in [CONTRIBUTING](../../../CONTRIBUTING.md). Do not
publish or distribute a candidate without running that gate.
