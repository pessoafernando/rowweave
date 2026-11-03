---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Deployment

This page gives supported paths for running RowWeave from an application JVM,
from an isolated in-process cluster, and from separate Master and Worker
processes. Use the path that matches the trust and failure boundary you need to
test; a successful run in a smaller path does not qualify a larger one.

## Supported deployment paths

| Path | Lifecycle | Use for | Primary validation |
|---|---|---|---|
| Local embedding | In the application JVM; no required listener | Application embedding, evaluation, and functional checks | Typed result or sink completion |
| In-process cluster | Owned `RowWeave.startInProcessCluster` Master and Worker roles in one JVM | Isolated cluster lifecycle, admission, placement, and parallelism checks | `AutoCloseable` close plus result parity |
| Batch process cluster | Separate Client, Master, and Worker JVMs | Shared nothing execution and independent process recovery | `MASTER_PORT=`, `WORKER_READY=`, and sink validation |
| Stream process cluster | Master-only and Worker-only JVMs | Durable state, savepoints, rescale, and resident sources | `STREAM_MASTER_READY=` and `STREAM_WORKER_READY=` plus operation status |

Do not mix versions, deployment sets, or security identities inside one
admission boundary.

## Shared prerequisites

Before starting a process cluster:

1. Use the same RowWeave version and the same connector/provider set on every
   process that can execute the graph.
2. Reserve an absolute, empty or product-owned directory for temporary files.
   Batch Workers use `java.io.tmpdir` for role-local state and
   `rowweave.worker.spill.dir` for spill.
3. Reserve a durable directory for shared artifacts when jobs survive client
   disconnects or when multiple Masters can be elected.
4. Check that clocks, DNS names, firewall rules, and advertised hosts resolve
   consistently from every node.
5. Run each process under a dedicated non-root identity. Give that identity
   read access to validated artifacts and write access only to its state, spill,
   and log directories.

The Java bytecode targets Java 8, but cross-host encrypted data-plane admission
is limited to the certified JDK 17 matrix. Keep the application JVM matrix and
the process JVM matrix aligned with [data-plane limits](../reference/data-plane-limits.md).

## Local embedding

Local Batch execution uses `RowWeave.createEnvironment`:

```java
ExecutionEnvironment environment = RowWeave.createEnvironment("local-deploy");
DataSet source = environment.fromSource(provider);
DataSet aggregate = source.groupBy("key").sum("value");
aggregate.sinkTo(new FileSink(aggregate.getSchema(), outputPath));
environment.execute();
```

This is the same API shape exercised end to end by
[`BatchFirstJobExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/BatchFirstJobExample.java)
and [`BatchFirstJobExampleTest`](../../../rowweave-examples/src/test/java/io/rowweave/examples/BatchFirstJobExampleTest.java),
which run the graph in this repository and assert on the written file.
`provider` and `outputPath` are placeholders bound by that example.

Expected checks:

* `execute()` returns normally or throws the typed execution failure.
* A file sink is readable from the declared output path.
* Local mode does not require a firewall change or a port reservation.

Local Stream execution owns its durable state root and product roles:

```java
try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
        stateRoot, sourceProviders, sinkProviders, functionProviders)) {
    StreamSession session = RowWeave.createStreamSession(
            "local-stream", "local-stream", client);
    // Compose the graph, submit it, and await the returned operation status.
}
```

The same client and session construction, including the four provider
arguments, is exercised by
[`StreamLocalAppendExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamLocalAppendExample.java)
and [`StreamLocalAppendExampleTest`](../../../rowweave-examples/src/test/java/io/rowweave/examples/StreamLocalAppendExampleTest.java).
The comment marks the composition, submission, and status-await steps that the
page deliberately does not repeat.

Closing the client stops the embedded runtime. Checkpoint recovery may resume
only from a status that the API reports as completed.

For a tested local and in-process comparison, run
[`RunModesExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/RunModesExample.java)
with the command in the [example index](../guides/examples.md). The expected
output ends with `Equivalent result sets: true`.

## In-process cluster deployment

`InProcessCluster` gives a Batch application explicit Master and Worker
lifecycles in one JVM without network listeners:

```java
try (InProcessCluster cluster = RowWeave.startInProcessCluster(4, new File(stateDirectory))) {
    ExecutionEnvironment environment = new ExecutionEnvironment(
            "in-process-deploy", "in-process-deploy", cluster);
    environment.setParallelism(4);

    InProcessCluster.Diagnostics diagnostics = cluster.diagnostics();
    if (diagnostics.handlerThreadCount() == 0) {
        throw new IllegalStateException("in-process roles have no handler threads");
    }

    DataSet source = environment.fromSource(provider);
    DataSet aggregate = source.groupBy("key").sum("value");
    aggregate.sinkTo(new FileSink(aggregate.getSchema(), outputPath));
    environment.execute();
}
```

Follow these rules:

* The state directory must be dedicated to one cluster lifetime.
* The parallelism hint is a lower bound for placement capacity, not a CPU
  promise. The runtime expands it to a safe slot budget.
* `close()` is the shutdown operation. After close, do not submit to the same
  client or environment.
* `diagnostics()` reports role wiring, not a readiness gate. An in-process
  cluster has no network listener, so `controlListenerCount()` and
  `dataListenerCount()` are `0` while both roles are healthy; test the
  submission outcome and `handlerThreadCount()` instead.

The builder, injection, and `setParallelism` calls above match
[`RunModesExample.runInProcessCluster`](../../../rowweave-examples/src/main/java/io/rowweave/examples/RunModesExample.java),
and the socket-free role wiring is asserted by
[`DefaultBatchRuntimeContractTest`](../../../rowweave-core/src/test/java/io/rowweave/DefaultBatchRuntimeContractTest.java).

## Build and verify the process classpath

The process entry points are in `rowweave-core`; optional Monitor and
ZooKeeper support are separate modules. From a clean source checkout:

```bash
export ROWWEAVE_VERSION=0.2.0
mvn -q -pl rowweave-core,rowweave-monitor,rowweave-registry-zk -am \
  -DskipTests package

for module in rowweave-core rowweave-monitor rowweave-registry-zk; do
  mvn -q -pl "$module" dependency:build-classpath \
    -Dmdep.outputFile="$module/target/runtime-classpath.txt"
done

CORE_JAR="rowweave-core/target/rowweave-core-${ROWWEAVE_VERSION}.jar"
MONITOR_JAR="rowweave-monitor/target/rowweave-monitor-${ROWWEAVE_VERSION}.jar"
ZK_JAR="rowweave-registry-zk/target/rowweave-registry-zk-${ROWWEAVE_VERSION}.jar"
NODE_CP="$(cat rowweave-core/target/runtime-classpath.txt):${CORE_JAR}"
NODE_CP="${NODE_CP}:$(cat rowweave-monitor/target/runtime-classpath.txt):${MONITOR_JAR}"
NODE_CP="${NODE_CP}:$(cat rowweave-registry-zk/target/runtime-classpath.txt):${ZK_JAR}"
export NODE_CP
```

Add connector runtime JARs only when the graph uses those providers. Keep the
resulting classpath identical on every role that can load application code.

For a packaged release candidate, copy each binary JAR, `bom.json`, `bom.xml`,
and its `.SHA-256` sidecar into the approved artifact directory. Verify the
copied set before any process starts:

```bash
(
  cd "$ROWWEAVE_ARTIFACTS"
  find . -maxdepth 1 -type f \( -name '*.jar' -o -name 'bom.json' -o -name 'bom.xml' \) -print0 \
    | xargs -0 -n1 -I{} sh -ec 'sha256sum --check "{}.SHA-256"'
)
```

## Cluster prerequisites

Every `ClusterMain` and `StreamClusterMain` process must declare its complete
resource envelope before roles are constructed. A command with only role
properties fails closed. The envelope includes heap, direct memory, Metaspace,
code cache, stack, active processors, and a process/container memory limit.

Create dedicated directories and record versions before startup:

```bash
export ROWWEAVE_HOME=/var/lib/rowweave
export ROWWEAVE_TMP=/var/spool/rowweave
sudo install -d -m 0750 "$ROWWEAVE_HOME/artifacts" "$ROWWEAVE_HOME/logs" \
  "$ROWWEAVE_HOME/spill" "$ROWWEAVE_TMP/master" "$ROWWEAVE_TMP/worker"
```

`stdout` is the readiness channel; diagnostics normally go to stderr. Capture
both streams and do not parse log formatting for readiness.

Worker capture permits 64 ordinary source-control actions; materialization
permits 8 large source-control actions. The two lane budgets are 64 MiB/192 MiB
and per-action caps are 1 MiB/24 MiB. One interval admits 4096 splits with 4 MiB
of split metadata. Do not exceed these limits without a new qualification
envelope.

## Registry setup

Use the default `rowweave.registry=inprocess` only for one fixed Master with no
Master failover. It requires no external service.

For Master election and discovery, deploy ZooKeeper and add
`rowweave-registry-zk` to every process classpath that participates in lookup
or election:

```bash
-Drowweave.registry=zk
-Drowweave.zk.connectString=zk1.example.test:2181,zk2.example.test:2181,zk3.example.test:2181
```

Batch HA also requires the same shared artifact directory on every Master:

```bash
-Drowweave.master.artifact.dir=/var/lib/rowweave/artifacts
```

Stream ZooKeeper mode additionally requires the shared Stream artifact root:

```bash
-Drowweave.stream.master.artifact.dir=/var/lib/rowweave/stream-artifacts
```

Before admission, verify ZooKeeper quorum from every RowWeave host, restrict
credentials and purge rights, and use a distinct registry identity for each
process. A Worker never borrows a Master registry session.

## Artifact security

Signed deployment sets are required for external Stream provider deployment.
Configure exact manifest and signer indexes; index gaps, duplicate signer IDs,
relative signer paths, and count mismatches fail before providers load:

```bash
-Drowweave.deployment.count=2
-Drowweave.deployment.manifest.0=/etc/rowweave/deploy/source.manifest
-Drowweave.deployment.signer.id.0=source-release
-Drowweave.deployment.signer.path.0=/etc/rowweave/keys/source-release.pem
-Drowweave.deployment.manifest.1=/etc/rowweave/deploy/sink.manifest
-Drowweave.deployment.signer.id.1=sink-release
-Drowweave.deployment.signer.path.1=/etc/rowweave/keys/sink-release.pem
```

Do not copy part of a deployment set to one Worker and another set to another
Worker. Keep private signing keys off execution hosts. Store checksums and
deployment-set IDs with the change record that authorized the rollout.

Batch custom code must remain outside untrusted data and job payloads. Load
providers from an approved classpath; never deserialize application classes
from job data.

## Start Batch Master and Worker processes

The following single-host example uses loopback transport. Start Master first:

```bash
ROWWEAVE_JVM=(\
  -Xms512m -Xmx1024m -XX:MaxDirectMemorySize=512m \
  -XX:MaxMetaspaceSize=256m -XX:ReservedCodeCacheSize=128m \
  -Xss1m -XX:ActiveProcessorCount=4 \
  -Drowweave.process.memoryLimitBytes=2147483648 \
  -Djava.io.tmpdir=/var/spool/rowweave/master)

nohup java "${ROWWEAVE_JVM[@]}" \
  -Drowweave.master.enable=true \
  -Drowweave.master.port=19000 \
  -Drowweave.control.host=127.0.0.1 \
  -Drowweave.master.artifact.dir=/var/lib/rowweave/artifacts \
  -cp "$NODE_CP" io.rowweave.cluster.ClusterMain \
  > /var/lib/rowweave/logs/master.out \
  2> /var/lib/rowweave/logs/master.err &
echo $! > /var/lib/rowweave/master.pid

grep -q '^MASTER_PORT=' /var/lib/rowweave/logs/master.out
```

Start each Worker after the Master readiness line:

```bash
ROWWEAVE_JVM=(\
  -Xms1g -Xmx2g -XX:MaxDirectMemorySize=1g \
  -XX:MaxMetaspaceSize=256m -XX:ReservedCodeCacheSize=128m \
  -Xss1m -XX:ActiveProcessorCount=4 \
  -Drowweave.process.memoryLimitBytes=4294967296 \
  -Djava.io.tmpdir=/var/spool/rowweave/worker)

nohup java "${ROWWEAVE_JVM[@]}" \
  -Drowweave.worker.enable=true \
  -Drowweave.worker.master.port=19000 \
  -Drowweave.worker.host=127.0.0.1 \
  -Drowweave.worker.slots=32 \
  -Drowweave.worker.memory=2147483648 \
  -Drowweave.worker.maxStageThreads=16 \
  -Drowweave.worker.spill.dir=/var/lib/rowweave/spill \
  -Drowweave.shuffle.ioThreads=2 \
  -cp "$NODE_CP" io.rowweave.cluster.ClusterMain \
  > /var/lib/rowweave/logs/worker-0.out \
  2> /var/lib/rowweave/logs/worker-0.err &
echo $! > /var/lib/rowweave/worker-0.pid

grep -q '^WORKER_READY=' /var/lib/rowweave/logs/worker-0.out
kill -0 "$(cat /var/lib/rowweave/worker-0.pid)"
```

For cross-host Batch deployment, keep the same envelope, set
`rowweave.control.securityProfile=MTLS`, advertise a routable
`rowweave.control.host` / `rowweave.worker.host`, and configure the key store,
trust store, and role authorization file on every process. Never set
`rowweave.shuffle.securityProfile`; data-plane identity follows the admitted
control identity. Plaintext is only for a single-host development profile.

A Worker connects to `rowweave.worker.master.host:port`. With
`rowweave.registry=zk`, it discovers the elected Master instead of using a
fixed endpoint.

## Validate the cluster

Check readiness, process health, and optional Monitor exposure:

```bash
grep '^MASTER_PORT=' /var/lib/rowweave/logs/master.out
grep '^WORKER_READY=' /var/lib/rowweave/logs/worker-0.out
kill -0 "$(cat /var/lib/rowweave/master.pid)"
kill -0 "$(cat /var/lib/rowweave/worker-0.pid)"
```

`MASTER_PORT=<port>` means the Batch Master listener is ready.
`WORKER_READY=<id> port=<port>` means the Worker has registered. `SHUTDOWN` is
the ordered-stop acknowledgement. If `rowweave.monitor.enable=true` and the
Monitor module is present, `MONITOR_PORT=<port>` follows Master readiness.
Monitoring is advisory and never controls execution.

Submit one graph with the production sink, parallelism, provider set, and
resource profile. Confirm the typed result, sink output, and absence of a new
`ERROR` or typed failure in the Worker logs. Do not use an in-memory collector
as a success check for remote Batch execution.

### Typed output checks

Readiness markers prove the processes are up; the typed results below prove
what the job actually published.

* `JobClient.submit(PlanGraph)` returns one `ExecutionReport` covering the
  whole graph, and `rowsOut()` totals the rows written across every sink
  (`io/rowweave/cluster/JobClient.java:47`,
  `io/rowweave/plan/ExecutionReport.java:193`).
* `withinBudget()` holds the no-OOM invariant for the run: the peak
  reservation never exceeded the acquired quota
  (`io/rowweave/plan/ExecutionReport.java:226`).
* For a run whose sink was written outside the submitting JVM, query the
  durable outcome with `queryBatchSinkOutcome(runId)`. It projects one of
  `NOT_FOUND`, `PREPARING`, `COMMIT_DECIDED`, `COMMITTED`, `ABORT_DECIDED`,
  `ABORTED`, or `CORRUPT_OR_CONFLICT`
  (`io/rowweave/cluster/ClusterJobClient.java:687`,
  `io/rowweave/commit/BatchSinkOutcome.java:9-16`). A communication failure
  never resolves to `NOT_FOUND`, and `COMMITTED` is the only state that
  certifies publication. The ZooKeeper-backed client exposes the same query
  through the currently elected leader
  (`rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkRemoteJobClient.java:120`).
* Read the sink output back through a source or the external system it
  targets and compare the observed rows with `rowsOut()` before promoting the
  cluster to serving traffic.

The properties used above are documented in the configuration reference:
[Batch process roles](../reference/configuration.md#11-batch-process-roles),
[control plane](../reference/configuration.md#13-control-plane-rpc-admission-and-transport),
[shuffle and data-plane transport](../reference/configuration.md#15-shuffle-and-data-plane-transport),
[deployment identity and signing](../reference/configuration.md#18-deployment-identity-and-signing),
and [Stream cluster and worker](../reference/configuration.md#110-stream-cluster-and-worker).

## Ports and network admission

Reserve and firewall these TCP boundaries:

| Boundary | Port policy | Required reachability |
|---|---|---|
| Batch Master RPC | `rowweave.master.port`, normally explicit | Clients and every Worker to every possible Master |
| Worker control/shuffle | Assigned by the Worker and published in roles | Master and peer Workers to each Worker advertised host |
| Stream Master RPC | `rowweave.stream.master.port`, normally explicit | Clients and Workers to every possible Stream Master |
| Stream Worker RPC | `rowweave.stream.worker.port`, normally explicit | Stream Master to each Worker |
| Monitor HTTP | `rowweave.monitor.port`, optional | Only authorized observer networks |

LOOPBACK mode rejects non-loopback advertised endpoints. mTLS mode requires
certificates for every cross-host identity and should reject plaintext
fallback. Keep control, shuffle, and checkpoint certificate authorities
separate according to [security](security.md).

## Logging directories

RowWeave writes readiness markers to stdout and framework/application
diagnostics through SLF4J. Capture both in service-managed files; this page
uses `logs/*.out` and `logs/*.err` for clarity. Do not treat a logger line as
readiness.

Use separate, absolute directories for role-local temporary files, spill, shared
artifacts, and captured logs. Monitor inode, descriptor, and free-space limits
with the same rigor as byte limits. Never point two roles or two versions at
the same Worker temporary directory.

## 6. Stream product bootstrap

Start `StreamClusterMain` in Master-only or Worker-only mode with
`rowweave.stream.master.enable`, `rowweave.stream.worker.enable`, and
`rowweave.registry` configured explicitly. Worker bootstrap can also use
`RowWeave.startStreamWorker`; embedded Master bootstrap can use
`RowWeave.startStreamMaster`. `StreamElasticExample` demonstrates a complete
submission, rescale, and stop-with-savepoint sequence.

A fixed Master-only deployment declares Worker endpoints with
`rowweave.stream.master.workers`. ZooKeeper mode uses election and discovery
plus the shared Stream artifact directory. Every role requires
`rowweave.stream.state.dir`, and every Worker requires a stable
`rowweave.stream.worker.id`.

external Stream process roles require MTLS. Plaintext fallback is only for the
explicit local development profile and must not be used cross-host. Keep
control, shuffle, and checkpoint certificates separate.

A Stream process is ready only after it prints
`STREAM_MASTER_READY=<host>:<port>`, `STREAM_WORKER_READY=<host>:<port>`, or
both. Await explicit operation status for submit, checkpoint, savepoint,
restore, rescale, and cancel; never infer completion from elapsed time.

## Shutdown and rollback checks

For an application-embedded runtime, close the client or `AutoCloseable`
cluster, then verify the application thread pool and state directory are no
longer growing.

For `ClusterMain`, send `SIGTERM` to Workers first, then to remaining Masters:

```bash
for role in worker-0 master; do
  pid=$(cat "/var/lib/rowweave/${role}.pid")
  kill -TERM "$pid"
  for attempt in 1 2 3 4 5 6; do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  kill -0 "$pid" 2>/dev/null && echo "rollback stop required for $role" >&2
done
grep -q '^SHUTDOWN$' /var/lib/rowweave/logs/worker-0.out
grep -q '^SHUTDOWN$' /var/lib/rowweave/logs/master.out
```

If the drain deadline passes, follow your site's forced-stop policy, record the
process state and logs, and do not reuse a temporary directory whose ownership
is unknown.

Before software rollback, stop admission and wait for active jobs to reach a
typed terminal state. Record checksums, deployment-set IDs, registry entries,
sink generations, and the latest completed checkpoint or savepoint. Restore the
previous checksummed artifact set and deployment manifests as one unit. Restart
Workers before Masters, require the same readiness markers, and submit the
same validation graph.

For durable state, follow the applicable
[rolling upgrade](../../runbooks/rolling-upgrade-v1.md) or
[store migration](../../runbooks/store-migration.md) procedure. Do not
open a checkpoint, savepoint, or store directory with a binary that has not
been qualified for that format. If a sink or store is not proven reversible,
use a new output or state generation instead of overwriting the previous one.

An executable shutdown/rollback example for Local and InProcess paths remains
[`RunModesExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/RunModesExample.java):
it runs both paths in fresh state and reports `Equivalent result sets: true`.
