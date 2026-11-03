package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** any new production thread creation site must update the cost model. */
class ThreadCreationInventoryArchitectureTest {

    @Test
    void productionThreadCreationInventoryCannotDriftSilently() throws Exception {
        Map<String, Integer> expected = new LinkedHashMap<>();
        // process-wide runtime owns one shutdown-only cleanup hook;
        // it is not a live operational thread and does not consume runtime capacity.
        expected.put("io/rowweave/InProcessBatchRuntime.java", 1);
        // the product Stream entrypoint registers the same
        // shutdown-only cleanup hook; it remains dormant during operation.
        expected.put("io/rowweave/StreamClusterMain.java", 1);
        expected.put("io/rowweave/artifact/ArtifactJanitor.java", 2);
        expected.put("io/rowweave/cluster/ClusterMain.java", 1);
        expected.put("io/rowweave/cluster/HeartbeatMonitor.java", 1); // dormant utility
        expected.put("io/rowweave/cluster/LeaderRecoveryInventory.java", 2);
        expected.put("io/rowweave/cluster/MasterServer.java", 2);
        expected.put("io/rowweave/cluster/StageAssignmentDispatcher.java", 2);
        expected.put("io/rowweave/cluster/StageCancellationCoordinator.java", 2);
        expected.put("io/rowweave/cluster/StageTask.java", 2);
        // includes the single-shot poisoned-runtime fence closer;
        // ProcessThreadCapacityModel reserves it explicitly.
        // Heartbeat isolation adds one maintenance executor/thread-factory pair;
        // one extra live thread must be charged in ProcessThreadCapacityModel.
        expected.put("io/rowweave/cluster/WorkerServer.java", 7);
        expected.put("io/rowweave/cluster/rpc/BoundedInProcessExecutor.java", 2);
        expected.put("io/rowweave/cluster/rpc/RpcServer.java", 2);
        expected.put("io/rowweave/deployment/DeploymentRuntime.java", 2);
        // one process-wide fixed provider action pool.
        expected.put("io/rowweave/internal/ProviderActionSupervisor.java", 2);
        expected.put("io/rowweave/mem/PipelinedExternalSortBuffer.java", 2);
        expected.put("io/rowweave/mem/spill/SpillWorkspaces.java", 1);
        // process-wide Stream monitor owns one shutdown-only cleanup hook;
        // it is dormant unless monitoring is explicitly enabled.
        expected.put("io/rowweave/monitor/MonitorRuntime.java", 1);
        // Optional client observation owns one daemon per enabled client, outside
        // Master/Worker task capacity. Closing the client releases it; NOOP creates none.
        expected.put("io/rowweave/monitor/MonitoredStreamJobClient.java", 2);
        expected.put("io/rowweave/ops/OrderBy.java", 1);
        expected.put("io/rowweave/ops/join/ConcurrentBinaryDrain.java", 2);
        expected.put("io/rowweave/run/TerminalRunJanitor.java", 2);
        expected.put("io/rowweave/sched/BlockingFanout.java", 1);
        expected.put("io/rowweave/sched/Task.java", 1);
        expected.put("io/rowweave/sched/net/BoundedShuffleIngressExecutor.java", 2);
        expected.put("io/rowweave/sched/net/DataPlaneHandshakeAdmission.java", 2);
        expected.put("io/rowweave/sched/net/MultiSocketChannelSource.java", 3);
        // both public constructors now share one validated group factory.
        expected.put("io/rowweave/sched/net/MuxConnectionPool.java", 1);
        expected.put("io/rowweave/sched/net/NamedDaemonThreadFactory.java", 1);
        expected.put("io/rowweave/sched/net/SocketChannelSink.java", 1);
        expected.put("io/rowweave/sched/net/SocketChannelSource.java", 3);
        expected.put("io/rowweave/sched/net/WorkerDataServer.java", 3);
        // the product Master owns one listener, one recovery
        // monitor, one checkpoint lane and a fixed four-thread control pool.
        // Slow checkpoint connections therefore cannot head-of-line block
        // independent status/evidence reads. In-process mode has no listener
        // or control pool. Executor factories and their daemon ThreadFactories
        // are intentionally inventoried as separate static creation sites.
        expected.put("io/rowweave/stream/cluster/StreamProductJobMasterServer.java", 6);
        // a product Worker owns one bounded
        // control facility plus one non-replacing source-discovery lane.
        // Remote mode uses one listener and a fixed four-thread request pool so
        // one stalled socket cannot head-of-line block the Worker; in-process
        // mode substitutes one bounded deadline executor for that remote
        // facility. Executor factories and daemon ThreadFactories are
        // intentionally inventoried as separate static creation sites.
        expected.put("io/rowweave/stream/cluster/StreamRemoteWorkerServer.java", 8);
        // each async wait owns a bounded request pool and one
        // timeout scheduler. Stream product admission and remote placement
        // charge runner + maxConcurrency + timeout per async node; the third static site is
        // the shared thread-factory expression used by both bounded pools.
        expected.put("io/rowweave/stream/cluster/StreamAsyncWaitOperatorTask.java", 3);
        // each admitted stateful plan has one dedicated mailbox
        // owner. Legacy/generic window, keyed aggregate and temporal join
        // shapes are mutually exclusive, so product placement charges the
        // selected implementation.
        expected.put("io/rowweave/stream/cluster/StreamGenericWindowOperatorTask.java", 1);
        expected.put("io/rowweave/stream/cluster/StreamKeyedAggregateOperatorTask.java", 1);
        expected.put("io/rowweave/stream/cluster/StreamKeyedProcessOperatorTask.java", 1);
        // every placed non-endpoint task owns one bounded
        // lifecycle thread. Placement charges it through the node-local
        // execution descriptor rather than a whole-graph shape estimate.
        expected.put("io/rowweave/stream/cluster/StreamRemoteOperatorTaskRunner.java", 1);
        expected.put("io/rowweave/stream/cluster/StreamTemporalJoinOperatorTask.java", 1);
        // UNION owns one bounded input-gate task, accounted per placed node.
        expected.put("io/rowweave/stream/cluster/StreamUnionOperatorTask.java", 1);
        expected.put("io/rowweave/stream/cluster/StreamWindowOperatorTask.java", 1);
        // each socket source lane owns one reverse-control
        // reader; StreamTaskResourceRequest charges task + lane helper against
        // StreamWorkerCapabilityProfile.maximumThreads before placement.
        expected.put("io/rowweave/stream/cluster/StreamSocketChannel.java", 1);
        // resident CDC and transactional Sink tasks replace the
        // ordinary source/Sink mailbox in mutually exclusive admitted shapes;
        // the existing remote source/sink request formulas conservatively
        // charge these threads.
        expected.put("io/rowweave/stream/runtime/LocalResidentCdcSourceTask.java", 1);
        expected.put("io/rowweave/stream/runtime/LocalStreamSinkTask.java", 1);
        // Event-time sources add one bounded watchdog that only wakes the
        // provider; the source mailbox remains the sole data/state owner.
        expected.put("io/rowweave/stream/runtime/LocalStreamSourceTask.java", 2);
        expected.put("io/rowweave/stream/runtime/LocalTransactionalStreamSinkTask.java", 1);
        expected.put("io/rowweave/stream/runtime/StreamTask.java", 1);
        // Public connector conformance has call-scoped owner threads for the
        // ordinary and bounded source kits plus a timeout-only cancel helper;
        // none is a deployed runtime role or Worker task capacity.
        expected.put("io/rowweave/stream/spi/conformance/StreamConnectorConformance.java", 3);

        Path root = sourceRoot();
        Map<String, Integer> actual = new LinkedHashMap<>();
        try (Stream<Path> files = Files.walk(root)) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .sorted()
                    .forEach(path -> {
                        try {
                            String source = new String(Files.readAllBytes(path),
                                    StandardCharsets.UTF_8);
                            int sites = occurrences(source, "new Thread(")
                                    + occurrences(source, "Executors.new")
                                    + occurrences(source, "new ThreadPoolExecutor(")
                                    + occurrences(source, "new NioEventLoopGroup(")
                                    + occurrences(source, "new DefaultEventExecutorGroup(");
                            if (sites > 0) {
                                actual.put(root.relativize(path).toString()
                                        .replace(java.io.File.separatorChar, '/'), sites);
                            }
                        } catch (java.io.IOException failure) {
                            throw new java.io.UncheckedIOException(failure);
                        }
                    });
        }
        assertEquals(expected, actual,
                "thread site drift requires StageThreadCostModel/ProcessThreadCapacityModel review");
    }

    private static int occurrences(String source, String token) {
        int count = 0;
        for (int offset = 0; (offset = source.indexOf(token, offset)) >= 0;
             offset += token.length()) {
            count++;
        }
        return count;
    }

    private static Path sourceRoot() {
        Path current = Paths.get("").toAbsolutePath();
        Path direct = current.resolve("src/main/java");
        return Files.isDirectory(direct) ? direct
                : current.resolve("rowweave-core/src/main/java");
    }
}
