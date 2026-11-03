package io.rowweave.soak;

import org.apache.zookeeper.server.DatadirCleanupManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamSoakZooKeeperPlacementTest {
    @TempDir Path temporary;

    @Test void placesDurableAuthorityInsideMeasuredOutputTree()
            throws Exception {
        Path output = temporary.resolve("run");
        Files.createDirectories(output);
        Path expected = output.resolve("zookeeper");

        try (StreamSoakMain.ManagedZooKeeper server =
                     StreamSoakMain.startZooKeeper(output)) {
            assertEquals(expected.toRealPath(),
                    server.tempDirectory().toPath().toRealPath());
            assertEquals(DatadirCleanupManager.PurgeTaskStatus.STARTED,
                    server.cleanupStatus());
            assertEquals(3, server.snapRetainCount());
            assertEquals(1, server.purgeIntervalHours());
        }

        assertTrue(Files.isDirectory(expected));
        assertTrue(StreamSoakResourceSampler.diskBytes(output) > 0L);
    }

    @Test void declaresDurableAuthorityLogRetention() {
        Map<String, Object> properties =
                StreamSoakMain.zooKeeperProperties();
        assertEquals("3", properties.get("autopurge.snapRetainCount"));
        assertEquals("1", properties.get("autopurge.purgeInterval"));
    }

    @Test void acceleratedHarnessBoundsRealSnapshotAndLogPreallocation()
            throws Exception {
        String prior = System.getProperty("zookeeper.snapCount");
        String priorPrealloc = System.getProperty("zookeeper.preAllocSize");
        long priorPreallocBytes = org.apache.zookeeper.server.persistence
                .FilePadding.getPreAllocSize();
        try {
            StreamSoakMain.configureAcceleratedZooKeeperSnapshotting(
                    StreamSoakConfig.Profile.ACCELERATED_FEASIBILITY);
            assertEquals("10240", System.getProperty("zookeeper.snapCount"));
            assertEquals("8192", System.getProperty("zookeeper.preAllocSize"));
            assertEquals(8L * 1024L * 1024L,
                    org.apache.zookeeper.server.persistence.FilePadding
                            .getPreAllocSize());
            Path output = temporary.resolve("accelerated");
            Files.createDirectories(output);
            try (StreamSoakMain.ManagedZooKeeper server =
                         StreamSoakMain.startZooKeeper(output)) {
                try (org.apache.zookeeper.ZooKeeper client =
                             new org.apache.zookeeper.ZooKeeper(
                                     server.getConnectString(), 5_000,
                                     event -> { })) {
                    client.create("/storage-probe", new byte[] {1},
                            org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            org.apache.zookeeper.CreateMode.PERSISTENT);
                }
                Path log;
                try (java.util.stream.Stream<Path> files = Files.list(
                        output.resolve("zookeeper/version-2"))) {
                    log = files.filter(path -> path.getFileName().toString()
                                    .startsWith("log."))
                            .findFirst().orElseThrow(AssertionError::new);
                }
                assertEquals(8L * 1024L * 1024L + 16L,
                        Files.size(log));
            }
        } finally {
            if (prior == null) System.clearProperty("zookeeper.snapCount");
            else System.setProperty("zookeeper.snapCount", prior);
            if (priorPrealloc == null) System.clearProperty("zookeeper.preAllocSize");
            else System.setProperty("zookeeper.preAllocSize", priorPrealloc);
            org.apache.zookeeper.server.persistence.FileTxnLog
                    .setPreallocSize(priorPreallocBytes);
        }
    }
}
