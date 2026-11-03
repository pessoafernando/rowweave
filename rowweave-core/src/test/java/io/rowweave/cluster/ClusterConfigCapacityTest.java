package io.rowweave.cluster;

import io.rowweave.cluster.rpc.ControlSecurityProfile;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** frozen role config and aggregate heap/RSS preflight. */
class ClusterConfigCapacityTest {

    private static final long MIB = 1024L * 1024L;
    private static final long GIB = 1024L * MIB;

    @Test
    void workerMemoryUsesOneCanonicalValueAndStrictSlots() {
        Properties canonical = workerProperties();
        canonical.setProperty("rowweave.worker.memory", Long.toString(GIB));
        canonical.setProperty("rowweave.memory.bytes", Long.toString(GIB));
        canonical.setProperty("rowweave.worker.slots", "8");
        ClusterConfig configured = ClusterConfig.fromProperties(
                canonical, capacity(4L * GIB, 8L * GIB));
        assertEquals(GIB, configured.memoryBytes());
        assertEquals(8, configured.workerSlots());
        assertEquals(24, configured.workerMaxStageThreads());

        Properties legacyOnly = workerProperties();
        legacyOnly.setProperty("rowweave.memory.bytes", Long.toString(GIB));
        assertEquals(GIB, ClusterConfig.fromProperties(
                legacyOnly, capacity(4L * GIB, 8L * GIB)).memoryBytes());

        assertEquals(2L * GIB, ClusterConfig.fromProperties(
                workerProperties(), capacity(4L * GIB, 8L * GIB)).memoryBytes());

        assertConfigurationFailure(workerProperty("rowweave.worker.memory", "01048576"));
        Properties conflict = workerProperty("rowweave.worker.memory", Long.toString(GIB));
        conflict.setProperty("rowweave.memory.bytes", Long.toString(GIB + 1L));
        assertConfigurationFailure(conflict);
        assertConfigurationFailure(workerProperty(
                "rowweave.worker.slots", "2147483648"));
        assertConfigurationFailure(workerProperty("rowweave.worker.slots", " 8"));
        assertConfigurationFailure(workerProperty(
                "rowweave.worker.maxStageThreads", "0"));
        assertConfigurationFailure(workerProperty(
                "rowweave.worker.maxStageThreads", " 24"));

        Properties excessiveMonitorThreads = new Properties();
        excessiveMonitorThreads.setProperty("rowweave.master.enable", "true");
        excessiveMonitorThreads.setProperty("rowweave.monitor.ioThreads", "65");
        assertConfigurationFailure(excessiveMonitorThreads);

        Properties frozenSource = workerProperty(
                "rowweave.worker.memory", Long.toString(GIB));
        frozenSource.setProperty("rowweave.worker.slots", "4");
        ClusterConfig frozen = ClusterConfig.fromProperties(
                frozenSource, capacity(4L * GIB, 8L * GIB));
        frozenSource.setProperty("rowweave.worker.memory", Long.toString(2L * GIB));
        frozenSource.setProperty("rowweave.worker.slots", "9");
        assertEquals(GIB, frozen.memoryBytes());
        assertEquals(4, frozen.workerSlots());
    }

    @Test
    void mtlsRoleConfigDoesNotReadMissingTlsMaterialDuringPreflight() {
        Properties properties = workerProperties();
        properties.setProperty("rowweave.master.enable", "true");
        properties.setProperty("rowweave.control.securityProfile", "MTLS");
        properties.setProperty("rowweave.control.host", "127.0.0.1");
        properties.setProperty("rowweave.worker.memory", Long.toString(GIB));

        ClusterConfig config = ClusterConfig.fromProperties(
                properties, capacity(4L * GIB, 8L * GIB));

        assertEquals(ControlSecurityProfile.MTLS, config.controlProfile());
        assertNotNull(config.dataPlaneConfig());
        assertEquals(DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1,
                config.dataPlaneConfig().protocol());
    }

    @Test
    void everyRoleAndSecurityProfileCombinationHasAPureSnapshot() {
        for (boolean master : new boolean[] {false, true}) {
            for (boolean worker : new boolean[] {false, true}) {
                if (!master && !worker) continue;
                for (String profile : new String[] {"LOOPBACK", "MTLS"}) {
                    Properties properties = new Properties();
                    properties.setProperty("rowweave.master.enable",
                            Boolean.toString(master));
                    properties.setProperty("rowweave.worker.enable",
                            Boolean.toString(worker));
                    properties.setProperty("rowweave.control.securityProfile", profile);
                    properties.setProperty("rowweave.control.host", "127.0.0.1");
                    if (worker) {
                        properties.setProperty("rowweave.worker.memory",
                                Long.toString(GIB));
                        properties.setProperty("rowweave.worker.slots", "8");
                        properties.setProperty("rowweave.worker.maxStageThreads", "24");
                    }
                    ClusterConfig config = ClusterConfig.fromProperties(
                            properties, capacity(4L * GIB, 8L * GIB));
                    assertEquals(master, config.masterEnabled());
                    assertEquals(worker, config.workerEnabled());
                    assertEquals(ControlSecurityProfile.valueOf(profile),
                            config.controlProfile());
                    assertEquals(worker, config.dataPlaneConfig() != null);
                }
            }
        }
    }

    @Test
    void heapAggregationIncludesOnlyEnabledRoleBudgets() {
        JvmCapacityEnvelope capacity = capacity(4L * GIB, 8L * GIB);
        long headroom = capacity.heapHeadroomBytes();
        long fixed = 256L * MIB + 256L * MIB + headroom;

        Properties master = new Properties();
        master.setProperty("rowweave.master.enable", "true");
        assertEquals(fixed, ClusterConfig.fromProperties(
                master, capacity).heapRequiredBytes());

        Properties workerNoMux = workerProperty(
                "rowweave.worker.memory", Long.toString(GIB));
        workerNoMux.setProperty("rowweave.shuffle.mux", "false");
        assertEquals(fixed + GIB, ClusterConfig.fromProperties(
                workerNoMux, capacity).heapRequiredBytes());

        Properties workerMux = workerProperty(
                "rowweave.worker.memory", Long.toString(GIB));
        assertEquals(fixed + GIB + 256L * MIB, ClusterConfig.fromProperties(
                workerMux, capacity).heapRequiredBytes());

        Properties mtls = workerProperty(
                "rowweave.worker.memory", Long.toString(GIB));
        mtls.setProperty("rowweave.control.securityProfile", "MTLS");
        mtls.setProperty("rowweave.control.host", "127.0.0.1");
        assertEquals(fixed + GIB + 256L * MIB + 832L * MIB,
                ClusterConfig.fromProperties(mtls, capacity).heapRequiredBytes());
    }

    @Test
    void heapAndBaseRssRejectAnEnvelopeShortByOneByte() {
        JvmCapacityEnvelope fourGiB = capacity(4L * GIB, 8L * GIB);
        long nonWorker = 256L * MIB + 256L * MIB + fourGiB.heapHeadroomBytes();
        Properties heapShort = workerProperty("rowweave.shuffle.mux", "false");
        heapShort.setProperty("rowweave.worker.memory",
                Long.toString(4L * GIB - nonWorker + 1L));
        ProcessCapacityException heapFailure = assertThrows(
                ProcessCapacityException.class,
                () -> ClusterConfig.fromProperties(heapShort, fourGiB));
        assertEquals(ProcessCapacityException.Code.HEAP_CAPACITY, heapFailure.code());

        ClusterConfig empty = ClusterConfig.fromProperties(
                new Properties(), capacity(4L * GIB, 8L * GIB));
        long baseRss = fourGiB.rssRequiredBytes(empty.javaThreadUpperBound());
        ProcessCapacityException rssFailure = assertThrows(
                ProcessCapacityException.class,
                () -> ClusterConfig.fromProperties(new Properties(),
                        capacity(4L * GIB, baseRss - 1L)));
        assertEquals(ProcessCapacityException.Code.RSS_CAPACITY, rssFailure.code());

        Properties overflow = workerProperty(
                "rowweave.worker.memory", Long.toString(Long.MAX_VALUE));
        ProcessCapacityException overflowFailure = assertThrows(
                ProcessCapacityException.class,
                () -> ClusterConfig.fromProperties(overflow, fourGiB));
        assertEquals(ProcessCapacityException.Code.HEAP_CAPACITY,
                overflowFailure.code());

        Properties master = new Properties();
        master.setProperty("rowweave.master.enable", "true");
        ProcessCapacityException obsoleteHalfHeapFailure = assertThrows(
                ProcessCapacityException.class,
                () -> ClusterConfig.fromProperties(
                        master, capacity(512L * MIB, 4L * GIB)));
        assertEquals(ProcessCapacityException.Code.HEAP_CAPACITY,
                obsoleteHalfHeapFailure.code());

        Properties invalidIngress = workerProperty(
                "rowweave.shuffle.ingressResidentBytes", "-1");
        ProcessCapacityException invalidIngressFailure = assertThrows(
                ProcessCapacityException.class,
                () -> ClusterConfig.fromProperties(invalidIngress, fourGiB));
        assertEquals(ProcessCapacityException.Code.CONFIGURATION,
                invalidIngressFailure.code());
    }

    @Test
    void roleSpecificNonHeapCertificationMinimumsFailByOneByte() {
        Properties master = new Properties();
        master.setProperty("rowweave.master.enable", "true");
        assertCertificationFailure(master, 64L * MIB - 1L,
                128L * MIB, 64L * MIB);
        assertCertificationFailure(master, 64L * MIB,
                128L * MIB - 1L, 64L * MIB);
        assertCertificationFailure(master, 64L * MIB,
                128L * MIB, 64L * MIB - 1L);
        ClusterConfig.fromProperties(master, capacity(4L * GIB, 8L * GIB,
                64L * MIB, 128L * MIB, 64L * MIB));

        Properties worker = workerProperties();
        worker.setProperty("rowweave.worker.memory", Long.toString(GIB));
        assertCertificationFailure(worker, 256L * MIB - 1L,
                128L * MIB, 64L * MIB);
        ClusterConfig.fromProperties(worker, capacity(4L * GIB, 8L * GIB,
                256L * MIB, 128L * MIB, 64L * MIB));
    }

    @Test
    void documentedMasterWorkerAndCombinedStartingPointsPassPurePreflight() {
        Properties master = new Properties();
        master.setProperty("rowweave.master.enable", "true");
        ClusterConfig.fromProperties(master, capacity(2L * GIB, 4L * GIB,
                512L * MIB, 384L * MIB, 128L * MIB));

        Properties worker = documentedWorkerProperties();
        ClusterConfig.fromProperties(worker, capacity(4L * GIB, 8L * GIB,
                768L * MIB, 384L * MIB, 128L * MIB));

        Properties combined = documentedWorkerProperties();
        combined.setProperty("rowweave.master.enable", "true");
        combined.setProperty("rowweave.worker.slots", "16");
        ClusterConfig.fromProperties(combined, capacity(4L * GIB, 8L * GIB,
                768L * MIB, 384L * MIB, 128L * MIB));
    }

    private static Properties documentedWorkerProperties() {
        Properties properties = new Properties();
        properties.setProperty("rowweave.worker.enable", "true");
        properties.setProperty("rowweave.worker.slots", "32");
        properties.setProperty("rowweave.worker.maxStageThreads", "16");
        properties.setProperty("rowweave.worker.memory", Long.toString(2L * GIB));
        properties.setProperty("rowweave.shuffle.ioThreads", "2");
        return properties;
    }

    private static void assertCertificationFailure(Properties properties,
                                                   long direct, long metaspace,
                                                   long codeCache) {
        ProcessCapacityException failure = assertThrows(
                ProcessCapacityException.class,
                () -> ClusterConfig.fromProperties(properties,
                        capacity(4L * GIB, 8L * GIB,
                                direct, metaspace, codeCache)));
        assertEquals(ProcessCapacityException.Code.CERTIFICATION, failure.code());
    }

    private static void assertConfigurationFailure(Properties properties) {
        ProcessCapacityException failure = assertThrows(
                ProcessCapacityException.class,
                () -> ClusterConfig.fromProperties(
                        properties, capacity(4L * GIB, 8L * GIB)));
        assertEquals(ProcessCapacityException.Code.CONFIGURATION, failure.code());
    }

    private static Properties workerProperties() {
        Properties properties = new Properties();
        properties.setProperty("rowweave.worker.enable", "true");
        properties.setProperty("rowweave.worker.slots", "8");
        // Keep this fixture independent from the certified product default.
        properties.setProperty("rowweave.worker.maxStageThreads", "24");
        return properties;
    }

    private static Properties workerProperty(String name, String value) {
        Properties properties = workerProperties();
        properties.setProperty(name, value);
        return properties;
    }

    private static JvmCapacityEnvelope capacity(long xmx, long processLimit) {
        return capacity(xmx, processLimit,
                768L * MIB, 384L * MIB, 128L * MIB);
    }

    private static JvmCapacityEnvelope capacity(long xmx, long processLimit,
                                                long direct, long metaspace,
                                                long codeCache) {
        Properties properties = new Properties();
        properties.setProperty(JvmCapacityEnvelope.PROCESS_MEMORY_LIMIT_BYTES,
                Long.toString(processLimit));
        return JvmCapacityEnvelope.from(Arrays.asList(
                "-Xms512m",
                "-Xmx" + xmx,
                "-XX:MaxDirectMemorySize=" + direct,
                "-XX:MaxMetaspaceSize=" + metaspace,
                "-XX:ReservedCodeCacheSize=" + codeCache,
                "-Xss1m",
                "-XX:ActiveProcessorCount=4"), properties);
    }
}
