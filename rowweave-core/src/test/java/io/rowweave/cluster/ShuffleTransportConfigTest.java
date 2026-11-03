package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShuffleTransportConfigTest {

    @Test
    void muxIsEnabledWhenPropertyIsAbsentOrBlank() {
        assertTrue(ShuffleTransportConfig.muxEnabled(new Properties()));
        Properties blank = new Properties();
        blank.setProperty(ShuffleTransportConfig.MUX_PROPERTY, "  ");
        assertTrue(ShuffleTransportConfig.muxEnabled(blank));
    }

    @Test
    void explicitBooleanSelectsMuxOrLegacy() {
        Properties enabled = new Properties();
        enabled.setProperty(ShuffleTransportConfig.MUX_PROPERTY, " TrUe ");
        assertTrue(ShuffleTransportConfig.muxEnabled(enabled));

        Properties disabled = new Properties();
        disabled.setProperty(ShuffleTransportConfig.MUX_PROPERTY, " FaLsE ");
        assertFalse(ShuffleTransportConfig.muxEnabled(disabled));
    }

    @Test
    void invalidValueFailsFastInsteadOfSilentlySelectingLegacy() {
        Properties invalid = new Properties();
        invalid.setProperty(ShuffleTransportConfig.MUX_PROPERTY, "treu");
        assertThrows(IllegalArgumentException.class,
                () -> ShuffleTransportConfig.muxEnabled(invalid));
    }

    @Test
    void masterAndWorkerCannotGrowIndependentPropertyParsers() throws Exception {
        String master = source("MasterServer.java");
        String worker = source("WorkerServer.java");
        String forbiddenBoolean = "Boolean.getBoolean(\"" + ShuffleTransportConfig.MUX_PROPERTY + "\")";
        String forbiddenString = "System.getProperty(\"" + ShuffleTransportConfig.MUX_PROPERTY + "\"";
        assertFalse(master.contains(forbiddenBoolean) || master.contains(forbiddenString));
        assertFalse(worker.contains(forbiddenBoolean) || worker.contains(forbiddenString));
    }

    @Test
    void workerDataPortMatchesEffectiveMuxMode(@TempDir Path tmp) {
        try (RpcServer master = new RpcServer(request -> RpcMessage.ack());
             WorkerServer worker = WorkerServer.newUnguarded(
                     "config-worker", 2, "127.0.0.1", master.port(), tmp.toFile())) {
            assertEquals(ShuffleTransportConfig.muxEnabled(), worker.dataPort() > 0,
                    "the default really binds one data port; an explicit false creates no mux server");
        }
    }

    @Test
    void eachWorkerUsesItsOwnStartupSnapshotInsteadOfClassInitializationState(@TempDir Path tmp) {
        String previous = System.getProperty(ShuffleTransportConfig.MUX_PROPERTY);
        try (RpcServer master = new RpcServer(request -> RpcMessage.ack())) {
            System.setProperty(ShuffleTransportConfig.MUX_PROPERTY, "true");
            try (WorkerServer enabled = WorkerServer.newUnguarded(
                    "mux-enabled-worker", 2, "127.0.0.1", master.port(),
                    tmp.resolve("enabled").toFile())) {
                assertTrue(enabled.dataPort() > 0);
            }

            System.setProperty(ShuffleTransportConfig.MUX_PROPERTY, "false");
            try (WorkerServer disabled = WorkerServer.newUnguarded(
                    "mux-disabled-worker", 2, "127.0.0.1", master.port(),
                    tmp.resolve("disabled").toFile())) {
                assertEquals(-1, disabled.dataPort());
            }
        } finally {
            if (previous == null) {
                System.clearProperty(ShuffleTransportConfig.MUX_PROPERTY);
            } else {
                System.setProperty(ShuffleTransportConfig.MUX_PROPERTY, previous);
            }
        }
    }

    @Test
    void workerActuallySendsLoopbackDataCapability(@TempDir Path tmp) {
        AtomicReference<RpcMessage> registration = new AtomicReference<>();
        try (RpcServer master = new RpcServer(request -> {
            if (request.type() == RpcMessage.Type.REGISTER_WORKER) registration.set(request);
            return request.type() == RpcMessage.Type.HEARTBEAT ? null : RpcMessage.ack();
        });
             WorkerServer ignored = WorkerServer.newUnguarded(
                     "capability-worker", 2, "127.0.0.1", master.port(), tmp.toFile())) {
            assertEquals(io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0,
                    registration.get().dataPlaneRegistration().protocol());
        }
    }

    private static String source(String fileName) throws Exception {
        Path cwd = Paths.get("").toAbsolutePath();
        Path source = cwd.resolve("src/main/java/io/rowweave/cluster").resolve(fileName);
        if (!Files.exists(source)) {
            source = cwd.resolve("rowweave-core/src/main/java/io/rowweave/cluster").resolve(fileName);
        }
        return new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
    }
}

