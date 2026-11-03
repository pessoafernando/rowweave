package io.rowweave.cluster;

import io.rowweave.cluster.rpc.ControlSecurityProfile;
import io.rowweave.sched.net.ShuffleIngressAdmissionConfig;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataPlaneConfigTest {

    private static final long MIB = 1024L * 1024L;
    private static final long HEAP = 4L * 1024L * MIB;

    @Test
    void loopbackDefaultsAreExactAndDirectionallyBounded() throws Exception {
        DataPlaneConfig config = config(new Properties(), ControlSecurityProfile.LOOPBACK,
                InetAddress.getByName("127.0.0.1"));

        assertEquals(DataPlaneConfig.Protocol.LOOPBACK_PLAIN_V0, config.protocol());
        assertTrue(config.muxEnabled());
        assertTrue(config.bindAddress().isLoopbackAddress());
        assertEquals(4, config.maxConcurrentHandshakes());
        assertEquals(2, config.inboundHandshakeSlots());
        assertEquals(2, config.outboundHandshakeSlots());
        assertEquals(10_000L, config.handshakeTimeoutMillis());
        assertEquals(10_000L, config.connectTimeoutMillis());
        assertEquals(24L * MIB, config.handshakeResidentBytes());
        assertEquals(1024, config.authFrameBytes());
        config.requireAllowedPeer(InetAddress.getByName("::1"));
        assertThrows(IllegalArgumentException.class,
                () -> config.requireAllowedPeer(InetAddress.getByName("192.0.2.1")));
    }

    @Test
    void mtlsRequiresMuxAndUsesOnlyV1() throws Exception {
        Properties properties = new Properties();
        DataPlaneConfig config = config(properties, ControlSecurityProfile.MTLS,
                InetAddress.getByName("192.0.2.10"));
        assertEquals(DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1, config.protocol());
        assertEquals(832L * MIB, config.handshakeResidentBytes());

        properties.setProperty("rowweave.shuffle.mux", "false");
        IllegalArgumentException disabled = assertThrows(IllegalArgumentException.class,
                () -> config(properties, ControlSecurityProfile.MTLS,
                        InetAddress.getByName("192.0.2.10")));
        assertTrue(disabled.getMessage().contains("mux"));
    }

    @Test
    void rejectsEveryInvalidHandshakeAndCrossBudgetDimension() throws Exception {
        assertInvalid("rowweave.shuffle.maxConcurrentHandshakes", "1");
        assertInvalid("rowweave.shuffle.maxConcurrentHandshakes", "33");
        assertInvalid("rowweave.shuffle.handshakeTimeoutMs", "999");
        assertInvalid("rowweave.shuffle.handshakeTimeoutMs", "60001");
        assertInvalid("rowweave.shuffle.connectTimeoutMs", "999");
        assertInvalid("rowweave.shuffle.connectTimeoutMs", "60001");
        assertInvalid("rowweave.shuffle.handshakeResidentBytes", String.valueOf(8L * MIB - 1));
        assertInvalid("rowweave.shuffle.handshakeResidentBytes", String.valueOf(HEAP / 4 + 1));
        assertInvalid("rowweave.shuffle.authFrameBytes", "2048");
        assertInvalid("rowweave.shuffle.securityProfile", "LOOPBACK");

        Properties tooManyHandshakes = new Properties();
        tooManyHandshakes.setProperty("rowweave.shuffle.maxConnections", "3");
        tooManyHandshakes.setProperty("rowweave.shuffle.maxConcurrentHandshakes", "4");
        assertThrows(IllegalArgumentException.class,
                () -> config(tooManyHandshakes, ControlSecurityProfile.MTLS,
                        InetAddress.getByName("192.0.2.10")));

        Properties insufficientCharge = new Properties();
        insufficientCharge.setProperty("rowweave.shuffle.maxConcurrentHandshakes", "4");
        insufficientCharge.setProperty("rowweave.shuffle.handshakeResidentBytes",
                String.valueOf(832L * MIB - 1L));
        assertThrows(IllegalArgumentException.class,
                () -> config(insufficientCharge, ControlSecurityProfile.MTLS,
                        InetAddress.getByName("192.0.2.10")));

        Properties exactCharge = new Properties();
        exactCharge.setProperty("rowweave.shuffle.maxConcurrentHandshakes", "4");
        exactCharge.setProperty("rowweave.shuffle.handshakeResidentBytes",
                String.valueOf(832L * MIB));
        assertEquals(832L * MIB,
                config(exactCharge, ControlSecurityProfile.MTLS,
                        InetAddress.getByName("192.0.2.10"))
                        .handshakeResidentBytes());

        Properties uncertifiedConcurrency = new Properties();
        uncertifiedConcurrency.setProperty(
                "rowweave.shuffle.maxConcurrentHandshakes", "5");
        assertThrows(IllegalArgumentException.class,
                () -> config(uncertifiedConcurrency, ControlSecurityProfile.MTLS,
                        InetAddress.getByName("192.0.2.10")));
        Properties loopbackMaximum = new Properties();
        loopbackMaximum.setProperty("rowweave.shuffle.maxConcurrentHandshakes", "32");
        assertEquals(32, config(loopbackMaximum,
                ControlSecurityProfile.LOOPBACK,
                InetAddress.getByName("127.0.0.1"))
                .maxConcurrentHandshakes());
    }

    @Test
    void mtlsIngressBudgetIncludesAllConnectionScratchPlusOneMaximumFrame()
            throws Exception {
        long required = ShuffleIngressAdmissionConfig
                .minimumMtlsV1ResidentBytes(256);
        Properties properties = new Properties();
        properties.setProperty("rowweave.shuffle.ingressResidentBytes",
                String.valueOf(required - 1L));
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> config(properties, ControlSecurityProfile.MTLS,
                        InetAddress.getByName("192.0.2.10")));
        assertTrue(failure.getMessage().contains("scratch"), failure.getMessage());

        properties.setProperty("rowweave.shuffle.ingressResidentBytes",
                String.valueOf(required));
        assertEquals(required, config(properties, ControlSecurityProfile.MTLS,
                InetAddress.getByName("192.0.2.10"))
                .ingressAdmission().ingressResidentBytes());
    }

    @Test
    void loopbackSnapshotRejectsNonLoopbackBindEvenWhenCallerWasWrong()
            throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> config(new Properties(), ControlSecurityProfile.LOOPBACK,
                        InetAddress.getByName("192.0.2.10")));
    }

    private static void assertInvalid(String key, String value) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        assertThrows(IllegalArgumentException.class,
                () -> config(properties, ControlSecurityProfile.LOOPBACK,
                        InetAddress.getByName("127.0.0.1")), key);
    }

    private static DataPlaneConfig config(Properties properties,
                                          ControlSecurityProfile profile,
                                          InetAddress bindAddress) {
        return DataPlaneConfig.fromResolvedControlForTesting(
                properties, profile, bindAddress, HEAP);
    }
}

