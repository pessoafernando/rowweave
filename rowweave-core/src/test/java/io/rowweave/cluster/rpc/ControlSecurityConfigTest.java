package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlSecurityConfigTest {

    @Test
    void emptyPropertiesSelectClosedLoopbackProfile() {
        ControlSecurityConfig config = ControlSecurityConfig.fromProperties(new Properties());

        assertEquals(ControlSecurityProfile.LOOPBACK, config.profile());
        assertEquals("127.0.0.1", config.advertisedHost());
        assertTrue(config.bindAddress().isLoopbackAddress());
    }

    @Test
    void loopbackRejectsWildcardAndNonLoopbackAdvertisedHosts() {
        assertInvalidLoopbackHost("0.0.0.0");
        assertInvalidLoopbackHost("192.0.2.10");
    }

    @Test
    void loopbackTargetResolutionRejectsNonLoopbackBeforeConnect() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ControlEndpointPolicy.resolveTarget(
                        ControlSecurityProfile.LOOPBACK, "192.0.2.10"));

        assertTrue(failure.getMessage().contains("LOOPBACK"), failure.getMessage());
    }

    @Test
    void loopbackTargetResolutionPinsResolvedAddress() throws Exception {
        InetAddress address = ControlEndpointPolicy.resolveTarget(
                ControlSecurityProfile.LOOPBACK, "localhost");

        assertTrue(address.isLoopbackAddress(), address.toString());
    }

    @Test
    void unknownProfileFailsClosed() {
        Properties properties = new Properties();
        properties.setProperty("rowweave.control.securityProfile", "optional-tls");

        assertThrows(IllegalArgumentException.class,
                () -> ControlSecurityConfig.fromProperties(properties));
    }

    @Test
    void mtlsProfileRequiresCompleteMaterialDuringConfigLoad() {
        Properties properties = new Properties();
        properties.setProperty("rowweave.control.securityProfile", "MTLS");
        properties.setProperty("rowweave.control.host", "127.0.0.1");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ControlSecurityConfig.fromProperties(properties));
        assertTrue(failure.getMessage().contains("keyStore"), failure.getMessage());
    }

    @Test
    void mtlsRejectsUnreadableMaterialBeforeAnySocketIsCreated() {
        Properties properties = new Properties();
        properties.setProperty("rowweave.control.securityProfile", "MTLS");
        properties.setProperty("rowweave.control.host", "127.0.0.1");
        properties.setProperty("rowweave.control.tls.keyStore", "/missing/key.p12");
        properties.setProperty("rowweave.control.tls.keyStorePassword", "secret");
        properties.setProperty("rowweave.control.tls.trustStore", "/missing/trust.p12");
        properties.setProperty("rowweave.control.tls.trustStorePassword", "secret");
        properties.setProperty("rowweave.control.tls.authzFile", "/missing/authz.txt");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ControlSecurityConfig.fromProperties(properties));
        assertTrue(failure.getMessage().contains("MTLS material"), failure.getMessage());
    }

    private static void assertInvalidLoopbackHost(String host) {
        Properties properties = new Properties();
        properties.setProperty("rowweave.control.securityProfile", "LOOPBACK");
        properties.setProperty("rowweave.control.host", host);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ControlSecurityConfig.fromProperties(properties));
        assertTrue(failure.getMessage().contains("LOOPBACK"), failure.getMessage());
    }
}

