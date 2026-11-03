package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RpcAdmissionConfigTest {

    @Test
    void productionDefaultsMatchAdrHardBounds() {
        RpcAdmissionConfig config = RpcAdmissionConfig.fromProperties(new Properties());

        assertEquals(256, config.maxConnections());
        assertEquals(128, config.listenBacklog());
        assertEquals(30_000L, config.frameDeadlineMillis());
        assertEquals(1024 * 1024, config.normalFrameBytes());
        assertEquals(80 * 1024 * 1024, config.largeFrameBytes());
        assertEquals(128L * 1024 * 1024, config.inFlightFrameBytes());
    }

    @Test
    void invalidRangesAndBudgetRelationshipFailAtParseTime() {
        assertInvalid("rowweave.control.maxConnections", "0");
        assertInvalid("rowweave.control.listenBacklog", "4097");
        assertInvalid("rowweave.control.frameDeadlineMs", "999");
        assertInvalid("rowweave.control.inFlightFrameBytes", String.valueOf(79L * 1024 * 1024));
        assertInvalid("rowweave.control.inFlightFrameBytes", String.valueOf(1025L * 1024 * 1024));
    }

    private static void assertInvalid(String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        assertThrows(IllegalArgumentException.class,
                () -> RpcAdmissionConfig.fromProperties(properties), key);
    }
}

