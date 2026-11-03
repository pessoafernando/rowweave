package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MultiSocketChannelSource binds a master-preferred port when one is given and falls back to
 * binding port 0, an operating-system choice, when the preferred port is taken, so the job does
 * not fail: pre-allocating a port is an optimization, and a conflict degrades automatically.
 * port() always returns the port actually bound, which is the preferred one when that succeeded
 * and the operating-system one after a fallback, which is what PORT_REPORT sends back.
 */
class PreferredPortBindTest {

    private static final DataSchema META = new DataSchema(new Field("k", DataType.IntegerType));

/** preferredPort=0, the default, lets the operating system choose, and port() returns the actual port above zero. */
    @Test
    void preferredPortZeroBindsRandom() {
        try (MultiSocketChannelSource src = new MultiSocketChannelSource(
                META, 1, 0, 64, 5000,
                SocketChannelSink.KEEPALIVE_DISABLED, SocketChannelSink.KEEPALIVE_DISABLED, 0)) {
            assertTrue(src.port() > 0, "binding port 0 gives an OS-chosen port");
        }
    }

/** A free preferredPort is bound exactly, so port() equals it. */
    @Test
    void preferredPortFreeBindsToIt() throws Exception {
        int free = freePort();
        try (MultiSocketChannelSource src = new MultiSocketChannelSource(
                META, 1, 0, 64, 5000,
                SocketChannelSink.KEEPALIVE_DISABLED, SocketChannelSink.KEEPALIVE_DISABLED, free)) {
            assertEquals(free, src.port(), "a free preferred port is bound directly, which skips the PORT_REPORT round trip");
        }
    }

/** A taken preferredPort falls back to an operating-system port without throwing, and port() returns the actual fallback port, which differs from the occupied one. */
    @Test
    void preferredPortTakenFallsBackToRandom() throws Exception {
        // Occupy the wildcard 0.0.0.0, which matches the production bind and makes the conflict detectable; on some
        // operating systems a loopback-specific bind does not conflict with a wildcard bind, so the fallback would not trigger.
        try (ServerSocket occupy = new ServerSocket()) {
            occupy.bind(new InetSocketAddress((java.net.InetAddress) null, 0));  // 0.0.0.0 with a random port
            int taken = occupy.getLocalPort();
            try (MultiSocketChannelSource src = new MultiSocketChannelSource(
                    META, 1, 0, 64, 5000,
                    SocketChannelSink.KEEPALIVE_DISABLED, SocketChannelSink.KEEPALIVE_DISABLED, taken)) {
                int actual = src.port();
                assertTrue(actual > 0, "falling back to port 0 succeeds");
                assertNotEquals(taken, actual, "a taken preferred port falls back to a different one instead of failing the job");
            }
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket()) {
            s.bind(new InetSocketAddress((java.net.InetAddress) null, 0));  // 0.0.0.0 with a random port, then closed so the port is free
            return s.getLocalPort();
        }
    }
}
