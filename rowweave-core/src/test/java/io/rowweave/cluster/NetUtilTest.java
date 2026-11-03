package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;

import static org.junit.jupiter.api.Assertions.*;

class NetUtilTest {

    @Test
    void detectReturnsUsableAddress() {
        String host = NetUtil.detectLocalHost();
        assertNotNull(host);
        assertFalse(host.isEmpty(), "the host is not empty");
        assertDoesNotThrow(() -> InetAddress.getByName(host), "the value resolves to a valid address");
    }

    @Test
    void detectIsStable() {
        assertEquals(NetUtil.detectLocalHost(), NetUtil.detectLocalHost(), "detection is stable within one process");
    }

    @Test
    void detectNotWildcard() {
        String host = NetUtil.detectLocalHost();
        assertNotEquals("0.0.0.0", host, "no wildcard address (unusable as an advertised host)");
    }

    @Test
    void explicitOverrideWins() {
        try {
            System.setProperty("rowweave.worker.host", "10.9.8.7");
            assertEquals("10.9.8.7", NetUtil.advertisedHost(), "explicit configuration overrides detection");
        } finally {
            System.clearProperty("rowweave.worker.host");
        }
    }

    @Test
    void unsetUsesDetection() {
        System.clearProperty("rowweave.worker.host");
        assertEquals(NetUtil.detectLocalHost(), NetUtil.advertisedHost(),
                "an unset value falls back to detection");
    }

    @Test
    void virtualInterfaceNamesFiltered() {
        for (String v : new String[]{"docker0", "br-abc123", "bridge101", "vmenet0", "veth1234",
                "utun0", "virbr0", "vboxnet0", "tun0", "tap0", "gif0", "awdl0", "ztabc", "tailscale0"}) {
            assertTrue(NetUtil.isVirtualName(v), v + " is a virtual interface (its address is not eligible)");
        }
        for (String p : new String[]{"en0", "eth0", "eth1", "enp3s0", "wlan0", "ens192"}) {
            assertFalse(NetUtil.isVirtualName(p), p + " is a physical interface (its address may be advertised)");
        }
    }

    @Test
    void detectPrefersPhysicalOverBridge() {
        String host = NetUtil.detectLocalHost();
        assertNotEquals("172.16.30.1", host, "a Docker or VM bridge address must not be selected");
    }
}

