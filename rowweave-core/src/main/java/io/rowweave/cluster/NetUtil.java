package io.rowweave.cluster;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * Detects the address of this host by walking the network interfaces, which is what a node
 * inside a container needs: the address advertised as reachable is reported by a worker when it
 * registers, and the master builds shuffle endpoints from it so that upstream instances connect
 * back to this worker.<p>Workers within one Docker or Kubernetes network reach each other over
 * their site-local IPv4 addresses, in the 172.x, 10.x and 192.168.x ranges, without anything
 * injected by the orchestrator. Assigning addresses by hand does not scale to dozens or
 * hundreds of workers, so detection is the default and {@code rowweave.worker.host} stays as an
 * explicit override, for a multi-homed host, for NAT, and for the case where detection guesses
 * wrong.<p>Detection order, highest priority first:<ol><li>A site-local IPv4 on an interface
 * that is up, is not loopback and is not virtual, where virtual means docker, bridge, vmenet,
 * veth, utun or a VPN, excluded by name prefix and by {@link NetworkInterface#isVirtual()} and
 * {@code isPointToPoint()}.</li><li>Failing that, the first non-loopback IPv4, that is a public
 * address.</li><li>Failing that, {@link InetAddress#getLocalHost()}.</li><li>Failing
 * everything, {@code 127.0.0.1}, so the single machine case never
 * regresses.</li></ol><p>Virtual interfaces are filtered because their addresses also report
 * true for isSiteLocalAddress and may be enumerated before the physical card, which would
 * advertise a container network address that another host cannot reach. For local tests set
 * {@code rowweave.worker.host=127.0.0.1} explicitly rather than relying on detection, since the
 * interfaces of a development machine are not controlled; detection is a fallback for
 * production and containers.
 */
public final class NetUtil {

    public static final String LOOPBACK = "127.0.0.1";

    private NetUtil() {
    }

    /**
     * The address a worker advertises: an explicit {@code rowweave.worker.host} wins, and
     * otherwise the host is detected with {@link #detectLocalHost()}. Deciding once at
     * production start-up is enough, because detection walks the interfaces and this is not the
     * hot path.
     */
    public static String advertisedHost() {
        String explicit = System.getProperty("rowweave.worker.host");
        if (explicit != null && !explicit.trim().isEmpty()) {
            return explicit.trim();
        }
        return detectLocalHost();
    }

    /** Walks the interfaces for an IPv4 address of this host, falling back in stages when nothing usable is found, as the class note describes. */
    public static String detectLocalHost() {
        String siteLocal = null;
        String anyNonLoopback = null;
        try {
            Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
            while (nics != null && nics.hasMoreElements()) {
                NetworkInterface nic = nics.nextElement();
                if (!isUsable(nic)) {
                    continue;
                }
                Enumeration<InetAddress> addrs = nic.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    InetAddress addr = addrs.nextElement();
                    if (!(addr instanceof Inet4Address) || addr.isLoopbackAddress()) {
                        continue;  // IPv4 only, and not loopback: an IPv6 link-local address carries a scope id, which makes a poor advertised host
                    }
                    if (addr.isSiteLocalAddress() && siteLocal == null) {
                        siteLocal = addr.getHostAddress();  // best case: a site-local private address, which reaches across a container or intranet
                    }
                    if (anyNonLoopback == null) {
                        anyNonLoopback = addr.getHostAddress();
                    }
                }
            }
        } catch (Exception ignored) {
            // Walking the interfaces failed, so fall back to getLocalHost.
        }
        if (siteLocal != null) {
            return siteLocal;
        }
        if (anyNonLoopback != null) {
            return anyNonLoopback;
        }
        try {
            InetAddress local = InetAddress.getLocalHost();
            if (local != null && !local.isLoopbackAddress()) {
                return local.getHostAddress();
            }
        } catch (Exception ignored) {
            // Fall back to loopback.
        }
        return LOOPBACK;
    }

    /** Usable interface: up, not loopback, and not virtual, since a docker, bridge, VM or VPN address is not reachable from outside. */
    private static boolean isUsable(NetworkInterface nic) {
        try {
            if (!nic.isUp() || nic.isLoopback() || nic.isPointToPoint() || nic.isVirtual()) {
                return false;
            }
            return !isVirtualName(nic.getName()) && !isVirtualName(nic.getDisplayName());
        } catch (Exception e) {
            return false;
        }
    }

    /** Name prefixes of known virtual and tunnel interfaces, whose addresses are not fit to advertise. */
    static boolean isVirtualName(String name) {
        if (name == null) {
            return false;
        }
        String n = name.toLowerCase();
        // macOS: bridge/vmenet/utun/gif/stf/awdl/llw/ap/anpi;Linux: docker/br-/veth/virbr/tun/tap/vmnet/vboxnet/cni/flannel
        return n.startsWith("docker") || n.startsWith("br-") || n.startsWith("bridge")
                || n.startsWith("veth") || n.startsWith("virbr") || n.startsWith("vmnet")
                || n.startsWith("vmenet") || n.startsWith("vboxnet") || n.startsWith("cni")
                || n.startsWith("flannel") || n.startsWith("tun") || n.startsWith("tap")
                || n.startsWith("utun") || n.startsWith("gif") || n.startsWith("stf")
                || n.startsWith("awdl") || n.startsWith("llw") || n.startsWith("ap")
                || n.startsWith("anpi") || n.startsWith("zt") || n.startsWith("tailscale")
                || n.contains("virtual") || n.contains("vpn");
    }
}
