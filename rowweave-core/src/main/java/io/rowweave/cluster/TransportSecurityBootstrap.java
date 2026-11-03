package io.rowweave.cluster;

import java.security.Provider;
import java.security.Security;
import java.util.Properties;

/**
 * The single process bootstrap that ClusterMain calls before any RowWeave TLS configuration class. The
 * certified boundary requires a dedicated ClusterMain process carrying no agent or premain that would
 * initialize JSSE before main: stock JSSE cannot prove whether a third-party premain read the default
 * SSLContext, so such a deployment is outside the certified matrix.
 */
public final class TransportSecurityBootstrap {

    private static final Object LOCK = new Object();
    private static volatile ProcessTlsEnvelope envelope;
    private static volatile ProcessBootstrapEnvelope processEnvelope;

    private TransportSecurityBootstrap() {
    }

    static ProcessTlsEnvelope bootstrap() {
        ProcessTlsEnvelope current = envelope;
        if (current != null) return current;
        synchronized (LOCK) {
            current = envelope;
            if (current == null) {
                Properties frozen = new Properties();
                frozen.putAll(System.getProperties());
                current = tlsEnvelope(frozen);
                envelope = current;
            }
            return current;
        }
    }

    /** ClusterMain-only atomic bootstrap: freezes one property snapshot and all JVM caps. */
    static ProcessBootstrapEnvelope bootstrapProcess() {
        ProcessBootstrapEnvelope current = processEnvelope;
        if (current != null) return current;
        synchronized (LOCK) {
            current = processEnvelope;
            if (current == null) {
                if (envelope != null) {
                    throw new ProcessCapacityException(
                            ProcessCapacityException.Code.CERTIFICATION,
                            "process capacity was not captured by the earliest bootstrap");
                }
                Properties frozen = new Properties();
                frozen.putAll(System.getProperties());
                ProcessTlsEnvelope tls = tlsEnvelope(frozen);
                JvmCapacityEnvelope capacity = JvmCapacityEnvelope.from(
                        java.lang.management.ManagementFactory.getRuntimeMXBean()
                                .getInputArguments(),
                        frozen);
                current = new ProcessBootstrapEnvelope(tls, capacity);
                envelope = tls;
                processEnvelope = current;
            }
            return current;
        }
    }

    /** Returns the bootstrap already done for the process, and never quietly performs one at a call site that has arrived too late. */
    static ProcessTlsEnvelope current() {
        return envelope;
    }

    private static ProcessTlsEnvelope tlsEnvelope(Properties frozen) {
        String vendor = frozen.getProperty("java.vendor");
        String runtimeVersion = frozen.getProperty("java.runtime.version");
        String vmName = frozen.getProperty("java.vm.name");
        String handshakeLimit = frozen.getProperty(
                ProcessTlsEnvelope.HANDSHAKE_LIMIT_PROPERTY);
        String chainLimit = frozen.getProperty(
                ProcessTlsEnvelope.CHAIN_LIMIT_PROPERTY);
        Provider provider = Security.getProvider("SunJSSE");
        return new ProcessTlsEnvelope(vendor, runtimeVersion, vmName, provider,
                handshakeLimit, chainLimit, frozen);
    }

}
