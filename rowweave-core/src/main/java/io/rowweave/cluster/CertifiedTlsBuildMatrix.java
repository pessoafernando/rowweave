package io.rowweave.cluster;

/** Only an exact JDK and JSSE build whose resource profile has been measured item by item may enter the production matrix. */
final class CertifiedTlsBuildMatrix {

    private static final String JAVA_VM_NAME = "OpenJDK 64-Bit Server VM";
    private static final String PROVIDER_NAME = "SunJSSE";
    private static final String PROVIDER_CLASS = "sun.security.ssl.SunJSSE";
    private static final String PROVIDER_VERSION = "17.0";
    private static final String NETTY_VERSION = "4.1.139.Final";
    private static final String[] NETTY_ARTIFACTS = {
            "netty-common", "netty-buffer", "netty-transport",
            "netty-codec", "netty-handler"
    };

    private CertifiedTlsBuildMatrix() {
    }

    static void requireCertified(ProcessTlsEnvelope envelope) {
        if (!hasCertifiedJavaBuild(envelope,
                System.getProperty("os.name"), System.getProperty("os.arch"))
                || !JAVA_VM_NAME.equals(envelope.javaVmName())
                || !PROVIDER_NAME.equals(envelope.providerName())
                || !PROVIDER_CLASS.equals(envelope.providerClassName())
                || !PROVIDER_VERSION.equals(envelope.providerVersion())
                || !hasCertifiedNettyArtifacts()) {
            throw new IllegalStateException(
                    "uncertified JDK/JSSE/Netty build for MTLS data plane");
        }
    }

    static boolean hasCertifiedJavaBuild(ProcessTlsEnvelope envelope,
            String operatingSystem, String architecture) {
        // Exact tuples require both handshake and ingress gates.
        return ("Homebrew".equals(envelope.javaVendor())
                && "17.0.15+0".equals(envelope.javaRuntimeVersion())
                && "Mac OS X".equals(operatingSystem)
                && "aarch64".equals(architecture))
                || ("Eclipse Adoptium".equals(envelope.javaVendor())
                && "17.0.20+8".equals(envelope.javaRuntimeVersion())
                && ("Linux".equals(operatingSystem)
                    || "Mac OS X".equals(operatingSystem))
                && "aarch64".equals(architecture));
    }

    private static boolean hasCertifiedNettyArtifacts() {
        java.util.Map<String, io.netty.util.Version> versions =
                io.netty.util.Version.identify();
        for (String artifact : NETTY_ARTIFACTS) {
            io.netty.util.Version version = versions.get(artifact);
            if (version == null || !NETTY_VERSION.equals(version.artifactVersion())) {
                return false;
            }
        }
        return true;
    }
}
