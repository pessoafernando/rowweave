package io.rowweave.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransportSecurityBootstrapForkTest {

    @Test
    void productionBootstrapIsNotPublicAndDoesNotRetainAPropertyBag() throws Exception {
        assertTrue(!Modifier.isPublic(TransportSecurityBootstrap.class
                .getDeclaredMethod("bootstrap").getModifiers()));
        assertTrue(!Modifier.isPublic(TransportSecurityBootstrap.class
                .getDeclaredMethod("bootstrapProcess").getModifiers()));
        for (java.lang.reflect.Field field : TransportSecurityBootstrap.class
                .getDeclaredFields()) {
            assertTrue(field.getType() != Properties.class, field.toGenericString());
        }
        for (java.lang.reflect.Field field : ProcessTlsEnvelope.class.getDeclaredFields()) {
            assertTrue(field.getType() != Properties.class, field.toGenericString());
            assertTrue(!field.getName().toLowerCase(java.util.Locale.ROOT)
                    .contains("password"), field.toGenericString());
        }
        for (java.lang.reflect.Field field : ProcessBootstrapEnvelope.class.getDeclaredFields()) {
            assertTrue(field.getType() != Properties.class, field.toGenericString());
        }
        for (java.lang.reflect.Field field : ClusterMain.class.getDeclaredFields()) {
            if (field.isSynthetic()) continue;
            assertTrue(!Modifier.isStatic(field.getModifiers()),
                    "ClusterMain must not initialize objects before main/bootstrap: "
                            + field.toGenericString());
        }
    }

    @Test
    @Timeout(30)
    void currentExactBuildAcceptsDefaultAndConfiguredBoundaryEnvelopes() throws Exception {
        ChildResult defaults = launch("--require-mtls-proof");
        assertEquals(0, defaults.exitCode, defaults.output);
        assertTrue(defaults.output.contains("CERTIFIED"), defaults.output);
        assertTrue(defaults.output.contains("handshake=absent"), defaults.output);
        assertTrue(defaults.output.contains("chain=absent"), defaults.output);

        ChildResult boundary = launch(
                "-Djdk.tls.maxHandshakeMessageSize=65536",
                "-Djdk.tls.maxCertificateChainLength=10",
                "--require-mtls-proof");
        assertEquals(0, boundary.exitCode, boundary.output);
        assertTrue(boundary.output.contains("handshake=65536"), boundary.output);
        assertTrue(boundary.output.contains("chain=10"), boundary.output);
    }

    @Test
    @Timeout(30)
    void invalidStartupEnvelopeAndUnknownBuildFailClosed() throws Exception {
        ChildResult oversized = launch(
                "-Djdk.tls.maxHandshakeMessageSize=65537",
                "--require-mtls-proof");
        assertTrue(oversized.exitCode != 0, oversized.output);
        assertTrue(oversized.output.contains("TLS_BOOTSTRAP_REJECTED"), oversized.output);

        ChildResult unknownBuild = launch(
                "--mutate-build-before-bootstrap", "--require-mtls-proof");
        assertTrue(unknownBuild.exitCode != 0, unknownBuild.output);
        assertTrue(unknownBuild.output.contains("TLS_BOOTSTRAP_REJECTED"), unknownBuild.output);

        ChildResult rejectedBeforeTlsMaterial = launch(
                "--mutate-build-before-bootstrap", "--load-certified-config");
        assertTrue(rejectedBeforeTlsMaterial.exitCode != 0,
                rejectedBeforeTlsMaterial.output);
        assertTrue(rejectedBeforeTlsMaterial.output.contains(
                "uncertified JDK/JSSE/Netty build"), rejectedBeforeTlsMaterial.output);

        ChildResult runtimeInjectedBoundary = launch(
                "--mutate-tls-before-bootstrap", "--require-mtls-proof");
        assertTrue(runtimeInjectedBoundary.exitCode != 0, runtimeInjectedBoundary.output);
        assertTrue(runtimeInjectedBoundary.output.contains("TLS_BOOTSTRAP_REJECTED"),
                runtimeInjectedBoundary.output);

        ChildResult nettyAllocatorOverride = launch(
                "-Dio.netty.allocator.type=pooled", "--require-mtls-proof");
        assertTrue(nettyAllocatorOverride.exitCode != 0,
                nettyAllocatorOverride.output);
        assertTrue(nettyAllocatorOverride.output.contains(
                "unsupported io.netty.* startup property"),
                nettyAllocatorOverride.output);
    }

    @Test
    @Timeout(30)
    void bootstrapIsUniqueAndRuntimePropertyMutationOnlyCreatesDrift() throws Exception {
        ChildResult result = launch(
                "--mutate-after-bootstrap", "--require-mtls-proof");
        assertEquals(0, result.exitCode, result.output);
        assertTrue(result.output.contains("same=true"), result.output);
        assertTrue(result.output.contains("DRIFT_REJECTED"), result.output);
        assertTrue(result.output.contains("CONFIG_DRIFT_REJECTED"), result.output);

        ChildResult nettyDrift = launch(
                "--mutate-netty-after-bootstrap", "--require-mtls-proof");
        assertTrue(nettyDrift.exitCode != 0, nettyDrift.output);
        assertTrue(nettyDrift.output.contains(
                "unsupported io.netty.* runtime property"), nettyDrift.output);
    }

    @Test
    @Timeout(30)
    void valueEquivalentEnvelopeFromAnotherSnapshotIsRejected() throws Exception {
        ChildResult result = launch("--different-proof", "--require-mtls-proof");
        assertEquals(0, result.exitCode, result.output);
        assertTrue(result.output.contains("FOREIGN_PROOF_REJECTED"), result.output);
    }

    @Test
    @Timeout(30)
    void loopbackBootstrapDoesNotClaimOrRequireMtlsBuildCertification() throws Exception {
        ChildResult result = launch("--mutate-build-before-bootstrap");
        assertEquals(0, result.exitCode, result.output);
        assertTrue(result.output.contains("CAPTURED"), result.output);
        assertTrue(!result.output.contains("CERTIFIED"), result.output);
    }

    @Test
    @Timeout(30)
    void processBootstrapCapturesCapacityFromTheSameFrozenConfiguration() throws Exception {
        ChildResult result = launch("--process-capacity");
        assertEquals(0, result.exitCode, result.output);
        assertTrue(result.output.contains("PROCESS_CAPACITY=268435456"), result.output);
        assertTrue(result.output.contains("PROCESS_CONFIG_DRIFT_REJECTED"), result.output);
    }

    private static ChildResult launch(String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/java").getAbsolutePath());
        command.add("-Xms64m");
        command.add("-Xmx256m");
        command.add("-XX:MaxDirectMemorySize=64m");
        command.add("-XX:MaxMetaspaceSize=128m");
        command.add("-XX:ReservedCodeCacheSize=64m");
        command.add("-Xss1m");
        command.add("-XX:ActiveProcessorCount=2");
        command.add("-Drowweave.process.memoryLimitBytes=2147483648");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        for (String argument : arguments) {
            if (argument.startsWith("-D")) command.add(argument);
        }
        command.add(Probe.class.getName());
        for (String argument : arguments) {
            if (!argument.startsWith("-D")) command.add(argument);
        }
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        boolean exited = process.waitFor(10, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
        }
        assertTrue(exited, "bootstrap child did not exit");
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (InputStream input = process.getInputStream()) {
            byte[] buffer = new byte[1024];
            for (int read; (read = input.read(buffer)) >= 0; ) {
                output.write(buffer, 0, read);
            }
        }
        return new ChildResult(process.exitValue(), output.toString("UTF-8"));
    }

    public static final class Probe {
        private Probe() {
        }

        public static void main(String[] args) {
            try {
                if (contains(args, "--mutate-build-before-bootstrap")) {
                    System.setProperty("java.runtime.version", "unsupported-test-build");
                }
                if (contains(args, "--mutate-tls-before-bootstrap")) {
                    System.setProperty("jdk.tls.maxHandshakeMessageSize", "65536");
                }
                if (contains(args, "--process-capacity")) {
                    ProcessBootstrapEnvelope process =
                            TransportSecurityBootstrap.bootstrapProcess();
                    System.out.println("PROCESS_CAPACITY="
                            + process.capacityEnvelope().maxHeapBytes());
                    System.setProperty("rowweave.worker.slots", "7");
                    try {
                        ClusterConfig.fromSystemProperties(process);
                        throw new AssertionError(
                                "process configuration drift was accepted");
                    } catch (IllegalStateException expected) {
                        System.out.println("PROCESS_CONFIG_DRIFT_REJECTED");
                    }
                    return;
                }
                ProcessTlsEnvelope first = TransportSecurityBootstrap.bootstrap();
                ProcessTlsEnvelope second = TransportSecurityBootstrap.bootstrap();
                if (contains(args, "--mutate-netty-after-bootstrap")) {
                    System.setProperty("io.netty.noPreferDirect", "true");
                }
                if (contains(args, "--require-mtls-proof")) {
                    first.requireCertifiedForMtls();
                }
                if (contains(args, "--load-certified-config")) {
                    Properties properties = new Properties();
                    properties.putAll(System.getProperties());
                    properties.setProperty("rowweave.control.securityProfile", "MTLS");
                    io.rowweave.cluster.rpc.ControlSecurityConfig.fromProperties(
                            properties, first);
                }
                System.out.println((contains(args, "--require-mtls-proof")
                        ? "CERTIFIED" : "CAPTURED") + " vendor=" + first.javaVendor()
                        + " runtime=" + first.javaRuntimeVersion()
                        + " provider=" + first.providerName()
                        + " handshake=" + optional(first.configuredMaxHandshakeMessageBytes())
                        + " chain=" + optional(first.configuredMaxCertificateChainLength()));
                System.out.println("same=" + (first == second));
                if (contains(args, "--different-proof")) {
                    ProcessTlsEnvelope foreign = new ProcessTlsEnvelope(
                            System.getProperty("java.vendor"),
                            System.getProperty("java.runtime.version"),
                            System.getProperty("java.vm.name"),
                            java.security.Security.getProvider("SunJSSE"),
                            System.getProperty("jdk.tls.maxHandshakeMessageSize"),
                            System.getProperty("jdk.tls.maxCertificateChainLength"));
                    try {
                        Properties mtls = new Properties();
                        mtls.putAll(System.getProperties());
                        mtls.setProperty("rowweave.control.securityProfile", "MTLS");
                        io.rowweave.cluster.rpc.ControlSecurityConfig.fromProperties(
                                mtls, foreign);
                        throw new AssertionError("value-equivalent foreign proof was accepted");
                    } catch (IllegalStateException expected) {
                        System.out.println("FOREIGN_PROOF_REJECTED");
                    }
                }
                if (contains(args, "--mutate-after-bootstrap")) {
                    System.setProperty("jdk.tls.maxHandshakeMessageSize", "65536");
                    System.setProperty("rowweave.shuffle.maxConcurrentHandshakes", "32");
                    try {
                        first.requireCurrentProcessMatch();
                        throw new AssertionError("runtime TLS property drift was accepted");
                    } catch (IllegalStateException expected) {
                        System.out.println("DRIFT_REJECTED");
                    }
                    try {
                        first.requireConfigurationMatch(System.getProperties());
                        throw new AssertionError("runtime RowWeave configuration drift was accepted");
                    } catch (IllegalStateException expected) {
                        System.out.println("CONFIG_DRIFT_REJECTED");
                    }
                } else {
                    first.requireCurrentProcessMatch();
                }
            } catch (Throwable rejected) {
                System.out.println("TLS_BOOTSTRAP_REJECTED " + rejected.getClass().getSimpleName()
                        + ":" + rejected.getMessage());
                System.exit(19);
            }
        }

        private static boolean contains(String[] args, String expected) {
            for (String arg : args) if (expected.equals(arg)) return true;
            return false;
        }

        private static String optional(java.util.OptionalInt value) {
            return value.isPresent() ? Integer.toString(value.getAsInt()) : "absent";
        }
    }

    private static final class ChildResult {
        private final int exitCode;
        private final String output;

        private ChildResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}

