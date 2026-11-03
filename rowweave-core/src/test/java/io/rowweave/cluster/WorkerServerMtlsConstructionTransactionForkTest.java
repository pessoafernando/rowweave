package io.rowweave.cluster;

import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerServerMtlsConstructionTransactionForkTest {

    private static final String PASSWORD = "changeit";

    @Test
    void mtlsDataAuthorizationFailureRollsBackAndReleasesGuard(
            @TempDir Path tmp) throws Exception {
        Path keyStore = tmp.resolve("combined.p12");
        Path certificate = tmp.resolve("combined.cer");
        Path trustStore = tmp.resolve("trust.p12");
        keytool("-genkeypair", "-alias", "identity", "-keyalg", "RSA",
                "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-dname", "CN=combined", "-validity", "30",
                "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth");
        keytool("-exportcert", "-alias", "identity",
                "-keystore", keyStore.toString(), "-storepass", PASSWORD,
                "-file", certificate.toString());
        keytool("-importcert", "-noprompt", "-alias", "combined",
                "-file", certificate.toString(), "-storetype", "PKCS12",
                "-keystore", trustStore.toString(), "-storepass", PASSWORD);
        Path authz = tmp.resolve("authz.txt");
        Files.write(authz, (fingerprint(certificate) + "=MASTER,WORKER\n")
                .getBytes(StandardCharsets.US_ASCII));

        List<String> command = ForkedJvm.baseCommand();
        ForkedJvm.appendJvmArguments(command,
                java.util.Collections.singletonList("-Xmx4096m"));
        command.add("-Drowweave.control.securityProfile=MTLS");
        command.add("-Drowweave.control.host=127.0.0.1");
        command.add("-Drowweave.control.tls.keyStore=" + keyStore);
        command.add("-Drowweave.control.tls.keyStorePassword=" + PASSWORD);
        command.add("-Drowweave.control.tls.trustStore=" + trustStore);
        command.add("-Drowweave.control.tls.trustStorePassword=" + PASSWORD);
        command.add("-Drowweave.control.tls.authzFile=" + authz);
        command.add("-Drowweave.control.residentBytes=234881024");
        command.add("-Drowweave.deployment.memoryBytes=8388608");
        command.add("-Drowweave.shuffle.ingressResidentBytes=100663296");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(Probe.class.getName());

        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        boolean exited = process.waitFor(30L, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            process.waitFor(5L, TimeUnit.SECONDS);
        }
        byte[] output;
        try (InputStream input = process.getInputStream()) {
            output = readAll(input);
        }
        assertTrue(exited, "MTLS construction probe did not exit");
        String text = new String(output, StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), text);
        assertTrue(text.contains("WORKER_MTLS_CONSTRUCTION_OK"), text);
    }

    public static final class Probe {
        private Probe() {
        }

        public static void main(String[] args) {
            Properties properties = new Properties();
            properties.putAll(System.getProperties());
            ProcessTlsEnvelope proof = TransportSecurityTestAccess.bootstrap();
            proof.requireCertifiedForMtls();
            ControlSecurityConfig control =
                    ControlSecurityConfig.fromProperties(properties, proof);
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                    properties, control, Runtime.getRuntime().maxMemory(), proof)) {
                for (int attempt = 0; attempt < 2; attempt++) {
                    Error primary = new AssertionError("injected-data-authz-" + attempt);
                    try {
                        new WorkerServer(
                                "mtls-construction-" + attempt, 1,
                                new FixedMasterLocator(new Endpoint("127.0.0.1", 1)),
                                new java.io.File(System.getProperty("java.io.tmpdir")),
                                true, WorkerServer.RuntimeSource.borrowed(runtime), 2,
                                step -> {
                                    if (step == WorkerConstructionStep.DATA_AUTHZ) throw primary;
                                });
                        throw new AssertionError("DATA_AUTHZ probe was not reached");
                    } catch (Error observed) {
                        if (observed != primary) throw observed;
                    }
                    runtime.requireAdmissionOpen();
                }
            }
            System.out.println("WORKER_MTLS_CONSTRUCTION_OK");
        }
    }

    private static String fingerprint(Path certificate) throws Exception {
        X509Certificate leaf;
        try (InputStream input = Files.newInputStream(certificate)) {
            leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(leaf.getEncoded());
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte value : digest) hex.append(String.format("%02x", value & 0xff));
        return hex.toString();
    }

    private static void keytool(String... arguments) throws Exception {
        List<String> command = new ArrayList<>(ForkedJvm.keytoolCommand(arguments));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output;
        try (InputStream input = process.getInputStream()) {
            output = readAll(input);
        }
        int exit = process.waitFor();
        if (exit != 0) {
            throw new AssertionError("keytool failed: "
                    + new String(output, StandardCharsets.UTF_8));
        }
    }

    private static byte[] readAll(InputStream input) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        for (int read; (read = input.read(buffer)) >= 0; ) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }
}
