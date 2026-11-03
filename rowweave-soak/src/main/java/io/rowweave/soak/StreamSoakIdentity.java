package io.rowweave.soak;

import io.rowweave.cluster.rpc.ControlSecurityConfig;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

final class StreamSoakIdentity {
    static final String PASSWORD = "changeit";

    final Path keyStore;
    final Path certificate;
    final String fingerprint;

    private StreamSoakIdentity(Path keyStore, Path certificate,
            String fingerprint) {
        this.keyStore = keyStore;
        this.certificate = certificate;
        this.fingerprint = fingerprint;
    }

    static StreamSoakIdentity create(Path root, String name, String host)
            throws Exception {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("identity host is required");
        }
        Path keyStore = root.resolve(name + ".p12");
        Path certificate = root.resolve(name + ".cer");
        keytool("-genkeypair", "-alias", "identity", "-keyalg", "RSA",
                "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-dname", "CN=" + name, "-validity", "30",
                "-ext", "SAN=ip:" + host + ",ip:127.0.0.1,dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth");
        keytool("-exportcert", "-alias", "identity", "-keystore",
                keyStore.toString(), "-storepass", PASSWORD,
                "-file", certificate.toString());
        X509Certificate leaf;
        try (InputStream input = Files.newInputStream(certificate)) {
            leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
        return new StreamSoakIdentity(keyStore, certificate,
                hex(MessageDigest.getInstance("SHA-256")
                        .digest(leaf.getEncoded())));
    }

    static void importCertificate(Path trustStore, String alias,
            Path certificate) throws Exception {
        keytool("-importcert", "-noprompt", "-alias", alias,
                "-file", certificate.toString(), "-storetype", "PKCS12",
                "-keystore", trustStore.toString(), "-storepass", PASSWORD);
    }

    ControlSecurityConfig security(Path trustStore, Path authz,
            String deploymentSetId, String host) {
        Properties properties = new Properties();
        properties.setProperty(ControlSecurityConfig.PROFILE_PROPERTY,
                "MTLS");
        properties.setProperty(ControlSecurityConfig.HOST_PROPERTY,
                host);
        properties.setProperty("rowweave.control.tls.keyStore",
                keyStore.toString());
        properties.setProperty("rowweave.control.tls.keyStorePassword", PASSWORD);
        properties.setProperty("rowweave.control.tls.trustStore",
                trustStore.toString());
        properties.setProperty("rowweave.control.tls.trustStorePassword", PASSWORD);
        properties.setProperty("rowweave.control.tls.authzFile", authz.toString());
        return ControlSecurityConfig.fromProperties(properties)
                .withDeploymentSetId(deploymentSetId);
    }

    private static void keytool(String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/keytool")
                .getAbsolutePath());
        Collections.addAll(command,
                "-J-Xms32m", "-J-Xmx256m",
                "-J-XX:MaxDirectMemorySize=32m",
                "-J-XX:MaxMetaspaceSize=96m",
                "-J-XX:ReservedCodeCacheSize=48m",
                "-J-Xss1m", "-J-XX:ActiveProcessorCount=2");
        command.addAll(Arrays.asList(arguments));
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).start();
        byte[] output = readAll(process.getInputStream());
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IllegalStateException("keytool failed: "
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

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }
}
