package io.rowweave;

import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamWorkerConfig;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;
import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** public roles use mTLS for control, data and checkpoint lanes. */
class StreamMtlsClusterIntegrationTest {
    private static final String PASSWORD = "changeit";
    private static final String SOURCE = "mtls-source-v1";
    private static final String SINK = "mtls-sink-v1";
    private static final String SOURCE_CONTRACT = digest("mtls-source");
    private static final String SINK_CONTRACT = digest("mtls-sink");
    private static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "value", "value", "value", DataType.StringType, false)));

    @Test
    @Timeout(60)
    void distinctPublicRolesRunCrossWorkerCheckpointOverMtls(
            @TempDir Path root) throws Exception {
        Identity masterIdentity = identity(root, "stream-master");
        Identity sourceIdentity = identity(root, "stream-source-worker");
        Identity sinkIdentity = identity(root, "stream-sink-worker");
        Identity clientIdentity = identity(root, "stream-client");
        Path trustStore = root.resolve("stream-trust.p12");
        importCertificate(trustStore, "master", masterIdentity.certificate);
        importCertificate(trustStore, "source", sourceIdentity.certificate);
        importCertificate(trustStore, "sink", sinkIdentity.certificate);
        importCertificate(trustStore, "client", clientIdentity.certificate);
        Path authz = root.resolve("stream-authz.txt");
        Files.write(authz, (masterIdentity.fingerprint + "=MASTER\n"
                + sourceIdentity.fingerprint + "=WORKER\n"
                + sinkIdentity.fingerprint + "=WORKER\n"
                + clientIdentity.fingerprint + "=CLIENT\n")
                .getBytes(StandardCharsets.US_ASCII));

        ControlSecurityConfig masterSecurity = security(
                masterIdentity, trustStore, authz);
        ControlSecurityConfig sourceSecurity = security(
                sourceIdentity, trustStore, authz);
        ControlSecurityConfig sinkSecurity = security(
                sinkIdentity, trustStore, authz);
        ControlSecurityConfig clientSecurity = security(
                clientIdentity, trustStore, authz);
        ExactSource source = new ExactSource();
        ExactSink sink = new ExactSink();

        assertThrows(IllegalStateException.class,
                () -> RowWeave.startStreamWorker(
                        StreamWorkerConfig.defaults("wrong-role-worker",
                                root.resolve("wrong-role-state"))
                                .withControlEndpoint(clientSecurity, 0),
                        Collections.singletonList(source),
                        Collections.emptyList(), Collections.emptyList(),
                        Collections.emptyList()),
                "a CLIENT certificate must not open a Worker listener");

        try (StreamWorkerRuntime sourceWorker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("mtls-source-worker",
                             root.resolve("source-state"))
                             .withControlEndpoint(sourceSecurity, 0),
                     Collections.singletonList(source),
                     Collections.emptyList(), Collections.emptyList(),
                     Collections.emptyList());
             StreamWorkerRuntime sinkWorker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("mtls-sink-worker",
                             root.resolve("sink-state"))
                             .withControlEndpoint(sinkSecurity, 0),
                     Collections.emptyList(), Collections.emptyList(),
                     Collections.singletonList(sink),
                     Collections.emptyList());
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     root.resolve("master-state"), Arrays.asList(
                             sourceWorker.endpoint(), sinkWorker.endpoint()),
                     masterSecurity, 0);
             StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                     master.host(), master.port(), clientSecurity)) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("mtls-submit"));
            job.awaitRunning(10_000L);
            source.awaitPrefix(10_000L);
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of("mtls-cut"))
                            .awaitCompletion(10_000L));
            job.cancel(StreamCommandId.of("mtls-cancel"))
                    .awaitCompletion(10_000L);
            assertEquals(1, sink.commitCount());
        }
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "mtls", "mtls", client);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                60_000L, 10_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.exactOrdered(
                        SCHEMA, SOURCE, SOURCE_CONTRACT, new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                digest("mtls-split"), "int32-v1",
                                new byte[] {2}))))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        SINK, SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static ControlSecurityConfig security(Identity identity,
            Path trustStore, Path authz) {
        Properties properties = new Properties();
        properties.setProperty(ControlSecurityConfig.PROFILE_PROPERTY, "MTLS");
        properties.setProperty(ControlSecurityConfig.HOST_PROPERTY, "127.0.0.1");
        properties.setProperty("rowweave.control.tls.keyStore",
                identity.keyStore.toString());
        properties.setProperty("rowweave.control.tls.keyStorePassword",
                PASSWORD);
        properties.setProperty("rowweave.control.tls.trustStore",
                trustStore.toString());
        properties.setProperty("rowweave.control.tls.trustStorePassword",
                PASSWORD);
        properties.setProperty("rowweave.control.tls.authzFile",
                authz.toString());
        return ControlSecurityConfig.fromProperties(properties);
    }

    private static Identity identity(Path root, String name) throws Exception {
        Path keyStore = root.resolve(name + ".p12");
        Path certificate = root.resolve(name + ".cer");
        keytool("-genkeypair", "-alias", "identity", "-keyalg", "RSA",
                "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-dname", "CN=" + name, "-validity", "30",
                "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth");
        keytool("-exportcert", "-alias", "identity", "-keystore",
                keyStore.toString(), "-storepass", PASSWORD,
                "-file", certificate.toString());
        X509Certificate leaf;
        try (InputStream input = Files.newInputStream(certificate)) {
            leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
        return new Identity(keyStore, certificate,
                digestBytes(MessageDigest.getInstance("SHA-256").digest(
                        leaf.getEncoded())));
    }

    private static void importCertificate(Path trustStore, String alias,
            Path certificate) throws Exception {
        keytool("-importcert", "-noprompt", "-alias", alias,
                "-file", certificate.toString(), "-storetype", "PKCS12",
                "-keystore", trustStore.toString(), "-storepass", PASSWORD);
    }

    private static void keytool(String... arguments) throws Exception {
        List<String> command = ForkedJvm.keytoolCommand(arguments);
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).start();
        byte[] output = readAll(process.getInputStream());
        int exit = process.waitFor();
        if (exit != 0) {
            throw new AssertionError("keytool failed: "
                    + new String(output, StandardCharsets.UTF_8));
        }
    }

    private static byte[] readAll(InputStream input) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4_096];
        for (int read; (read = input.read(buffer)) >= 0; ) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static final class ExactSource implements StreamSourceProvider {
        private final Semaphore prefix = new Semaphore(0);
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(
                                context.restoredPositionFrame()).getInt() : 0;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next == 0) {
                        DataRow row = new DataRow(1);
                        row.set(0, "secured");
                        next++;
                        prefix.release();
                        return StreamSourceRecord.of(
                                codec.encode(row), position(next));
                    }
                    while (!canceled) {
                        if (wakeups.tryAcquire(20L, TimeUnit.MILLISECONDS)) {
                            return null;
                        }
                    }
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    canceled = true;
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }
        void awaitPrefix(long timeoutMillis) throws InterruptedException {
            if (!prefix.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("source prefix timed out");
            }
        }
        private static byte[] position(int value) {
            return ByteBuffer.allocate(4).putInt(value).array();
        }
    }

    private static final class ExactSink
            implements StreamTransactionalSinkProvider {
        private final Map<String, StreamTransactionStatus> transactions =
                new LinkedHashMap<>();
        private int commits;
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamTransactionalSinkWriter open(
                StreamTransactionalSinkContext context) {
            return new StreamTransactionalSinkWriter() {
                @Override public void write(byte[] frame) {
                    new StreamDataRowCodec(SCHEMA).decode(frame);
                }
                @Override public byte[] prepare(String identity) {
                    synchronized (transactions) {
                        transactions.putIfAbsent(identity,
                                StreamTransactionStatus.PREPARED);
                    }
                    return digest(identity).getBytes(StandardCharsets.US_ASCII);
                }
                @Override public StreamTransactionStatus query(
                        String identity) {
                    synchronized (transactions) {
                        StreamTransactionStatus status = transactions.get(identity);
                        return status == null
                                ? StreamTransactionStatus.UNKNOWN : status;
                    }
                }
                @Override public void commit(String identity) {
                    synchronized (transactions) {
                        if (transactions.get(identity)
                                != StreamTransactionStatus.VISIBLE) {
                            transactions.put(identity,
                                    StreamTransactionStatus.VISIBLE);
                            commits++;
                        }
                    }
                }
                @Override public void finalizeTransaction(String identity) { }
                @Override public void abort(String identity) {
                    synchronized (transactions) {
                        if (transactions.containsKey(identity)) {
                            transactions.put(identity,
                                    StreamTransactionStatus.ABORTED);
                        }
                    }
                }
                @Override public void close() { }
            };
        }
        int commitCount() {
            synchronized (transactions) { return commits; }
        }
    }

    private static final class Identity {
        final Path keyStore;
        final Path certificate;
        final String fingerprint;
        private Identity(Path keyStore, Path certificate, String fingerprint) {
            this.keyStore = keyStore;
            this.certificate = certificate;
            this.fingerprint = fingerprint;
        }
    }

    private static String digest(String value) {
        try {
            return digestBytes(MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String digestBytes(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte one : bytes) {
            result.append(Character.forDigit((one >>> 4) & 0x0f, 16));
            result.append(Character.forDigit(one & 0x0f, 16));
        }
        return result.toString();
    }
}
