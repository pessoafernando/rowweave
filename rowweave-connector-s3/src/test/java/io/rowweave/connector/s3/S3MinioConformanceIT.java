package io.rowweave.connector.s3;

import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.spi.StreamSecretAccess;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEvent;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.conformance.StreamConnectorConformance;
import io.rowweave.stream.spi.conformance.StreamConformanceReport;
import io.rowweave.stream.spi.conformance.StreamSourceConformanceFixture;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in conformance against an independently launched real MinIO server. */
class S3MinioConformanceIT {
    private static final String ACCESS_KEY = "rowweave-minio-access";
    private static final String SECRET_KEY = "rowweave-minio-secret-key";
    private static final String BUCKET = "ledger-snapshots";
    private static final String KEY = "frozen/ledger.ndjson";
    private static final byte[] FROZEN = ("{\"id\":1}\n"
            + "{\"id\":2}\n").getBytes(StandardCharsets.UTF_8);
    private static final byte[] REPLACEMENT = ("{\"id\":101}\n"
            + "{\"id\":102}\n").getBytes(StandardCharsets.UTF_8);
    private static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "id", "id", "id", DataType.LongType, false)));

    @Test
    void versionedSnapshotRemainsFrozenAndPassesPublicReplayContract(
            @TempDir Path temp) throws Exception {
        String configuredBinary = System.getProperty("minio.binary");
        Assumptions.assumeTrue(configuredBinary != null,
                "set -Dminio.binary to run real MinIO conformance");
        Path binary = Paths.get(configuredBinary).toAbsolutePath().normalize();
        Assumptions.assumeTrue(Files.isExecutable(binary),
                "MinIO binary is not executable: " + binary);

        int apiPort = freePort();
        int consolePort = freePort();
        Path data = Files.createDirectories(temp.resolve("data"));
        Path log = temp.resolve("minio.log");
        Process process = launch(binary, data, log, apiPort, consolePort);
        byte[] credentialFrame = credentials();
        try {
            awaitReady(process, log, apiPort);
            S3ConnectorConfiguration configuration = configuration(apiPort);
            PutObjectResponse frozen = provision(configuration, credentialFrame,
                    FROZEN);

            S3Manifest manifest = S3ManifestPlanner.planPrefix(
                    configuration, SCHEMA, "frozen/", credentialFrame);
            assertEquals(1, manifest.objects().size());
            S3ObjectDescriptor descriptor = manifest.objects().get(0);
            assertTrue(descriptor.versioned());
            assertEquals(frozen.versionId(), descriptor.versionId());

            PutObjectResponse replacement = put(configuration,
                    credentialFrame, REPLACEMENT);
            assertNotEquals(frozen.versionId(), replacement.versionId());

            Fixture fixture = new Fixture(configuration, descriptor,
                    credentialFrame);
            StreamConformanceReport report =
                    StreamConnectorConformance.verifyBoundedSource(fixture);
            assertTrue(report.passed("SOURCE_EXACT_REPLAY"));
            assertTrue(report.passed("SOURCE_END_ONCE"));
            assertTrue(report.passed("SOURCE_TERMINAL_RESTORE"));

            try (StreamSourceReader reader = fixture.provider().open(
                    fixture.initialContext())) {
                StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
                DataRow first = codec.decode(
                        reader.nextEvent().record().rowFrame());
                DataRow second = codec.decode(
                        reader.nextEvent().record().rowFrame());
                assertEquals(1L, first.getLong(0));
                assertEquals(2L, second.getLong(0));
                assertEquals(StreamSourceEvent.Kind.END_OF_INPUT,
                        reader.nextEvent().kind());
            }
        } finally {
            Arrays.fill(credentialFrame, (byte) 0);
            stop(process);
        }
    }

    private static PutObjectResponse provision(
            S3ConnectorConfiguration configuration, byte[] credentialFrame,
            byte[] content) throws Exception {
        S3Credentials credentials = S3CredentialCodec.decode(credentialFrame);
        try (S3Client client = AwsS3ClientFactory.create(
                configuration, credentials)) {
            client.createBucket(CreateBucketRequest.builder()
                    .bucket(BUCKET).build());
            client.putBucketVersioning(PutBucketVersioningRequest.builder()
                    .bucket(BUCKET).versioningConfiguration(
                            VersioningConfiguration.builder()
                                    .status(BucketVersioningStatus.ENABLED)
                                    .build()).build());
            return client.putObject(PutObjectRequest.builder()
                    .bucket(BUCKET).key(KEY)
                    .contentType("application/x-ndjson").build(),
                    RequestBody.fromBytes(content));
        } finally {
            credentials.destroy();
        }
    }

    private static PutObjectResponse put(
            S3ConnectorConfiguration configuration, byte[] credentialFrame,
            byte[] content) throws Exception {
        S3Credentials credentials = S3CredentialCodec.decode(credentialFrame);
        try (S3Client client = AwsS3ClientFactory.create(
                configuration, credentials)) {
            return client.putObject(PutObjectRequest.builder()
                    .bucket(BUCKET).key(KEY)
                    .contentType("application/x-ndjson").build(),
                    RequestBody.fromBytes(content));
        } finally {
            credentials.destroy();
        }
    }

    private static S3ConnectorConfiguration configuration(int port) {
        return S3ConnectorConfiguration.builder()
                .endpoint(URI.create("http://127.0.0.1:" + port))
                .region("us-east-1").bucket(BUCKET)
                .format(S3ObjectFormat.NDJSON_UTF8)
                .credentialsReference("vault/minio")
                .maximumObjectBytes(1024 * 1024)
                .maximumRowBytes(64 * 1024)
                .temporaryDiskBudgetBytes(1024 * 1024)
                .connectTimeoutMillis(2_000).readTimeoutMillis(10_000)
                .maximumAttempts(3).pathStyleAccess(true)
                .allowInsecureEndpoint(true).build();
    }

    private static byte[] credentials() {
        S3Credentials value = S3Credentials.basic(
                ACCESS_KEY.getBytes(StandardCharsets.UTF_8),
                SECRET_KEY.getBytes(StandardCharsets.UTF_8));
        try {
            return S3CredentialCodec.encode(value);
        } finally {
            value.destroy();
        }
    }

    private static Process launch(Path binary, Path data, Path log,
            int apiPort, int consolePort) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(
                binary.toString(), "server",
                "--address", "127.0.0.1:" + apiPort,
                "--console-address", "127.0.0.1:" + consolePort,
                data.toString());
        builder.environment().put("MINIO_ROOT_USER", ACCESS_KEY);
        builder.environment().put("MINIO_ROOT_PASSWORD", SECRET_KEY);
        builder.environment().put("MINIO_BROWSER", "off");
        builder.redirectErrorStream(true);
        builder.redirectOutput(log.toFile());
        return builder.start();
    }

    private static void awaitReady(Process process, Path log, int port)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(30L);
        URL health = new URL("http://127.0.0.1:" + port
                + "/minio/health/ready");
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new AssertionError("MinIO exited before ready:\n"
                        + read(log));
            }
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) health.openConnection();
                connection.setConnectTimeout(250);
                connection.setReadTimeout(250);
                if (connection.getResponseCode() == 200) return;
            } catch (IOException notReady) {
                // Startup is bounded by the outer deadline.
            } finally {
                if (connection != null) connection.disconnect();
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("MinIO did not become ready:\n" + read(log));
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(
                0, 1, InetAddress.getByName("127.0.0.1"))) {
            return socket.getLocalPort();
        }
    }

    private static void stop(Process process) throws Exception {
        process.destroy();
        if (!process.waitFor(10L, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            assertTrue(process.waitFor(5L, TimeUnit.SECONDS),
                    "MinIO process did not stop");
        }
    }

    private static String read(Path path) throws IOException {
        if (!Files.exists(path)) return "<missing log>";
        byte[] bytes = Files.readAllBytes(path);
        int start = Math.max(0, bytes.length - 8192);
        return new String(bytes, start, bytes.length - start,
                StandardCharsets.UTF_8);
    }

    private static final class Fixture
            implements StreamSourceConformanceFixture {
        private final S3ConnectorConfiguration configuration;
        private final S3ObjectDescriptor descriptor;
        private final byte[] credentials;
        private final StreamSourceProvider provider =
                new S3ConnectorProvider();

        private Fixture(S3ConnectorConfiguration configuration,
                S3ObjectDescriptor descriptor, byte[] credentials) {
            this.configuration = configuration;
            this.descriptor = descriptor;
            this.credentials = credentials;
        }

        @Override public StreamSourceProvider provider() { return provider; }
        @Override public StreamSourceContext initialContext() {
            return context(null, -1L);
        }
        @Override public StreamSourceContext restoredContext(
                byte[] positionFrame, long logicalPosition) {
            return context(positionFrame, logicalPosition);
        }
        @Override public int comparePositions(byte[] left, byte[] right) {
            return Long.compare(
                    S3SourcePositionCodec.decode(left).completedRecords(),
                    S3SourcePositionCodec.decode(right).completedRecords());
        }
        @Override public long wakeupTimeoutMillis() { return 5_000L; }

        private StreamSourceContext context(byte[] position,
                long logicalPosition) {
            return new StreamSourceContext("s3-minio-source",
                    configuration.encode(), descriptor.splitId(),
                    S3SourcePositionCodec.CODEC_ID,
                    S3ObjectDescriptorCodec.encode(descriptor), position,
                    logicalPosition, secretAccess());
        }

        private StreamSecretAccess secretAccess() {
            return new StreamSecretAccess() {
                @Override public <T> T withSecret(String alias,
                        Operation<T> operation) throws Exception {
                    assertEquals(S3ConnectorConfiguration.CREDENTIALS_ALIAS,
                            alias);
                    byte[] issued = credentials.clone();
                    try {
                        return operation.apply(issued);
                    } finally {
                        Arrays.fill(issued, (byte) 0);
                    }
                }
            };
        }
    }
}
