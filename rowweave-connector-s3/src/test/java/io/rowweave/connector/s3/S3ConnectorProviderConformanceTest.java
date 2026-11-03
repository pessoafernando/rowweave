package io.rowweave.connector.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.rowweave.DataType;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.spi.StreamSecretAccess;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.conformance.StreamConnectorConformance;
import io.rowweave.stream.spi.conformance.StreamConformanceReport;
import io.rowweave.stream.spi.conformance.StreamSourceConformanceFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Public-provider conformance over the real AWS SDK HTTP protocol path. */
class S3ConnectorProviderConformanceTest {
    private static final String ACCESS_KEY = "rowweave-access";
    private static final String SECRET_KEY = "rowweave-secret-key";
    private static final String ROTATED_ACCESS_KEY = "rowweave-rotated-access";
    private static final String ROTATED_SECRET_KEY = "rowweave-rotated-secret";
    private static final String VERSION = "version-1";
    private static final String ETAG = "\"etag-version-1\"";
    private static final String OBJECT_PATH =
            "/ledger-snapshots/frozen/ledger.ndjson";
    private static final byte[] CONTENT = ("{\"id\":1}\n"
            + "{\"id\":2}\n").getBytes(StandardCharsets.UTF_8);
    private static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "id", "id", "id", DataType.LongType, false)));

    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final AtomicReference<String> expectedAccessKey =
            new AtomicReference<>(ACCESS_KEY);
    private HttpServer server;
    private ExecutorService executor;
    private S3ConnectorConfiguration configuration;
    private S3ObjectDescriptor descriptor;
    private byte[] credentials;

    @BeforeEach
    void startProtocolEndpoint() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "s3-provider-test-http");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.createContext("/", this::handle);
        server.start();

        URI endpoint = URI.create("http://127.0.0.1:"
                + server.getAddress().getPort());
        configuration = S3ConnectorConfiguration.builder()
                .endpoint(endpoint).region("us-east-1")
                .bucket("ledger-snapshots")
                .format(S3ObjectFormat.NDJSON_UTF8)
                .credentialsReference("vault/s3")
                .maximumObjectBytes(1024 * 1024)
                .maximumRowBytes(64 * 1024)
                .temporaryDiskBudgetBytes(1024 * 1024)
                .connectTimeoutMillis(2_000).readTimeoutMillis(5_000)
                .maximumAttempts(2).pathStyleAccess(true)
                .allowInsecureEndpoint(true).build();
        descriptor = S3ObjectDescriptor.versioned(
                "ledger-snapshots", "frozen/ledger.ndjson", VERSION, ETAG,
                CONTENT.length, sha256(CONTENT),
                S3ObjectFormat.NDJSON_UTF8, SCHEMA);
        expectedAccessKey.set(ACCESS_KEY);
        credentials = credentials(ACCESS_KEY, SECRET_KEY);
    }

    @AfterEach
    void stopProtocolEndpoint() {
        if (server != null) server.stop(0);
        if (executor != null) executor.shutdownNow();
        if (credentials != null) Arrays.fill(credentials, (byte) 0);
    }

    @Test
    void productionProviderPassesPublishedBoundedReplayContract()
            throws Exception {
        StreamConformanceReport report =
                StreamConnectorConformance.verifyBoundedSource(
                        new Fixture());

        assertTrue(report.passed("SOURCE_POSITION_MONOTONIC"));
        assertTrue(report.passed("SOURCE_EXACT_REPLAY"));
        assertTrue(report.passed("SOURCE_END_ONCE"));
        assertTrue(report.passed("SOURCE_TERMINAL_RESTORE"));
        assertTrue(report.passed("SOURCE_WAKEUP_CANCEL_CLOSE"));
        assertEquals(3, requests.size(), requests.toString());
        for (String request : requests) {
            assertTrue(request.contains("versionId=" + VERSION), request);
            assertTrue(request.contains("Credential=" + ACCESS_KEY + "/"),
                    request);
        }
    }

    @Test
    void rotatedCredentialIsResolvedWhenTheReaderReopens()
            throws Exception {
        S3ConnectorProvider provider = new S3ConnectorProvider();
        Fixture fixture = new Fixture();
        StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
        try (StreamSourceReader initial = provider.open(
                fixture.initialContext())) {
            assertEquals(1L, codec.decode(initial.nextEvent().record()
                    .rowFrame()).getLong(0));
        }

        byte[] previous = credentials;
        credentials = credentials(
                ROTATED_ACCESS_KEY, ROTATED_SECRET_KEY);
        Arrays.fill(previous, (byte) 0);
        expectedAccessKey.set(ROTATED_ACCESS_KEY);

        try (StreamSourceReader reopened = provider.open(
                fixture.initialContext())) {
            assertEquals(1L, codec.decode(reopened.nextEvent().record()
                    .rowFrame()).getLong(0));
        }

        assertEquals(2, requests.size(), requests.toString());
        assertTrue(requests.get(0).contains(
                "Credential=" + ACCESS_KEY + "/"), requests.toString());
        assertTrue(requests.get(1).contains(
                "Credential=" + ROTATED_ACCESS_KEY + "/"),
                requests.toString());
    }

    private void handle(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getRawQuery();
        String authorization = exchange.getRequestHeaders()
                .getFirst("Authorization");
        requests.add(exchange.getRequestMethod() + " "
                + exchange.getRequestURI() + " " + authorization);
        if (!"GET".equals(exchange.getRequestMethod())
                || !OBJECT_PATH.equals(exchange.getRequestURI().getPath())
                || query == null || !query.contains("versionId=" + VERSION)
                || authorization == null
                || !authorization.contains("Credential="
                + expectedAccessKey.get() + "/")) {
            exchange.sendResponseHeaders(403, -1L);
            exchange.close();
            return;
        }
        exchange.getResponseHeaders().add("ETag", ETAG);
        exchange.getResponseHeaders().add("x-amz-version-id", VERSION);
        exchange.getResponseHeaders().add("Content-Type",
                "application/x-ndjson");
        exchange.getResponseHeaders().add("x-amz-request-id", "rowweave-test");
        exchange.sendResponseHeaders(200, CONTENT.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(CONTENT);
        }
    }

    private final class Fixture implements StreamSourceConformanceFixture {
        private final StreamSourceProvider provider =
                new S3ConnectorProvider();

        @Override public StreamSourceProvider provider() { return provider; }

        @Override public StreamSourceContext initialContext() {
            return context(null, -1L);
        }

        @Override public StreamSourceContext restoredContext(
                byte[] positionFrame, long logicalPosition) {
            return context(positionFrame, logicalPosition);
        }

        @Override public int comparePositions(byte[] left, byte[] right) {
            long leftValue = S3SourcePositionCodec.decode(left)
                    .completedRecords();
            long rightValue = S3SourcePositionCodec.decode(right)
                    .completedRecords();
            return Long.compare(leftValue, rightValue);
        }

        @Override public long wakeupTimeoutMillis() { return 5_000L; }

        private StreamSourceContext context(byte[] position,
                long logicalPosition) {
            return new StreamSourceContext("s3-source",
                    configuration.encode(), descriptor.splitId(),
                    S3SourcePositionCodec.CODEC_ID,
                    S3ObjectDescriptorCodec.encode(descriptor), position,
                    logicalPosition, secretAccess());
        }
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

    private static byte[] credentials(String accessKey, String secretKey) {
        S3Credentials value = S3Credentials.basic(
                accessKey.getBytes(StandardCharsets.UTF_8),
                secretKey.getBytes(StandardCharsets.UTF_8));
        try {
            return S3CredentialCodec.encode(value);
        } finally {
            value.destroy();
        }
    }

    private static String sha256(byte[] bytes) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder value = new StringBuilder(64);
        for (byte item : digest) value.append(String.format("%02x", item));
        return value.toString();
    }
}
