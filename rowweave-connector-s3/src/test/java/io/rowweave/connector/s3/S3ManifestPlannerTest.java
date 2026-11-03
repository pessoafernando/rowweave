package io.rowweave.connector.s3;

import io.rowweave.DataType;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3ManifestPlannerTest {
    private static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "id", "id", "id", DataType.LongType, false)));

    @Test
    void pagedPrefixIsFrozenFromActualObjectResponseAndCanonicallySorted()
            throws Exception {
        FakePlanningClient client = new FakePlanningClient();
        client.pages.add(new S3ListingPage(
                Arrays.asList("prefix/z.ndjson", "prefix/a.ndjson"),
                true, "page-2"));
        client.pages.add(new S3ListingPage(
                Collections.singletonList("prefix/m.ndjson"), false, null));
        client.put("prefix/a.ndjson", "version-a", "etag-a",
                "{\"id\":1}\n");
        client.put("prefix/m.ndjson", null, "etag-m",
                "{\"id\":2}\n");
        client.put("prefix/z.ndjson", "version-z", "etag-z",
                "{\"id\":3}\n");

        S3Manifest manifest = S3ManifestPlanner.planPrefix(
                configuration(1024 * 1024), SCHEMA, "prefix/", client);

        assertEquals(Arrays.asList("prefix/a.ndjson", "prefix/m.ndjson",
                        "prefix/z.ndjson"),
                Arrays.asList(manifest.objects().get(0).objectKey(),
                        manifest.objects().get(1).objectKey(),
                        manifest.objects().get(2).objectKey()));
        assertTrue(manifest.objects().get(0).versioned());
        assertFalse(manifest.objects().get(1).versioned());
        assertEquals("version-z", manifest.objects().get(2).versionId());
        assertEquals("etag-a", manifest.objects().get(0).etag());
        assertEquals(64, manifest.objects().get(0).contentSha256().length());
        assertEquals(Arrays.asList(null, "page-2"), client.tokens);
        assertEquals(3, client.opened.get());
        assertEquals(3, client.streamCloses.get());
        assertEquals(0, client.clientCloses.get(),
                "the injected planner client remains caller-owned");
    }

    @Test
    void duplicateKeyTokenCycleLengthDriftAndBudgetFailClosed() {
        FakePlanningClient duplicate = new FakePlanningClient();
        duplicate.pages.add(new S3ListingPage(
                Arrays.asList("p/a", "p/a"), false, null));
        assertFailure("S3_MANIFEST_DUPLICATE", () ->
                S3ManifestPlanner.planPrefix(configuration(1024), SCHEMA,
                        "p/", duplicate));
        assertEquals(0, duplicate.opened.get());

        FakePlanningClient cycle = new FakePlanningClient();
        cycle.pages.add(new S3ListingPage(
                Collections.singletonList("p/a"), true, "again"));
        cycle.pages.add(new S3ListingPage(
                Collections.singletonList("p/b"), true, "again"));
        cycle.put("p/a", "v1", "e1", "{\"id\":1}\n");
        assertFailure("S3_MANIFEST_PAGINATION_INVALID", () ->
                S3ManifestPlanner.planPrefix(configuration(1024), SCHEMA,
                        "p/", cycle));

        FakePlanningClient lengthDrift = new FakePlanningClient();
        lengthDrift.pages.add(new S3ListingPage(
                Collections.singletonList("p/a"), false, null));
        lengthDrift.put("p/a", "v1", "e1", "{\"id\":1}\n");
        lengthDrift.lengthDelta = 1;
        assertFailure("S3_OBJECT_CHANGED", () ->
                S3ManifestPlanner.planPrefix(configuration(1024), SCHEMA,
                        "p/", lengthDrift));
        assertEquals(1, lengthDrift.streamCloses.get());

        FakePlanningClient tooLarge = new FakePlanningClient();
        tooLarge.pages.add(new S3ListingPage(
                Collections.singletonList("p/a"), false, null));
        tooLarge.put("p/a", "v1", "e1", repeat('x', 1025));
        assertFailure("S3_OBJECT_LIMIT", () ->
                S3ManifestPlanner.planPrefix(configuration(1024), SCHEMA,
                        "p/", tooLarge));
        assertEquals(1, tooLarge.streamCloses.get());
    }

    @Test
    void objectByteBudgetHonorsLimitMinusOneExactAndPlusOne()
            throws Exception {
        String content = repeat('x', 1024);

        FakePlanningClient below = planningClient(content);
        assertFailure("S3_OBJECT_LIMIT", () ->
                S3ManifestPlanner.planPrefix(configuration(1023), SCHEMA,
                        "p/", below));

        S3Manifest exact = S3ManifestPlanner.planPrefix(
                configuration(1024), SCHEMA, "p/", planningClient(content));
        assertEquals(1024L, exact.objects().get(0).contentLength());

        S3Manifest above = S3ManifestPlanner.planPrefix(
                configuration(1025), SCHEMA, "p/", planningClient(content));
        assertEquals(1024L, above.objects().get(0).contentLength());
    }

    @Test
    void emptyPrefixAndMalformedListingAreRejectedWithoutObjectOpen() {
        FakePlanningClient empty = new FakePlanningClient();
        empty.pages.add(new S3ListingPage(
                Collections.<String>emptyList(), false, null));
        assertFailure("S3_MANIFEST_EMPTY", () ->
                S3ManifestPlanner.planPrefix(configuration(1024), SCHEMA,
                        "prefix/", empty));
        assertEquals(0, empty.opened.get());

        FakePlanningClient outside = new FakePlanningClient();
        outside.pages.add(new S3ListingPage(
                Collections.singletonList("other/a"), false, null));
        assertFailure("S3_MANIFEST_LISTING_INVALID", () ->
                S3ManifestPlanner.planPrefix(configuration(1024), SCHEMA,
                        "prefix/", outside));
        assertEquals(0, outside.opened.get());
    }

    private static S3ConnectorConfiguration configuration(long maximum) {
        return S3ConnectorConfiguration.builder()
                .endpoint(java.net.URI.create("https://objects.invalid"))
                .region("us-east-1").bucket("ledger-snapshots")
                .format(S3ObjectFormat.NDJSON_UTF8)
                .credentialsReference("vault/s3")
                .maximumObjectBytes(maximum)
                .temporaryDiskBudgetBytes(maximum).build();
    }

    private static FakePlanningClient planningClient(String content) {
        FakePlanningClient client = new FakePlanningClient();
        client.pages.add(new S3ListingPage(
                Collections.singletonList("p/a"), false, null));
        client.put("p/a", "v1", "e1", content);
        return client;
    }

    private static void assertFailure(String code, ThrowingRunnable action) {
        Exception failure = assertThrows(Exception.class, action::run);
        assertEquals(code, failure.getMessage());
    }

    private interface ThrowingRunnable { void run() throws Exception; }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class FakePlanningClient
            implements S3ManifestPlanningClient {
        private final List<S3ListingPage> pages = new ArrayList<>();
        private final Map<String, Snapshot> snapshots = new HashMap<>();
        private final List<String> tokens = new ArrayList<>();
        private final AtomicInteger opened = new AtomicInteger();
        private final AtomicInteger streamCloses = new AtomicInteger();
        private final AtomicInteger clientCloses = new AtomicInteger();
        private int page;
        private int lengthDelta;

        private void put(String key, String version, String etag,
                String content) {
            snapshots.put(key, new Snapshot(version, etag,
                    content.getBytes(StandardCharsets.UTF_8)));
        }

        @Override public S3ListingPage list(String prefix,
                String continuationToken, int maximumKeys) {
            tokens.add(continuationToken);
            return pages.get(page++);
        }

        @Override public S3ObjectStream openCurrent(String bucket,
                String key) {
            opened.incrementAndGet();
            Snapshot value = snapshots.get(key);
            return new S3ObjectStream(new ByteArrayInputStream(value.content) {
                @Override public void close() {
                    streamCloses.incrementAndGet();
                }
            }, value.version, value.etag,
                    value.content.length + lengthDelta);
        }

        @Override public void close() { clientCloses.incrementAndGet(); }
    }

    private static final class Snapshot {
        private final String version;
        private final String etag;
        private final byte[] content;
        private Snapshot(String version, String etag, byte[] content) {
            this.version = version;
            this.etag = etag;
            this.content = content;
        }
    }
}
