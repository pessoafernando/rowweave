package io.rowweave.connector.s3;

import io.rowweave.DataType;
import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3ConnectorModelTest {
    private static final String SHA256 = repeat('a', 64);
    private static final StreamSchema SCHEMA = new StreamSchema(Arrays.asList(
            new StreamField("id", "id", "id", DataType.LongType, false),
            new StreamField("amount", "amount", "amount",
                    DataType.BigDecimalType, true),
            new StreamField("posted_at", "posted_at", "posted_at",
                    DataType.TimestampType, true)));

    @Test
    void configurationIsCanonicalCredentialFreeAndResourceBounded() {
        S3ConnectorConfiguration configuration = configuration();

        byte[] frame = configuration.encode();
        S3ConnectorConfiguration restored =
                S3ConnectorConfiguration.decode(frame);
        assertArrayEquals(frame, restored.encode());
        assertEquals(URI.create("https://objects.example.invalid"),
                restored.endpoint());
        assertEquals("cn-north-1", restored.region());
        assertEquals("ledger-snapshots", restored.bucket());
        assertEquals(S3ObjectFormat.NDJSON_UTF8, restored.format());
        assertEquals(8L * 1024 * 1024, restored.maximumObjectBytes());
        assertEquals(64 * 1024, restored.maximumRowBytes());
        assertEquals(16L * 1024 * 1024,
                restored.temporaryDiskBudgetBytes());
        assertEquals("vault/s3/ledger", restored.credentialsReference());

        StreamConnectorConfiguration publicFrame =
                StreamConnectorConfiguration.decode(frame);
        assertEquals(Collections.singletonMap(
                S3ConnectorConfiguration.CREDENTIALS_ALIAS,
                "vault/s3/ledger"), publicFrame.secretReferences());
        assertFalse(publicFrame.options().toString().contains("vault"));
        assertFalse(restored.toString().contains("objects.example.invalid"));
        assertFalse(restored.toString().contains("ledger-snapshots"));

        assertInvalid(() -> S3ConnectorConfiguration.builder()
                .endpoint(URI.create("http://127.0.0.1:9000"))
                .region("us-east-1").bucket("b")
                .credentialsReference("secret").build());
        assertInvalid(() -> S3ConnectorConfiguration.builder()
                .endpoint(URI.create("https://objects.example.invalid/path"))
                .region("us-east-1").bucket("b")
                .credentialsReference("secret").build());
        assertInvalid(() -> S3ConnectorConfiguration.builder()
                .endpoint(URI.create("https://objects.example.invalid"))
                .region("us-east-1").bucket("b")
                .credentialsReference("secret")
                .maximumObjectBytes(4096).temporaryDiskBudgetBytes(4095)
                .build());

        Map<String, String> unknown = new LinkedHashMap<>(
                publicFrame.options());
        unknown.put("unknown", "value");
        byte[] unknownFrame = StreamConnectorConfiguration.of(
                unknown, publicFrame.secretReferences()).encode();
        assertInvalid(() -> S3ConnectorConfiguration.decode(unknownFrame));
    }

    @Test
    void exactObjectDescriptorBindsEveryReplayAuthorityField() {
        S3ObjectDescriptor descriptor = descriptor(
                "ledger/2026-09-27.ndjson", "version-42", SHA256, SCHEMA);

        byte[] frame = S3ObjectDescriptorCodec.encode(descriptor);
        S3ObjectDescriptor restored = S3ObjectDescriptorCodec.decode(frame);
        assertArrayEquals(frame, S3ObjectDescriptorCodec.encode(restored));
        assertEquals(descriptor.splitId(), restored.splitId());
        assertEquals(SCHEMA, restored.schema());
        assertEquals(SCHEMA.fingerprint(), restored.schemaFingerprint());
        assertTrue(restored.versioned());
        assertEquals("version-42", restored.versionId());
        assertFalse(restored.toString().contains("ledger/2026"));
        assertFalse(restored.toString().contains("version-42"));

        assertNotEquals(descriptor.splitId(), descriptor(
                "ledger/other.ndjson", "version-42", SHA256, SCHEMA).splitId());
        assertNotEquals(descriptor.splitId(), descriptor(
                "ledger/2026-09-27.ndjson", "version-43", SHA256,
                SCHEMA).splitId());
        assertNotEquals(descriptor.splitId(), descriptor(
                "ledger/2026-09-27.ndjson", "version-42", repeat('b', 64),
                SCHEMA).splitId());
        StreamSchema drifted = new StreamSchema(Arrays.asList(
                SCHEMA.field(0),
                new StreamField("amount", "amount", "amount",
                        DataType.StringType, true), SCHEMA.field(2)));
        assertNotEquals(descriptor.splitId(), descriptor(
                "ledger/2026-09-27.ndjson", "version-42", SHA256,
                drifted).splitId());

        S3ObjectDescriptor unversioned = S3ObjectDescriptor.unversioned(
                "ledger-snapshots", "ledger/frozen.ndjson", "etag-frozen",
                1234L, SHA256, S3ObjectFormat.NDJSON_UTF8, SCHEMA);
        assertFalse(unversioned.versioned());
        assertThrows(IllegalStateException.class, unversioned::versionId);

        assertCorrupt(frame, S3ObjectDescriptorCodec::decode);
    }

    @Test
    void positionIsCanonicalAndBoundToTheExactSplit() {
        S3ObjectDescriptor descriptor = descriptor(
                "ledger/2026-09-27.ndjson", "version-42", SHA256, SCHEMA);
        S3SourcePosition position = new S3SourcePosition(
                descriptor.splitId(), 37L);

        byte[] frame = S3SourcePositionCodec.encode(position);
        S3SourcePosition restored = S3SourcePositionCodec.decode(frame);
        assertEquals(descriptor.splitId(), restored.splitId());
        assertEquals(37L, restored.completedRecords());
        assertArrayEquals(frame, S3SourcePositionCodec.encode(restored));
        assertCorrupt(frame, S3SourcePositionCodec::decode);
        assertThrows(IllegalArgumentException.class,
                () -> new S3SourcePosition(repeat('f', 64), -1L));
    }

    @Test
    void manifestProducesOnlyBoundedExactOrderedPublicSplits() {
        S3ObjectDescriptor second = descriptor(
                "ledger/part-2.ndjson", "version-2", repeat('2', 64), SCHEMA);
        S3ObjectDescriptor first = descriptor(
                "ledger/part-1.ndjson", "version-1", repeat('1', 64), SCHEMA);
        S3Manifest manifest = S3Manifest.of(SCHEMA,
                Arrays.asList(second, first));

        StreamSourceSpec source = manifest.toSourceSpec(configuration());

        assertTrue(source.boundedTermination());
        assertEquals(S3ConnectorContract.PROVIDER_KEY, source.providerKey());
        assertEquals(S3ConnectorContract.contractDigest(),
                source.providerContractDigest());
        assertEquals(SCHEMA, source.schema());
        assertEquals(2, source.splits().size());
        assertEquals(first.splitId(), source.splits().get(0).splitId());
        assertEquals(second.splitId(), source.splits().get(1).splitId());
        for (StreamSplitSpec split : source.splits()) {
            assertEquals(S3SourcePositionCodec.CODEC_ID,
                    split.positionCodecId());
            assertEquals(split.splitId(), S3ObjectDescriptorCodec.decode(
                    split.descriptorFrame()).splitId());
        }
        assertArrayEquals(configuration().encode(),
                source.configurationFrame());
        assertEquals(64, manifest.manifestDigest().length());

        S3ObjectDescriptor wrongBucket = S3ObjectDescriptor.versioned(
                "other-bucket", "ledger/part-3.ndjson", "version-3",
                "etag-3", 1234L, repeat('3', 64),
                S3ObjectFormat.NDJSON_UTF8, SCHEMA);
        assertInvalid(() -> S3Manifest.of(SCHEMA,
                Arrays.asList(first, wrongBucket)).toSourceSpec(configuration()));
    }

    @Test
    void manifestLimitMatchesPortableFixedSplitEnvelope() {
        assertEquals(4096, S3Manifest.MAX_OBJECTS);
        List<S3ObjectDescriptor> objects = new ArrayList<>();
        for (int index = 0; index <= S3Manifest.MAX_OBJECTS; index++) {
            objects.add(descriptor("ledger/part-" + index + ".ndjson",
                    "version-" + index, SHA256, SCHEMA));
        }

        S3Manifest belowLimit = S3Manifest.of(SCHEMA,
                objects.subList(0, S3Manifest.MAX_OBJECTS - 1));
        assertEquals(S3Manifest.MAX_OBJECTS - 1,
                belowLimit.toSourceSpec(configuration()).splits().size());
        S3Manifest exactLimit = S3Manifest.of(SCHEMA,
                objects.subList(0, S3Manifest.MAX_OBJECTS));
        assertEquals(S3Manifest.MAX_OBJECTS,
                exactLimit.toSourceSpec(configuration()).splits().size());
        assertInvalid(() -> S3Manifest.of(SCHEMA, objects));
    }

    private static S3ConnectorConfiguration configuration() {
        return S3ConnectorConfiguration.builder()
                .endpoint(URI.create("https://objects.example.invalid"))
                .region("cn-north-1")
                .bucket("ledger-snapshots")
                .format(S3ObjectFormat.NDJSON_UTF8)
                .credentialsReference("vault/s3/ledger")
                .maximumObjectBytes(8L * 1024 * 1024)
                .maximumRowBytes(64 * 1024)
                .temporaryDiskBudgetBytes(16L * 1024 * 1024)
                .connectTimeoutMillis(5_000)
                .readTimeoutMillis(30_000)
                .maximumAttempts(3)
                .pathStyleAccess(true)
                .build();
    }

    private static S3ObjectDescriptor descriptor(String key,
            String version, String sha256, StreamSchema schema) {
        return S3ObjectDescriptor.versioned(
                "ledger-snapshots", key, version, "etag-42", 1234L,
                sha256, S3ObjectFormat.NDJSON_UTF8, schema);
    }

    private static void assertCorrupt(byte[] frame, Decoder decoder) {
        byte[] corrupt = frame.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertInvalid(() -> decoder.decode(corrupt));
        assertInvalid(() -> decoder.decode(Arrays.copyOf(
                frame, frame.length - 1)));
        byte[] trailing = Arrays.copyOf(frame, frame.length + 1);
        assertInvalid(() -> decoder.decode(trailing));
    }

    private static void assertInvalid(Runnable action) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class, action::run);
        assertTrue(failure.getMessage().startsWith("S3_"),
                failure.getMessage());
    }

    private interface Decoder { Object decode(byte[] frame); }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
