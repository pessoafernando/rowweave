package io.rowweave.connector.postgres;

import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.DataType;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;
import io.rowweave.stream.api.StreamSourceSpec;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresConnectorModelTest {
    @Test
    void signedContractRequiresResidentCdcV2CompletionAcknowledgement() {
        StreamConnectorProviderContract contract =
                PostgresConnectorContract.contract();

        assertEquals(StreamConnectorProviderContract.Kind.SOURCE,
                contract.kind());
        assertEquals("rowweave-connector-postgres-cdc",
                contract.deploymentId());
        assertEquals("rowweave.postgres.logical-cdc",
                contract.providerKey());
        assertTrue(contract.residentCdc());
        assertTrue(contract.externalCheckpointAcknowledgement());
        assertFalse(contract.boundedEndOfInput());
        assertFalse(contract.dynamicDiscovery());
        assertEquals(StreamConnectorProviderContract.SourceReplay.EXACT_ORDERED,
                contract.sourceReplay());
        assertEquals(contract, StreamConnectorProviderContractCodec.decode(
                PostgresConnectorContract.encodedContract()));
    }

    @Test
    void configurationFreezesServerSchemaAndResourceAuthorityWithoutSecrets() {
        List<PostgresColumnBinding> mutableColumns = new ArrayList<>(
                sampleColumns());
        PostgresTableBinding table = new PostgresTableBinding(
                "ledger", "entries", 16_384L,
                PostgresReplicaIdentity.DEFAULT, 0L,
                mutableColumns, Collections.singletonList(1));
        mutableColumns.clear();
        PostgresConnectorConfiguration configuration = configuration(table);

        byte[] frame = configuration.encode();
        PostgresConnectorConfiguration restored =
                PostgresConnectorConfiguration.decode(frame);

        assertArrayEquals(frame, restored.encode());
        assertEquals(493, frame.length);
        assertEquals("64cf39afbac9057ba5ea9b6056f3ba09b"
                        + "94d5941969428e8509b54d62ed5619f",
                hex(sha256(frame)));
        assertEquals("pg-ledger.internal", restored.host());
        assertEquals(5432, restored.port());
        assertEquals("ledger", restored.database());
        assertEquals("rowweave_ledger_slot", restored.slot());
        assertEquals("rowweave_ledger_publication", restored.publication());
        assertEquals("18446744073709551615",
                restored.databaseSystemIdentifier());
        assertEquals(4_294_967_295L, restored.databaseOid());
        assertEquals(PostgresTlsMode.VERIFY_FULL, restored.tlsMode());
        assertEquals(2, restored.tables().get(0).columns().size());
        assertEquals(20L, restored.tables().get(0).columns().get(0).typeOid());
        assertEquals(Collections.singletonList(1),
                restored.tables().get(0).primaryKeyAttributeNumbers());
        assertEquals(DataType.LongType,
                restored.streamSchema().field(0).dataType());
        assertEquals(DataType.BigDecimalType,
                restored.streamSchema().field(1).dataType());
        assertEquals(Collections.singletonList("id"),
                restored.stableKeyFieldIds());
        StreamSourceSpec source = restored.toSourceSpec();
        assertTrue(source.residentCdc());
        assertEquals(2, source.residentCdcStateVersion());
        assertEquals(PostgresConnectorContract.PROVIDER_KEY,
                source.providerKey());
        assertEquals(PostgresConnectorContract.contractDigest(),
                source.providerContractDigest());
        assertEquals(restored.streamSchema(), source.schema());
        assertEquals(Collections.singletonList("id"),
                source.stableKeyFieldIds());
        assertEquals(Collections.singletonList("snapshot-all"),
                source.snapshotSplitIds());
        assertEquals(restored.snapshotIdentity(), source.snapshotId());
        assertEquals(10_000, restored.maximumTransactionChanges());
        assertEquals(32L * 1024 * 1024,
                restored.maximumTransactionBytes());
        assertEquals(4L * 1024 * 1024 * 1024,
                restored.maximumRetainedWalBytes());

        StreamConnectorConfiguration envelope =
                StreamConnectorConfiguration.decode(frame);
        assertEquals("vault/postgres/ledger",
                envelope.secretReferences().get(
                        PostgresConnectorConfiguration.CREDENTIALS_ALIAS));
        assertEquals("vault/postgres/ca",
                envelope.secretReferences().get(
                        PostgresConnectorConfiguration.TLS_TRUST_ALIAS));
        assertFalse(envelope.options().toString().contains("vault"));
        assertFalse(configuration.toString().contains("pg-ledger"));
        assertFalse(configuration.toString().contains("ledger"));

        assertThrows(UnsupportedOperationException.class,
                () -> restored.tables().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> restored.tables().get(0).columns().clear());

        Map<String, String> unknown = new LinkedHashMap<>(
                envelope.options());
        unknown.put("unknown", "value");
        byte[] unknownFrame = StreamConnectorConfiguration.of(
                unknown, envelope.secretReferences()).encode();
        assertInvalid(() -> PostgresConnectorConfiguration.decode(
                unknownFrame));
        assertCorrupt(frame, PostgresConnectorConfiguration::decode);
    }

    @Test
    void schemaBindingRejectsAmbiguousKeysAndDriftProneMetadata() {
        List<PostgresColumnBinding> columns = sampleColumns();

        assertInvalid(() -> new PostgresTableBinding(
                "ledger", "entries", 16_384L,
                PostgresReplicaIdentity.DEFAULT, 0L,
                Arrays.asList(columns.get(0), columns.get(0)),
                Collections.singletonList(1)));
        assertInvalid(() -> new PostgresTableBinding(
                "ledger", "entries", 16_384L,
                PostgresReplicaIdentity.DEFAULT, 0L, columns,
                Collections.singletonList(99)));
        assertInvalid(() -> new PostgresTableBinding(
                "ledger", "entries", 16_384L,
                PostgresReplicaIdentity.USING_INDEX, 0L, columns,
                Collections.singletonList(1)));
        assertInvalid(() -> new PostgresTableBinding(
                "ledger", "entries", 16_384L,
                PostgresReplicaIdentity.DEFAULT, 0L,
                Arrays.asList(
                        new PostgresColumnBinding("id", "id", 1,
                                20L, -1, true),
                        columns.get(1)), Collections.singletonList(1)));
        assertInvalid(() -> configuration(Arrays.asList(
                new PostgresTableBinding(
                        "ledger", "entries", 16_384L,
                        PostgresReplicaIdentity.DEFAULT, 0L, columns,
                        Collections.singletonList(1)),
                new PostgresTableBinding(
                        "ledger", "entries_drifted", 16_385L,
                        PostgresReplicaIdentity.DEFAULT, 0L,
                        Arrays.asList(columns.get(0),
                                new PostgresColumnBinding(
                                        "description", "description", 2,
                                        25L, -1, true)),
                        Collections.singletonList(1)))));

        List<PostgresTableBinding> tooMany = new ArrayList<>();
        for (int index = 0;
                index <= PostgresConnectorConfiguration.MAX_TABLES;
                index++) {
            tooMany.add(new PostgresTableBinding(
                    "ledger", "entries_" + index, 20_000L + index,
                    PostgresReplicaIdentity.DEFAULT, 0L, columns,
                    Collections.singletonList(1)));
        }
        assertEquals(PostgresConnectorConfiguration.MAX_TABLES - 1,
                configuration(tooMany.subList(0,
                        PostgresConnectorConfiguration.MAX_TABLES - 1))
                        .tables().size());
        assertEquals(PostgresConnectorConfiguration.MAX_TABLES,
                configuration(tooMany.subList(0,
                        PostgresConnectorConfiguration.MAX_TABLES))
                        .tables().size());
        assertInvalid(() -> configuration(tooMany));
    }

    private static PostgresConnectorConfiguration configuration(
            PostgresTableBinding table) {
        return configuration(Collections.singletonList(table));
    }

    private static PostgresConnectorConfiguration configuration(
            List<PostgresTableBinding> tables) {
        return PostgresConnectorConfiguration.builder()
                .host("pg-ledger.internal").port(5432)
                .database("ledger").slot("rowweave_ledger_slot")
                .publication("rowweave_ledger_publication")
                .databaseSystemIdentifier("18446744073709551615")
                .databaseOid(4_294_967_295L)
                .tlsMode(PostgresTlsMode.VERIFY_FULL)
                .credentialsReference("vault/postgres/ledger")
                .tlsTrustReference("vault/postgres/ca")
                .tables(tables)
                .maximumTransactionChanges(10_000)
                .maximumTransactionBytes(32L * 1024 * 1024)
                .maximumRetainedWalBytes(4L * 1024 * 1024 * 1024)
                .snapshotFetchRows(2_000)
                .connectTimeoutMillis(10_000)
                .socketTimeoutMillis(60_000)
                .statusIntervalMillis(10_000)
                .build();
    }

    private static List<PostgresColumnBinding> sampleColumns() {
        return Arrays.asList(
                new PostgresColumnBinding("id", "id", 1,
                        20L, -1, false),
                new PostgresColumnBinding("amount", "amount", 2,
                        1700L, (18 << 16) | 2, true));
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
        assertTrue(failure.getMessage().startsWith("POSTGRES_CDC_"),
                failure.getMessage());
    }

    private interface Decoder { Object decode(byte[] frame); }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte item : value) {
            result.append(String.format("%02x", item));
        }
        return result.toString();
    }
}
