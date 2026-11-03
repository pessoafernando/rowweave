package io.rowweave.connector.postgres;

import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSourceSpec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Credential-free canonical PostgreSQL snapshot/log source authority. */
public final class PostgresConnectorConfiguration {
    public static final String CREDENTIALS_ALIAS = "credentials";
    public static final String TLS_TRUST_ALIAS = "tls-trust";
    public static final int MAX_TABLES = 256;
    public static final int MAX_TOTAL_COLUMNS = 4096;
    public static final int MAX_CONFIGURATION_BYTES = 512 * 1024;
    public static final int MAX_TRANSACTION_CHANGES = 1_000_000;
    public static final long MAX_TRANSACTION_BYTES = 1024L * 1024 * 1024;
    public static final long MAX_RETAINED_WAL_BYTES = 1024L * 1024
            * 1024 * 1024;

    private static final String CODE = "POSTGRES_CDC_CONFIGURATION_INVALID";
    private static final byte[] MAGIC =
            "RWXPGCF1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_PAYLOAD_BYTES = 256 * 1024;
    private static final int OPTION_CHUNK_CHARS = 4096;
    private static final String OPTION_PREFIX = "postgres.config.";

    private final String host;
    private final int port;
    private final String database;
    private final String slot;
    private final String publication;
    private final String databaseSystemIdentifier;
    private final long databaseOid;
    private final PostgresTlsMode tlsMode;
    private final String credentialsReference;
    private final String tlsTrustReference;
    private final List<PostgresTableBinding> tables;
    private final int maximumTransactionChanges;
    private final long maximumTransactionBytes;
    private final long maximumRetainedWalBytes;
    private final int snapshotFetchRows;
    private final int connectTimeoutMillis;
    private final int socketTimeoutMillis;
    private final int statusIntervalMillis;
    private final StreamSchema streamSchema;
    private final StreamSchema stableKeySchema;
    private final List<String> stableKeyFieldIds;

    private PostgresConnectorConfiguration(Builder builder) {
        this.host = text(builder.host, 253);
        this.port = builder.port;
        this.database = text(builder.database, 63);
        this.slot = slot(builder.slot);
        this.publication = text(builder.publication, 63);
        this.databaseSystemIdentifier =
                PostgresCodecSupport.requireSystemIdentifier(
                        builder.databaseSystemIdentifier, CODE);
        this.databaseOid = PostgresCodecSupport.requireUint32(
                builder.databaseOid, false, CODE);
        this.tlsMode = builder.tlsMode;
        this.credentialsReference = text(builder.credentialsReference, 512);
        this.tlsTrustReference = builder.tlsTrustReference == null
                ? null : text(builder.tlsTrustReference, 512);
        this.tables = tables(builder.tables);
        this.streamSchema = schema(this.tables.get(0));
        this.stableKeyFieldIds = keyFieldIds(this.tables.get(0));
        this.stableKeySchema = keySchema(
                streamSchema, stableKeyFieldIds);
        requireCompatibleTables(this.tables, streamSchema,
                stableKeyFieldIds);
        this.maximumTransactionChanges = builder.maximumTransactionChanges;
        this.maximumTransactionBytes = builder.maximumTransactionBytes;
        this.maximumRetainedWalBytes = builder.maximumRetainedWalBytes;
        this.snapshotFetchRows = builder.snapshotFetchRows;
        this.connectTimeoutMillis = builder.connectTimeoutMillis;
        this.socketTimeoutMillis = builder.socketTimeoutMillis;
        this.statusIntervalMillis = builder.statusIntervalMillis;
        if (port < 1 || port > 65_535 || tlsMode == null
                || ((tlsMode == PostgresTlsMode.DISABLED
                || tlsMode == PostgresTlsMode.REQUIRE)
                && tlsTrustReference != null)
                || maximumTransactionChanges < 1
                || maximumTransactionChanges > MAX_TRANSACTION_CHANGES
                || maximumTransactionBytes < 1L
                || maximumTransactionBytes > MAX_TRANSACTION_BYTES
                || maximumRetainedWalBytes < maximumTransactionBytes
                || maximumRetainedWalBytes > MAX_RETAINED_WAL_BYTES
                || snapshotFetchRows < 1 || snapshotFetchRows > 100_000
                || connectTimeoutMillis < 100
                || connectTimeoutMillis > 120_000
                || socketTimeoutMillis < 100
                || socketTimeoutMillis > 600_000
                || statusIntervalMillis < 100
                || statusIntervalMillis > 60_000) {
            throw invalid();
        }
        // A public value must never be constructible if it cannot cross the
        // signed deployment/configuration boundary it represents.
        encode();
    }

    public static Builder builder() { return new Builder(); }

    public byte[] encode() {
        byte[] payload = PostgresCodecSupport.encode(
                MAGIC, MAX_PAYLOAD_BYTES,
                this::writePayload, CODE);
        String base64 = Base64.getEncoder().encodeToString(payload);
        Map<String, String> options = new LinkedHashMap<>();
        int chunks = (base64.length() + OPTION_CHUNK_CHARS - 1)
                / OPTION_CHUNK_CHARS;
        if (chunks < 1 || chunks > 99) throw invalid();
        for (int index = 0; index < chunks; index++) {
            int start = index * OPTION_CHUNK_CHARS;
            int end = Math.min(base64.length(),
                    start + OPTION_CHUNK_CHARS);
            options.put(optionName(index), base64.substring(start, end));
        }
        Map<String, String> secrets = new LinkedHashMap<>();
        secrets.put(CREDENTIALS_ALIAS, credentialsReference);
        if (tlsTrustReference != null) {
            secrets.put(TLS_TRUST_ALIAS, tlsTrustReference);
        }
        byte[] frame = StreamConnectorConfiguration.of(
                options, secrets).encode();
        if (frame.length > MAX_CONFIGURATION_BYTES) throw invalid();
        return frame;
    }

    public static PostgresConnectorConfiguration decode(byte[] frame) {
        if (frame == null || frame.length > MAX_CONFIGURATION_BYTES) {
            throw invalid();
        }
        final StreamConnectorConfiguration envelope;
        try {
            envelope = StreamConnectorConfiguration.decode(frame);
        } catch (RuntimeException malformed) {
            throw invalid();
        }
        Map<String, String> options = envelope.options();
        Map<String, String> secrets = envelope.secretReferences();
        if (options.isEmpty() || options.size() > 99
                || !secrets.containsKey(CREDENTIALS_ALIAS)
                || secrets.size() < 1 || secrets.size() > 2
                || (secrets.size() == 2
                && !secrets.containsKey(TLS_TRUST_ALIAS))) {
            throw invalid();
        }
        StringBuilder base64 = new StringBuilder();
        for (int index = 0; index < options.size(); index++) {
            String value = options.get(optionName(index));
            if (value == null || value.isEmpty()
                    || value.length() > OPTION_CHUNK_CHARS
                    || (index + 1 < options.size()
                    && value.length() != OPTION_CHUNK_CHARS)) {
                throw invalid();
            }
            base64.append(value);
        }
        final byte[] payload;
        try {
            payload = Base64.getDecoder().decode(base64.toString());
        } catch (IllegalArgumentException malformed) {
            throw invalid();
        }
        PostgresConnectorConfiguration result =
                PostgresCodecSupport.decode(payload, MAGIC,
                        MAX_PAYLOAD_BYTES, input -> readPayload(input,
                                secrets.get(CREDENTIALS_ALIAS),
                                secrets.get(TLS_TRUST_ALIAS)), CODE);
        if (!MessageDigest.isEqual(frame, result.encode())) throw invalid();
        return result;
    }

    private void writePayload(DataOutputStream output) throws IOException {
        PostgresCodecSupport.writeText(output, host, 253, CODE);
        output.writeInt(port);
        PostgresCodecSupport.writeText(output, database, 63, CODE);
        PostgresCodecSupport.writeText(output, slot, 63, CODE);
        PostgresCodecSupport.writeText(output, publication, 63, CODE);
        output.writeLong(PostgresCodecSupport.systemIdentifierBits(
                databaseSystemIdentifier, CODE));
        output.writeInt((int) databaseOid);
        output.writeByte(tlsMode.ordinal());
        output.writeInt(maximumTransactionChanges);
        output.writeLong(maximumTransactionBytes);
        output.writeLong(maximumRetainedWalBytes);
        output.writeInt(snapshotFetchRows);
        output.writeInt(connectTimeoutMillis);
        output.writeInt(socketTimeoutMillis);
        output.writeInt(statusIntervalMillis);
        output.writeInt(tables.size());
        for (PostgresTableBinding table : tables) writeTable(output, table);
    }

    private static void writeTable(DataOutputStream output,
            PostgresTableBinding table) throws IOException {
        PostgresCodecSupport.writeText(
                output, table.schemaName(), 63, CODE);
        PostgresCodecSupport.writeText(
                output, table.tableName(), 63, CODE);
        output.writeInt((int) table.tableOid());
        output.writeByte(table.replicaIdentity().ordinal());
        output.writeInt((int) table.replicaIdentityIndexOid());
        output.writeInt(table.columns().size());
        for (PostgresColumnBinding column : table.columns()) {
            PostgresCodecSupport.writeText(
                    output, column.fieldId(), 256, CODE);
            PostgresCodecSupport.writeText(
                    output, column.columnName(), 63, CODE);
            output.writeInt(column.attributeNumber());
            output.writeInt((int) column.typeOid());
            output.writeInt(column.typeModifier());
            output.writeBoolean(column.nullable());
        }
        output.writeInt(table.primaryKeyAttributeNumbers().size());
        for (Integer attribute : table.primaryKeyAttributeNumbers()) {
            output.writeInt(attribute);
        }
    }

    private static PostgresConnectorConfiguration readPayload(
            DataInputStream input, String credentialsReference,
            String tlsTrustReference) throws IOException {
        Builder builder = builder()
                .host(PostgresCodecSupport.readText(input, 253, CODE))
                .port(input.readInt())
                .database(PostgresCodecSupport.readText(input, 63, CODE))
                .slot(PostgresCodecSupport.readText(input, 63, CODE))
                .publication(PostgresCodecSupport.readText(input, 63, CODE))
                .databaseSystemIdentifier(Long.toUnsignedString(
                        input.readLong()))
                .databaseOid(Integer.toUnsignedLong(input.readInt()));
        int tls = input.readUnsignedByte();
        if (tls >= PostgresTlsMode.values().length) throw invalid();
        builder.tlsMode(PostgresTlsMode.values()[tls])
                .credentialsReference(credentialsReference)
                .tlsTrustReference(tlsTrustReference)
                .maximumTransactionChanges(input.readInt())
                .maximumTransactionBytes(input.readLong())
                .maximumRetainedWalBytes(input.readLong())
                .snapshotFetchRows(input.readInt())
                .connectTimeoutMillis(input.readInt())
                .socketTimeoutMillis(input.readInt())
                .statusIntervalMillis(input.readInt());
        int tableCount = input.readInt();
        if (tableCount < 1 || tableCount > MAX_TABLES) throw invalid();
        List<PostgresTableBinding> tables = new ArrayList<>(tableCount);
        int totalColumns = 0;
        for (int index = 0; index < tableCount; index++) {
            PostgresTableBinding table = readTable(input);
            totalColumns += table.columns().size();
            if (totalColumns > MAX_TOTAL_COLUMNS) throw invalid();
            tables.add(table);
        }
        return builder.tables(tables).build();
    }

    private static PostgresTableBinding readTable(DataInputStream input)
            throws IOException {
        String schema = PostgresCodecSupport.readText(input, 63, CODE);
        String table = PostgresCodecSupport.readText(input, 63, CODE);
        long oid = Integer.toUnsignedLong(input.readInt());
        int identity = input.readUnsignedByte();
        if (identity >= PostgresReplicaIdentity.values().length) {
            throw invalid();
        }
        long identityIndex = Integer.toUnsignedLong(input.readInt());
        int columnCount = input.readInt();
        if (columnCount < 1 || columnCount > PostgresTableBinding.MAX_COLUMNS) {
            throw invalid();
        }
        List<PostgresColumnBinding> columns = new ArrayList<>(columnCount);
        for (int index = 0; index < columnCount; index++) {
            columns.add(new PostgresColumnBinding(
                    PostgresCodecSupport.readText(input, 256, CODE),
                    PostgresCodecSupport.readText(input, 63, CODE),
                    input.readInt(),
                    Integer.toUnsignedLong(input.readInt()),
                    input.readInt(), input.readBoolean()));
        }
        int keyCount = input.readInt();
        if (keyCount < 1 || keyCount > columnCount) throw invalid();
        List<Integer> keys = new ArrayList<>(keyCount);
        for (int index = 0; index < keyCount; index++) {
            keys.add(input.readInt());
        }
        return new PostgresTableBinding(schema, table, oid,
                PostgresReplicaIdentity.values()[identity], identityIndex,
                columns, keys);
    }

    private static List<PostgresTableBinding> tables(
            List<PostgresTableBinding> values) {
        if (values == null || values.isEmpty() || values.size() > MAX_TABLES) {
            throw invalid();
        }
        List<PostgresTableBinding> copy = new ArrayList<>(values.size());
        Set<Long> oids = new HashSet<>();
        Set<String> names = new HashSet<>();
        int columns = 0;
        for (PostgresTableBinding table : values) {
            if (table == null || !oids.add(table.tableOid())
                    || !names.add(table.schemaName() + '\0'
                    + table.tableName())) {
                throw invalid();
            }
            columns += table.columns().size();
            if (columns > MAX_TOTAL_COLUMNS) throw invalid();
            copy.add(table);
        }
        copy.sort(Comparator.comparing(PostgresTableBinding::schemaName)
                .thenComparing(PostgresTableBinding::tableName)
                .thenComparingLong(PostgresTableBinding::tableOid));
        return Collections.unmodifiableList(copy);
    }

    private static String text(String value, int maximumBytes) {
        PostgresCodecSupport.utf8(value, maximumBytes, false, CODE);
        return value;
    }

    private static String slot(String value) {
        text(value, 63);
        for (int index = 0; index < value.length(); index++) {
            char item = value.charAt(index);
            if (!((item >= 'a' && item <= 'z')
                    || (item >= '0' && item <= '9') || item == '_')) {
                throw invalid();
            }
        }
        return value;
    }

    private static String optionName(int index) {
        if (index < 0 || index > 99) throw invalid();
        return OPTION_PREFIX + (index < 10 ? "00" : index < 100 ? "0" : "")
                + index;
    }

    public String host() { return host; }
    public int port() { return port; }
    public String database() { return database; }
    public String slot() { return slot; }
    public String publication() { return publication; }
    public String databaseSystemIdentifier() {
        return databaseSystemIdentifier;
    }
    public long databaseOid() { return databaseOid; }
    public PostgresTlsMode tlsMode() { return tlsMode; }
    public String credentialsReference() { return credentialsReference; }
    public boolean hasTlsTrustReference() {
        return tlsTrustReference != null;
    }
    public String tlsTrustReference() {
        if (tlsTrustReference == null) {
            throw new IllegalStateException("no PostgreSQL TLS trust reference");
        }
        return tlsTrustReference;
    }
    public List<PostgresTableBinding> tables() { return tables; }
    public int maximumTransactionChanges() {
        return maximumTransactionChanges;
    }
    public long maximumTransactionBytes() { return maximumTransactionBytes; }
    public long maximumRetainedWalBytes() {
        return maximumRetainedWalBytes;
    }
    public int snapshotFetchRows() { return snapshotFetchRows; }
    public int connectTimeoutMillis() { return connectTimeoutMillis; }
    public int socketTimeoutMillis() { return socketTimeoutMillis; }
    public int statusIntervalMillis() { return statusIntervalMillis; }
    public StreamSchema streamSchema() { return streamSchema; }
    public StreamSchema stableKeySchema() { return stableKeySchema; }
    public List<String> stableKeyFieldIds() {
        return stableKeyFieldIds;
    }

    /** Declarative resident CDC v2 source using one atomic snapshot split. */
    public StreamSourceSpec toSourceSpec() {
        return StreamSourceSpec.residentCdcV2(
                streamSchema, stableKeyFieldIds,
                PostgresConnectorContract.PROVIDER_KEY,
                PostgresConnectorContract.contractDigest(), encode(),
                snapshotIdentity(), 0L,
                Collections.singletonList("snapshot-all"));
    }

    public String snapshotIdentity() {
        return PostgresCodecSupport.hex(
                PostgresCodecSupport.sha256(encode()));
    }

    @Override public String toString() {
        return "PostgresConnectorConfiguration{tables=" + tables.size()
                + ",maxTransactionChanges=" + maximumTransactionChanges
                + ",maxTransactionBytes=" + maximumTransactionBytes + '}';
    }

    private static IllegalArgumentException invalid() {
        return PostgresCodecSupport.invalid(CODE);
    }

    private static StreamSchema schema(PostgresTableBinding table) {
        List<StreamField> fields = new ArrayList<>(table.columns().size());
        for (PostgresColumnBinding column : table.columns()) {
            fields.add(new StreamField(column.fieldId(),
                    column.columnName(), column.columnName(),
                    PostgresTypeMapping.dataType(column.typeOid()),
                    column.nullable()));
        }
        return new StreamSchema(fields);
    }

    private static List<String> keyFieldIds(PostgresTableBinding table) {
        List<String> result = new ArrayList<>(
                table.primaryKeyAttributeNumbers().size());
        for (Integer attribute : table.primaryKeyAttributeNumbers()) {
            for (PostgresColumnBinding column : table.columns()) {
                if (column.attributeNumber() == attribute) {
                    result.add(column.fieldId());
                    break;
                }
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static StreamSchema keySchema(StreamSchema valueSchema,
            List<String> keyFieldIds) {
        List<StreamField> fields = new ArrayList<>(keyFieldIds.size());
        for (String fieldId : keyFieldIds) {
            for (StreamField field : valueSchema.fields()) {
                if (field.fieldId().equals(fieldId)) {
                    fields.add(field);
                    break;
                }
            }
        }
        return new StreamSchema(fields);
    }

    private static void requireCompatibleTables(
            List<PostgresTableBinding> tables, StreamSchema schema,
            List<String> keyFieldIds) {
        for (PostgresTableBinding table : tables) {
            if (!schema.equals(schema(table))
                    || !keyFieldIds.equals(keyFieldIds(table))) {
                throw invalid();
            }
        }
    }

    public static final class Builder {
        private String host;
        private int port = 5432;
        private String database;
        private String slot;
        private String publication;
        private String databaseSystemIdentifier;
        private long databaseOid;
        private PostgresTlsMode tlsMode = PostgresTlsMode.VERIFY_FULL;
        private String credentialsReference;
        private String tlsTrustReference;
        private List<PostgresTableBinding> tables;
        private int maximumTransactionChanges = 10_000;
        private long maximumTransactionBytes = 32L * 1024 * 1024;
        private long maximumRetainedWalBytes = 4L * 1024 * 1024 * 1024;
        private int snapshotFetchRows = 2_000;
        private int connectTimeoutMillis = 10_000;
        private int socketTimeoutMillis = 60_000;
        private int statusIntervalMillis = 10_000;

        private Builder() { }
        public Builder host(String value) { host = value; return this; }
        public Builder port(int value) { port = value; return this; }
        public Builder database(String value) { database = value; return this; }
        public Builder slot(String value) { slot = value; return this; }
        public Builder publication(String value) {
            publication = value; return this;
        }
        public Builder databaseSystemIdentifier(String value) {
            databaseSystemIdentifier = value; return this;
        }
        public Builder databaseOid(long value) {
            databaseOid = value; return this;
        }
        public Builder tlsMode(PostgresTlsMode value) {
            tlsMode = value; return this;
        }
        public Builder credentialsReference(String value) {
            credentialsReference = value; return this;
        }
        public Builder tlsTrustReference(String value) {
            tlsTrustReference = value; return this;
        }
        public Builder tables(List<PostgresTableBinding> value) {
            tables = value; return this;
        }
        public Builder maximumTransactionChanges(int value) {
            maximumTransactionChanges = value; return this;
        }
        public Builder maximumTransactionBytes(long value) {
            maximumTransactionBytes = value; return this;
        }
        public Builder maximumRetainedWalBytes(long value) {
            maximumRetainedWalBytes = value; return this;
        }
        public Builder snapshotFetchRows(int value) {
            snapshotFetchRows = value; return this;
        }
        public Builder connectTimeoutMillis(int value) {
            connectTimeoutMillis = value; return this;
        }
        public Builder socketTimeoutMillis(int value) {
            socketTimeoutMillis = value; return this;
        }
        public Builder statusIntervalMillis(int value) {
            statusIntervalMillis = value; return this;
        }
        public PostgresConnectorConfiguration build() {
            return new PostgresConnectorConfiguration(this);
        }
    }
}
