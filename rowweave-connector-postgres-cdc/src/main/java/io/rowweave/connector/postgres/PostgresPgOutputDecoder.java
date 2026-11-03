package io.rowweave.connector.postgres;

import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamUpsertCodec;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Strict pgoutput protocol-v1 decoder with one bounded transaction buffer. */
final class PostgresPgOutputDecoder {
    private final PostgresConnectorConfiguration configuration;
    private final Map<Long, PostgresTableBinding> tables = new HashMap<>();
    private final Set<Long> verifiedRelations = new HashSet<>();
    private final StreamUpsertCodec upsertCodec;
    private final StreamDataRowCodec keyCodec;
    private Transaction transaction;

    PostgresPgOutputDecoder(PostgresConnectorConfiguration configuration) {
        if (configuration == null) {
            throw failure("POSTGRES_CDC_CONFIGURATION_INVALID");
        }
        this.configuration = configuration;
        for (PostgresTableBinding table : configuration.tables()) {
            tables.put(table.tableOid(), table);
        }
        this.upsertCodec = new StreamUpsertCodec(
                configuration.stableKeySchema(),
                configuration.streamSchema());
        this.keyCodec = new StreamDataRowCodec(configuration.stableKeySchema());
    }

    PostgresCommittedTransaction accept(ByteBuffer message) {
        if (message == null || !message.hasRemaining()) {
            throw protocol();
        }
        ByteBuffer input = message.slice();
        int messageBytes = input.remaining();
        int kind = unsignedByte(input);
        PostgresCommittedTransaction result;
        switch (kind) {
            case 'R':
                if (transaction != null) {
                    transaction.addBytes(messageBytes,
                            configuration.maximumTransactionBytes());
                }
                relation(input);
                result = null;
                break;
            case 'B':
                begin(input, messageBytes);
                result = null;
                break;
            case 'I':
                requireTransaction(messageBytes);
                insert(input);
                result = null;
                break;
            case 'U':
                requireTransaction(messageBytes);
                update(input);
                result = null;
                break;
            case 'D':
                requireTransaction(messageBytes);
                delete(input);
                result = null;
                break;
            case 'C':
                result = commit(input, messageBytes);
                break;
            case 'T':
                throw failure("POSTGRES_CDC_TRUNCATE_UNSUPPORTED");
            default:
                throw failure("POSTGRES_CDC_MESSAGE_UNSUPPORTED");
        }
        if (input.hasRemaining()) throw protocol();
        return result;
    }

    private void relation(ByteBuffer input) {
        long oid = uint32(input);
        PostgresTableBinding expected = tables.get(oid);
        if (expected == null) throw drift();
        String schema = cstring(input, 63);
        String table = cstring(input, 63);
        int identity = unsignedByte(input);
        int count = unsignedShort(input);
        if (!schema.equals(expected.schemaName())
                || !table.equals(expected.tableName())
                || identity != identityCode(expected.replicaIdentity())
                || count != expected.columns().size()) {
            throw drift();
        }
        Set<Integer> keys = new HashSet<>(
                expected.primaryKeyAttributeNumbers());
        for (int index = 0; index < count; index++) {
            int flags = unsignedByte(input);
            PostgresColumnBinding column = expected.columns().get(index);
            if ((flags & ~1) != 0
                    || ((flags & 1) != 0)
                    != keys.contains(column.attributeNumber())
                    || !cstring(input, 63).equals(column.columnName())
                    || uint32(input) != column.typeOid()
                    || integer(input) != column.typeModifier()) {
                throw drift();
            }
        }
        verifiedRelations.add(oid);
    }

    private void begin(ByteBuffer input, int messageBytes) {
        requireNoTransaction();
        long finalLsn = longValue(input);
        long commitTime = longValue(input);
        long xid = uint32(input);
        if (xid == 0L) throw protocol();
        Transaction next = new Transaction(xid, finalLsn, commitTime);
        next.addBytes(messageBytes, configuration.maximumTransactionBytes());
        transaction = next;
    }

    private void insert(ByteBuffer input) {
        PostgresTableBinding table = table(input);
        if (unsignedByte(input) != 'N') throw protocol();
        Tuple after = tuple(input, table);
        DataRow row = afterImage(after, null, table);
        add(upsertCodec.encodeAfterImage(key(row, table), row));
    }

    private void update(ByteBuffer input) {
        PostgresTableBinding table = table(input);
        int marker = unsignedByte(input);
        Tuple old = null;
        if (marker == 'K' || marker == 'O') {
            old = tuple(input, table);
            marker = unsignedByte(input);
        }
        if (marker != 'N') throw protocol();
        Tuple next = tuple(input, table);
        DataRow after = afterImage(next, old, table);
        DataRow newKey = key(after, table);
        DataRow oldKey = old == null ? newKey : key(old, table);
        byte[] oldKeyFrame = keyCodec.encode(oldKey);
        byte[] newKeyFrame = keyCodec.encode(newKey);
        if (!MessageDigest.isEqual(oldKeyFrame, newKeyFrame)) {
            add(upsertCodec.encodeTombstone(oldKey));
        }
        add(upsertCodec.encodeAfterImage(newKey, after));
    }

    private void delete(ByteBuffer input) {
        PostgresTableBinding table = table(input);
        int marker = unsignedByte(input);
        if (marker != 'K' && marker != 'O') {
            throw failure("POSTGRES_CDC_OLD_KEY_REQUIRED");
        }
        add(upsertCodec.encodeTombstone(key(tuple(input, table), table)));
    }

    private PostgresCommittedTransaction commit(ByteBuffer input,
            int messageBytes) {
        if (transaction == null) throw protocol();
        transaction.addBytes(messageBytes,
                configuration.maximumTransactionBytes());
        int flags = unsignedByte(input);
        long commitLsn = longValue(input);
        long endLsn = longValue(input);
        long commitTime = longValue(input);
        if (flags != 0 || Long.compareUnsigned(endLsn, commitLsn) < 0
                || Long.compareUnsigned(commitLsn,
                transaction.finalLsn) > 0
                || commitTime < transaction.commitTime) {
            throw protocol();
        }
        Transaction completed = transaction;
        transaction = null;
        PostgresCdcPosition position =
                PostgresCdcPosition.fromBits(
                        PostgresCdcPosition.Boundary.TRANSACTION_COMMIT,
                        PostgresCodecSupport.systemIdentifierBits(
                                configuration.databaseSystemIdentifier(),
                                "POSTGRES_CDC_POSITION_INVALID"),
                        configuration.databaseOid(), endLsn,
                        completed.xid);
        return new PostgresCommittedTransaction(
                completed.xid, position, completed.frames);
    }

    private PostgresTableBinding table(ByteBuffer input) {
        long oid = uint32(input);
        PostgresTableBinding result = tables.get(oid);
        if (result == null || !verifiedRelations.contains(oid)) {
            throw drift();
        }
        return result;
    }

    private Tuple tuple(ByteBuffer input, PostgresTableBinding table) {
        int count = unsignedShort(input);
        if (count != table.columns().size()) throw drift();
        Object[] values = new Object[count];
        boolean[] unchanged = new boolean[count];
        for (int index = 0; index < count; index++) {
            int kind = unsignedByte(input);
            PostgresColumnBinding column = table.columns().get(index);
            if (kind == 'n') {
                values[index] = null;
            } else if (kind == 'u') {
                unchanged[index] = true;
            } else if (kind == 't') {
                int length = integer(input);
                if (length < 0
                        || length > configuration.maximumTransactionBytes()
                        || input.remaining() < length) throw protocol();
                byte[] bytes = new byte[length];
                input.get(bytes);
                values[index] = PostgresTypeMapping.decode(bytes, column);
            } else {
                // Binary tuple data is deliberately not requested in v1.
                throw failure("POSTGRES_CDC_TUPLE_FORMAT_UNSUPPORTED");
            }
        }
        return new Tuple(values, unchanged);
    }

    private DataRow afterImage(Tuple next, Tuple old,
            PostgresTableBinding table) {
        DataRow row = new DataRow(next.values.length);
        for (int index = 0; index < next.values.length; index++) {
            Object value;
            if (next.unchanged[index]) {
                if (old == null || old.unchanged[index]) {
                    throw failure("POSTGRES_CDC_TOAST_UNRESOLVED");
                }
                value = old.values[index];
                if (value == null) {
                    throw failure("POSTGRES_CDC_TOAST_UNRESOLVED");
                }
            } else {
                value = next.values[index];
            }
            if (value == null && !table.columns().get(index).nullable()) {
                throw failure("POSTGRES_CDC_VALUE_INVALID");
            }
            row.set(index, value);
        }
        return row;
    }

    private DataRow key(Tuple tuple, PostgresTableBinding table) {
        DataRow row = new DataRow(tuple.values.length);
        for (int index = 0; index < tuple.values.length; index++) {
            if (!tuple.unchanged[index]) row.set(index, tuple.values[index]);
        }
        return key(row, table);
    }

    private DataRow key(DataRow row, PostgresTableBinding table) {
        DataRow key = new DataRow(table.primaryKeyAttributeNumbers().size());
        int target = 0;
        for (Integer attribute : table.primaryKeyAttributeNumbers()) {
            int source = columnIndex(table, attribute);
            Object value = row.get(source);
            if (value == null) {
                throw failure("POSTGRES_CDC_OLD_KEY_REQUIRED");
            }
            key.set(target++, value);
        }
        return key;
    }

    private static int columnIndex(PostgresTableBinding table,
            int attribute) {
        for (int index = 0; index < table.columns().size(); index++) {
            if (table.columns().get(index).attributeNumber() == attribute) {
                return index;
            }
        }
        throw drift();
    }

    private void add(byte[] frame) {
        transaction.add(frame,
                configuration.maximumTransactionChanges(),
                configuration.maximumTransactionBytes());
    }

    private void requireTransaction(int messageBytes) {
        if (transaction == null) throw protocol();
        transaction.addBytes(messageBytes,
                configuration.maximumTransactionBytes());
    }

    private void requireNoTransaction() {
        if (transaction != null) throw protocol();
    }

    private static int identityCode(PostgresReplicaIdentity identity) {
        switch (identity) {
            case DEFAULT: return 'd';
            case USING_INDEX: return 'i';
            case FULL: return 'f';
            default: throw drift();
        }
    }

    private static int unsignedByte(ByteBuffer input) {
        if (input.remaining() < 1) throw protocol();
        return input.get() & 0xff;
    }

    private static int unsignedShort(ByteBuffer input) {
        if (input.remaining() < 2) throw protocol();
        return input.getShort() & 0xffff;
    }

    private static int integer(ByteBuffer input) {
        if (input.remaining() < 4) throw protocol();
        return input.getInt();
    }

    private static long uint32(ByteBuffer input) {
        return Integer.toUnsignedLong(integer(input));
    }

    private static long longValue(ByteBuffer input) {
        if (input.remaining() < 8) throw protocol();
        return input.getLong();
    }

    private static String cstring(ByteBuffer input, int maximumBytes) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        boolean terminated = false;
        while (input.hasRemaining()) {
            byte value = input.get();
            if (value == 0) {
                terminated = true;
                break;
            }
            if (bytes.size() >= maximumBytes) throw protocol();
            bytes.write(value);
        }
        if (!terminated || bytes.size() == 0) throw protocol();
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes.toByteArray())).toString();
        } catch (CharacterCodingException malformed) {
            throw protocol();
        }
    }

    private static IllegalArgumentException protocol() {
        return failure("POSTGRES_CDC_PROTOCOL_INVALID");
    }

    private static IllegalArgumentException drift() {
        return failure("POSTGRES_CDC_SCHEMA_DRIFT");
    }

    private static IllegalArgumentException failure(String code) {
        return new IllegalArgumentException(code);
    }

    private static final class Tuple {
        private final Object[] values;
        private final boolean[] unchanged;
        private Tuple(Object[] values, boolean[] unchanged) {
            this.values = values;
            this.unchanged = unchanged;
        }
    }

    private static final class Transaction {
        private final long xid;
        private final long finalLsn;
        private final long commitTime;
        private final List<byte[]> frames = new ArrayList<>();
        private long bytes;

        private Transaction(long xid, long finalLsn, long commitTime) {
            this.xid = xid;
            this.finalLsn = finalLsn;
            this.commitTime = commitTime;
        }

        private void addBytes(long amount, long maximumBytes) {
            if (amount < 0L || bytes > maximumBytes - amount) {
                throw failure("POSTGRES_CDC_TRANSACTION_LIMIT");
            }
            bytes += amount;
        }

        private void add(byte[] frame, int maximumChanges,
                long maximumBytes) {
            if (frames.size() >= maximumChanges) {
                throw failure("POSTGRES_CDC_TRANSACTION_LIMIT");
            }
            addBytes(frame.length, maximumBytes);
            frames.add(frame);
        }
    }
}
