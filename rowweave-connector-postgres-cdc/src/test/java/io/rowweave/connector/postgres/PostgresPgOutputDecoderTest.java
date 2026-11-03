package io.rowweave.connector.postgres;

import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamUpsertCodec;
import io.rowweave.stream.api.StreamUpsertValue;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresPgOutputDecoderTest {
    private static final long TABLE_OID = 16_384L;

    @Test
    void committedTransactionProducesAfterImagesKeyMoveAndTombstones()
            throws Exception {
        PostgresConnectorConfiguration configuration = configuration(16,
                1024 * 1024);
        PostgresPgOutputDecoder decoder =
                new PostgresPgOutputDecoder(configuration);
        StreamUpsertCodec upserts = new StreamUpsertCodec(
                configuration.stableKeySchema(),
                configuration.streamSchema());

        assertNull(decoder.accept(relation(20L, 1700L)));
        assertNull(decoder.accept(begin(7L)));
        assertNull(decoder.accept(insert("1", "10.00")));
        assertNull(decoder.accept(update(null,
                tuple(text("1"), text("12.50")))));
        assertNull(decoder.accept(update(
                tuple(text("1"), nil()),
                tuple(text("2"), text("20.00")))));
        assertNull(decoder.accept(delete(tuple(text("2"), nil()))));

        PostgresCommittedTransaction committed = decoder.accept(
                commit(0x16b6d00L, 0x16b6d20L));
        assertEquals(7L, committed.transactionId());
        assertEquals("0/16B6D20", committed.position().endLsn());
        assertEquals(5, committed.upsertFrames().size());

        assertAfter(upserts, committed.upsertFrames().get(0), 1L, "10.00");
        assertAfter(upserts, committed.upsertFrames().get(1), 1L, "12.50");
        assertTombstone(configuration, committed.upsertFrames().get(2), 1L);
        assertAfter(upserts, committed.upsertFrames().get(3), 2L, "20.00");
        assertTombstone(configuration, committed.upsertFrames().get(4), 2L);

        byte[] mutated = committed.upsertFrames().get(0);
        mutated[0] ^= 1;
        assertFalse(Arrays.equals(mutated,
                committed.upsertFrames().get(0)),
                "committed transaction must own defensive frame copies");
        assertThrows(UnsupportedOperationException.class,
                () -> committed.upsertFrames().clear());
    }

    @Test
    void schemaDriftTruncateToastAndTransactionOverflowFailClosed()
            throws Exception {
        PostgresPgOutputDecoder drift = new PostgresPgOutputDecoder(
                configuration(16, 1024 * 1024));
        assertCode("POSTGRES_CDC_SCHEMA_DRIFT",
                () -> drift.accept(relation(23L, 1700L)));

        PostgresPgOutputDecoder truncate = new PostgresPgOutputDecoder(
                configuration(16, 1024 * 1024));
        truncate.accept(relation(20L, 1700L));
        truncate.accept(begin(8L));
        assertCode("POSTGRES_CDC_TRUNCATE_UNSUPPORTED",
                () -> truncate.accept(truncate()));

        PostgresPgOutputDecoder toast = new PostgresPgOutputDecoder(
                configuration(16, 1024 * 1024));
        toast.accept(relation(20L, 1700L));
        toast.accept(begin(9L));
        assertCode("POSTGRES_CDC_TOAST_UNRESOLVED",
                () -> toast.accept(update(null,
                        tuple(text("1"), unchangedToast()))));

        PostgresPgOutputDecoder overflow = new PostgresPgOutputDecoder(
                configuration(2, 1024 * 1024));
        overflow.accept(relation(20L, 1700L));
        overflow.accept(begin(10L));
        overflow.accept(insert("1", "1.00"));
        overflow.accept(insert("2", "2.00"));
        assertCode("POSTGRES_CDC_TRANSACTION_LIMIT",
                () -> overflow.accept(insert("3", "3.00")));
    }

    @Test
    void transactionByteBudgetAccountsForTheEntireDecodedTransaction()
            throws Exception {
        String amount = "1234.50";
        PostgresCommittedTransaction probe = committed(
                configuration(4, 1024 * 1024), amount);
        long exactBytes = begin(11L).remaining()
                + insert("1", amount).remaining()
                + commit(0x16b6d00L, 0x16b6d20L).remaining()
                + probe.upsertFrames().get(0).length;

        assertCode("POSTGRES_CDC_TRANSACTION_LIMIT", () -> committed(
                configuration(4, exactBytes - 1L), amount));
        assertEquals(1, committed(configuration(4, exactBytes),
                amount).upsertFrames().size());
        assertEquals(1, committed(configuration(4, exactBytes + 1L),
                amount).upsertFrames().size());
    }

    @Test
    void committedTransactionMayAdvancePositionWithoutPublishedRows()
            throws Exception {
        PostgresPgOutputDecoder decoder = new PostgresPgOutputDecoder(
                configuration(16, 1024 * 1024));
        decoder.accept(begin(12L));

        PostgresCommittedTransaction committed = decoder.accept(
                commit(0x16b6d00L, 0x16b6d20L));

        assertEquals(12L, committed.transactionId());
        assertTrue(committed.upsertFrames().isEmpty());
        assertEquals("0/16B6D20", committed.position().endLsn());
    }

    private static PostgresCommittedTransaction committed(
            PostgresConnectorConfiguration configuration, String amount)
            throws Exception {
        PostgresPgOutputDecoder decoder =
                new PostgresPgOutputDecoder(configuration);
        decoder.accept(relation(20L, 1700L));
        decoder.accept(begin(11L));
        decoder.accept(insert("1", amount));
        return decoder.accept(commit(0x16b6d00L, 0x16b6d20L));
    }

    private static PostgresConnectorConfiguration configuration(
            int maximumChanges, long maximumBytes) {
        return PostgresConnectorConfiguration.builder()
                .host("postgres.internal").database("ledger")
                .slot("rowweave_ledger").publication("rowweave_publication")
                .databaseSystemIdentifier("123456789")
                .databaseOid(16_000L)
                .tlsMode(PostgresTlsMode.REQUIRE)
                .credentialsReference("vault/postgres")
                .tables(Collections.singletonList(new PostgresTableBinding(
                        "ledger", "entries", TABLE_OID,
                        PostgresReplicaIdentity.DEFAULT, 0L,
                        Arrays.asList(
                                new PostgresColumnBinding(
                                        "id", "id", 1, 20L, -1, false),
                                new PostgresColumnBinding(
                                        "amount", "amount", 2,
                                        1700L, (18 << 16) | 2, true)),
                        Collections.singletonList(1))))
                .maximumTransactionChanges(maximumChanges)
                .maximumTransactionBytes(maximumBytes)
                .maximumRetainedWalBytes(maximumBytes)
                .build();
    }

    private static ByteBuffer relation(long idType, long amountType)
            throws IOException {
        return message(output -> {
            output.writeByte('R');
            output.writeInt((int) TABLE_OID);
            cstring(output, "ledger");
            cstring(output, "entries");
            output.writeByte('d');
            output.writeShort(2);
            output.writeByte(1);
            cstring(output, "id");
            output.writeInt((int) idType);
            output.writeInt(-1);
            output.writeByte(0);
            cstring(output, "amount");
            output.writeInt((int) amountType);
            output.writeInt((18 << 16) | 2);
        });
    }

    private static ByteBuffer begin(long xid) throws IOException {
        return message(output -> {
            output.writeByte('B');
            output.writeLong(0x16b6d20L);
            output.writeLong(123456L);
            output.writeInt((int) xid);
        });
    }

    private static ByteBuffer commit(long commitLsn, long endLsn)
            throws IOException {
        return message(output -> {
            output.writeByte('C');
            output.writeByte(0);
            output.writeLong(commitLsn);
            output.writeLong(endLsn);
            output.writeLong(123457L);
        });
    }

    private static ByteBuffer insert(String id, String amount)
            throws IOException {
        return message(output -> {
            output.writeByte('I');
            output.writeInt((int) TABLE_OID);
            output.writeByte('N');
            writeTuple(output, tuple(text(id), text(amount)));
        });
    }

    private static ByteBuffer update(Tuple oldKey, Tuple after)
            throws IOException {
        return message(output -> {
            output.writeByte('U');
            output.writeInt((int) TABLE_OID);
            if (oldKey != null) {
                output.writeByte('K');
                writeTuple(output, oldKey);
            }
            output.writeByte('N');
            writeTuple(output, after);
        });
    }

    private static ByteBuffer delete(Tuple oldKey) throws IOException {
        return message(output -> {
            output.writeByte('D');
            output.writeInt((int) TABLE_OID);
            output.writeByte('K');
            writeTuple(output, oldKey);
        });
    }

    private static ByteBuffer truncate() throws IOException {
        return message(output -> {
            output.writeByte('T');
            output.writeInt(1);
            output.writeByte(0);
            output.writeInt((int) TABLE_OID);
        });
    }

    private static ByteBuffer message(Writer writer) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writer.write(output);
        }
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private static void writeTuple(DataOutputStream output, Tuple tuple)
            throws IOException {
        output.writeShort(tuple.values.length);
        for (byte[] value : tuple.values) {
            if (value == null) {
                output.writeByte('n');
            } else if (value == UNCHANGED) {
                output.writeByte('u');
            } else {
                output.writeByte('t');
                output.writeInt(value.length);
                output.write(value);
            }
        }
    }

    private static void cstring(DataOutputStream output, String value)
            throws IOException {
        output.write(value.getBytes(StandardCharsets.UTF_8));
        output.writeByte(0);
    }

    private static Tuple tuple(byte[]... values) { return new Tuple(values); }
    private static byte[] text(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
    private static byte[] nil() { return null; }
    private static final byte[] UNCHANGED = new byte[0];
    private static byte[] unchangedToast() { return UNCHANGED; }

    private static void assertAfter(StreamUpsertCodec codec, byte[] frame,
            long id, String amount) {
        StreamUpsertValue value = codec.decode(frame);
        assertEquals(id, value.stableKey().getLong(0));
        assertEquals(id, value.afterImage().getLong(0));
        assertEquals(amount,
                value.afterImage().getBigDecimal(1).toPlainString());
    }

    private static void assertTombstone(
            PostgresConnectorConfiguration configuration,
            byte[] frame, long id) {
        StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                frame, 4 * 1024 * 1024, 4 * 1024 * 1024);
        assertEquals(StreamUpsertRecord.Kind.TOMBSTONE, record.kind());
        DataRow key = new StreamDataRowCodec(configuration.stableKeySchema())
                .decode(record.stableKeyFrame());
        assertEquals(id, key.getLong(0));
    }

    private static void assertCode(String code, Throwing action) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class, action::run);
        assertEquals(code, failure.getMessage());
    }

    private interface Writer { void write(DataOutputStream output)
            throws IOException; }
    private interface Throwing { void run() throws Exception; }
    private static final class Tuple {
        private final byte[][] values;
        private Tuple(byte[][] values) { this.values = values; }
    }
}
