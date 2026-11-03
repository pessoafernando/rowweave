package io.rowweave.connector.postgres;

import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamUpsertCodec;
import io.rowweave.stream.spi.StreamCdcCheckpointCompletion;
import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcSourceEvent;
import io.rowweave.stream.spi.StreamCdcSourceReader;
import io.rowweave.stream.spi.StreamSecretAccess;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresCdcReaderLifecycleTest {
    private static final long TABLE_OID = 16_384L;

    @Test
    void snapshotHandoffLogAndCompletedCheckpointAckAreOrdered()
            throws Exception {
        PostgresConnectorConfiguration configuration = configuration();
        PostgresCdcPosition handoff = PostgresCdcPosition.snapshotHandoff(
                configuration.databaseSystemIdentifier(),
                configuration.databaseOid(), "0/16B6C50");
        FakeClient client = new FakeClient(handoff,
                Collections.singletonList(row(1L, "10.00")),
                Arrays.asList(relation(), begin(7L),
                        insert("2", "20.00"),
                        commit(0x16b6d00L, 0x16b6d20L)));
        PostgresConnectorProvider provider = new PostgresConnectorProvider(
                (actual, credentials, trust) -> {
                    assertArrayEquals(bytes("rowweave_reader"),
                            credentials.username());
                    assertArrayEquals(bytes("secret"),
                            credentials.password());
                    assertNull(trust);
                    return client;
                });

        StreamCdcSourceReader reader = provider.openCdc(context(
                configuration, StreamCdcSourceContext.Phase.SNAPSHOT,
                0L, null));

        StreamCdcSourceEvent snapshot = reader.next();
        assertEquals(StreamCdcSourceEvent.Kind.SNAPSHOT_AFTER_IMAGE,
                snapshot.kind());
        assertEquals(1L, upserts(configuration).decode(
                snapshot.upsertFrame()).stableKey().getLong(0));

        StreamCdcSourceEvent boundary = reader.next();
        assertEquals(StreamCdcSourceEvent.Kind.SNAPSHOT_HANDOFF_COMPLETE,
                boundary.kind());
        assertArrayEquals(PostgresCdcPositionCodec.encode(handoff),
                boundary.completedExternalPositionFrame());
        assertEquals(handoff.endLsn(), client.startedLog.endLsn());
        assertTrue(client.acknowledged.isEmpty(),
                "handoff must not advance the slot before checkpoint completion");

        StreamCdcSourceEvent log = reader.next();
        assertEquals(StreamCdcSourceEvent.Kind.LOG_RECORD, log.kind());
        assertEquals(1L, log.logSequence());
        assertEquals(2L, upserts(configuration).decode(
                log.upsertFrame()).stableKey().getLong(0));
        assertTrue(log.hasCompletedExternalPosition());
        PostgresCdcPosition committed = PostgresCdcPositionCodec.decode(
                log.completedExternalPositionFrame());
        assertEquals("0/16B6D20", committed.endLsn());
        assertTrue(client.acknowledged.isEmpty());

        reader.notifyCheckpointComplete(new StreamCdcCheckpointCompletion(
                3L, 1L, log.completedExternalPositionFrame()));
        assertEquals(Collections.singletonList("0/16B6D20"),
                client.acknowledged);
        reader.wakeUp();
        assertTrue(client.woken);
        reader.cancel();
        assertTrue(client.cancelled);
        reader.close();
        reader.close();
        assertEquals(1, client.closeCount);
    }

    @Test
    void restoredCompletedPositionIsReaffirmedAndAckFailureIsRetryable()
            throws Exception {
        PostgresConnectorConfiguration configuration = configuration();
        PostgresCdcPosition restored = PostgresCdcPosition.transactionCommit(
                configuration.databaseSystemIdentifier(),
                configuration.databaseOid(), "0/16B6D20", 7L);
        FakeClient client = new FakeClient(restored,
                Collections.<DataRow>emptyList(),
                Collections.<ByteBuffer>emptyList());
        client.failNextAck = true;
        PostgresConnectorProvider provider = new PostgresConnectorProvider(
                (actual, credentials, trust) -> client);

        IllegalStateException first = assertThrows(IllegalStateException.class,
                () -> provider.openCdc(context(configuration,
                        StreamCdcSourceContext.Phase.LOG, 5L,
                        PostgresCdcPositionCodec.encode(restored))));
        assertEquals("POSTGRES_CDC_ACK_FAILED", first.getMessage());
        assertEquals(1, client.ackAttempts);
        assertEquals(1, client.closeCount);

        FakeClient reopened = new FakeClient(restored,
                Collections.<DataRow>emptyList(),
                Collections.<ByteBuffer>emptyList());
        PostgresConnectorProvider retrying = new PostgresConnectorProvider(
                (actual, credentials, trust) -> reopened);
        StreamCdcSourceReader reader = retrying.openCdc(context(
                configuration, StreamCdcSourceContext.Phase.LOG, 5L,
                PostgresCdcPositionCodec.encode(restored)));
        assertEquals("0/16B6D20", reopened.startedLog.endLsn());
        assertEquals(Collections.singletonList("0/16B6D20"),
                reopened.acknowledged,
                "a completed restored cut must be reaffirmed on reopen");
        reader.wakeUp();
        assertNull(reader.next());

        PostgresCdcPosition wrongDatabase =
                PostgresCdcPosition.transactionCommit(
                        configuration.databaseSystemIdentifier(), 99L,
                        "0/16B6D20", 7L);
        IllegalArgumentException mismatch = assertThrows(
                IllegalArgumentException.class,
                () -> reader.notifyCheckpointComplete(
                        new StreamCdcCheckpointCompletion(6L, 5L,
                                PostgresCdcPositionCodec.encode(
                                        wrongDatabase))));
        assertEquals("POSTGRES_CDC_POSITION_MISMATCH",
                mismatch.getMessage());
        reader.close();
    }

    @Test
    void retainedWalBudgetHonorsLimitMinusOneExactAndPlusOne()
            throws Exception {
        PostgresConnectorConfiguration configuration = configuration();
        long limit = configuration.maximumRetainedWalBytes();
        PostgresCdcPosition restored = PostgresCdcPosition.transactionCommit(
                configuration.databaseSystemIdentifier(),
                configuration.databaseOid(), "0/16B6C50", 6L);

        FakeClient below = logClient(restored, limit - 1L);
        StreamCdcSourceReader belowReader = openLogReader(
                configuration, restored, below);
        assertEquals(StreamCdcSourceEvent.Kind.LOG_RECORD,
                belowReader.next().kind());
        belowReader.close();

        FakeClient exact = logClient(restored, limit);
        StreamCdcSourceReader exactReader = openLogReader(
                configuration, restored, exact);
        assertEquals(StreamCdcSourceEvent.Kind.LOG_RECORD,
                exactReader.next().kind());
        exactReader.close();

        FakeClient above = logClient(restored, limit + 1L);
        StreamCdcSourceReader aboveReader = openLogReader(
                configuration, restored, above);
        IllegalStateException failure = assertThrows(
                IllegalStateException.class, aboveReader::next);
        assertEquals("POSTGRES_CDC_WAL_RETENTION_LIMIT",
                failure.getMessage());
        assertEquals(1, above.closeCount);
    }

    @Test
    void emptyCommittedTransactionAdvancesCheckpointPositionWithoutRow()
            throws Exception {
        PostgresConnectorConfiguration configuration = configuration();
        PostgresCdcPosition restored = PostgresCdcPosition.transactionCommit(
                configuration.databaseSystemIdentifier(),
                configuration.databaseOid(), "0/16B6C50", 6L);
        FakeClient client = new FakeClient(restored,
                Collections.<DataRow>emptyList(), Arrays.asList(
                begin(8L), commit(0x16b6d00L, 0x16b6d20L)));

        try (StreamCdcSourceReader reader = openLogReader(
                configuration, restored, client)) {
            StreamCdcSourceEvent position = reader.next();
            assertEquals(StreamCdcSourceEvent.Kind.LOG_POSITION,
                    position.kind());
            assertEquals(-1L, position.logSequence());
            PostgresCdcPosition completed = PostgresCdcPositionCodec.decode(
                    position.completedExternalPositionFrame());
            assertEquals("0/16B6D20", completed.endLsn());
        }
    }

    private static StreamCdcSourceReader openLogReader(
            PostgresConnectorConfiguration configuration,
            PostgresCdcPosition restored, FakeClient client)
            throws Exception {
        return new PostgresConnectorProvider(
                (actual, credentials, trust) -> client).openCdc(context(
                configuration, StreamCdcSourceContext.Phase.LOG, 0L,
                PostgresCdcPositionCodec.encode(restored)));
    }

    private static FakeClient logClient(PostgresCdcPosition restored,
            long retainedWalBytes) throws Exception {
        FakeClient client = new FakeClient(restored,
                Collections.<DataRow>emptyList(), Arrays.asList(
                relation(), begin(7L), insert("2", "20.00"),
                commit(0x16b6d00L, 0x16b6d20L)));
        client.retainedWalBytes = retainedWalBytes;
        return client;
    }

    private static StreamCdcSourceContext context(
            PostgresConnectorConfiguration configuration,
            StreamCdcSourceContext.Phase phase, long lastLogSequence,
            byte[] position) {
        StreamSecretAccess secrets = new StreamSecretAccess() {
            @Override public <T> T withSecret(String alias,
                    Operation<T> operation) throws Exception {
                assertEquals(
                        PostgresConnectorConfiguration.CREDENTIALS_ALIAS,
                        alias);
                PostgresCredentials credentials = PostgresCredentials.of(
                        bytes("rowweave_reader"), bytes("secret"));
                byte[] frame;
                try {
                    frame = PostgresCredentialCodec.encode(credentials);
                } finally {
                    credentials.destroy();
                }
                try {
                    return operation.apply(frame);
                } finally {
                    Arrays.fill(frame, (byte) 0);
                }
            }
        };
        return new StreamCdcSourceContext("postgres-source",
                configuration.encode(), configuration.snapshotIdentity(), 0L,
                Collections.singletonList("snapshot-all"),
                phase == StreamCdcSourceContext.Phase.LOG
                        ? Collections.singletonList("snapshot-all")
                        : Collections.<String>emptyList(),
                phase, lastLogSequence, position, secrets);
    }

    private static PostgresConnectorConfiguration configuration() {
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
                .maximumTransactionChanges(16)
                .maximumTransactionBytes(1024 * 1024)
                .maximumRetainedWalBytes(1024 * 1024)
                .build();
    }

    private static StreamUpsertCodec upserts(
            PostgresConnectorConfiguration configuration) {
        return new StreamUpsertCodec(configuration.stableKeySchema(),
                configuration.streamSchema());
    }

    private static DataRow row(long id, String amount) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, new java.math.BigDecimal(amount));
        return row;
    }

    private static ByteBuffer relation() throws Exception {
        return message(output -> {
            output.writeByte('R'); output.writeInt((int) TABLE_OID);
            cstring(output, "ledger"); cstring(output, "entries");
            output.writeByte('d'); output.writeShort(2);
            output.writeByte(1); cstring(output, "id");
            output.writeInt(20); output.writeInt(-1);
            output.writeByte(0); cstring(output, "amount");
            output.writeInt(1700); output.writeInt((18 << 16) | 2);
        });
    }

    private static ByteBuffer begin(long xid) throws Exception {
        return message(output -> {
            output.writeByte('B'); output.writeLong(0x16b6d20L);
            output.writeLong(123456L); output.writeInt((int) xid);
        });
    }

    private static ByteBuffer insert(String id, String amount)
            throws Exception {
        return message(output -> {
            output.writeByte('I'); output.writeInt((int) TABLE_OID);
            output.writeByte('N'); output.writeShort(2);
            text(output, id); text(output, amount);
        });
    }

    private static ByteBuffer commit(long commitLsn, long endLsn)
            throws Exception {
        return message(output -> {
            output.writeByte('C'); output.writeByte(0);
            output.writeLong(commitLsn); output.writeLong(endLsn);
            output.writeLong(123457L);
        });
    }

    private static void text(DataOutputStream output, String value)
            throws Exception {
        byte[] bytes = bytes(value);
        output.writeByte('t'); output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void cstring(DataOutputStream output, String value)
            throws Exception {
        output.write(bytes(value)); output.writeByte(0);
    }

    private static ByteBuffer message(Writer writer) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writer.write(output);
        }
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private interface Writer {
        void write(DataOutputStream output) throws Exception;
    }

    private static final class FakeClient implements PostgresCdcClient {
        private final PostgresCdcPosition handoff;
        private final List<DataRow> snapshotRows;
        private final Deque<ByteBuffer> messages;
        private final List<String> acknowledged = new ArrayList<>();
        private PostgresCdcPosition startedLog;
        private boolean failNextAck;
        private int ackAttempts;
        private boolean woken;
        private boolean cancelled;
        private int closeCount;
        private long retainedWalBytes;

        private FakeClient(PostgresCdcPosition handoff,
                List<DataRow> snapshotRows, List<ByteBuffer> messages) {
            this.handoff = handoff;
            this.snapshotRows = snapshotRows;
            this.messages = new ArrayDeque<>(messages);
        }

        @Override public PostgresSnapshotCursor startSnapshot() {
            return new PostgresSnapshotCursor() {
                private int index;
                @Override public PostgresCdcPosition handoffPosition() {
                    return handoff;
                }
                @Override public DataRow next() {
                    return index == snapshotRows.size()
                            ? null : snapshotRows.get(index++).copy();
                }
                @Override public void close() { }
            };
        }

        @Override public void startLog(PostgresCdcPosition position) {
            startedLog = position;
        }
        @Override public ByteBuffer readPending() { return messages.poll(); }
        @Override public long retainedWalBytes() { return retainedWalBytes; }
        @Override public void acknowledge(PostgresCdcPosition position) {
            ackAttempts++;
            if (failNextAck) {
                failNextAck = false;
                throw new IllegalStateException("injected");
            }
            acknowledged.add(position.endLsn());
        }
        @Override public void wakeUp() { woken = true; }
        @Override public void cancel() { cancelled = true; }
        @Override public void close() { closeCount++; }
    }
}
