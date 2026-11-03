package io.rowweave.connector.postgres;

import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamUpsertCodec;
import io.rowweave.stream.spi.StreamCdcCheckpointCompletion;
import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcSourceEvent;
import io.rowweave.stream.spi.StreamCdcSourceReader;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class PostgresCdcReader implements StreamCdcSourceReader {
    private final PostgresConnectorConfiguration configuration;
    private final PostgresCdcClient client;
    private final PostgresPgOutputDecoder decoder;
    private final StreamUpsertCodec upsertCodec;
    private final int[] keyIndices;
    private final AtomicBoolean clientClosed = new AtomicBoolean();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object wakeMonitor = new Object();
    private boolean wakeRequested;
    private PostgresSnapshotCursor snapshot;
    private PostgresCdcPosition handoffPosition;
    private PostgresCdcPosition latestBoundary;
    private PostgresCdcPosition acknowledgedBoundary;
    private List<byte[]> pending = Collections.emptyList();
    private int pendingIndex;
    private PostgresCdcPosition pendingBoundary;
    private long logSequence;

    PostgresCdcReader(PostgresConnectorConfiguration configuration,
            StreamCdcSourceContext context, PostgresCdcClient client)
            throws Exception {
        if (configuration == null || context == null || client == null) {
            throw new IllegalArgumentException("POSTGRES_CDC_READER_INVALID");
        }
        this.configuration = configuration;
        this.client = client;
        this.decoder = new PostgresPgOutputDecoder(configuration);
        this.upsertCodec = new StreamUpsertCodec(
                configuration.stableKeySchema(),
                configuration.streamSchema());
        this.keyIndices = keyIndices(configuration);
        this.logSequence = context.lastLogSequence();
        if (context.phase() == StreamCdcSourceContext.Phase.SNAPSHOT) {
            snapshot = client.startSnapshot();
            if (snapshot == null || snapshot.handoffPosition() == null) {
                throw new IllegalStateException(
                        "POSTGRES_CDC_SNAPSHOT_FAILED");
            }
            handoffPosition = snapshot.handoffPosition();
            requireIdentity(handoffPosition);
        } else {
            PostgresCdcPosition restored = PostgresCdcPositionCodec.decode(
                    context.externalPositionFrame());
            requireIdentity(restored);
            client.startLog(restored);
            latestBoundary = restored;
            acknowledge(restored);
        }
    }

    @Override public StreamCdcSourceEvent next() throws Exception {
        if (cancelled.get() || closed.get()) return null;
        try {
            if (consumeWakeRequest()) return null;
            if (snapshot != null) return nextSnapshot();
            StreamCdcSourceEvent queued = nextPending();
            if (queued != null) return queued;
            while (!cancelled.get() && !closed.get()) {
                if (client.retainedWalBytes()
                        > configuration.maximumRetainedWalBytes()) {
                    throw new IllegalStateException(
                            "POSTGRES_CDC_WAL_RETENTION_LIMIT");
                }
                ByteBuffer message = client.readPending();
                if (message == null) {
                    if (awaitPollOrWake()) return null;
                    continue;
                }
                PostgresCommittedTransaction transaction =
                        decoder.accept(message);
                if (transaction != null) {
                    pending = transaction.upsertFrames();
                    pendingIndex = 0;
                    pendingBoundary = transaction.position();
                    latestBoundary = pendingBoundary;
                    if (pending.isEmpty()) {
                        PostgresCdcPosition boundary = pendingBoundary;
                        pendingBoundary = null;
                        return StreamCdcSourceEvent.logPosition(
                                PostgresCdcPositionCodec.encode(boundary));
                    }
                    return nextPending();
                }
            }
            return null;
        } catch (Throwable failure) {
            closeAfterFailure(failure);
            if (cancelled.get()) return null;
            throw failure;
        }
    }

    private boolean consumeWakeRequest() {
        synchronized (wakeMonitor) {
            if (!wakeRequested) return false;
            wakeRequested = false;
            return true;
        }
    }

    private boolean awaitPollOrWake() throws InterruptedException {
        synchronized (wakeMonitor) {
            if (wakeRequested) {
                wakeRequested = false;
                return true;
            }
            wakeMonitor.wait(Math.min(100L,
                    configuration.statusIntervalMillis()));
            if (!wakeRequested) return false;
            wakeRequested = false;
            return true;
        }
    }

    private StreamCdcSourceEvent nextSnapshot() throws Exception {
        DataRow row = snapshot.next();
        if (row != null) {
            return StreamCdcSourceEvent.snapshotAfterImage(
                    "snapshot-all", upsertCodec.encodeAfterImage(
                    key(row), row));
        }
        PostgresSnapshotCursor completed = snapshot;
        snapshot = null;
        completed.close();
        client.startLog(handoffPosition);
        latestBoundary = handoffPosition;
        return StreamCdcSourceEvent.snapshotHandoffComplete(
                "snapshot-all",
                PostgresCdcPositionCodec.encode(handoffPosition));
    }

    private StreamCdcSourceEvent nextPending() {
        if (pendingIndex >= pending.size()) return null;
        byte[] frame = pending.get(pendingIndex++);
        logSequence = Math.addExact(logSequence, 1L);
        if (pendingIndex == pending.size()) {
            PostgresCdcPosition boundary = pendingBoundary;
            pending = Collections.emptyList();
            pendingIndex = 0;
            pendingBoundary = null;
            return StreamCdcSourceEvent.logRecord(logSequence, frame,
                    PostgresCdcPositionCodec.encode(boundary));
        }
        return StreamCdcSourceEvent.logRecord(logSequence, frame);
    }

    @Override public void notifyCheckpointComplete(
            StreamCdcCheckpointCompletion completion) throws Exception {
        if (completion == null || closed.get() || cancelled.get()) return;
        PostgresCdcPosition position = PostgresCdcPositionCodec.decode(
                completion.externalPositionFrame());
        requireIdentity(position);
        if (completion.lastLogSequence() > logSequence
                || latestBoundary == null
                || compare(position, latestBoundary) > 0
                || (acknowledgedBoundary != null
                && compare(position, acknowledgedBoundary) < 0)) {
            throw new IllegalArgumentException(
                    "POSTGRES_CDC_POSITION_MISMATCH");
        }
        acknowledge(position);
    }

    private void acknowledge(PostgresCdcPosition position) throws Exception {
        try {
            client.acknowledge(position);
            acknowledgedBoundary = position;
        } catch (Exception failure) {
            throw new IllegalStateException("POSTGRES_CDC_ACK_FAILED");
        }
    }

    private void requireIdentity(PostgresCdcPosition position) {
        if (!configuration.databaseSystemIdentifier().equals(
                position.databaseSystemIdentifier())
                || configuration.databaseOid() != position.databaseOid()) {
            throw new IllegalArgumentException(
                    "POSTGRES_CDC_POSITION_MISMATCH");
        }
    }

    private static int compare(PostgresCdcPosition left,
            PostgresCdcPosition right) {
        return Long.compareUnsigned(left.endLsnBits(), right.endLsnBits());
    }

    private DataRow key(DataRow row) {
        if (row == null || row.size()
                != configuration.streamSchema().fieldCount()) {
            throw new IllegalArgumentException("POSTGRES_CDC_VALUE_INVALID");
        }
        DataRow key = new DataRow(keyIndices.length);
        for (int index = 0; index < keyIndices.length; index++) {
            Object value = row.get(keyIndices[index]);
            if (value == null) {
                throw new IllegalArgumentException(
                        "POSTGRES_CDC_VALUE_INVALID");
            }
            key.set(index, value);
        }
        return key;
    }

    private static int[] keyIndices(
            PostgresConnectorConfiguration configuration) {
        int[] result = new int[configuration.stableKeyFieldIds().size()];
        for (int key = 0; key < result.length; key++) {
            String id = configuration.stableKeyFieldIds().get(key);
            result[key] = -1;
            for (int field = 0;
                    field < configuration.streamSchema().fieldCount();
                    field++) {
                StreamField candidate =
                        configuration.streamSchema().field(field);
                if (id.equals(candidate.fieldId())) {
                    result[key] = field;
                    break;
                }
            }
            if (result[key] < 0) {
                throw new IllegalArgumentException(
                        "POSTGRES_CDC_READER_INVALID");
            }
        }
        return result;
    }

    @Override public void wakeUp() {
        synchronized (wakeMonitor) {
            wakeRequested = true;
            wakeMonitor.notifyAll();
        }
        client.wakeUp();
    }

    @Override public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            synchronized (wakeMonitor) {
                wakeRequested = true;
                wakeMonitor.notifyAll();
            }
            closeSnapshotQuietly();
            client.cancel();
        }
    }

    @Override public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) return;
        synchronized (wakeMonitor) {
            wakeRequested = true;
            wakeMonitor.notifyAll();
        }
        Throwable failure = null;
        PostgresSnapshotCursor current = snapshot;
        snapshot = null;
        if (current != null) {
            try { current.close(); } catch (Throwable close) {
                failure = close;
            }
        }
        try { closeClient(); } catch (Throwable close) {
            if (failure == null) failure = close;
            else failure.addSuppressed(close);
        }
        if (failure != null) {
            throw new IllegalStateException("POSTGRES_CDC_CLOSE_FAILED");
        }
    }

    private void closeAfterFailure(Throwable primary) {
        closed.set(true);
        PostgresSnapshotCursor current = snapshot;
        snapshot = null;
        if (current != null) {
            try { current.close(); } catch (Throwable close) {
                primary.addSuppressed(close);
            }
        }
        try { closeClient(); } catch (Throwable close) {
            primary.addSuppressed(close);
        }
    }

    private void closeSnapshotQuietly() {
        PostgresSnapshotCursor current = snapshot;
        snapshot = null;
        if (current != null) {
            try { current.close(); } catch (Throwable ignored) { }
        }
    }

    private void closeClient() throws Exception {
        if (clientClosed.compareAndSet(false, true)) client.close();
    }
}
