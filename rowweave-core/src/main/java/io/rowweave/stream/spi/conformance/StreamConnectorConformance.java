package io.rowweave.stream.spi.conformance;

import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEvent;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSinkWriter;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.rowweave.stream.spi.conformance.StreamConnectorConformanceException.Code;

/** Published backend-neutral conformance checks for the Stream connector SDK. */
public final class StreamConnectorConformance {
    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;

    private StreamConnectorConformance() { }

    public static StreamConformanceReport verifySource(
            StreamSourceConformanceFixture fixture) throws Exception {
        requireSourceFixture(fixture);
        Set<String> passed = new LinkedHashSet<>();
        StreamSourceProvider provider = fixture.provider();
        byte[] firstPosition;
        byte[] secondPosition;
        byte[] secondRow;
        try (StreamSourceReader reader = opened(
                provider, fixture.initialContext())) {
            StreamSourceRecord first = record(reader.nextEvent());
            firstPosition = frame(first.positionFrame());
            if (!Arrays.equals(firstPosition,
                    frame(reader.checkpointPositionFrame()))) {
                throw failure(Code.SOURCE_REPLAY_MISMATCH,
                        "source checkpoint does not follow the emitted record");
            }
            StreamSourceRecord second = record(reader.nextEvent());
            secondPosition = frame(second.positionFrame());
            secondRow = frame(second.rowFrame());
            if (fixture.comparePositions(firstPosition.clone(),
                    secondPosition.clone()) >= 0) {
                throw failure(Code.SOURCE_POSITION_NOT_MONOTONIC,
                        "source positions are not strictly monotonic");
            }
            passed.add("SOURCE_POSITION_MONOTONIC");
            verifyTimeProtocol(reader);
            passed.add("SOURCE_WATERMARK_IDLE_GENERATION");
        }
        try (StreamSourceReader restored = opened(provider,
                fixture.restoredContext(firstPosition.clone(), 1L))) {
            StreamSourceRecord replay = record(restored.nextEvent());
            if (!Arrays.equals(secondRow, frame(replay.rowFrame()))
                    || !Arrays.equals(secondPosition,
                    frame(replay.positionFrame()))
                    || !Arrays.equals(secondPosition,
                    frame(restored.checkpointPositionFrame()))) {
                throw failure(Code.SOURCE_REPLAY_MISMATCH,
                        "source restore does not replay the exact successor");
            }
        }
        passed.add("SOURCE_EXACT_REPLAY");
        verifyWakeupAndClose(fixture, secondPosition);
        passed.add("SOURCE_WAKEUP_CANCEL_CLOSE");
        passed.add("SOURCE_FRAME_LIMITS");
        return new StreamConformanceReport(passed);
    }

    /**
     * Verifies a fixture split with exactly two records followed by one END.
     * The fixture must replay from either record position and from its terminal
     * position; all provider-owned reads stay on one thread and are time-bounded.
     */
    public static StreamConformanceReport verifyBoundedSource(
            StreamSourceConformanceFixture fixture) throws Exception {
        requireSourceFixture(fixture);
        long timeout = fixture.wakeupTimeoutMillis();
        if (timeout <= 0L || timeout > 60_000L) {
            throw failure(Code.INVALID_FIXTURE,
                    "bounded source timeout is invalid");
        }
        AtomicReference<StreamSourceReader> active = new AtomicReference<>();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        AtomicReference<StreamConformanceReport> result =
                new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread owner = new Thread(() -> {
            try {
                result.set(verifyBoundedOnOwner(fixture, active));
            } catch (Throwable failure) {
                failed.set(failure);
            } finally {
                done.countDown();
            }
        }, "rowweave-stream-bounded-source-conformance-owner");
        owner.setDaemon(true);
        owner.start();
        if (!done.await(timeout, TimeUnit.MILLISECONDS)) {
            owner.interrupt();
            StreamSourceReader reader = active.get();
            if (reader != null) {
                Thread cancellation = new Thread(() -> {
                    try { reader.cancel(); } catch (Throwable ignored) {
                        // The timeout is the stable public failure.
                    }
                }, "rowweave-stream-bounded-source-conformance-cancel");
                cancellation.setDaemon(true);
                cancellation.start();
            }
            throw failure(Code.SOURCE_CONTROL_TIMEOUT,
                    "bounded source conformance exceeded its deadline");
        }
        Throwable sourceFailure = failed.get();
        if (sourceFailure instanceof StreamConnectorConformanceException) {
            throw (StreamConnectorConformanceException) sourceFailure;
        }
        if (sourceFailure != null) {
            throw failure(Code.SOURCE_TERMINATION_PROTOCOL_INVALID,
                    "bounded source conformance failed");
        }
        return result.get();
    }

    private static StreamConformanceReport verifyBoundedOnOwner(
            StreamSourceConformanceFixture fixture,
            AtomicReference<StreamSourceReader> active) throws Exception {
        Set<String> passed = new LinkedHashSet<>();
        byte[] firstPosition;
        byte[] secondPosition;
        byte[] secondRow;
        try (StreamSourceReader reader = opened(
                fixture.provider(), fixture.initialContext())) {
            active.set(reader);
            StreamSourceRecord first = record(nextBoundedEvent(reader));
            firstPosition = frame(first.positionFrame()).clone();
            if (!Arrays.equals(firstPosition,
                    frame(reader.checkpointPositionFrame()))) {
                throw failure(Code.SOURCE_REPLAY_MISMATCH,
                        "bounded checkpoint missed the first record");
            }
            StreamSourceRecord second = record(nextBoundedEvent(reader));
            secondPosition = frame(second.positionFrame()).clone();
            secondRow = frame(second.rowFrame()).clone();
            if (fixture.comparePositions(firstPosition.clone(),
                    secondPosition.clone()) >= 0) {
                throw failure(Code.SOURCE_POSITION_NOT_MONOTONIC,
                        "bounded source positions are not increasing");
            }
            passed.add("SOURCE_POSITION_MONOTONIC");
            if (!Arrays.equals(secondPosition,
                    frame(reader.checkpointPositionFrame()))) {
                throw failure(Code.SOURCE_REPLAY_MISMATCH,
                        "bounded checkpoint missed the second record");
            }
            requireEnd(nextBoundedEvent(reader));
            if (!Arrays.equals(secondPosition,
                    frame(reader.checkpointPositionFrame()))) {
                throw failure(Code.SOURCE_TERMINATION_PROTOCOL_INVALID,
                        "bounded END changed the checkpoint position");
            }
            verifyBoundedControl(reader);
            passed.add("SOURCE_END_ONCE");
        } finally {
            active.set(null);
        }
        try (StreamSourceReader reader = opened(fixture.provider(),
                fixture.restoredContext(firstPosition.clone(), 1L))) {
            active.set(reader);
            StreamSourceRecord replay = record(nextBoundedEvent(reader));
            if (!Arrays.equals(secondRow, frame(replay.rowFrame()))
                    || !Arrays.equals(secondPosition,
                    frame(replay.positionFrame()))
                    || !Arrays.equals(secondPosition,
                    frame(reader.checkpointPositionFrame()))) {
                throw failure(Code.SOURCE_REPLAY_MISMATCH,
                        "bounded restore did not replay the successor");
            }
            requireEnd(nextBoundedEvent(reader));
            passed.add("SOURCE_EXACT_REPLAY");
        } finally {
            active.set(null);
        }
        try (StreamSourceReader reader = opened(fixture.provider(),
                fixture.restoredContext(secondPosition.clone(), 2L))) {
            active.set(reader);
            requireEnd(nextBoundedEvent(reader));
            if (!Arrays.equals(secondPosition,
                    frame(reader.checkpointPositionFrame()))) {
                throw failure(Code.SOURCE_TERMINATION_PROTOCOL_INVALID,
                        "terminal restore changed the checkpoint position");
            }
            passed.add("SOURCE_TERMINAL_RESTORE");
        } finally {
            active.set(null);
        }
        passed.add("SOURCE_WAKEUP_CANCEL_CLOSE");
        passed.add("SOURCE_FRAME_LIMITS");
        return new StreamConformanceReport(passed);
    }

    private static void verifyBoundedControl(StreamSourceReader reader) {
        try {
            reader.wakeUp();
            if (reader.nextEvent() != null || reader.nextEvent() != null) {
                throw failure(Code.SOURCE_TERMINATION_PROTOCOL_INVALID,
                        "bounded source emitted after END");
            }
            reader.cancel();
        } catch (StreamConnectorConformanceException failure) {
            throw failure;
        } catch (Throwable failure) {
            throw failure(Code.SOURCE_CONTROL_TIMEOUT,
                    "bounded source control lifecycle failed", failure);
        }
    }

    private static void requireEnd(StreamSourceEvent event) {
        if (event == null || event.kind()
                != StreamSourceEvent.Kind.END_OF_INPUT) {
            throw failure(Code.SOURCE_TERMINATION_PROTOCOL_INVALID,
                    "bounded source did not emit its terminal event");
        }
    }

    private static StreamSourceEvent nextBoundedEvent(
            StreamSourceReader reader) throws Exception {
        StreamSourceEvent event;
        while ((event = reader.nextEvent()) == null) {
            // A null poll is temporary. The owner thread's overall deadline
            // bounds this loop and cancel/wake remain available to the caller.
            Thread.sleep(1L);
        }
        return event;
    }

    public static StreamConformanceReport verifyTransactionalSink(
            StreamTransactionalSinkConformanceFixture fixture)
            throws Exception {
        requireSinkFixture(fixture);
        Set<String> passed = new LinkedHashSet<>();
        StreamTransactionalSinkProvider provider = fixture.provider();
        byte[] record = frame(fixture.recordFrame());
        String commit = identity(fixture.commitTransactionIdentity());
        String abort = identity(fixture.abortTransactionIdentity());
        if (commit.equals(abort)) {
            throw failure(Code.INVALID_FIXTURE,
                    "Sink conformance transaction identities must differ");
        }

        try (StreamTransactionalSinkWriter writer = opened(provider, fixture)) {
            writer.write(record.clone());
            frame(writer.prepare(commit));
            requireStatus(writer.query(commit), StreamTransactionStatus.PREPARED);
            writer.commit(commit);
            requireStatus(writer.query(commit), StreamTransactionStatus.VISIBLE);
        }
        passed.add("SINK_PREPARE_QUERY_COMMIT");
        passed.add("SINK_COMMIT_ACK_LOSS");

        try (StreamTransactionalSinkWriter restarted = opened(provider, fixture)) {
            requireStatus(restarted.query(commit),
                    StreamTransactionStatus.VISIBLE);
            restarted.commit(commit);
            requireStatus(restarted.query(commit),
                    StreamTransactionStatus.VISIBLE);
            restarted.finalizeTransaction(commit);
            restarted.finalizeTransaction(commit);
        }
        passed.add("SINK_PROCESS_RESTART");

        try (StreamTransactionalSinkWriter writer = opened(provider, fixture)) {
            writer.write(record.clone());
            frame(writer.prepare(abort));
            requireStatus(writer.query(abort), StreamTransactionStatus.PREPARED);
            writer.abort(abort);
            writer.abort(abort);
            requireStatus(writer.query(abort), StreamTransactionStatus.ABORTED);
        }
        try (StreamTransactionalSinkWriter restarted = opened(provider, fixture)) {
            requireStatus(restarted.query(abort),
                    StreamTransactionStatus.ABORTED);
            restarted.abort(abort);
        }
        passed.add("SINK_ABORT_IDEMPOTENCE");
        passed.add("SINK_FRAME_LIMITS");
        return new StreamConformanceReport(passed);
    }

    public static StreamConformanceReport verifyEagerSink(
            StreamEagerSinkConformanceFixture fixture) throws Exception {
        if (fixture == null || fixture.provider() == null
                || empty(fixture.provider().providerKey())
                || empty(fixture.provider().contractDigest())
                || fixture.context() == null
                || fixture.observedWrites() != 0
                || fixture.observedCancels() != 0
                || fixture.observedCloses() != 0) {
            throw failure(Code.INVALID_FIXTURE,
                    "eager Sink conformance fixture is invalid");
        }
        Set<String> passed = new LinkedHashSet<>();
        StreamSinkProvider provider = fixture.provider();
        byte[] record = frame(fixture.recordFrame()).clone();
        try (StreamSinkWriter writer = provider.open(fixture.context())) {
            if (writer == null) {
                throw failure(Code.INVALID_FIXTURE,
                        "eager Sink provider returned no writer");
            }
            writer.write(record.clone());
            if (fixture.observedWrites() != 1) {
                throw failure(Code.SINK_TRANSACTION_PROTOCOL_INVALID,
                        "eager Sink did not expose exactly one write");
            }
            passed.add("SINK_EAGER_WRITE");
            writer.cancel();
            if (fixture.observedCancels() != 1) {
                throw failure(Code.SINK_TRANSACTION_PROTOCOL_INVALID,
                        "eager Sink cancel did not converge");
            }
        }
        if (fixture.observedCloses() != 1) {
            throw failure(Code.SINK_TRANSACTION_PROTOCOL_INVALID,
                    "eager Sink close did not converge");
        }
        passed.add("SINK_CANCEL_CLOSE");
        passed.add("SINK_FRAME_LIMITS");
        return new StreamConformanceReport(passed);
    }

    private static void verifyTimeProtocol(StreamSourceReader reader)
            throws Exception {
        StreamSourceEvent watermark = reader.nextEvent();
        StreamSourceEvent idle = reader.nextEvent();
        StreamSourceEvent active = reader.nextEvent();
        if (watermark == null || watermark.kind()
                != StreamSourceEvent.Kind.WATERMARK
                || idle == null || idle.kind()
                != StreamSourceEvent.Kind.WATERMARK_STATUS
                || idle.watermarkStatus()
                != StreamSourceEvent.WatermarkStatus.IDLE
                || active == null || active.kind()
                != StreamSourceEvent.Kind.WATERMARK_STATUS
                || active.watermarkStatus()
                != StreamSourceEvent.WatermarkStatus.ACTIVE
                || idle.watermarkStatusGeneration() <= 0L
                || active.watermarkStatusGeneration()
                <= idle.watermarkStatusGeneration()) {
            throw failure(Code.SOURCE_TIME_PROTOCOL_INVALID,
                    "source watermark/idle generation protocol is invalid");
        }
    }

    private static void verifyWakeupAndClose(
            StreamSourceConformanceFixture fixture, byte[] endPosition)
            throws Exception {
        long timeout = fixture.wakeupTimeoutMillis();
        if (timeout <= 0L || timeout > 60_000L) {
            throw failure(Code.INVALID_FIXTURE,
                    "source wakeup timeout is invalid");
        }
        AtomicReference<StreamSourceReader> opened = new AtomicReference<>();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        CountDownLatch blocking = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        Thread owner = new Thread(() -> {
            try (StreamSourceReader reader = opened(
                    fixture.provider(), fixture.restoredContext(
                            endPosition.clone(), 2L))) {
                opened.set(reader);
                verifyTimeProtocol(reader);
                blocking.countDown();
                if (reader.nextEvent() != null) {
                    throw failure(Code.SOURCE_CONTROL_TIMEOUT,
                            "source wakeup returned an unexpected event");
                }
            } catch (Throwable value) {
                failed.set(value);
            } finally {
                done.countDown();
            }
        }, "rowweave-stream-source-conformance-owner");
        owner.setDaemon(true);
        owner.start();
        if (!blocking.await(timeout, TimeUnit.MILLISECONDS)) {
            owner.interrupt();
            throw failure(Code.SOURCE_CONTROL_TIMEOUT,
                    "source did not enter its blocking read");
        }
        StreamSourceReader reader = opened.get();
        if (reader == null) {
            throw failure(Code.SOURCE_CONTROL_TIMEOUT,
                    "source reader was not published to the controller");
        }
        reader.wakeUp();
        if (!done.await(timeout, TimeUnit.MILLISECONDS)) {
            reader.cancel();
            owner.interrupt();
            throw failure(Code.SOURCE_CONTROL_TIMEOUT,
                    "source wakeup did not release its blocking read");
        }
        reader.cancel();
        if (failed.get() != null) {
            throw failure(Code.SOURCE_CONTROL_TIMEOUT,
                    "source control lifecycle failed", failed.get());
        }
    }

    private static StreamSourceRecord record(StreamSourceEvent event) {
        if (event == null || event.kind() != StreamSourceEvent.Kind.RECORD) {
            throw failure(Code.SOURCE_REPLAY_MISMATCH,
                    "source did not emit the required replay record");
        }
        frame(event.record().rowFrame());
        frame(event.record().positionFrame());
        return event.record();
    }

    private static StreamSourceReader opened(
            StreamSourceProvider provider, StreamSourceContext context)
            throws Exception {
        StreamSourceReader reader = provider.open(context);
        if (reader == null) {
            throw failure(Code.INVALID_FIXTURE,
                    "source provider returned no reader");
        }
        return reader;
    }

    private static StreamTransactionalSinkWriter opened(
            StreamTransactionalSinkProvider provider,
            StreamTransactionalSinkConformanceFixture fixture)
            throws Exception {
        StreamTransactionalSinkWriter writer = provider.open(fixture.context());
        if (writer == null) {
            throw failure(Code.INVALID_FIXTURE,
                    "transactional Sink provider returned no writer");
        }
        return writer;
    }

    private static void requireSourceFixture(
            StreamSourceConformanceFixture fixture) {
        if (fixture == null || fixture.provider() == null
                || empty(fixture.provider().providerKey())
                || empty(fixture.provider().contractDigest())
                || fixture.initialContext() == null) {
            throw failure(Code.INVALID_FIXTURE,
                    "source conformance fixture is invalid");
        }
    }

    private static void requireSinkFixture(
            StreamTransactionalSinkConformanceFixture fixture) {
        if (fixture == null || fixture.provider() == null
                || empty(fixture.provider().providerKey())
                || empty(fixture.provider().contractDigest())
                || fixture.context() == null) {
            throw failure(Code.INVALID_FIXTURE,
                    "transactional Sink conformance fixture is invalid");
        }
    }

    private static byte[] frame(byte[] value) {
        if (value == null || value.length == 0
                || value.length > MAX_FRAME_BYTES) {
            throw failure(Code.FRAME_LIMIT_EXCEEDED,
                    "connector frame is outside the published limit");
        }
        return value;
    }

    private static String identity(String value) {
        if (empty(value) || value.length() > 512) {
            throw failure(Code.INVALID_FIXTURE,
                    "transaction identity is invalid");
        }
        return value;
    }

    private static void requireStatus(
            StreamTransactionStatus actual, StreamTransactionStatus expected) {
        if (actual != expected) {
            throw failure(Code.SINK_TRANSACTION_PROTOCOL_INVALID,
                    "transactional Sink returned an invalid durable status");
        }
    }

    private static boolean empty(String value) {
        return value == null || value.isEmpty();
    }

    private static StreamConnectorConformanceException failure(
            Code code, String message) {
        return new StreamConnectorConformanceException(code, message);
    }

    private static StreamConnectorConformanceException failure(
            Code code, String message, Throwable cause) {
        return new StreamConnectorConformanceException(code, message, cause);
    }
}
