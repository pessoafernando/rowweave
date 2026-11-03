package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamEnvelopeCodec;
import io.rowweave.stream.contract.StreamEnvelopeSequenceGuard;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One-producer/one-consumer local Stream data lane with exact event and encoded-byte bounds.
 * Cancellation is a terminal control-lane action and therefore never waits for data credit.
 */
public final class LocalStreamChannel
        implements StreamChannelOutput, StreamChannelInput {
    private final int maximumEvents;
    private final int maximumFrameBytes;
    private final long maximumBytes;
    private final StreamChannelIdentity channelIdentity;
    private final StreamEnvelopeSequenceGuard sequenceGuard;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();
    private final Deque<Entry> queue = new ArrayDeque<>();
    private final AtomicLong parkCount = new AtomicLong();
    private final AtomicLong blockedNanos = new AtomicLong();
    private final AtomicLong throughput = new AtomicLong();

    private long queuedBytes;
    private int maximumObservedEvents;
    private long maximumObservedBytes;
    private boolean closed;
    private boolean cancelled;
    private Throwable failure;

    public LocalStreamChannel(
            StreamChannelIdentity channelIdentity,
            int maximumEvents, long maximumBytes) {
        this(channelIdentity, maximumEvents,
                maximumBytes > Integer.MAX_VALUE
                        ? Integer.MAX_VALUE : (int) maximumBytes,
                maximumBytes);
    }

    public LocalStreamChannel(
            StreamChannelIdentity channelIdentity,
            int maximumEvents, int maximumFrameBytes,
            long maximumBytes) {
        if (channelIdentity == null) {
            throw new IllegalArgumentException("channel identity is required");
        }
        if (maximumEvents <= 0 || maximumFrameBytes <= 0
                || maximumBytes < maximumFrameBytes) {
            throw new IllegalArgumentException(
                    "positive compatible event, frame and credit limits are required");
        }
        this.maximumEvents = maximumEvents;
        this.maximumFrameBytes = maximumFrameBytes;
        this.maximumBytes = maximumBytes;
        this.channelIdentity = channelIdentity;
        this.sequenceGuard = new StreamEnvelopeSequenceGuard(channelIdentity);
    }

    /** Enqueues one ordered envelope, blocking without overshooting either configured bound. */
    public void send(StreamEnvelope envelope) {
        if (envelope == null) {
            throw new IllegalArgumentException("Stream envelope is required");
        }
        int encodedBytes = StreamEnvelopeCodec.encode(envelope).length;
        if (encodedBytes > maximumFrameBytes) {
            throw new IllegalArgumentException(
                    "encoded Stream envelope exceeds the channel frame limit");
        }

        lock.lock();
        try {
            requireWritable();
            sequenceGuard.accept(envelope);
            boolean counted = false;
            long blockedAt = 0L;
            while (isFull(encodedBytes) && !closed && !cancelled) {
                if (!counted) {
                    parkCount.incrementAndGet();
                    counted = true;
                    blockedAt = System.nanoTime();
                }
                notFull.awaitUninterruptibly();
            }
            if (counted) {
                long elapsed = Math.max(1L, System.nanoTime() - blockedAt);
                blockedNanos.updateAndGet(current -> current
                        > Long.MAX_VALUE - elapsed
                        ? Long.MAX_VALUE : current + elapsed);
            }
            requireWritable();
            queue.addLast(new Entry(envelope, encodedBytes));
            queuedBytes += encodedBytes;
            maximumObservedEvents = Math.max(maximumObservedEvents, queue.size());
            maximumObservedBytes = Math.max(maximumObservedBytes, queuedBytes);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /** Returns the next envelope, or null only after a normally closed lane is fully drained. */
    public StreamEnvelope receive() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (queue.isEmpty() && !closed && !cancelled) {
                notEmpty.await();
            }
            if (cancelled) {
                throw new StreamChannelCancelledException();
            }
            if (failure != null) {
                throw new StreamChannelFailedException(failure);
            }
            return removeFirst();
        } finally {
            lock.unlock();
        }
    }

    /** Bounded wait used by a single mailbox owner multiplexing finite inputs. */
    public StreamEnvelope poll(long timeoutMillis) throws InterruptedException {
        if (timeoutMillis < 0L) {
            throw new IllegalArgumentException("poll timeout must not be negative");
        }
        long remaining = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        lock.lockInterruptibly();
        try {
            while (queue.isEmpty() && !closed && !cancelled
                    && remaining > 0L) {
                remaining = notEmpty.awaitNanos(remaining);
            }
            if (cancelled) throw new StreamChannelCancelledException();
            if (failure != null) throw new StreamChannelFailedException(failure);
            return removeFirst();
        } finally {
            lock.unlock();
        }
    }

    /** Normal producer completion: queued data remains visible before end-of-input. */
    public void close() {
        lock.lock();
        try {
            if (!cancelled) {
                closed = true;
                notEmpty.signalAll();
                notFull.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Terminal control action that drops pending data and wakes producer and consumer waiters. */
    public void cancel() {
        lock.lock();
        try {
            if (cancelled || failure != null) return;
            cancelled = true;
            closed = true;
            queue.clear();
            queuedBytes = 0L;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** First-poison-wins terminal failure; pending data is never exposed afterward. */
    public void fail(Throwable cause) {
        if (cause == null) {
            throw new IllegalArgumentException("Stream channel failure cause is required");
        }
        lock.lock();
        try {
            if (cancelled || failure != null) return;
            failure = cause;
            closed = true;
            queue.clear();
            queuedBytes = 0L;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public StreamChannelIdentity channelIdentity() {
        return channelIdentity;
    }

    public long parkCount() { return parkCount.get(); }
    public long throughput() { return throughput.get(); }

    public int queuedEvents() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public long queuedBytes() {
        lock.lock();
        try {
            return queuedBytes;
        } finally {
            lock.unlock();
        }
    }

    public int maximumObservedEvents() {
        lock.lock();
        try {
            return maximumObservedEvents;
        } finally {
            lock.unlock();
        }
    }

    public long maximumObservedBytes() {
        lock.lock();
        try {
            return maximumObservedBytes;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public StreamChannelMetrics metrics() {
        lock.lock();
        try {
            return StreamChannelMetrics.available(queue.size(), queuedBytes,
                    maximumObservedEvents, maximumObservedBytes,
                    blockedNanos.get());
        } finally {
            lock.unlock();
        }
    }

    private boolean isFull(int nextEncodedBytes) {
        return queue.size() >= maximumEvents
                || queuedBytes + nextEncodedBytes > maximumBytes;
    }

    private void requireWritable() {
        if (cancelled) {
            throw new StreamChannelCancelledException();
        }
        if (failure != null) {
            throw new StreamChannelFailedException(failure);
        }
        if (closed) {
            throw new IllegalStateException("send on closed local Stream channel");
        }
    }

    private StreamEnvelope removeFirst() {
        Entry entry = queue.pollFirst();
        if (entry == null) return null;
        queuedBytes -= entry.encodedBytes;
        if (entry.envelope.kind() == StreamEnvelope.Kind.RECORD) {
            throughput.incrementAndGet();
        }
        notFull.signal();
        return entry.envelope;
    }

    private static final class Entry {
        private final StreamEnvelope envelope;
        private final int encodedBytes;

        private Entry(StreamEnvelope envelope, int encodedBytes) {
            this.envelope = envelope;
            this.encodedBytes = encodedBytes;
        }
    }
}
