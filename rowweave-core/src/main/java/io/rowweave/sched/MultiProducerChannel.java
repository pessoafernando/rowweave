package io.rowweave.sched;

import io.rowweave.sched.stream.StreamEvent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded, poisonable channel written by N producers and drained by one consumer. In a keyed shuffle
 * this is the single inbound channel of one consumer, which removes the deadlock cycle that fixed-order
 * per-pair inbound edges would create.
 *
 * <p>On top of the bounded and poison semantics of {@link LocalChannel} it adds a <b>producer-count close
 * latch</b>: the number of producers N is fixed at construction, {@link #producerDone()} decrements it, and
 * the channel really closes, that is posts EOS, <b>only when the count reaches zero</b>. Any {@link #fail}
 * poisons it at once.
 *
 * <p><b>Race correctness, all of it atomic under the lock</b>: decrementing the latch and deciding to close
 * or poison both happen under the channel {@code lock}. {@code poisoned} is a <b>terminal state</b> and wins
 * over a late close, so when {@code producerDone()} brings the count to zero on an already poisoned channel
 * it does nothing rather than appending an EOS behind the error sentinel. {@link #fail} is idempotent and
 * terminal, and may still poison after close, which clears the EOS and posts the error in its place. One
 * {@code closed || poisoned} terminal test decides the outcome, with no reliance on the order of peeks.
 */
public final class MultiProducerChannel implements Channel {

    private final int capacity;  // Hard bound on rows, the no-OOM backstop
    private final int producerCount;
    // The double bound in bytes: a positive maxBytes bounds wire bytes on top of rows, which a socket
    // shuffle uses. Zero means the row bound alone, which the in-memory local path uses.
    private final long maxBytes;
    private long queuedBytes;  // Guarded by lock: the wire bytes currently queued
    private final Deque<Integer> byteSizes = new ArrayDeque<>();  // Parallel to the queue, the wire bytes of each event, subtracted on poll
    private static final Runnable NOOP_CONSUMED = () -> { };
    // Kept parallel to the queue. Once a record has really been polled out by the consumer, the wire credit
    // of its mux target is returned outside the lock.
    private final Deque<Runnable> consumedCallbacks = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();
    private final Deque<StreamEvent> queue = new ArrayDeque<>();
    private int liveProducers;  // Guarded by lock: the close latch count
    private boolean closed;  // Guarded by lock: EOS posted after every producer finished
    private boolean poisoned;  // Guarded by lock: any failure, which is terminal
    private Throwable poisonCause;  // Guarded by lock; the first poison wins
    // Taken only on the first global terminal state, and the callback must run outside the lock, so a
    // transport or ingress cannot re-enter it and form a lock cycle.
    private Deque<Runnable> terminalCallbacks = new ArrayDeque<>();
    private final AtomicLong parkCount = new AtomicLong();

    public MultiProducerChannel(int capacity, int producerCount) {
        this(capacity, producerCount, 0L);
    }

    /** The byte-aware double-bound constructor: a positive maxBytes adds backpressure on wire bytes, which a socket shuffle uses. */
    public MultiProducerChannel(int capacity, int producerCount, long maxBytes) {
        this.maxBytes = maxBytes;
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (producerCount <= 0) {
            throw new IllegalArgumentException("producerCount must be > 0");
        }
        this.capacity = capacity;
        this.producerCount = producerCount;
        this.liveProducers = producerCount;
    }

    /** Whether a local router may safely batch while holding the lock. Batching across several producers forms a convoy, so those send row by row. */
    boolean singleProducer() {
        return producerCount == 1;
    }

    /** One producer finished: decrement the latch, and post EOS and close only when it reaches zero and the channel is <b>not poisoned</b>. Already poisoned, this does nothing, because the error terminal state wins. */
    public void producerDone() {
        Deque<Runnable> callbacks = null;
        lock.lock();
        try {
            if (!poisoned) {
                if (liveProducers > 0) {
                    liveProducers--;
                }
                if (liveProducers == 0 && !closed) {
                    closed = true;
                    queue.addLast(StreamEvent.endOfStream());
                    byteSizes.addLast(0);  // Sentinel zero bytes so byteSizes stays parallel to the queue
                    consumedCallbacks.addLast(NOOP_CONSUMED);
                    notEmpty.signal();
                    notFull.signalAll();  // Wake any sends parked on notFull
                    callbacks = detachTerminalCallbacks();
                }
            }
        } finally {
            lock.unlock();
        }
        notifyTerminal(callbacks);
    }

    /**
     * Observes the global terminal state of the shared producer latch. Registering and closing or poisoning
     * are linearized by the same lock, and each registration is notified exactly once. The callback runs
     * outside the channel lock, and a RuntimeException there affects neither the EOS or error state nor any
     * other observer.
     */
    public void onTerminal(Runnable callback) {
        Objects.requireNonNull(callback, "callback");
        boolean notifyNow;
        lock.lock();
        try {
            notifyNow = closed || poisoned;
            if (!notifyNow) {
                terminalCallbacks.addLast(callback);
            }
        } finally {
            lock.unlock();
        }
        if (notifyNow) {
            notifyTerminal(callback);
        }
    }

    @Override
    public void send(StreamEvent event) {
        send(event, 0);
    }

    /**
     * The byte-aware send, bounded twice: by rows in capacity and by bytes in maxBytes. <b>Full</b> means
     * {@code (queue.size() >= capacity || (maxBytes > 0 && queuedBytes >= maxBytes)) && !queue.isEmpty()}.
     * <b>An empty queue always admits</b>: with nothing queued the event is enqueued unconditionally, even a
     * single event larger than maxBytes, because one event is the smallest atomic unit. Otherwise an empty
     * queue holding an oversized row would block on notFull while the consumer waits on notEmpty, which is
     * deadlock.
     */
    public void send(StreamEvent event, int wireBytes) {
        send(event, wireBytes, NOOP_CONSUMED);
    }

    /** Runs {@code onConsumed} after the record is dequeued; neither enqueue nor failure cleanup runs it. */
    public void send(StreamEvent event, int wireBytes, Runnable onConsumed) {
        lock.lock();
        try {
            boolean counted = false;
            while (isFull() && !queue.isEmpty() && !poisoned && !closed) {
                if (!counted) {
                    parkCount.incrementAndGet();
                    counted = true;
                }
                Task pipelineTask = Task.currentPipelineTask();
                if (pipelineTask != null) pipelineTask.beginExternalBackpressure();
                try {
                    notFull.awaitUninterruptibly();
                } finally {
                    if (pipelineTask != null) pipelineTask.endExternalBackpressure();
                }
            }
            if (poisoned) {
                throw new ChannelPoisonedException(
                        "send on poisoned channel (consumer/peer failed)", poisonCause);
            }
            if (closed) {
                throw new IllegalStateException("send on closed channel");
            }
            queue.addLast(event);
            byteSizes.addLast(wireBytes);
            consumedCallbacks.addLast(onConsumed == null ? NOOP_CONSUMED : onConsumed);
            queuedBytes += wireBytes;
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Bounded batch enqueue of RECORDs for the in-process router. It only reduces the number of lock and
     * signal operations and leaves the capacity backpressure unchanged. A byte-bound network channel must
     * keep using the one-at-a-time send or offer, so that the wire credit callback stays exact.
     */
    public void sendRecords(StreamEvent[] records, int count) {
        if (records == null || count < 0 || count > records.length) {
            throw new IllegalArgumentException("invalid record batch count: " + count);
        }
        if (maxBytes != 0L) {
            throw new IllegalStateException("record batch is only supported by local row-bounded channels");
        }
        for (int i = 0; i < count; i++) {
            if (records[i] == null || !records[i].isRecord()) {
                throw new IllegalArgumentException("record batch contains non-record at index " + i);
            }
        }
        if (count == 0) {
            return;
        }

        lock.lock();
        try {
            int offset = 0;
            boolean counted = false;
            while (offset < count) {
                while (queue.size() >= capacity && !poisoned && !closed) {
                    if (!counted) {
                        parkCount.incrementAndGet();
                        counted = true;
                    }
                    Task pipelineTask = Task.currentPipelineTask();
                    if (pipelineTask != null) pipelineTask.beginExternalBackpressure();
                    try {
                        notFull.awaitUninterruptibly();
                    } finally {
                        if (pipelineTask != null) pipelineTask.endExternalBackpressure();
                    }
                }
                if (poisoned) {
                    throw new ChannelPoisonedException(
                            "batch send on poisoned channel (consumer/peer failed)", poisonCause);
                }
                if (closed) {
                    throw new IllegalStateException("batch send on closed channel");
                }

                int chunk = Math.min(count - offset, capacity - queue.size());
                boolean wasEmpty = queue.isEmpty();
                for (int i = 0; i < chunk; i++) {
                    queue.addLast(records[offset++]);
                    byteSizes.addLast(0);
                    consumedCallbacks.addLast(NOOP_CONSUMED);
                }
                if (wasEmpty) {
                    notEmpty.signal();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * A <b>non-blocking</b> offer. The server demux enqueues on a Netty EventLoop thread and <b>must never
     * block</b>, since blocking the EventLoop head-of-line blocks frame reads for every target on that
     * connection. When full it returns false at once, which the caller should never reach because
     * per-target credit guarantees space. An empty queue always admits, the same smallest atomic unit as
     * {@link #send}. When poisoned it throws, since that is the terminal state, as in send.
     */
    public boolean offer(StreamEvent event, int wireBytes) {
        return offer(event, wireBytes, NOOP_CONSUMED);
    }

    /** The consumer-callback form of the non-blocking offer. */
    public boolean offer(StreamEvent event, int wireBytes, Runnable onConsumed) {
        lock.lock();
        try {
            if (poisoned) {
                throw new ChannelPoisonedException(
                        "offer on poisoned channel (consumer/peer failed)", poisonCause);
            }
            if (closed) {
                throw new IllegalStateException("offer on closed channel");
            }
            if (isFull() && !queue.isEmpty()) {
                return false;  // Full and non-empty: refuse without blocking, which the credit protocol should keep from being reached
            }
            queue.addLast(event);
            byteSizes.addLast(wireBytes);
            consumedCallbacks.addLast(onConsumed == null ? NOOP_CONSUMED : onConsumed);
            queuedBytes += wireBytes;
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** The double-bound full test, by rows or by bytes. Guarded by lock. */
    private boolean isFull() {
        return queue.size() >= capacity || (maxBytes > 0 && queuedBytes >= maxBytes);
    }

    @Override
    public StreamEvent poll() throws InterruptedException {
        StreamEvent result;
        Runnable onConsumed = NOOP_CONSUMED;
        lock.lock();
        try {
            while (queue.isEmpty()) {
                Task pipelineTask = Task.currentPipelineTask();
                if (pipelineTask != null) pipelineTask.beginExternalBackpressure();
                try {
                    notEmpty.await();
                } finally {
                    if (pipelineTask != null) pipelineTask.endExternalBackpressure();
                }
            }
            StreamEvent head = queue.peekFirst();
            if (head.isEndOfStream()) {
                return null;  // Left in the queue, so it re-arms
            }
            if (head.isError()) {
                return head;  // The error stays in the queue
            }
            result = queue.pollFirst();
            queuedBytes -= byteSizes.pollFirst();  // Subtract the bytes of this event, parallel to the queue
            onConsumed = consumedCallbacks.pollFirst();
            notFull.signal();
        } finally {
            lock.unlock();
        }
        // Never call a Netty write while holding the channel lock, so a credit callback cannot form a lock cycle with transport teardown or backpressure.
        try {
            onConsumed.run();
        } catch (RuntimeException ignored) {
            // The transport's own write future and idle handling converge a connection failure, and a record already consumed must not be processed twice because a credit notification threw.
        }
        return result;
    }

    @Override
    public long parkCount() {
        return parkCount.get();
    }

    /**
     * Closes directly, meaning all producers are implicitly done, and posts EOS. <b>Already poisoned, this is
     * a no-op</b>, since poisoning is terminal. Normally the count reaching zero in {@link #producerDone}
     * triggers it, and this method serves the degenerate or single-producer case.
     */
    @Override
    public void close() {
        Deque<Runnable> callbacks = null;
        lock.lock();
        try {
            if (!poisoned && !closed) {
                closed = true;
                liveProducers = 0;
                queue.addLast(StreamEvent.endOfStream());
                byteSizes.addLast(0);
                consumedCallbacks.addLast(NOOP_CONSUMED);
                notEmpty.signal();
                notFull.signalAll();
                callbacks = detachTerminalCallbacks();
            }
        } finally {
            lock.unlock();
        }
        notifyTerminal(callbacks);
    }

    /**
     * Failure poisoning, idempotent and terminal: drain the dirty records from the queue, post the error
     * placeholder, and wake both sides. <b>It wins over a late close()</b>, so failing after close still
     * clears the EOS and posts the error in its place.
     */
    @Override
    public void fail(Throwable cause) {
        Deque<Runnable> callbacks = null;
        lock.lock();
        try {
            if (poisoned) {
                return;  // Strictly idempotent: a cascading fail must not overwrite the first cause or notify the terminal state twice
            }
            boolean firstTerminal = !closed && !poisoned;
            poisoned = true;
            poisonCause = cause;
            closed = true;
            queue.clear();
            byteSizes.clear();
            consumedCallbacks.clear();
            queuedBytes = 0;
            queue.addLast(StreamEvent.error(cause));
            byteSizes.addLast(0);
            consumedCallbacks.addLast(NOOP_CONSUMED);
            notEmpty.signal();
            notFull.signalAll();
            if (firstTerminal) {
                callbacks = detachTerminalCallbacks();
            }
        } finally {
            lock.unlock();
        }
        notifyTerminal(callbacks);
    }

    /** Guarded by lock; set to null once the first terminal state is published, after which onTerminal takes the immediate-notification branch. */
    private Deque<Runnable> detachTerminalCallbacks() {
        Deque<Runnable> callbacks = terminalCallbacks;
        terminalCallbacks = null;
        return callbacks;
    }

    private static void notifyTerminal(Deque<Runnable> callbacks) {
        if (callbacks == null) {
            return;
        }
        Runnable callback;
        while ((callback = callbacks.pollFirst()) != null) {
            notifyTerminal(callback);
        }
    }

    private static void notifyTerminal(Runnable callback) {
        try {
            callback.run();
        } catch (RuntimeException ignored) {
            // The terminal state was published at its linearization point, so one diagnostic or cleanup observer cannot block the others or corrupt the channel result.
        }
    }
}
