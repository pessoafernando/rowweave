package io.rowweave.sched;

import io.rowweave.sched.stream.StreamEvent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A local channel passing references, with poisoning: a bounded queue whose capacity is the backpressure
 * boundary, since send blocks, that is parks, when the queue is full, which keeps a slow consumer from
 * letting the producer pile up rows until out of memory. {@link StreamEvent} travels by reference.
 *
 * <p>A {@link ReentrantLock} with two {@link Condition}s, notFull and notEmpty, manages the flow
 * explicitly, so the <b>poison semantics</b> are implemented correctly: {@link #fail} sets poisoned under
 * the lock, drains the records from the queue, posts the error placeholder and wakes the sends parked on a
 * full queue <b>atomically</b>, making them throw {@link ChannelPoisonedException}. This releases the
 * upstream from a send-park deadlock when a middle stage fails, and keeps the downstream from polling a
 * dirty record ever again so that it sees only the error, with no race between send and fail.
 *
 * <p>The EOS and error sentinels stay in the queue, seen by peek but not removed, so a re-poll finds them
 * again, which is safe under the single-consumer contract of not polling after null or error.
 */
public final class LocalChannel implements Channel {

    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();
    private final Deque<StreamEvent> queue = new ArrayDeque<>();
    private boolean closed;   // guarded by lock
    private boolean poisoned;  // Guarded by lock, set by fail
    private Throwable poisonCause;  // Guarded by lock; the first poison wins
    private final AtomicLong parkCount = new AtomicLong();
    private final AtomicLong throughput = new AtomicLong();  // Records successfully polled so far, which is the signal of progress

    public LocalChannel(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.capacity = capacity;
    }

    /** Pushes one event, blocking until the downstream makes room when the queue is full. <b>After poisoning</b>, a send waking from park throws {@link ChannelPoisonedException}. */
    @Override
    public void send(StreamEvent event) {
        lock.lock();
        try {
            boolean counted = false;
            while (queue.size() >= capacity && !poisoned && !closed) {
                if (!counted) {
                    parkCount.incrementAndGet();
                    counted = true;
                }
                notFull.awaitUninterruptibly();
            }
            if (poisoned) {
                throw new ChannelPoisonedException(
                        "send on poisoned channel (downstream/peer failed)", poisonCause);
            }
            if (closed) {
                throw new IllegalStateException("send on closed channel");
            }
            queue.addLast(event);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long parkCount() {
        return parkCount.get();
    }

    @Override
    public long throughput() {
        return throughput.get();
    }

    /**
     * Takes one event, blocking when there is none. EOS returns null, with the sentinel left in the queue and
     * re-armed; an error sentinel returns that error event, also left in the queue. An ordinary record is
     * removed and wakes a send.
     */
    @Override
    public StreamEvent poll() throws InterruptedException {
        lock.lock();
        try {
            while (queue.isEmpty()) {
                notEmpty.await();
            }
            StreamEvent head = queue.peekFirst();
            if (head.isEndOfStream()) {
                return null;  // Left in the queue, so it re-arms
            }
            if (head.isError()) {
                return head;  // The error stays in the queue: seeing it ends the downstream, which does not poll again
            }
            queue.pollFirst();
            throughput.incrementAndGet();  // One row successfully out is progress
            notFull.signal();
            return head;
        } finally {
            lock.unlock();
        }
    }

    /** Marks the end of the stream: posts the EOS sentinel and wakes a consumer blocked on poll. */
    @Override
    public void close() {
        lock.lock();
        try {
            closed = true;
            queue.addLast(StreamEvent.endOfStream());
            notEmpty.signal();
            notFull.signalAll();  // Wake any sends parked on notFull, on the closed path
        } finally {
            lock.unlock();
        }
    }

    /**
     * Failure poisoning, atomic under the lock: sets poisoned, so a send waking from park throws
     * ChannelPoisonedException; drains the records already queued, so the downstream never polls a dirty row;
     * posts the error placeholder, so the next poll from the downstream certainly gets the error; and wakes
     * both sides.
     */
    @Override
    public void fail(Throwable cause) {
        lock.lock();
        try {
            if (poisoned) {
                return;  // Strictly idempotent: a cascading fail must not overwrite the first cause
            }
            poisoned = true;
            poisonCause = cause;
            closed = true;
            queue.clear();  // Drain the dirty records
            queue.addLast(StreamEvent.error(cause));  // The error placeholder, with room guaranteed after clearing
            notEmpty.signal();  // Wake the poll
            notFull.signalAll();  // Wake the sends parked on the queue so they throw ChannelPoisonedException
        } finally {
            lock.unlock();
        }
    }
}
