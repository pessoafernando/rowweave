package io.rowweave.mem;

import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegment;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class HeapPoolConcurrencyTest {

    @Test
    void concurrentAllocateFreeKeepsLiveLedgerConsistent() throws Exception {
        HeapPool pool = new HeapPool();
        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 2000; i++) {
                        MemorySegment segment = pool.allocate(32 + (i & 3));
                        segment.putInt(0, i);
                        pool.free(segment);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                } finally {
                    done.countDown();
                }
            }).start();
        }
        start.countDown();
        done.await();

        assertNull(failure.get());
        assertEquals(0, pool.outstanding());
    }
}
