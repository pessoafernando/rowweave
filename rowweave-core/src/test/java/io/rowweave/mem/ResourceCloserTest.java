package io.rowweave.mem;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ResourceCloserTest {

    @Test
    void businessRootWinsAndEveryCleanupIsAttempted() {
        RuntimeException business = new RuntimeException("business-root");
        RuntimeException firstCleanup = new RuntimeException("cleanup-1");
        RuntimeException secondCleanup = new RuntimeException("cleanup-2");
        AtomicInteger attempts = new AtomicInteger();

        ResourceCloser.close(business,
                () -> { attempts.incrementAndGet(); throw firstCleanup; },
                () -> { attempts.incrementAndGet(); throw secondCleanup; });

        assertEquals(2, attempts.get());
        assertEquals(1, business.getSuppressed().length);
        assertSame(firstCleanup, business.getSuppressed()[0]);
        assertEquals(1, firstCleanup.getSuppressed().length);
        assertSame(secondCleanup, firstCleanup.getSuppressed()[0]);
    }

    @Test
    void firstCleanupBecomesRootWhenBusinessSucceeded() {
        RuntimeException firstCleanup = new RuntimeException("cleanup-1");
        RuntimeException secondCleanup = new RuntimeException("cleanup-2");
        AtomicInteger attempts = new AtomicInteger();

        RuntimeException actual = assertThrows(RuntimeException.class, () -> ResourceCloser.close(null,
                () -> { attempts.incrementAndGet(); throw firstCleanup; },
                () -> { attempts.incrementAndGet(); throw secondCleanup; }));

        assertSame(firstCleanup, actual);
        assertEquals(2, attempts.get());
        assertEquals(1, actual.getSuppressed().length);
        assertSame(secondCleanup, actual.getSuppressed()[0]);
    }
}
