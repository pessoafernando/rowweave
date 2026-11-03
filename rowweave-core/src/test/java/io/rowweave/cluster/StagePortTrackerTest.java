package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class StagePortTrackerTest {

    @Test
    void duplicateLeftReportDoesNotCompleteDualEdgeTracker() throws Exception {
        StagePortTracker tracker = new StagePortTracker(true, true);
        CompletableFuture<int[]> ports = CompletableFuture.supplyAsync(() -> {
            try {
                return tracker.awaitBoth(2000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });

        tracker.onPort(0, 12001);
        tracker.onPort(0, 12001);
        Thread.sleep(80L);
        assertFalse(ports.isDone(), "a repeated edge0 must not stand in for edge1 to release the two-port barrier");

        tracker.onPort(1, 12002);
        assertArrayEquals(new int[]{12001, 12002}, ports.get(1, TimeUnit.SECONDS));
    }

    @Test
    void invalidEdgeOrPortFailsWaiterInsteadOfReturningMinusOne() {
        StagePortTracker badEdge = new StagePortTracker(true, true);
        badEdge.onPort(2, 12001);
        IllegalStateException edgeFailure = assertThrows(IllegalStateException.class,
                () -> badEdge.awaitBoth(1000L));
        assertTrue(edgeFailure.getMessage().contains("edge"));

        StagePortTracker badPort = new StagePortTracker(true, false);
        badPort.onPort(0, 0);
        IllegalStateException portFailure = assertThrows(IllegalStateException.class,
                () -> badPort.awaitBoth(1000L));
        assertTrue(portFailure.getMessage().contains("port"));
    }

    @Test
    void workerLossFailsPortWaiterImmediately() {
        StagePortTracker tracker = new StagePortTracker(true, true);
        IllegalStateException lost = new IllegalStateException("worker lost");

        tracker.fail(lost);

        assertSame(lost, assertThrows(IllegalStateException.class,
                () -> tracker.awaitBoth(TimeUnit.MINUTES.toMillis(1))),
                "a lost peer must not wait out the full port timeout");
    }
}

