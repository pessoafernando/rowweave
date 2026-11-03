package io.rowweave.cluster;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Barrier over the ports a stage listens on: each edge accepts one valid report, and only one. */
final class StagePortTracker {
    private final CountDownLatch latch;
    private final boolean[] expected = new boolean[2];
    private final int[] ports = {-1, -1};
    private RuntimeException failure;

    StagePortTracker(boolean leftExpected, boolean rightExpected) {
        expected[0] = leftExpected;
        expected[1] = rightExpected;
        latch = new CountDownLatch((leftExpected ? 1 : 0) + (rightExpected ? 1 : 0));
    }

    synchronized void onPort(int edge, int port) {
        if (failure != null) {
            return;
        }
        if (edge < 0 || edge >= ports.length || !expected[edge]) {
            fail(new IllegalStateException("unexpected port edge " + edge));
            return;
        }
        if (port <= 0 || port > 65535) {
            fail(new IllegalStateException("invalid port " + port + " for edge " + edge));
            return;
        }
        if (ports[edge] >= 0) {
            if (ports[edge] != port) {
                fail(new IllegalStateException("conflicting port report for edge " + edge
                        + ": " + ports[edge] + " vs " + port));
            }
            return;
        }
        ports[edge] = port;
        latch.countDown();
    }

    int await(long timeoutMs) throws InterruptedException {
        return await(timeoutMs, 0);
    }

    int await(long timeoutMs, int edge) throws InterruptedException {
        awaitReady(timeoutMs);
        synchronized (this) {
            if (edge < 0 || edge >= ports.length || !expected[edge] || ports[edge] < 0) {
                throw new IllegalStateException("missing port for edge " + edge);
            }
            return ports[edge];
        }
    }

    int[] awaitBoth(long timeoutMs) throws InterruptedException {
        awaitReady(timeoutMs);
        synchronized (this) {
            return new int[]{ports[0], ports[1]};
        }
    }

    private void awaitReady(long timeoutMs) throws InterruptedException {
        if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("port report timeout");
        }
        synchronized (this) {
            if (failure != null) {
                throw failure;
            }
        }
    }

    synchronized void fail(RuntimeException cause) {
        if (failure != null) {
            return;
        }
        failure = cause;
        while (latch.getCount() > 0) {
            latch.countDown();
        }
    }
}
