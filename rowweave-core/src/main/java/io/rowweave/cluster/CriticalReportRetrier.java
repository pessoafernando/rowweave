package io.rowweave.cluster;

import java.util.function.BooleanSupplier;

/** Bounded backoff state machine that delivers a worker's critical control report at least once while the worker lives. */
final class CriticalReportRetrier {

    private static final long INITIAL_BACKOFF_MS = 25L;
    private static final long MAX_BACKOFF_MS = 500L;

    @FunctionalInterface
    interface Delivery {
        void deliver();
    }

    @FunctionalInterface
    interface Pause {
        void pause(long millis) throws InterruptedException;
    }

    static final class Result {
        private final boolean delivered;
        private final int attempts;
        private final RuntimeException firstFailure;

        Result(boolean delivered, int attempts, RuntimeException firstFailure) {
            this.delivered = delivered;
            this.attempts = attempts;
            this.firstFailure = firstFailure;
        }

        boolean delivered() {
            return delivered;
        }

        int attempts() {
            return attempts;
        }

        RuntimeException firstFailure() {
            return firstFailure;
        }
    }

    private CriticalReportRetrier() {
    }

    static Result deliver(Delivery delivery, BooleanSupplier stopping, Pause pause) {
        if (delivery == null || stopping == null || pause == null) {
            throw new IllegalArgumentException("delivery, stopping and pause must not be null");
        }
        int attempts = 0;
        long backoffMs = INITIAL_BACKOFF_MS;
        RuntimeException firstFailure = null;
        while (!stopping.getAsBoolean()) {
            attempts++;
            try {
                delivery.deliver();
                return new Result(true, attempts, firstFailure);
            } catch (RuntimeException failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                }
            }
            if (stopping.getAsBoolean()) {
                break;
            }
            try {
                pause.pause(backoffMs);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
            backoffMs = Math.min(MAX_BACKOFF_MS, backoffMs * 2L);
        }
        return new Result(false, attempts, firstFailure);
    }
}
