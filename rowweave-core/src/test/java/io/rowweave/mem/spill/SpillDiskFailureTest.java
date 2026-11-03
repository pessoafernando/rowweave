package io.rowweave.mem.spill;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpillDiskFailureTest {

    @Test
    void failureCodeSetIsClosedAndStable() {
        assertEquals(EnumSet.of(
                SpillFailureCode.SPILL_CONFIG_INVALID,
                SpillFailureCode.SPILL_WORKSPACE_UNAVAILABLE,
                SpillFailureCode.SPILL_WORKER_QUOTA_EXCEEDED,
                SpillFailureCode.SPILL_JOB_QUOTA_EXCEEDED,
                SpillFailureCode.SPILL_MIN_FREE_SPACE,
                SpillFailureCode.SPILL_IO_WRITE_FAILED,
                SpillFailureCode.SPILL_CLEANUP_FAILED),
                EnumSet.allOf(SpillFailureCode.class));
    }

    @Test
    void structuredFailureBoundsAndRedactsMessageButRetainsCause() {
        IOException cause = new IOException("/private/customer-ledger/secret");
        SpillDiskException failure = SpillDiskException.resourceLimit(
                SpillFailureCode.SPILL_JOB_QUOTA_EXCEEDED,
                4096, 8192, 10_000, "0123456789abcdef", cause);

        assertEquals(SpillFailureCode.SPILL_JOB_QUOTA_EXCEEDED, failure.code());
        assertEquals(4096L, failure.requestedBytes());
        assertEquals(8192L, failure.usedBytes());
        assertEquals(10_000L, failure.limitBytes());
        assertSame(cause, failure.getCause());
        assertTrue(failure.getMessage().contains("0123456789abcdef"));
        assertTrue(!failure.getMessage().contains("customer-ledger"));
        assertTrue(!failure.getMessage().contains("secret"));
        assertTrue(failure.getMessage().length() <= 256);
    }
}
