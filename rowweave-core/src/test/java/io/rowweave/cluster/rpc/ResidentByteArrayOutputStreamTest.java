package io.rowweave.cluster.rpc;

import io.rowweave.memory.ResidentByteArrayOutputStream;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ResidentByteArrayOutputStreamTest {

    @Test
    void everyBackingGrowthAndFinalCopyIsChargedBeforeAllocation() throws Exception {
        AtomicLong charged = new AtomicLong();
        ResidentByteArrayOutputStream out = new ResidentByteArrayOutputStream(
                charged::addAndGet, 2, 16);
        out.write(new byte[] {1, 2, 3, 4, 5});
        assertEquals(2L + 5L, charged.get());
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, out.toByteArray());
        assertEquals(2L + 5L + 5L, charged.get());
    }

    @Test
    void hardLimitRejectsBeforeGrowing() {
        ResidentByteArrayOutputStream out = new ResidentByteArrayOutputStream(
                ignored -> { }, 1, 4);
        assertThrows(java.io.IOException.class,
                () -> out.write(new byte[] {1, 2, 3, 4, 5}));
        assertEquals(0, out.size());
    }
}
