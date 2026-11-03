package io.rowweave.mem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Worker-level configuration under `rowweave.worker.*`, covering defaults and system
 * property overrides.
 *
 * <p>Keys: memory (half of the heap by default), slots (128 by default),
 * operator.maxMemory (16M) and operator.minMemory (512K). ClusterMain freezes memory and
 * slots, while a direct call parses them strictly.
 */
class WorkerConfigTest {

    @AfterEach
    void clearProps() {
        WorkerConfig.resetProcessSnapshotForTest();
        System.clearProperty("rowweave.worker.memory");
        System.clearProperty("rowweave.worker.slots");
        System.clearProperty("rowweave.worker.maxStageThreads");
        System.clearProperty("rowweave.worker.operator.maxMemory");
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    @Test
    void processSnapshotIgnoresLaterPropertyDrift() {
        WorkerConfig.configureProcessSnapshot(104857600L, 4, 12);
        System.setProperty("rowweave.worker.memory", "209715200");
        System.setProperty("rowweave.worker.slots", "8");
        assertEquals(104857600L, WorkerConfig.memoryBytes());
        assertEquals(4, WorkerConfig.slots());
        assertEquals(12, WorkerConfig.maxStageThreads());
        assertThrows(IllegalStateException.class,
                () -> WorkerConfig.configureProcessSnapshot(209715200L, 8, 16));
    }

    @Test
    void defaultsWhenUnset() {
        assertEquals(128, WorkerConfig.slots(), "slots defaults to 128 (a production heap of 2G or more)");
        assertEquals(16, WorkerConfig.maxStageThreads());
        assertEquals(512L * 1024, WorkerConfig.operatorMinMemory(), "operator.minMemory defaults to 512K");
        // the memory default = heap maxMemory times 0.5
        long expectedMem = (long) (Runtime.getRuntime().maxMemory() * 0.5);
        assertEquals(expectedMem, WorkerConfig.memoryBytes(), "memory defaults to half the heap");
        // the max default = clamp(memory/slots, 2M, 16M)
        long expectedMax = Math.min(16L * 1024 * 1024, Math.max(2L * 1024 * 1024, expectedMem / 128));
        assertEquals(expectedMax, WorkerConfig.operatorMaxMemory(), "max defaults to clamp(memory/128, 2M, 16M)");
    }

    @Test
    void systemPropertyOverrides() {
        System.setProperty("rowweave.worker.memory", "104857600"); // 100M
        System.setProperty("rowweave.worker.slots", "4");
        System.setProperty("rowweave.worker.maxStageThreads", "11");
        System.setProperty("rowweave.worker.operator.maxMemory", "8388608"); // 8M
        System.setProperty("rowweave.worker.operator.minMemory", "262144"); // 256K
        assertEquals(104857600L, WorkerConfig.memoryBytes());
        assertEquals(4, WorkerConfig.slots());
        assertEquals(11, WorkerConfig.maxStageThreads());
        assertEquals(8L * 1024 * 1024, WorkerConfig.operatorMaxMemory());
        assertEquals(256L * 1024, WorkerConfig.operatorMinMemory());
    }

    @Test
    void slotSemaphoreSingletonHasConfiguredPermits() {
        System.setProperty("rowweave.worker.slots", "3");
        SlotSemaphore.resetForTest();
        assertEquals(3, SlotSemaphore.instance().availablePermits(), "the semaphore holds as many permits as there are slots");
        // basic acquire and release semantics
        SlotSemaphore.instance().acquireUninterruptibly();
        assertEquals(2, SlotSemaphore.instance().availablePermits());
        SlotSemaphore.instance().release();
        assertEquals(3, SlotSemaphore.instance().availablePermits());
        SlotSemaphore.resetForTest();
    }

    @Test
    void invalidAndOverflowingSlotsFailClosed() {
        System.setProperty("rowweave.worker.slots", "notanumber");
        assertThrows(IllegalArgumentException.class, WorkerConfig::slots);
        System.setProperty("rowweave.worker.slots", "2147483648");
        assertThrows(IllegalArgumentException.class, WorkerConfig::slots);
    }
}
