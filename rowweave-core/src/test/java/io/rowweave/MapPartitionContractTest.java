package io.rowweave;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapPartitionContractTest {
    @Test
    void publicFunctionUsesCursorAndPushCollector() throws Exception {
        Method method = MapPartitionFunction.class.getMethod(
                "mapPartition", PartitionDataRowCursor.class, Collector.class);

        assertEquals(void.class, method.getReturnType());
        assertTrue(Serializable.class.isAssignableFrom(
                MapPartitionFunction.class));
        assertTrue(ResultSchemaAware.class.isAssignableFrom(
                MapPartitionFunction.class));
        assertEquals(DataRow.class,
                PartitionDataRowCursor.class.getMethod("next").getReturnType());
    }

    @Test
    void emissionLimitIsPositiveBoundedAndValueStable() {
        MapPartitionEmissionLimit limit = new MapPartitionEmissionLimit(7L, 99L);
        assertEquals(7L, limit.maxOutputRowsPerPartition());
        assertEquals(99L, limit.maxOutputBytesPerPartition());
        assertEquals(limit, new MapPartitionEmissionLimit(7L, 99L));

        assertThrows(IllegalArgumentException.class,
                () -> new MapPartitionEmissionLimit(0L, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new MapPartitionEmissionLimit(1L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new MapPartitionEmissionLimit(
                        MapPartitionEmissionLimit.MAX_OUTPUT_ROWS_PER_PARTITION + 1L,
                        1L));
        assertThrows(IllegalArgumentException.class,
                () -> new MapPartitionEmissionLimit(1L,
                        MapPartitionEmissionLimit.MAX_OUTPUT_BYTES_PER_PARTITION + 1L));
    }
}
