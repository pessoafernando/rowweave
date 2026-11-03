package io.rowweave;

import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** generic-window callbacks stay portable and cursor based. */
class StreamGenericWindowSdkContractTest {

    @Test
    void aggregateFunctionExposesIncrementalAndSessionMergePrimitives()
            throws Exception {
        Class<?> function = Class.forName(
                "io.rowweave.StreamWindowAggregateFunction");
        Class<?> context = Class.forName(
                "io.rowweave.StreamWindowContext");

        assertTrue(Serializable.class.isAssignableFrom(function));
        assertEquals(byte[].class, function.getMethod(
                "createAccumulator").getReturnType());
        assertEquals(byte[].class, function.getMethod(
                "add", DataRow.class, byte[].class).getReturnType());
        assertEquals(byte[].class, function.getMethod(
                "merge", byte[].class, byte[].class).getReturnType());
        assertEquals(DataRow.class, function.getMethod(
                "getResult", byte[].class, context).getReturnType());
    }

    @Test
    void processFunctionReadsAOnePassCloseableRowCursor() throws Exception {
        Class<?> function = Class.forName(
                "io.rowweave.StreamWindowProcessFunction");
        Class<?> context = Class.forName(
                "io.rowweave.StreamWindowContext");
        Class<?> cursor = Class.forName(
                "io.rowweave.StreamWindowDataRowCursor");
        Class<?> collector = Class.forName(
                "io.rowweave.StreamWindowCollector");

        assertTrue(Serializable.class.isAssignableFrom(function));
        assertTrue(AutoCloseable.class.isAssignableFrom(cursor));
        assertEquals(void.class, function.getMethod("process", context,
                cursor, collector).getReturnType());
        assertEquals(boolean.class, cursor.getMethod("advance")
                .getReturnType());
        assertEquals(DataRow.class, cursor.getMethod("row").getReturnType());
        assertEquals(void.class, collector.getMethod(
                "collect", DataRow.class).getReturnType());
    }

    @Test
    void contextProvidesKeyWindowAndMailboxTimeWithoutRuntimeHandles()
            throws Exception {
        Class<?> context = Class.forName(
                "io.rowweave.StreamWindowContext");

        assertEquals(DataRow.class, context.getMethod("currentKey")
                .getReturnType());
        assertEquals(long.class, context.getMethod("windowStart")
                .getReturnType());
        assertEquals(long.class, context.getMethod("windowEnd")
                .getReturnType());
        assertEquals(long.class, context.getMethod("currentWatermark")
                .getReturnType());
        assertEquals(long.class, context.getMethod("currentProcessingTime")
                .getReturnType());
    }
}
