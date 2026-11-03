package io.rowweave;

import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public symmetric keyed co-process SDK is portable. */
class StreamKeyedCoProcessSdkContractTest {

    @Test
    void coProcessFunctionHasDistinctInputsAndSharedProcessPrimitives()
            throws Exception {
        Class<?> type = Class.forName(
                "io.rowweave.StreamKeyedCoProcessFunction");

        assertTrue(Serializable.class.isAssignableFrom(type));
        assertEquals(void.class, type.getMethod(
                "processLeft", DataRow.class, StreamProcessContext.class,
                StreamCollector.class).getReturnType());
        assertEquals(void.class, type.getMethod(
                "processRight", DataRow.class, StreamProcessContext.class,
                StreamCollector.class).getReturnType());
        assertEquals(void.class, type.getMethod(
                "onTimer", long.class, StreamTimerContext.class,
                StreamCollector.class).getReturnType());
    }
}
