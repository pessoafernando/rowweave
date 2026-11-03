package io.rowweave;

import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** broadcast updates and keyed reads are type-separated. */
class StreamKeyedBroadcastProcessSdkContractTest {

    @Test
    void publicTypesSeparateBroadcastMutationFromKeyedAndTimerReads()
            throws Exception {
        Class<?> function = Class.forName(
                "io.rowweave.StreamKeyedBroadcastProcessFunction");
        Class<?> keyedContext = Class.forName(
                "io.rowweave.StreamKeyedBroadcastProcessContext");
        Class<?> updateContext = Class.forName(
                "io.rowweave.StreamBroadcastProcessContext");
        Class<?> timerContext = Class.forName(
                "io.rowweave.StreamBroadcastTimerContext");
        Class<?> readOnly = Class.forName(
                "io.rowweave.StreamReadOnlyBroadcastState");
        Class<?> mutable = Class.forName(
                "io.rowweave.StreamBroadcastState");

        assertTrue(Serializable.class.isAssignableFrom(function));
        assertTrue(StreamProcessContext.class.isAssignableFrom(keyedContext));
        assertTrue(StreamTimerContext.class.isAssignableFrom(timerContext));
        assertTrue(readOnly.isAssignableFrom(mutable));
        assertEquals(readOnly, keyedContext.getMethod(
                "broadcastState", String.class).getReturnType());
        assertEquals(readOnly, timerContext.getMethod(
                "broadcastState", String.class).getReturnType());
        assertEquals(mutable, updateContext.getMethod(
                "broadcastState", String.class).getReturnType());
        assertEquals(void.class, function.getMethod("processKeyed",
                DataRow.class, keyedContext, StreamCollector.class)
                .getReturnType());
        assertEquals(void.class, function.getMethod("processBroadcast",
                DataRow.class, updateContext, StreamCollector.class)
                .getReturnType());
        assertEquals(void.class, function.getMethod("onTimer", long.class,
                timerContext, StreamCollector.class).getReturnType());
        assertEquals(StreamStateCursor.class, readOnly.getMethod("entries")
                .getReturnType());
        assertEquals(void.class, mutable.getMethod("put", byte[].class,
                byte[].class).getReturnType());
    }
}
