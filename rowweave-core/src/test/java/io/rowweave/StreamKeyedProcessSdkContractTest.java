package io.rowweave;

import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTimerSpec;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public keyed-process SDK stays portable and bounded. */
class StreamKeyedProcessSdkContractTest {

    @Test
    void processFunctionExposesCanonicalStateTimerAndTaggedOutputPrimitives()
            throws Exception {
        assertTrue(Serializable.class.isAssignableFrom(
                StreamKeyedProcessFunction.class));
        assertEquals(void.class, StreamKeyedProcessFunction.class.getMethod(
                "processElement", DataRow.class, StreamProcessContext.class,
                StreamCollector.class).getReturnType());
        assertEquals(void.class, StreamKeyedProcessFunction.class.getMethod(
                "onTimer", long.class, StreamTimerContext.class,
                StreamCollector.class).getReturnType());

        assertEquals(byte[].class,
                StreamValueState.class.getMethod("value").getReturnType());
        assertEquals(StreamStateCursor.class,
                StreamMapState.class.getMethod("entries").getReturnType());
        assertEquals(StreamStateCursor.class,
                StreamListState.class.getMethod("values").getReturnType());
        assertEquals(byte[].class, StreamAggregatingState.class
                .getMethod("value").getReturnType());
        assertTrue(AutoCloseable.class.isAssignableFrom(
                StreamStateCursor.class));
        assertEquals(void.class, StreamTimerService.class.getMethod(
                "registerTimer", String.class, byte[].class, long.class)
                .getReturnType());
        assertEquals(void.class, StreamCollector.class.getMethod(
                "collect", StreamOutputTag.class, DataRow.class)
                .getReturnType());
    }

    @Test
    void keyedListIsAStableManagedStateKind() {
        StreamStateSpec list = new StreamStateSpec(
                "history", StreamStateSpec.Kind.KEYED_LIST,
                "canonical-row-v1", 1,
                StreamStateSpec.TtlMode.EVENT_TIME, 60_000L);

        assertEquals("history", list.stateName());
        assertEquals(StreamStateSpec.Kind.KEYED_LIST, list.kind());
    }

    @Test
    void userFunctionCanBeWrittenOnlyAgainstThePublicSdk() {
        StreamKeyedProcessFunction function = new StreamKeyedProcessFunction() {
            @Override
            public void processElement(DataRow value,
                    StreamProcessContext context, StreamCollector output) {
                byte[] encoded = Long.toString(value.getLong(0))
                        .getBytes(StandardCharsets.UTF_8);
                context.valueState("latest").update(encoded);
                context.mapState("by-id").put(encoded, encoded);
                context.listState("history").add(encoded);
                context.aggregatingState("total").update(encoded);
                context.timerService().registerTimer(
                        "expiry", new byte[0],
                        context.currentProcessingTime() + 1_000L);
                output.collect(value);
            }
        };

        assertTrue(function instanceof Serializable);
    }
}
