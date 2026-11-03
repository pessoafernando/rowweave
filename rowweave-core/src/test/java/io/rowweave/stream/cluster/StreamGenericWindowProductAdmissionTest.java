package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamAccumulationMode;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamGenericWindowSpec;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.api.StreamWindowAssignerSpec;
import io.rowweave.stream.api.StreamWindowResourceSpec;
import io.rowweave.stream.api.StreamWindowTriggerSpec;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamOperatorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** the product planner admits bounded generic windows. */
class StreamGenericWindowProductAdmissionTest {

    @Test
    void eventTimeAggregateIsAStatefulProductNodeWithFunctionCapability() {
        StreamTestGraph plan = plan();

        assertEquals(1, plan.product.nodes(
                StreamOperatorCode.GENERIC_WINDOW).size());
        assertEquals(StreamOperatorCode.GENERIC_WINDOW,
                plan.onlyNode(StreamOperatorCode.GENERIC_WINDOW).opcode());
        assertTrue(plan.product.executionCapabilities().contains(
                PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                        + function().deploymentSetId()));
        assertEquals(1, plan.product.executionGraph.node("window")
                .inputEdges().size());
    }

    @Test
    void countWindowCannotAdmitMoreRowsThanItsPerWindowBound() {
        assertThrows(UnsupportedOperationException.class, () -> plan(
                StreamWindowAssignerSpec.countTumbling(1_001L),
                StreamWindowTriggerSpec.count(1_001L),
                StreamAccumulationMode.DISCARDING, 0L, false));
    }

    static StreamTestGraph plan() {
        return plan(StreamAccumulationMode.DISCARDING, 0L, false);
    }

    static StreamTestGraph plan(
            StreamAccumulationMode accumulationMode,
            long allowedLatenessMillis, boolean lateOutput) {
        return plan(StreamWindowAssignerSpec.eventTimeTumbling(
                        "event-time", 100L),
                StreamWindowTriggerSpec.eventTimeFinal(), accumulationMode,
                allowedLatenessMillis, lateOutput);
    }

    static StreamTestGraph plan(
            StreamWindowAssignerSpec assigner,
            StreamWindowTriggerSpec trigger,
            StreamAccumulationMode accumulationMode,
            long allowedLatenessMillis, boolean lateOutput) {
        return plan(assigner, trigger, accumulationMode,
                allowedLatenessMillis, lateOutput, 8, false);
    }

    static StreamTestGraph plan(
            StreamWindowAssignerSpec assigner,
            StreamWindowTriggerSpec trigger,
            StreamAccumulationMode accumulationMode,
            long allowedLatenessMillis, boolean lateOutput,
            int maximumSessionMergeFanIn) {
        return plan(assigner, trigger, accumulationMode,
                allowedLatenessMillis, lateOutput,
                maximumSessionMergeFanIn, false);
    }

    static StreamTestGraph processPlan(
            StreamWindowAssignerSpec assigner,
            StreamWindowTriggerSpec trigger,
            StreamAccumulationMode accumulationMode) {
        return plan(assigner, trigger, accumulationMode,
                0L, false, 8, true);
    }

    private static StreamTestGraph plan(
            StreamWindowAssignerSpec assigner,
            StreamWindowTriggerSpec trigger,
            StreamAccumulationMode accumulationMode,
            long allowedLatenessMillis, boolean lateOutput,
            int maximumSessionMergeFanIn, boolean processFunction) {
        StreamSession session = RowWeave.createStreamSession(
                "generic-window-admission");
        if (assigner.eventTime()) {
            session.setTimeMode(StreamTimeMode.EVENT_TIME);
        }
        if (accumulationMode == StreamAccumulationMode.ACCUMULATING) {
            session.setChangelogMode(StreamChangelogMode.UPSERT);
        }
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 16_384L, 32, 32,
                65_536L, 5, 32, 1_048_576L));
        StreamSourceSpec source = assigner.eventTime()
                ? StreamSourceSpec.exactOrderedEventTime(
                        inputSchema(), "source-v1", digest("source"),
                        new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(digest("split"),
                                "sequence-v1", new byte[] {2})),
                        StreamWatermarkPolicy.providerEvents(1_000L))
                : StreamSourceSpec.exactOrdered(inputSchema(), "source-v1",
                        digest("source"), new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                digest("split"), "sequence-v1",
                                new byte[] {2})));
        io.rowweave.DataStream keyed = session.fromSource(source)
                .uid("source").keyBy("adaptor").uid("key");
        FunctionDeploymentReference function = processFunction
                ? processFunction() : function();
        java.util.List<String> outputKeys = accumulationMode
                == StreamAccumulationMode.ACCUMULATING
                ? Arrays.asList("adaptor", "window-start", "window-end")
                : Collections.singletonList("adaptor");
        StreamGenericWindowSpec specification = processFunction
                ? StreamGenericWindowSpec.process(
                assigner, trigger,
                accumulationMode, allowedLatenessMillis, function,
                outputSchema(), outputKeys,
                new StreamStateSpec("window-state",
                        StreamStateSpec.Kind.KEYED_LIST,
                        "generic-window-rows-v1", 1,
                        StreamStateSpec.TtlMode.NONE, 0L),
                StreamWindowResourceSpec.of(8, 1_000L,
                        8_192L, 32_768L, maximumSessionMergeFanIn,
                        StreamEmissionLimit.of(1, 4_096L)))
                : StreamGenericWindowSpec.aggregate(
                        assigner, trigger,
                        accumulationMode, allowedLatenessMillis, function(),
                        outputSchema(), outputKeys,
                        new StreamStateSpec("window-state",
                                StreamStateSpec.Kind.KEYED_AGGREGATING,
                                "generic-window-state-v1", 1,
                                StreamStateSpec.TtlMode.NONE, 0L),
                        StreamWindowResourceSpec.of(8, 1_000L,
                                8_192L, 32_768L,
                                maximumSessionMergeFanIn,
                                StreamEmissionLimit.of(1, 4_096L)));
        StreamSinkSpec sink = StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", digest("sink"), new byte[] {3});
        if (lateOutput) {
            StreamChangelogMode mode = accumulationMode
                    == StreamAccumulationMode.ACCUMULATING
                    ? StreamChangelogMode.UPSERT : StreamChangelogMode.APPEND;
            StreamOutputTag late = StreamOutputTag.of("late", inputSchema(),
                    mode, mode == StreamChangelogMode.UPSERT
                    ? Collections.singletonList("adaptor")
                    : Collections.emptyList(),
                    StreamEmissionLimit.of(1, 4_096L),
                    StreamEmissionLimit.of(1, 4_096L),
                    StreamEmissionLimit.of(1, 4_096L));
            io.rowweave.StreamWindowResult result = keyed
                    .genericWindowWithLateOutput(
                            specification.withLateOutput(late), "window");
            result.main().sinkTo("main-sink", sink);
            result.late().sinkTo("late-sink", sink);
        } else {
            keyed.genericWindow(specification).uid("window")
                    .sinkTo("sink", sink);
        }
        return StreamTestGraph.from(session.toPlanArtifact());
    }

    static FunctionDeploymentReference function() {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(digest("deployment-set"),
                "generic-window-function", digest("jar"),
                "example.GenericWindowFunction",
                FunctionAbi.STREAM_WINDOW_AGGREGATE_V1, "config-v1",
                digest(config), config,
                new FunctionResourceProfile(1_048_576L, 0L, 1));
    }

    static FunctionDeploymentReference processFunction() {
        byte[] config = new byte[] {5};
        return new FunctionDeploymentReference(digest("process-deployment"),
                "generic-window-process", digest("process-jar"),
                "example.GenericWindowProcessFunction",
                FunctionAbi.STREAM_WINDOW_PROCESS_V1, "config-v1",
                digest(config), config,
                new FunctionResourceProfile(1_048_576L, 0L, 1));
    }

    static StreamSchema inputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("event-time", "eventTime", "eventTime",
                        DataType.TimestampType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    static StreamSchema outputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("window-start", "windowStart", "windowStart",
                        DataType.LongType, false),
                new StreamField("window-end", "windowEnd", "windowEnd",
                        DataType.LongType, false),
                new StreamField("total", "total", "total",
                        DataType.BigDecimalType, false)));
    }

    static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    static String digest(byte[] value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : bytes) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
