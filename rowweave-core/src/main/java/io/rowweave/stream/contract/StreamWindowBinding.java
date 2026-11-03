package io.rowweave.stream.contract;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.stream.api.StreamWindowAssignerSpec;

import java.util.Objects;

/** Portable assigner, trigger, accumulation and function contract for a window. */
public final class StreamWindowBinding implements StreamNodeBinding {
    public enum AssignerKind {
        EVENT_TIME_TUMBLING(1), EVENT_TIME_SLIDING(2),
        EVENT_TIME_SESSION(3), PROCESSING_TIME_TUMBLING(4),
        PROCESSING_TIME_SLIDING(5), PROCESSING_TIME_SESSION(6),
        COUNT_TUMBLING(7), COUNT_SLIDING(8);
        private final int wireId;
        AssignerKind(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static AssignerKind fromWireId(int wireId) {
            for (AssignerKind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_WINDOW_ASSIGNER", "unknown assigner");
        }
    }

    public enum TriggerKind {
        EVENT_TIME_FINAL(1), PROCESSING_TIME(2), COUNT(3);
        private final int wireId;
        TriggerKind(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static TriggerKind fromWireId(int wireId) {
            for (TriggerKind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_WINDOW_TRIGGER", "unknown trigger");
        }
    }

    public enum AccumulationMode {
        DISCARDING(1), ACCUMULATING(2);
        private final int wireId;
        AccumulationMode(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static AccumulationMode fromWireId(int wireId) {
            for (AccumulationMode value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_WINDOW_ACCUMULATION",
                    "unknown accumulation mode");
        }
    }

    public enum FunctionKind {
        AGGREGATE(1, FunctionAbi.STREAM_WINDOW_AGGREGATE_V1),
        PROCESS(2, FunctionAbi.STREAM_WINDOW_PROCESS_V1);
        private final int wireId;
        private final FunctionAbi abi;
        FunctionKind(int wireId, FunctionAbi abi) {
            this.wireId = wireId;
            this.abi = abi;
        }
        int wireId() { return wireId; }
        static FunctionKind fromWireId(int wireId) {
            for (FunctionKind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_WINDOW_FUNCTION_KIND",
                    "unknown window function kind");
        }
    }

    private final AssignerKind assignerKind;
    private final String eventTimeFieldId;
    private final long sizeOrGap;
    private final long slide;
    private final TriggerKind triggerKind;
    private final long triggerIntervalOrCount;
    private final boolean purging;
    private final AccumulationMode accumulationMode;
    private final long allowedLatenessMillis;
    private final FunctionKind functionKind;
    private final FunctionDeploymentReference deploymentReference;
    private final StreamWindowResourceDescriptor resources;
    private final StreamOutputTagDescriptor lateOutputTag;

    public StreamWindowBinding(AssignerKind assignerKind,
            String eventTimeFieldId, long sizeOrGap, long slide,
            TriggerKind triggerKind, long triggerIntervalOrCount,
            boolean purging, AccumulationMode accumulationMode,
            long allowedLatenessMillis, FunctionKind functionKind,
            FunctionDeploymentReference deploymentReference,
            StreamWindowResourceDescriptor resources) {
        this(assignerKind, eventTimeFieldId, sizeOrGap, slide, triggerKind,
                triggerIntervalOrCount, purging, accumulationMode,
                allowedLatenessMillis, functionKind, deploymentReference,
                resources, null);
    }

    public StreamWindowBinding(AssignerKind assignerKind,
            String eventTimeFieldId, long sizeOrGap, long slide,
            TriggerKind triggerKind, long triggerIntervalOrCount,
            boolean purging, AccumulationMode accumulationMode,
            long allowedLatenessMillis, FunctionKind functionKind,
            FunctionDeploymentReference deploymentReference,
            StreamWindowResourceDescriptor resources,
            StreamOutputTagDescriptor lateOutputTag) {
        if (assignerKind == null || triggerKind == null
                || accumulationMode == null || functionKind == null
                || deploymentReference == null || resources == null
                || !acceptsAbi(functionKind,
                deploymentReference.functionAbi())
                || sizeOrGap <= 0L || slide < 0L
                || allowedLatenessMillis < 0L
                || allowedLatenessMillis
                > StreamWindowAssignerSpec.MAX_TIME_MILLIS) {
            throw failure("INVALID_WINDOW_BINDING",
                    "window binding is invalid");
        }
        boolean eventTime = eventTime(assignerKind);
        boolean count = count(assignerKind);
        boolean sliding = sliding(assignerKind);
        long maximum = count ? StreamWindowAssignerSpec.MAX_COUNT
                : StreamWindowAssignerSpec.MAX_TIME_MILLIS;
        if (sizeOrGap > maximum
                || sliding && (slide <= 0L || slide > sizeOrGap
                || slide > maximum)
                || !sliding && slide != 0L
                || eventTime != (eventTimeFieldId != null
                && !eventTimeFieldId.isEmpty())
                || !eventTime && allowedLatenessMillis != 0L
                || eventTime && triggerKind != TriggerKind.EVENT_TIME_FINAL
                || count && triggerKind != TriggerKind.COUNT
                || !eventTime && !count
                && triggerKind != TriggerKind.PROCESSING_TIME
                || triggerKind == TriggerKind.EVENT_TIME_FINAL
                && triggerIntervalOrCount != 0L
                || triggerKind != TriggerKind.EVENT_TIME_FINAL
                && (triggerIntervalOrCount <= 0L
                || triggerIntervalOrCount > maximum)) {
            throw failure("INVALID_WINDOW_BINDING",
                    "window assigner and trigger bounds are inconsistent");
        }
        this.assignerKind = assignerKind;
        this.eventTimeFieldId = eventTimeFieldId == null ? null
                : StreamContractSupport.text(eventTimeFieldId,
                "nodes.window.eventTimeFieldId", 256);
        this.sizeOrGap = sizeOrGap;
        this.slide = slide;
        this.triggerKind = triggerKind;
        this.triggerIntervalOrCount = triggerIntervalOrCount;
        this.purging = purging;
        this.accumulationMode = accumulationMode;
        this.allowedLatenessMillis = allowedLatenessMillis;
        this.functionKind = functionKind;
        this.deploymentReference = deploymentReference;
        this.resources = resources;
        if (lateOutputTag != null && !eventTime) {
            throw failure("INVALID_WINDOW_BINDING",
                    "late output requires event-time assignment");
        }
        this.lateOutputTag = lateOutputTag;
    }

    private static boolean acceptsAbi(FunctionKind kind, FunctionAbi abi) {
        return kind == FunctionKind.AGGREGATE
                ? abi == FunctionAbi.STREAM_WINDOW_AGGREGATE_V1
                || abi == FunctionAbi.STREAM_RETRACT_WINDOW_AGGREGATE_V2
                : abi == FunctionAbi.STREAM_WINDOW_PROCESS_V1;
    }

    private static boolean eventTime(AssignerKind value) {
        return value == AssignerKind.EVENT_TIME_TUMBLING
                || value == AssignerKind.EVENT_TIME_SLIDING
                || value == AssignerKind.EVENT_TIME_SESSION;
    }

    private static boolean count(AssignerKind value) {
        return value == AssignerKind.COUNT_TUMBLING
                || value == AssignerKind.COUNT_SLIDING;
    }

    private static boolean sliding(AssignerKind value) {
        return value == AssignerKind.EVENT_TIME_SLIDING
                || value == AssignerKind.PROCESSING_TIME_SLIDING
                || value == AssignerKind.COUNT_SLIDING;
    }

    private static StreamContractException failure(String code,
            String message) {
        return StreamContractSupport.failure(code, "nodes.window", message);
    }

    @Override public StreamOperatorCode.BindingKind kind() {
        return StreamOperatorCode.BindingKind.WINDOW;
    }
    public AssignerKind assignerKind() { return assignerKind; }
    public String eventTimeFieldId() { return eventTimeFieldId; }
    public long sizeOrGap() { return sizeOrGap; }
    public long slide() { return slide; }
    public TriggerKind triggerKind() { return triggerKind; }
    public long triggerIntervalOrCount() { return triggerIntervalOrCount; }
    public boolean purging() { return purging; }
    public AccumulationMode accumulationMode() { return accumulationMode; }
    public long allowedLatenessMillis() { return allowedLatenessMillis; }
    public FunctionKind functionKind() { return functionKind; }
    public FunctionDeploymentReference deploymentReference() {
        return deploymentReference;
    }
    public StreamWindowResourceDescriptor resources() { return resources; }
    public StreamOutputTagDescriptor lateOutputTag() { return lateOutputTag; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamWindowBinding)) return false;
        StreamWindowBinding that = (StreamWindowBinding) other;
        return assignerKind == that.assignerKind
                && Objects.equals(eventTimeFieldId, that.eventTimeFieldId)
                && sizeOrGap == that.sizeOrGap && slide == that.slide
                && triggerKind == that.triggerKind
                && triggerIntervalOrCount == that.triggerIntervalOrCount
                && purging == that.purging
                && accumulationMode == that.accumulationMode
                && allowedLatenessMillis == that.allowedLatenessMillis
                && functionKind == that.functionKind
                && deploymentReference.equals(that.deploymentReference)
                && resources.equals(that.resources)
                && Objects.equals(lateOutputTag, that.lateOutputTag);
    }

    @Override public int hashCode() {
        return Objects.hash(assignerKind, eventTimeFieldId, sizeOrGap, slide,
                triggerKind, triggerIntervalOrCount, purging,
                accumulationMode, allowedLatenessMillis, functionKind,
                deploymentReference, resources, lateOutputTag);
    }
}
