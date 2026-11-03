package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamAsyncBinding;
import io.rowweave.stream.contract.StreamBroadcastStateDescriptor;
import io.rowweave.stream.contract.StreamProcessBinding;
import io.rowweave.stream.contract.StreamStateDescriptor;
import io.rowweave.stream.contract.StreamTimerDescriptor;
import io.rowweave.stream.contract.StreamWindowBinding;
import io.rowweave.stream.contract.StreamWindowResourceDescriptor;
import io.rowweave.stream.runtime.FinalEventTimeDecimalWindow;

/** Validates one operator's finite resource contract without graph shapes. */
final class StreamNodeResourceValidator {
    private static final long TEMPORAL_INDEX_OVERHEAD_BYTES = 256L;

    private StreamNodeResourceValidator() { }

    static void validate(StreamExecutionGraph.Node node) {
        PortableStreamNode portable = node.portableNode();
        switch (portable.opcode()) {
            case ASYNC_WAIT:
                validateAsync((StreamAsyncBinding) portable.binding(), node);
                return;
            case EVENT_TIME_WINDOW:
                validateEventTimeWindow(portable, node);
                return;
            case GENERIC_WINDOW:
                validateGenericWindow((StreamWindowBinding)
                        portable.binding(), node);
                return;
            case KEYED_AGGREGATE:
                validateAggregate(portable, node);
                return;
            case KEYED_PROCESS:
            case KEYED_CO_PROCESS:
                validateProcess((StreamProcessBinding) portable.binding(),
                        node);
                return;
            case TEMPORAL_JOIN:
                validateTemporalJoin(node);
                return;
            default:
                return;
        }
    }

    private static void validateAsync(StreamAsyncBinding binding,
            StreamExecutionGraph.Node node) {
        StreamExecutionGraph.ResourceRequest resources =
                node.resourceRequest();
        if (binding.resources().maximumPendingBytes()
                > resources.maximumResidentBytes()
                || resources.threadCount()
                < binding.resources().maximumConcurrency() + 2
                || resources.checkpointBytes() <= 0L) {
            incomplete("async wait");
        }
    }

    private static void validateEventTimeWindow(PortableStreamNode portable,
            StreamExecutionGraph.Node node) {
        StreamExecutionGraph.ResourceRequest resources =
                node.resourceRequest();
        long size = Long.parseLong(
                portable.scalarParameters().get("window.sizeMillis"));
        long slide = Long.parseLong(
                portable.scalarParameters().get("window.slideMillis"));
        long windowsPerRecord = 1L + (size - 1L) / slide;
        StreamStateDescriptor state = portable.stateDescriptors().get(0);
        if (!FinalEventTimeDecimalWindow.DECIMAL_STATE_SERIALIZER.equals(
                state.serializerContractId())
                || resources.maximumResidentBytes() <= 0L
                || resources.spillDiskBytes() <= 0L
                || resources.timerCount() <= 0
                || resources.windowCount() < windowsPerRecord
                || windowsPerRecord > 1_024L) {
            incomplete("window");
        }
    }

    private static void validateGenericWindow(StreamWindowBinding binding,
            StreamExecutionGraph.Node node) {
        StreamExecutionGraph.ResourceRequest resources =
                node.resourceRequest();
        StreamWindowResourceDescriptor descriptor = binding.resources();
        boolean timed = binding.assignerKind()
                != StreamWindowBinding.AssignerKind.COUNT_TUMBLING
                && binding.assignerKind()
                != StreamWindowBinding.AssignerKind.COUNT_SLIDING;
        boolean count = !timed;
        long assignmentFanOut = binding.slide() == 0L ? 1L
                : 1L + (binding.sizeOrGap() - 1L) / binding.slide();
        long stateCapacity = add(resources.maximumResidentBytes(),
                resources.spillDiskBytes(), "generic window");
        if (resources.maximumResidentBytes() <= 0L
                || resources.spillDiskBytes() <= 0L
                || resources.checkpointBytes() <= 0L
                || timed && resources.timerCount() <= 0
                || resources.windowCount()
                < descriptor.maximumActiveWindowsPerKey()
                || count && binding.sizeOrGap()
                > descriptor.maximumRowsPerWindow()
                || assignmentFanOut
                > descriptor.maximumActiveWindowsPerKey()
                || descriptor.maximumTotalBytes() > stateCapacity) {
            incomplete("generic window");
        }
    }

    private static void validateAggregate(PortableStreamNode portable,
            StreamExecutionGraph.Node node) {
        StreamExecutionGraph.ResourceRequest resources =
                node.resourceRequest();
        if (portable.stateDescriptors().size() != 1) {
            incomplete("aggregate");
        }
        StreamStateDescriptor state = portable.stateDescriptors().get(0);
        if (state.kind() != StreamStateDescriptor.Kind.KEYED_AGGREGATING
                || state.ttlMode() == StreamStateDescriptor.TtlMode.NONE
                || resources.maximumResidentBytes() <= 0L
                || resources.spillDiskBytes() <= 0L
                || resources.checkpointBytes() <= 0L) {
            incomplete("aggregate");
        }
    }

    private static void validateProcess(StreamProcessBinding binding,
            StreamExecutionGraph.Node node) {
        StreamExecutionGraph.ResourceRequest resources =
                node.resourceRequest();
        if (binding.managedStates().isEmpty()
                && binding.broadcastState() == null
                || resources.maximumResidentBytes() <= 0L
                || resources.spillDiskBytes() <= 0L
                || resources.checkpointBytes() <= 0L
                || !timerBudgetFits(binding, resources.timerCount())
                || !broadcastBudgetFits(binding.broadcastState(),
                resources)) {
            incomplete("keyed process");
        }
    }

    private static boolean timerBudgetFits(StreamProcessBinding binding,
            int timerBudget) {
        try {
            long declaredPerSubtask = 0L;
            for (StreamTimerDescriptor timer : binding.timerFamilies()) {
                declaredPerSubtask = Math.addExact(declaredPerSubtask,
                        timer.maximumTotalTimers());
            }
            return declaredPerSubtask <= timerBudget;
        } catch (ArithmeticException overflow) {
            return false;
        }
    }

    private static boolean broadcastBudgetFits(
            StreamBroadcastStateDescriptor broadcast,
            StreamExecutionGraph.ResourceRequest resources) {
        if (broadcast == null) return true;
        try {
            return broadcast.maximumTotalBytes() <= Math.addExact(
                    resources.maximumResidentBytes(),
                    resources.spillDiskBytes());
        } catch (ArithmeticException overflow) {
            return false;
        }
    }

    private static void validateTemporalJoin(
            StreamExecutionGraph.Node node) {
        StreamExecutionGraph.ResourceRequest resources =
                node.resourceRequest();
        long minimumResident = add(resources.maximumFrameBytes(),
                TEMPORAL_INDEX_OVERHEAD_BYTES, "temporal join");
        if (resources.maximumResidentBytes() < minimumResident
                || resources.spillDiskBytes() <= 0L
                || resources.checkpointBytes() <= 0L) {
            incomplete("temporal join");
        }
    }

    private static long add(long left, long right, String operator) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new UnsupportedOperationException(
                    "Stream " + operator + " resource bounds overflow",
                    overflow);
        }
    }

    private static void incomplete(String operator) {
        throw new UnsupportedOperationException(
                "Stream " + operator + " resource bounds are incomplete");
    }
}
