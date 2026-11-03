package io.rowweave.portable;

/** Stable function ABI IDs; Java interface names are not used as wire tags. */
public enum FunctionAbi {
    FILTER_V1(1),
    MAP_V1(2),
    FLAT_MAP_V1(3),
    REDUCE_GROUP_V1(4),
    COMBINE_GROUP_V1(5),
    COMBINE_REDUCE_GROUP_V1(6),
    CO_GROUP_V1(7),
    STREAM_AGGREGATE_V1(8),
    KEYED_PROCESS_V1(9),
    KEYED_CO_PROCESS_V1(10),
    STREAM_WINDOW_AGGREGATE_V1(11),
    STREAM_WINDOW_PROCESS_V1(12),
    STREAM_ASYNC_V1(13),
    STREAM_RETRACT_AGGREGATE_V2(14),
    STREAM_RETRACT_WINDOW_AGGREGATE_V2(15),
    MAP_PARTITION_V1(16);

    private final int wireId;

    FunctionAbi(int wireId) { this.wireId = wireId; }

    public int wireId() { return wireId; }

    public static FunctionAbi fromWireId(int wireId) {
        for (FunctionAbi value : values()) {
            if (value.wireId == wireId) return value;
        }
        throw PortablePlanText.failure("UNKNOWN_FUNCTION_ABI", "function.abi",
                "function ABI is not supported");
    }
}
