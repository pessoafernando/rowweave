package io.rowweave.portable;

/** Closed v1 batch opcode registry with explicit numeric IDs and binding roles. */
public enum PortableOperatorCode {
    SOURCE(1, 0, BindingKind.SOURCE),
    SINK(2, 1, BindingKind.SINK),

    FILTER_EXPRESSION(10, 1, BindingKind.NONE),
    SELECT_EXPRESSIONS(11, 1, BindingKind.NONE),
    SELECT_INDICES(12, 1, BindingKind.NONE),
    ADD_FIELDS(13, 1, BindingKind.NONE),
    SCHEMA_CONVERT(14, 1, BindingKind.NONE),
    ROW_VALIDATE(15, 1, BindingKind.NONE),
    ROW_CLASSIFY(16, 1, BindingKind.NONE),
    QUALITY_ROUTE(17, 1, BindingKind.NONE),
    OBJECT_QUALITY_ROUTE(18, 1, BindingKind.NONE),
    PARTITION_HASH(19, 1, BindingKind.NONE),
    TYPED_QUALITY_ACCEPTED_SUMMARY(20, 1, BindingKind.NONE),
    TYPED_QUALITY_REJECTED_SUMMARY(21, 1, BindingKind.NONE),

    GROUP_SUM(30, 1, BindingKind.NONE),
    GROUP_COUNT(31, 1, BindingKind.NONE),
    GROUP_MAX(32, 1, BindingKind.NONE),
    GROUP_MIN(33, 1, BindingKind.NONE),
    GROUP_MULTI(34, 1, BindingKind.NONE),
    DISTINCT(35, 1, BindingKind.NONE),
    ORDER_BY(36, 1, BindingKind.NONE),
    TOP_SORTED(37, 1, BindingKind.NONE),
    TOP_FIRST(38, 1, BindingKind.NONE),
    WHOLE_AGG(39, 1, BindingKind.NONE),

    UNION(50, 2, BindingKind.NONE),
    JOIN(51, 2, BindingKind.NONE),

    FUNCTION_FILTER(60, 1, BindingKind.FUNCTION),
    FUNCTION_MAP(61, 1, BindingKind.FUNCTION),
    FUNCTION_FLAT_MAP(62, 1, BindingKind.FUNCTION),
    FUNCTION_REDUCE_GROUP(63, 1, BindingKind.FUNCTION),
    FUNCTION_COMBINE_GROUP(64, 1, BindingKind.FUNCTION),
    FUNCTION_COMBINE_REDUCE_GROUP(65, 1, BindingKind.FUNCTION),
    FUNCTION_CO_GROUP(66, 2, BindingKind.FUNCTION),
    FUNCTION_MAP_PARTITION(67, 1, BindingKind.FUNCTION);

    public enum BindingKind { NONE, SOURCE, FUNCTION, SINK }

    private final int wireId;
    private final int inputCount;
    private final BindingKind bindingKind;

    PortableOperatorCode(int wireId, int inputCount, BindingKind bindingKind) {
        this.wireId = wireId;
        this.inputCount = inputCount;
        this.bindingKind = bindingKind;
    }

    public int wireId() { return wireId; }
    public int inputCount() { return inputCount; }
    public BindingKind bindingKind() { return bindingKind; }

    public static PortableOperatorCode fromWireId(int wireId) {
        for (PortableOperatorCode value : values()) {
            if (value.wireId == wireId) return value;
        }
        throw PortablePlanText.failure("UNKNOWN_OPCODE", "nodes.opcode",
                "portable operator code is not supported");
    }
}
