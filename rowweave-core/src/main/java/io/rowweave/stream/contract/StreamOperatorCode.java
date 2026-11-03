package io.rowweave.stream.contract;

/** Closed Stream opcode registry with explicit wire IDs. */
public enum StreamOperatorCode {
    SOURCE(1, 0, BindingKind.SOURCE),
    MAP(2, 1, BindingKind.FUNCTION),
    FILTER(3, 1, BindingKind.FUNCTION),
    KEY_BY(4, 1, BindingKind.NONE),
    KEYED_AGGREGATE(5, 1, BindingKind.FUNCTION),
    EVENT_TIME_WINDOW(6, 1, BindingKind.NONE),
    TEMPORAL_JOIN(7, 2, BindingKind.FUNCTION),
    SINK(8, 1, BindingKind.SINK),
    CALC(9, 1, BindingKind.CALC),
    FLAT_MAP(10, 1, BindingKind.FUNCTION),
    UNION(11, 2, BindingKind.NONE),
    ASSIGN_EVENT_TIME(12, 1, BindingKind.TIME),
    KEYED_PROCESS(13, 1, BindingKind.FUNCTION),
    KEYED_CO_PROCESS(14, 2, BindingKind.FUNCTION),
    GENERIC_WINDOW(15, 1, BindingKind.WINDOW),
    ASYNC_WAIT(16, 1, BindingKind.ASYNC);

    public enum BindingKind {
        NONE, SOURCE, FUNCTION, SINK, CALC, TIME, WINDOW, ASYNC
    }

    private final int wireId;
    private final int inputCount;
    private final BindingKind bindingKind;

    StreamOperatorCode(int wireId, int inputCount, BindingKind bindingKind) {
        this.wireId = wireId;
        this.inputCount = inputCount;
        this.bindingKind = bindingKind;
    }

    public int wireId() { return wireId; }
    public int inputCount() { return inputCount; }
    public int minimumInputCount() { return inputCount; }
    public int maximumInputCount() { return inputCount; }
    public BindingKind bindingKind() { return bindingKind; }

    static StreamOperatorCode fromWire(int id) {
        for (StreamOperatorCode value : values()) if (value.wireId == id) return value;
        throw StreamContractSupport.failure("UNKNOWN_OPCODE", "nodes.opcode",
                "unknown Stream operator code");
    }
}
