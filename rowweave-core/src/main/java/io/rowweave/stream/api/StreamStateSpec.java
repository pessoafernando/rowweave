package io.rowweave.stream.api;

import java.util.Objects;

/** Stable managed-state identity and finite TTL policy exposed by the Stream SDK. */
public final class StreamStateSpec {
    public static final String DECIMAL_WINDOW_SERIALIZER_CONTRACT =
            "rowweave.decimal-unscaled-v1";
    public enum Kind {
        KEYED_VALUE, KEYED_MAP, KEYED_AGGREGATING, OPERATOR_LIST, KEYED_LIST
    }
    public enum TtlMode { NONE, PROCESSING_TIME, EVENT_TIME }

    private final String stateName;
    private final Kind kind;
    private final String serializerContractId;
    private final int stateSchemaVersion;
    private final TtlMode ttlMode;
    private final long ttlMillis;

    public StreamStateSpec(String stateName, Kind kind,
            String serializerContractId, int stateSchemaVersion,
            TtlMode ttlMode, long ttlMillis) {
        if (stateName == null || stateName.isEmpty() || kind == null
                || serializerContractId == null || serializerContractId.isEmpty()
                || stateSchemaVersion <= 0 || ttlMode == null || ttlMillis < 0L
                || (ttlMode == TtlMode.NONE && ttlMillis != 0L)
                || (ttlMode != TtlMode.NONE && ttlMillis == 0L)) {
            throw new IllegalArgumentException("invalid Stream state specification");
        }
        this.stateName = stateName;
        this.kind = kind;
        this.serializerContractId = serializerContractId;
        this.stateSchemaVersion = stateSchemaVersion;
        this.ttlMode = ttlMode;
        this.ttlMillis = ttlMillis;
    }

    /** Canonical managed-state contract for the built-in final decimal window. */
    public static StreamStateSpec decimalWindow(String stateName) {
        return new StreamStateSpec(stateName, Kind.KEYED_VALUE,
                DECIMAL_WINDOW_SERIALIZER_CONTRACT, 1, TtlMode.NONE, 0L);
    }

    public String stateName() { return stateName; }
    public Kind kind() { return kind; }
    public String serializerContractId() { return serializerContractId; }
    public int stateSchemaVersion() { return stateSchemaVersion; }
    public TtlMode ttlMode() { return ttlMode; }
    public long ttlMillis() { return ttlMillis; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamStateSpec)) return false;
        StreamStateSpec that = (StreamStateSpec) other;
        return stateSchemaVersion == that.stateSchemaVersion
                && ttlMillis == that.ttlMillis && stateName.equals(that.stateName)
                && kind == that.kind
                && serializerContractId.equals(that.serializerContractId)
                && ttlMode == that.ttlMode;
    }

    @Override public int hashCode() {
        return Objects.hash(stateName, kind, serializerContractId,
                stateSchemaVersion, ttlMode, ttlMillis);
    }
}
