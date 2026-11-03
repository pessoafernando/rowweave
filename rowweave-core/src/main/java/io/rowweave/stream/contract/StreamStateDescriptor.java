package io.rowweave.stream.contract;

import java.util.Objects;

/** Stable managed-state identity; runtime storage is introduced by 3. */
public final class StreamStateDescriptor {
    public enum Kind { KEYED_VALUE(1), KEYED_MAP(2), KEYED_AGGREGATING(3), OPERATOR_LIST(4), KEYED_LIST(5);
        private final int id; Kind(int id) { this.id=id; } int wireId(){return id;}
        static Kind fromWire(int id){for(Kind v:values())if(v.id==id)return v;throw StreamContractSupport.failure("UNKNOWN_STATE_KIND","state.kind","unknown state kind");}}
    public enum TtlMode { NONE(0), PROCESSING_TIME(1), EVENT_TIME(2);
        private final int id; TtlMode(int id){this.id=id;} int wireId(){return id;}
        static TtlMode fromWire(int id){for(TtlMode v:values())if(v.id==id)return v;throw StreamContractSupport.failure("UNKNOWN_TTL_MODE","state.ttlMode","unknown TTL mode");}}
    private final String stateName;
    private final Kind kind;
    private final String serializerContractId;
    private final int stateSchemaVersion;
    private final TtlMode ttlMode;
    private final long ttlMillis;

    public StreamStateDescriptor(String stateName, Kind kind, String serializerContractId,
                                 int stateSchemaVersion, TtlMode ttlMode, long ttlMillis) {
        this.stateName=StreamContractSupport.text(stateName,"state.name",256);
        this.serializerContractId=StreamContractSupport.text(serializerContractId,"state.serializer",256);
        if(kind==null||ttlMode==null||stateSchemaVersion<=0||ttlMillis<0L||(ttlMode==TtlMode.NONE&&ttlMillis!=0L)||(ttlMode!=TtlMode.NONE&&ttlMillis==0L))
            throw StreamContractSupport.failure("INVALID_STATE_DESCRIPTOR","state","invalid state schema or TTL");
        this.kind=kind;this.stateSchemaVersion=stateSchemaVersion;this.ttlMode=ttlMode;this.ttlMillis=ttlMillis;
    }
    public String stateName(){return stateName;} public Kind kind(){return kind;} public String serializerContractId(){return serializerContractId;}
    public int stateSchemaVersion(){return stateSchemaVersion;} public TtlMode ttlMode(){return ttlMode;} public long ttlMillis(){return ttlMillis;}
    @Override public boolean equals(Object o){if(!(o instanceof StreamStateDescriptor))return false;StreamStateDescriptor s=(StreamStateDescriptor)o;return stateSchemaVersion==s.stateSchemaVersion&&ttlMillis==s.ttlMillis&&stateName.equals(s.stateName)&&kind==s.kind&&serializerContractId.equals(s.serializerContractId)&&ttlMode==s.ttlMode;}
    @Override public int hashCode(){return Objects.hash(stateName,kind,serializerContractId,stateSchemaVersion,ttlMode,ttlMillis);}
}
