package io.rowweave.stream.api;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Ordered immutable schema used as the authority for Stream field identity. */
public final class StreamSchema {
    private final List<StreamField> fields;
    private final PortableRowSchema portable;

    public StreamSchema(List<StreamField> fields) {
        if (fields == null || fields.isEmpty()
                || fields.size() > PortableRowSchema.MAX_FIELDS) {
            throw new IllegalArgumentException("Stream schema requires 1..1024 fields");
        }
        List<StreamField> copied = new ArrayList<>(fields.size());
        List<PortableField> portableFields = new ArrayList<>(fields.size());
        Set<String> identities = new HashSet<>();
        for (StreamField field : fields) {
            if (field == null || !identities.add(field.fieldId())) {
                throw new IllegalArgumentException("Stream field identities must be non-null and unique");
            }
            copied.add(field);
            portableFields.add(new PortableField(field.fieldId(), field.name(),
                    field.alias(), portableType(field.dataType()), field.nullable()));
        }
        this.fields = Collections.unmodifiableList(copied);
        this.portable = new PortableRowSchema(portableFields);
    }

    private static PortableDataType portableType(DataType type) {
        switch (type) {
            case BooleanType: return PortableDataType.BOOLEAN;
            case StringType: return PortableDataType.STRING;
            case IntegerType: return PortableDataType.INTEGER;
            case LongType: return PortableDataType.LONG;
            case BigDecimalType: return PortableDataType.BIG_DECIMAL;
            case DateType: return PortableDataType.DATE;
            case TimestampType: return PortableDataType.TIMESTAMP;
            case NullType: return PortableDataType.NULL;
            default: throw new IllegalArgumentException("unsupported data type: " + type);
        }
    }

    public int fieldCount() { return fields.size(); }
    public StreamField field(int index) { return fields.get(index); }
    public List<StreamField> fields() { return fields; }
    public String fingerprint() { return portable.fingerprint(); }
    public PortableRowSchema toPortableSchema() { return portable; }

    public DataSchema toDataSchema() {
        Field[] projection = new Field[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            StreamField field = fields.get(i);
            projection[i] = new Field(field.name(), field.alias(), field.dataType());
        }
        return new DataSchema(projection);
    }

    @Override public boolean equals(Object other) {
        return other instanceof StreamSchema
                && fields.equals(((StreamSchema) other).fields);
    }

    @Override public int hashCode() { return Objects.hash(fields); }
}
