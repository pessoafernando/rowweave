package io.rowweave.portable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Canonical ordered v1 row schema with a stable semantic fingerprint. */
public final class PortableRowSchema {
    public static final int MAX_FIELDS = 1024;
    private static final byte[] MAGIC =
            "RWXXSC01".getBytes(StandardCharsets.US_ASCII);

    private final List<PortableField> fields;
    private final String fingerprint;

    public PortableRowSchema(List<PortableField> fields) {
        if (fields == null || fields.isEmpty()) {
            throw PortablePlanText.failure("INVALID_SCHEMA", "schema.fields",
                    "portable schema requires at least one field");
        }
        if (fields.size() > MAX_FIELDS) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "schema.fields",
                    "portable schema field count exceeds its limit");
        }
        List<PortableField> copied = new ArrayList<>(fields.size());
        Set<String> fieldIds = new HashSet<>();
        for (int index = 0; index < fields.size(); index++) {
            PortableField field = fields.get(index);
            if (field == null) {
                throw PortablePlanText.failure("INVALID_FIELD",
                        "schema.fields", "schema field is required");
            }
            if (!fieldIds.add(field.fieldId())) {
                throw PortablePlanText.failure("DUPLICATE_FIELD_ID",
                        "schema.fields", "field IDs must be unique");
            }
            copied.add(field);
        }
        this.fields = Collections.unmodifiableList(copied);
        this.fingerprint = PortablePlanText.hex(
                PortablePlanText.sha256(canonicalBytes()));
    }

    public List<PortableField> fields() { return fields; }
    public int fieldCount() { return fields.size(); }
    public PortableField field(int index) { return fields.get(index); }
    public String fingerprint() { return fingerprint; }

    byte[] canonicalBytes() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.write(MAGIC);
                output.writeInt(1);
                output.writeInt(fields.size());
                for (PortableField field : fields) {
                    writeText(output, field.fieldId());
                    writeText(output, field.name());
                    writeText(output, field.alias());
                    output.writeInt(field.dataType().wireId());
                    output.writeBoolean(field.nullable());
                }
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory schema encoding failed", impossible);
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PortableRowSchema)) return false;
        PortableRowSchema that = (PortableRowSchema) other;
        return fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }
}
