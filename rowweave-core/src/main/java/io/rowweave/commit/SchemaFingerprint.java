package io.rowweave.commit;

import io.rowweave.Field;
import io.rowweave.DataSchema;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Stable SHA-256 fingerprint binding a complete, ordered DataSchema to a manifest member. */
public final class SchemaFingerprint {

    private SchemaFingerprint() {
    }

    public static String of(DataSchema dataSchema) {
        if (dataSchema == null) {
            throw new IllegalArgumentException("dataSchema must not be null");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(1);
                out.writeInt(dataSchema.getFieldCount());
                for (int i = 0; i < dataSchema.getFieldCount(); i++) {
                    Field field = dataSchema.getField(i);
                    if (field == null || field.getDataType() == null) {
                        throw new IllegalArgumentException("dataSchema contains null field/type at index " + i);
                    }
                    writeText(out, field.getName(), "field name");
                    writeText(out, field.getAlias(), "field alias");
                    writeText(out, field.getDataType().name(), "field type");
                }
            }
            return hex(CommitManifest.sha256(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory schema fingerprint failed", impossible);
        }
    }

    private static void writeText(DataOutputStream out, String value, String field) throws IOException {
        if (value == null) {
            throw new IllegalArgumentException(field + " must not be null");
        }
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            result[i * 2] = digits[(bytes[i] >>> 4) & 0x0f];
            result[i * 2 + 1] = digits[bytes[i] & 0x0f];
        }
        return new String(result);
    }
}
