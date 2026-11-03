package io.rowweave.incremental;

import io.rowweave.Field;
import io.rowweave.IncrementalMapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.mem.RowSerializer;

import java.security.MessageDigest;
import java.util.List;

/** Per-row byte-exact identity guard for the dedicated incremental map. */
final class IncrementalMapIdentityGuard {
    private IncrementalMapIdentityGuard() {
    }

    static IncrementalMapFunction guard(
            IncrementalMapFunction function, DataSchema dataSchema,
            IncrementalPlanDescriptor descriptor) {
        if (function == null || dataSchema == null || descriptor == null
                || !SchemaFingerprint.of(dataSchema).equals(
                hex(descriptor.outputSchemaFingerprint()))) {
            throw new IllegalArgumentException(
                    "incremental map binding is invalid");
        }
        int[] identityIndexes = identityIndexes(descriptor);
        Field[] identityFields = new Field[identityIndexes.length];
        for (int index = 0; index < identityIndexes.length; index++) {
            identityFields[index] = dataSchema.getField(identityIndexes[index]);
        }
        RowSerializer serializer = new RowSerializer(
                new DataSchema(identityFields));
        return input -> {
            if (input == null || input.size() != dataSchema.getFieldCount()) {
                throw invalid();
            }
            byte[] before = serializer.serializeProjected(
                    input, identityIndexes);
            DataRow output = function.map(input);
            if (output == null || output.size() != dataSchema.getFieldCount()) {
                throw invalid();
            }
            byte[] after = serializer.serializeProjected(
                    output, identityIndexes);
            if (!MessageDigest.isEqual(before, after)) {
                throw invalid();
            }
            return output;
        };
    }

    private static int[] identityIndexes(
            IncrementalPlanDescriptor descriptor) {
        List<Integer> keys = descriptor.primaryKeyFieldIndexes();
        List<Integer> positions = descriptor.recordPositionFieldIndexes();
        int[] indexes = new int[1 + keys.size() + positions.size()];
        int cursor = 0;
        indexes[cursor++] = descriptor.operationFieldIndex();
        for (Integer key : keys) indexes[cursor++] = key;
        for (Integer position : positions) indexes[cursor++] = position;
        return indexes;
    }

    private static String hex(byte[] value) {
        char[] chars = new char[value.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int index = 0; index < value.length; index++) {
            int unsigned = value[index] & 0xff;
            chars[index * 2] = digits[unsigned >>> 4];
            chars[index * 2 + 1] = digits[unsigned & 0xf];
        }
        return new String(chars);
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException(
                "incremental map changed operation, key, or record position");
    }
}
