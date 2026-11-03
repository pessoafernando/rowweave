package io.rowweave.pipeline;

import java.util.Arrays;

final class VersionedDatasetHandoffSupport {
    private VersionedDatasetHandoffSupport() { }

    static String id(String value, String path) {
        return PipelineText.required(value, path,
                VersionedDatasetHandoffLimits.MAX_ID_BYTES);
    }

    static String digest(String value, String path) {
        PipelineText.required(value, path, 64);
        if (!value.matches("[0-9a-f]{64}")) {
            throw PipelineText.failure("INVALID_DIGEST", path,
                    "digest must be 64 lowercase hexadecimal characters");
        }
        return value;
    }

    static int positiveVersion(int value, String path) {
        if (value <= 0) {
            throw PipelineText.failure("INVALID_VERSION", path,
                    "version must be positive");
        }
        return value;
    }

    static long positive(long value, String path, String code) {
        if (value <= 0L) {
            throw PipelineText.failure(code, path, "value must be positive");
        }
        return value;
    }

    static long nonNegative(long value, String path) {
        if (value < 0L) {
            throw PipelineText.failure("INVALID_COUNT", path,
                    "value must not be negative");
        }
        return value;
    }

    static byte[] bytes(byte[] value, String path, int maximum) {
        if (value == null) {
            throw PipelineText.failure("INVALID_BYTES", path, "value is required");
        }
        if (value.length > maximum) {
            throw PipelineText.failure("LIMIT_EXCEEDED", path,
                    "byte value exceeds limit");
        }
        return value.clone();
    }

    static boolean bytesEqual(byte[] left, byte[] right) {
        return Arrays.equals(left, right);
    }

    static int bytesHash(byte[] value) {
        return Arrays.hashCode(value);
    }
}
