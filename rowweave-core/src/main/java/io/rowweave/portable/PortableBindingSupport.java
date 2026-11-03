package io.rowweave.portable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

final class PortableBindingSupport {
    private static final Pattern KEY = Pattern.compile("[A-Za-z0-9._-]{1,256}");
    private static final Pattern DIGEST = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern CLASS_NAME = Pattern.compile(
            "[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+");

    private PortableBindingSupport() { }

    static String key(String value, String path) {
        if (value == null || !KEY.matcher(value).matches()) {
            throw PortablePlanText.failure("INVALID_BINDING", path,
                    "binding identity is not canonical");
        }
        return value;
    }

    static String digest(String value, String path) {
        if (value == null || !DIGEST.matcher(value).matches()) {
            throw PortablePlanText.failure("INVALID_DIGEST", path,
                    "binding digest must be lowercase SHA-256 hex");
        }
        return value;
    }

    static String className(String value) {
        if (value == null || value.length() > 512
                || !CLASS_NAME.matcher(value).matches()) {
            throw PortablePlanText.failure("INVALID_BINDING", "function.className",
                    "function class name is not canonical");
        }
        return value;
    }

    static byte[] frame(byte[] value, int maximum, String path) {
        if (value == null || value.length == 0 || value.length > maximum) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", path,
                    "binding frame length is outside its contract limit");
        }
        return value.clone();
    }

    static List<byte[]> splitFrames(List<byte[]> values) {
        if (values == null || values.isEmpty()
                || values.size() > PortableBindingLimits.MAX_SPLITS) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "source.splitFrames",
                    "source split count is outside its contract limit");
        }
        long total = 0L;
        List<byte[]> copied = new ArrayList<>(values.size());
        for (byte[] value : values) {
            byte[] frame = frame(value,
                    PortableBindingLimits.MAX_SPLIT_FRAME_BYTES,
                    "source.splitFrames");
            total += frame.length;
            if (total > PortableBindingLimits.MAX_TOTAL_SPLIT_BYTES) {
                throw PortablePlanText.failure("LIMIT_EXCEEDED", "source.splitFrames",
                        "source split bytes exceed their aggregate limit");
            }
            copied.add(frame);
        }
        return Collections.unmodifiableList(copied);
    }

    static List<byte[]> copyFrames(List<byte[]> values) {
        List<byte[]> copied = new ArrayList<>(values.size());
        for (byte[] value : values) copied.add(value.clone());
        return Collections.unmodifiableList(copied);
    }
}
