package io.rowweave.stream.api;

final class StreamIdentifierSupport {
    private StreamIdentifierSupport() { }

    static String validate(String value, String label) {
        if (value == null || value.trim().isEmpty() || value.length() > 256) {
            throw new IllegalArgumentException(label + " must contain 1..256 characters");
        }
        return value;
    }
}
