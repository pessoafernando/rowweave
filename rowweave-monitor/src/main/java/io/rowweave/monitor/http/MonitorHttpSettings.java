package io.rowweave.monitor.http;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Sanitized monitor HTTP runtime settings exposed by the config endpoint. */
final class MonitorHttpSettings {
    private final String bindAddress;
    private final int history;
    private final List<String> allowedOrigins;

    MonitorHttpSettings(String bindAddress, int history, List<String> allowedOrigins) {
        this.bindAddress = bindAddress == null || bindAddress.codePoints().allMatch(Character::isWhitespace)
                ? "127.0.0.1" : bindAddress;
        if (history < 1) {
            throw new IllegalArgumentException("monitor history must be positive");
        }
        this.history = history;
        for (String origin : allowedOrigins) {
            if (origin == null || origin.codePoints().allMatch(Character::isWhitespace) || origin.contains("*")) {
                throw new IllegalArgumentException(
                        "monitor CORS origins must be exact values; wildcard is not allowed");
            }
        }
        this.allowedOrigins = Collections.unmodifiableList(new ArrayList<>(allowedOrigins));
    }

    String bindAddress() { return bindAddress; }

    int history() { return history; }

    List<String> allowedOrigins() { return allowedOrigins; }

    MonitorHttpSettings forListenerPort(int port) {
        List<String> resolved = new ArrayList<>();
        if (allowedOrigins.isEmpty()) {
            resolved.add("http://127.0.0.1:" + port);
        }
        for (String origin : allowedOrigins) {
            resolved.add("http://127.0.0.1".equals(origin)
                    ? "http://127.0.0.1:" + port : origin);
        }
        return new MonitorHttpSettings(bindAddress, history, resolved);
    }
}
