package io.rowweave.cluster.rpc;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;

/** Maps the SHA-256 fingerprint of a certificate DER encoding to its non-empty set of roles. */
public final class ControlAuthzPolicy {
    private final Map<String, EnumSet<ControlRole>> entries;
    private final String authzSetId;

    private ControlAuthzPolicy(Map<String, EnumSet<ControlRole>> entries, String canonical) {
        this.entries = entries;
        this.authzSetId = sha256Hex(canonical.getBytes(StandardCharsets.US_ASCII));
    }

    public static ControlAuthzPolicy parse(String text) {
        if (text == null) throw new IllegalArgumentException("authz policy is required");
        TreeMap<String, EnumSet<ControlRole>> parsed = new TreeMap<>();
        String[] lines = text.split("\\r?\\n", -1);
        for (int lineNumber = 0; lineNumber < lines.length; lineNumber++) {
            String line = lines[lineNumber].trim();
            if (line.isEmpty() || line.startsWith("#")) continue;
            int separator = line.indexOf('=');
            if (separator <= 0 || separator != line.lastIndexOf('=')) {
                throw invalid(lineNumber, "expected fingerprint=ROLE[,ROLE]");
            }
            String fingerprint = line.substring(0, separator).trim();
            requireFingerprint(fingerprint, lineNumber);
            EnumSet<ControlRole> roles = EnumSet.noneOf(ControlRole.class);
            String roleText = line.substring(separator + 1).trim();
            if (roleText.isEmpty()) throw invalid(lineNumber, "role set must not be empty");
            for (String token : roleText.split(",", -1)) {
                String name = token.trim();
                final ControlRole role;
                try {
                    role = ControlRole.valueOf(name);
                } catch (RuntimeException unknown) {
                    throw invalid(lineNumber, "unknown role: " + name);
                }
                if (!roles.add(role)) throw invalid(lineNumber, "duplicate role: " + name);
            }
            if (parsed.put(fingerprint, roles) != null) {
                throw invalid(lineNumber, "duplicate fingerprint");
            }
        }
        if (parsed.isEmpty()) throw new IllegalArgumentException("authz policy must not be empty");

        StringBuilder canonical = new StringBuilder();
        Map<String, EnumSet<ControlRole>> immutable = new TreeMap<>();
        for (Map.Entry<String, EnumSet<ControlRole>> entry : parsed.entrySet()) {
            EnumSet<ControlRole> roles = EnumSet.copyOf(entry.getValue());
            immutable.put(entry.getKey(), roles);
            canonical.append(entry.getKey()).append('=');
            boolean first = true;
            for (ControlRole role : roles) {
                if (!first) canonical.append(',');
                canonical.append(role.name());
                first = false;
            }
            canonical.append('\n');
        }
        return new ControlAuthzPolicy(Collections.unmodifiableMap(immutable),
                canonical.toString());
    }

    public EnumSet<ControlRole> roles(String fingerprint) {
        if (fingerprint == null) return EnumSet.noneOf(ControlRole.class);
        EnumSet<ControlRole> roles = entries.get(fingerprint);
        return roles == null ? EnumSet.noneOf(ControlRole.class) : EnumSet.copyOf(roles);
    }

    public String authzSetId() {
        return authzSetId;
    }

    private static void requireFingerprint(String value, int lineNumber) {
        if (value.length() != 64) throw invalid(lineNumber, "fingerprint must be 64 lowercase hex");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw invalid(lineNumber, "fingerprint must be 64 lowercase hex");
            }
        }
    }

    private static IllegalArgumentException invalid(int zeroBasedLine, String message) {
        return new IllegalArgumentException(
                "invalid control authz policy line " + (zeroBasedLine + 1) + ": " + message);
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder hex = new StringBuilder(64);
            for (byte value : digest) hex.append(String.format("%02x", value & 0xff));
            return hex.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
