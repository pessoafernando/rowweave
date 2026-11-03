package io.rowweave.soak;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Binds each qualification role to independently replayable production logs. */
final class ManagedMemorySoakRoleActivity {
    private static final Pattern SHUFFLE_LIFETIME = Pattern.compile(
            "^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3})"
                    + ".*ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME protocol=\\S+ "
                    + "connectNanos=[0-9]+ logicalStreams=([0-9]+) "
                    + "records=([0-9]+) wireBytes=([0-9]+) "
                    + "activeDataNanos=[0-9]+ lifetimeNanos=([0-9]+)$");

    private ManagedMemorySoakRoleActivity() { }

    static void write(List<ManagedMemorySoakCluster.RoleProcess> roles,
                      Path evidence, int concurrentJobs) throws IOException {
        if (roles == null || roles.isEmpty() || concurrentJobs <= 0) {
            throw new IllegalArgumentException("roles are required");
        }
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(evidence)) {
            for (ManagedMemorySoakCluster.RoleProcess role : roles) {
                log.append(parse(
                        role.role, role.log, concurrentJobs).fields());
            }
        }
    }

    static Activity parse(String role, Path log, int concurrentJobs)
            throws IOException {
        if (role == null || role.isEmpty() || log == null
                || !Files.isRegularFile(log) || concurrentJobs <= 0) {
            throw new IllegalArgumentException(
                    "role, regular role log and concurrency are required");
        }
        long connections = 0L;
        long logicalStreams = 0L;
        long records = 0L;
        long wireBytes = 0L;
        long concurrentMuxGroups = 0L;
        try (BufferedReader reader = Files.newBufferedReader(
                log, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher matcher = SHUFFLE_LIFETIME.matcher(line);
                if (!matcher.find()) continue;
                connections = Math.addExact(connections, 1L);
                long connectionStreams = Long.parseLong(matcher.group(2));
                logicalStreams = Math.addExact(
                        logicalStreams, connectionStreams);
                records = Math.addExact(records,
                        Long.parseLong(matcher.group(3)));
                wireBytes = Math.addExact(wireBytes,
                        Long.parseLong(matcher.group(4)));
                if (connectionStreams >= concurrentJobs) {
                    concurrentMuxGroups++;
                }
            }
        }
        return new Activity(role, StreamSoakEvidenceLog.fileSha256(log),
                connections, logicalStreams, records, wireBytes,
                concurrentMuxGroups);
    }

    static final class Activity {
        final String role;
        final String roleLogSha256;
        final long connections;
        final long logicalStreams;
        final long records;
        final long wireBytes;
        final long concurrentMuxGroups;

        private Activity(String role, String roleLogSha256,
                         long connections, long logicalStreams,
                         long records, long wireBytes,
                         long concurrentMuxGroups) {
            this.role = role;
            this.roleLogSha256 = roleLogSha256;
            this.connections = connections;
            this.logicalStreams = logicalStreams;
            this.records = records;
            this.wireBytes = wireBytes;
            this.concurrentMuxGroups = concurrentMuxGroups;
        }

        private Map<String, String> fields() {
            Map<String, String> result = new LinkedHashMap<>();
            result.put("kind", "role-activity");
            result.put("role", role);
            result.put("role_log_sha256", roleLogSha256);
            result.put("connections", Long.toString(connections));
            result.put("logical_streams", Long.toString(logicalStreams));
            result.put("records", Long.toString(records));
            result.put("wire_bytes", Long.toString(wireBytes));
            result.put("concurrent_mux_groups",
                    Long.toString(concurrentMuxGroups));
            return result;
        }
    }
}
