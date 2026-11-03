package io.rowweave.soak;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ManagedMemorySoakRoleActivityTest {
    @Test
    void parsesAndAggregatesProductionShuffleLifetimeRecords()
            throws Exception {
        Path log = Files.createTempFile("managed-memory-worker-", ".log");
        String first = "2026-10-07 23:29:33.700 INFO "
                + "io.rowweave.sched.net.MuxConnectionPool - ";
        Files.write(log, (first
                + "ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME protocol=LOOPBACK_PLAIN_V0 "
                + "connectNanos=11 logicalStreams=4 records=74934 "
                + "wireBytes=3596000 activeDataNanos=17 lifetimeNanos=100000000\n")
                .getBytes(StandardCharsets.UTF_8));

        ManagedMemorySoakRoleActivity.Activity activity =
                ManagedMemorySoakRoleActivity.parse("worker-0", log, 2);

        assertEquals("worker-0", activity.role);
        assertEquals(1L, activity.connections);
        assertEquals(4L, activity.logicalStreams);
        assertEquals(74934L, activity.records);
        assertEquals(3596000L, activity.wireBytes);
        assertEquals(1L, activity.concurrentMuxGroups);
        assertEquals(StreamSoakEvidenceLog.fileSha256(log),
                activity.roleLogSha256);
    }

    @Test
    void detectsSequentialShuffleLifetimes() throws Exception {
        Path log = Files.createTempFile("managed-memory-sequential-", ".log");
        String message = " INFO pool - ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME "
                + "protocol=LOOPBACK_PLAIN_V0 connectNanos=1 "
                + "logicalStreams=1 records=1 wireBytes=1 "
                + "activeDataNanos=1 lifetimeNanos=1000000\n";
        Files.write(log, ("2026-10-07 23:29:33.700" + message
                + "2026-10-07 23:29:33.750" + message)
                .getBytes(StandardCharsets.UTF_8));

        ManagedMemorySoakRoleActivity.Activity activity =
                ManagedMemorySoakRoleActivity.parse("worker-0", log, 2);

        assertEquals(2L, activity.connections);
        assertEquals(0L, activity.concurrentMuxGroups);
    }
}
