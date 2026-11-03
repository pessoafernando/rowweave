package io.rowweave.soak;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamSoakOracleTest {
    @Test void reconstructsCommittedTransactionOrder(@TempDir Path root)
            throws Exception {
        Path later = root.resolve("visible-b");
        Path earlier = root.resolve("visible-a");
        Files.write(later, "D|2\n".getBytes(StandardCharsets.UTF_8));
        Files.write(earlier, "A|1|1.00\nA|2|2.00\n"
                .getBytes(StandardCharsets.UTF_8));
        Files.setLastModifiedTime(earlier, FileTime.fromMillis(1_000L));
        Files.setLastModifiedTime(later, FileTime.fromMillis(2_000L));
        assertEquals(Arrays.asList("A|1|1.00", "A|2|2.00", "D|2"),
                StreamSoakOracle.visibleRows(root));
        assertEquals(64, StreamSoakOracle.digest(
                StreamSoakOracle.visibleRows(root)).length());
    }

    @Test void comparesOracleAsOrderedStream(@TempDir Path root)
            throws Exception {
        Path expected = root.resolve("expected.log");
        Path actual = root.resolve("actual");
        Files.createDirectories(actual);
        Files.write(expected, Arrays.asList("A", "B", "C"),
                StandardCharsets.UTF_8);
        Files.write(actual.resolve("visible-1"), Arrays.asList("A", "B"),
                StandardCharsets.UTF_8);

        StreamSoakOracle.Comparison prefix = StreamSoakOracle.compare(
                expected, actual);
        assertEquals(StreamSoakOracle.Status.ACTUAL_PREFIX, prefix.status);
        assertEquals(3L, prefix.expectedRows);
        assertEquals(2L, prefix.actualRows);

        Files.setLastModifiedTime(actual.resolve("visible-1"),
                FileTime.fromMillis(1_000L));
        Files.write(actual.resolve("visible-2"),
                Collections.singletonList("C"), StandardCharsets.UTF_8);
        Files.setLastModifiedTime(actual.resolve("visible-2"),
                FileTime.fromMillis(2_000L));
        StreamSoakOracle.Comparison match = StreamSoakOracle.compare(
                expected, actual);
        assertEquals(StreamSoakOracle.Status.MATCH, match.status);
        assertEquals(3L, match.actualRows);
        assertEquals(match.expectedDigest, match.actualDigest);

        Files.write(actual.resolve("visible-2"),
                Collections.singletonList("X"), StandardCharsets.UTF_8);
        assertEquals(StreamSoakOracle.Status.DIVERGED,
                StreamSoakOracle.compare(expected, actual).status);
    }
}
