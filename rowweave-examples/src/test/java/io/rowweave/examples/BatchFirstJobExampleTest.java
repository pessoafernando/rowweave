package io.rowweave.examples;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchFirstJobExampleTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void filtersGroupsOrdersAndAppliesOneDecimalOutputScale() throws Exception {
        Path output = temporaryDirectory.resolve("first-job.tsv");

        BatchFirstJobExample.run(BatchFirstJobExample.sampleRows(), output.toString());

        List<String> lines = Files.readAllLines(output);
        assertEquals(3, lines.size());
        assertEquals("alpha\t13.1234567891", lines.get(0));
        assertEquals("beta\t4.0000000000", lines.get(1));
        assertEquals("gamma\t3.1234567891", lines.get(2));
        for (String line : lines) {
            String[] fields = line.split("\t");
            assertEquals(2, fields.length);
            assertTrue(new BigDecimal(fields[1]).scale() == 10);
        }
    }
}
