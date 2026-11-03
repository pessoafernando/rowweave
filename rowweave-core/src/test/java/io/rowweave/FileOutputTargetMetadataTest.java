package io.rowweave;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** An atomic move must not change the target identity or its visible permission semantics. */
class FileOutputTargetMetadataTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));

    @Test
    void newOutputUsesNormalProviderPermissionsInsteadOfSecureTempPermissions(@TempDir Path tmp) throws Exception {
        assumePosix(tmp);
        Path baseline = tmp.resolve("baseline.tsv");
        Files.newOutputStream(baseline).close();
        Set<PosixFilePermission> expected = Files.getPosixFilePermissions(baseline);
        Path target = tmp.resolve("result.tsv");

        FileSink output = new FileSink(META, target.toString());
        output.collect(row(1));
        output.close();

        assertEquals(expected, Files.getPosixFilePermissions(target),
                "the new result's permissions must match ordinary provider/umask creation semantics");
    }

    @Test
    void replacingExistingOutputPreservesItsPosixPermissionBits(@TempDir Path tmp) throws Exception {
        assumePosix(tmp);
        Path target = tmp.resolve("existing.tsv");
        Files.write(target, "old\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Set<PosixFilePermission> expected = PosixFilePermissions.fromString("rw-r-----");
        Files.setPosixFilePermissions(target, expected);

        FileSink output = new FileSink(META, target.toString());
        output.collect(row(2));
        output.close();

        assertEquals(expected, Files.getPosixFilePermissions(target));
        List<DataRow> rows = FileSink.read(META, target.toString());
        assertEquals(1, rows.size());
        assertEquals(2L, rows.get(0).get(0));
    }

    @Test
    void abortAndZeroRowReplacementBothPreserveExistingPermissions(@TempDir Path tmp) throws Exception {
        assumePosix(tmp);
        Path target = tmp.resolve("zero.tsv");
        byte[] previous = "old\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(target, previous);
        Set<PosixFilePermission> expected = PosixFilePermissions.fromString("rw-r-----");
        Files.setPosixFilePermissions(target, expected);

        FileSink failed = new FileSink(META, target.toString());
        failed.collect(row(4));
        failed.abort();
        assertEquals(expected, Files.getPosixFilePermissions(target));
        org.junit.jupiter.api.Assertions.assertArrayEquals(previous, Files.readAllBytes(target));

        new FileSink(META, target.toString()).close();
        assertEquals(expected, Files.getPosixFilePermissions(target));
        assertEquals(0L, Files.size(target));
    }

    @Test
    void publishingThroughSymlinkKeepsLinkAndUpdatesRealTarget(@TempDir Path tmp) throws Exception {
        Path realTarget = tmp.resolve("real.tsv");
        Files.write(realTarget, "old\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Path link = tmp.resolve("result-link.tsv");
        try {
            Files.createSymbolicLink(link, realTarget.getFileName());
        } catch (UnsupportedOperationException | IOException unavailable) {
            assumeTrue(false, "filesystem does not support test symlinks: " + unavailable);
        }

        FileSink output = new FileSink(META, link.toString());
        output.collect(row(3));
        output.close();

        assertTrue(Files.isSymbolicLink(link), "publishing must not replace a symlink directory entry with a regular file");
        List<DataRow> rows = FileSink.read(META, realTarget.toString());
        assertEquals(1, rows.size());
        assertEquals(3L, rows.get(0).get(0));
    }

    private static void assumePosix(Path directory) throws IOException {
        FileStore store = Files.getFileStore(directory);
        assumeTrue(store.supportsFileAttributeView(PosixFileAttributeView.class),
                "filesystem has no POSIX attribute view");
    }

    private static DataRow row(long id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }
}
