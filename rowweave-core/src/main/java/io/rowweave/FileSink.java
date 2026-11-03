package io.rowweave;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Timestamp;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * {@link Sink} that writes result rows to a file. Self-describing text format: one record per line,
 * fields separated by TAB, and every field value carrying what is needed to restore its type, so a
 * persist/read round trip is bit-for-bit identical:
 *
 * <ul>
 * <li>null → {@code \N} (distinct from the empty string).</li>
 * <li>BigDecimal → {@code toPlainString()} for a non-negative scale, {@code toString()} for a negative
 * scale, so both {@code 1.5000000000} and {@code 1E+30} round-trip exactly.</li>
 * <li>Timestamp → {@code millis:nanos} (nanos preserved); Date → epoch millis.</li>
 * <li>String → escapes {@code \ \t \n} to keep rows and columns safe; the empty string is written as is.</li>
 * <li>Boolean/Integer/Long → text literals.</li>
 * </ul>
 *
 * <p>Production results are read back through {@link FileSource#openReader()} and processed row by row,
 * rebuilding every cell exactly from the DataSchema's DataType; {@link #read(DataSchema, String)} remains
 * only as a compatibility read for small results. NullType (an untyped computed column) is outside the
 * file's persisted scope, since an output sink's meta always has concrete types.
 */
public final class FileSink implements Sink {

    private static final long serialVersionUID = 1L;

    private static final String NULL_TOKEN = "\\N";
    private static final int ATTEMPT_NAME_RETRIES = 8;

    private enum AttemptState {
        NEW,
        OPEN,
        PREPARED,
        PUBLISHED,
        FINALIZED,
        ABORTED
    }

    private final DataSchema dataSchema;
    private final String path; // holds the path (serializable, sent to a Worker), not a BufferedWriter
    // Serialized with the Sink description, so client and Master use one identity for a duplicated destination.
    private final String exclusiveDestinationId;
    // Attempt execution state never takes part in serialization; a deserialized Worker copy creates its own staging file in the same directory, starting from NEW.
    private transient BufferedWriter writer;
    private transient Path attemptPath;
    private transient Path attemptTargetPath;
    private transient Path backupPath;
    private transient boolean publishedOverPreviousTarget;
    private transient AttemptState attemptState;

    public FileSink(DataSchema dataSchema, String path) {
        this.dataSchema = dataSchema;
        this.path = path;
        this.exclusiveDestinationId = fileDestinationId(path);
    }

    /** The target path of this FileSink (used to derive shards). */
    public String path() {
        return path;
    }

    /**
     * Derives the FileSink for shard {@code shard} by appending a {@code .shard} suffix to
     * the path (the Spark/Flink convention: each reducer writes one shard, and the union is the global result). Shards are written independently and never interfere.
     */
    public FileSink withShardSuffix(int shard) {
        return new FileSink(dataSchema, path + "." + shard);
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    /** FileSink holds a path and each Worker shard writes independently, so it is worker-writable. */
    @Override
    public boolean isWorkerWritable() {
        return true;
    }

    @Override
    public String exclusiveDestinationId() {
        // Java-serialization compatibility: a newly added final field is absent and therefore null, so it is rebuilt from the original path.
        return exclusiveDestinationId != null ? exclusiveDestinationId : fileDestinationId(path);
    }

    /**
     * Pure descriptive identity: an absolute lexical normalize is the floor, and when the target or an ancestor is currently visible, a best-effort realpath removes symlink aliases.
     * A resolution failure must not stop a shared filesystem path from being submitted to another Worker, so the code falls back to lexical identity.
     */
    private static String fileDestinationId(String rawPath) {
        Path absolute = Paths.get(rawPath).toAbsolutePath().normalize();
        Path canonical = absolute;
        try {
            Path existing = absolute;
            while (existing != null && !Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
                existing = existing.getParent();
            }
            if (existing != null) {
                Path suffix = existing.relativize(absolute);
                canonical = existing.toRealPath().resolve(suffix).normalize();
            }
        } catch (IOException | SecurityException ignored) {
            // The Client and Master may not mount the Worker filesystem; lexical identity still recognizes paths that differ only by . or .. as the same.
        }
        return "file:" + canonical;
    }

    /** Lazily opens the private writer for this attempt; the final path changes only in the atomic move of a successful close. */
    private BufferedWriter writer() {
        AttemptState state = state();
        if (state != AttemptState.NEW && state != AttemptState.OPEN) {
            throw new IllegalStateException("FileSink attempt is already " + state.name().toLowerCase());
        }
        if (writer == null) {
            openAttemptWriter();
        }
        return writer;
    }

    private AttemptState state() {
        return attemptState == null ? AttemptState.NEW : attemptState;
    }

    private void openAttemptWriter() {
        try {
            Path target = resolveTargetPath();
            Path parent = target.getParent();
            if (parent == null) {
                throw new IllegalArgumentException("FileSink target has no parent: " + path);
            }
            Set<PosixFilePermission> inheritedPermissions = existingPosixPermissions(target);
            OpenedAttempt opened = createAttempt(parent, inheritedPermissions);
            try {
                writer = new BufferedWriter(new OutputStreamWriter(opened.stream, StandardCharsets.UTF_8));
            } catch (RuntimeException | Error writerInitFailure) {
                cleanupFailedOpen(opened.path, opened.stream, writerInitFailure);
                throw writerInitFailure;
            }
            attemptPath = opened.path;
            attemptTargetPath = target;
            attemptState = AttemptState.OPEN;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open FileSink attempt for: " + path, e);
        }
    }

    /** Pins the real target when the attempt starts; an existing symlink is followed to its target, so an atomic move never replaces the link's directory entry. */
    private Path resolveTargetPath() throws IOException {
        Path target = Paths.get(path).toAbsolutePath().normalize();
        return Files.isSymbolicLink(target) ? target.toRealPath() : target;
    }

    /** Freezes the mode bits of a POSIX target; non-POSIX or a new target returns null and takes the provider's ordinary create-default permissions. */
    private static Set<PosixFilePermission> existingPosixPermissions(Path target) throws IOException {
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        PosixFileAttributeView view = Files.getFileAttributeView(
                target, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (view == null) {
            return null;
        }
        Set<PosixFilePermission> copy = EnumSet.noneOf(PosixFilePermission.class);
        copy.addAll(view.readAttributes().permissions());
        return copy;
    }

    /** Claims staging atomically with a UUID plus CREATE_NEW; an existing POSIX mode takes effect at creation, so permissions are never widened and then narrowed. */
    private static OpenedAttempt createAttempt(
            Path parent, Set<PosixFilePermission> inheritedPermissions) throws IOException {
        for (int i = 0; i < ATTEMPT_NAME_RETRIES; i++) {
            Path candidate = parent.resolve(".rowweave-output-" + UUID.randomUUID() + ".tmp");
            try {
                if (inheritedPermissions == null) {
                    OutputStream stream = Files.newOutputStream(candidate,
                            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                    return new OpenedAttempt(candidate, stream);
                }
                SeekableByteChannel channel = Files.newByteChannel(candidate,
                        EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                        PosixFilePermissions.asFileAttribute(inheritedPermissions));
                try {
                    // The provider may tighten an explicit mode further by umask; the exact old mode is restored after open, so there is no window wider than the old target had.
                    Files.setPosixFilePermissions(candidate, inheritedPermissions);
                    return new OpenedAttempt(candidate, Channels.newOutputStream(channel));
                } catch (IOException | RuntimeException | Error metadataFailure) {
                    cleanupFailedOpen(candidate, channel, metadataFailure);
                    throw metadataFailure;
                }
            } catch (FileAlreadyExistsException collision) {
                // A UUID collision or an external file with the same name; never delete a file that is not this attempt's, and retry a bounded number of times under a new name.
            }
        }
        throw new IOException("failed to allocate unique FileSink attempt in: " + parent);
    }

    private static void cleanupFailedOpen(
            Path candidate, AutoCloseable resource, Throwable primary) {
        try {
            resource.close();
        } catch (Throwable closeFailure) {
            primary.addSuppressed(closeFailure);
        }
        try {
            Files.deleteIfExists(candidate);
        } catch (Throwable cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
    }

    private static final class OpenedAttempt {
        private final Path path;
        private final OutputStream stream;

        private OpenedAttempt(Path path, OutputStream stream) {
            this.path = path;
            this.stream = stream;
        }
    }

    @Override
    public void collect(DataRow row) {
        String encoded = encodeRowText(dataSchema, row);
        try {
            writer().write(encoded);
            writer().write('\n');
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write row", e);
        }
    }

    /** Canonical ROWWEAVE_ROW_TEXT_V1 row body, shared with strict Object decoding. */
    static String encodeRowText(DataSchema dataSchema, DataRow row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.size(); i++) {
            if (i > 0) {
                sb.append('\t');
            }
            sb.append(encodeCell(dataSchema.getField(i).getDataType(), row.get(i)));
        }
        return sb.toString();
    }

    @Override
    public void close() {
        AttemptState state = state();
        if (state == AttemptState.FINALIZED || state == AttemptState.ABORTED) {
            return;
        }

        try {
            prepareCommit();
            commitPrepared();
            finalizeCommit();
        } catch (Throwable failure) {
            try {
                rollbackCommit();
            } catch (Throwable rollbackFailure) {
                failure.addSuppressed(rollbackFailure);
            }
            throwUnchecked(failure);
        }
    }

    @Override
    public boolean supportsCoordinatedCommit() {
        return true;
    }

    @Override
    public void prepareCommit() {
        AttemptState state = state();
        if (state == AttemptState.PREPARED || state == AttemptState.PUBLISHED
                || state == AttemptState.FINALIZED) {
            return;
        }
        if (state == AttemptState.ABORTED) {
            throw new IllegalStateException("FileSink attempt is already aborted");
        }

        // Zero rows is still a success: an empty attempt is established and published through exactly the same atomic path as a non-empty result.
        BufferedWriter current = writer();
        Throwable closeFailure = null;
        try {
            current.close();
        } catch (IOException e) {
            closeFailure = new UncheckedIOException("failed to close FileSink attempt: " + path, e);
        } catch (RuntimeException | Error e) {
            closeFailure = e;
        } finally {
            writer = null;
        }

        if (closeFailure != null) {
            Throwable cleanupFailure = cleanupAttempt();
            attemptState = AttemptState.ABORTED;
            if (cleanupFailure != null) {
                closeFailure.addSuppressed(cleanupFailure);
            }
            throwUnchecked(closeFailure);
            return;
        }

        attemptState = AttemptState.PREPARED;
    }

    @Override
    public void commitPrepared() {
        AttemptState state = state();
        if (state == AttemptState.PUBLISHED || state == AttemptState.FINALIZED) {
            return;
        }
        if (state != AttemptState.PREPARED) {
            throw new IllegalStateException("FileSink is not prepared: " + state.name().toLowerCase());
        }

        Path publishing = attemptPath;
        Path target = attemptTargetPath;
        Path backup = null;
        boolean hadPrevious = Files.exists(target, LinkOption.NOFOLLOW_LINKS);
        try {
            if (hadPrevious) {
                backup = moveTargetToUniqueBackup(target);
                backupPath = backup;
            }
            Files.move(publishing, target,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            attemptPath = null;
            publishedOverPreviousTarget = hadPrevious;
            attemptState = AttemptState.PUBLISHED;
        } catch (IOException e) {
            UncheckedIOException publishFailure = new UncheckedIOException(
                    "failed to publish FileSink atomically: " + path, e);
            compensateFailedPublish(target, backup, publishFailure);
            throw publishFailure;
        } catch (RuntimeException | Error publishFailure) {
            compensateFailedPublish(target, backup, publishFailure);
            throw publishFailure;
        }
    }

    private static Path moveTargetToUniqueBackup(Path target) throws IOException {
        Path parent = target.getParent();
        for (int i = 0; i < ATTEMPT_NAME_RETRIES; i++) {
            Path candidate = parent.resolve(".rowweave-output-" + UUID.randomUUID() + ".bak");
            try {
                Files.move(target, candidate, StandardCopyOption.ATOMIC_MOVE);
                return candidate;
            } catch (FileAlreadyExistsException collision) {
                // A unique-name collision retries a bounded number of times under a new name; never overwrite a file belonging to another instance.
            }
        }
        throw new IOException("failed to allocate unique FileSink backup in: " + parent);
    }

    private void compensateFailedPublish(Path target, Path backup, Throwable primary) {
        if (backup != null) {
            try {
                Files.move(backup, target,
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                backupPath = null;
            } catch (Throwable restoreFailure) {
                primary.addSuppressed(restoreFailure);
            }
        }
        Throwable cleanupFailure = cleanupAttempt();
        attemptTargetPath = null;
        publishedOverPreviousTarget = false;
        attemptState = AttemptState.ABORTED;
        if (cleanupFailure != null) {
            primary.addSuppressed(cleanupFailure);
        }
    }

    @Override
    public void rollbackCommit() {
        AttemptState state = state();
        if (state == AttemptState.ABORTED || state == AttemptState.FINALIZED) {
            return;
        }
        Throwable closeFailure = null;
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                closeFailure = new UncheckedIOException("failed to close FileSink on abort: " + path, e);
            } catch (RuntimeException | Error e) {
                closeFailure = e;
            } finally {
                writer = null;
            }
        }
        Throwable restoreFailure = null;
        if (state == AttemptState.PUBLISHED) {
            Path target = attemptTargetPath;
            try {
                if (publishedOverPreviousTarget) {
                    Files.move(backupPath, target,
                            StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    Files.deleteIfExists(target);
                }
                backupPath = null;
            } catch (Throwable failure) {
                restoreFailure = failure;
            }
        }
        Throwable cleanupFailure = cleanupAttempt();
        // A published version may reach ABORTED only when recovery succeeds; a failed recovery must keep it PUBLISHED so the layer above can retry.
        if (restoreFailure == null) {
            backupPath = null;
            attemptTargetPath = null;
            publishedOverPreviousTarget = false;
            attemptState = AttemptState.ABORTED;
        }
        // A failed attempt deletion means a dirty file remains, and it takes precedence over a writer close failure.
        Throwable failure = cleanupFailure != null ? cleanupFailure : restoreFailure;
        if (failure == null) {
            failure = closeFailure;
        } else if (closeFailure != null) {
            failure.addSuppressed(closeFailure);
        }
        if (cleanupFailure != null && restoreFailure != null) {
            failure.addSuppressed(restoreFailure);
        }
        if (failure != null) {
            throwUnchecked(failure);
        }
    }

    @Override
    public void finalizeCommit() {
        AttemptState state = state();
        if (state == AttemptState.FINALIZED) {
            return;
        }
        if (state != AttemptState.PUBLISHED) {
            throw new IllegalStateException("FileSink is not published: " + state.name().toLowerCase());
        }
        Path backup = backupPath;
        if (backup != null) {
            try {
                Files.deleteIfExists(backup);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to finalize FileSink backup: " + backup, e);
            }
        }
        backupPath = null;
        attemptTargetPath = null;
        publishedOverPreviousTarget = false;
        attemptState = AttemptState.FINALIZED;
    }

    /**
     * Discard on failure: close the writer and delete this attempt's private staging. Never delete an earlier successful version sitting at the final path.
     * Cleanup is still attempted when close fails; when cleanup fails, the primary exception reports that a half-finished attempt file remains, and the close failure is attached as suppressed.
     */
    @Override
    public void abort() {
        rollbackCommit();
    }

    /** Deletes this attempt and only this attempt; fields are cleared first so that a repeated call in a terminal state never operates on an unknown path. */
    private Throwable cleanupAttempt() {
        Path dirtyAttempt = attemptPath;
        attemptPath = null;
        if (dirtyAttempt == null) {
            return null;
        }
        try {
            Files.deleteIfExists(dirtyAttempt);
            return null;
        } catch (IOException e) {
            return new UncheckedIOException(
                    "failed to delete FileSink attempt: " + dirtyAttempt, e);
        } catch (RuntimeException | Error e) {
            return e;
        }
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    /**
     * Reads a small result back in full according to the DataSchema (symmetric with {@link #collect}, restored bit by bit).
 *
     * @deprecated This materializes the entire result into a List, so memory is O(N); it is kept for compatibility only. For a large
     * production result, use {@link FileSource#openReader()} to process rows one by one, or hand the FileSource directly to an ExecutionEnvironment.
     */
    @Deprecated
    public static List<DataRow> read(DataSchema dataSchema, String path) {
        return read(dataSchema, path, CompatibilityDataLimits.defaults());
    }

    /**
     * Materializes a small result under an explicit hard bound; exceeding it throws and never returns a partial result.
 *
     * @deprecated For a large production result, use {@link FileSource#openReader()} to process rows one by one.
     */
    @Deprecated
    public static List<DataRow> read(
            DataSchema dataSchema, String path, CompatibilityDataLimits limits) {
        CollectingSink rows = new CollectingSink(dataSchema, limits);
        try (FileSource.Reader reader = new FileSource(dataSchema, path).openReader()) {
            while (reader.hasNext()) {
                try {
                    rows.collect(reader.next());
                } catch (CompatibilityDataLimitException limit) {
                    throw new CompatibilityDataLimitException(
                            limit.kind(), limit.limit(), limit.observed(),
                            "FileSink.read");
                }
            }
        }
        rows.close();
        return rows.getRows();
    }

    /** The single one-line decoding rule shared by FileSink and FileSource. */
    static DataRow decodeRow(DataSchema dataSchema, String line) {
        int fieldCount = dataSchema.getFieldCount();
        String[] cells = splitTab(line, fieldCount);
        DataRow row = new DataRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            row.set(i, decodeCell(dataSchema.getField(i).getDataType(), cells[i]));
        }
        return row;
    }

    private static String encodeCell(DataType type, Object v) {
        if (v == null) {
            return NULL_TOKEN;
        }
        switch (type) {
            case StringType:
                return escape((String) v);
            case BigDecimalType:
                BigDecimal decimal = (BigDecimal) v;
                return decimal.scale() < 0 ? decimal.toString() : decimal.toPlainString();
            case BooleanType:
                return ((Boolean) v) ? "true" : "false";
            case IntegerType:
                return Integer.toString((Integer) v);
            case LongType:
                return Long.toString((Long) v);
            case TimestampType: {
                Timestamp ts = (Timestamp) v;
                return ts.getTime() + ":" + ts.getNanos();
            }
            case DateType:
                return Long.toString(((Date) v).getTime());
            default:
                throw new IllegalArgumentException("Unsupported type for FileSink: " + type);
        }
    }

    private static Object decodeCell(DataType type, String token) {
        if (NULL_TOKEN.equals(token)) {
            return null;
        }
        switch (type) {
            case StringType:
                return unescape(token);
            case BigDecimalType:
                return new BigDecimal(token);
            case BooleanType:
                return Boolean.parseBoolean(token);
            case IntegerType:
                return Integer.parseInt(token);
            case LongType:
                return Long.parseLong(token);
            case TimestampType: {
                int sep = token.indexOf(':');
                long millis = Long.parseLong(token.substring(0, sep));
                int nanos = Integer.parseInt(token.substring(sep + 1));
                Timestamp ts = new Timestamp(millis);
                ts.setNanos(nanos);
                return ts;
            }
            case DateType:
                return new Date(Long.parseLong(token));
            default:
                throw new IllegalArgumentException("Unsupported type for FileSink: " + type);
        }
    }

    private static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '\t': sb.append("\\t"); break;
                case '\n': sb.append("\\n"); break;
                default: sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String unescape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                switch (n) {
                    case '\\': sb.append('\\'); break;
                    case 't': sb.append('\t'); break;
                    case 'n': sb.append('\n'); break;
                    default: sb.append(n);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /** Splits on TAB, keeps empty-string fields (the limit=-1 behaviour) and validates the column count. */
    private static String[] splitTab(String line, int expected) {
        String[] cells = line.split("\t", -1);
        if (cells.length != expected) {
            throw new IllegalStateException("expected " + expected + " columns, got " + cells.length
                    + " in line: " + line);
        }
        return cells;
    }
}
