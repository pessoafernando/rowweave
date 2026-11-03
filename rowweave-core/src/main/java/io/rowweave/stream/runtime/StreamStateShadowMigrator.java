package io.rowweave.stream.runtime;

import io.rowweave.deployment.SignedStreamStateMigrationResolver;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamStateDescriptor;
import io.rowweave.stream.contract.StreamStateMigrationBinding;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Runs one signed state mapping twice before publishing it to shadow chunks. */
public final class StreamStateShadowMigrator {
    private final Path workingDirectory;
    private final FileCheckpointChunkStore sourceStore;
    private final FileCheckpointChunkStore shadowStore;
    private final SignedStreamStateMigrationResolver resolver;
    private final ProviderActionSupervisor supervisor;
    private final long actionTimeoutNanos;

    public StreamStateShadowMigrator(
            Path workingDirectory, FileCheckpointChunkStore sourceStore,
            FileCheckpointChunkStore shadowStore,
            SignedStreamStateMigrationResolver resolver,
            ProviderActionSupervisor supervisor, long actionTimeoutMillis) {
        if (workingDirectory == null || sourceStore == null
                || shadowStore == null || resolver == null
                || supervisor == null || actionTimeoutMillis < 1L
                || actionTimeoutMillis > TimeUnit.MINUTES.toMillis(10L)) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "valid state migration stores and bounds are required");
        }
        this.workingDirectory = workingDirectory.toAbsolutePath().normalize();
        this.sourceStore = sourceStore;
        this.shadowStore = shadowStore;
        this.resolver = resolver;
        this.supervisor = supervisor;
        this.actionTimeoutNanos =
                TimeUnit.MILLISECONDS.toNanos(actionTimeoutMillis);
        ensureWorkingDirectory(this.workingDirectory);
    }

    public StreamCheckpointMember migrate(
            StreamStateMigrationBinding binding,
            StreamCheckpointMember sourceMember,
            FileCheckpointChunkStore.StoredChunkedPayload sourcePayload) {
        validateSource(binding, sourceMember, sourcePayload);
        String token = UUID.randomUUID().toString();
        Path restoredSource = workingDirectory.resolve(
                ".rowweave-stream-migration-source-" + token + ".tmp");
        Path firstTarget = workingDirectory.resolve(
                ".rowweave-stream-migration-target-a-" + token + ".tmp");
        Path secondTarget = workingDirectory.resolve(
                ".rowweave-stream-migration-target-b-" + token + ".tmp");
        Throwable primary = null;
        try {
            sourceStore.restore(sourcePayload, restoredSource);
            StreamStateMigrationAdapter adapter = supervisor.call(
                    "stream-state-migration-resolve", deadline(),
                    () -> resolver.resolve(binding));
            runAdapter(adapter, restoredSource, firstTarget,
                    binding.maximumOutputBytes(), "a");
            runAdapter(adapter, restoredSource, secondTarget,
                    binding.maximumOutputBytes(), "b");
            long firstBytes = Files.size(firstTarget);
            long secondBytes = Files.size(secondTarget);
            String firstDigest = digest(firstTarget);
            String secondDigest = digest(secondTarget);
            if (firstBytes != secondBytes
                    || !firstDigest.equals(secondDigest)) {
                throw failure(StreamCheckpointException.Code
                                .STATE_MIGRATION_NON_DETERMINISTIC,
                        "state migration output is not deterministic");
            }
            if (!binding.targetContentDigest().equals(firstDigest)) {
                throw failure(StreamCheckpointException.Code
                                .STATE_MIGRATION_OUTPUT_MISMATCH,
                        "state migration target digest does not match");
            }
            FileCheckpointChunkStore.StoredChunkedPayload target =
                    shadowStore.store(firstTarget);
            if (!firstDigest.equals(target.digest())
                    || firstBytes != target.contentBytes()) {
                throw failure(StreamCheckpointException.Code
                                .STATE_MIGRATION_OUTPUT_MISMATCH,
                        "shadow state publication drifted");
            }
            StreamStateDescriptor targetState = binding.targetState();
            return StreamCheckpointMember.stateHandle(
                    binding.operatorUid(), sourceMember.subtaskIndex(),
                    sourceMember.keyGroupStart(), sourceMember.keyGroupEnd(),
                    targetState.stateName(),
                    targetState.serializerContractId(),
                    targetState.stateSchemaVersion(), target.digest(),
                    target.contentBytes());
        } catch (StreamCheckpointException migrationFailure) {
            primary = migrationFailure;
            throw migrationFailure;
        } catch (IOException ioFailure) {
            StreamCheckpointException wrapped = failure(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "state migration staging failed", ioFailure);
            primary = wrapped;
            throw wrapped;
        } catch (RuntimeException | Error migrationFailure) {
            primary = migrationFailure;
            throw migrationFailure;
        } finally {
            cleanup(primary, restoredSource, firstTarget, secondTarget);
        }
    }

    private void runAdapter(
            StreamStateMigrationAdapter adapter, Path source, Path target,
            long maximumOutputBytes, String run) {
        try {
            supervisor.call("stream-state-migration-run-" + run, deadline(),
                    () -> {
                        try (InputStream input = Files.newInputStream(source);
                             OutputStream output = new BoundedOutputStream(
                                     Files.newOutputStream(target,
                                             StandardOpenOption.CREATE_NEW,
                                             StandardOpenOption.WRITE),
                                     maximumOutputBytes)) {
                            adapter.migrate(input, output);
                        }
                        return null;
                    });
        } catch (RuntimeException adapterFailure) {
            if (hasCause(adapterFailure, OutputLimitException.class)) {
                throw failure(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                        "state migration output exceeds its bound",
                        adapterFailure);
            }
            throw failure(StreamCheckpointException.Code.STATE_MIGRATION_FAILED,
                    "state migration adapter failed", adapterFailure);
        }
    }

    private static void validateSource(
            StreamStateMigrationBinding binding,
            StreamCheckpointMember sourceMember,
            FileCheckpointChunkStore.StoredChunkedPayload sourcePayload) {
        if (binding == null || sourceMember == null || sourcePayload == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "state migration binding and source are required");
        }
        StreamStateDescriptor sourceState = binding.sourceState();
        if (sourceMember.kind() != StreamCheckpointMember.Kind.STATE
                || !binding.operatorUid().equals(sourceMember.operatorUid())
                || !sourceState.stateName().equals(sourceMember.subjectId())
                || !sourceState.serializerContractId().equals(
                        sourceMember.codecId())
                || sourceState.stateSchemaVersion()
                != sourceMember.schemaVersion()
                || !sourceMember.contentDigest().equals(sourcePayload.digest())
                || sourceMember.contentBytes() != sourcePayload.contentBytes()) {
            throw failure(StreamCheckpointException.Code
                            .STATE_MIGRATION_INCOMPATIBLE,
                    "source state does not match the migration binding");
        }
    }

    private long deadline() {
        long now = System.nanoTime();
        return now > Long.MAX_VALUE - actionTimeoutNanos
                ? Long.MAX_VALUE : now + actionTimeoutNanos;
    }

    private static String digest(Path path) throws IOException {
        MessageDigest digest = sha256();
        byte[] buffer = new byte[64 * 1024];
        try (InputStream input = Files.newInputStream(path)) {
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read > 0) digest.update(buffer, 0, read);
            }
        }
        return hex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String hex(byte[] value) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            result[index * 2] = digits[(value[index] >>> 4) & 0xf];
            result[index * 2 + 1] = digits[value[index] & 0xf];
        }
        return new String(result);
    }

    private static void ensureWorkingDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(StreamCheckpointException.Code.IO_FAILURE,
                        "state migration working path is not a directory");
            }
        } catch (IOException ioFailure) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "state migration working directory failed", ioFailure);
        }
    }

    private static void cleanup(Throwable primary, Path... paths) {
        IOException firstCleanupFailure = null;
        for (Path path : paths) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException cleanupFailure) {
                if (primary != null) {
                    primary.addSuppressed(cleanupFailure);
                } else if (firstCleanupFailure == null) {
                    firstCleanupFailure = cleanupFailure;
                } else {
                    firstCleanupFailure.addSuppressed(cleanupFailure);
                }
            }
        }
        if (firstCleanupFailure != null) {
            throw failure(StreamCheckpointException.Code.IO_FAILURE,
                    "state migration staging cleanup failed",
                    firstCleanupFailure);
        }
    }

    private static boolean hasCause(
            Throwable failure, Class<? extends Throwable> type) {
        for (Throwable current = failure; current != null;
             current = current.getCause()) {
            if (type.isInstance(current)) return true;
        }
        return false;
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message,
            Throwable cause) {
        return new StreamCheckpointException(code, message, cause);
    }

    private static final class BoundedOutputStream
            extends FilterOutputStream {
        private final long maximumBytes;
        private long bytes;

        private BoundedOutputStream(OutputStream output, long maximumBytes) {
            super(output);
            this.maximumBytes = maximumBytes;
        }

        @Override
        public void write(int value) throws IOException {
            requireCapacity(1);
            out.write(value);
            bytes++;
        }

        @Override
        public void write(byte[] value, int offset, int length)
                throws IOException {
            if (value == null || offset < 0 || length < 0
                    || offset > value.length - length) {
                throw new IndexOutOfBoundsException();
            }
            requireCapacity(length);
            out.write(value, offset, length);
            bytes += length;
        }

        private void requireCapacity(int length) throws OutputLimitException {
            if (length > maximumBytes - bytes) {
                throw new OutputLimitException();
            }
        }
    }

    private static final class OutputLimitException extends IOException {
        private static final long serialVersionUID = 1L;
    }
}
