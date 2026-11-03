package io.rowweave;

import io.rowweave.internal.RowSnapshot;
import io.rowweave.mem.RowSizeEstimator;
import io.rowweave.mem.RowSerializer;
import io.rowweave.commit.SchemaFingerprint;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A Source whose data is an in-memory collection (purely on the heap).
 * The row snapshot is frozen at construction, and every iterator() replays independent copies from the start.
 */
public final class CollectionSource implements BatchSource {
    private static final long serialVersionUID = 1L;
    private static final String LIMIT_OWNER = "CollectionSource";

    private final DataSchema dataSchema;
    private final DataRow[] rows;
    private final CompatibilityDataLimits limits;
    /** null means the compatibility constructor remains LEGACY_SINGLE_ATTEMPT. */
    private final BatchSourceSplitDescriptor batchSourceSplit;

    public CollectionSource(DataSchema dataSchema, Collection<DataRow> rows) {
        this(dataSchema, rows, CompatibilityDataLimits.defaults(), null);
    }

    /** Build an exact replayable bounded embedded snapshot with an application-stable split id. */
    public CollectionSource(DataSchema dataSchema, String stableSplitId, Collection<DataRow> rows) {
        this(dataSchema, rows, CompatibilityDataLimits.defaults(),
                requireStableSplitId(stableSplitId));
    }

    /**
     * Builds a replayable, bounded in-memory snapshot. An explicit limit may never exceed
     * the hard, non-disableable ceiling defined by {@link CompatibilityDataLimits}.
     */
    public CollectionSource(
            DataSchema dataSchema, Collection<DataRow> rows,
            CompatibilityDataLimits limits) {
        this(dataSchema, rows, limits, null);
    }

    /** Exact embedded snapshot overload with explicit compatibility limits. */
    public CollectionSource(
            DataSchema dataSchema, String stableSplitId, Collection<DataRow> rows,
            CompatibilityDataLimits limits) {
        this(dataSchema, rows, limits, requireStableSplitId(stableSplitId));
    }

    private CollectionSource(
            DataSchema dataSchema, Collection<DataRow> rows,
            CompatibilityDataLimits limits, String stableSplitId) {
        this.dataSchema = Objects.requireNonNull(dataSchema, "dataSchema");
        Objects.requireNonNull(rows, "rows");
        this.limits = Objects.requireNonNull(limits, "limits");

        int declaredRows = rows.size();
        if (declaredRows < 0) {
            throw new IllegalArgumentException("CollectionSource source size is negative");
        }
        if (declaredRows > limits.maxRows()) {
            throw new CompatibilityDataLimitException(
                    CompatibilityDataLimitException.Kind.ROWS,
                    limits.maxRows(), declaredRows, LIMIT_OWNER);
        }

        long residentBytes = RowSizeEstimator.estimateReferenceArray(declaredRows);
        requireResidentBytes(residentBytes);
        DataRow[] snapshots = new DataRow[declaredRows];
        Iterator<DataRow> source = rows.iterator();
        int index = 0;
        while (source.hasNext()) {
            if (index == declaredRows) {
                throw new IllegalStateException(
                        "CollectionSource source size changed during snapshot: declared="
                                + declaredRows + " observedAtLeast=" + ((long) index + 1L));
            }
            DataRow snapshot = RowSnapshot.copyOf(source.next());
            residentBytes = limits.requireAdditional(
                    index, residentBytes, RowSizeEstimator.estimateRow(snapshot),
                    LIMIT_OWNER);
            snapshots[index++] = snapshot;
        }
        if (index != declaredRows) {
            throw new IllegalStateException(
                    "CollectionSource source size changed during snapshot: declared="
                            + declaredRows + " observed=" + index);
        }
        this.rows = snapshots;
        this.batchSourceSplit = stableSplitId == null ? null
                : new BatchSourceSplitDescriptor(
                        BatchSourceSplitDescriptor.CURRENT_VERSION,
                        "embedded-collection", 1, stableSplitId,
                        SchemaFingerprint.of(dataSchema), snapshotId(dataSchema, snapshots),
                        BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
    }

    private void requireResidentBytes(long observedBytes) {
        if (observedBytes > limits.maxBytes()) {
            throw new CompatibilityDataLimitException(
                    CompatibilityDataLimitException.Kind.BYTES,
                    limits.maxBytes(), observedBytes, LIMIT_OWNER);
        }
    }

    private static String requireStableSplitId(String stableSplitId) {
        if (stableSplitId == null) {
            throw new IllegalArgumentException("stableSplitId must not be null");
        }
        return stableSplitId;
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    /** null is reserved for this built-in's legacy compatibility constructors. */
    @Override
    public BatchSourceSplitDescriptor batchSourceSplit() {
        return batchSourceSplit;
    }

    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < rows.length;
            }

            @Override
            public DataRow next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return RowSnapshot.copyOf(rows[index++]);
            }
        };
    }

    private static String snapshotId(DataSchema dataSchema, DataRow[] rows) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update("rowweave-embedded-snapshot-v1".getBytes(StandardCharsets.UTF_8));
            updateBytes(digest, SchemaFingerprint.of(dataSchema).getBytes(StandardCharsets.US_ASCII));
            updateInt(digest, rows.length);
            RowSerializer serializer = new RowSerializer(dataSchema);
            for (DataRow row : rows) {
                byte[] encoded = serializer.serialize(row);
                updateBytes(digest, encoded);
            }
            return hex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    /** Rebuild through the public invariants; Java serialization must not forge rows or identity. */
    private Object readResolve() throws ObjectStreamException {
        try {
            String splitId = batchSourceSplit == null ? null : batchSourceSplit.splitId();
            CollectionSource rebuilt = new CollectionSource(
                    dataSchema, Arrays.asList(rows), limits, splitId);
            if (!Objects.equals(batchSourceSplit, rebuilt.batchSourceSplit)) {
                throw new IllegalArgumentException(
                        "embedded collection descriptor does not match its canonical snapshot");
            }
            return rebuilt;
        } catch (RuntimeException invalid) {
            InvalidObjectException rejected = new InvalidObjectException(
                    "invalid serialized CollectionSource: " + invalid.getMessage());
            rejected.initCause(invalid);
            throw rejected;
        }
    }

    private static void updateBytes(MessageDigest digest, byte[] value) {
        updateInt(digest, value.length);
        digest.update(value);
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update((byte) (value >>> 24));
        digest.update((byte) (value >>> 16));
        digest.update((byte) (value >>> 8));
        digest.update((byte) value);
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            result[i * 2] = digits[(bytes[i] >>> 4) & 0x0f];
            result[i * 2 + 1] = digits[bytes[i] & 0x0f];
        }
        return new String(result);
    }
}
