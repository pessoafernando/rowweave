package io.rowweave;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.SchemaFingerprint;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.util.Objects;

/** Each open pins one validated manifest generation's member Source. */
public final class ManifestFileSource implements BatchSource {

    private static final long serialVersionUID = 1L;

    private final DataSchema dataSchema;
    private final String domainPath;
    private final String memberName;
    private final Long generation;
    private final String schemaFingerprint;
    private final String domainId;
    /** null means the latest-generation compatibility constructor remains legacy. */
    private final BatchSourceSplitDescriptor batchSourceSplit;

    public ManifestFileSource(DataSchema dataSchema, String domainPath, String memberName) {
        this(dataSchema, domainPath, memberName, null);
    }

    public ManifestFileSource(DataSchema dataSchema, String domainPath, String memberName, long generation) {
        this(dataSchema, domainPath, memberName, Long.valueOf(generation));
    }

    private ManifestFileSource(DataSchema dataSchema, String domainPath, String memberName, Long generation) {
        if (dataSchema == null) {
            throw new IllegalArgumentException("dataSchema must not be null");
        }
        if (domainPath == null || domainPath.isEmpty()) {
            throw new IllegalArgumentException("domainPath must not be empty");
        }
        new CommitManifest.Member(memberName, "data/1/probe.data", "probe", 0L,
                new byte[CommitManifest.SHA256_BYTES]);
        if (generation != null && generation <= 0L) {
            throw new IllegalArgumentException("generation must be > 0");
        }
        this.dataSchema = dataSchema;
        this.domainPath = Paths.get(domainPath).toAbsolutePath().normalize().toString();
        this.memberName = memberName;
        this.generation = generation;
        this.schemaFingerprint = SchemaFingerprint.of(dataSchema);
        this.domainId = new ManifestFileStore(Paths.get(this.domainPath)).domainId();
        this.batchSourceSplit = generation == null ? null
                : exactDescriptor(generation.longValue());
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    /** null is reserved for the built-in latest-generation compatibility constructor. */
    @Override
    public BatchSourceSplitDescriptor batchSourceSplit() {
        return batchSourceSplit;
    }

    public FileSource.Reader openReader() {
        ManifestFileStore store = new ManifestFileStore(Paths.get(domainPath));
        if (!domainId.equals(store.domainId())) {
            throw new IllegalStateException("manifest commit domain identity changed after input creation: expected="
                    + domainId + " actual=" + store.domainId());
        }
        CommitManifest manifest = generation == null ? store.latest() : store.exact(generation);
        if (manifest == null) {
            throw new IllegalArgumentException("manifest generation does not exist in domain: "
                    + (generation == null ? "latest" : generation));
        }
        CommitManifest.Member selected = null;
        for (CommitManifest.Member member : manifest.members()) {
            if (memberName.equals(member.name())) {
                selected = member;
                break;
            }
        }
        if (selected == null) {
            throw new IllegalArgumentException("manifest member is missing: " + memberName);
        }
        if (!schemaFingerprint.equals(selected.schemaFingerprint())) {
            throw new IllegalArgumentException("manifest member schema fingerprint mismatch: " + memberName);
        }
        Path data = store.domainPath().resolve(selected.relativeDataPath()).normalize();
        return new FileSource(dataSchema, data.toString()).openReader();
    }

    @Override
    public FileSource.Reader iterator() {
        return openReader();
    }

    private BatchSourceSplitDescriptor exactDescriptor(long exactGeneration) {
        String logicalMember = digest("member", domainId, memberName);
        String snapshot = digest("snapshot", domainId,
                Long.toString(exactGeneration), memberName, schemaFingerprint);
        return new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                "manifest-file", 1, "manifest-member:" + logicalMember,
                schemaFingerprint, snapshot,
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
    }

    /** Validate the serialized descriptor association without resolving or opening external data. */
    private Object readResolve() throws ObjectStreamException {
        try {
            if (dataSchema == null || domainPath == null || domainPath.isEmpty()
                    || memberName == null || domainId == null || domainId.isEmpty()) {
                throw new IllegalArgumentException("manifest source identity fields are required");
            }
            String normalized = Paths.get(domainPath).toAbsolutePath().normalize().toString();
            if (!normalized.equals(domainPath)) {
                throw new IllegalArgumentException("manifest domainPath is not canonical");
            }
            new CommitManifest.Member(memberName, "data/1/probe.data", "probe", 0L,
                    new byte[CommitManifest.SHA256_BYTES]);
            if (generation != null && generation.longValue() <= 0L) {
                throw new IllegalArgumentException("manifest generation must be positive");
            }
            if (!SchemaFingerprint.of(dataSchema).equals(schemaFingerprint)) {
                throw new IllegalArgumentException("manifest source schema fingerprint mismatch");
            }
            BatchSourceSplitDescriptor expected = generation == null
                    ? null : exactDescriptor(generation.longValue());
            // A serialized pinned Source written before the descriptor field existed has no descriptor. It remains legacy;
            // clearing an exact descriptor can only downgrade and durable admission will reject it.
            boolean mismatch = generation == null
                    ? batchSourceSplit != null
                    : batchSourceSplit != null && !Objects.equals(expected, batchSourceSplit);
            if (mismatch) {
                throw new IllegalArgumentException(
                        "manifest descriptor does not match its pinned snapshot identity");
            }
            return this;
        } catch (RuntimeException invalid) {
            InvalidObjectException rejected = new InvalidObjectException(
                    "invalid serialized ManifestFileSource: " + invalid.getMessage());
            rejected.initCause(invalid);
            throw rejected;
        }
    }

    private static String digest(String... fields) {
        java.security.MessageDigest digest;
        try {
            digest = java.security.MessageDigest.getInstance("SHA-256");
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
        for (String field : fields) {
            byte[] encoded = field.getBytes(StandardCharsets.UTF_8);
            digest.update((byte) (encoded.length >>> 24));
            digest.update((byte) (encoded.length >>> 16));
            digest.update((byte) (encoded.length >>> 8));
            digest.update((byte) encoded.length);
            digest.update(encoded);
        }
        byte[] bytes = digest.digest();
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            result[i * 2] = digits[(bytes[i] >>> 4) & 0x0f];
            result[i * 2 + 1] = digits[bytes[i] & 0x0f];
        }
        return new String(result);
    }
}
