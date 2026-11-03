package io.rowweave.commit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Deterministic, bounded, immutable generation manifest carrying a SHA-256 frame checksum. */
public final class CommitManifest {

    public static final int SHA256_BYTES = 32;
    public static final int MAX_ENCODED_BYTES = 16 * 1024 * 1024;
    private static final int MAGIC = 0x414D4631; // AMF1
    private static final int VERSION = 1;
    private static final int MAX_MEMBERS = 10_000;
    private static final int MAX_ID_BYTES = 1_024;
    private static final int MAX_PATH_BYTES = 4_096;
    private static final int MAX_SCHEMA_BYTES = 4_096;

    private final String domainId;
    private final long generation;
    private final String attemptId;
    private final List<Member> members;

    public CommitManifest(String domainId, long generation, String attemptId, List<Member> members) {
        this.domainId = requireText(domainId, "domainId", MAX_ID_BYTES);
        if (generation <= 0L) {
            throw new IllegalArgumentException("generation must be > 0");
        }
        this.generation = generation;
        this.attemptId = requireText(attemptId, "attemptId", MAX_ID_BYTES);
        if (members == null || members.isEmpty() || members.size() > MAX_MEMBERS) {
            throw new IllegalArgumentException("manifest member count must be in [1," + MAX_MEMBERS + "]");
        }
        List<Member> ordered = new ArrayList<>(members);
        ordered.sort(Comparator.comparing(Member::name));
        Set<String> names = new HashSet<>();
        for (Member member : ordered) {
            if (member == null || !names.add(member.name())) {
                throw new IllegalArgumentException("duplicate/null manifest member");
            }
        }
        this.members = Collections.unmodifiableList(ordered);
    }

    public String domainId() { return domainId; }
    public long generation() { return generation; }
    public String attemptId() { return attemptId; }
    public List<Member> members() { return members; }

    public byte[] encode() {
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(payloadBytes)) {
                out.writeInt(MAGIC);
                out.writeInt(VERSION);
                out.writeLong(generation);
                writeText(out, domainId, MAX_ID_BYTES);
                writeText(out, attemptId, MAX_ID_BYTES);
                out.writeInt(members.size());
                for (Member member : members) {
                    writeText(out, member.name, MAX_ID_BYTES);
                    writeText(out, member.relativeDataPath, MAX_PATH_BYTES);
                    writeText(out, member.schemaFingerprint, MAX_SCHEMA_BYTES);
                    out.writeLong(member.length);
                    out.write(member.sha256);
                }
            }
            byte[] payload = payloadBytes.toByteArray();
            byte[] encoded = Arrays.copyOf(payload, payload.length + SHA256_BYTES);
            System.arraycopy(sha256(payload), 0, encoded, payload.length, SHA256_BYTES);
            if (encoded.length > MAX_ENCODED_BYTES) {
                throw new IllegalArgumentException("manifest exceeds max encoded bytes: " + encoded.length);
            }
            return encoded;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory manifest encoding failed", impossible);
        }
    }

    public static CommitManifest decode(byte[] encoded) {
        if (encoded == null || encoded.length < 4 + 4 + 8 + SHA256_BYTES
                || encoded.length > MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("invalid manifest frame length: "
                    + (encoded == null ? -1 : encoded.length));
        }
        int payloadLength = encoded.length - SHA256_BYTES;
        byte[] payload = Arrays.copyOf(encoded, payloadLength);
        byte[] actualChecksum = Arrays.copyOfRange(encoded, payloadLength, encoded.length);
        if (!MessageDigest.isEqual(sha256(payload), actualChecksum)) {
            throw new IllegalArgumentException("manifest checksum mismatch");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (in.readInt() != MAGIC) {
                throw new IllegalArgumentException("manifest magic mismatch");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("unsupported manifest version: " + version);
            }
            long generation = in.readLong();
            String domainId = readText(in, "domainId", MAX_ID_BYTES);
            String attemptId = readText(in, "attemptId", MAX_ID_BYTES);
            int count = in.readInt();
            if (count <= 0 || count > MAX_MEMBERS) {
                throw new IllegalArgumentException("invalid manifest member count: " + count);
            }
            List<Member> members = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String name = readText(in, "member name", MAX_ID_BYTES);
                String relativePath = readText(in, "member path", MAX_PATH_BYTES);
                String schema = readText(in, "schema fingerprint", MAX_SCHEMA_BYTES);
                long length = in.readLong();
                byte[] hash = new byte[SHA256_BYTES];
                in.readFully(hash);
                members.add(new Member(name, relativePath, schema, length, hash));
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("manifest has trailing payload bytes: " + in.available());
            }
            return new CommitManifest(domainId, generation, attemptId, members);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException("truncated manifest payload", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException("failed to decode manifest", malformed);
        }
    }

    private static void writeText(DataOutputStream out, String value, int maxBytes) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maxBytes) {
            throw new IllegalArgumentException("manifest text exceeds " + maxBytes + " bytes");
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readText(DataInputStream in, String field, int maxBytes) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maxBytes) {
            throw new IllegalArgumentException("invalid " + field + " byte length: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("invalid UTF-8 in " + field, malformed);
        }
    }

    private static String requireText(String value, String field, int maxBytes) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be empty");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
            throw new IllegalArgumentException(field + " exceeds " + maxBytes + " UTF-8 bytes");
        }
        if (value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(field + " contains NUL");
        }
        return value;
    }

    static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof CommitManifest)) return false;
        CommitManifest that = (CommitManifest) other;
        return generation == that.generation && domainId.equals(that.domainId)
                && attemptId.equals(that.attemptId) && members.equals(that.members);
    }

    @Override
    public int hashCode() {
        return Objects.hash(domainId, generation, attemptId, members);
    }

    /** One manifest member; its data path is relative to the commit domain and its content is immutable before the manifest is published. */
    public static final class Member {
        private final String name;
        private final String relativeDataPath;
        private final String schemaFingerprint;
        private final long length;
        private final byte[] sha256;

        public Member(String name, String relativeDataPath, String schemaFingerprint,
                      long length, byte[] sha256) {
            this.name = validateMemberName(name);
            this.relativeDataPath = validateRelativePath(relativeDataPath);
            this.schemaFingerprint = requireText(
                    schemaFingerprint, "schemaFingerprint", MAX_SCHEMA_BYTES);
            if (length < 0L) {
                throw new IllegalArgumentException("member length must be >= 0");
            }
            this.length = length;
            if (sha256 == null || sha256.length != SHA256_BYTES) {
                throw new IllegalArgumentException("member sha256 must be exactly " + SHA256_BYTES + " bytes");
            }
            this.sha256 = sha256.clone();
        }

        public String name() { return name; }
        public String relativeDataPath() { return relativeDataPath; }
        public String schemaFingerprint() { return schemaFingerprint; }
        public long length() { return length; }
        public byte[] sha256() { return sha256.clone(); }

        private static String validateMemberName(String name) {
            requireText(name, "member name", MAX_ID_BYTES);
            if (".".equals(name) || "..".equals(name)
                    || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0) {
                throw new IllegalArgumentException("unsafe manifest member name: " + name);
            }
            if (!Normalizer.normalize(name, Normalizer.Form.NFC).equals(name)) {
                throw new IllegalArgumentException("manifest member name must use NFC normalization: " + name);
            }
            return name;
        }

        private static String validateRelativePath(String raw) {
            requireText(raw, "relativeDataPath", MAX_PATH_BYTES);
            if (raw.indexOf('\\') >= 0) {
                throw new IllegalArgumentException("manifest path must use '/' separators: " + raw);
            }
            Path path = Paths.get(raw);
            if (path.isAbsolute() || path.getNameCount() == 0 || !path.normalize().equals(path)
                    || path.startsWith("..")) {
                throw new IllegalArgumentException("unsafe manifest relative path: " + raw);
            }
            return raw;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Member)) return false;
            Member that = (Member) other;
            return length == that.length && name.equals(that.name)
                    && relativeDataPath.equals(that.relativeDataPath)
                    && schemaFingerprint.equals(that.schemaFingerprint)
                    && Arrays.equals(sha256, that.sha256);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name, relativeDataPath, schemaFingerprint, length);
            return 31 * result + Arrays.hashCode(sha256);
        }
    }
}
