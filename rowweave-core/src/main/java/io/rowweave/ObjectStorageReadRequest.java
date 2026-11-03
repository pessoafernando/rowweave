package io.rowweave;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Pattern;

/** Immutable, bounded request for an exact object version. Values are never rendered by toString. */
public final class ObjectStorageReadRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final long MAX_OBJECT_BYTES = 1L << 40;
    public static final int MAX_NAMESPACE_BYTES = 512;
    public static final int MAX_OBJECT_KEY_BYTES = 4096;
    public static final int MAX_VERSION_ID_BYTES = 1024;

    private static final Pattern REFERENCE =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

    private final String endpointRef;
    private final String credentialRef;
    private final String namespace;
    private final String objectKey;
    private final String versionId;
    private final long contentLength;
    private final String contentSha256;

    public ObjectStorageReadRequest(
            String endpointRef, String credentialRef, String namespace,
            String objectKey, String versionId, long contentLength,
            String contentSha256) {
        this.endpointRef = reference(endpointRef, "endpointRef");
        this.credentialRef = reference(credentialRef, "credentialRef");
        this.namespace = objectText(
                namespace, "namespace", MAX_NAMESPACE_BYTES, true);
        this.objectKey = objectText(
                objectKey, "objectKey", MAX_OBJECT_KEY_BYTES, true);
        this.versionId = objectText(
                versionId, "versionId", MAX_VERSION_ID_BYTES, false);
        if (contentLength < 0L || contentLength > MAX_OBJECT_BYTES) {
            throw invalid("contentLength");
        }
        if (contentSha256 == null || !contentSha256.matches("[0-9a-f]{64}")) {
            throw invalid("contentSha256");
        }
        this.contentLength = contentLength;
        this.contentSha256 = contentSha256;
    }

    public String endpointRef() { return endpointRef; }
    public String credentialRef() { return credentialRef; }
    public String namespace() { return namespace; }
    public String objectKey() { return objectKey; }
    public String versionId() { return versionId; }
    public long contentLength() { return contentLength; }
    public String contentSha256() { return contentSha256; }

    private Object readResolve() throws ObjectStreamException {
        return new ObjectStorageReadRequest(
                endpointRef, credentialRef, namespace, objectKey, versionId,
                contentLength, contentSha256);
    }

    private static String reference(String value, String field) {
        if (value == null || !REFERENCE.matcher(value).matches()) {
            throw invalid(field);
        }
        return value;
    }

    private static String objectText(
            String value, String field, int maxBytes, boolean rejectParentSegment) {
        if (value == null || value.isEmpty() || value.indexOf('?') >= 0
                || value.indexOf('#') >= 0 || containsControl(value)
                || encodedLength(value, field) > maxBytes
                || (rejectParentSegment && hasParentSegment(value))) {
            throw invalid(field);
        }
        return value;
    }

    private static boolean containsControl(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) return true;
        }
        return false;
    }

    private static boolean hasParentSegment(String value) {
        String normalized = value.replace('\\', '/');
        if ("..".equals(normalized)) return true;
        String[] segments = normalized.split("/", -1);
        for (String segment : segments) {
            if ("..".equals(segment)) return true;
        }
        return false;
    }

    private static int encodedLength(String value, String field) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            return encoded.remaining();
        } catch (CharacterCodingException invalid) {
            throw invalid(field);
        }
    }

    private static IllegalArgumentException invalid(String field) {
        return new IllegalArgumentException(
                "object storage request " + field + " is invalid or exceeds its hard limit");
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ObjectStorageReadRequest)) return false;
        ObjectStorageReadRequest that = (ObjectStorageReadRequest) other;
        return contentLength == that.contentLength
                && endpointRef.equals(that.endpointRef)
                && credentialRef.equals(that.credentialRef)
                && namespace.equals(that.namespace)
                && objectKey.equals(that.objectKey)
                && versionId.equals(that.versionId)
                && contentSha256.equals(that.contentSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpointRef, credentialRef, namespace, objectKey,
                versionId, contentLength, contentSha256);
    }

    @Override
    public String toString() {
        return "ObjectStorageReadRequest{redacted}";
    }
}
