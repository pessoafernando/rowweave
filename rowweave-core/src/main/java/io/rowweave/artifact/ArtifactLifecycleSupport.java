package io.rowweave.artifact;

/** Untrusted-identity fence shared by the model and the codec. */
final class ArtifactLifecycleSupport {
    static final int MAX_TEXT_BYTES = 256;

    private ArtifactLifecycleSupport() { }

    static String requireText(String value, String field) {
        ArtifactText.encode(value, field, MAX_TEXT_BYTES);
        return value;
    }

    static byte[] textBytes(String value, String field) {
        return ArtifactText.encode(value, field, MAX_TEXT_BYTES);
    }

    static String decodeText(byte[] bytes, String field) {
        return ArtifactText.decode(bytes, field, MAX_TEXT_BYTES);
    }

    static int shard(String artifactId) {
        ArtifactRepositorySupport.validateId(artifactId);
        return Integer.parseInt(artifactId.substring(0, 2), 16);
    }

    static byte[] hexBytes(String value) {
        ArtifactRepositorySupport.validateId(value);
        byte[] result = new byte[32];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) out.append(String.format("%02x", value & 0xff));
        return out.toString();
    }
}
