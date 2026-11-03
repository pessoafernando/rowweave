package io.rowweave.deployment;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Base64;

/** Strict bounded UTF-8 codec for one canonical RWXXDEP1 operations manifest. */
public final class DeploymentManifestCodec {
    static final int MAX_MANIFEST_BYTES = 16 * 1024;

    private DeploymentManifestCodec() { }

    public static byte[] encode(DeploymentManifest manifest) {
        if (manifest == null) throw new IllegalArgumentException("deployment manifest is required");
        String source = manifest.absoluteJarPath().toString();
        if (containsLineBreak(source)) {
            throw invalid("absoluteJarPath contains a line break");
        }
        String encoded = DeploymentManifest.FORMAT + '\n'
                + "deploymentId=" + manifest.deploymentId() + '\n'
                + "absoluteJarPath=" + source + '\n'
                + "jarLength=" + manifest.jarLength() + '\n'
                + "sha256=" + hex(manifest.sha256()) + '\n'
                + "signerKeyId=" + manifest.signerKeyId() + '\n'
                + "detachedSignatureBase64="
                + Base64.getEncoder().encodeToString(manifest.detachedSignature()) + '\n';
        byte[] bytes = encoded.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_MANIFEST_BYTES) throw invalid("manifest exceeds byte limit");
        return bytes;
    }

    public static DeploymentManifest decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_MANIFEST_BYTES) {
            throw invalid("manifest byte length is out of bounds");
        }
        try {
            String text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
            if (text.indexOf('\r') >= 0 || text.indexOf('\0') >= 0 || !text.endsWith("\n")) {
                throw invalid("manifest is not canonical LF text");
            }
            String[] lines = text.split("\n", -1);
            if (lines.length != 8 || !lines[7].isEmpty()
                    || !DeploymentManifest.FORMAT.equals(lines[0])) {
                throw invalid("manifest field count or format is invalid");
            }
            String deploymentId = field(lines[1], "deploymentId");
            String source = field(lines[2], "absoluteJarPath");
            String lengthText = field(lines[3], "jarLength");
            String digestText = field(lines[4], "sha256");
            String signerKeyId = field(lines[5], "signerKeyId");
            String signatureText = field(lines[6], "detachedSignatureBase64");
            if (containsLineBreak(source)) throw invalid("invalid absoluteJarPath");
            long jarLength = parseCanonicalLong(lengthText);
            byte[] digest = decodeLowerHex(digestText);
            byte[] signature = decodeCanonicalBase64(signatureText);
            DeploymentManifest manifest = new DeploymentManifest(deploymentId,
                    Paths.get(source), jarLength, digest, signerKeyId, signature);
            if (!java.util.Arrays.equals(bytes, encode(manifest))) {
                throw invalid("manifest is not in canonical form");
            }
            return manifest;
        } catch (DeploymentValidationException failure) {
            throw failure;
        } catch (CharacterCodingException | IllegalArgumentException failure) {
            throw new DeploymentValidationException("INVALID_DEPLOYMENT_MANIFEST",
                    "manifest cannot be decoded", failure);
        }
    }

    private static String field(String line, String name) {
        String prefix = name + '=';
        if (!line.startsWith(prefix)) throw invalid("expected field " + name);
        String value = line.substring(prefix.length());
        if (value.isEmpty()) throw invalid("empty field " + name);
        return value;
    }

    private static long parseCanonicalLong(String value) {
        if (value.isEmpty() || (value.length() > 1 && value.charAt(0) == '0')) {
            throw invalid("jarLength is not canonical");
        }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) < '0' || value.charAt(i) > '9') {
                throw invalid("jarLength is not decimal");
            }
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException failure) {
            throw invalid("jarLength is out of range");
        }
    }

    private static byte[] decodeLowerHex(String value) {
        if (value.length() != 64) throw invalid("sha256 length is invalid");
        byte[] bytes = new byte[32];
        for (int i = 0; i < bytes.length; i++) {
            int high = lowerHex(value.charAt(i * 2));
            int low = lowerHex(value.charAt(i * 2 + 1));
            if (high < 0 || low < 0) throw invalid("sha256 is not lowercase hex");
            bytes[i] = (byte) ((high << 4) | low);
        }
        return bytes;
    }

    private static byte[] decodeCanonicalBase64(String value) {
        try {
            byte[] bytes = Base64.getDecoder().decode(value);
            if (!Base64.getEncoder().encodeToString(bytes).equals(value)) {
                throw invalid("signature base64 is not canonical");
            }
            return bytes;
        } catch (IllegalArgumentException failure) {
            if (failure instanceof DeploymentValidationException) {
                throw (DeploymentValidationException) failure;
            }
            throw invalid("signature base64 is invalid");
        }
    }

    private static int lowerHex(char value) {
        if (value >= '0' && value <= '9') return value - '0';
        if (value >= 'a' && value <= 'f') return value - 'a' + 10;
        return -1;
    }

    private static String hex(byte[] bytes) {
        char[] result = new char[bytes.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            result[i * 2] = digits[(bytes[i] >>> 4) & 0xf];
            result[i * 2 + 1] = digits[bytes[i] & 0xf];
        }
        return new String(result);
    }

    private static boolean containsLineBreak(String value) {
        return value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0;
    }

    private static DeploymentValidationException invalid(String message) {
        return new DeploymentValidationException("INVALID_DEPLOYMENT_MANIFEST", message);
    }
}
