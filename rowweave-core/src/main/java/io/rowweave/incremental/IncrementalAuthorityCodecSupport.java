package io.rowweave.incremental;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

final class IncrementalAuthorityCodecSupport {
    private IncrementalAuthorityCodecSupport() {}

    static void writeText(DataOutputStream out, String value) throws IOException {
        IncrementalAuthorityValue.text(value, "wire text");
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    static String readText(DataInputStream in, String field) throws IOException {
        byte[] encoded = readBytes(in, IncrementalAuthorityValue.MAX_ID_BYTES, field);
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded)).toString();
            return IncrementalAuthorityValue.text(value, field);
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(field + " is invalid UTF-8", invalid);
        }
    }

    static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    static byte[] readBytes(DataInputStream in, int limit, String field) throws IOException {
        if (in.available() < 4) throw new IllegalArgumentException(field + " length is missing");
        int length = in.readInt();
        if (length < 0 || length > limit || length > in.available()) {
            throw new IllegalArgumentException(field + " length is invalid");
        }
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    static byte[] readDigest(DataInputStream in, String field) throws IOException {
        if (in.available() < IncrementalAuthorityValue.DIGEST_BYTES) {
            throw new IllegalArgumentException(field + " is truncated");
        }
        byte[] value = new byte[IncrementalAuthorityValue.DIGEST_BYTES];
        in.readFully(value);
        return value;
    }
}
