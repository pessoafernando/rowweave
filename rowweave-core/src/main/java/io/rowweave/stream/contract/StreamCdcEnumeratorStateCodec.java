package io.rowweave.stream.contract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Canonical checksum-protected resident CDC enumerator checkpoint frame. */
public final class StreamCdcEnumeratorStateCodec {
    private static final byte[] MAGIC =
            "RWXXCES1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_BODY_BYTES = 1024 * 1024;
    private static final int CHECKSUM_BYTES = 32;

    private StreamCdcEnumeratorStateCodec() { }

    public static byte[] encode(StreamCdcEnumeratorState state) {
        if (state == null) throw new IllegalArgumentException("state is required");
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                body.writeInt(state.version());
                writeText(body, state.sourceOperatorUid());
                writeText(body, state.snapshotId());
                body.writeLong(state.snapshotCutSequence());
                body.writeInt(state.expectedSplitIds().size());
                for (String split : state.expectedSplitIds()) {
                    writeText(body, split);
                }
                body.writeInt(state.completedSplits().size());
                for (StreamCdcEnumeratorState.CompletedSplit completed
                        : state.completedSplits()) {
                    writeText(body, completed.splitId());
                    body.writeLong(completed.recordCount());
                    writeText(body, completed.contentDigest());
                }
                body.writeInt(state.phase().wireId());
                body.writeLong(state.lastLogSequence());
                body.writeBoolean(state.handoffDigest() != null);
                if (state.handoffDigest() != null) {
                    writeText(body, state.handoffDigest());
                }
                if (state.version()
                        >= StreamCdcEnumeratorState.CURRENT_VERSION) {
                    body.writeBoolean(state.hasExternalPosition());
                    if (state.hasExternalPosition()) {
                        byte[] position = state.externalPositionFrame();
                        body.writeInt(position.length);
                        body.write(position);
                    }
                }
            }
            byte[] body = bodyBytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw new IllegalArgumentException(
                        "resident CDC enumerator state exceeds its byte bound");
            }
            ByteArrayOutputStream frame = new ByteArrayOutputStream(
                    MAGIC.length + 4 + body.length + CHECKSUM_BYTES);
            try (DataOutputStream output = new DataOutputStream(frame)) {
                output.write(MAGIC);
                output.writeInt(body.length);
                output.write(body);
                output.write(sha256(body));
            }
            return frame.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory resident CDC state encoding failed", impossible);
        }
    }

    public static StreamCdcEnumeratorState decode(byte[] frame) {
        if (frame == null || frame.length < MAGIC.length + 4 + CHECKSUM_BYTES
                || frame.length > MAGIC.length + 4 + MAX_BODY_BYTES
                + CHECKSUM_BYTES) {
            throw malformed("resident CDC enumerator frame length is invalid", null);
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(MAGIC, magic)) {
                throw malformed("resident CDC enumerator magic is invalid", null);
            }
            int bodyLength = input.readInt();
            if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES
                    || bodyLength != frame.length - MAGIC.length - 4
                    - CHECKSUM_BYTES) {
                throw malformed("resident CDC enumerator body length is invalid", null);
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] checksum = new byte[CHECKSUM_BYTES];
            input.readFully(checksum);
            if (input.read() != -1) {
                throw malformed("resident CDC enumerator frame has trailing bytes", null);
            }
            if (!MessageDigest.isEqual(checksum, sha256(body))) {
                throw malformed("resident CDC enumerator checksum mismatch", null);
            }
            StreamCdcEnumeratorState decoded = decodeBody(body);
            if (!Arrays.equals(frame, encode(decoded))) {
                throw malformed("resident CDC enumerator frame is not canonical", null);
            }
            return decoded;
        } catch (StreamContractException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw malformed("resident CDC enumerator frame is truncated", truncated);
        } catch (IOException failure) {
            throw malformed("resident CDC enumerator frame is malformed", failure);
        }
    }

    public static byte[] magic() { return MAGIC.clone(); }

    private static StreamCdcEnumeratorState decodeBody(byte[] body)
            throws IOException {
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            int version = input.readInt();
            String operator = readText(input, 256);
            String snapshot = readText(input, 64);
            long cut = input.readLong();
            int expectedCount = readCount(input);
            List<String> expected = new ArrayList<>(expectedCount);
            for (int index = 0; index < expectedCount; index++) {
                expected.add(readText(input, 256));
            }
            int completedCount = readCount(input);
            List<StreamCdcEnumeratorState.CompletedSplit> completed =
                    new ArrayList<>(completedCount);
            for (int index = 0; index < completedCount; index++) {
                completed.add(new StreamCdcEnumeratorState.CompletedSplit(
                        readText(input, 256), input.readLong(),
                        readText(input, 64)));
            }
            StreamCdcEnumeratorState.Phase phase =
                    StreamCdcEnumeratorState.Phase.fromWire(input.readInt());
            long lastLog = input.readLong();
            String handoff = input.readBoolean() ? readText(input, 64) : null;
            byte[] externalPosition = null;
            if (version >= StreamCdcEnumeratorState.CURRENT_VERSION) {
                if (input.readBoolean()) {
                    int length = input.readInt();
                    if (length < 1 || length
                            > StreamCdcEnumeratorState
                            .MAX_EXTERNAL_POSITION_BYTES) {
                        throw malformed(
                                "resident CDC external position length is invalid",
                                null);
                    }
                    externalPosition = new byte[length];
                    input.readFully(externalPosition);
                }
            }
            if (input.read() != -1) {
                throw malformed("resident CDC state body has trailing bytes", null);
            }
            return new StreamCdcEnumeratorState(
                    version, operator, snapshot, cut, expected, completed,
                    phase, lastLog, handoff, externalPosition);
        }
    }

    private static int readCount(DataInputStream input) throws IOException {
        int count = input.readInt();
        if (count < 0 || count > StreamCdcEnumeratorState.MAX_SPLITS) {
            throw malformed("resident CDC split count exceeds its bound", null);
        }
        return count;
    }

    private static String readText(DataInputStream input, int maximumBytes)
            throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maximumBytes) {
            throw malformed("resident CDC text length is invalid", null);
        }
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded));
            return decoded.toString();
        } catch (CharacterCodingException malformed) {
            throw malformed("resident CDC text is malformed UTF-8", malformed);
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static StreamContractException malformed(
            String message, Throwable cause) {
        StreamContractException failure = StreamContractSupport.failure(
                "MALFORMED_CDC_ENUMERATOR_STATE", "cdc.enumerator", message);
        if (cause != null) failure.initCause(cause);
        return failure;
    }
}
