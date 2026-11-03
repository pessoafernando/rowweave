package io.rowweave.cluster.rpc;

import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunExecutionEventPageCodec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Bounded streaming RPC codec compatible with the existing wire format. */
public final class RpcCodec {
    private static final int MAX_STRING_BYTES = 64 * 1024;
    private static final int DEFAULT_NORMAL_FRAME_BYTES = 1024 * 1024;
    private static final int DEFAULT_LARGE_FRAME_BYTES = 80 * 1024 * 1024;
    private static final byte[] SECURE_MAGIC = "RWXXRPCS1".getBytes(StandardCharsets.US_ASCII);
    static final int SECURE_MAGIC_BYTES = 9;
    static final int SECURE_SUFFIX_BYTES = SECURE_MAGIC_BYTES + Integer.BYTES + 64 + 64;
    private static final int SECURE_VERSION = 1;
    private static final byte[] SUBMIT_TIMEOUT_MAGIC =
            "RWXXRPCT1".getBytes(StandardCharsets.US_ASCII);
    static final int SUBMIT_TIMEOUT_MAGIC_BYTES = 9;
    static final int SUBMIT_TIMEOUT_EXTENSION_BYTES =
            SUBMIT_TIMEOUT_MAGIC_BYTES + Integer.BYTES + 2 * Long.BYTES;
    private static final int SUBMIT_TIMEOUT_VERSION = 1;
    private static final byte[] DATA_REGISTRATION_MAGIC =
            "RWXXRPCD1".getBytes(StandardCharsets.US_ASCII);
    static final int DATA_REGISTRATION_MAGIC_BYTES = 9;
    static final int DATA_REGISTRATION_LOOPBACK_BYTES =
            DATA_REGISTRATION_MAGIC_BYTES + 3 * Integer.BYTES;
    private static final int DATA_REGISTRATION_VERSION = 1;

    private RpcCodec() { }

    public static byte[] encode(RpcMessage message) {
        int frameBytes = checkedFrameBytes(message, DEFAULT_NORMAL_FRAME_BYTES, DEFAULT_LARGE_FRAME_BYTES);
        byte[] encoded = new byte[frameBytes + Integer.BYTES];
        try {
            encodeToChecked(message, new FixedOutputStream(encoded), frameBytes);
        } catch (IOException impossible) {
            throw new IllegalStateException("fixed RPC array write failed", impossible);
        }
        return encoded;
    }

    public static byte[] encodeV1(
            RpcMessage message, NegotiatedProtocol negotiated) {
        requireNegotiatedMessage(message, negotiated);
        int frameBytes = checkedFrameBytes(
                message, DEFAULT_NORMAL_FRAME_BYTES, DEFAULT_LARGE_FRAME_BYTES);
        if (frameBytes > negotiated.maximumFrameBytes()) {
            throw new ProtocolNegotiationException("FRAME_LIMIT_EXCEEDED",
                    "RPC frame exceeds the negotiated maximum");
        }
        byte[] encoded = new byte[frameBytes + Integer.BYTES];
        try {
            encodeToCheckedV1(message, new FixedOutputStream(encoded), frameBytes);
        } catch (IOException impossible) {
            throw new IllegalStateException("fixed v1 RPC array write failed", impossible);
        }
        return encoded;
    }

    public static void encodeTo(RpcMessage message, OutputStream target) throws IOException {
        int frameBytes = checkedFrameBytes(message, DEFAULT_NORMAL_FRAME_BYTES, DEFAULT_LARGE_FRAME_BYTES);
        encodeToChecked(message, target, frameBytes);
    }

    static void encodeTo(RpcMessage message, OutputStream target, RpcAdmissionConfig admission)
            throws IOException {
        int frameBytes = checkedFrameBytes(message, admission.normalFrameBytes(), admission.largeFrameBytes());
        encodeToChecked(message, target, frameBytes);
    }

    static void encodeToV1(
            RpcMessage message, OutputStream target, RpcAdmissionConfig admission,
            NegotiatedProtocol negotiated) throws IOException {
        requireNegotiatedMessage(message, negotiated);
        int frameBytes = checkedFrameBytes(
                message, admission.normalFrameBytes(), admission.largeFrameBytes());
        if (frameBytes > negotiated.maximumFrameBytes()) {
            throw new ProtocolNegotiationException("FRAME_LIMIT_EXCEEDED",
                    "RPC frame exceeds the negotiated maximum");
        }
        encodeToCheckedV1(message, target, frameBytes);
    }

    private static void encodeToChecked(RpcMessage message, OutputStream target, int frameBytes)
            throws IOException {
        DataOutputStream out = new DataOutputStream(target);
        out.writeInt(frameBytes);
        writeBody(out, message);
    }

    private static void encodeToCheckedV1(
            RpcMessage message, OutputStream target, int frameBytes) throws IOException {
        DataOutputStream out = new DataOutputStream(target);
        out.writeInt(frameBytes);
        writeBody(out, message, ControlMessageId.wireId(message.type()));
    }

    public static RpcMessage decode(DataInputStream input) throws IOException {
        return decode(input, null, null);
    }

    public static RpcMessage decodeV1(
            DataInputStream input, NegotiatedProtocol negotiated) throws IOException {
        return decodeV1(input, negotiated, null, null);
    }

    static RpcMessage decodeV1(
            DataInputStream input, NegotiatedProtocol negotiated,
            ControlResidentLease lease,
            ControlResidentLease.OwnerToken owner) throws IOException {
        if (negotiated == null) {
            throw new IllegalArgumentException("negotiated protocol is required");
        }
        Header header = readHeaderV1(input, DEFAULT_NORMAL_FRAME_BYTES,
                DEFAULT_LARGE_FRAME_BYTES, negotiated);
        if (header == null) return null;
        LimitedInputStream limited = new LimitedInputStream(
                input, header.length - Integer.BYTES);
        RpcMessage decoded = decodeBody(new DataInputStream(limited), limited,
                header.type, header.limit, lease, owner);
        if (limited.remaining() != 0) {
            throw new IOException("RPC frame has unknown trailing bytes");
        }
        return decoded;
    }

    static RpcMessage decode(DataInputStream input, ControlResidentLease lease,
                             ControlResidentLease.OwnerToken owner) throws IOException {
        Header header = readHeader(input, DEFAULT_NORMAL_FRAME_BYTES, DEFAULT_LARGE_FRAME_BYTES, null);
        if (header == null) return null;
        LimitedInputStream limited = new LimitedInputStream(input, header.length - Integer.BYTES);
        RpcMessage decoded = decodeBody(new DataInputStream(limited), limited, header.type,
                header.limit, lease, owner);
        if (limited.remaining() != 0) throw new IOException("RPC frame has unknown trailing bytes");
        return decoded;
    }

    static InboundFrame decodeRequest(DataInputStream input, RpcAdmissionConfig admission,
                                      RpcFrameBudget budget, RpcRequestPolicy policy) throws IOException {
        return decodeRequest(input, admission, budget, policy, ControlResidentBudget.processWide());
    }

    static InboundFrame decodeRequest(DataInputStream input, RpcAdmissionConfig admission,
                                      RpcFrameBudget budget, RpcRequestPolicy policy,
                                      ControlResidentBudget residentBudget) throws IOException {
        return decodeRequest(input, admission, budget, policy, residentBudget, null);
    }

    static InboundFrame decodeRequest(DataInputStream input, RpcAdmissionConfig admission,
                                      RpcFrameBudget budget, RpcRequestPolicy policy,
                                      ControlResidentBudget residentBudget,
                                      java.util.EnumSet<ControlRole> peerRoles) throws IOException {
        Header header = readHeader(input, admission.normalFrameBytes(), admission.largeFrameBytes(),
                policy, peerRoles);
        if (header == null) return null;
        RpcFrameBudget.Reservation reservation = budget.reserve(header.length);
        ControlResidentLease lease = null;
        ControlResidentLease.OwnerToken decodeOwner = null;
        try {
            lease = residentBudget.openLease();
            decodeOwner = lease.ownerToken();
            LimitedInputStream limited = new LimitedInputStream(input, header.length - Integer.BYTES);
            RpcMessage decoded = decodeBody(new DataInputStream(limited), limited, header.type,
                    header.limit, lease, decodeOwner);
            if (limited.remaining() != 0) throw new IOException("RPC frame has unknown trailing bytes");
            ControlResidentLease.OwnerToken handlerOwner = lease.transfer(
                    decodeOwner, ControlResidentOwner.HANDLER_OWNED);
            decoded.attachResidentLease(lease, handlerOwner);
            return new InboundFrame(decoded, reservation, lease, handlerOwner);
        } catch (IOException | RuntimeException | Error failure) {
            if (lease != null) lease.release(lease.ownerToken());
            reservation.close();
            throw failure;
        }
    }

    static InboundFrame decodeRequestV1(
            DataInputStream input, RpcAdmissionConfig admission,
            RpcFrameBudget budget, RpcRequestPolicy policy,
            ControlResidentBudget residentBudget,
            java.util.EnumSet<ControlRole> peerRoles,
            NegotiatedProtocol negotiated) throws IOException {
        Header header = readHeaderV1(input, admission.normalFrameBytes(),
                admission.largeFrameBytes(), negotiated);
        if (header == null) return null;
        policy.requireAllowed(header.type);
        if (peerRoles != null) {
            ControlRequestAuthorization.requireAllowed(
                    policy, peerRoles, header.type);
        }
        RpcFrameBudget.Reservation reservation = budget.reserve(header.length);
        ControlResidentLease lease = null;
        ControlResidentLease.OwnerToken decodeOwner = null;
        try {
            lease = residentBudget.openLease(negotiated.maximumResidentBytes());
            decodeOwner = lease.ownerToken();
            LimitedInputStream limited = new LimitedInputStream(
                    input, header.length - Integer.BYTES);
            RpcMessage decoded = decodeBody(new DataInputStream(limited), limited,
                    header.type, header.limit, lease, decodeOwner);
            if (limited.remaining() != 0) {
                throw new IOException("RPC frame has unknown trailing bytes");
            }
            ControlResidentLease.OwnerToken handlerOwner = lease.transfer(
                    decodeOwner, ControlResidentOwner.HANDLER_OWNED);
            decoded.attachResidentLease(lease, handlerOwner);
            return new InboundFrame(decoded, reservation, lease, handlerOwner);
        } catch (IOException | RuntimeException | Error failure) {
            if (lease != null) lease.release(lease.ownerToken());
            reservation.close();
            throw failure;
        }
    }

    private static Header readHeader(DataInputStream input, int normalLimit, int largeLimit,
                                     RpcRequestPolicy policy) throws IOException {
        return readHeader(input, normalLimit, largeLimit, policy, null);
    }

    private static Header readHeader(DataInputStream input, int normalLimit, int largeLimit,
                                     RpcRequestPolicy policy,
                                     java.util.EnumSet<ControlRole> peerRoles) throws IOException {
        int first = input.read();
        if (first < 0) return null;
        int length;
        try {
            length = (first << 24) | (input.readUnsignedByte() << 16)
                    | (input.readUnsignedByte() << 8) | input.readUnsignedByte();
        } catch (EOFException partial) {
            throw new EOFException("truncated RPC frame length header");
        }
        if (length < Integer.BYTES || length > largeLimit) throw lengthFailure(length, largeLimit);
        int ordinal;
        try { ordinal = input.readInt(); }
        catch (EOFException partial) { throw new EOFException("truncated RPC type header"); }
        RpcMessage.Type[] types = RpcMessage.Type.values();
        if (ordinal < 0 || ordinal >= types.length) throw new IOException("unknown RPC type ordinal: " + ordinal);
        RpcMessage.Type type = types[ordinal];
        if (policy != null) policy.requireAllowed(type);
        if (peerRoles != null) {
            ControlRequestAuthorization.requireAllowed(policy, peerRoles, type);
        }
        int limit = isLarge(type) ? largeLimit : normalLimit;
        if (length > limit) {
            throw new IOException("RPC frame exceeds " + type + " limit: length=" + length + " max=" + limit);
        }
        return new Header(length, type, limit);
    }

    private static Header readHeaderV1(
            DataInputStream input, int normalLimit, int largeLimit,
            NegotiatedProtocol negotiated) throws IOException {
        int first = input.read();
        if (first < 0) return null;
        int length;
        try {
            length = (first << 24) | (input.readUnsignedByte() << 16)
                    | (input.readUnsignedByte() << 8) | input.readUnsignedByte();
        } catch (EOFException partial) {
            throw new EOFException("truncated RPC frame length header");
        }
        int negotiatedLimit = Math.min(largeLimit, negotiated.maximumFrameBytes());
        if (length < Integer.BYTES || length > negotiatedLimit) {
            throw lengthFailure(length, negotiatedLimit);
        }
        int messageId;
        try {
            messageId = input.readInt();
        } catch (EOFException partial) {
            throw new EOFException("truncated RPC message ID header");
        }
        RpcMessage.Type type;
        try {
            type = ControlMessageId.fromWireId(messageId);
            negotiated.requireMessage(type);
        } catch (IllegalArgumentException rejected) {
            throw new IOException("unnegotiated RPC message ID: " + messageId, rejected);
        }
        int limit = Math.min(isLarge(type) ? largeLimit : normalLimit,
                negotiated.maximumFrameBytes());
        if (length > limit) {
            throw new IOException("RPC frame exceeds " + type
                    + " negotiated limit: length=" + length + " max=" + limit);
        }
        return new Header(length, type, limit);
    }

    private static RpcMessage decodeBody(DataInputStream body, LimitedInputStream limited,
                                         RpcMessage.Type type, int fieldLimit,
                                         ControlResidentLease lease,
                                         ControlResidentLease.OwnerToken owner) throws IOException {
        String workerId = readString(body, limited, "workerId", MAX_STRING_BYTES, lease, owner);
        String host = readString(body, limited, "host", MAX_STRING_BYTES, lease, owner);
        int port = body.readInt();
        int slots = body.readInt();
            int planFieldLimit = type == RpcMessage.Type.RUN_EVENTS_PAGE
                ? RunExecutionEventPageCodec.MAX_FRAME_BYTES
                : type == RpcMessage.Type.QUALITY_GATE_DECISION
                ? io.rowweave.QualityGateDecisionCodec.MAX_FRAME_BYTES
                : type == RpcMessage.Type.INCREMENTAL_FRONTIER_VIEW
                ? io.rowweave.incremental.IncrementalFrontierViewCodec.MAX_FRAME_BYTES
                : type == RpcMessage.Type.SUBMIT_INCREMENTAL_RUN
                ? io.rowweave.incremental.IncrementalRunSubmissionCodec.MAX_FRAME_BYTES
                : type == RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED
                ? io.rowweave.incremental.IncrementalInitializeRequestCodec.MAX_FRAME_BYTES
                : type == RpcMessage.Type.EXECUTE_INCREMENTAL_SOURCE_ACTION
                ? io.rowweave.cluster.IncrementalSourceActionCommandCodec.MAX_FRAME_BYTES
                : type == RpcMessage.Type.INCREMENTAL_SOURCE_ACTION_RESULT
                ? io.rowweave.cluster.IncrementalSourceActionResultCodec.MAX_FRAME_BYTES
                : fieldLimit;
        byte[] plan = readBytes(body, limited, "planGraphBytes", planFieldLimit, lease, owner);
        long stageId = body.readLong();
        byte[] command = readBytes(body, limited, "stageCommandBlob", fieldLimit, lease, owner);
        boolean success = body.readBoolean();
        long rowCount = body.readLong();
        int errorLimit = type == RpcMessage.Type.RUN_CANCELLATION_RESULT
                ? 1024 : MAX_STRING_BYTES;
        String error = readString(body, limited, "errorMessage", errorLimit, lease, owner);
        long inputRows = 0L;
        long freeSlots = -1L;
        int dataPort = -1;
        String incarnation = null;
        String commitGroup = null;
        String attempt = null;
        long generation = 0L;
        byte[] commitPayload = null;
        String domainPath = null;
        String domainId = null;
        String outcome = null;
        RunAttemptFence fence = null;
        String queryRunId = null;
        EphemeralSubmitTimeoutPolicy.Budget submitTimeouts = null;
        RpcSecurityMetadata security = null;
        DataPlaneRegistration dataRegistration = null;

        if (limited.remaining() > 0) inputRows = readAtomicLong(body, limited, "inputRows");
        if (limited.remaining() > 0) freeSlots = readAtomicLong(body, limited, "freeSlots");
        if (limited.remaining() > 0) dataPort = readAtomicInt(body, limited, "dataPort");
        if (limited.remaining() > 0) incarnation = readString(body, limited, "incarnationId", MAX_STRING_BYTES, lease, owner);
        if (limited.remaining() > 0) commitGroup = readString(body, limited, "commitGroupId", MAX_STRING_BYTES, lease, owner);
        if (limited.remaining() > 0) {
            try {
                int attemptLimit = type == RpcMessage.Type.CANCEL_RUN
                        ? io.rowweave.run.RunCancellationAuthorityKernel.MAX_REQUEST_ID_BYTES
                        : MAX_STRING_BYTES;
                attempt = readString(body, limited, "attemptId", attemptLimit, lease, owner);
                generation = body.readLong();
                commitPayload = readBytes(body, limited, "commitPayload",
                        commitPayloadFieldLimit(type, fieldLimit), lease, owner);
                domainPath = readString(body, limited, "commitDomainPath", MAX_STRING_BYTES, lease, owner);
                domainId = readString(body, limited, "commitDomainId", MAX_STRING_BYTES, lease, owner);
                outcome = readString(body, limited, "commitOutcome", MAX_STRING_BYTES, lease, owner);
            } catch (EOFException partial) {
                throw new EOFException("truncated manifest field group");
            }
        }
        if (limited.remaining() > 0) {
            try {
                String runId = readString(body, limited, "runFence.runId", MAX_STRING_BYTES, lease, owner);
                String runAttempt = readString(body, limited, "runFence.attemptId", MAX_STRING_BYTES, lease, owner);
                long token = body.readLong();
                fence = decodeRunAttemptFence(runId, runAttempt, token);
            } catch (EOFException partial) {
                throw new EOFException("truncated run-fence field group");
            }
        }
        if (limited.remaining() > 0) queryRunId = readRunId(body, limited, lease, owner);
        if (limited.remaining() > 0 && supportsDataRegistration(type)
                && limited.remaining() != SECURE_SUFFIX_BYTES) {
            dataRegistration = readDataRegistrationExtension(body, limited, lease, owner);
        }
        if (limited.remaining() > 0 && isEphemeralSubmit(type)
                && limited.remaining() != SECURE_SUFFIX_BYTES) {
            submitTimeouts = readSubmitTimeoutExtension(body, limited);
        }
        if (limited.remaining() > 0) security = readSecureSuffix(body, limited, lease, owner);

        RpcMessage message = new RpcMessage(type, workerId, host, port, slots, plan, stageId, command,
                success, rowCount, error, inputRows, freeSlots, dataPort, incarnation, commitGroup,
                attempt, generation, commitPayload, domainPath, domainId, outcome, fence, queryRunId);
        if (dataRegistration != null) message = message.withDataPlaneRegistration(dataRegistration);
        if (submitTimeouts != null) message = message.withEphemeralSubmitTimeouts(submitTimeouts);
        validateUnionFields(message);
        return security == null ? message : message.withSecurityMetadata(security);
    }

    private static EphemeralSubmitTimeoutPolicy.Budget readSubmitTimeoutExtension(
            DataInputStream body, LimitedInputStream limited) throws IOException {
        int remaining = limited.remaining();
        if (remaining != SUBMIT_TIMEOUT_EXTENSION_BYTES
                && remaining != SUBMIT_TIMEOUT_EXTENSION_BYTES + SECURE_SUFFIX_BYTES) {
            throw new IOException("invalid submit timeout extension length: " + remaining);
        }
        byte[] magic = new byte[SUBMIT_TIMEOUT_MAGIC_BYTES];
        body.readFully(magic);
        if (!Arrays.equals(SUBMIT_TIMEOUT_MAGIC, magic)) {
            throw new IOException("invalid submit timeout extension magic");
        }
        int version = body.readInt();
        if (version != SUBMIT_TIMEOUT_VERSION) {
            throw new IOException("unsupported submit timeout extension version: " + version);
        }
        long executionTimeoutMs = body.readLong();
        long cleanupTimeoutMs = body.readLong();
        try {
            return EphemeralSubmitTimeoutPolicy.Budget.of(
                    executionTimeoutMs, cleanupTimeoutMs);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid submit timeout extension budget", invalid);
        }
    }

    private static DataPlaneRegistration readDataRegistrationExtension(
            DataInputStream body, LimitedInputStream limited,
            ControlResidentLease lease, ControlResidentLease.OwnerToken owner) throws IOException {
        byte[] magic = new byte[DATA_REGISTRATION_MAGIC_BYTES];
        body.readFully(magic);
        if (!Arrays.equals(DATA_REGISTRATION_MAGIC, magic)) {
            throw new IOException("invalid data registration extension magic");
        }
        int version = body.readInt();
        if (version != DATA_REGISTRATION_VERSION) {
            throw new IOException("unsupported data registration extension version: " + version);
        }
        int protocolWireId = body.readInt();
        int fingerprintLength = body.readInt();
        if (fingerprintLength != 0
                && fingerprintLength != DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES) {
            throw new IOException(
                    "data certificate fingerprint length out of bounds: " + fingerprintLength);
        }
        if (fingerprintLength > limited.remaining()) {
            throw new EOFException("truncated data certificate fingerprint");
        }
        reserve(lease, owner, fingerprintLength);
        byte[] fingerprint = fingerprintLength == 0 ? null : new byte[fingerprintLength];
        if (fingerprint != null) body.readFully(fingerprint);
        if (limited.remaining() != 0 && limited.remaining() != SECURE_SUFFIX_BYTES) {
            throw new IOException("invalid data registration extension length");
        }
        try {
            return DataPlaneRegistration.fromWire(protocolWireId, fingerprint);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid data registration extension", invalid);
        }
    }

    private static RpcSecurityMetadata readSecureSuffix(DataInputStream body, LimitedInputStream limited,
                                                        ControlResidentLease lease,
                                                        ControlResidentLease.OwnerToken owner)
            throws IOException {
        if (limited.remaining() != SECURE_SUFFIX_BYTES) {
            throw new IOException("invalid secure RPC suffix length: " + limited.remaining());
        }
        reserve(lease, owner, SECURE_MAGIC_BYTES);
        byte[] magic = new byte[SECURE_MAGIC_BYTES];
        body.readFully(magic);
        if (!Arrays.equals(SECURE_MAGIC, magic)) throw new IOException("invalid secure RPC magic");
        int version = body.readInt();
        if (version != SECURE_VERSION) throw new IOException("unsupported secure RPC version: " + version);
        String deployment = readFixedAscii(body, 64, "deploymentSetId", lease, owner);
        String authz = readFixedAscii(body, 64, "authzSetId", lease, owner);
        try { return RpcSecurityMetadata.of(deployment, authz); }
        catch (IllegalArgumentException invalid) { throw new IOException("invalid secure RPC identity", invalid); }
    }

    private static String readFixedAscii(DataInputStream body, int length, String name,
                                         ControlResidentLease lease,
                                         ControlResidentLease.OwnerToken owner) throws IOException {
        reserve(lease, owner, 3L * length);
        byte[] bytes = new byte[length];
        body.readFully(bytes);
        for (byte value : bytes) if (value < 0) throw new IOException(name + " is not ASCII");
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    private static void writeBody(DataOutputStream out, RpcMessage message) throws IOException {
        writeBody(out, message, message.type().ordinal());
    }

    private static void writeBody(
            DataOutputStream out, RpcMessage message, int typeTag) throws IOException {
        out.writeInt(typeTag);
        writeString(out, message.workerId());
        writeString(out, message.host());
        out.writeInt(message.port());
        out.writeInt(message.slots());
        writeBytes(out, message.planGraphBytes());
        out.writeLong(message.stageId());
        writeBytes(out, message.stageCommandBlob());
        out.writeBoolean(message.success());
        out.writeLong(message.rowCount());
        writeString(out, message.errorMessage());
        out.writeLong(message.inputRows());
        out.writeLong(message.freeSlots());
        out.writeInt(message.dataPort());
        writeString(out, message.incarnationId());
        writeString(out, message.commitGroupId());
        writeString(out, message.attemptId());
        out.writeLong(message.commitGeneration());
        writeBytes(out, message.borrowedCommitPayload());
        writeString(out, message.commitDomainPath());
        writeString(out, message.commitDomainId());
        writeString(out, message.commitOutcome());
        RunAttemptFence fence = message.runAttemptFence();
        writeString(out, fence == null ? null : fence.runId());
        writeString(out, fence == null ? null : fence.attemptId());
        out.writeLong(fence == null ? 0L : fence.fenceToken());
        writeString(out, message.runId());
        DataPlaneRegistration dataRegistration = message.dataPlaneRegistration();
        if (dataRegistration != null) {
            out.write(DATA_REGISTRATION_MAGIC);
            out.writeInt(DATA_REGISTRATION_VERSION);
            out.writeInt(dataRegistration.protocol().wireId());
            out.writeInt(dataRegistration.fingerprintLength());
            dataRegistration.writeFingerprint(out);
        }
        EphemeralSubmitTimeoutPolicy.Budget submitTimeouts =
                message.ephemeralSubmitTimeouts();
        if (submitTimeouts != null) {
            out.write(SUBMIT_TIMEOUT_MAGIC);
            out.writeInt(SUBMIT_TIMEOUT_VERSION);
            out.writeLong(submitTimeouts.executionTimeoutMs());
            out.writeLong(submitTimeouts.cleanupTimeoutMs());
        }
        RpcSecurityMetadata security = message.securityMetadata();
        if (security != null) {
            out.write(SECURE_MAGIC);
            out.writeInt(SECURE_VERSION);
            out.write(security.deploymentSetId().getBytes(StandardCharsets.US_ASCII));
            out.write(security.authzSetId().getBytes(StandardCharsets.US_ASCII));
        }
    }

    private static void requireNegotiatedMessage(
            RpcMessage message, NegotiatedProtocol negotiated) {
        if (message == null || negotiated == null) {
            throw new IllegalArgumentException(
                    "message and negotiated protocol are required");
        }
        negotiated.requireMessage(message.type());
    }

    private static int checkedFrameBytes(RpcMessage message, int normalLimit, int largeLimit) {
        if (message == null || message.type() == null) throw new IllegalArgumentException("RPC message/type required");
        validateUnionFields(message);
        long size = 4L + 4L + 4L + 8L + 1L + 8L + 8L + 8L + 4L + 8L + 8L;
        size += stringSize(message.workerId()) + stringSize(message.host())
                + bytesSize(message.planGraphBytes()) + bytesSize(message.stageCommandBlob())
                + stringSize(message.errorMessage()) + stringSize(message.incarnationId())
                + stringSize(message.commitGroupId()) + stringSize(message.attemptId())
                + bytesSize(message.borrowedCommitPayload()) + stringSize(message.commitDomainPath())
                + stringSize(message.commitDomainId()) + stringSize(message.commitOutcome())
                + stringSize(message.runAttemptFence() == null ? null : message.runAttemptFence().runId())
                + stringSize(message.runAttemptFence() == null ? null : message.runAttemptFence().attemptId())
                + stringSize(message.runId());
        if (message.ephemeralSubmitTimeouts() != null) {
            size += SUBMIT_TIMEOUT_EXTENSION_BYTES;
        }
        DataPlaneRegistration dataRegistration = message.dataPlaneRegistration();
        if (dataRegistration != null) {
            size += DATA_REGISTRATION_LOOPBACK_BYTES
                    + dataRegistration.fingerprintLength();
        }
        if (message.securityMetadata() != null) size += SECURE_SUFFIX_BYTES;
        int limit = isLarge(message.type()) ? largeLimit : normalLimit;
        if (size > limit || size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("RPC " + message.type() + " frame exceeds limit: bytes="
                    + size + " max=" + limit);
        }
        return (int) size;
    }

    private static long stringSize(String value) {
        if (value == null) return Integer.BYTES;
        byte[] bytes = strictUtf8(value);
        if (bytes.length > MAX_STRING_BYTES) throw new IllegalArgumentException("RPC string exceeds 64 KiB");
        return Integer.BYTES + (long) bytes.length;
    }

    private static long bytesSize(byte[] value) { return Integer.BYTES + (value == null ? 0L : value.length); }

    private static void validateUnionFields(RpcMessage message) {
        RpcMessage.Type type = message.type();
        if (message.ephemeralSubmitTimeouts() != null && !isEphemeralSubmit(type)) {
            throw new IllegalArgumentException(
                    "RPC " + type + " cannot carry ephemeral submit timeouts");
        }
        if (message.dataPlaneRegistration() != null && !supportsDataRegistration(type)) {
            throw new IllegalArgumentException(
                    "RPC " + type + " cannot carry data registration");
        }
        if (type == RpcMessage.Type.WORKER_REGISTERED
                && (message.dataPlaneRegistration() == null
                || message.incarnationId() == null)) {
            throw new IllegalArgumentException(
                    "WORKER_REGISTERED requires incarnation and data registration");
        }
        if (message.planGraphBytes() != null && type != RpcMessage.Type.SUBMIT_JOB
                && type != RpcMessage.Type.SUBMIT_PLAN_ARTIFACT && type != RpcMessage.Type.SUBMIT_RUN
                && type != RpcMessage.Type.RUN_SNAPSHOT && type != RpcMessage.Type.ASSIGNMENT_ACCEPTED
                && type != RpcMessage.Type.STAGE_REPORT
                && type != RpcMessage.Type.RECONCILE_DURABLE_STAGE
                && type != RpcMessage.Type.DURABLE_STAGE_RECONCILED
                && type != RpcMessage.Type.RUN_EVENTS_PAGE
                && type != RpcMessage.Type.QUALITY_GATE_DECISION
                && type != RpcMessage.Type.INCREMENTAL_FRONTIER_VIEW
                && type != RpcMessage.Type.SUBMIT_INCREMENTAL_RUN
                && type != RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED
                && type != RpcMessage.Type.EXECUTE_INCREMENTAL_SOURCE_ACTION
                && type != RpcMessage.Type.INCREMENTAL_SOURCE_ACTION_RESULT) {
            throw new IllegalArgumentException("RPC " + type + " cannot carry planGraphBytes");
        }
        if (message.stageCommandBlob() != null
                && type != RpcMessage.Type.ASSIGN_STAGE
                && type != RpcMessage.Type.STAGE_REPORT
                && type != RpcMessage.Type.DURABLE_STAGE_RECONCILED) {
            throw new IllegalArgumentException("RPC " + type + " cannot carry stageCommandBlob");
        }
        if ((type == RpcMessage.Type.STAGE_REPORT
                || type == RpcMessage.Type.DURABLE_STAGE_RECONCILED)
                && message.stageCommandBlob() != null
                && message.stageCommandBlob().length
                > io.rowweave.run.PreparedSinkReceiptCodec.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "RPC STAGE_REPORT prepared Sink receipt exceeds limit");
        }
        if (message.borrowedCommitPayload() != null && !supportsCommitPayload(type)) {
            throw new IllegalArgumentException("RPC " + type + " cannot carry commitPayload");
        }
        if (message.borrowedCommitPayload() != null
                && RpcMessage.supportsExecutionTelemetry(type)) {
            int max = type == RpcMessage.Type.STAGE_REPORT
                    || type == RpcMessage.Type.DURABLE_STAGE_RECONCILED
                    ? io.rowweave.plan.StageExecutionTelemetryCodec.MAX_FRAME_BYTES
                    : io.rowweave.plan.ExecutionTelemetrySummaryCodec.MAX_FRAME_BYTES;
            if (message.borrowedCommitPayload().length > max) {
                throw new IllegalArgumentException(
                        "RPC " + type + " execution telemetry exceeds limit");
            }
        }
        if (type == RpcMessage.Type.QUERY_RUN_EVENTS
                && (message.runId() == null || message.eventAfterSequence() < 0L
                || message.eventLimit() < 1 || message.eventLimit() > 256)) {
            throw new IllegalArgumentException("invalid QUERY_RUN_EVENTS fields");
        }
        if (type == RpcMessage.Type.RUN_EVENTS_PAGE) {
            byte[] page = message.runEventPageBytes();
            if (message.runId() == null
                    || (message.success() && (page == null || page.length == 0
                    || page.length > RunExecutionEventPageCodec.MAX_FRAME_BYTES
                    || message.errorMessage() != null))
                    || (!message.success() && (page != null || message.errorMessage() == null
                    || message.errorMessage().isEmpty()))) {
                throw new IllegalArgumentException("invalid RUN_EVENTS_PAGE fields");
            }
        }
        if (type == RpcMessage.Type.QUERY_QUALITY_GATE_DECISION
                && message.runId() == null) {
            throw new IllegalArgumentException(
                    "invalid QUERY_QUALITY_GATE_DECISION fields");
        }
        if (type == RpcMessage.Type.QUALITY_GATE_DECISION) {
            byte[] decision = message.qualityGateDecisionBytes();
            if (message.runId() == null
                    || (message.success() && (decision == null
                    || decision.length == 0
                    || decision.length
                    > io.rowweave.QualityGateDecisionCodec.MAX_FRAME_BYTES
                    || message.errorMessage() != null))
                    || (!message.success() && (decision != null
                    || (message.errorMessage() != null
                    && message.errorMessage().isEmpty())))) {
                throw new IllegalArgumentException(
                        "invalid QUALITY_GATE_DECISION fields");
            }
        }
        if (type == RpcMessage.Type.QUERY_INCREMENTAL_FRONTIER
                && message.runId() == null) {
            throw new IllegalArgumentException(
                    "invalid QUERY_INCREMENTAL_FRONTIER fields");
        }
        if (type == RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED
                && (message.runId() == null || message.attemptId() == null
                || message.planGraphBytes() == null
                || message.planGraphBytes().length == 0
                || message.planGraphBytes().length
                > io.rowweave.incremental.IncrementalInitializeRequestCodec
                .MAX_FRAME_BYTES)) {
            throw new IllegalArgumentException(
                    "invalid INITIALIZE_INCREMENTAL_FEED fields");
        }
        if (type == RpcMessage.Type.SUBMIT_INCREMENTAL_RUN
                && (message.planGraphBytes() == null
                || message.planGraphBytes().length == 0
                || message.planGraphBytes().length
                > io.rowweave.incremental.IncrementalRunSubmissionCodec
                .MAX_FRAME_BYTES)) {
            throw new IllegalArgumentException(
                    "invalid SUBMIT_INCREMENTAL_RUN fields");
        }
        if (type == RpcMessage.Type.EXECUTE_INCREMENTAL_SOURCE_ACTION
                && (message.workerId() == null
                || message.incarnationId() == null
                || message.host() == null || message.port() <= 0
                || message.planGraphBytes() == null
                || message.planGraphBytes().length == 0
                || message.planGraphBytes().length
                > io.rowweave.cluster.IncrementalSourceActionCommandCodec
                .MAX_FRAME_BYTES)) {
            throw new IllegalArgumentException(
                    "invalid EXECUTE_INCREMENTAL_SOURCE_ACTION fields");
        }
        if (type == RpcMessage.Type.INCREMENTAL_SOURCE_ACTION_RESULT
                && (!message.success() || message.planGraphBytes() == null
                || message.planGraphBytes().length == 0
                || message.planGraphBytes().length
                > io.rowweave.cluster.IncrementalSourceActionResultCodec
                .MAX_FRAME_BYTES)) {
            throw new IllegalArgumentException(
                    "invalid INCREMENTAL_SOURCE_ACTION_RESULT fields");
        }
        if ((type == RpcMessage.Type.INSTALL_INCREMENTAL_CONTROL_MEMBER
                || type == RpcMessage.Type.PUBLISH_INCREMENTAL_MANIFEST)
                && (message.workerId() == null
                || message.incarnationId() == null
                || message.runAttemptFence() == null
                || message.commitGroupId() == null
                || message.attemptId() == null
                || message.commitGeneration() <= 0L
                || message.borrowedCommitPayload() == null
                || message.borrowedCommitPayload().length == 0
                || message.borrowedCommitPayload().length
                > io.rowweave.cluster.IncrementalControlMemberInstallCodec
                .MAX_FRAME_BYTES)) {
            throw new IllegalArgumentException(
                    "invalid incremental Manifest control fields");
        }
        if (type == RpcMessage.Type.INCREMENTAL_FRONTIER_VIEW) {
            byte[] view = message.incrementalFrontierViewBytes();
            if (message.runId() == null
                    || (message.success() && (view == null || view.length == 0
                    || view.length
                    > io.rowweave.incremental.IncrementalFrontierViewCodec.MAX_FRAME_BYTES
                    || message.errorMessage() != null))
                    || (!message.success() && (view != null
                    || message.errorMessage() != null
                    && message.errorMessage().isEmpty()))) {
                throw new IllegalArgumentException(
                        "invalid INCREMENTAL_FRONTIER_VIEW fields");
            }
        }
        if (type == RpcMessage.Type.CANCEL_RUN) {
            io.rowweave.run.RunCancellationAuthorityKernel.validateRunId(message.runId());
            io.rowweave.run.RunCancellationAuthorityKernel.validateRequestId(
                    message.cancelRequestId());
            if (message.success() || message.rowCount() != 0L
                    || message.errorMessage() != null
                    || message.commitOutcome() != null
                    || message.commitDomainId() != null) {
                throw new IllegalArgumentException("invalid CANCEL_RUN fields");
            }
        }
        if (type == RpcMessage.Type.RUN_CANCELLATION_RESULT) {
            io.rowweave.run.RunCancellationAuthorityKernel.validateRunId(message.runId());
            if (message.success()) {
                if (message.errorMessage() != null) {
                    throw new IllegalArgumentException(
                            "successful cancellation result cannot carry an error");
                }
                final io.rowweave.run.RunCancellationResult.Disposition disposition;
                final io.rowweave.run.RunStatus status;
                try {
                    disposition = io.rowweave.run.RunCancellationResult.Disposition.valueOf(
                            message.runCancellationDisposition());
                    status = message.runCancellationStatus() == null ? null
                            : io.rowweave.run.RunStatus.valueOf(
                            message.runCancellationStatus());
                } catch (RuntimeException malformed) {
                    throw new IllegalArgumentException(
                            "invalid RUN_CANCELLATION_RESULT enum", malformed);
                }
                new io.rowweave.run.RunCancellationResult(message.runId(), disposition,
                        status, message.runCancellationRevision());
            } else if (message.errorMessage() == null
                    || message.errorMessage().isEmpty()
                    || message.runCancellationDisposition() != null
                    || message.runCancellationStatus() != null
                    || message.runCancellationRevision() != 0L) {
                throw new IllegalArgumentException(
                        "invalid failed RUN_CANCELLATION_RESULT fields");
            }
        }
    }

    private static boolean isLarge(RpcMessage.Type type) {
        return type == RpcMessage.Type.SUBMIT_JOB || type == RpcMessage.Type.SUBMIT_PLAN_ARTIFACT
                || type == RpcMessage.Type.SUBMIT_RUN || type == RpcMessage.Type.ASSIGN_STAGE
                || type == RpcMessage.Type.RUN_SNAPSHOT
                || type == RpcMessage.Type.EXECUTE_PROVIDER_GROUP
                || type == RpcMessage.Type.SUBMIT_INCREMENTAL_RUN
                || type == RpcMessage.Type.EXECUTE_INCREMENTAL_SOURCE_ACTION
                || type == RpcMessage.Type.INCREMENTAL_SOURCE_ACTION_RESULT
                || type == RpcMessage.Type.INSTALL_INCREMENTAL_CONTROL_MEMBER
                || type == RpcMessage.Type.PUBLISH_INCREMENTAL_MANIFEST;
    }

    private static boolean supportsCommitPayload(RpcMessage.Type type) {
        return type == RpcMessage.Type.ASSIGN_STAGE
                || type == RpcMessage.Type.ASSIGNMENT_ACCEPTED
                || type == RpcMessage.Type.RECONCILE_DURABLE_STAGE
                || type == RpcMessage.Type.MANIFEST_MEMBER_INSTALLED
                || type == RpcMessage.Type.PUBLISH_MANIFEST_GROUP
                || type == RpcMessage.Type.MANIFEST_OUTCOME
                || type == RpcMessage.Type.BATCH_SINK_OUTCOME
                || type == RpcMessage.Type.EXECUTE_PROVIDER_GROUP
                || type == RpcMessage.Type.PROVIDER_GROUP_RESULT
                || type == RpcMessage.Type.INSTALL_INCREMENTAL_CONTROL_MEMBER
                || type == RpcMessage.Type.PUBLISH_INCREMENTAL_MANIFEST
                || RpcMessage.supportsExecutionTelemetry(type);
    }

    private static int commitPayloadFieldLimit(RpcMessage.Type type, int frameFieldLimit) {
        if (type == RpcMessage.Type.STAGE_REPORT
                || type == RpcMessage.Type.DURABLE_STAGE_RECONCILED) {
            return Math.min(frameFieldLimit,
                    io.rowweave.plan.StageExecutionTelemetryCodec.MAX_FRAME_BYTES);
        }
        if (type == RpcMessage.Type.JOB_RESULT || type == RpcMessage.Type.RUN_SNAPSHOT) {
            return Math.min(frameFieldLimit,
                    io.rowweave.plan.ExecutionTelemetrySummaryCodec.MAX_FRAME_BYTES);
        }
        return frameFieldLimit;
    }

    private static boolean isEphemeralSubmit(RpcMessage.Type type) {
        return type == RpcMessage.Type.SUBMIT_JOB
                || type == RpcMessage.Type.SUBMIT_PLAN_ARTIFACT;
    }

    private static boolean supportsDataRegistration(RpcMessage.Type type) {
        return type == RpcMessage.Type.REGISTER_WORKER
                || type == RpcMessage.Type.WORKER_REGISTERED;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        if (value == null) { out.writeInt(-1); return; }
        byte[] bytes = strictUtf8(value);
        if (bytes.length > MAX_STRING_BYTES) throw new IllegalArgumentException("RPC string exceeds 64 KiB");
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value == null) { out.writeInt(-1); return; }
        out.writeInt(value.length);
        out.write(value);
    }

    private static String readString(DataInputStream in, LimitedInputStream limited,
                                     String field, int max) throws IOException {
        return readString(in, limited, field, max, null, null);
    }

    private static String readString(DataInputStream in, LimitedInputStream limited,
                                     String field, int max, ControlResidentLease lease,
                                     ControlResidentLease.OwnerToken owner) throws IOException {
        int length = in.readInt();
        if (length == -1) return null;
        if (length < 0 || length > max || length > limited.remaining()) {
            throw new IOException(field + " length out of bounds: " + length);
        }
        // raw UTF-8 array + conservative UTF-16/materializer charge coexist for request lifetime.
        reserve(lease, owner, 3L * length);
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw new IOException(field + " contains malformed UTF-8", malformed);
        }
    }

    private static byte[] readBytes(DataInputStream in, LimitedInputStream limited,
                                    String field, int max) throws IOException {
        return readBytes(in, limited, field, max, null, null);
    }

    private static byte[] readBytes(DataInputStream in, LimitedInputStream limited,
                                    String field, int max, ControlResidentLease lease,
                                    ControlResidentLease.OwnerToken owner) throws IOException {
        int length = in.readInt();
        if (length == -1) return null;
        if (length < 0 || length > max || length > limited.remaining()) {
            throw new IOException(field + " length out of bounds: " + length);
        }
        reserve(lease, owner, length);
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    private static String readRunId(DataInputStream in, LimitedInputStream limited,
                                    ControlResidentLease lease,
                                    ControlResidentLease.OwnerToken owner) throws IOException {
        String value = readString(in, limited, "query runId", 256, lease, owner);
        if (value == null) return null;
        try { return new RunAttemptFence(value, "query", 1L).runId(); }
        catch (IllegalArgumentException invalid) { throw new IOException("invalid query runId", invalid); }
    }

    private static long readAtomicLong(DataInputStream in, LimitedInputStream limited, String name)
            throws IOException {
        if (limited.remaining() < Long.BYTES) throw new EOFException("truncated " + name + " field group");
        return in.readLong();
    }

    private static void reserve(ControlResidentLease lease,
                                ControlResidentLease.OwnerToken owner, long bytes) {
        if (lease != null && bytes != 0L) lease.reserve(owner, bytes);
    }

    private static int readAtomicInt(DataInputStream in, LimitedInputStream limited, String name)
            throws IOException {
        if (limited.remaining() < Integer.BYTES) throw new EOFException("truncated " + name + " field group");
        return in.readInt();
    }

    private static RunAttemptFence decodeRunAttemptFence(String runId, String attemptId, long token)
            throws IOException {
        if (runId == null && attemptId == null && token == 0L) return null;
        try { return new RunAttemptFence(runId, attemptId, token); }
        catch (IllegalArgumentException invalid) { throw new IOException("invalid run attempt fence", invalid); }
    }

    private static byte[] strictUtf8(String value) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("RPC string is not strict UTF-8", malformed);
        }
    }

    private static IOException lengthFailure(int length, int max) {
        return new IOException("rpc frame length out of range: length=" + length
                + " hex=" + String.format(java.util.Locale.ROOT, "0x%08X", length)
                + " ASCII=" + printableAscii(length) + " max=" + max);
    }

    private static String printableAscii(int value) {
        char[] chars = new char[Integer.BYTES];
        for (int i = 0; i < chars.length; i++) {
            int shift = (chars.length - 1 - i) * Byte.SIZE;
            int b = (value >>> shift) & 0xFF;
            chars[i] = b >= 0x20 && b <= 0x7E ? (char) b : '.';
        }
        return new String(chars);
    }

    static final class InboundFrame implements AutoCloseable {
        private final RpcMessage message;
        private final RpcFrameBudget.Reservation reservation;
        private final ControlResidentLease residentLease;
        private final ControlResidentLease.OwnerToken handlerOwner;
        InboundFrame(RpcMessage message, RpcFrameBudget.Reservation reservation,
                     ControlResidentLease residentLease,
                     ControlResidentLease.OwnerToken handlerOwner) {
            this.message = message;
            this.reservation = reservation;
            this.residentLease = residentLease;
            this.handlerOwner = handlerOwner;
        }
        RpcMessage message() { return message; }
        @Override public void close() {
            residentLease.release(handlerOwner);
            reservation.close();
        }
    }

    private static final class Header {
        final int length; final RpcMessage.Type type; final int limit;
        Header(int length, RpcMessage.Type type, int limit) {
            this.length = length; this.type = type; this.limit = limit;
        }
    }

    private static final class LimitedInputStream extends InputStream {
        private final InputStream source;
        private int remaining;
        LimitedInputStream(InputStream source, int remaining) { this.source = source; this.remaining = remaining; }
        int remaining() { return remaining; }
        @Override public int read() throws IOException {
            if (remaining == 0) return -1;
            int value = source.read();
            if (value < 0) throw new EOFException("truncated RPC frame body");
            remaining--;
            return value;
        }
        @Override public int read(byte[] bytes, int offset, int length) throws IOException {
            if (remaining == 0) return -1;
            int count = source.read(bytes, offset, Math.min(length, remaining));
            if (count < 0) throw new EOFException("truncated RPC frame body");
            remaining -= count;
            return count;
        }
        @Override public int available() { return remaining; }
    }

    private static final class FixedOutputStream extends OutputStream {
        private final byte[] target;
        private int position;
        FixedOutputStream(byte[] target) { this.target = target; }
        @Override public void write(int value) throws IOException {
            if (position >= target.length) throw new EOFException("RPC encoder size mismatch");
            target[position++] = (byte) value;
        }
        @Override public void write(byte[] bytes, int offset, int length) throws IOException {
            if (length > target.length - position) throw new EOFException("RPC encoder size mismatch");
            System.arraycopy(bytes, offset, target, position, length);
            position += length;
        }
    }
}
