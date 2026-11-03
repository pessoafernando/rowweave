package io.rowweave.cluster;

import io.rowweave.CoGroupFunction;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.Sink;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.memory.ResidentByteArrayOutputStream;
import io.rowweave.ops.join.JoinPhysicalStrategy;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanGraphEnvelopeScanner;
import io.rowweave.plan.TrustedObjectCodec;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.net.ShuffleAuthorizationRecord;
import io.rowweave.sched.net.ShuffleInboundAuthorization;
import io.rowweave.sched.net.ShuffleProducerGrant;
import io.rowweave.sched.net.WorkerDataIdentity;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.rpc.DataPlaneRegistration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.LongConsumer;

/** Explicit StageCommand wire envelope; the top level is never Java deserialized. */
public final class StageCommandCodecV2 {
    private static final byte[] MAGIC = new byte[]{'R', 'W', 'X', 'X', 'V', 'S', 'C', '2'};
    static final int FIXED_ROUTING_VERSION = 9;
    private static final int WRITE_VERSION = FIXED_ROUTING_VERSION;
    private static final int PORTABLE_STAGE_WRITE_VERSION = 8;
    private static final int BASELINE_WRITE_VERSION = 7;
    private static final int MIN_READ_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_ENVELOPE_BYTES = 80 * 1024 * 1024;
    private static final int MAX_ROUTING_BYTES = 1024 * 1024;
    private static final int MAX_COLLECTION = 4096;
    private static final int MAX_UTF8_BYTES = 64 * 1024;
    private static final StageCommand.Role[] ROLES = StageCommand.Role.values();
    private static final DataType[] DATA_TYPES = DataType.values();
    static final int OUTPUT_ENCODING_LEGACY_OIS = 0;
    static final int OUTPUT_ENCODING_PROVIDER_NATIVE = 1;

    private StageCommandCodecV2() {
    }

    public static boolean hasMagic(byte[] bytes) {
        if (bytes == null || bytes.length < MAGIC.length) return false;
        for (int i = 0; i < MAGIC.length; i++) {
            if (bytes[i] != MAGIC[i]) return false;
        }
        return true;
    }

    public static byte[] encode(StageCommand command, ObjectTrustContext trustContext,
                                LongConsumer reservation) {
        if (command == null || trustContext == null || reservation == null) {
            throw new IllegalArgumentException("command, trust context and reservation are required");
        }
        StageCommandFieldInventory.assertComplete();
        validateRole(command.role(), command.window(), command.dataSchema(), command.input(),
                command.output(), command.fragment(), command.sinkFragment(), command.coGroupFn(),
                command.portableArtifactBytesUnsafe(), command.portableStageSpec(),
                command.joinType(),
                command.rawJoinStrategy());
        validateSourceAssignment(command.role(), command.sourceSplitAssignment());
        validateShuffleAuthorizationBinding(
                command.runAttemptFence(), command.inboundTargetId(),
                command.inboundTargetIdRight(), command.inboundAuthorizations(),
                command.downstream(), command.sinkEndpoint(),
                command.downTargets(), command.broadcastGroups());

        byte[] fragment = writePlan(command.fragment(), trustContext, reservation);
        byte[] sinkFragment = writePlan(command.sinkFragment(), trustContext, reservation);
        byte[] input = writeObject(command.input(), Source.class, trustContext, reservation);
        byte[] output = writeOutput(command.output(), trustContext, reservation);
        byte[] coGroup = writeObject(command.coGroupFn(), CoGroupFunction.class,
                trustContext, reservation);

        ResidentByteArrayOutputStream body = new ResidentByteArrayOutputStream(
                reservation, 1024, MAX_ENVELOPE_BYTES - DIGEST_BYTES);
        try (DataOutputStream out = new DataOutputStream(body)) {
            out.write(MAGIC);
            int commandSchemaVersion = wireVersion(command);
            out.writeInt(commandSchemaVersion);
            writeRouting(out, command, reservation,
                    commandSchemaVersion);
            writeSlice(out, fragment);
            writeSlice(out, sinkFragment);
            writeSlice(out, input);
            writeSlice(out, output);
            writeSlice(out, coGroup);
            writeSlice(out, command.portableArtifactBytesUnsafe());
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
        byte[] prefix = body.toByteArray();
        if (prefix.length > MAX_ENVELOPE_BYTES - DIGEST_BYTES) {
            throw new IllegalArgumentException("stage command envelope exceeds limit");
        }
        byte[] digest = sha256(prefix, 0, prefix.length);
        reservation.accept((long) prefix.length + DIGEST_BYTES);
        byte[] encoded = Arrays.copyOf(prefix, prefix.length + DIGEST_BYTES);
        System.arraycopy(digest, 0, encoded, prefix.length, DIGEST_BYTES);
        return encoded;
    }

    public static PreparedStageCommand preflight(byte[] envelope, LongConsumer reservation) {
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        if (envelope == null || envelope.length < MAGIC.length + Integer.BYTES + DIGEST_BYTES
                || envelope.length > MAX_ENVELOPE_BYTES) {
            throw new IllegalArgumentException("stage command envelope length out of bounds");
        }
        StageCommandFieldInventory.assertComplete();
        int bodyLength = envelope.length - DIGEST_BYTES;
        verifyDigest(envelope, bodyLength);
        Cursor in = new Cursor(envelope, bodyLength, reservation);
        in.expect(MAGIC, "stage command magic");
        int commandSchemaVersion = in.readInt("stage command version");
        if (commandSchemaVersion < MIN_READ_VERSION || commandSchemaVersion > WRITE_VERSION) {
            throw new IllegalArgumentException("unsupported stage command version");
        }
        int routingStart = in.position();
        in.beginRouting(MAX_ROUTING_BYTES);
        StageCommand.WireFields fields = readRouting(in, commandSchemaVersion);
        in.endRouting();
        if (in.position() - routingStart > MAX_ROUTING_BYTES) {
            throw new IllegalArgumentException("stage command routing metadata exceeds limit");
        }
        PreparedStageCommand.Slice fragment = in.slice(
                "fragment", PlanGraphEnvelopeScanner.MAX_PLAN_BYTES);
        PreparedStageCommand.Slice sinkFragment = in.slice(
                "sink fragment", PlanGraphEnvelopeScanner.MAX_PLAN_BYTES);
        PreparedStageCommand.Slice input = in.slice(
                "input object", PlanGraphEnvelopeScanner.MAX_OBJECT_BLOB_BYTES);
        int outputMaximum = fields.outputEncoding == OUTPUT_ENCODING_PROVIDER_NATIVE
                ? ProviderSinkStageEnvelopeCodec.MAX_FRAME_BYTES
                : PlanGraphEnvelopeScanner.MAX_OBJECT_BLOB_BYTES;
        PreparedStageCommand.Slice output = in.slice(
                "output object", outputMaximum);
        fields.outputEncoding = output != null
                && ProviderSinkStageEnvelopeCodec.hasMagic(
                envelope, output.offset, output.length)
                ? OUTPUT_ENCODING_PROVIDER_NATIVE
                : OUTPUT_ENCODING_LEGACY_OIS;
        PreparedStageCommand.Slice coGroup = in.slice(
                "coGroup object", PlanGraphEnvelopeScanner.MAX_OBJECT_BLOB_BYTES);
        PreparedStageCommand.Slice portableArtifact = commandSchemaVersion >= 7
                ? in.slice("portable artifact", PlanArtifactCodec.MAX_FRAME_BYTES)
                : null;
        if (in.position() != bodyLength) {
            throw new IllegalArgumentException("stage command envelope has trailing body bytes");
        }
        scanPlan(envelope, fragment, reservation);
        scanPlan(envelope, sinkFragment, reservation);
        scanObjectHeader(envelope, input, "input object");
        if (fields.outputEncoding == OUTPUT_ENCODING_PROVIDER_NATIVE) {
            if (output == null) {
                throw new IllegalArgumentException(
                        "native provider output is missing");
            }
            ProviderSinkStageEnvelopeCodec.decode(
                    envelope, output.offset, output.length, reservation);
        } else {
            scanObjectHeader(envelope, output, "output object");
        }
        scanObjectHeader(envelope, coGroup, "coGroup object");
        validateRole(fields.role, fields.window, fields.dataSchema,
                input, output, fragment, sinkFragment, coGroup,
                portableArtifact, fields.portableStageSpec,
                fields.joinType, fields.joinStrategy);
        validateSourceAssignment(fields.role, fields.sourceSplitAssignment);
        validateShuffleAuthorizationBinding(
                fields.runAttemptFence, fields.inboundTargetId,
                fields.inboundTargetIdRight, fields.inboundAuthorizations,
                fields.downstream, fields.sinkEndpoint,
                fields.downTargets, fields.broadcastGroups);
        reservation.accept(512L);
        return new PreparedStageCommand(envelope, bodyLength, fields, fragment,
                sinkFragment, input, output, coGroup, portableArtifact);
    }

    static void verifyDigest(byte[] envelope, int bodyLength) {
        if (envelope == null || bodyLength < 0 || envelope.length != bodyLength + DIGEST_BYTES) {
            throw new IllegalArgumentException("invalid stage command digest boundary");
        }
        byte[] actual = sha256(envelope, 0, bodyLength);
        int difference = 0;
        for (int i = 0; i < DIGEST_BYTES; i++) {
            difference |= actual[i] ^ envelope[bodyLength + i];
        }
        if (difference != 0) throw new IllegalArgumentException("stage command digest mismatch");
    }

    private static int wireVersion(StageCommand command) {
        if (command.downFixedPartition() >= 0) {
            return WRITE_VERSION;
        }
        List<StageCommand.BroadcastGroup> groups =
                command.broadcastGroups();
        if (groups != null) {
            for (StageCommand.BroadcastGroup group : groups) {
                if (group.fixedPartition() >= 0) {
                    return WRITE_VERSION;
                }
            }
        }
        return command.portableStageSpec() == null
                ? BASELINE_WRITE_VERSION
                : PORTABLE_STAGE_WRITE_VERSION;
    }

    private static void writeRouting(DataOutputStream out, StageCommand command,
                                     LongConsumer reservation,
                                     int commandSchemaVersion) throws IOException {
        out.writeInt(command.role().ordinal());
        writeMeta(out, command.dataSchema(), reservation);
        out.writeInt(command.window());
        writeString(out, command.host(), reservation);
        out.writeInt(command.port());
        writeEndpoints(out, command.downstream(), reservation);
        writeStrings(out, command.keyFields(), reservation);
        out.writeBoolean(command.numericNormalized());
        out.writeInt(command.inboundConnections());
        writeEndpoint(out, command.sinkEndpoint(), reservation);
        writeMeta(out, command.rightMeta(), reservation);
        out.writeInt(command.rightInboundConnections());
        writeMeta(out, command.joinOutMeta(), reservation);
        writeStrings(out, command.rightKeyFields(), reservation);
        writeString(out, command.joinType(), reservation);
        writeStrings(out, command.downKeys(), reservation);
        out.writeBoolean(command.downNormalized());
        if (commandSchemaVersion >= 9) {
            int fixedPartition = command.downFixedPartition();
            out.writeBoolean(fixedPartition >= 0);
            if (fixedPartition >= 0) out.writeInt(fixedPartition);
        }
        writeTargets(out, command.downTargets(), reservation);
        writeBroadcastGroups(out, command.broadcastGroups(), reservation,
                commandSchemaVersion);
        out.writeInt(command.localInbound());
        out.writeInt(command.localInboundRight());
        writeString(out, command.localInboundKey(), reservation);
        writeString(out, command.localInboundKeyRight(), reservation);
        out.writeBoolean(command.objectReuse());
        writeUuid(out, command.inboundTargetId());
        writeUuid(out, command.inboundTargetIdRight());
        writeString(out, command.commitGroupId(), reservation);
        writeString(out, command.manifestAttemptId(), reservation);
        out.writeLong(command.manifestGeneration());
        writeFence(out, command.runAttemptFence(), reservation);
        JoinPhysicalStrategy strategy = command.rawJoinStrategy();
        out.writeInt(strategy == null ? -1 : strategy.ordinal());
        writeSourceAssignment(out, command.sourceSplitAssignment(), reservation);
        writeInboundAuthorizations(out, command.inboundAuthorizations(), reservation);
        writeSinkContext(out, command.sinkAttemptIdentity(),
                command.publicationId(), command.publicationGeneration(), reservation);
        if (commandSchemaVersion >= PORTABLE_STAGE_WRITE_VERSION) {
            writePortableStageSpec(out, command.portableStageSpec());
        }
    }

    private static StageCommand.WireFields readRouting(Cursor in, int commandSchemaVersion) {
        StageCommand.WireFields fields = new StageCommand.WireFields();
        int role = in.readInt("role");
        if (role < 0 || role >= ROLES.length) throw new IllegalArgumentException("role ordinal out of bounds");
        fields.role = ROLES[role];
        fields.dataSchema = in.meta("row meta");
        fields.window = in.readInt("window");
        fields.host = in.string("host");
        fields.port = in.readInt("port");
        fields.downstream = in.endpoints("downstream", commandSchemaVersion);
        fields.keyFields = in.strings("key fields");
        fields.numericNormalized = in.bool("numeric normalized");
        fields.inboundConnections = in.readInt("inbound connections");
        fields.sinkEndpoint = in.endpoint("sink endpoint", commandSchemaVersion);
        fields.rightMeta = in.meta("right meta");
        fields.rightInboundConnections = in.readInt("right inbound connections");
        fields.joinOutMeta = in.meta("join output meta");
        fields.rightKeyFields = in.strings("right key fields");
        fields.joinType = in.string("join type");
        fields.downKeys = in.strings("down keys");
        fields.downNormalized = in.bool("down normalized");
        if (commandSchemaVersion >= 9) {
            fields.downFixedRouting = in.bool(
                    "fixed downstream routing present");
            if (fields.downFixedRouting) {
                fields.downFixedPartition = in.readInt(
                        "fixed downstream partition");
                if (fields.downFixedPartition < 0) {
                    throw new IllegalArgumentException(
                            "fixed downstream partition is negative");
                }
            }
        }
        fields.downTargets = in.targets("down targets", commandSchemaVersion);
        if (fields.downFixedRouting) {
            int targetCount = fields.downTargets != null
                    ? fields.downTargets.size()
                    : fields.downstream == null ? 0
                    : fields.downstream.size();
            if (fields.downFixedPartition >= targetCount) {
                throw new IllegalArgumentException(
                        "fixed downstream partition is outside the target range");
            }
        }
        fields.broadcastGroups = in.broadcastGroups(commandSchemaVersion);
        fields.localInbound = in.readInt("local inbound");
        fields.localInboundRight = in.readInt("local right inbound");
        fields.localInboundKey = in.string("local inbound key");
        fields.localInboundKeyRight = in.string("local right inbound key");
        fields.objectReuse = in.bool("object reuse");
        fields.inboundTargetId = in.uuid("inbound target id");
        fields.inboundTargetIdRight = in.uuid("right inbound target id");
        fields.commitGroupId = in.string("commit group id");
        fields.manifestAttemptId = in.string("manifest attempt id");
        fields.manifestGeneration = in.readLong("manifest generation");
        fields.runAttemptFence = in.fence();
        if (commandSchemaVersion >= 2) {
            int strategy = in.readInt("join strategy");
            if (strategy == -1) {
                fields.joinStrategy = null;
            } else {
                JoinPhysicalStrategy[] strategies = JoinPhysicalStrategy.values();
                if (strategy < 0 || strategy >= strategies.length) {
                    throw new IllegalArgumentException("join strategy ordinal out of bounds");
                }
                fields.joinStrategy = strategies[strategy];
            }
        } else if (fields.role == StageCommand.Role.JOIN_CONSUMER && fields.joinType != null) {
            fields.joinStrategy = JoinPhysicalStrategy.SORT_MERGE;
        }
        if (commandSchemaVersion >= 3) {
            fields.sourceSplitAssignment = readSourceAssignment(in);
        }
        if (commandSchemaVersion >= 5) {
            fields.inboundAuthorizations = in.inboundAuthorizations();
        }
        if (commandSchemaVersion >= 6) {
            readSinkContext(in, fields);
        }
        if (commandSchemaVersion >= 8) {
            fields.portableStageSpec = readPortableStageSpec(in);
        }
        return fields;
    }

    private static void writePortableStageSpec(
            DataOutputStream out, PortableStageSpec spec) throws IOException {
        out.writeBoolean(spec != null);
        if (spec == null) return;
        out.writeInt(spec.logicalStage());
        out.writeInt(spec.members().size());
        for (int member : spec.members()) out.writeInt(member);
        out.writeInt(spec.instanceIndex());
        out.writeInt(spec.instanceCount());
        out.writeBoolean(spec.canonicalizeSink());
    }

    private static PortableStageSpec readPortableStageSpec(Cursor in) {
        if (!in.bool("portable stage slice present")) return null;
        int logicalStage = in.readInt("portable logical stage");
        int count = in.readInt("portable stage member count");
        if (count < 1 || count > PortableStageSpec.MAX_MEMBERS) {
            throw new IllegalArgumentException(
                    "portable stage member count is out of bounds");
        }
        List<Integer> members = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            members.add(in.readInt("portable stage member"));
        }
        return new PortableStageSpec(
                logicalStage, members,
                in.readInt("portable instance index"),
                in.readInt("portable instance count"),
                in.bool("portable sink canonicalization"));
    }

    private static void writeSinkContext(
            DataOutputStream out,
            io.rowweave.run.SinkAttemptIdentity identity,
            String publicationId, long publicationGeneration,
            LongConsumer reservation) throws IOException {
        out.writeBoolean(identity != null);
        if (identity == null) {
            if (publicationId != null || publicationGeneration != 0L) {
                throw new IllegalArgumentException(
                        "partial durable Sink context");
            }
            return;
        }
        byte[] encoded = io.rowweave.run.SinkAttemptIdentityCodec.encode(identity);
        reservation.accept(encoded.length);
        out.writeInt(encoded.length);
        out.write(encoded);
        writeRequiredString(out, publicationId, reservation);
        if (publicationGeneration <= 0L) {
            throw new IllegalArgumentException(
                    "durable Sink publication generation must be positive");
        }
        out.writeLong(publicationGeneration);
    }

    private static void readSinkContext(
            Cursor in, StageCommand.WireFields fields) {
        if (!in.bool("durable Sink context present")) return;
        int length = in.readInt("Sink attempt identity length");
        if (length < 1
                || length > io.rowweave.run.SinkAttemptIdentityCodec.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "Sink attempt identity length out of bounds");
        }
        fields.sinkAttemptIdentity =
                io.rowweave.run.SinkAttemptIdentityCodec.decode(
                        in.fixed(length, "Sink attempt identity"));
        fields.publicationId = in.requiredString("Sink publication id");
        fields.publicationGeneration = in.readLong("Sink publication generation");
        if (fields.publicationGeneration <= 0L) {
            throw new IllegalArgumentException(
                    "Sink publication generation must be positive");
        }
    }

    private static void validateSourceAssignment(
            StageCommand.Role role,
            io.rowweave.run.SourceSplitAssignment.Context sourceAssignment) {
        if (sourceAssignment != null
                && role != StageCommand.Role.SOURCE_STAGE
                && role != StageCommand.Role.WHOLE_GRAPH) {
            throw new IllegalArgumentException(
                    "source ownership is invalid for role " + role);
        }
    }

    private static void validateShuffleAuthorizationBinding(
            io.rowweave.run.RunAttemptFence fence,
            java.util.UUID inboundTarget,
            java.util.UUID inboundTargetRight,
            List<ShuffleInboundAuthorization> authorizations,
            List<ShuffleEndpoint> downstream,
            ShuffleEndpoint sinkEndpoint,
            List<ShuffleTarget> downTargets,
            List<StageCommand.BroadcastGroup> broadcastGroups) {
        boolean carriesGrant = hasGrant(downstream) || hasGrant(sinkEndpoint)
                || hasGrantTargets(downTargets);
        if (broadcastGroups != null) {
            for (StageCommand.BroadcastGroup group : broadcastGroups) {
                if (group != null && hasGrantTargets(group.targets)) {
                    carriesGrant = true;
                }
            }
        }
        if ((carriesGrant || authorizations != null) && fence == null) {
            throw new IllegalArgumentException(
                    "shuffle grant binding requires a run attempt fence");
        }
        if (authorizations == null) return;

        java.util.Set<java.util.UUID> commandTargets = new java.util.HashSet<>();
        if (inboundTarget != null) commandTargets.add(inboundTarget);
        if (inboundTargetRight != null) commandTargets.add(inboundTargetRight);
        java.util.Set<java.util.UUID> authorizedTargets = new java.util.HashSet<>();
        for (ShuffleInboundAuthorization authorization : authorizations) {
            if (authorization == null
                    || !authorizedTargets.add(authorization.targetId())) {
                throw new IllegalArgumentException(
                        "invalid inbound shuffle authorization target set");
            }
        }
        if (!commandTargets.equals(authorizedTargets)) {
            throw new IllegalArgumentException(
                    "inbound shuffle authorization targets do not match command targets");
        }
    }

    private static boolean hasGrant(List<ShuffleEndpoint> endpoints) {
        if (endpoints == null) return false;
        for (ShuffleEndpoint endpoint : endpoints) {
            if (hasGrant(endpoint)) return true;
        }
        return false;
    }

    private static boolean hasGrant(ShuffleEndpoint endpoint) {
        return endpoint != null && endpoint.producerGrant() != null;
    }

    private static boolean hasGrantTargets(List<ShuffleTarget> targets) {
        if (targets == null) return false;
        for (ShuffleTarget target : targets) {
            if (target != null && !target.isLocal() && hasGrant(target.endpoint())) {
                return true;
            }
        }
        return false;
    }

    private static void writeSourceAssignment(
            DataOutputStream out,
            io.rowweave.run.SourceSplitAssignment.Context context,
            LongConsumer reservation) throws IOException {
        out.writeBoolean(context != null);
        if (context == null) return;
        writeRequiredString(out, context.artifactId(), reservation);
        out.writeInt(context.logicalStage());
        out.writeInt(context.instanceIndex());
        checkedCount(context.sourceGroups().size(), "source ownership group count");
        out.writeInt(context.sourceGroups().size());
        for (io.rowweave.run.SourceSplitAssignment.Context.SourceGroupContext group
                : context.sourceGroups()) {
            writeRequiredString(out, group.sourceOperatorId(), reservation);
            out.writeInt(group.sourceSplitCount());
        }
        out.writeInt(context.sourceSplitCount());
        out.write(context.ownershipDigest());
    }

    private static io.rowweave.run.SourceSplitAssignment.Context readSourceAssignment(
            Cursor in) {
        if (!in.bool("source ownership present")) return null;
        String artifactId = in.requiredString("source artifact id");
        int logicalStage = in.readInt("source logical stage");
        int instanceIndex = in.readInt("source instance index");
        int count = in.count("source ownership group count");
        if (count == 0) {
            throw new IllegalArgumentException("source ownership requires at least one group");
        }
        in.reservation.accept(24L + 8L * count);
        List<io.rowweave.run.SourceSplitAssignment.Context.SourceGroupContext> groups =
                new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            groups.add(io.rowweave.run.SourceSplitAssignment.Context.SourceGroupContext.create(
                    in.requiredString("source operator id"),
                    in.readInt("source group split count")));
        }
        int sourceSplitCount = in.readInt("source split count");
        byte[] digest = in.fixed(DIGEST_BYTES, "source ownership digest");
        return io.rowweave.run.SourceSplitAssignment.Context.create(
                artifactId, logicalStage, instanceIndex, groups,
                sourceSplitCount, digest);
    }

    private static void validateRole(StageCommand.Role role, int window, DataSchema dataSchema,
                                     Object input, Object output, Object fragment,
                                     Object sinkFragment, Object coGroup,
                                     Object portableArtifact,
                                     PortableStageSpec portableStageSpec,
                                     String joinType, JoinPhysicalStrategy joinStrategy) {
        if (role == null) throw new IllegalArgumentException("stage command role is required");
        boolean whole = role == StageCommand.Role.WHOLE_GRAPH;
        if (whole ? window != 0 : window <= 0) {
            throw new IllegalArgumentException("invalid window for role " + role);
        }
        if (whole ? dataSchema != null : dataSchema == null) {
            throw new IllegalArgumentException("invalid row meta for role " + role);
        }
        boolean needsInput = role == StageCommand.Role.PRODUCER
                || role == StageCommand.Role.ROUTER || role == StageCommand.Role.SOURCE_STAGE;
        boolean needsOutput = role == StageCommand.Role.CONSUMER || role == StageCommand.Role.SINK;
        boolean needsFragment = role == StageCommand.Role.KEYED_OP
                || role == StageCommand.Role.SOURCE_STAGE
                || whole && portableArtifact == null;
        boolean needsSinkFragment = role == StageCommand.Role.SINK;
        requirePresence("input", input, needsInput, role);
        requirePresence("output", output, needsOutput, role);
        requirePresence("fragment", fragment, needsFragment, role);
        requirePresence("sink fragment", sinkFragment, needsSinkFragment, role);
        if (!whole && ((portableArtifact == null)
                != (portableStageSpec == null))) {
            throw new IllegalArgumentException(
                    "portable stage artifact and slice must be paired");
        }
        if (whole && (fragment != null) == (portableArtifact != null)) {
            throw new IllegalArgumentException(
                    "WHOLE_GRAPH requires exactly one graph representation");
        }
        if (whole && portableStageSpec != null) {
            throw new IllegalArgumentException(
                    "WHOLE_GRAPH cannot carry a portable stage slice");
        }
        if (coGroup != null && role != StageCommand.Role.JOIN_CONSUMER) {
            throw new IllegalArgumentException("coGroup object is invalid for role " + role);
        }
        if (role != StageCommand.Role.JOIN_CONSUMER) {
            if (joinStrategy != null) {
                throw new IllegalArgumentException("join strategy is invalid for role " + role);
            }
        } else if (coGroup != null) {
            if (joinStrategy != null) {
                throw new IllegalArgumentException("join strategy is invalid for CoGroup");
            }
        } else {
            if (joinType == null) {
                throw new IllegalArgumentException("join type is required for JOIN_CONSUMER");
            }
            if (joinStrategy == null) {
                throw new IllegalArgumentException("join strategy is required for JOIN_CONSUMER");
            }
        }
    }

    private static void requirePresence(String name, Object value, boolean required,
                                        StageCommand.Role role) {
        if ((value != null) != required) {
            throw new IllegalArgumentException(name + " presence is invalid for role " + role);
        }
    }

    private static byte[] writePlan(PlanGraph graph, ObjectTrustContext trustContext,
                                    LongConsumer reservation) {
        return graph == null ? null : graph.toBytes(trustContext, reservation);
    }

    private static byte[] writeObject(Object value, Class<?> root,
                                      ObjectTrustContext trustContext, LongConsumer reservation) {
        return value == null ? null : TrustedObjectCodec.write(value, root, trustContext, reservation);
    }

    private static byte[] writeOutput(
            Sink output, ObjectTrustContext trustContext,
            LongConsumer reservation) {
        if (output == null) return null;
        if (output.getClass() == ProviderBatchSink.class) {
            return ProviderSinkStageEnvelopeCodec.encode(
                    (ProviderBatchSink) output, reservation);
        }
        return writeObject(output, Sink.class, trustContext, reservation);
    }

    private static void scanPlan(byte[] envelope, PreparedStageCommand.Slice slice,
                                 LongConsumer reservation) {
        if (slice != null) PlanGraphEnvelopeScanner.scan(
                envelope, slice.offset, slice.length, reservation);
    }

    private static void scanObjectHeader(byte[] envelope, PreparedStageCommand.Slice slice,
                                         String name) {
        if (slice == null) return;
        if (slice.length < 4 || (envelope[slice.offset] & 0xff) != 0xac
                || (envelope[slice.offset + 1] & 0xff) != 0xed
                || (envelope[slice.offset + 2] & 0xff) != 0x00
                || (envelope[slice.offset + 3] & 0xff) != 0x05) {
            throw new IllegalArgumentException(name + " has invalid serialization header");
        }
    }

    private static void writeSlice(DataOutputStream out, byte[] value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(value.length);
            out.write(value);
        }
    }

    private static void writeMeta(DataOutputStream out, DataSchema meta,
                                  LongConsumer reservation) throws IOException {
        out.writeBoolean(meta != null);
        if (meta == null) return;
        int fields = meta.getFieldCount();
        checkedCount(fields, "field count");
        out.writeInt(fields);
        for (int i = 0; i < fields; i++) {
            Field field = meta.getField(i);
            writeRequiredString(out, field.getName(), reservation);
            writeRequiredString(out, field.getAlias(), reservation);
            if (field.getDataType() == null) throw new IllegalArgumentException("field data type is required");
            out.writeInt(field.getDataType().ordinal());
        }
    }

    private static void writeEndpoints(DataOutputStream out, List<ShuffleEndpoint> values,
                                       LongConsumer reservation) throws IOException {
        if (values == null) { out.writeInt(-1); return; }
        checkedCount(values.size(), "endpoint count");
        out.writeInt(values.size());
        for (ShuffleEndpoint value : values) writeEndpoint(out, value, reservation);
    }

    private static void writeEndpoint(DataOutputStream out, ShuffleEndpoint endpoint,
                                      LongConsumer reservation) throws IOException {
        out.writeBoolean(endpoint != null);
        if (endpoint == null) return;
        writeRequiredString(out, endpoint.host(), reservation);
        out.writeInt(endpoint.port());
        out.writeInt(endpoint.dataPort());
        writeUuid(out, endpoint.targetId());
        writeWorkerIdentity(out, endpoint.destinationIdentity(), reservation);
        ShuffleProducerGrant grant = endpoint.producerGrant();
        out.writeBoolean(grant != null);
        if (grant != null) {
            byte[] rawGrant = grant.copyBytes();
            if (rawGrant.length != ShuffleProducerGrant.BYTES) {
                throw new IllegalArgumentException("producer grant must be exactly 32 bytes");
            }
            reservation.accept(rawGrant.length);
            out.write(rawGrant);
            out.writeInt(endpoint.producerOrdinal());
            byte[] cohortId = endpoint.cohortId();
            if (cohortId == null || cohortId.length != 32) {
                throw new IllegalArgumentException("cohort id must be exactly 32 bytes");
            }
            reservation.accept(cohortId.length);
            out.write(cohortId);
            out.writeInt(endpoint.expectedOrdinalCount());
        }
    }

    private static void writeWorkerIdentity(DataOutputStream out,
                                            WorkerDataIdentity identity,
                                            LongConsumer reservation) throws IOException {
        out.writeBoolean(identity != null);
        if (identity == null) return;
        writeRequiredString(out, identity.workerId(), reservation);
        writeRequiredString(out, identity.incarnationId(), reservation);
        // Wire ids are part of the durable command contract; enum ordinals are not.
        out.writeInt(1);
        writeRequiredString(out, identity.deploymentSetId(), reservation);
        writeRequiredString(out, identity.authzSetId(), reservation);
        byte[] fingerprint = identity.certificateFingerprint();
        if (fingerprint.length != DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES) {
            throw new IllegalArgumentException(
                    "Worker certificate fingerprint must be exactly 32 bytes");
        }
        reservation.accept(fingerprint.length);
        out.write(fingerprint);
    }

    private static void writeInboundAuthorizations(
            DataOutputStream out, List<ShuffleInboundAuthorization> authorizations,
            LongConsumer reservation) throws IOException {
        if (authorizations == null) {
            out.writeInt(-1);
            return;
        }
        checkedCount(authorizations.size(), "inbound authorization target count");
        if (authorizations.size() > 2) {
            throw new IllegalArgumentException(
                    "inbound authorization target count exceeds two edges");
        }
        out.writeInt(authorizations.size());
        for (ShuffleInboundAuthorization authorization : authorizations) {
            if (authorization == null) {
                throw new IllegalArgumentException("inbound authorization is required");
            }
            writeUuid(out, authorization.targetId());
            out.writeInt(authorization.expectedPoolCount());
            List<ShuffleAuthorizationRecord> records = authorization.records();
            checkedCount(records.size(), "authorization producer count");
            out.writeInt(records.size());
            for (ShuffleAuthorizationRecord record : records) {
                if (record == null || !authorization.targetId().equals(record.targetId())) {
                    throw new IllegalArgumentException("authorization record target mismatch");
                }
                out.writeInt(record.producerOrdinal());
                writeWorkerIdentity(out, record.expectedSourceIdentity(), reservation);
                byte[] digest = record.authorizationDigest();
                if (digest.length != ShuffleAuthorizationRecord.DIGEST_BYTES) {
                    throw new IllegalArgumentException(
                            "authorization digest must be exactly 32 bytes");
                }
                reservation.accept(digest.length);
                out.write(digest);
            }
        }
    }

    private static void writeTargets(DataOutputStream out, List<ShuffleTarget> targets,
                                     LongConsumer reservation) throws IOException {
        if (targets == null) { out.writeInt(-1); return; }
        checkedCount(targets.size(), "target count");
        out.writeInt(targets.size());
        for (ShuffleTarget target : targets) {
            if (target == null) throw new IllegalArgumentException("shuffle target is required");
            out.writeBoolean(target.isLocal());
            if (target.isLocal()) writeRequiredString(out, target.localKey(), reservation);
            else writeEndpoint(out, target.endpoint(), reservation);
        }
    }

    private static void writeBroadcastGroups(DataOutputStream out,
                                             List<StageCommand.BroadcastGroup> groups,
                                             LongConsumer reservation,
                                             int commandSchemaVersion) throws IOException {
        if (groups == null) { out.writeInt(-1); return; }
        checkedCount(groups.size(), "broadcast group count");
        out.writeInt(groups.size());
        for (StageCommand.BroadcastGroup group : groups) {
            if (group == null) throw new IllegalArgumentException("broadcast group is required");
            writeTargets(out, group.targets, reservation);
            writeStrings(out, group.downKeys, reservation);
            out.writeBoolean(group.normalized);
            out.writeBoolean(group.blocking);
            if (commandSchemaVersion >= 9) {
                int fixedPartition = group.fixedPartition();
                out.writeBoolean(fixedPartition >= 0);
                if (fixedPartition >= 0) out.writeInt(fixedPartition);
            }
        }
    }

    private static void writeStrings(DataOutputStream out, String[] values,
                                     LongConsumer reservation) throws IOException {
        if (values == null) { out.writeInt(-1); return; }
        checkedCount(values.length, "string array count");
        out.writeInt(values.length);
        for (String value : values) writeRequiredString(out, value, reservation);
    }

    private static void writeFence(DataOutputStream out, RunAttemptFence fence,
                                   LongConsumer reservation) throws IOException {
        out.writeBoolean(fence != null);
        if (fence == null) return;
        writeRequiredString(out, fence.runId(), reservation);
        writeRequiredString(out, fence.attemptId(), reservation);
        out.writeLong(fence.fenceToken());
    }

    private static void writeUuid(DataOutputStream out, UUID value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeLong(value.getMostSignificantBits());
            out.writeLong(value.getLeastSignificantBits());
        }
    }

    private static void writeRequiredString(DataOutputStream out, String value,
                                            LongConsumer reservation) throws IOException {
        if (value == null) throw new IllegalArgumentException("required string is null");
        writeString(out, value, reservation);
    }

    private static void writeString(DataOutputStream out, String value,
                                    LongConsumer reservation) throws IOException {
        if (value == null) { out.writeInt(-1); return; }
        byte[] bytes;
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > MAX_UTF8_BYTES) {
                throw new IllegalArgumentException("UTF-8 string exceeds limit");
            }
            reservation.accept(encoded.remaining());
            bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
        } catch (CharacterCodingException failure) {
            throw new IllegalArgumentException("invalid UTF-16 string", failure);
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void checkedCount(int count, String name) {
        if (count < 0 || count > MAX_COLLECTION) {
            throw new IllegalArgumentException(name + " out of bounds: " + count);
        }
    }

    private static byte[] sha256(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static final class Cursor {
        private final byte[] bytes;
        private final int limit;
        private final LongConsumer reservation;
        private int position;
        private int routingLimit = Integer.MAX_VALUE;

        Cursor(byte[] bytes, int limit, LongConsumer reservation) {
            this.bytes = bytes;
            this.limit = limit;
            this.reservation = reservation;
        }

        int position() { return position; }

        void beginRouting(int maxBytes) {
            routingLimit = position > Integer.MAX_VALUE - maxBytes
                    ? Integer.MAX_VALUE : position + maxBytes;
        }

        void endRouting() {
            routingLimit = Integer.MAX_VALUE;
        }

        void expect(byte[] expected, String name) {
            require(expected.length, name);
            for (byte value : expected) {
                if (bytes[position++] != value) throw new IllegalArgumentException(name + " mismatch");
            }
        }

        int readInt(String name) {
            require(Integer.BYTES, name);
            int value = ((bytes[position] & 0xff) << 24)
                    | ((bytes[position + 1] & 0xff) << 16)
                    | ((bytes[position + 2] & 0xff) << 8)
                    | (bytes[position + 3] & 0xff);
            position += Integer.BYTES;
            return value;
        }

        long readLong(String name) {
            require(Long.BYTES, name);
            long value = 0L;
            for (int i = 0; i < Long.BYTES; i++) value = (value << 8) | (bytes[position++] & 0xffL);
            return value;
        }

        boolean bool(String name) {
            require(1, name);
            int value = bytes[position++] & 0xff;
            if (value > 1) throw new IllegalArgumentException(name + " is not a boolean");
            return value == 1;
        }

        byte[] fixed(int length, String name) {
            require(length, name);
            byte[] value = Arrays.copyOfRange(bytes, position, position + length);
            position += length;
            reservation.accept(length);
            return value;
        }

        String string(String name) {
            int length = readInt(name + " length");
            if (length == -1) return null;
            if (length < 0 || length > MAX_UTF8_BYTES) {
                throw new IllegalArgumentException(name + " length out of bounds");
            }
            require(length, name);
            reservation.accept(Math.max(40L, 2L * length));
            try {
                String value = StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(bytes, position, length)).toString();
                position += length;
                return value;
            } catch (CharacterCodingException failure) {
                throw new IllegalArgumentException(name + " is not valid UTF-8", failure);
            }
        }

        String requiredString(String name) {
            String value = string(name);
            if (value == null) throw new IllegalArgumentException(name + " is required");
            return value;
        }

        String[] strings(String name) {
            int count = nullableCount(name);
            if (count < 0) return null;
            reservation.accept(16L + 8L * count);
            String[] values = new String[count];
            for (int i = 0; i < count; i++) values[i] = requiredString(name + '[' + i + ']');
            return values;
        }

        DataSchema meta(String name) {
            if (!bool(name + " present")) return null;
            int count = count(name + " field count");
            reservation.accept(24L + 8L * count);
            Field[] fields = new Field[count];
            for (int i = 0; i < count; i++) {
                String fieldName = requiredString(name + " field name");
                String alias = requiredString(name + " field alias");
                int type = readInt(name + " field type");
                if (type < 0 || type >= DATA_TYPES.length) {
                    throw new IllegalArgumentException(name + " field type out of bounds");
                }
                reservation.accept(40L);
                fields[i] = new Field(fieldName, alias, DATA_TYPES[type]);
            }
            reservation.accept(32L);
            return new DataSchema(fields);
        }

        List<ShuffleEndpoint> endpoints(String name, int commandSchemaVersion) {
            int count = nullableCount(name);
            if (count < 0) return null;
            reservation.accept(24L + 8L * count);
            List<ShuffleEndpoint> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ShuffleEndpoint endpoint = endpoint(name + '[' + i + ']', commandSchemaVersion);
                if (endpoint == null) throw new IllegalArgumentException(name + " endpoint is required");
                values.add(endpoint);
            }
            return Collections.unmodifiableList(values);
        }

        ShuffleEndpoint endpoint(String name, int commandSchemaVersion) {
            if (!bool(name + " present")) return null;
            String host = requiredString(name + " host");
            int port = readInt(name + " port");
            int dataPort = readInt(name + " data port");
            UUID target = uuid(name + " target id");
            if (port < 0 || port > 65535 || dataPort < -1 || dataPort > 65535 || target == null) {
                throw new IllegalArgumentException(name + " has invalid endpoint values");
            }
            WorkerDataIdentity identity = null;
            if (commandSchemaVersion >= 4) identity = workerIdentity(name);
            ShuffleProducerGrant grant = null;
            int producerOrdinal = 0;
            byte[] cohortId = null;
            int expectedOrdinalCount = 0;
            if (commandSchemaVersion >= 5 && bool(name + " authorization present")) {
                grant = ShuffleProducerGrant.fromBytes(fixed(
                        ShuffleProducerGrant.BYTES, name + " producer grant"));
                producerOrdinal = readInt(name + " producer ordinal");
                cohortId = fixed(32, name + " cohort id");
                expectedOrdinalCount = readInt(name + " expected ordinal count");
            }
            reservation.accept(identity == null ? 56L : 224L);
            if (dataPort < 0) {
                if (identity != null || grant != null) {
                    throw new IllegalArgumentException(
                            name + " legacy endpoint cannot carry data authorization");
                }
                return new ShuffleEndpoint(host, port);
            }
            if (grant != null) {
                reservation.accept(96L);
                return ShuffleEndpoint.muxAuthorized(host, dataPort, target, identity,
                        grant, producerOrdinal, cohortId, expectedOrdinalCount);
            }
            return identity == null ? ShuffleEndpoint.mux(host, dataPort, target)
                    : ShuffleEndpoint.muxAuthenticated(host, dataPort, target, identity);
        }

        WorkerDataIdentity workerIdentity(String name) {
            if (!bool(name + " identity present")) return null;
            String workerId = requiredString(name + " worker id");
            String incarnation = requiredString(name + " worker incarnation");
            int protocolWireId = readInt(name + " data protocol");
            if (protocolWireId != 1) {
                throw new IllegalArgumentException(name + " data protocol is unsupported");
            }
            String deploymentSet = requiredString(name + " deployment set id");
            String authzSet = requiredString(name + " authorization set id");
            byte[] fingerprint = fixed(
                    DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES,
                    name + " certificate fingerprint");
            return WorkerDataIdentity.authenticated(
                    workerId, incarnation, DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                    deploymentSet, authzSet, fingerprint);
        }

        List<ShuffleInboundAuthorization> inboundAuthorizations() {
            int targetCount = nullableCount("inbound authorization targets");
            if (targetCount < 0) return null;
            if (targetCount < 1 || targetCount > 2) {
                throw new IllegalArgumentException(
                        "inbound authorization target count out of bounds: " + targetCount);
            }
            reservation.accept(24L + 8L * targetCount);
            List<ShuffleInboundAuthorization> authorizations = new ArrayList<>(targetCount);
            for (int targetIndex = 0; targetIndex < targetCount; targetIndex++) {
                String prefix = "inbound authorization[" + targetIndex + ']';
                UUID targetId = uuid(prefix + " target");
                int expectedPoolCount = readInt(prefix + " expected pool count");
                int producerCount = count(prefix + " producer count");
                if (producerCount < 1) {
                    throw new IllegalArgumentException(prefix + " has no producers");
                }
                // Count is checked before the list or any per-record object is allocated.
                reservation.accept(24L + 8L * producerCount);
                List<ShuffleAuthorizationRecord> records = new ArrayList<>(producerCount);
                for (int producerIndex = 0; producerIndex < producerCount; producerIndex++) {
                    String recordName = prefix + " producer[" + producerIndex + ']';
                    int ordinal = readInt(recordName + " ordinal");
                    WorkerDataIdentity source = workerIdentity(recordName + " source");
                    byte[] digest = fixed(ShuffleAuthorizationRecord.DIGEST_BYTES,
                            recordName + " digest");
                    reservation.accept(96L);
                    records.add(ShuffleAuthorizationRecord.fromDigest(
                            targetId, ordinal, source, digest));
                }
                reservation.accept(64L);
                authorizations.add(ShuffleInboundAuthorization.of(
                        targetId, expectedPoolCount, records));
            }
            return Collections.unmodifiableList(authorizations);
        }

        List<ShuffleTarget> targets(String name, int commandSchemaVersion) {
            int count = nullableCount(name);
            if (count < 0) return null;
            reservation.accept(24L + 8L * count);
            List<ShuffleTarget> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                boolean local = bool(name + " local");
                values.add(local ? ShuffleTarget.local(requiredString(name + " local key"))
                        : ShuffleTarget.socket(requiredEndpoint(
                                name + " socket", commandSchemaVersion)));
            }
            return Collections.unmodifiableList(values);
        }

        List<StageCommand.BroadcastGroup> broadcastGroups(int commandSchemaVersion) {
            int count = nullableCount("broadcast groups");
            if (count < 0) return null;
            reservation.accept(24L + 8L * count);
            List<StageCommand.BroadcastGroup> groups = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                List<ShuffleTarget> targets = targets(
                        "broadcast targets", commandSchemaVersion);
                String[] keys = strings("broadcast keys");
                boolean normalized = bool("broadcast normalized");
                boolean blocking = bool("broadcast blocking");
                reservation.accept(40L);
                StageCommand.BroadcastGroup group =
                        new StageCommand.BroadcastGroup(
                                targets, keys, normalized, blocking);
                if (commandSchemaVersion >= 9
                        && bool("fixed broadcast routing present")) {
                    int fixedPartition = readInt(
                            "fixed broadcast partition");
                    if (fixedPartition < 0) {
                        throw new IllegalArgumentException(
                                "fixed broadcast partition is negative");
                    }
                    if (targets == null
                            || fixedPartition >= targets.size()) {
                        throw new IllegalArgumentException(
                                "fixed broadcast partition is outside the target range");
                    }
                    group.withFixedPartition(
                            fixedPartition, targets.size());
                }
                groups.add(group);
            }
            return Collections.unmodifiableList(groups);
        }

        UUID uuid(String name) {
            if (!bool(name + " present")) return null;
            reservation.accept(32L);
            return new UUID(readLong(name + " most"), readLong(name + " least"));
        }

        RunAttemptFence fence() {
            if (!bool("run attempt fence present")) return null;
            String runId = requiredString("run id");
            String attemptId = requiredString("attempt id");
            long token = readLong("fence token");
            reservation.accept(48L);
            return new RunAttemptFence(runId, attemptId, token);
        }

        PreparedStageCommand.Slice slice(String name, int maxLength) {
            int length = readInt(name + " length");
            if (length == -1) return null;
            if (length < 0 || length > maxLength) {
                throw new IllegalArgumentException(name + " length out of bounds");
            }
            require(length, name);
            PreparedStageCommand.Slice slice = new PreparedStageCommand.Slice(position, length);
            position += length;
            return slice;
        }

        private ShuffleEndpoint requiredEndpoint(String name, int commandSchemaVersion) {
            ShuffleEndpoint value = endpoint(name, commandSchemaVersion);
            if (value == null) throw new IllegalArgumentException(name + " is required");
            return value;
        }

        private int count(String name) {
            int count = readInt(name);
            if (count < 0 || count > MAX_COLLECTION) {
                throw new IllegalArgumentException(name + " out of bounds: " + count);
            }
            return count;
        }

        private int nullableCount(String name) {
            int count = readInt(name + " count");
            if (count < -1 || count > MAX_COLLECTION) {
                throw new IllegalArgumentException(name + " count out of bounds: " + count);
            }
            return count;
        }

        private void require(int length, String name) {
            int effectiveLimit = Math.min(limit, routingLimit);
            if (length < 0 || position > effectiveLimit - length) {
                throw new IllegalArgumentException("truncated " + name);
            }
        }
    }
}
