package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.AssignedSourceBatch;
import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.CollectionSource;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanGraphEnvelopeScanner;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.SourceSplitAssignment;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.net.WorkerDataIdentity;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.cluster.rpc.ControlSecurityProfile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageCommandCodecV2Test {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static final RunAttemptFence FENCE = new RunAttemptFence("run-v2", "attempt-1", 7L);
    private static final AtomicInteger READ_CALLBACKS = new AtomicInteger();

    @Test
    void reflectionInventoryClassifiesEveryPersistentFieldExactlyOnce() {
        StageCommandFieldInventory.assertComplete();
    }

    @Test
    void preflightScansWholeGraphWithoutMaterializingUserInput() {
        READ_CALLBACKS.set(0);
        StageCommand command = StageCommand.wholeGraph(new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META,
                        Collections.<Source>singletonList(new CallbackInput())),
                PlanNode.sink(META, 0, new DiscardSink(META)).build())))
                .withRunAttemptFence(FENCE);

        byte[] encoded = StageCommandCodecV2.encode(command,
                ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { });
        assertTrue(StageCommandCodecV2.hasMagic(encoded));
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(encoded, ignored -> { });

        assertEquals(0, READ_CALLBACKS.get());
        StageCommand decoded = prepared.materialize(
                ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { });
        assertEquals(1, READ_CALLBACKS.get());
        assertEquals(StageCommand.Role.WHOLE_GRAPH, decoded.role());
        assertEquals(FENCE, decoded.runAttemptFence());
        assertEquals(CallbackInput.class,
                decoded.fragment().node(0).inputs().get(0).getClass());
    }

    @Test
    void portableWholeGraphCarriesOnlyArtifactBytesUntilWorkerResolution() {
        byte[] artifactBytes = new byte[]{7, 4, 1, 9};
        StageCommand command = StageCommand.wholePortableArtifact(
                artifactBytes).withRunAttemptFence(FENCE);

        byte[] encoded = StageCommandCodecV2.encode(command,
                ObjectTrustContext.localTrusted(getClass().getClassLoader()),
                ignored -> { });
        assertEquals(7, ByteBuffer.wrap(
                encoded, 8, Integer.BYTES).getInt());
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                encoded, ignored -> { });

        assertEquals(StageCommand.Role.WHOLE_GRAPH, prepared.role());
        assertArrayEquals(artifactBytes, prepared.portableArtifactBytes());
        assertThrows(IllegalStateException.class, () -> prepared.materialize(
                ObjectTrustContext.localTrusted(getClass().getClassLoader()),
                ignored -> { }));
    }

    @Test
    void portablePhysicalStageUsesV8WhileBaselineCommandsRemainV7() {
        StageCommand command = StageCommand.sourceStageFanout(
                        META, META, 32, new CallbackInput(), fragment(),
                        Collections.singletonList(new ShuffleEndpoint(
                                "127.0.0.1", 7001)),
                        new String[]{"id"}, false)
                .withPortableStageArtifact(
                        new byte[]{7, 4, 1, 9},
                        new PortableStageSpec(
                                3, Collections.singletonList(0),
                                1, 2, false))
                .withRunAttemptFence(FENCE);

        byte[] encoded = StageCommandCodecV2.encode(command,
                ObjectTrustContext.localTrusted(
                        getClass().getClassLoader()), ignored -> { });
        assertEquals(8, ByteBuffer.wrap(
                encoded, 8, Integer.BYTES).getInt());
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                encoded, ignored -> { });
        assertTrue(prepared.hasPortableArtifact());
        assertEquals(3, prepared.portableStageSpec().logicalStage());
        assertEquals(Collections.singletonList(0),
                prepared.portableStageSpec().members());
        assertEquals(1, prepared.portableStageSpec().instanceIndex());
        assertEquals(2, prepared.portableStageSpec().instanceCount());
    }

    @Test
    void fixedPartitionRoutingUsesExplicitV9MetadataWithoutFieldNameCollision() {
        String userField = "__rowweave_internal_forward_partition_v1__:1";
        DataSchema userMeta = new DataSchema(new Field(
                userField, DataType.IntegerType));
        StageCommand ordinary = StageCommand.sourceStageFanout(
                userMeta, userMeta, 32, new CallbackInput(), fragment(),
                Arrays.asList(new ShuffleEndpoint("127.0.0.1", 7001),
                        new ShuffleEndpoint("127.0.0.1", 7002)),
                new String[]{userField}, false);

        byte[] ordinaryBytes = StageCommandCodecV2.encode(ordinary,
                ObjectTrustContext.localTrusted(
                        getClass().getClassLoader()), ignored -> { });
        assertEquals(7, ByteBuffer.wrap(
                ordinaryBytes, 8, Integer.BYTES).getInt());
        StageCommand ordinaryDecoded = StageCommandCodecV2.preflight(
                ordinaryBytes, ignored -> { }).materialize(
                ObjectTrustContext.localTrusted(
                        getClass().getClassLoader()), ignored -> { });
        assertArrayEquals(new String[]{userField},
                ordinaryDecoded.downKeys());
        assertEquals(-1, ordinaryDecoded.downFixedPartition());

        StageCommand fixed = StageCommand.sourceStageFanout(
                        META, META, 32, new CallbackInput(), fragment(),
                        Arrays.asList(
                                new ShuffleEndpoint("127.0.0.1", 7001),
                                new ShuffleEndpoint("127.0.0.1", 7002)),
                        new String[]{"id"}, false)
                .withDownFixedPartition(1, 2);
        byte[] fixedBytes = StageCommandCodecV2.encode(fixed,
                ObjectTrustContext.localTrusted(
                        getClass().getClassLoader()), ignored -> { });
        assertEquals(9, ByteBuffer.wrap(
                fixedBytes, 8, Integer.BYTES).getInt());
        StageCommand fixedDecoded = StageCommandCodecV2.preflight(
                fixedBytes, ignored -> { }).materialize(
                ObjectTrustContext.localTrusted(
                        getClass().getClassLoader()), ignored -> { });
        assertArrayEquals(new String[]{"id"}, fixedDecoded.downKeys());
        assertEquals(1, fixedDecoded.downFixedPartition());
    }

    @Test
    void complexRoutingMetadataRoundTripsWithoutJavaTopLevelSerialization() {
        UUID inbound = UUID.randomUUID();
        byte[] fingerprint = new byte[DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES];
        Arrays.fill(fingerprint, (byte) 0x5a);
        WorkerDataIdentity destination = WorkerDataIdentity.authenticated(
                "worker-a", "incarnation-a",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-set-a", "authz-set-a", fingerprint);
        ShuffleEndpoint endpoint = ShuffleEndpoint.muxAuthenticated(
                "127.0.0.1", 7001, UUID.randomUUID(), destination);
        StageCommand command = StageCommand.sourceStageFanout(
                        META, META, 32, new CallbackInput(), fragment(),
                        Collections.singletonList(endpoint), new String[]{"id"}, true)
                .withDownTargets(Collections.singletonList(ShuffleTarget.local("local-key")))
                .withBroadcastGroups(Collections.singletonList(new StageCommand.BroadcastGroup(
                        Collections.singletonList(ShuffleTarget.socket(endpoint)),
                        new String[]{"id"}, true, true)
                        .withFixedPartition(0, 1)))
                .withInboundTargetId(inbound)
                .withObjectReuse(false)
                .withRunAttemptFence(FENCE);

        StageCommand decoded = StageCommandCodecV2.preflight(
                StageCommandCodecV2.encode(command,
                        ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { }),
                ignored -> { }).materialize(
                ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { });

        assertEquals(command.role(), decoded.role());
        assertEquals(32, decoded.window());
        assertEquals(endpoint, decoded.downstream().get(0));
        assertEquals(destination, decoded.downstream().get(0).destinationIdentity());
        assertArrayEquals(new String[]{"id"}, decoded.downKeys());
        assertTrue(decoded.downNormalized());
        assertTrue(decoded.downTargets().get(0).isLocal());
        assertTrue(decoded.broadcastGroups().get(0).blocking);
        assertEquals(0,
                decoded.broadcastGroups().get(0).fixedPartition());
        assertEquals(inbound, decoded.inboundTargetId());
        assertFalse(decoded.objectReuse());
        assertEquals(FENCE, decoded.runAttemptFence());
    }

    @Test
    void sourceOwnershipIsExplicitPreflightMetadataAndRoundTripsWithoutOpeningInput() {
        CollectionSource input = new CollectionSource(
                META, "split-a", Collections.emptyList());
        BatchSourceSplitBinding binding = new BatchSourceSplitBinding(
                input, ((BatchSource) input).batchSourceSplit());
        SourceSplitAssignment ownership = SourceSplitAssignment.create(
                "artifact-a", 3, 0, Collections.singletonList(
                        SourceSplitAssignment.SourceGroup.create(
                                "source-a", Collections.singletonList(binding))));
        StageCommand command = StageCommand.sourceStageFanout(
                        META, META, 32,
                        new AssignedSourceBatch(Collections.singletonList(binding)),
                        fragment(), Collections.singletonList(
                                new ShuffleEndpoint("127.0.0.1", 7001)),
                        new String[]{"id"}, false)
                .withSourceSplitAssignment(ownership)
                .withRunAttemptFence(FENCE);

        byte[] encoded = StageCommandCodecV2.encode(command,
                ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { });
        assertEquals(7, ByteBuffer.wrap(encoded, 8, Integer.BYTES).getInt());
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(encoded, ignored -> { });

        assertEquals(1, prepared.sourceSplitAssignment().sourceSplitCount());
        assertArrayEquals(ownership.ownershipDigest(),
                prepared.sourceSplitAssignment().ownershipDigest());
        StageCommand decoded = prepared.materialize(
                ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { });
        assertTrue(decoded.sourceSplitAssignment().matches(ownership));
    }

    @Test
    void digestTrailingAndNestedPlanCorruptionFailInPreflight() {
        byte[] valid = StageCommandCodecV2.encode(
                StageCommand.wholeGraph(fragment()).withRunAttemptFence(FENCE),
                ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { });
        byte[] corrupt = valid.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> StageCommandCodecV2.preflight(corrupt, ignored -> { }));

        byte[] trailing = Arrays.copyOf(valid, valid.length + 1);
        assertThrows(IllegalArgumentException.class,
                () -> StageCommandCodecV2.preflight(trailing, ignored -> { }));
    }

    @Test
    void everyRoleRoundTripsThroughExplicitEnvelope() {
        ShuffleEndpoint endpoint = new ShuffleEndpoint("127.0.0.1", 7001);
        StageCommand[] commands = {
                StageCommand.producer(META, 8, new CallbackInput(), "127.0.0.1", 7001),
                StageCommand.consumer(META, 8, new DiscardSink(META)),
                StageCommand.router(META, 8, new CallbackInput(), new String[]{"id"},
                        Collections.singletonList(endpoint)),
                StageCommand.keyedOp(META, 8, 1, fragment(), endpoint),
                StageCommand.sink(META, 8, 1, fragment(), new DiscardSink(META)),
                StageCommand.wholeGraph(fragment()),
                StageCommand.joinConsumer(META, META, META, 8, 1, 1,
                        new String[]{"id"}, new String[]{"id"}, "INNER", endpoint),
                StageCommand.sourceStage(META, META, 8, new CallbackInput(), fragment(), endpoint)
        };
        ObjectTrustContext trust = ObjectTrustContext.localTrusted(getClass().getClassLoader());
        for (StageCommand command : commands) {
            command.withRunAttemptFence(FENCE);
            StageCommand decoded = StageCommandCodecV2.preflight(
                    StageCommandCodecV2.encode(command, trust, ignored -> { }),
                    ignored -> { }).materialize(trust, ignored -> { });
            assertEquals(command.role(), decoded.role());
            assertEquals(FENCE, decoded.runAttemptFence());
        }
    }

    @Test
    void oversizeConflictObjectSliceAndNestedPlanFailBeforeMaterialization() throws Exception {
        ObjectTrustContext trust = ObjectTrustContext.localTrusted(getClass().getClassLoader());
        char[] chars = new char[PlanGraphEnvelopeScanner.MAX_UTF8_BYTES + 1];
        Arrays.fill(chars, 'x');
        StageCommand oversize = StageCommand.producer(
                META, 8, new CallbackInput(), new String(chars), 7001).withRunAttemptFence(FENCE);
        assertThrows(IllegalArgumentException.class,
                () -> StageCommandCodecV2.encode(oversize, trust, ignored -> { }));

        byte[] whole = StageCommandCodecV2.encode(
                StageCommand.wholeGraph(fragment()).withRunAttemptFence(FENCE),
                trust, ignored -> { });
        ByteBuffer.wrap(whole, 12, Integer.BYTES).putInt(StageCommand.Role.PRODUCER.ordinal());
        resign(whole);
        assertThrows(IllegalArgumentException.class,
                () -> StageCommandCodecV2.preflight(whole, ignored -> { }));

        byte[] objectEnvelope = StageCommandCodecV2.encode(
                StageCommand.producer(META, 8, new CallbackInput(), "127.0.0.1", 7001)
                        .withRunAttemptFence(FENCE), trust, ignored -> { });
        PreparedStageCommand objectPrepared = StageCommandCodecV2.preflight(
                objectEnvelope, ignored -> { });
        PreparedStageCommand.Slice objectSlice = slice(objectPrepared, "input");
        objectEnvelope[objectSlice.offset] ^= 1;
        resign(objectEnvelope);
        assertThrows(IllegalArgumentException.class,
                () -> StageCommandCodecV2.preflight(objectEnvelope, ignored -> { }));

        byte[] planEnvelope = StageCommandCodecV2.encode(
                StageCommand.wholeGraph(fragment()).withRunAttemptFence(FENCE),
                trust, ignored -> { });
        PreparedStageCommand planPrepared = StageCommandCodecV2.preflight(
                planEnvelope, ignored -> { });
        PreparedStageCommand.Slice planSlice = slice(planPrepared, "fragment");
        ByteBuffer.wrap(planEnvelope, planSlice.offset, Integer.BYTES).putInt(0);
        resign(planEnvelope);
        assertThrows(IllegalArgumentException.class,
                () -> StageCommandCodecV2.preflight(planEnvelope, ignored -> { }));
    }

    @Test
    void legacyFallbackIsOnlyEphemeralLoopback() {
        assertTrue(WorkerServer.allowsLegacyStageCommand(ControlSecurityProfile.LOOPBACK, false));
        assertFalse(WorkerServer.allowsLegacyStageCommand(ControlSecurityProfile.LOOPBACK, true));
        assertFalse(WorkerServer.allowsLegacyStageCommand(ControlSecurityProfile.MTLS, false));
        assertFalse(WorkerServer.allowsLegacyStageCommand(ControlSecurityProfile.MTLS, true));
    }

    private static PreparedStageCommand.Slice slice(
            PreparedStageCommand command, String name) throws Exception {
        java.lang.reflect.Field field = PreparedStageCommand.class.getDeclaredField(name);
        field.setAccessible(true);
        return (PreparedStageCommand.Slice) field.get(command);
    }

    private static void resign(byte[] envelope) throws Exception {
        int bodyLength = envelope.length - 32;
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(Arrays.copyOf(envelope, bodyLength));
        System.arraycopy(digest, 0, envelope, bodyLength, digest.length);
    }

    private static PlanGraph fragment() {
        return new PlanGraph(Collections.singletonList(PlanNode.placeholderSource(META)));
    }

    private static final class CallbackInput implements Source {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
        private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
            input.defaultReadObject();
            READ_CALLBACKS.incrementAndGet();
        }
    }
}

