package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.net.ShuffleAuthorizationRecord;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.net.ShuffleInboundAuthorization;
import io.rowweave.sched.net.ShuffleProducerGrant;
import io.rowweave.sched.net.WorkerDataIdentity;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** producer bearer and consumer digest are explicit bounded command fields. */
class StageCommandGrantCodecTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-codec-grant", "attempt-1", 31L);

    @Test
    void currentSchemaRoundTripsProducerBearerAndConsumerDigestWithoutMixingThem() {
        UUID target = new UUID(41L, 43L);
        ShuffleProducerGrant grant = grant((byte) 0x55);
        WorkerDataIdentity source = identity("source", (byte) 1);
        WorkerDataIdentity destination = identity("destination", (byte) 2);
        byte[] cohort = filled(32, (byte) 0x33);
        ShuffleEndpoint endpoint = ShuffleEndpoint.muxAuthorized(
                "127.0.0.1", 7501, target, destination,
                grant, 1, cohort, 1);

        StageCommand producer = StageCommand.sourceStageFanout(
                        META, META, 8, new io.rowweave.CollectionSource(
                                META, "split", Collections.emptyList()),
                        fragment(), Collections.singletonList(endpoint),
                        new String[]{"id"}, false)
                .withRunAttemptFence(FENCE);
        byte[] producerWire = encode(producer);
        assertEquals(7, ByteBuffer.wrap(producerWire, 8, Integer.BYTES).getInt());
        StageCommand producerDecoded = decode(producerWire);
        ShuffleEndpoint endpointDecoded = producerDecoded.downstream().get(0);
        assertArrayEquals(grant.copyBytes(), endpointDecoded.producerGrant().copyBytes());
        assertEquals(1, endpointDecoded.producerOrdinal());
        assertArrayEquals(cohort, endpointDecoded.cohortId());

        ShuffleAuthorizationRecord record = ShuffleAuthorizationRecord.authorize(
                FENCE, target, 1, source, grant);
        ShuffleInboundAuthorization inbound = ShuffleInboundAuthorization.of(
                target, 1, Collections.singletonList(record));
        StageCommand consumer = StageCommand.consumer(META, 8, new DiscardSink(META))
                .withInboundTargetId(target)
                .withInboundAuthorizations(Collections.singletonList(inbound))
                .withRunAttemptFence(FENCE);
        byte[] consumerWire = encode(consumer);
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                consumerWire, ignored -> { });
        assertEquals(1, prepared.inboundAuthorizationCount());
        StageCommand consumerDecoded = prepared.materialize(trust(), ignored -> { });
        ShuffleAuthorizationRecord decodedRecord = consumerDecoded
                .inboundAuthorizations().get(0).records().get(0);
        assertArrayEquals(record.authorizationDigest(), decodedRecord.authorizationDigest());
        assertEquals(source, decodedRecord.expectedSourceIdentity());
    }

    @Test
    void aggregateRejectsDuplicateWrongTargetPoolCountAndMoreThan4096BeforeCopy() {
        UUID target = new UUID(47L, 53L);
        ShuffleProducerGrant grant = grant((byte) 7);
        WorkerDataIdentity source = identity("source", (byte) 3);
        ShuffleAuthorizationRecord record = ShuffleAuthorizationRecord.authorize(
                FENCE, target, 1, source, grant);
        assertThrows(IllegalArgumentException.class, () -> ShuffleInboundAuthorization.of(
                target, 1, Arrays.asList(record, record)));
        assertThrows(IllegalArgumentException.class, () -> ShuffleInboundAuthorization.of(
                new UUID(59L, 61L), 1, Collections.singletonList(record)));
        assertThrows(IllegalArgumentException.class, () -> ShuffleInboundAuthorization.of(
                target, 2, Collections.singletonList(record)));

        List<ShuffleAuthorizationRecord> excessive = new ArrayList<>(4097);
        for (int i = 0; i < 4097; i++) excessive.add(record);
        assertThrows(IllegalArgumentException.class,
                () -> ShuffleInboundAuthorization.of(target, 1, excessive));
    }

    @Test
    void codecRejectsGrantWithoutFenceAndAuthorizationTargetMismatch() {
        UUID target = new UUID(67L, 71L);
        UUID wrong = new UUID(73L, 79L);
        ShuffleProducerGrant grant = grant((byte) 13);
        WorkerDataIdentity source = identity("source", (byte) 14);
        WorkerDataIdentity destination = identity("destination", (byte) 15);
        ShuffleEndpoint endpoint = ShuffleEndpoint.muxAuthorized(
                "127.0.0.1", 7701, target, destination, grant,
                1, filled(32, (byte) 16), 1);
        StageCommand unfencedProducer = StageCommand.sourceStageFanout(
                META, META, 8,
                new io.rowweave.CollectionSource(
                        META, "split", Collections.emptyList()),
                fragment(), Collections.singletonList(endpoint),
                new String[]{"id"}, false);
        assertThrows(IllegalArgumentException.class,
                () -> encode(unfencedProducer));

        ShuffleInboundAuthorization inbound = ShuffleInboundAuthorization.of(
                target, 1, Collections.singletonList(
                        ShuffleAuthorizationRecord.authorize(
                                FENCE, target, 1, source, grant)));
        StageCommand mismatchedConsumer = StageCommand.consumer(
                        META, 8, new DiscardSink(META))
                .withInboundTargetId(wrong)
                .withInboundAuthorizations(Collections.singletonList(inbound))
                .withRunAttemptFence(FENCE);
        assertThrows(IllegalArgumentException.class,
                () -> encode(mismatchedConsumer));

        StageCommand unfencedConsumer = StageCommand.consumer(
                        META, 8, new DiscardSink(META))
                .withInboundTargetId(target)
                .withInboundAuthorizations(Collections.singletonList(inbound));
        assertThrows(IllegalArgumentException.class,
                () -> encode(unfencedConsumer));
    }

    private static byte[] encode(StageCommand command) {
        return StageCommandCodecV2.encode(command, trust(), ignored -> { });
    }

    private static StageCommand decode(byte[] wire) {
        return StageCommandCodecV2.preflight(wire, ignored -> { })
                .materialize(trust(), ignored -> { });
    }

    private static ObjectTrustContext trust() {
        return ObjectTrustContext.localTrusted(StageCommandGrantCodecTest.class.getClassLoader());
    }

    private static io.rowweave.plan.PlanGraph fragment() {
        return new io.rowweave.plan.PlanGraph(Collections.singletonList(
                io.rowweave.plan.PlanNode.placeholderSource(META)));
    }

    private static ShuffleProducerGrant grant(byte fill) {
        return ShuffleProducerGrant.fromBytes(filled(32, fill));
    }

    private static byte[] filled(int length, byte fill) {
        byte[] value = new byte[length];
        Arrays.fill(value, fill);
        return value;
    }

    private static WorkerDataIdentity identity(String id, byte fill) {
        return WorkerDataIdentity.authenticated(id, "inc-" + id,
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-a", "authz-a", filled(32, fill));
    }
}
