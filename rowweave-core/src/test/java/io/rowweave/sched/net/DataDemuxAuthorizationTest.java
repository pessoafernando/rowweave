package io.rowweave.sched.net;

import io.netty.channel.embedded.EmbeddedChannel;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.mem.RowSerializer;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.MultiProducerChannel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MTLS V1 must authorize OPEN before touching the business ingress. */
class DataDemuxAuthorizationTest {
    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));
    private static final RowSerializer SERIALIZER = new RowSerializer(META);
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-demux-auth", "attempt-demux-auth", 211L);
    private static final UUID TARGET = new UUID(211L, 223L);

    @Test
    void rejectsWrongOpenWithoutWaitingForBusinessTargetOrConsumingGrant() throws Exception {
        DataInboundRegistry inbound = new DataInboundRegistry();
        DataInboundAuthorizationRegistry authorizations = new DataInboundAuthorizationRegistry();
        WorkerDataIdentity source = identity("source", (byte) 1);
        ShuffleProducerGrant grant = grant((byte) 2);
        DataInboundAuthorizationRegistry.PreparedHandle prepared = authorizations.prepare(
                "assignment-demux", FENCE, Collections.singletonList(
                        ShuffleInboundAuthorization.of(TARGET, 1, Collections.singletonList(
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 1, source, grant)))));
        DataDemuxHandler handler = DataDemuxHandler.authenticated(
                inbound, authorizations, source, 17L, 60_000L, Runnable::run);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        assertTimeoutPreemptively(Duration.ofMillis(250), () -> channel.writeInbound(
                new Frame(FrameCodec.FRAME_OPEN, TARGET, 1, grant((byte) 9).copyBytes())));
        Frame rejection = channel.readOutbound();
        assertEquals(FrameCodec.FRAME_ERROR, rejection.type());
        assertEquals(TARGET, rejection.targetId());
        assertEquals(1, rejection.producerOrdinal());
        assertEquals(0, authorizations.openStreamCount());
        channel.runPendingTasks();

        MultiProducerChannel business = new MultiProducerChannel(8, 1, 1024);
        inbound.register(TARGET, "job", business, META, 1, 1024);
        channel.writeInbound(new Frame(
                FrameCodec.FRAME_OPEN, TARGET, 1, grant.copyBytes()));
        Frame credit = channel.readOutbound();
        assertEquals(FrameCodec.FRAME_CREDIT, credit.type());
        assertEquals(1, credit.producerOrdinal());
        assertEquals(1, authorizations.openStreamCount());

        channel.writeInbound(new Frame(FrameCodec.FRAME_RECORD, TARGET, 1,
                SERIALIZER.serialize(row(7))));
        channel.writeInbound(new Frame(FrameCodec.FRAME_EOS, TARGET, 1, new byte[0]));
        assertEquals(7, business.poll().row().get(0));
        assertNull(business.poll());
        assertEquals(0, authorizations.openStreamCount());

        prepared.close();
        channel.finishAndReleaseAll();
        authorizations.close();
        inbound.close();
    }

    @Test
    void isolatesUnauthorizedOrdinalAndKeepsAuthorizedSiblingAlive() throws Exception {
        DataInboundRegistry inbound = new DataInboundRegistry();
        DataInboundAuthorizationRegistry authorizations = new DataInboundAuthorizationRegistry();
        WorkerDataIdentity source = identity("source", (byte) 3);
        ShuffleProducerGrant first = grant((byte) 4);
        ShuffleProducerGrant second = grant((byte) 5);
        DataInboundAuthorizationRegistry.PreparedHandle prepared = authorizations.prepare(
                "assignment-demux", FENCE, Collections.singletonList(
                        ShuffleInboundAuthorization.of(TARGET, 1, Arrays.asList(
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 1, source, first),
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 2, source, second)))));
        MultiProducerChannel business = new MultiProducerChannel(8, 2, 2048);
        inbound.register(TARGET, "job", business, META, 2, 2048);
        EmbeddedChannel channel = new EmbeddedChannel(DataDemuxHandler.authenticated(
                inbound, authorizations, source, 19L, 1000L, Runnable::run));

        channel.writeInbound(new Frame(FrameCodec.FRAME_OPEN, TARGET, 1, first.copyBytes()));
        assertEquals(FrameCodec.FRAME_CREDIT, ((Frame) channel.readOutbound()).type());
        channel.writeInbound(new Frame(FrameCodec.FRAME_OPEN, TARGET, 2,
                grant((byte) 8).copyBytes()));
        Frame rejection = channel.readOutbound();
        assertEquals(FrameCodec.FRAME_ERROR, rejection.type());
        assertEquals(2, rejection.producerOrdinal());

        channel.writeInbound(new Frame(FrameCodec.FRAME_RECORD, TARGET, 1,
                SERIALIZER.serialize(row(31))));
        channel.writeInbound(new Frame(FrameCodec.FRAME_EOS, TARGET, 1, new byte[0]));
        assertEquals(31, business.poll().row().get(0));
        assertEquals(0, authorizations.openStreamCount());
        assertTrue(channel.isActive(), "authorization failure is stream-scoped");

        prepared.close();
        channel.finishAndReleaseAll();
        authorizations.close();
        inbound.close();
    }

    @Test
    void connectionCloseReleasesOnlyItsOpenedAuthorization() throws Exception {
        DataInboundRegistry inbound = new DataInboundRegistry();
        DataInboundAuthorizationRegistry authorizations = new DataInboundAuthorizationRegistry();
        WorkerDataIdentity source = identity("source", (byte) 11);
        ShuffleProducerGrant grant = grant((byte) 12);
        DataInboundAuthorizationRegistry.PreparedHandle prepared = authorizations.prepare(
                "assignment-close", FENCE, Collections.singletonList(
                        ShuffleInboundAuthorization.of(TARGET, 1, Collections.singletonList(
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 1, source, grant)))));
        MultiProducerChannel business = new MultiProducerChannel(8, 1, 1024);
        inbound.register(TARGET, "job", business, META, 1, 1024);
        EmbeddedChannel channel = new EmbeddedChannel(DataDemuxHandler.authenticated(
                inbound, authorizations, source, 23L, 1000L, Runnable::run));

        channel.writeInbound(new Frame(FrameCodec.FRAME_OPEN, TARGET, 1, grant.copyBytes()));
        assertEquals(FrameCodec.FRAME_CREDIT, ((Frame) channel.readOutbound()).type());
        assertEquals(1, authorizations.openStreamCount());
        channel.close();
        channel.runPendingTasks();

        assertEquals(0, authorizations.openStreamCount());
        io.rowweave.sched.stream.StreamEvent failure = business.poll();
        assertTrue(failure.isError());
        assertFalse(allMessages(failure.cause()).contains(TARGET.toString()));
        prepared.close();
        channel.finishAndReleaseAll();
        authorizations.close();
        inbound.close();
    }

    @Test
    void sameSourceOrdinalsShareOneInitialProgressCreditPool() {
        DataInboundRegistry inbound = new DataInboundRegistry();
        DataInboundAuthorizationRegistry authorizations = new DataInboundAuthorizationRegistry();
        WorkerDataIdentity source = identity("source", (byte) 21);
        ShuffleProducerGrant first = grant((byte) 22);
        ShuffleProducerGrant second = grant((byte) 23);
        DataInboundAuthorizationRegistry.PreparedHandle prepared = authorizations.prepare(
                "assignment-credit", FENCE, Collections.singletonList(
                        ShuffleInboundAuthorization.of(TARGET, 1, Arrays.asList(
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 1, source, first),
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 2, source, second)))));
        MultiProducerChannel business = new MultiProducerChannel(8, 2, 1);
        inbound.register(TARGET, "job", business, META, 2, 1);
        EmbeddedChannel channel = new EmbeddedChannel(DataDemuxHandler.authenticated(
                inbound, authorizations, source, 29L, 1000L, Runnable::run));

        channel.writeInbound(new Frame(FrameCodec.FRAME_OPEN, TARGET, 1, first.copyBytes()));
        Frame initial = channel.readOutbound();
        assertEquals(FrameCodec.FRAME_CREDIT, initial.type());
        assertEquals(1, initial.producerOrdinal());
        assertEquals(1, java.nio.ByteBuffer.wrap(initial.payload()).getInt());

        channel.writeInbound(new Frame(FrameCodec.FRAME_OPEN, TARGET, 2, second.copyBytes()));
        assertNull(channel.readOutbound(),
                "same connection-target pool must not duplicate progress credit per ordinal");

        channel.close();
        channel.runPendingTasks();
        prepared.close();
        channel.finishAndReleaseAll();
        authorizations.close();
        inbound.close();
    }

    @Test
    void twoSourcesAcrossTwoTargetsReceiveCrossProgressWithoutTimeoutOrCancellation()
            throws Exception {
        UUID otherTarget = new UUID(227L, 229L);
        DataInboundRegistry inbound = new DataInboundRegistry();
        DataInboundAuthorizationRegistry authorizations =
                new DataInboundAuthorizationRegistry();
        WorkerDataIdentity sourceA = identity("source-a", (byte) 31);
        WorkerDataIdentity sourceB = identity("source-b", (byte) 32);
        ShuffleProducerGrant aToFirst = grant((byte) 33);
        ShuffleProducerGrant bToFirst = grant((byte) 34);
        ShuffleProducerGrant aToSecond = grant((byte) 35);
        ShuffleProducerGrant bToSecond = grant((byte) 36);
        DataInboundAuthorizationRegistry.PreparedHandle prepared = authorizations.prepare(
                "assignment-cross-credit", FENCE, Arrays.asList(
                        ShuffleInboundAuthorization.of(TARGET, 2, Arrays.asList(
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 1, sourceA, aToFirst),
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 2, sourceB, bToFirst))),
                        ShuffleInboundAuthorization.of(otherTarget, 2, Arrays.asList(
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, otherTarget, 1, sourceA, aToSecond),
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, otherTarget, 2, sourceB, bToSecond)))));
        MultiProducerChannel firstBusiness = new MultiProducerChannel(8, 2, 1);
        MultiProducerChannel secondBusiness = new MultiProducerChannel(8, 2, 1);
        inbound.register(TARGET, "job-first", firstBusiness, META, 2, 1);
        inbound.register(otherTarget, "job-second", secondBusiness, META, 2, 1);
        EmbeddedChannel connectionA = new EmbeddedChannel(DataDemuxHandler.authenticated(
                inbound, authorizations, sourceA, 41L, 1000L, Runnable::run));
        EmbeddedChannel connectionB = new EmbeddedChannel(DataDemuxHandler.authenticated(
                inbound, authorizations, sourceB, 43L, 1000L, Runnable::run));

        connectionA.writeInbound(new Frame(
                FrameCodec.FRAME_OPEN, TARGET, 1, aToFirst.copyBytes()));
        connectionA.writeInbound(new Frame(
                FrameCodec.FRAME_OPEN, otherTarget, 1, aToSecond.copyBytes()));
        connectionB.writeInbound(new Frame(
                FrameCodec.FRAME_OPEN, TARGET, 2, bToFirst.copyBytes()));
        connectionB.writeInbound(new Frame(
                FrameCodec.FRAME_OPEN, otherTarget, 2, bToSecond.copyBytes()));
        assertOneByteCredits(connectionA, TARGET, otherTarget);
        assertOneByteCredits(connectionB, TARGET, otherTarget);

        // Crossed first destinations are the classic window=1 cycle: both must progress
        // without a timeout, cancellation, or consuming an unrelated target first.
        connectionA.writeInbound(new Frame(FrameCodec.FRAME_RECORD, otherTarget, 1,
                SERIALIZER.serialize(row(101))));
        connectionB.writeInbound(new Frame(FrameCodec.FRAME_RECORD, TARGET, 2,
                SERIALIZER.serialize(row(202))));
        assertEquals(101, secondBusiness.poll().row().get(0));
        assertEquals(202, firstBusiness.poll().row().get(0));

        connectionA.close();
        connectionB.close();
        connectionA.runPendingTasks();
        connectionB.runPendingTasks();
        prepared.close();
        connectionA.finishAndReleaseAll();
        connectionB.finishAndReleaseAll();
        authorizations.close();
        inbound.close();
    }

    @Test
    void duplicateEosIsRejectedWithoutCompletingAuthorizedSibling() throws Exception {
        DataInboundRegistry inbound = new DataInboundRegistry();
        DataInboundAuthorizationRegistry authorizations =
                new DataInboundAuthorizationRegistry();
        WorkerDataIdentity source = identity("source", (byte) 41);
        ShuffleProducerGrant first = grant((byte) 42);
        ShuffleProducerGrant second = grant((byte) 43);
        DataInboundAuthorizationRegistry.PreparedHandle prepared = authorizations.prepare(
                "assignment-duplicate-eos", FENCE, Collections.singletonList(
                        ShuffleInboundAuthorization.of(TARGET, 1, Arrays.asList(
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 1, source, first),
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 2, source, second)))));
        MultiProducerChannel business = new MultiProducerChannel(8, 2, 1024);
        inbound.register(TARGET, "job", business, META, 2, 1024);
        EmbeddedChannel channel = new EmbeddedChannel(DataDemuxHandler.authenticated(
                inbound, authorizations, source, 47L, 1000L, Runnable::run));

        channel.writeInbound(new Frame(FrameCodec.FRAME_OPEN, TARGET, 1, first.copyBytes()));
        assertEquals(FrameCodec.FRAME_CREDIT, ((Frame) channel.readOutbound()).type());
        channel.writeInbound(new Frame(FrameCodec.FRAME_OPEN, TARGET, 2, second.copyBytes()));
        channel.writeInbound(new Frame(FrameCodec.FRAME_EOS, TARGET, 1, new byte[0]));
        channel.writeInbound(new Frame(FrameCodec.FRAME_EOS, TARGET, 1, new byte[0]));

        Frame rejection = channel.readOutbound();
        assertEquals(FrameCodec.FRAME_ERROR, rejection.type());
        assertEquals(1, rejection.producerOrdinal());
        assertEquals(1, authorizations.openStreamCount(),
                "duplicate EOS must not complete another logical producer");
        channel.writeInbound(new Frame(FrameCodec.FRAME_EOS, TARGET, 2, new byte[0]));
        assertNull(business.poll());

        prepared.close();
        channel.finishAndReleaseAll();
        authorizations.close();
        inbound.close();
    }

    @Test
    void crossConnectionRecordEosAndErrorCannotClaimOpenedStream() throws Exception {
        DataInboundRegistry inbound = new DataInboundRegistry();
        DataInboundAuthorizationRegistry authorizations =
                new DataInboundAuthorizationRegistry();
        WorkerDataIdentity source = identity("source", (byte) 51);
        ShuffleProducerGrant grant = grant((byte) 52);
        DataInboundAuthorizationRegistry.PreparedHandle prepared = authorizations.prepare(
                "assignment-cross-generation", FENCE, Collections.singletonList(
                        ShuffleInboundAuthorization.of(TARGET, 1, Collections.singletonList(
                                ShuffleAuthorizationRecord.authorize(
                                        FENCE, TARGET, 1, source, grant)))));
        MultiProducerChannel business = new MultiProducerChannel(8, 1, 1024);
        inbound.register(TARGET, "job", business, META, 1, 1024);
        EmbeddedChannel owner = new EmbeddedChannel(DataDemuxHandler.authenticated(
                inbound, authorizations, source, 53L, 1000L, Runnable::run));
        owner.writeInbound(new Frame(FrameCodec.FRAME_OPEN, TARGET, 1, grant.copyBytes()));
        assertEquals(FrameCodec.FRAME_CREDIT, ((Frame) owner.readOutbound()).type());

        byte[] types = {FrameCodec.FRAME_RECORD, FrameCodec.FRAME_EOS,
                FrameCodec.FRAME_ERROR};
        for (int i = 0; i < types.length; i++) {
            EmbeddedChannel foreign = new EmbeddedChannel(DataDemuxHandler.authenticated(
                    inbound, authorizations, source, 59L + i, 1000L, Runnable::run));
            byte[] payload = types[i] == FrameCodec.FRAME_RECORD
                    ? SERIALIZER.serialize(row(999)) : new byte[0];
            foreign.writeInbound(new Frame(types[i], TARGET, 1, payload));
            Frame rejection = foreign.readOutbound();
            assertEquals(FrameCodec.FRAME_ERROR, rejection.type());
            assertEquals(1, rejection.producerOrdinal());
            assertEquals(1, authorizations.openStreamCount());
            foreign.finishAndReleaseAll();
        }

        owner.writeInbound(new Frame(FrameCodec.FRAME_RECORD, TARGET, 1,
                SERIALIZER.serialize(row(61))));
        owner.writeInbound(new Frame(FrameCodec.FRAME_EOS, TARGET, 1, new byte[0]));
        assertEquals(61, business.poll().row().get(0));
        assertNull(business.poll());
        assertEquals(0, authorizations.openStreamCount());

        prepared.close();
        owner.finishAndReleaseAll();
        authorizations.close();
        inbound.close();
    }

    private static void assertOneByteCredits(EmbeddedChannel channel,
                                             UUID first, UUID second) {
        java.util.Set<UUID> credited = new java.util.HashSet<>();
        for (int i = 0; i < 2; i++) {
            Frame credit = channel.readOutbound();
            assertEquals(FrameCodec.FRAME_CREDIT, credit.type());
            assertEquals(1, java.nio.ByteBuffer.wrap(credit.payload()).getInt());
            credited.add(credit.targetId());
        }
        assertEquals(new java.util.HashSet<>(Arrays.asList(first, second)), credited);
        assertNull(channel.readOutbound());
    }

    private static DataRow row(int value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static ShuffleProducerGrant grant(byte fill) {
        byte[] bytes = new byte[ShuffleProducerGrant.BYTES];
        Arrays.fill(bytes, fill);
        return ShuffleProducerGrant.fromBytes(bytes);
    }

    private static WorkerDataIdentity identity(String worker, byte fill) {
        byte[] fingerprint = new byte[32];
        Arrays.fill(fingerprint, fill);
        return WorkerDataIdentity.authenticated(worker, "inc-" + worker,
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-a", "authz-a", fingerprint);
    }

    private static String allMessages(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current.getMessage() != null) messages.append(current.getMessage()).append('\n');
        }
        return messages.toString();
    }
}
