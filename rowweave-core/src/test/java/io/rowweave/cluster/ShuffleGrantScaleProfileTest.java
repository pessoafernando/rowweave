package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.net.DataInboundAuthorizationRegistry;
import io.rowweave.sched.net.ShuffleAuthorizationRecord;
import io.rowweave.sched.net.ShuffleInboundAuthorization;
import io.rowweave.sched.net.ShuffleProducerGrant;
import io.rowweave.sched.net.WorkerDataIdentity;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** wire and consumer-registry profiles at declared producer scales. */
@Tag("profile")
class ShuffleGrantScaleProfileTest {
    private static final int[] PRODUCER_COUNTS = {1, 64, 4096};
    private static final long MAX_COMMAND_BYTES = 1024L * 1024L;
    private static final long MAX_CODEC_RESIDENT_BYTES = 8L * 1024L * 1024L;
    private static final long MAX_REGISTRY_RESIDENT_BYTES = 2L * 1024L * 1024L;
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-grant-scale", "attempt-1", 313L);

    @Test
    void oneSixtyFourAndMaximumProducerCommandsStayWithinWireAndResidentBounds() {
        for (int producerCount : PRODUCER_COUNTS) {
            Profile profile = profile(producerCount);
            System.out.printf("[SHUFFLE-GRANT-PROFILE] producers=%d command=%dB "
                            + "codecResident=%dB registryResident=%dB%n",
                    producerCount, profile.commandBytes,
                    profile.codecResidentBytes, profile.registryResidentBytes);
        }
    }

    private static Profile profile(int producerCount) {
        UUID target = new UUID(317L, producerCount);
        WorkerDataIdentity source = sourceIdentity();
        List<ShuffleAuthorizationRecord> records = new ArrayList<>(producerCount);
        for (int ordinal = 1; ordinal <= producerCount; ordinal++) {
            records.add(ShuffleAuthorizationRecord.authorize(
                    FENCE, target, ordinal, source, grant(ordinal)));
        }
        ShuffleInboundAuthorization authorization = ShuffleInboundAuthorization.of(
                target, 1, records);
        StageCommand command = StageCommand.consumer(
                        META, 1, new DiscardSink(META))
                .withInboundTargetId(target)
                .withInboundAuthorizations(Collections.singletonList(authorization))
                .withRunAttemptFence(FENCE);

        ObjectTrustContext trust = ObjectTrustContext.localTrusted(
                ShuffleGrantScaleProfileTest.class.getClassLoader());
        AtomicLong codecResident = new AtomicLong();
        byte[] wire = StageCommandCodecV2.encode(
                command, trust, codecResident::addAndGet);
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                wire, codecResident::addAndGet);
        StageCommand decoded = prepared.materialize(
                trust, codecResident::addAndGet);
        assertEquals(producerCount,
                decoded.inboundAuthorizations().get(0).records().size());
        assertTrue(wire.length <= MAX_COMMAND_BYTES,
                "command wire bound at producers=" + producerCount);
        assertTrue(codecResident.get() <= MAX_CODEC_RESIDENT_BYTES,
                "codec resident bound at producers=" + producerCount);

        ControlResidentBudget budget = ControlResidentBudget.forTesting(
                MAX_REGISTRY_RESIDENT_BYTES);
        DataInboundAuthorizationRegistry registry =
                new DataInboundAuthorizationRegistry(budget);
        DataInboundAuthorizationRegistry.PreparedHandle handle = registry.prepare(
                "assignment-" + producerCount, FENCE,
                Collections.singletonList(authorization));
        long registryResident = budget.snapshot().usedBytes();
        assertTrue(registryResident <= MAX_REGISTRY_RESIDENT_BYTES,
                "registry resident bound at producers=" + producerCount);
        handle.close();
        assertEquals(0L, budget.snapshot().usedBytes());
        registry.close();
        return new Profile(wire.length, codecResident.get(), registryResident);
    }

    private static ShuffleProducerGrant grant(int ordinal) {
        byte[] bytes = new byte[ShuffleProducerGrant.BYTES];
        Arrays.fill(bytes, (byte) 0x5a);
        ByteBuffer.wrap(bytes).putInt(bytes.length - Integer.BYTES, ordinal);
        return ShuffleProducerGrant.fromBytes(bytes);
    }

    private static WorkerDataIdentity sourceIdentity() {
        byte[] fingerprint = new byte[32];
        Arrays.fill(fingerprint, (byte) 0x6b);
        return WorkerDataIdentity.authenticated(
                "source", "inc-source",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-a", "authz-a", fingerprint);
    }

    private static final class Profile {
        final int commandBytes;
        final long codecResidentBytes;
        final long registryResidentBytes;

        Profile(int commandBytes, long codecResidentBytes,
                long registryResidentBytes) {
            this.commandBytes = commandBytes;
            this.codecResidentBytes = codecResidentBytes;
            this.registryResidentBytes = registryResidentBytes;
        }
    }
}
