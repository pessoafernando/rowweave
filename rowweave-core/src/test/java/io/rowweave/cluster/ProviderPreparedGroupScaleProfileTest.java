package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.RpcCodec;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** maximum provider group control and resident profile. */
@Tag("profile")
class ProviderPreparedGroupScaleProfileTest {
    private static final int MAX_PROVIDER_SINKS = 4095;
    private static final long CONTROL_RESIDENT_LIMIT = 256L * 1024L * 1024L;
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void maximumProviderGroupCrossesCanonicalCodecAndLargeRpcFrame()
            throws Exception {
        Fixture fixture = fixture(MAX_PROVIDER_SINKS);
        byte[] command = ProviderPreparedGroupCommandCodec.encode(
                new ProviderPreparedGroupCommandCodec.Command(
                        ProviderPreparedGroupCommandCodec.Action.QUERY,
                        fixture.outputs, fixture.locators, digest(7)));
        assertTrue(command.length
                <= ProviderPreparedGroupCommandCodec.MAX_FRAME_BYTES);

        AtomicLong resident = new AtomicLong();
        ProviderPreparedGroupCommandCodec.Command decoded =
                ProviderPreparedGroupCommandCodec.decode(
                        command, resident::addAndGet);
        assertEquals(MAX_PROVIDER_SINKS, decoded.outputs.size());
        assertEquals(MAX_PROVIDER_SINKS, decoded.locators.size());
        assertTrue(resident.get() <= CONTROL_RESIDENT_LIMIT,
                "provider control resident bytes=" + resident.get());

        byte[] rpc = RpcCodec.encode(RpcMessage.executeProviderGroup(
                "worker", 1L, "provider-profile-group", "publication-1",
                1L, command, "incarnation", fixture.fence));
        assertTrue(rpc.length <= 80 * 1024 * 1024);
        System.out.printf("[R4-PROVIDER-GROUP-PROFILE] sinks=%d command=%dB "
                        + "rpc=%dB resident=%dB%n",
                MAX_PROVIDER_SINKS, command.length, rpc.length,
                resident.get());
    }

    @Test
    void synthetic4096MemberControlRecordIsRejectedBeforeEncoding()
            throws Exception {
        Fixture fixture = fixture(MAX_PROVIDER_SINKS);
        fixture.outputs.add(fixture.outputs.get(0));
        fixture.locators.add(fixture.locators.get(0));
        assertThrows(IllegalArgumentException.class,
                () -> new ProviderPreparedGroupCommandCodec.Command(
                        ProviderPreparedGroupCommandCodec.Action.PUBLISH,
                        fixture.outputs, fixture.locators, digest(9)));
    }

    private static Fixture fixture(int count) throws Exception {
        RunAttemptFence fence = new RunAttemptFence(
                "provider-profile-run", "attempt-1", 1L);
        List<StageAssignmentRecord> records = new ArrayList<>(count);
        for (int ordinal = 0; ordinal < count; ordinal++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, ordinal, ordinal + 1L, ordinal, 0,
                    StageAssignmentRole.SINK, "worker", "127.0.0.1",
                    7100, -1, "incarnation"));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "provider-profile-artifact", 1L, records);
        List<ProviderBatchSink> outputs = new ArrayList<>(count);
        List<PreparedSinkLocator> locators = new ArrayList<>(count);
        DurableSinkProviderKey providerKey = new DurableSinkProviderKey(
                "rowweave.manifest-file", 1,
                io.rowweave.BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1);
        DurableSinkCodecDescriptor locatorCodec =
                new DurableSinkCodecDescriptor(
                        "rowweave.manifest-member", 1, 4096);
        String root = Paths.get(System.getProperty("java.io.tmpdir"),
                "rowweave-r4-provider-profile").toString();
        for (int ordinal = 0; ordinal < count; ordinal++) {
            String member = String.format("m%04d", ordinal);
            ProviderBatchSink output = new ManifestFileSink(
                    META, root, member).toProviderSink();
            outputs.add(output);
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    manifest, records.get(ordinal), "output-" + ordinal,
                    output.batchSinkDescriptor());
            byte[] payload = member.getBytes(StandardCharsets.UTF_8);
            locators.add(new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    identity.capabilityProfile(), identity.commitDomainId(),
                    providerKey, locatorCodec, payload,
                    MessageDigest.getInstance("SHA-256").digest(payload),
                    1L, digest(ordinal), ordinal));
        }
        return new Fixture(fence, outputs, locators);
    }

    private static byte[] digest(int value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(new byte[] {
                (byte) (value >>> 24), (byte) (value >>> 16),
                (byte) (value >>> 8), (byte) value});
    }

    private static final class Fixture {
        private final RunAttemptFence fence;
        private final List<ProviderBatchSink> outputs;
        private final List<PreparedSinkLocator> locators;

        private Fixture(
                RunAttemptFence fence,
                List<ProviderBatchSink> outputs,
                List<PreparedSinkLocator> locators) {
            this.fence = fence;
            this.outputs = outputs;
            this.locators = locators;
        }
    }
}
