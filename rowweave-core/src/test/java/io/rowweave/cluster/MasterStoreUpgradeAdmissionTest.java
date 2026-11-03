package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.ControlMessageId;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.ProtocolHelloConfig;
import io.rowweave.cluster.rpc.ProtocolHelloV1;
import io.rowweave.cluster.rpc.ProtocolHelloV1Codec;
import io.rowweave.cluster.rpc.ProtocolRevisionRange;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.upgrade.FileUpgradeAuthority;
import io.rowweave.upgrade.StoreFormatHeader;
import io.rowweave.upgrade.UpgradeAuthorityRecord;
import io.rowweave.upgrade.UpgradeCommand;
import io.rowweave.upgrade.UpgradeOwner;
import io.rowweave.upgrade.UpgradeTransition;
import io.rowweave.upgrade.UpgradeAuthorityKernel;
import io.rowweave.upgrade.UpgradeAuthorityStore;
import io.rowweave.upgrade.UpgradePhase;
import io.rowweave.upgrade.WriterRevisionRejectedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MasterStoreUpgradeAdmissionTest {
    @TempDir
    Path temp;

    @Test
    void writerPromotionIsAuthorityBoundAndLateOldWorkersCannotResumeWrites() {
        ProtocolHelloV1 current = hello(0, 1);
        ProtocolHelloV1 previous = hello(0, 0);
        Properties properties = properties(current);
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(properties);
        RecordingAuthority authority = new RecordingAuthority(verifiedRecord());
        InProcessRegistry registry = new InProcessRegistry(authority);
        registry.register(worker("old", previous));

        try (DeploymentRuntime runtime = DeploymentRuntime.load(
                     properties, security, Runtime.getRuntime().maxMemory());
             DeploymentRuntime previousRuntime = DeploymentRuntime.load(
                     properties(previous), security,
                     Runtime.getRuntime().maxMemory(), null);
             DeploymentRuntime replacementRuntime = DeploymentRuntime.load(
                     properties(current), security,
                     Runtime.getRuntime().maxMemory(), null);
             MasterServer master = new MasterServer(5_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP, null, runtime)) {
            master.startFixedLeadershipServices();
            WriterRevisionRejectedException blocked = assertThrows(
                    WriterRevisionRejectedException.class,
                    () -> master.applyStoreUpgrade(command(authority.read(),
                            UpgradeCommand.Kind.CUTOVER, "cutover-old"), 1_000L));
            assertEquals("ACTIVE_WRITER_REVISION_UNAVAILABLE", blocked.code());
            assertEquals(0, authority.applyCalls);
            assertEquals(UpgradePhase.VERIFIED, authority.read().phase());

            registry.report(worker("old", current));
            UpgradeTransition cutover = master.applyStoreUpgrade(command(
                    authority.read(), UpgradeCommand.Kind.CUTOVER,
                    "cutover-current"), 1_000L);
            assertEquals(UpgradeTransition.Code.ACCEPTED, cutover.code());
            assertEquals(1, authority.applyCalls);
            assertEquals(1, authority.read().activeHeader().activeRevision());

            FixedMasterLocator locator = new FixedMasterLocator(
                    new Endpoint("127.0.0.1", master.port()));
            try (WorkerServer lateOld = WorkerServer.newUnguarded(
                    "late-old", 1, locator,
                    temp.resolve("late-old").toFile(), previousRuntime)) {
                assertFalse(schedulableWorkers(master).stream().anyMatch(
                        candidate -> "late-old".equals(candidate.workerId())),
                        "active revision must fence a late old worker from assignment");
                assertThrows(WriterRevisionRejectedException.class,
                        () -> master.applyStoreUpgrade(command(authority.read(),
                                UpgradeCommand.Kind.RESUME_WRITES,
                                "resume-old"), 1_000L));
                assertEquals(1, authority.applyCalls,
                        "rejected resume must not reach the authority CAS");
            }

            try (WorkerServer replacement = WorkerServer.newUnguarded(
                    "late-old", 1, locator,
                    temp.resolve("replacement").toFile(), replacementRuntime)) {
                assertTrue(schedulableWorkers(master).stream().anyMatch(
                        candidate -> "late-old".equals(candidate.workerId())));
                UpgradeTransition resumed = master.applyStoreUpgrade(command(
                        authority.read(), UpgradeCommand.Kind.RESUME_WRITES,
                        "resume-current"), 1_000L);
                assertEquals(UpgradeTransition.Code.ACCEPTED, resumed.code());
                assertTrue(resumed.record().writesAllowed());
            }
        }
    }

    @Test
    void activeLeaderFreezesAllSubmitTypesBeforePayloadDecoding()
            throws Exception {
        Files.createDirectories(temp.resolve("namespaces/legacy"));
        FileUpgradeAuthority authority = new FileUpgradeAuthority(temp);
        UpgradeOwner owner = new UpgradeOwner(1L, "master", "inc", 1L);
        UpgradeAuthorityRecord idle = authority.bootstrap(
                new StoreFormatHeader(1, "store", 1, 0, digest('a')),
                new StoreFormatHeader(1, "store", 1, 1, digest('b')),
                1L, "legacy", "shadow", owner, 5_000L);
        InProcessRegistry registry = new InProcessRegistry(authority);

        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            UpgradeTransition frozen = master.applyStoreUpgrade(command(
                    idle, UpgradeCommand.Kind.FREEZE_ADMISSION, "freeze"),
                    1_000L);
            assertEquals(UpgradeTransition.Code.ACCEPTED, frozen.code());

            RpcMessage[] requests = {
                    RpcMessage.submitJob(new byte[]{1}),
                    RpcMessage.submitPlanArtifact(new byte[]{1}, "publication"),
                    RpcMessage.submitRun(new byte[]{1}),
                    RpcMessage.submitIncrementalRun(new byte[]{1})
            };
            for (RpcMessage request : requests) {
                RpcMessage response = master.handler().apply(request);
                assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
                assertFalse(response.success());
                assertTrue(response.errorMessage().contains(
                        "STORE_UPGRADE_ADMISSION_FROZEN"));
            }
        }
    }

    private static UpgradeCommand command(
            UpgradeAuthorityRecord record, UpgradeCommand.Kind kind,
            String commandId) {
        return new UpgradeCommand(kind, commandId, "release", "store", 1L,
                "legacy", record.sourceDigest(), "shadow",
                record.shadowDigest(), record.owner(),
                record.activePointerVersion(), 0L, 0,
                record.sourceDigest(), record.shadowDigest(), 5_000L);
    }

    private static UpgradeAuthorityRecord verifiedRecord() {
        StoreFormatHeader source = new StoreFormatHeader(
                1, "store", 1, 0, digest('a'));
        StoreFormatHeader target = new StoreFormatHeader(
                1, "store", 1, 1, digest('a'));
        return new UpgradeAuthorityRecord(1, source, target, 1L,
                UpgradePhase.VERIFIED, "legacy", digest('c'), "shadow",
                digest('c'), new UpgradeOwner(1L, "master", "inc", 1L),
                0L, "legacy", 0L, 1L, 1L, 1,
                digest('c'), digest('c'), 5_000L, false, false,
                Collections.emptyList());
    }

    private static RegistryWorker worker(String id, ProtocolHelloV1 hello) {
        return new RegistryWorker(id, "127.0.0.1", 19_000, 1, -1,
                "inc-" + id, null, null, hello);
    }

    @SuppressWarnings("unchecked")
    private static List<RegistryWorker> schedulableWorkers(MasterServer master) {
        try {
            Method method = MasterServer.class.getDeclaredMethod(
                    "schedulableWorkers");
            method.setAccessible(true);
            return (List<RegistryWorker>) method.invoke(master);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError(failure);
        }
    }

    private static Properties properties(ProtocolHelloV1 hello) {
        Properties properties = new Properties();
        properties.setProperty(ProtocolHelloConfig.HELLO_HEX_PROPERTY,
                hex(ProtocolHelloV1Codec.encode(hello)));
        return properties;
    }

    private static ProtocolHelloV1 hello(int minimumWriter, int maximumWriter) {
        List<Integer> messageIds = new ArrayList<Integer>();
        for (RpcMessage.Type type : RpcMessage.Type.values()) {
            messageIds.add(ControlMessageId.wireId(type));
        }
        return new ProtocolHelloV1(digest('a'), 1,
                new ProtocolRevisionRange(0, 1),
                new ProtocolRevisionRange(minimumWriter, maximumWriter),
                messageIds, Collections.singleton(1), Collections.singleton(7),
                Collections.singleton(1), Collections.singleton(1),
                Collections.singleton("rowweave.runtime.batch.v1"),
                Collections.<Integer>emptySet(),
                Collections.<Integer, byte[]>emptyMap(),
                80 * 1024 * 1024, 128L * 1024 * 1024);
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }

    private static final class RecordingAuthority
            implements UpgradeAuthorityStore {
        private UpgradeAuthorityRecord record;
        private int applyCalls;

        private RecordingAuthority(UpgradeAuthorityRecord record) {
            this.record = record;
        }

        @Override
        public UpgradeAuthorityRecord read() {
            return record;
        }

        @Override
        public UpgradeTransition apply(UpgradeCommand command,
                                       long activeRunCount,
                                       long activeIntervalCount,
                                       long authorityTimeMillis) {
            applyCalls++;
            UpgradeTransition transition = UpgradeAuthorityKernel.apply(
                    record, command, activeRunCount, activeIntervalCount,
                    authorityTimeMillis);
            if (transition.code() == UpgradeTransition.Code.ACCEPTED) {
                record = transition.record();
            }
            return transition;
        }
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
