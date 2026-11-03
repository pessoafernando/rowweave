package io.rowweave.cluster;

import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.net.ShuffleInboundAuthorization;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** one immutable route closure is the only local/remote and grant decision. */
class ShuffleTransportPlanTest {

    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-transport", "attempt-transport", 71L);

    @Test
    void remoteCartesianRoutesHaveUniqueGrantStableOrdinalsAndConsumerProjection() {
        ExecutionGraph graph = new ExecutionGraph(Arrays.asList(
                stage(0, Collections.emptyList(), "source"),
                stage(1, Collections.singletonList(0), "groupBy")));
        PhysicalPlan physical = physical(
                instance(0, 0, "w1"), instance(0, 1, "w1"),
                instance(1, 0, "w0"));

        ShuffleTransportPlan plan = ShuffleTransportPlan.create(
                graph, physical, workers(), FENCE, "job-a", true, seededRandom());
        ShuffleGrantPlan grants = ShuffleGrantPlan.create(plan, seededRandom());

        assertEquals(2, plan.routes().size());
        assertEquals(2, plan.remoteRouteCount());
        ShuffleTransportPlan.Route first = plan.routes().get(0);
        ShuffleTransportPlan.Route second = plan.routes().get(1);
        assertEquals(first, plan.route(1, 0, 0, 0, 0));
        assertEquals(second, plan.route(1, 0, 0, 0, 1));
        assertEquals(1, first.producerOrdinal());
        assertEquals(2, second.producerOrdinal());
        assertEquals(first.targetId(), second.targetId());
        assertFalse(Arrays.equals(grants.producerEndpoint(first.key())
                        .producerGrant().copyBytes(),
                grants.producerEndpoint(second.key()).producerGrant().copyBytes()));
        assertArrayEquals(grants.producerEndpoint(first.key()).cohortId(),
                grants.producerEndpoint(second.key()).cohortId());
        assertEquals(2, grants.producerEndpoint(first.key()).expectedOrdinalCount());

        ShuffleInboundAuthorization inbound = grants.consumerAuthorization(1, 0, 0);
        assertNotNull(inbound);
        assertEquals(2, inbound.records().size());
        assertEquals(1, inbound.expectedPoolCount(),
                "same exact source Worker generation shares one connection-target pool");
        assertArrayEquals(grants.producerEndpoint(first.key()).producerGrant().authorizationDigest(
                        FENCE, first.targetId(), first.producerOrdinal()),
                inbound.records().get(0).authorizationDigest());
        assertEquals(Collections.singletonList(inbound),
                grants.consumerAuthorizations(1, 0));
    }

    @Test
    void localRoutesNeverReceiveTargetGrantOrConsumerAuthorization() {
        ExecutionGraph graph = new ExecutionGraph(Arrays.asList(
                stage(0, Collections.emptyList(), "source"),
                stage(1, Collections.singletonList(0), "groupBy")));
        PhysicalPlan physical = physical(
                instance(0, 0, "w0"), instance(1, 0, "w0"));

        ShuffleTransportPlan plan = ShuffleTransportPlan.create(
                graph, physical, workers(), FENCE, "job-local", true, seededRandom());
        ShuffleTransportPlan.Route route = plan.routes().get(0);
        assertTrue(route.isLocal());
        assertNull(route.targetId());
        ShuffleGrantPlan grants = ShuffleGrantPlan.create(plan, seededRandom());
        assertNull(grants.producerEndpoint(route.key()));
        assertNotNull(route.localKey());
        assertEquals(0, plan.remoteRouteCount());
        assertNull(grants.consumerAuthorization(1, 0, 0));
    }

    @Test
    void selfJoinEdgesRemainDistinctWhileUnionEdgesShareOneTargetAndOrdinalSpace() {
        ExecutionGraph selfJoin = new ExecutionGraph(Arrays.asList(
                stage(0, Collections.emptyList(), "source"),
                stage(1, Arrays.asList(0, 0), "join")));
        PhysicalPlan physical = physical(
                instance(0, 0, "w1"), instance(1, 0, "w0"));
        ShuffleTransportPlan joinPlan = ShuffleTransportPlan.create(
                selfJoin, physical, workers(), FENCE, "job-self", false, seededRandom());
        assertEquals(2, joinPlan.routes().size());
        assertNotEquals(joinPlan.routes().get(0).key(), joinPlan.routes().get(1).key());
        assertNotEquals(joinPlan.routes().get(0).targetId(), joinPlan.routes().get(1).targetId());

        ExecutionGraph union = new ExecutionGraph(Arrays.asList(
                stage(0, Collections.emptyList(), "source"),
                stage(1, Collections.emptyList(), "source"),
                stage(2, Arrays.asList(0, 1), "union")));
        PhysicalPlan unionPhysical = physical(
                instance(0, 0, "w1"), instance(1, 0, "w1"),
                instance(2, 0, "w0"));
        ShuffleTransportPlan unionPlan = ShuffleTransportPlan.create(
                union, unionPhysical, workers(), FENCE, "job-union", false, seededRandom());
        ShuffleGrantPlan unionGrants = ShuffleGrantPlan.create(unionPlan, seededRandom());
        assertEquals(unionPlan.routes().get(0).targetId(), unionPlan.routes().get(1).targetId());
        assertEquals(1, unionPlan.routes().get(0).producerOrdinal());
        assertEquals(2, unionPlan.routes().get(1).producerOrdinal());
        assertEquals(2, unionGrants.consumerAuthorization(2, 0, 0).records().size());
        assertEquals(unionGrants.consumerAuthorization(2, 0, 0).targetId(),
                unionGrants.consumerAuthorization(2, 1, 0).targetId());
        assertEquals(1, unionGrants.consumerAuthorizations(2, 0).size(),
                "union edge aliases must install one target aggregate");
    }

    @Test
    void missingWorkerOrMoreThan4096RemoteProducersFailsBeforeAnyCommandExists() {
        ExecutionGraph graph = new ExecutionGraph(Arrays.asList(
                stage(0, Collections.emptyList(), "source"),
                stage(1, Collections.singletonList(0), "groupBy")));
        PhysicalPlan missing = physical(
                instance(0, 0, "missing"), instance(1, 0, "w0"));
        assertThrows(IllegalArgumentException.class, () -> ShuffleTransportPlan.create(
                graph, missing, workers(), FENCE, "job-missing", false, seededRandom()));

        java.util.ArrayList<PhysicalPlan.Instance> excessive = new java.util.ArrayList<>();
        for (int i = 0; i < 4097; i++) excessive.add(instance(0, i, "w1"));
        excessive.add(instance(1, 0, "w0"));
        PhysicalPlan tooMany = new PhysicalPlan(4097, excessive,
                Collections.singletonList(new int[]{0, 1}), false);
        assertThrows(IllegalArgumentException.class, () -> ShuffleTransportPlan.create(
                graph, tooMany, workers(), FENCE, "job-large", false, seededRandom()));
    }

    private static Stage stage(int index, List<Integer> inputs, String headType) {
        return new Stage(index, Collections.singletonList(index), inputs,
                false, headType);
    }

    private static PhysicalPlan.Instance instance(int stage, int index, String worker) {
        return new PhysicalPlan.Instance(stage, index, worker, stage != 0);
    }

    private static PhysicalPlan physical(PhysicalPlan.Instance... instances) {
        return new PhysicalPlan(2, Arrays.asList(instances),
                Collections.emptyList(), false);
    }

    private static SecureRandom seededRandom() {
        try {
            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(new byte[]{1, 3, 5, 7});
            return random;
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static Map<String, RegistryWorker> workers() {
        Map<String, RegistryWorker> workers = new LinkedHashMap<>();
        workers.put("w0", worker("w0", 7600, (byte) 1));
        workers.put("w1", worker("w1", 7601, (byte) 2));
        return workers;
    }

    private static RegistryWorker worker(String id, int dataPort, byte fingerprintByte) {
        byte[] fingerprint = new byte[32];
        Arrays.fill(fingerprint, fingerprintByte);
        RpcSecurityMetadata security = RpcSecurityMetadata.of(
                repeatHex(fingerprintByte), repeatHex((byte) (fingerprintByte + 7)));
        return new RegistryWorker(id, "127.0.0.1", dataPort - 100, 8,
                dataPort, "inc-" + id, security,
                DataPlaneRegistration.shuffleMtlsV1(fingerprint));
    }

    private static String repeatHex(byte value) {
        char digit = "0123456789abcdef".charAt(value & 0x0f);
        char[] chars = new char[64];
        Arrays.fill(chars, digit);
        return new String(chars);
    }
}
