package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.net.ShuffleInboundAuthorization;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** independently enumerated route/grant closure over 22 DAG topologies. */
class TwentyTwoShuffleTopologyClosureTest {
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-22", "attempt-22", 101L);
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void everyLogicalEdgeAndPhysicalInstancePairHasExactlyOneTransportDecision()
            throws Exception {
        List<Fixture> fixtures = fixtures();
        assertEquals(22, fixtures.size(), "the acceptance matrix must not silently shrink");

        for (Fixture fixture : fixtures) {
            ExecutionGraph graph = fixture.graph();
            PhysicalPlan physical = fixture.physical();
            List<ExpectedRoute> expected = independentlyEnumerate(fixture);
            ShuffleTransportPlan transport = ShuffleTransportPlan.create(
                    graph, physical, workers(), FENCE, fixture.name,
                    fixture.objectReuse, seededRandom(fixture.name));
            ShuffleGrantPlan grants = ShuffleGrantPlan.create(
                    transport, seededRandom(fixture.name + "-grants"));

            assertEquals(expected.size(), transport.routes().size(), fixture.name);
            long expectedRemote = expected.stream().filter(route -> !route.local).count();
            assertEquals(expectedRemote, transport.remoteRouteCount(), fixture.name);
            assertEquals(expectedRemote, grants.remoteGrantCount(), fixture.name);
            assertRoutesAndGrants(fixture, expected, transport, grants);
            assertFinalCommandClosure(fixture, transport, grants);
        }
    }

    private static void assertFinalCommandClosure(
            Fixture fixture, ShuffleTransportPlan transport,
            ShuffleGrantPlan grants) {
        StageCommandShuffleClosure closure = StageCommandShuffleClosure.create(
                fixture.graph(), fixture.physical(), transport, grants);
        for (InstSpec instance : fixture.instances) {
            StageCommand command = finalCommand(
                    fixture, instance, transport, grants)
                    .withRunAttemptFence(FENCE);
            closure.accept(instance.stage, instance.index, instance.worker,
                    wireRoundTrip(command));
        }
        closure.assertComplete();
    }

    private static StageCommand wireRoundTrip(StageCommand command) {
        ObjectTrustContext trust = ObjectTrustContext.localTrusted(
                TwentyTwoShuffleTopologyClosureTest.class.getClassLoader());
        byte[] envelope = StageCommandCodecV2.encode(
                command, trust, ignored -> { });
        return StageCommandCodecV2.preflight(envelope, ignored -> { })
                .materialize(trust, ignored -> { });
    }

    /** Independent fixture projection of the wire-visible shuffle fields. */
    private static StageCommand finalCommand(
            Fixture fixture, InstSpec instance,
            ShuffleTransportPlan transport, ShuffleGrantPlan grants) {
        StageSpec stage = fixture.stages[instance.stage];
        PlanGraph fragment = new PlanGraph(Collections.singletonList(
                PlanNode.placeholderSource(META)));
        List<int[]> consumers = consumerEdges(fixture, instance.stage);
        List<List<ShuffleTarget>> outputGroups = new ArrayList<>();
        for (int[] consumer : consumers) {
            outputGroups.add(targetsFor(fixture, instance, consumer,
                    transport, grants));
        }

        StageCommand command;
        if ("source".equals(stage.type)) {
            command = StageCommand.sourceStageFanout(
                    META, META, 1,
                    new CollectionSource(META,
                            fixture.name + "-" + instance.stage + "-" + instance.index,
                            Collections.emptyList()),
                    fragment, Collections.emptyList(), null, false);
        } else if ("sink".equals(stage.type)) {
            command = StageCommand.sink(META, 1,
                    producerCount(fixture, stage), fragment,
                    new DiscardSink(META));
        } else if ("join".equals(stage.type)) {
            command = StageCommand.joinConsumerFanout(
                    META, META, META, 1,
                    stageProducerCount(fixture, stage.inputs[0]),
                    stageProducerCount(fixture, stage.inputs[1]),
                    new String[]{"id"}, new String[]{"id"}, "INNER",
                    Collections.emptyList(), null, false);
        } else {
            command = StageCommand.keyedOpFanout(
                    META, META, 1, producerCount(fixture, stage),
                    fragment, Collections.emptyList(), null, false);
        }

        applyInbound(fixture, stage, instance, command, transport, grants);
        if (!outputGroups.isEmpty()) {
            command.withDownTargets(outputGroups.get(0))
                    .withObjectReuse(fixture.objectReuse);
        }
        if (outputGroups.size() > 1) {
            List<StageCommand.BroadcastGroup> groups = new ArrayList<>();
            for (List<ShuffleTarget> targets : outputGroups) {
                groups.add(new StageCommand.BroadcastGroup(
                        targets, null, false, false));
            }
            command.withBroadcastGroups(groups)
                    .withObjectReuse(fixture.objectReuse);
        }
        return command;
    }

    private static void applyInbound(
            Fixture fixture, StageSpec stage, InstSpec consumer,
            StageCommand command, ShuffleTransportPlan transport,
            ShuffleGrantPlan grants) {
        if (stage.inputs.length == 0) return;
        List<ShuffleInboundAuthorization> authorizations =
                grants.consumerAuthorizations(stage.index, consumer.index);
        if (!authorizations.isEmpty()) {
            command.withInboundAuthorizations(authorizations);
        }
        if ("union".equals(stage.type)) {
            List<ShuffleTransportPlan.Route> routes = new ArrayList<>();
            for (int edge = 0; edge < stage.inputs.length; edge++) {
                routes.addAll(inboundRoutes(
                        fixture, stage, edge, consumer, transport));
            }
            applyLeftLocal(command, routes);
            if (!authorizations.isEmpty()) {
                command.withInboundTargetId(authorizations.get(0).targetId());
            }
            return;
        }
        List<ShuffleTransportPlan.Route> left = inboundRoutes(
                fixture, stage, 0, consumer, transport);
        applyLeftLocal(command, left);
        java.util.UUID leftTarget = targetFor(grants, stage.index, 0, consumer.index);
        if (stage.inputs.length == 2) {
            List<ShuffleTransportPlan.Route> right = inboundRoutes(
                    fixture, stage, 1, consumer, transport);
            int rightLocal = localCount(right);
            if (rightLocal > 0) {
                command.withLocalInboundRight(rightLocal, localKey(right));
            }
            command.withInboundTargetIds(leftTarget,
                    targetFor(grants, stage.index, 1, consumer.index));
        } else if (leftTarget != null) {
            command.withInboundTargetId(leftTarget);
        }
    }

    private static void applyLeftLocal(
            StageCommand command, List<ShuffleTransportPlan.Route> routes) {
        int count = localCount(routes);
        if (count > 0) command.withLocalInbound(count, localKey(routes));
    }

    private static int localCount(List<ShuffleTransportPlan.Route> routes) {
        int count = 0;
        for (ShuffleTransportPlan.Route route : routes) {
            if (route.isLocal()) count++;
        }
        return count;
    }

    private static String localKey(List<ShuffleTransportPlan.Route> routes) {
        String result = null;
        for (ShuffleTransportPlan.Route route : routes) {
            if (!route.isLocal()) continue;
            if (result == null) result = route.localKey();
            else assertEquals(result, route.localKey(),
                    "fixture local target must be canonical");
        }
        return result;
    }

    private static java.util.UUID targetFor(
            ShuffleGrantPlan grants, int stage, int edge, int instance) {
        ShuffleInboundAuthorization authorization =
                grants.consumerAuthorization(stage, edge, instance);
        return authorization == null ? null : authorization.targetId();
    }

    private static List<ShuffleTransportPlan.Route> inboundRoutes(
            Fixture fixture, StageSpec stage, int edge,
            InstSpec consumer, ShuffleTransportPlan transport) {
        List<ShuffleTransportPlan.Route> result = new ArrayList<>();
        for (InstSpec producer : fixture.instances) {
            if (producer.stage != stage.inputs[edge]) continue;
            result.add(transport.route(stage.index, edge, consumer.index,
                    producer.stage, producer.index));
        }
        return result;
    }

    private static List<ShuffleTarget> targetsFor(
            Fixture fixture, InstSpec producer, int[] consumerEdge,
            ShuffleTransportPlan transport, ShuffleGrantPlan grants) {
        List<ShuffleTarget> result = new ArrayList<>();
        List<InstSpec> consumers = new ArrayList<>();
        for (InstSpec instance : fixture.instances) {
            if (instance.stage == consumerEdge[0]) consumers.add(instance);
        }
        consumers.sort(Comparator.comparingInt(value -> value.index));
        for (InstSpec consumer : consumers) {
            ShuffleTransportPlan.Route route = transport.route(
                    consumer.stage, consumerEdge[1], consumer.index,
                    producer.stage, producer.index);
            result.add(route.isLocal()
                    ? ShuffleTarget.local(route.localKey())
                    : ShuffleTarget.socket(grants.producerEndpoint(route.key())));
        }
        return result;
    }

    private static List<int[]> consumerEdges(Fixture fixture, int producerStage) {
        List<int[]> result = new ArrayList<>();
        for (StageSpec stage : fixture.stages) {
            for (int edge = 0; edge < stage.inputs.length; edge++) {
                if (stage.inputs[edge] == producerStage) {
                    result.add(new int[]{stage.index, edge});
                }
            }
        }
        return result;
    }

    private static int producerCount(Fixture fixture, StageSpec stage) {
        int count = 0;
        for (int input : stage.inputs) {
            count += stageProducerCount(fixture, input);
        }
        return count;
    }

    private static int stageProducerCount(Fixture fixture, int stage) {
        int count = 0;
        for (InstSpec instance : fixture.instances) {
            if (instance.stage == stage) count++;
        }
        return count;
    }

    private static void assertRoutesAndGrants(
            Fixture fixture, List<ExpectedRoute> expected,
            ShuffleTransportPlan transport, ShuffleGrantPlan grants) {
        Map<String, List<ExpectedRoute>> remoteTargets = new LinkedHashMap<>();
        Set<UUID> targetIds = new HashSet<>();
        for (ExpectedRoute expectedRoute : expected) {
            ShuffleTransportPlan.Route actual = transport.route(
                    expectedRoute.consumerStage, expectedRoute.edge,
                    expectedRoute.consumerInstance, expectedRoute.producerStage,
                    expectedRoute.producerInstance);
            assertNotNull(actual, fixture.name + " " + expectedRoute);
            assertEquals(expectedRoute.local, actual.isLocal(),
                    fixture.name + " " + expectedRoute);
            ShuffleEndpoint endpoint = grants.producerEndpoint(actual.key());
            if (expectedRoute.local) {
                assertNotNull(actual.localKey(), fixture.name);
                assertNull(actual.targetId(), fixture.name);
                assertNull(endpoint, fixture.name);
            } else {
                assertNull(actual.localKey(), fixture.name);
                assertNotNull(actual.targetId(), fixture.name);
                assertNotNull(endpoint, fixture.name);
                assertEquals(expectedRoute.consumerWorker,
                        actual.destinationIdentity().workerId(), fixture.name);
                assertEquals(expectedRoute.producerWorker,
                        actual.sourceIdentity().workerId(), fixture.name);
                assertEquals(actual.targetId(), endpoint.targetId(), fixture.name);
                remoteTargets.computeIfAbsent(expectedRoute.targetKey(fixture),
                        ignored -> new ArrayList<>()).add(expectedRoute.withActual(actual));
            }
        }

        for (Map.Entry<String, List<ExpectedRoute>> entry : remoteTargets.entrySet()) {
            List<ExpectedRoute> targetRoutes = entry.getValue();
            targetRoutes.sort(Comparator
                    .comparingInt((ExpectedRoute route) -> route.edge)
                    .thenComparingInt(route -> route.producerStage)
                    .thenComparingInt(route -> route.producerInstance));
            UUID targetId = targetRoutes.get(0).actual.targetId();
            assertTrue(targetIds.add(targetId),
                    fixture.name + " target ids must not alias across consumer targets");
            Set<String> sourcePools = new HashSet<>();
            Map<String, Integer> sourceCohortSizes = new HashMap<>();
            for (int i = 0; i < targetRoutes.size(); i++) {
                ExpectedRoute route = targetRoutes.get(i);
                assertEquals(targetId, route.actual.targetId(), fixture.name);
                assertEquals(i + 1, route.actual.producerOrdinal(), fixture.name);
                sourcePools.add(route.producerWorker);
                sourceCohortSizes.merge(route.producerWorker, 1, Math::addExact);
            }
            for (ExpectedRoute route : targetRoutes) {
                ShuffleEndpoint endpoint = grants.producerEndpoint(route.actual.key());
                assertEquals(sourceCohortSizes.get(route.producerWorker).intValue(),
                        endpoint.expectedOrdinalCount(), fixture.name);
                ShuffleInboundAuthorization authorization = grants.consumerAuthorization(
                        route.consumerStage, route.edge, route.consumerInstance);
                assertNotNull(authorization, fixture.name);
                assertEquals(targetId, authorization.targetId(), fixture.name);
                assertEquals(targetRoutes.size(), authorization.records().size(), fixture.name);
                assertEquals(sourcePools.size(), authorization.expectedPoolCount(), fixture.name);
            }
        }
    }

    /** This deliberately does not call any planner helper or inspect planner routes. */
    private static List<ExpectedRoute> independentlyEnumerate(Fixture fixture) {
        Map<Integer, List<InstSpec>> byStage = new HashMap<>();
        for (InstSpec instance : fixture.instances) {
            byStage.computeIfAbsent(instance.stage, ignored -> new ArrayList<>()).add(instance);
        }
        List<ExpectedRoute> result = new ArrayList<>();
        for (StageSpec consumer : fixture.stages) {
            for (int edge = 0; edge < consumer.inputs.length; edge++) {
                int producerStage = consumer.inputs[edge];
                for (InstSpec consumerInstance : byStage.get(consumer.index)) {
                    for (InstSpec producerInstance : byStage.get(producerStage)) {
                        result.add(new ExpectedRoute(
                                consumer.index, edge, consumerInstance.index,
                                producerStage, producerInstance.index,
                                producerInstance.worker, consumerInstance.worker,
                                fixture.objectReuse
                                        && producerInstance.worker.equals(consumerInstance.worker)));
                    }
                }
            }
        }
        return result;
    }

    private static List<Fixture> fixtures() {
        List<Fixture> fixtures = new ArrayList<>();
        fixtures.add(f("chain-local", true, ss(s(0, "source"), s(1, "group", 0)), is(i(0,0,"w0"),i(1,0,"w0"))));
        fixtures.add(f("chain-remote", true, ss(s(0, "source"), s(1, "group", 0)), is(i(0,0,"w0"),i(1,0,"w1"))));
        fixtures.add(f("chain-copy-remote", false, ss(s(0, "source"), s(1, "group", 0)), is(i(0,0,"w0"),i(1,0,"w0"))));
        fixtures.add(f("two-to-one-mixed", true, ss(s(0,"source"),s(1,"group",0)), is(i(0,0,"w0"),i(0,1,"w1"),i(1,0,"w0"))));
        fixtures.add(f("one-to-two-mixed", true, ss(s(0,"source"),s(1,"group",0)), is(i(0,0,"w0"),i(1,0,"w0"),i(1,1,"w1"))));
        fixtures.add(f("two-by-two-mixed", true, ss(s(0,"source"),s(1,"group",0)), is(i(0,0,"w0"),i(0,1,"w1"),i(1,0,"w0"),i(1,1,"w1"))));
        fixtures.add(f("two-by-two-remote", true, ss(s(0,"source"),s(1,"group",0)), is(i(0,0,"w0"),i(0,1,"w1"),i(1,0,"w2"),i(1,1,"w2"))));
        fixtures.add(f("three-by-two", true, ss(s(0,"source"),s(1,"group",0)), is(i(0,0,"w0"),i(0,1,"w1"),i(0,2,"w2"),i(1,0,"w0"),i(1,1,"w1"))));
        fixtures.add(f("union-remote", true, ss(s(0,"source"),s(1,"source"),s(2,"union",0,1)), is(i(0,0,"w0"),i(1,0,"w1"),i(2,0,"w2"))));
        fixtures.add(f("union-local-remote", true, ss(s(0,"source"),s(1,"source"),s(2,"union",0,1)), is(i(0,0,"w0"),i(1,0,"w1"),i(2,0,"w0"))));
        fixtures.add(f("union-parallel-inputs", true, ss(s(0,"source"),s(1,"source"),s(2,"union",0,1)), is(i(0,0,"w0"),i(0,1,"w1"),i(1,0,"w1"),i(1,1,"w2"),i(2,0,"w0"))));
        fixtures.add(f("join-remote", true, ss(s(0,"source"),s(1,"source"),s(2,"join",0,1)), is(i(0,0,"w0"),i(1,0,"w1"),i(2,0,"w2"))));
        fixtures.add(f("join-mixed", true, ss(s(0,"source"),s(1,"source"),s(2,"join",0,1)), is(i(0,0,"w0"),i(1,0,"w1"),i(2,0,"w0"))));
        fixtures.add(f("self-join-remote", true, ss(s(0,"source"),s(1,"join",0,0)), is(i(0,0,"w0"),i(1,0,"w1"))));
        fixtures.add(f("self-join-local", true, ss(s(0,"source"),s(1,"join",0,0)), is(i(0,0,"w0"),i(1,0,"w0"))));
        fixtures.add(f("fanout-remote", true, ss(s(0,"source"),s(1,"group",0),s(2,"group",0)), is(i(0,0,"w0"),i(1,0,"w1"),i(2,0,"w2"))));
        fixtures.add(f("fanout-mixed", true, ss(s(0,"source"),s(1,"group",0),s(2,"group",0)), is(i(0,0,"w0"),i(1,0,"w0"),i(2,0,"w1"))));
        fixtures.add(f("diamond-rejoin", true, ss(s(0,"source"),s(1,"group",0),s(2,"group",0),s(3,"join",1,2)), is(i(0,0,"w0"),i(1,0,"w1"),i(2,0,"w2"),i(3,0,"w0"))));
        fixtures.add(f("two-outputs", true, ss(s(0,"source"),s(1,"sink",0),s(2,"sink",0)), is(i(0,0,"w0"),i(1,0,"w1"),i(2,0,"w2"))));
        fixtures.add(f("three-outputs", true, ss(s(0,"source"),s(1,"sink",0),s(2,"sink",0),s(3,"sink",0)), is(i(0,0,"w0"),i(1,0,"w0"),i(2,0,"w1"),i(3,0,"w2"))));
        fixtures.add(f("three-stage-chain", true, ss(s(0,"source"),s(1,"group",0),s(2,"sink",1)), is(i(0,0,"w0"),i(1,0,"w1"),i(2,0,"w2"))));
        fixtures.add(f("join-fanout-outputs", true, ss(s(0,"source"),s(1,"source"),s(2,"join",0,1),s(3,"sink",2),s(4,"sink",2)), is(i(0,0,"w0"),i(1,0,"w1"),i(2,0,"w2"),i(3,0,"w0"),i(4,0,"w1"))));
        return fixtures;
    }

    private static Fixture f(String name, boolean reuse, StageSpec[] stages, InstSpec[] instances) {
        return new Fixture(name, reuse, stages, instances);
    }
    private static StageSpec s(int index, String type, int... inputs) { return new StageSpec(index, type, inputs); }
    private static StageSpec[] ss(StageSpec... values) { return values; }
    private static InstSpec i(int stage, int index, String worker) { return new InstSpec(stage, index, worker); }
    private static InstSpec[] is(InstSpec... values) { return values; }

    private static SecureRandom seededRandom(String seed) throws Exception {
        SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
        random.setSeed(seed.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return random;
    }

    private static Map<String, RegistryWorker> workers() {
        Map<String, RegistryWorker> workers = new LinkedHashMap<>();
        workers.put("w0", worker("w0", 7600, (byte) 1));
        workers.put("w1", worker("w1", 7601, (byte) 2));
        workers.put("w2", worker("w2", 7602, (byte) 3));
        return workers;
    }

    private static RegistryWorker worker(String id, int dataPort, byte fill) {
        byte[] fingerprint = new byte[32];
        Arrays.fill(fingerprint, fill);
        return new RegistryWorker(id, "127.0.0.1", dataPort - 100, 64,
                dataPort, "inc-" + id,
                RpcSecurityMetadata.of(hex(fill), hex((byte) (fill + 8))),
                DataPlaneRegistration.shuffleMtlsV1(fingerprint));
    }

    private static String hex(byte fill) {
        char digit = "0123456789abcdef".charAt(fill & 0x0f);
        char[] chars = new char[64];
        Arrays.fill(chars, digit);
        return new String(chars);
    }

    private static final class Fixture {
        final String name;
        final boolean objectReuse;
        final StageSpec[] stages;
        final InstSpec[] instances;
        Fixture(String name, boolean objectReuse, StageSpec[] stages, InstSpec[] instances) {
            this.name = name; this.objectReuse = objectReuse;
            this.stages = stages; this.instances = instances;
        }
        ExecutionGraph graph() {
            List<Stage> values = new ArrayList<>();
            for (StageSpec stage : stages) {
                List<Integer> inputs = new ArrayList<>();
                for (int input : stage.inputs) inputs.add(input);
                values.add(new Stage(stage.index, Collections.singletonList(stage.index),
                        inputs, "sink".equals(stage.type), stage.type));
            }
            return new ExecutionGraph(values);
        }
        PhysicalPlan physical() {
            List<PhysicalPlan.Instance> values = new ArrayList<>();
            int parallelism = 1;
            for (InstSpec instance : instances) {
                values.add(new PhysicalPlan.Instance(instance.stage, instance.index,
                        instance.worker, stages[instance.stage].inputs.length > 0));
                parallelism = Math.max(parallelism, instance.index + 1);
            }
            return new PhysicalPlan(parallelism, values, Collections.emptyList(), false);
        }
        boolean union(int stage) { return "union".equals(stages[stage].type); }
    }

    private static final class StageSpec {
        final int index; final String type; final int[] inputs;
        StageSpec(int index, String type, int[] inputs) {
            this.index = index; this.type = type; this.inputs = inputs;
        }
    }

    private static final class InstSpec {
        final int stage; final int index; final String worker;
        InstSpec(int stage, int index, String worker) {
            this.stage = stage; this.index = index; this.worker = worker;
        }
    }

    private static final class ExpectedRoute {
        final int consumerStage, edge, consumerInstance, producerStage, producerInstance;
        final String producerWorker, consumerWorker;
        final boolean local;
        ShuffleTransportPlan.Route actual;
        ExpectedRoute(int consumerStage, int edge, int consumerInstance,
                      int producerStage, int producerInstance,
                      String producerWorker, String consumerWorker, boolean local) {
            this.consumerStage = consumerStage; this.edge = edge;
            this.consumerInstance = consumerInstance; this.producerStage = producerStage;
            this.producerInstance = producerInstance; this.producerWorker = producerWorker;
            this.consumerWorker = consumerWorker; this.local = local;
        }
        ExpectedRoute withActual(ShuffleTransportPlan.Route route) { this.actual = route; return this; }
        String targetKey(Fixture fixture) {
            return consumerStage + ":" + (fixture.union(consumerStage) ? 0 : edge)
                    + ":" + consumerInstance;
        }
        @Override public String toString() {
            return producerStage + ":" + producerInstance + "->"
                    + consumerStage + ":" + edge + ":" + consumerInstance;
        }
    }
}
