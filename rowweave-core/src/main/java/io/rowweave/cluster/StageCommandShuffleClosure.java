package io.rowweave.cluster;

import io.rowweave.sched.net.ShuffleAuthorizationRecord;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.net.ShuffleInboundAuthorization;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * one-shot proof that final StageCommands consume an exact shuffle plan.
 * The validator is attempt-local and never renders target ids, bearer bytes or digests.
 */
final class StageCommandShuffleClosure {

    private static final class InstanceKey {
        final int stage;
        final int instance;

        InstanceKey(int stage, int instance) {
            this.stage = stage;
            this.instance = instance;
        }

        @Override public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof InstanceKey)) return false;
            InstanceKey that = (InstanceKey) other;
            return stage == that.stage && instance == that.instance;
        }

        @Override public int hashCode() {
            return Objects.hash(stage, instance);
        }
    }

    private static final class ConsumerEdge {
        final int stage;
        final int edge;

        ConsumerEdge(int stage, int edge) {
            this.stage = stage;
            this.edge = edge;
        }
    }

    private final List<Stage> stages;
    private final Map<Integer, Stage> stagesByIndex;
    private final Map<InstanceKey, PhysicalPlan.Instance> instances;
    private final Map<Integer, List<PhysicalPlan.Instance>> instancesByStage;
    private final ShuffleTransportPlan transport;
    private final ShuffleGrantPlan grants;
    private final Set<InstanceKey> accepted = new HashSet<>();
    private final Set<ShuffleTransportPlan.RouteKey> producerConsumed = new HashSet<>();
    private final Set<ShuffleTransportPlan.RouteKey> consumerConsumed = new HashSet<>();
    private boolean completed;

    private StageCommandShuffleClosure(
            ExecutionGraph graph, PhysicalPlan physical,
            ShuffleTransportPlan transport, ShuffleGrantPlan grants) {
        this.stages = graph.stages();
        this.transport = transport;
        this.grants = grants;
        this.stagesByIndex = new HashMap<>();
        for (Stage stage : stages) {
            if (stagesByIndex.put(stage.index(), stage) != null) {
                throw invalid("duplicate logical stage");
            }
        }
        this.instances = new LinkedHashMap<>();
        this.instancesByStage = new HashMap<>();
        for (PhysicalPlan.Instance instance : physical.instances()) {
            InstanceKey key = new InstanceKey(
                    instance.logicalStage(), instance.instanceIndex());
            if (!stagesByIndex.containsKey(instance.logicalStage())
                    || instances.put(key, instance) != null) {
                throw invalid("invalid physical instance closure");
            }
            instancesByStage.computeIfAbsent(instance.logicalStage(),
                    ignored -> new ArrayList<>()).add(instance);
        }
        for (Stage stage : stages) {
            if (!instancesByStage.containsKey(stage.index())) {
                throw invalid("logical stage has no physical instance");
            }
        }
        if (transport == null || grants == null) {
            throw invalid("shuffle plans are required");
        }
    }

    static StageCommandShuffleClosure create(
            ExecutionGraph graph, PhysicalPlan physical,
            ShuffleTransportPlan transport, ShuffleGrantPlan grants) {
        if (graph == null || physical == null) {
            throw new IllegalArgumentException(
                    "execution graph and physical plan are required");
        }
        return new StageCommandShuffleClosure(graph, physical, transport, grants);
    }

    synchronized void accept(int logicalStage, int instanceIndex,
                             String workerId, StageCommand command) {
        if (completed) throw invalid("closure is already complete");
        if (workerId == null || command == null) {
            throw invalid("final command identity is incomplete");
        }
        InstanceKey instanceKey = new InstanceKey(logicalStage, instanceIndex);
        PhysicalPlan.Instance physical = instances.get(instanceKey);
        if (physical == null || !workerId.equals(physical.workerId())) {
            throw invalid("final command does not match its physical instance");
        }
        if (!accepted.add(instanceKey)) {
            throw invalid("duplicate final command for physical instance");
        }
        Stage stage = stagesByIndex.get(logicalStage);
        requireRole(stage, command);
        validateInbound(stage, instanceIndex, command);
        validateOutbound(stage, instanceIndex, command);
    }

    synchronized void assertComplete() {
        if (completed) return;
        if (accepted.size() != instances.size()
                || producerConsumed.size() != transport.routes().size()
                || consumerConsumed.size() != transport.routes().size()
                || grants.remoteGrantCount() != transport.remoteRouteCount()) {
            throw invalid("unconsumed final command, route or grant closure");
        }
        completed = true;
    }

    private void requireRole(Stage stage, StageCommand command) {
        StageCommand.Role expected;
        if (stage.isSink()) {
            expected = StageCommand.Role.SINK;
        } else if (stage.inputStages().isEmpty()) {
            expected = StageCommand.Role.SOURCE_STAGE;
        } else if ("join".equals(stage.headTypeId())
                || "coGroup".equals(stage.headTypeId())) {
            expected = StageCommand.Role.JOIN_CONSUMER;
        } else {
            expected = StageCommand.Role.KEYED_OP;
        }
        if (command.role() != expected) {
            throw invalid("final command role does not match logical stage");
        }
    }

    private void validateInbound(Stage stage, int consumerInstance,
                                 StageCommand command) {
        List<Integer> inputs = stage.inputStages();
        if (inputs.isEmpty()) {
            if (command.inboundConnections() != 0
                    || command.rightInboundConnections() != 0
                    || command.localInbound() != 0
                    || command.localInboundRight() != 0
                    || command.inboundTargetId() != null
                    || command.inboundTargetIdRight() != null
                    || !safeAuthorizations(command).isEmpty()) {
                throw invalid("source command unexpectedly owns inbound shuffle state");
            }
            return;
        }

        boolean union = "union".equals(stage.headTypeId());
        if (union) {
            List<ShuffleTransportPlan.Route> combined = new ArrayList<>();
            for (int edge = 0; edge < inputs.size(); edge++) {
                combined.addAll(routesForConsumer(stage.index(), edge,
                        consumerInstance, inputs.get(edge)));
            }
            requireCount(command.inboundConnections(), combined.size(),
                    "union inbound count");
            requireCount(command.rightInboundConnections(), 0,
                    "union right inbound count");
            requireLocal(command.localInbound(), command.localInboundKey(), combined,
                    "union local inbound");
            requireCount(command.localInboundRight(), 0,
                    "union right local inbound");
            requireNull(command.localInboundKeyRight(), "union right local key");
            List<ShuffleInboundAuthorization> expected =
                    grants.consumerAuthorizations(stage.index(), consumerInstance);
            requireAuthorizations(expected, safeAuthorizations(command));
            requireUuid(command.inboundTargetId(), firstTarget(expected),
                    "union inbound target");
            requireNull(command.inboundTargetIdRight(), "union right target");
            markConsumer(combined);
            return;
        }

        List<ShuffleTransportPlan.Route> left = routesForConsumer(
                stage.index(), 0, consumerInstance, inputs.get(0));
        requireCount(command.inboundConnections(), left.size(), "left inbound count");
        requireLocal(command.localInbound(), command.localInboundKey(), left,
                "left local inbound");
        List<ShuffleTransportPlan.Route> right = Collections.emptyList();
        if (inputs.size() == 2) {
            right = routesForConsumer(stage.index(), 1, consumerInstance, inputs.get(1));
            requireCount(command.rightInboundConnections(), right.size(),
                    "right inbound count");
            requireLocal(command.localInboundRight(), command.localInboundKeyRight(), right,
                    "right local inbound");
        } else {
            requireCount(command.rightInboundConnections(), 0,
                    "unexpected right inbound count");
            requireCount(command.localInboundRight(), 0,
                    "unexpected right local count");
            requireNull(command.localInboundKeyRight(), "unexpected right local key");
        }

        List<ShuffleInboundAuthorization> expected =
                grants.consumerAuthorizations(stage.index(), consumerInstance);
        requireAuthorizations(expected, safeAuthorizations(command));
        requireUuid(command.inboundTargetId(), targetForEdge(
                stage.index(), 0, consumerInstance), "left inbound target");
        UUID expectedRight = inputs.size() == 2
                ? targetForEdge(stage.index(), 1, consumerInstance) : null;
        requireUuid(command.inboundTargetIdRight(), expectedRight,
                "right inbound target");
        markConsumer(left);
        markConsumer(right);
    }

    private void validateOutbound(Stage producer, int producerInstance,
                                  StageCommand command) {
        List<ConsumerEdge> edges = consumerEdges(producer.index());
        if (edges.isEmpty()) {
            if (command.downTargets() != null && !command.downTargets().isEmpty()) {
                throw invalid("terminal command unexpectedly owns producer targets");
            }
            if (command.broadcastGroups() != null
                    && !command.broadcastGroups().isEmpty()) {
                throw invalid("terminal command unexpectedly owns broadcast targets");
            }
            return;
        }

        List<List<ShuffleTarget>> expectedGroups = new ArrayList<>(edges.size());
        for (ConsumerEdge edge : edges) {
            List<ShuffleTarget> expected = expectedTargets(
                    producer.index(), producerInstance, edge);
            expectedGroups.add(expected);
            for (PhysicalPlan.Instance consumer : instancesByStage.get(edge.stage)) {
                ShuffleTransportPlan.Route route = transport.route(
                        edge.stage, edge.edge, consumer.instanceIndex(),
                        producer.index(), producerInstance);
                if (route == null || !producerConsumed.add(route.key())) {
                    throw invalid("producer route is missing or consumed twice");
                }
            }
        }

        requireTargets(expectedGroups.get(0), command.downTargets(),
                "producer targets");
        if (edges.size() == 1) {
            if (command.broadcastGroups() != null
                    && !command.broadcastGroups().isEmpty()) {
                throw invalid("single consumer command has broadcast groups");
            }
            return;
        }
        List<StageCommand.BroadcastGroup> actual = command.broadcastGroups();
        if (actual == null || actual.size() != expectedGroups.size()) {
            throw invalid("producer broadcast group closure mismatch");
        }
        for (int i = 0; i < actual.size(); i++) {
            requireTargets(expectedGroups.get(i), actual.get(i).targets,
                    "producer broadcast targets");
        }
    }

    private List<ShuffleTarget> expectedTargets(
            int producerStage, int producerInstance, ConsumerEdge edge) {
        List<PhysicalPlan.Instance> consumers = instancesByStage.get(edge.stage);
        List<ShuffleTarget> result = new ArrayList<>(consumers.size());
        for (PhysicalPlan.Instance consumer : consumers) {
            ShuffleTransportPlan.Route route = transport.route(
                    edge.stage, edge.edge, consumer.instanceIndex(),
                    producerStage, producerInstance);
            if (route == null) throw invalid("planned producer route is missing");
            if (route.isLocal()) {
                result.add(ShuffleTarget.local(route.localKey()));
            } else {
                ShuffleEndpoint endpoint = grants.producerEndpoint(route.key());
                if (endpoint == null) throw invalid("planned producer grant is missing");
                result.add(ShuffleTarget.socket(endpoint));
            }
        }
        return result;
    }

    private List<ConsumerEdge> consumerEdges(int producerStage) {
        List<ConsumerEdge> result = new ArrayList<>();
        for (Stage consumer : stages) {
            for (int edge = 0; edge < consumer.inputStages().size(); edge++) {
                if (consumer.inputStages().get(edge) == producerStage) {
                    result.add(new ConsumerEdge(consumer.index(), edge));
                }
            }
        }
        return result;
    }

    private List<ShuffleTransportPlan.Route> routesForConsumer(
            int consumerStage, int edge, int consumerInstance, int producerStage) {
        List<ShuffleTransportPlan.Route> result = new ArrayList<>();
        for (PhysicalPlan.Instance producer : instancesByStage.get(producerStage)) {
            ShuffleTransportPlan.Route route = transport.route(
                    consumerStage, edge, consumerInstance,
                    producerStage, producer.instanceIndex());
            if (route == null) throw invalid("planned consumer route is missing");
            result.add(route);
        }
        return result;
    }

    private void markConsumer(List<ShuffleTransportPlan.Route> routes) {
        for (ShuffleTransportPlan.Route route : routes) {
            if (!consumerConsumed.add(route.key())) {
                throw invalid("consumer route is consumed twice");
            }
        }
    }

    private UUID targetForEdge(int stage, int edge, int instance) {
        ShuffleInboundAuthorization authorization =
                grants.consumerAuthorization(stage, edge, instance);
        return authorization == null ? null : authorization.targetId();
    }

    private static UUID firstTarget(List<ShuffleInboundAuthorization> values) {
        return values.isEmpty() ? null : values.get(0).targetId();
    }

    private static List<ShuffleInboundAuthorization> safeAuthorizations(
            StageCommand command) {
        List<ShuffleInboundAuthorization> values = command.inboundAuthorizations();
        return values == null ? Collections.emptyList() : values;
    }

    private static void requireLocal(
            int actualCount, String actualKey,
            List<ShuffleTransportPlan.Route> routes, String label) {
        int expectedCount = 0;
        String expectedKey = null;
        for (ShuffleTransportPlan.Route route : routes) {
            if (!route.isLocal()) continue;
            expectedCount++;
            if (expectedKey == null) expectedKey = route.localKey();
            else if (!expectedKey.equals(route.localKey())) {
                throw invalid("planned " + label + " keys are not canonical");
            }
        }
        if (actualCount != expectedCount || !Objects.equals(actualKey, expectedKey)) {
            throw invalid(label + " mismatch");
        }
    }

    private static void requireTargets(
            List<ShuffleTarget> expected, List<ShuffleTarget> actual, String label) {
        if (actual == null || actual.size() != expected.size()) {
            throw invalid(label + " size mismatch");
        }
        for (int i = 0; i < expected.size(); i++) {
            ShuffleTarget left = expected.get(i);
            ShuffleTarget right = actual.get(i);
            boolean equal = right != null && left.isLocal() == right.isLocal()
                    && (left.isLocal()
                    ? Objects.equals(left.localKey(), right.localKey())
                    : Objects.equals(left.endpoint(), right.endpoint()));
            if (!equal) throw invalid(label + " mismatch");
        }
    }

    private static void requireAuthorizations(
            List<ShuffleInboundAuthorization> expected,
            List<ShuffleInboundAuthorization> actual) {
        if (expected.size() != actual.size()) {
            throw invalid("consumer authorization target count mismatch");
        }
        for (int i = 0; i < expected.size(); i++) {
            ShuffleInboundAuthorization left = expected.get(i);
            ShuffleInboundAuthorization right = actual.get(i);
            if (right == null || !left.targetId().equals(right.targetId())
                    || left.expectedPoolCount() != right.expectedPoolCount()
                    || left.records().size() != right.records().size()) {
                throw invalid("consumer authorization mismatch");
            }
            for (int record = 0; record < left.records().size(); record++) {
                ShuffleAuthorizationRecord a = left.records().get(record);
                ShuffleAuthorizationRecord b = right.records().get(record);
                if (b == null || a.producerOrdinal() != b.producerOrdinal()
                        || !a.targetId().equals(b.targetId())
                        || !a.expectedSourceIdentity().equals(b.expectedSourceIdentity())
                        || !MessageDigest.isEqual(a.authorizationDigest(),
                        b.authorizationDigest())) {
                    throw invalid("consumer authorization record mismatch");
                }
            }
        }
    }

    private static void requireCount(int actual, int expected, String label) {
        if (actual != expected) throw invalid(label + " mismatch");
    }

    private static void requireUuid(UUID actual, UUID expected, String label) {
        if (!Objects.equals(actual, expected)) throw invalid(label + " mismatch");
    }

    private static void requireNull(Object value, String label) {
        if (value != null) throw invalid(label + " must be absent");
    }

    private static IllegalStateException invalid(String reason) {
        return new IllegalStateException("secure shuffle command closure invalid: " + reason);
    }
}
