package io.rowweave.cluster;

import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.net.Frame;
import io.rowweave.sched.net.ShuffleProducerGrant;
import io.rowweave.sched.net.WorkerDataIdentity;

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
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * immutable expansion of every logical edge into producer-to-consumer routes.
 * This is the single owner of LOCAL/REMOTE decisions, target ids, ordinals and grants.
 */
public final class ShuffleTransportPlan {

    public static final class RouteKey implements Comparable<RouteKey> {
        private final RunAttemptFence attemptFence;
        private final int consumerStage;
        private final int consumerEdge;
        private final int consumerInstance;
        private final int producerStage;
        private final int producerInstance;

        private RouteKey(RunAttemptFence attemptFence,
                         int consumerStage, int consumerEdge, int consumerInstance,
                         int producerStage, int producerInstance) {
            this.attemptFence = attemptFence;
            this.consumerStage = consumerStage;
            this.consumerEdge = consumerEdge;
            this.consumerInstance = consumerInstance;
            this.producerStage = producerStage;
            this.producerInstance = producerInstance;
        }

        public RunAttemptFence attemptFence() { return attemptFence; }
        public int consumerStage() { return consumerStage; }
        public int consumerEdge() { return consumerEdge; }
        public int consumerInstance() { return consumerInstance; }
        public int producerStage() { return producerStage; }
        public int producerInstance() { return producerInstance; }

        @Override
        public int compareTo(RouteKey other) {
            int value = Integer.compare(consumerStage, other.consumerStage);
            if (value == 0) value = Integer.compare(consumerEdge, other.consumerEdge);
            if (value == 0) value = Integer.compare(consumerInstance, other.consumerInstance);
            if (value == 0) value = Integer.compare(producerStage, other.producerStage);
            if (value == 0) value = Integer.compare(producerInstance, other.producerInstance);
            return value;
        }

        @Override public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof RouteKey)) return false;
            RouteKey that = (RouteKey) other;
            return consumerStage == that.consumerStage
                    && consumerEdge == that.consumerEdge
                    && consumerInstance == that.consumerInstance
                    && producerStage == that.producerStage
                    && producerInstance == that.producerInstance
                    && attemptFence.equals(that.attemptFence);
        }

        @Override public int hashCode() {
            return Objects.hash(attemptFence, consumerStage, consumerEdge,
                    consumerInstance, producerStage, producerInstance);
        }

        @Override public String toString() {
            return "ShuffleRoute{" + producerStage + ':' + producerInstance + "->"
                    + consumerStage + ':' + consumerEdge + ':' + consumerInstance + '}';
        }
    }

    public static final class Route {
        private final RouteKey key;
        private final boolean local;
        private final String localKey;
        private final UUID targetId;
        private final int producerOrdinal;
        private final WorkerDataIdentity sourceIdentity;
        private final WorkerDataIdentity destinationIdentity;
        private final String destinationHost;
        private final int destinationDataPort;

        private Route(RouteKey key, boolean local, String localKey, UUID targetId,
                      int producerOrdinal, WorkerDataIdentity sourceIdentity,
                      WorkerDataIdentity destinationIdentity,
                      String destinationHost, int destinationDataPort) {
            this.key = key;
            this.local = local;
            this.localKey = localKey;
            this.targetId = targetId;
            this.producerOrdinal = producerOrdinal;
            this.sourceIdentity = sourceIdentity;
            this.destinationIdentity = destinationIdentity;
            this.destinationHost = destinationHost;
            this.destinationDataPort = destinationDataPort;
        }

        public RouteKey key() { return key; }
        public boolean isLocal() { return local; }
        public String localKey() { return localKey; }
        public UUID targetId() { return targetId; }
        public int producerOrdinal() { return producerOrdinal; }
        public WorkerDataIdentity sourceIdentity() { return sourceIdentity; }
        public WorkerDataIdentity destinationIdentity() { return destinationIdentity; }
        public String destinationHost() { return destinationHost; }
        public int destinationDataPort() { return destinationDataPort; }
    }

    private static final class Draft {
        final int position;
        final RouteKey key;
        final RegistryWorker producer;
        final RegistryWorker consumer;
        final boolean local;
        final TargetKey targetKey;

        Draft(int position, RouteKey key, RegistryWorker producer,
              RegistryWorker consumer, boolean local, TargetKey targetKey) {
            this.position = position;
            this.key = key;
            this.producer = producer;
            this.consumer = consumer;
            this.local = local;
            this.targetKey = targetKey;
        }
    }

    private static final class TargetKey {
        final int consumerStage;
        final int canonicalEdge;
        final int consumerInstance;

        TargetKey(int consumerStage, int canonicalEdge, int consumerInstance) {
            this.consumerStage = consumerStage;
            this.canonicalEdge = canonicalEdge;
            this.consumerInstance = consumerInstance;
        }

        @Override public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof TargetKey)) return false;
            TargetKey that = (TargetKey) other;
            return consumerStage == that.consumerStage
                    && canonicalEdge == that.canonicalEdge
                    && consumerInstance == that.consumerInstance;
        }

        @Override public int hashCode() {
            return Objects.hash(consumerStage, canonicalEdge, consumerInstance);
        }
    }

    private final List<Route> routes;
    private final Map<RouteKey, Route> routesByKey;
    private final int remoteRouteCount;
    private final RunAttemptFence attemptFence;

    private ShuffleTransportPlan(List<Route> routes, int remoteRouteCount,
                                 RunAttemptFence attemptFence) {
        this.routes = Collections.unmodifiableList(new ArrayList<>(routes));
        Map<RouteKey, Route> byKey = new LinkedHashMap<>();
        for (Route route : routes) {
            if (byKey.put(route.key(), route) != null) {
                throw new IllegalArgumentException("duplicate shuffle route key");
            }
        }
        this.routesByKey = Collections.unmodifiableMap(byKey);
        this.remoteRouteCount = remoteRouteCount;
        this.attemptFence = attemptFence;
    }

    public static ShuffleTransportPlan create(
            ExecutionGraph graph, PhysicalPlan physicalPlan,
            Map<String, RegistryWorker> workers, RunAttemptFence attemptFence,
            String jobIdentity, boolean objectReuse, SecureRandom secureRandom) {
        if (graph == null || physicalPlan == null || workers == null
                || attemptFence == null || jobIdentity == null || jobIdentity.isEmpty()
                || secureRandom == null) {
            throw new IllegalArgumentException("complete shuffle planning context is required");
        }
        Map<Integer, List<PhysicalPlan.Instance>> instances = indexInstances(
                graph, physicalPlan, workers);
        List<Draft> drafts = new ArrayList<>();
        Map<TargetKey, List<Draft>> remoteByTarget = new LinkedHashMap<>();
        for (Stage consumerStage : graph.stages()) {
            List<PhysicalPlan.Instance> consumers = requireInstances(
                    instances, consumerStage.index());
            boolean sharedUnionTarget = "union".equals(consumerStage.headTypeId());
            for (int edge = 0; edge < consumerStage.inputStages().size(); edge++) {
                int producerStage = consumerStage.inputStages().get(edge);
                List<PhysicalPlan.Instance> producers = requireInstances(instances, producerStage);
                for (PhysicalPlan.Instance consumer : consumers) {
                    TargetKey targetKey = new TargetKey(consumerStage.index(),
                            sharedUnionTarget ? 0 : edge, consumer.instanceIndex());
                    for (PhysicalPlan.Instance producer : producers) {
                        RouteKey key = new RouteKey(attemptFence,
                                consumerStage.index(), edge, consumer.instanceIndex(),
                                producerStage, producer.instanceIndex());
                        RegistryWorker producerWorker = requireWorker(workers, producer.workerId());
                        RegistryWorker consumerWorker = requireWorker(workers, consumer.workerId());
                        boolean local = objectReuse
                                && producer.workerId().equals(consumer.workerId());
                        Draft draft = new Draft(drafts.size(), key, producerWorker,
                                consumerWorker, local, targetKey);
                        drafts.add(draft);
                        if (!local) {
                            List<Draft> targetRoutes = remoteByTarget.computeIfAbsent(
                                    targetKey, ignored -> new ArrayList<>());
                            if (targetRoutes.size()
                                    >= ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET) {
                                throw new IllegalArgumentException(
                                        "remote producers per target exceed 4096");
                            }
                            targetRoutes.add(draft);
                        }
                    }
                }
            }
        }

        Route[] result = new Route[drafts.size()];
        Set<UUID> allocatedTargets = new HashSet<>();
        for (Map.Entry<TargetKey, List<Draft>> entry : remoteByTarget.entrySet()) {
            UUID targetId = freshTarget(secureRandom, allocatedTargets);
            List<Draft> targetDrafts = entry.getValue();
            targetDrafts.sort(Comparator.comparing(draft -> draft.key));

            int ordinal = 1;
            for (Draft draft : targetDrafts) {
                WorkerDataIdentity sourceIdentity = exactIdentity(draft.producer);
                WorkerDataIdentity destinationIdentity = exactIdentity(draft.consumer);
                result[draft.position] = new Route(draft.key, false, null,
                        targetId, ordinal, sourceIdentity, destinationIdentity,
                        draft.consumer.host(), requireDataPort(draft.consumer));
                ordinal++;
            }
        }
        for (Draft draft : drafts) {
            if (draft.local) {
                String localKey = localKey(jobIdentity, draft.targetKey);
                result[draft.position] = new Route(draft.key, true, localKey,
                        null, 0, null, null, null, -1);
            }
        }
        return new ShuffleTransportPlan(Arrays.asList(result),
                remoteByTarget.values().stream()
                        .mapToInt(List::size).sum(), attemptFence);
    }

    public List<Route> routes() { return routes; }
    public int remoteRouteCount() { return remoteRouteCount; }

    public Route route(RouteKey key) { return routesByKey.get(key); }

    public Route route(int consumerStage, int consumerEdge, int consumerInstance,
                       int producerStage, int producerInstance) {
        return routesByKey.get(new RouteKey(attemptFence,
                consumerStage, consumerEdge, consumerInstance,
                producerStage, producerInstance));
    }

    private static Map<Integer, List<PhysicalPlan.Instance>> indexInstances(
            ExecutionGraph graph, PhysicalPlan physicalPlan,
            Map<String, RegistryWorker> workers) {
        Map<Integer, Stage> stages = new HashMap<>();
        for (Stage stage : graph.stages()) {
            if (stages.put(stage.index(), stage) != null) {
                throw new IllegalArgumentException("duplicate logical stage index");
            }
        }
        Map<Integer, List<PhysicalPlan.Instance>> result = new HashMap<>();
        Set<String> identities = new HashSet<>();
        for (PhysicalPlan.Instance instance : physicalPlan.instances()) {
            if (!stages.containsKey(instance.logicalStage())) {
                throw new IllegalArgumentException("physical instance references unknown stage");
            }
            requireWorker(workers, instance.workerId());
            String identity = instance.logicalStage() + ":" + instance.instanceIndex();
            if (!identities.add(identity)) {
                throw new IllegalArgumentException("duplicate physical stage instance");
            }
            result.computeIfAbsent(instance.logicalStage(), ignored -> new ArrayList<>())
                    .add(instance);
        }
        for (List<PhysicalPlan.Instance> stageInstances : result.values()) {
            stageInstances.sort(Comparator.comparingInt(PhysicalPlan.Instance::instanceIndex));
            for (int i = 0; i < stageInstances.size(); i++) {
                if (stageInstances.get(i).instanceIndex() != i) {
                    throw new IllegalArgumentException(
                            "physical instance indexes must be contiguous from zero");
                }
            }
        }
        return result;
    }

    private static List<PhysicalPlan.Instance> requireInstances(
            Map<Integer, List<PhysicalPlan.Instance>> instances, int stage) {
        List<PhysicalPlan.Instance> result = instances.get(stage);
        if (result == null || result.isEmpty()) {
            throw new IllegalArgumentException("logical stage has no physical instances: " + stage);
        }
        return result;
    }

    private static RegistryWorker requireWorker(
            Map<String, RegistryWorker> workers, String workerId) {
        RegistryWorker worker = workers.get(workerId);
        if (worker == null) throw new IllegalArgumentException("physical worker is unavailable");
        return worker;
    }

    private static int requireDataPort(RegistryWorker worker) {
        if (worker.dataPort() < 1 || worker.dataPort() > 65535) {
            throw new IllegalArgumentException("remote Worker has no valid data port");
        }
        return worker.dataPort();
    }

    private static WorkerDataIdentity exactIdentity(RegistryWorker worker) {
        DataPlaneRegistration registration = worker.dataPlaneRegistration();
        RpcSecurityMetadata metadata = worker.securityMetadata();
        if (registration == null
                || registration.protocol()
                != DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1
                || metadata == null || worker.incarnationId() == null) {
            throw new IllegalArgumentException(
                    "remote shuffle grant requires exact authenticated Worker identity");
        }
        return WorkerDataIdentity.authenticated(
                worker.workerId(), worker.incarnationId(), registration.protocol(),
                metadata.deploymentSetId(), metadata.authzSetId(),
                registration.certificateFingerprint());
    }

    private static UUID freshTarget(SecureRandom random, Set<UUID> allocated) {
        for (int attempt = 0; attempt < 16; attempt++) {
            UUID candidate = new UUID(random.nextLong(), random.nextLong());
            if (!Frame.NO_TARGET.equals(candidate) && allocated.add(candidate)) return candidate;
        }
        throw new IllegalStateException("secure target id generator repeated values");
    }

    private static String localKey(String jobIdentity, TargetKey target) {
        return "L" + jobIdentity + '#' + target.consumerStage + '#'
                + target.consumerInstance + '#' + target.canonicalEdge;
    }
}
