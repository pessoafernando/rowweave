package io.rowweave.cluster;

import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.net.ShuffleAuthorizationRecord;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.net.ShuffleInboundAuthorization;
import io.rowweave.sched.net.ShuffleProducerGrant;
import io.rowweave.sched.net.WorkerDataIdentity;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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

/** Producer bearer and consumer digest projection of an already closed transport plan. */
public final class ShuffleGrantPlan {

    private static final class ConsumerKey {
        final int stage;
        final int edge;
        final int instance;
        ConsumerKey(int stage, int edge, int instance) {
            this.stage = stage; this.edge = edge; this.instance = instance;
        }
        @Override public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof ConsumerKey)) return false;
            ConsumerKey that = (ConsumerKey) other;
            return stage == that.stage && edge == that.edge && instance == that.instance;
        }
        @Override public int hashCode() { return Objects.hash(stage, edge, instance); }
    }

    private final Map<ShuffleTransportPlan.RouteKey, ShuffleEndpoint> producerEndpoints;
    private final Map<ConsumerKey, ShuffleInboundAuthorization> consumerAuthorizations;

    private ShuffleGrantPlan(
            Map<ShuffleTransportPlan.RouteKey, ShuffleEndpoint> producerEndpoints,
            Map<ConsumerKey, ShuffleInboundAuthorization> consumerAuthorizations) {
        this.producerEndpoints = Collections.unmodifiableMap(
                new LinkedHashMap<>(producerEndpoints));
        this.consumerAuthorizations = Collections.unmodifiableMap(
                new HashMap<>(consumerAuthorizations));
    }

    public static ShuffleGrantPlan create(
            ShuffleTransportPlan transportPlan, SecureRandom secureRandom) {
        if (transportPlan == null || secureRandom == null) {
            throw new IllegalArgumentException("transport plan and secure random are required");
        }
        Map<UUID, List<ShuffleTransportPlan.Route>> routesByTarget = new LinkedHashMap<>();
        for (ShuffleTransportPlan.Route route : transportPlan.routes()) {
            if (route.isLocal()) continue;
            if (route.targetId() == null || route.producerOrdinal() < 1
                    || route.sourceIdentity() == null
                    || route.destinationIdentity() == null) {
                throw new IllegalArgumentException("remote transport route is incomplete");
            }
            routesByTarget.computeIfAbsent(route.targetId(), ignored -> new ArrayList<>())
                    .add(route);
        }

        Map<ShuffleTransportPlan.RouteKey, ShuffleEndpoint> endpoints = new LinkedHashMap<>();
        Map<ConsumerKey, ShuffleInboundAuthorization> authorizations = new HashMap<>();
        Set<String> allocatedGrantFingerprints = new HashSet<>();
        for (Map.Entry<UUID, List<ShuffleTransportPlan.Route>> targetEntry
                : routesByTarget.entrySet()) {
            UUID targetId = targetEntry.getKey();
            List<ShuffleTransportPlan.Route> routes = targetEntry.getValue();
            if (routes.size() > ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET) {
                throw new IllegalArgumentException("remote producers per target exceed 4096");
            }
            routes.sort(Comparator.comparingInt(ShuffleTransportPlan.Route::producerOrdinal));
            Map<WorkerDataIdentity, Integer> cohortSizes = new HashMap<>();
            for (int i = 0; i < routes.size(); i++) {
                ShuffleTransportPlan.Route route = routes.get(i);
                if (route.producerOrdinal() != i + 1) {
                    throw new IllegalArgumentException(
                            "remote producer ordinals are not a closed contiguous set");
                }
                cohortSizes.merge(route.sourceIdentity(), 1, Math::addExact);
            }

            List<ShuffleAuthorizationRecord> records = new ArrayList<>(routes.size());
            for (ShuffleTransportPlan.Route route : routes) {
                ShuffleProducerGrant grant = freshGrant(
                        secureRandom, allocatedGrantFingerprints);
                byte[] cohortId = cohortId(route.key().attemptFence(), targetId,
                        route.sourceIdentity(), route.destinationIdentity());
                ShuffleEndpoint endpoint = ShuffleEndpoint.muxAuthorized(
                        route.destinationHost(), route.destinationDataPort(), targetId,
                        route.destinationIdentity(), grant, route.producerOrdinal(),
                        cohortId, cohortSizes.get(route.sourceIdentity()));
                if (endpoints.put(route.key(), endpoint) != null) {
                    throw new IllegalArgumentException("duplicate remote route projection");
                }
                records.add(ShuffleAuthorizationRecord.authorize(
                        route.key().attemptFence(), targetId,
                        route.producerOrdinal(), route.sourceIdentity(), grant));
            }
            ShuffleInboundAuthorization authorization = ShuffleInboundAuthorization.of(
                    targetId, cohortSizes.size(), records);
            for (ShuffleTransportPlan.Route route : routes) {
                ConsumerKey key = new ConsumerKey(route.key().consumerStage(),
                        route.key().consumerEdge(), route.key().consumerInstance());
                ShuffleInboundAuthorization previous = authorizations.put(key, authorization);
                if (previous != null && previous != authorization) {
                    throw new IllegalArgumentException(
                            "consumer route has conflicting authorization projections");
                }
            }
        }
        if (endpoints.size() != transportPlan.remoteRouteCount()) {
            throw new IllegalArgumentException(
                    "grant projection did not consume the remote route closure");
        }
        return new ShuffleGrantPlan(endpoints, authorizations);
    }

    public ShuffleEndpoint producerEndpoint(ShuffleTransportPlan.RouteKey routeKey) {
        return producerEndpoints.get(routeKey);
    }

    public ShuffleInboundAuthorization consumerAuthorization(
            int consumerStage, int edge, int consumerInstance) {
        return consumerAuthorizations.get(new ConsumerKey(
                consumerStage, edge, consumerInstance));
    }

    /** Unique target aggregates for one consumer command, ordered by input edge. */
    public List<ShuffleInboundAuthorization> consumerAuthorizations(
            int consumerStage, int consumerInstance) {
        List<Map.Entry<ConsumerKey, ShuffleInboundAuthorization>> matches =
                new ArrayList<>();
        for (Map.Entry<ConsumerKey, ShuffleInboundAuthorization> entry
                : consumerAuthorizations.entrySet()) {
            ConsumerKey key = entry.getKey();
            if (key.stage == consumerStage && key.instance == consumerInstance) {
                matches.add(entry);
            }
        }
        matches.sort(Comparator.comparingInt(entry -> entry.getKey().edge));
        Map<UUID, ShuffleInboundAuthorization> unique = new LinkedHashMap<>();
        for (Map.Entry<ConsumerKey, ShuffleInboundAuthorization> entry : matches) {
            ShuffleInboundAuthorization authorization = entry.getValue();
            unique.putIfAbsent(authorization.targetId(), authorization);
        }
        return Collections.unmodifiableList(new ArrayList<>(unique.values()));
    }

    public int remoteGrantCount() { return producerEndpoints.size(); }

    private static ShuffleProducerGrant freshGrant(
            SecureRandom random, Set<String> allocatedFingerprints) {
        for (int attempt = 0; attempt < 16; attempt++) {
            ShuffleProducerGrant grant = ShuffleProducerGrant.generate(random);
            byte[] raw = grant.copyBytes();
            byte[] fingerprint = sha256().digest(raw);
            Arrays.fill(raw, (byte) 0);
            String key = Base64.getEncoder().encodeToString(fingerprint);
            Arrays.fill(fingerprint, (byte) 0);
            if (allocatedFingerprints.add(key)) return grant;
        }
        throw new IllegalStateException("secure grant generator repeated values");
    }

    private static byte[] cohortId(
            RunAttemptFence fence, UUID targetId,
            WorkerDataIdentity source, WorkerDataIdentity destination) {
        MessageDigest digest = sha256();
        digest.update("rowweave.shuffle.cohort.v1".getBytes(StandardCharsets.US_ASCII));
        updateString(digest, fence.runId());
        updateString(digest, fence.attemptId());
        updateLong(digest, fence.fenceToken());
        updateLong(digest, targetId.getMostSignificantBits());
        updateLong(digest, targetId.getLeastSignificantBits());
        updateIdentity(digest, source);
        updateIdentity(digest, destination);
        return digest.digest();
    }

    private static void updateIdentity(MessageDigest digest, WorkerDataIdentity identity) {
        updateString(digest, identity.workerId());
        updateString(digest, identity.incarnationId());
        updateString(digest, identity.protocol().name());
        updateString(digest, identity.deploymentSetId());
        updateString(digest, identity.authzSetId());
        digest.update(identity.certificateFingerprint());
    }

    private static void updateString(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
