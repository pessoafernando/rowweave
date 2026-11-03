package io.rowweave.cluster;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** A new persistent stage command field may never be silently left out of the versioned codec. */
final class StageCommandFieldInventory {
    private static final Set<String> ROUTING = names(
            "role", "dataSchema", "window", "host", "port", "downstream", "keyFields",
            "numericNormalized", "inboundConnections", "sinkEndpoint", "rightMeta",
            "rightInboundConnections", "joinOutMeta", "rightKeyFields", "joinType", "joinStrategy",
            "downKeys", "downNormalized", "downFixedRouting", "downFixedPartition",
            "downTargets", "broadcastGroups", "localInbound",
            "localInboundRight", "localInboundKey", "localInboundKeyRight", "objectReuse",
            "inboundTargetId", "inboundTargetIdRight", "commitGroupId", "manifestAttemptId",
            "manifestGeneration", "runAttemptFence", "sourceSplitAssignment",
            "inboundAuthorizations", "publicationId", "publicationGeneration",
            "portableStageSpec");
    private static final Set<String> PLANS = names(
            "fragmentBytes", "sinkFragmentBytes", "portableArtifactBytes");
    private static final Set<String> OBJECTS = names("input", "output", "coGroupFn");

    private StageCommandFieldInventory() {
    }

    static void assertComplete() {
        Set<String> actual = new HashSet<>();
        for (Field field : StageCommand.class.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (!Modifier.isStatic(modifiers) && !Modifier.isTransient(modifiers)) {
                actual.add(field.getName());
            }
        }
        Set<String> classified = new HashSet<>();
        addDisjoint(classified, ROUTING, "routing");
        addDisjoint(classified, PLANS, "plan");
        addDisjoint(classified, OBJECTS, "object");
        if (!actual.equals(classified)) {
            Set<String> missing = new HashSet<>(actual);
            missing.removeAll(classified);
            Set<String> stale = new HashSet<>(classified);
            stale.removeAll(actual);
            throw new IllegalStateException(
                    "StageCommand V2 field inventory mismatch missing=" + missing + " stale=" + stale);
        }
    }

    private static void addDisjoint(Set<String> target, Set<String> values, String category) {
        for (String value : values) {
            if (!target.add(value)) {
                throw new IllegalStateException(
                        "StageCommand field classified more than once: " + value + " in " + category);
            }
        }
    }

    private static Set<String> names(String... values) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(values)));
    }
}
