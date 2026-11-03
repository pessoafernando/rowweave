package io.rowweave.stream.cluster;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Exhaustive GDF1 retirement manifest for known job-level shape gates.
 *
 * <p>Entries may only disappear as GDF2 migrates code to the node-local
 * lifecycle. Adding an entry means the architecture regressed.</p>
 */
final class StreamShapeGateDebtManifest {
    private static final Set<String> PRODUCT_PLAN_SINGLETON_CONSUMERS =
            Collections.emptySet();

    private static final Set<String> FIXED_TWO_NODE_EXECUTORS =
            Collections.emptySet();

    private static final Set<String> SHAPE_PREDICATE_TOKENS = set(
            "isTemporalJoin(",
            "isKeyedCoProcess(",
            "hasSerialUnaryStatefulGraph(",
            "validateStatefulShape(",
            "validateTemporalShape(",
            "validateCoProcessShape(");

    private static final Set<String> SHAPE_PREDICATE_SITES =
            Collections.emptySet();

    private StreamShapeGateDebtManifest() { }

    static Set<String> productPlanSingletonConsumers() {
        return PRODUCT_PLAN_SINGLETON_CONSUMERS;
    }

    static Set<String> fixedTwoNodeExecutors() {
        return FIXED_TWO_NODE_EXECUTORS;
    }

    static Set<String> shapePredicateTokens() {
        return SHAPE_PREDICATE_TOKENS;
    }

    static Set<String> shapePredicateSites() {
        return SHAPE_PREDICATE_SITES;
    }

    private static Set<String> set(String... values) {
        return Collections.unmodifiableSet(new LinkedHashSet<>(
                Arrays.asList(values)));
    }
}
