package io.rowweave.release;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Honest pre-v1 inventory; internal and legacy entries block publication. */
public final class ReleaseContractCatalog {
    private ReleaseContractCatalog() { }

    public static List<ReleaseComponentContract> preV1Inventory() {
        List<ReleaseComponentContract> values = new ArrayList<>();
        addPublic(values, "rowweave.release-contract-manifest");
        addPublic(values, "rowweave.plan-artifact-envelope");
        addPublic(values, "rowweave.portable-batch-plan");
        addInternal(values, "rowweave.plan-graph");
        addLegacy(values, "rowweave.java-object-plan-payload");
        addPublic(values, "rowweave.row-schema");
        addPublic(values, "rowweave.batch-source-split");
        addPublic(values, "rowweave.batch-sink");
        addPublic(values, "rowweave.durable-sink-provider");
        addPublic(values, "rowweave.function-provider");
        addPublic(values, "rowweave.portable-connector-provider");
        addPublic(values, "rowweave.incremental-source-provider");
        addPublic(values, "rowweave.incremental-frontier");
        addPublic(values, "rowweave.incremental-record-position");
        addPublic(values, "rowweave.captured-target-receipt");
        addPublic(values, "rowweave.incremental-definition");
        addPublic(values, "rowweave.incremental-authority");
        addPublic(values, "rowweave.manifest-publication");
        addPublic(values, "rowweave.run-attempt");
        addPublic(values, "rowweave.execution-event");
        addPublic(values, "rowweave.control-rpc");
        addPublic(values, "rowweave.control-protocol");
        addLegacy(values, "rowweave.rpc-enum-ordinal");
        addPublic(values, "rowweave.stage-command");
        addPublic(values, "rowweave.data-protocol");
        addInternal(values, "rowweave.store-layout");
        addPublic(values, "rowweave.store-format-header");
        addPublic(values, "rowweave.upgrade-authority");
        Collections.sort(values);
        return Collections.unmodifiableList(values);
    }

    /** Builds the exact first public production baseline from the closed catalog. */
    public static ReleaseContractManifest publicV1Baseline(String publicApiBaselineDigest) {
        List<ReleaseComponentContract> components = new ArrayList<>();
        for (ReleaseComponentContract component : preV1Inventory()) {
            if (component.scope() == ReleaseComponentContract.Scope.PUBLIC_FROZEN) {
                components.add(component);
            }
        }
        ReleaseContractManifest manifest = new ReleaseContractManifest(
                "rowweave-1.0.0", 1, 0, 0, 0, components,
                Arrays.asList("rowweave.runtime.batch.v1"),
                publicApiBaselineDigest, "rowweave-decimal-semantics-v1");
        requirePublishable(manifest);
        return manifest;
    }

    /** Resolves one catalog entry only when it is eligible for a public v1 contract. */
    public static ReleaseComponentContract requirePublicV1Component(
            String componentId) {
        ReleaseComponentContract match = null;
        for (ReleaseComponentContract component : preV1Inventory()) {
            if (component.componentId().equals(componentId)) {
                match = component;
                break;
            }
        }
        if (match == null) {
            throw ReleaseContractText.failure("UNKNOWN_COMPONENT", "componentId",
                    "release component is not present in the closed catalog");
        }
        switch (match.scope()) {
            case PUBLIC_FROZEN:
                return match;
            case INTERNAL_SAME_BUILD:
                throw ReleaseContractText.failure("SAME_BUILD_ONLY", "componentId",
                        "release component is restricted to the same build");
            case LEGACY_EPHEMERAL:
                throw ReleaseContractText.failure("LEGACY_NOT_PUBLIC", "componentId",
                        "legacy component cannot enter a public v1 contract");
            case REQUIRED_ABSENT:
                throw ReleaseContractText.failure("REQUIRED_COMPONENT_ABSENT", "componentId",
                        "required public v1 component has not been implemented");
            default:
                throw new IllegalStateException("unhandled release component scope");
        }
    }

    public static void requirePublishable(ReleaseContractManifest manifest) {
        if (manifest == null) {
            throw ReleaseContractText.failure("INVALID_MANIFEST", "release",
                    "release manifest is required");
        }
        Map<String, ReleaseComponentContract> catalog = new HashMap<>();
        for (ReleaseComponentContract component : preV1Inventory()) {
            catalog.put(component.componentId(), component);
        }
        Set<String> present = new LinkedHashSet<>();
        for (ReleaseComponentContract component : manifest.componentContracts()) {
            ReleaseComponentContract expected = catalog.get(component.componentId());
            if (expected == null) {
                throw ReleaseContractText.failure("UNKNOWN_COMPONENT", "components",
                        "release manifest contains an unknown component");
            }
            if (component.scope() != ReleaseComponentContract.Scope.PUBLIC_FROZEN) {
                throw ReleaseContractText.failure("NON_PUBLIC_COMPONENT", "components",
                        "release manifest contains a non-public component");
            }
            if (!component.equals(expected)) {
                throw ReleaseContractText.failure("CONTRACT_DRIFT", "components",
                        "release component contract does not match the catalog");
            }
            present.add(component.componentId());
        }
        for (ReleaseComponentContract component : preV1Inventory()) {
            if (component.required()
                    && component.scope() != ReleaseComponentContract.Scope.INTERNAL_SAME_BUILD
                    && component.scope() != ReleaseComponentContract.Scope.LEGACY_EPHEMERAL
                    && !present.contains(component.componentId())) {
                throw ReleaseContractText.failure("MISSING_REQUIRED_COMPONENT", "components",
                        "release manifest is missing a required public component");
            }
        }
    }

    private static void addPublic(List<ReleaseComponentContract> values, String id) {
        add(values, id, ReleaseComponentContract.Scope.PUBLIC_FROZEN);
    }

    private static void addInternal(List<ReleaseComponentContract> values, String id) {
        add(values, id, ReleaseComponentContract.Scope.INTERNAL_SAME_BUILD);
    }

    private static void addLegacy(List<ReleaseComponentContract> values, String id) {
        add(values, id, ReleaseComponentContract.Scope.LEGACY_EPHEMERAL);
    }

    private static void addAbsent(List<ReleaseComponentContract> values, String id) {
        add(values, id, ReleaseComponentContract.Scope.REQUIRED_ABSENT);
    }

    private static void add(
            List<ReleaseComponentContract> values, String id,
            ReleaseComponentContract.Scope scope) {
        String specification = "pre-v1|" + id + "|" + scope.name() + "|format=1";
        String digest = ReleaseContractManifestCodec.hex(
                ReleaseContractManifestCodec.sha256(
                        specification.getBytes(StandardCharsets.UTF_8)));
        values.add(new ReleaseComponentContract(
                id, 1, 0, 0, 0, 0, scope, digest, true));
    }
}
