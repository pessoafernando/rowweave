package io.rowweave.release;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReleaseContractManifestTest {

    private static final String API_DIGEST = repeat('a', 64);

    @Test
    void canonicalManifestRoundTripsWithStableIdentity() {
        ReleaseComponentContract plan = component(
                "rowweave.plan-artifact-envelope", ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                repeat('1', 64), true);
        ReleaseComponentContract schema = component(
                "rowweave.row-schema", ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                repeat('2', 64), true);
        ReleaseContractManifest first = manifest(
                Arrays.asList(schema, plan),
                Arrays.asList("rowweave.runtime.batch.v1", "rowweave.changelog.append.v1"));
        ReleaseContractManifest second = manifest(
                Arrays.asList(plan, schema),
                Arrays.asList("rowweave.changelog.append.v1", "rowweave.runtime.batch.v1"));

        assertEquals(Arrays.asList(plan, schema), first.componentContracts());
        assertEquals(Arrays.asList(
                "rowweave.changelog.append.v1", "rowweave.runtime.batch.v1"),
                first.runtimeCapabilities());
        assertArrayEquals(ReleaseContractManifestCodec.encode(first),
                ReleaseContractManifestCodec.encode(second));
        assertEquals(first.manifestDigest(), second.manifestDigest());
        assertEquals(first, ReleaseContractManifestCodec.decode(
                ReleaseContractManifestCodec.encode(first)));
    }

    @Test
    void rejectsDuplicateOrInvalidIdentityBeforeEncoding() {
        ReleaseComponentContract plan = component(
                "rowweave.plan-artifact-envelope", ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                repeat('1', 64), true);

        ReleaseContractException duplicate = assertThrows(
                ReleaseContractException.class,
                () -> manifest(Arrays.asList(plan, plan),
                        Collections.singletonList("rowweave.runtime.batch.v1")));
        assertEquals("DUPLICATE_COMPONENT", duplicate.code());

        ReleaseContractException duplicateCapability = assertThrows(
                ReleaseContractException.class,
                () -> manifest(Collections.singletonList(plan),
                        Arrays.asList("rowweave.runtime.batch.v1", "rowweave.runtime.batch.v1")));
        assertEquals("DUPLICATE_CAPABILITY", duplicateCapability.code());

        ReleaseContractException badDigest = assertThrows(
                ReleaseContractException.class,
                () -> new ReleaseComponentContract(
                        "rowweave.bad", 1, 0, 0, 0, 0,
                        ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                        "not-a-digest", true));
        assertEquals("INVALID_DIGEST", badDigest.code());
    }

    @Test
    void corruptUnknownAndOversizedFramesFailClosed() {
        byte[] encoded = ReleaseContractManifestCodec.encode(manifest(
                Collections.singletonList(component(
                        "rowweave.plan-artifact-envelope",
                        ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                        repeat('1', 64), true)),
                Collections.singletonList("rowweave.runtime.batch.v1")));

        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertCode("CHECKSUM_MISMATCH", corrupt);
        assertCode("FRAME_LENGTH_MISMATCH",
                Arrays.copyOf(encoded, encoded.length - 1));
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        assertCode("FRAME_LENGTH_MISMATCH", trailing);
        byte[] unknownVersion = encoded.clone();
        unknownVersion[ReleaseContractManifestCodec.magic().length + 3] = 2;
        assertCode("UNSUPPORTED_VERSION", unknownVersion);

        byte[] oversized = new byte[ReleaseContractManifestCodec.MAX_FRAME_BYTES + 1];
        assertCode("LIMIT_EXCEEDED", oversized);
    }

    @Test
    void preV1InventoryIsExhaustiveAndCannotBePublished() {
        List<ReleaseComponentContract> inventory =
                ReleaseContractCatalog.preV1Inventory();
        Set<String> ids = new HashSet<>();
        for (ReleaseComponentContract component : inventory) {
            assertTrue(ids.add(component.componentId()), component.componentId());
        }
        assertEquals(expectedComponentIds(), ids);

        assertEquals(ReleaseComponentContract.Scope.LEGACY_EPHEMERAL,
                byId(inventory, "rowweave.java-object-plan-payload").scope());
        assertEquals(ReleaseComponentContract.Scope.LEGACY_EPHEMERAL,
                byId(inventory, "rowweave.rpc-enum-ordinal").scope());
        assertEquals(ReleaseComponentContract.Scope.INTERNAL_SAME_BUILD,
                byId(inventory, "rowweave.plan-graph").scope());
        assertEquals(ReleaseComponentContract.Scope.INTERNAL_SAME_BUILD,
                byId(inventory, "rowweave.store-layout").scope());
        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                byId(inventory, "rowweave.portable-batch-plan").scope());
        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                byId(inventory, "rowweave.function-provider").scope());
        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                byId(inventory, "rowweave.portable-connector-provider").scope());
        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                byId(inventory, "rowweave.incremental-frontier").scope());
        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                byId(inventory, "rowweave.store-format-header").scope());
        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                byId(inventory, "rowweave.upgrade-authority").scope());
        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                byId(inventory, "rowweave.control-rpc").scope());
        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                byId(inventory, "rowweave.control-protocol").scope());
        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                byId(inventory, "rowweave.stage-command").scope());

        ReleaseContractManifest candidate = manifest(inventory,
                Collections.singletonList("rowweave.runtime.batch.v1"));
        ReleaseContractException failure = assertThrows(
                ReleaseContractException.class,
                () -> ReleaseContractCatalog.requirePublishable(candidate));
        assertEquals("NON_PUBLIC_COMPONENT", failure.code());
        assertFalse(failure.getMessage().contains("java.lang"));
    }

    @Test
    void publishedManifestRequiresEveryFrozenV1Boundary() {
        List<ReleaseComponentContract> inventory = new ArrayList<>(
                ReleaseContractCatalog.preV1Inventory());
        inventory.remove(byId(inventory, "rowweave.execution-event"));
        List<ReleaseComponentContract> publicOnly = new ArrayList<>();
        for (ReleaseComponentContract component : inventory) {
            if (component.scope() == ReleaseComponentContract.Scope.PUBLIC_FROZEN) {
                publicOnly.add(component);
            }
        }
        ReleaseContractManifest incomplete = manifest(publicOnly,
                Collections.singletonList("rowweave.runtime.batch.v1"));

        ReleaseContractException failure = assertThrows(
                ReleaseContractException.class,
                () -> ReleaseContractCatalog.requirePublishable(incomplete));
        assertEquals("MISSING_REQUIRED_COMPONENT", failure.code());
        assertEquals("components", failure.path());
    }

    @Test
    void publicationRejectsUnknownOrDriftingCatalogContracts() {
        List<ReleaseComponentContract> publicOnly = new ArrayList<>();
        for (ReleaseComponentContract component :
                ReleaseContractCatalog.preV1Inventory()) {
            if (component.scope() == ReleaseComponentContract.Scope.PUBLIC_FROZEN) {
                publicOnly.add(component);
            }
        }
        ReleaseComponentContract original = publicOnly.get(0);
        publicOnly.set(0, component(original.componentId(), original.scope(),
                repeat('f', 64), original.required()));
        ReleaseContractException drift = assertThrows(
                ReleaseContractException.class,
                () -> ReleaseContractCatalog.requirePublishable(
                        manifest(publicOnly,
                                Collections.singletonList("rowweave.runtime.batch.v1"))));
        assertEquals("CONTRACT_DRIFT", drift.code());

        ReleaseComponentContract unknown = component(
                "rowweave.unknown-public-contract",
                ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                repeat('e', 64), true);
        ReleaseContractException unknownFailure = assertThrows(
                ReleaseContractException.class,
                () -> ReleaseContractCatalog.requirePublishable(manifest(
                        Collections.singletonList(unknown),
                        Collections.singletonList("rowweave.runtime.batch.v1"))));
        assertEquals("UNKNOWN_COMPONENT", unknownFailure.code());
    }

    private static ReleaseContractManifest manifest(
            List<ReleaseComponentContract> components,
            List<String> capabilities) {
        return new ReleaseContractManifest(
                "rowweave-1.0.0", 1, 0, 0, 0, components, capabilities,
                API_DIGEST, "rowweave-decimal-semantics-v1");
    }

    private static ReleaseComponentContract component(
            String id, ReleaseComponentContract.Scope scope,
            String digest, boolean required) {
        return new ReleaseComponentContract(
                id, 1, 0, 0, 0, 0, scope, digest, required);
    }

    private static ReleaseComponentContract byId(
            List<ReleaseComponentContract> components, String id) {
        for (ReleaseComponentContract component : components) {
            if (id.equals(component.componentId())) return component;
        }
        throw new AssertionError("missing component " + id);
    }

    private static Set<String> expectedComponentIds() {
        return new HashSet<>(Arrays.asList(
                "rowweave.release-contract-manifest",
                "rowweave.plan-artifact-envelope",
                "rowweave.portable-batch-plan",
                "rowweave.plan-graph",
                "rowweave.java-object-plan-payload",
                "rowweave.row-schema",
                "rowweave.batch-source-split",
                "rowweave.batch-sink",
                "rowweave.durable-sink-provider",
                "rowweave.function-provider",
                "rowweave.portable-connector-provider",
                "rowweave.incremental-source-provider",
                "rowweave.incremental-frontier",
                "rowweave.incremental-record-position",
                "rowweave.captured-target-receipt",
                "rowweave.incremental-definition",
                "rowweave.incremental-authority",
                "rowweave.manifest-publication",
                "rowweave.run-attempt",
                "rowweave.execution-event",
                "rowweave.control-rpc",
                "rowweave.control-protocol",
                "rowweave.rpc-enum-ordinal",
                "rowweave.stage-command",
                "rowweave.data-protocol",
                "rowweave.store-layout",
                "rowweave.store-format-header",
                "rowweave.upgrade-authority"));
    }

    private static void assertCode(String code, byte[] frame) {
        ReleaseContractException failure = assertThrows(
                ReleaseContractException.class,
                () -> ReleaseContractManifestCodec.decode(frame));
        assertEquals(code, failure.code());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
