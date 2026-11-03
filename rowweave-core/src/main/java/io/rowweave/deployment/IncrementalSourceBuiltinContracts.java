package io.rowweave.deployment;

import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.JdbcIncrementalSourceProfile;
import io.rowweave.incremental.ObjectManifestIncrementalSourceProfile;

import java.util.Arrays;
import java.util.List;

/** Exact same-build contracts for the three Phase-2 production profiles. */
final class IncrementalSourceBuiltinContracts {
    private IncrementalSourceBuiltinContracts() { }

    static List<IncrementalSourceProviderContract> all() {
        return Arrays.asList(
                JdbcIncrementalSourceProfile.sequenceContract(),
                JdbcIncrementalSourceProfile.timestampKeyContract(),
                ObjectManifestIncrementalSourceProfile.builtInContract());
    }
}
