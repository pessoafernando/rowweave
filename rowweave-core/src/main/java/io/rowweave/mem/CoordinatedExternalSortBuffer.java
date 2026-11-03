package io.rowweave.mem;

/** Lets OrderBy switch every partition to external sorting once any single partition has spilled. */
public interface CoordinatedExternalSortBuffer {

    boolean externalized();

    void externalizeFromEscrow();
}
