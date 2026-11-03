package io.rowweave.mem;

/** Node-local memory-owner requirement used for pre-execution admission. */
public final class MemoryOwnerRequirement {
    private static final MemoryOwnerRequirement ORDINARY = new MemoryOwnerRequirement(1);

    private final int childArity;

    private MemoryOwnerRequirement(int childArity) {
        if (childArity <= 0) {
            throw new IllegalArgumentException("childArity must be > 0");
        }
        this.childArity = childArity;
    }

    public static MemoryOwnerRequirement ordinary() {
        return ORDINARY;
    }

    public static MemoryOwnerRequirement partitioned(int childArity) {
        return new MemoryOwnerRequirement(childArity);
    }

    public int childArity() {
        return childArity;
    }
}
