package io.rowweave.portable;

/** Allocation limits checked before copying portable binding frames. */
public final class PortableBindingLimits {
    public static final int MAX_CONFIGURATION_BYTES = 1024 * 1024;
    public static final int MAX_DESCRIPTOR_BYTES = 1024 * 1024;
    public static final int MAX_SPLIT_FRAME_BYTES = 64 * 1024;
    public static final int MAX_SPLITS = 4096;
    public static final int MAX_TOTAL_SPLIT_BYTES = 16 * 1024 * 1024;

    private PortableBindingLimits() { }
}
