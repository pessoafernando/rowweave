package io.rowweave.mem;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.lang.management.ManagementFactory;

/** Support matrix for managed-memory estimates and the maximum object layout; a mismatch fails loudly. */
final class ManagedMemoryLayout {

    static final long MAX_ALIGNMENT = 16;
    static final long MAX_OBJECT_HEADER = 16;
    static final long MAX_ARRAY_HEADER = 32;
    static final long MAX_REFERENCE = 8;

    private static final boolean SUPPORTED = verifyRuntime();

    private ManagedMemoryLayout() {
    }

    static void requireSupported() {
        if (!SUPPORTED) {
            throw new IllegalStateException("unsupported managed-memory VM layout");
        }
    }

    static long align(long bytes) {
        return Math.multiplyExact(Math.addExact(bytes, MAX_ALIGNMENT - 1) / MAX_ALIGNMENT, MAX_ALIGNMENT);
    }

    static long arrayBytes(long elementBytes) {
        return align(Math.addExact(MAX_ARRAY_HEADER, elementBytes));
    }

    private static boolean verifyRuntime() {
        String vmName = System.getProperty("java.vm.name", "");
        String dataModel = System.getProperty("sun.arch.data.model", "");
        String specVersion = System.getProperty("java.specification.version", "");
        if ((!vmName.contains("HotSpot") && !vmName.contains("OpenJDK"))
                || !"64".equals(dataModel) || !"17".equals(specVersion)) {
            throw new IllegalStateException("managed memory requires HotSpot/OpenJDK 17 64-bit: vm="
                    + vmName + " dataModel=" + dataModel + " java=" + specVersion);
        }
        try {
            HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            int alignment = Integer.parseInt(bean.getVMOption("ObjectAlignmentInBytes").getValue());
            if (alignment <= 0 || alignment > MAX_ALIGNMENT) {
                throw new IllegalStateException("unsupported ObjectAlignmentInBytes=" + alignment
                        + " (maximum " + MAX_ALIGNMENT + ")");
            }
            // Both compressed and uncompressed references/class pointers are covered by the 8-byte maxima.
            bean.getVMOption("UseCompressedOops");
            bean.getVMOption("UseCompressedClassPointers");
            return true;
        } catch (RuntimeException failure) {
            throw new IllegalStateException("cannot verify managed-memory VM layout", failure);
        }
    }
}
