package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;

import java.util.Arrays;
import java.util.Objects;

/** One exact split descriptor plus inert provider open-request metadata. */
public final class IncrementalMaterializedSplit {
    public static final int MAX_OPEN_REQUEST_BYTES = 1024 * 1024;

    private final BatchSourceSplitDescriptor descriptor;
    private final byte[] openRequest;

    public IncrementalMaterializedSplit(
            BatchSourceSplitDescriptor descriptor, byte[] openRequest) {
        if (descriptor == null || openRequest == null
                || openRequest.length == 0
                || openRequest.length > MAX_OPEN_REQUEST_BYTES) {
            throw new IllegalArgumentException(
                    "incremental materialized split fields are invalid");
        }
        if (descriptor.replayGuarantee()
                != BatchSourceSplitDescriptor.ReplayGuarantee
                .EXACT_ORDERED_FROM_START) {
            throw new IllegalArgumentException(
                    "incremental split must replay exactly from its start");
        }
        byte[] encoded = BatchSourceSplitDescriptorCodec.encode(descriptor);
        if (encoded.length > BatchSourceSplitDescriptorCodec.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental split descriptor exceeds bound");
        }
        this.descriptor = descriptor;
        this.openRequest = openRequest.clone();
    }

    public BatchSourceSplitDescriptor descriptor() { return descriptor; }
    public byte[] openRequest() { return openRequest.clone(); }
    public int openRequestSize() { return openRequest.length; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalMaterializedSplit)) return false;
        IncrementalMaterializedSplit that =
                (IncrementalMaterializedSplit) other;
        return descriptor.equals(that.descriptor)
                && Arrays.equals(openRequest, that.openRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(descriptor, Arrays.hashCode(openRequest));
    }
}
