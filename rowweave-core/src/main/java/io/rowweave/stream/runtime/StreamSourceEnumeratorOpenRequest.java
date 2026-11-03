package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamSourceBinding;

/** Immutable, credential-free request for opening a dynamic enumerator. */
public final class StreamSourceEnumeratorOpenRequest {
    private final String sourceOperatorUid;
    private final StreamSourceBinding binding;

    public StreamSourceEnumeratorOpenRequest(String sourceOperatorUid,
            StreamSourceBinding binding) {
        if (sourceOperatorUid == null || sourceOperatorUid.isEmpty()
                || binding == null || !binding.dynamicDiscovery()) {
            throw new IllegalArgumentException(
                    "dynamic source enumerator request is invalid");
        }
        this.sourceOperatorUid = sourceOperatorUid;
        this.binding = binding;
    }

    public String sourceOperatorUid() { return sourceOperatorUid; }
    public StreamSourceBinding binding() { return binding; }
}
