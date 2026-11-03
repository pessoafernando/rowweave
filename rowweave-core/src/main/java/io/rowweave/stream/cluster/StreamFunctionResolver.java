package io.rowweave.stream.cluster;

import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.stream.api.StreamAsyncReplaySpec;

import java.io.Serializable;

/** Worker-local resolver for an exact signed Stream function deployment. */
@FunctionalInterface
public interface StreamFunctionResolver {
    Serializable resolve(FunctionDeploymentReference reference);

    default boolean supportsAsyncReplay(
            FunctionDeploymentReference reference,
            StreamAsyncReplaySpec replay) {
        return false;
    }
}
