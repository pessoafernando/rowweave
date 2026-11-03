package io.rowweave.stream.spi;

import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import java.io.Serializable;

/** Local Worker resolver for one explicitly identified signed deployment set. */
public interface StreamFunctionProvider {
    String deploymentSetId();
    Serializable resolve(FunctionDeploymentReference reference);

    /**
     * Confirms that this exact deployment and replay authority can reproduce
     * the same logical async result after recovery. Providers remain
     * fail-closed unless they explicitly attest the declaration.
     */
    default boolean supportsAsyncReplay(
            FunctionDeploymentReference reference,
            StreamAsyncReplaySpec replay) {
        return false;
    }
}
