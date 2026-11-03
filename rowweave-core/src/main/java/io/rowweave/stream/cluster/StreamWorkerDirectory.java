package io.rowweave.stream.cluster;

import java.util.List;

/** Current, incarnation-exact Stream Worker endpoints available to placement. */
public interface StreamWorkerDirectory {
    List<RemoteStreamWorkerEndpoint> snapshot();
}
