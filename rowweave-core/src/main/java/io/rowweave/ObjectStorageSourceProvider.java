package io.rowweave;

import java.io.Serializable;

/** Stateless, signed deployment extension used to open one exact object version on a Worker. */
public interface ObjectStorageSourceProvider extends Serializable {

    /** Stable provider family identifier, for example a vendor adapter name. */
    String providerType();

    /** Positive execution contract version. */
    int providerVersion();

    /** Open the exact request. Implementations resolve credentialRef only in the Worker. */
    ObjectStorageReadHandle open(ObjectStorageReadRequest request);
}
