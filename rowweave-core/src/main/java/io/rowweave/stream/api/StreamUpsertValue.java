package io.rowweave.stream.api;

import io.rowweave.DataRow;

/** Decoded immutable-view UPSERT key and after-image for connector SDKs. */
public final class StreamUpsertValue {
    private final DataRow stableKey;
    private final DataRow afterImage;

    StreamUpsertValue(DataRow stableKey, DataRow afterImage) {
        this.stableKey = stableKey.copy();
        this.afterImage = afterImage.copy();
    }

    public DataRow stableKey() { return stableKey.copy(); }
    public DataRow afterImage() { return afterImage.copy(); }
}
