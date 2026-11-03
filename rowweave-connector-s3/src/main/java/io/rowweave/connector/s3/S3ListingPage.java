package io.rowweave.connector.s3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class S3ListingPage {
    private final List<String> keys;
    private final boolean truncated;
    private final String nextContinuationToken;

    S3ListingPage(List<String> keys, boolean truncated,
            String nextContinuationToken) {
        if (keys == null) {
            throw new IllegalArgumentException("S3_MANIFEST_LISTING_INVALID");
        }
        this.keys = Collections.unmodifiableList(new ArrayList<>(keys));
        this.truncated = truncated;
        this.nextContinuationToken = nextContinuationToken;
    }

    List<String> keys() { return keys; }
    boolean truncated() { return truncated; }
    String nextContinuationToken() { return nextContinuationToken; }
}
