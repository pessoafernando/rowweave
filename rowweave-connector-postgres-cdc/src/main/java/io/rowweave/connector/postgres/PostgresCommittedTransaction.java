package io.rowweave.connector.postgres;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Bounded committed pgoutput transaction ready for ordered event emission. */
final class PostgresCommittedTransaction {
    private final long transactionId;
    private final PostgresCdcPosition position;
    private final List<byte[]> upsertFrames;

    PostgresCommittedTransaction(long transactionId,
            PostgresCdcPosition position, List<byte[]> upsertFrames) {
        if (transactionId < 1L || position == null
                || upsertFrames == null) {
            throw new IllegalArgumentException(
                    "POSTGRES_CDC_TRANSACTION_INVALID");
        }
        this.transactionId = transactionId;
        this.position = position;
        this.upsertFrames = own(upsertFrames);
    }

    long transactionId() { return transactionId; }
    PostgresCdcPosition position() { return position; }

    List<byte[]> upsertFrames() { return own(upsertFrames); }

    private static List<byte[]> own(List<byte[]> values) {
        List<byte[]> copy = new ArrayList<>(values.size());
        for (byte[] value : values) {
            if (value == null || value.length == 0) {
                throw new IllegalArgumentException(
                        "POSTGRES_CDC_TRANSACTION_INVALID");
            }
            copy.add(value.clone());
        }
        return Collections.unmodifiableList(copy);
    }
}
