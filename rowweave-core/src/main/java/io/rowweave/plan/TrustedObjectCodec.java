package io.rowweave.plan;

import io.rowweave.memory.ResidentByteArrayOutputStream;
import io.rowweave.memory.ResidentObjectOutputStreams;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.function.LongConsumer;

/** Symmetric trusted codec for a standalone Source, Sink or Function object slice. */
public final class TrustedObjectCodec {
    private TrustedObjectCodec() {
    }

    public static byte[] write(Object value, Class<?> expectedRoot,
                               ObjectTrustContext trustContext, LongConsumer reservation) {
        if (trustContext == null || reservation == null) {
            throw new IllegalArgumentException("trust context and reservation are required");
        }
        try {
            trustContext.verifyRootForWrite(value, expectedRoot);
            ResidentByteArrayOutputStream bytes = new ResidentByteArrayOutputStream(
                    reservation, 1024, PlanGraphEnvelopeScanner.MAX_OBJECT_BLOB_BYTES);
            try (ObjectOutputStream output = ResidentObjectOutputStreams.open(
                    bytes, reservation, trustContext::verifyObjectForWrite)) {
                output.writeObject(value);
            }
            return bytes.toByteArray();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    public static <T> T readSlice(byte[] envelope, int offset, int length,
                                  Class<T> expectedRoot, ObjectTrustContext trustContext,
                                  LongConsumer reservation) {
        try {
            return TrustedObjectInputStream.readSlice(
                    envelope, offset, length, expectedRoot, trustContext, reservation);
        } catch (ClassNotFoundException failure) {
            throw new IllegalArgumentException("trusted object class is unavailable", failure);
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }
}
