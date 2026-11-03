package io.rowweave.memory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.function.LongConsumer;

/** Initial resident reservation for a serialization writer's block buffer and handle/replace tables. */
public final class ResidentObjectOutputStreams {
    public static final long BASELINE_BYTES = 64L * 1024L;

    private ResidentObjectOutputStreams() {
    }

    /** The baseline must precede construction of the ObjectOutputStream; later growth is covered one allocation at a time by the resident byte writer. */
    public static ObjectOutputStream open(OutputStream out, LongConsumer reservation)
            throws IOException {
        return open(out, reservation, value -> { });
    }

    public static ObjectOutputStream open(OutputStream out, LongConsumer reservation,
                                          ObjectValidator validator)
            throws IOException {
        if (out == null || reservation == null) {
            throw new IllegalArgumentException("output and reservation are required");
        }
        if (validator == null) throw new IllegalArgumentException("object validator is required");
        reservation.accept(BASELINE_BYTES);
        return new ChargingObjectOutputStream(out, reservation, validator);
    }

    @FunctionalInterface
    public interface ObjectValidator {
        void validate(Object value) throws IOException;
    }

    /** replaceObject fires before the new object is written to the handle table; each reference is charged a conservative 128 bytes. */
    private static final class ChargingObjectOutputStream extends ObjectOutputStream {
        private final LongConsumer reservation;
        private final ObjectValidator validator;

        private ChargingObjectOutputStream(OutputStream out, LongConsumer reservation,
                                           ObjectValidator validator)
                throws IOException {
            super(out);
            this.reservation = reservation;
            this.validator = validator;
            enableReplaceObject(true);
        }

        @Override
        protected Object replaceObject(Object value) throws IOException {
            reservation.accept(128L);
            validator.validate(value);
            return value;
        }
    }
}
