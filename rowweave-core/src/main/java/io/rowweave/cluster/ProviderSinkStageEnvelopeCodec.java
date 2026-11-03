package io.rowweave.cluster;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.DurableSinkConfigurationEnvelope;
import io.rowweave.DurableSinkConfigurationEnvelopeCodec;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.LongConsumer;

/** Native, bounded StageCommand carrier for one core provider proxy. */
final class ProviderSinkStageEnvelopeCodec {
    static final int MAX_FRAME_BYTES =
            DurableSinkConfigurationEnvelopeCodec.MAX_FRAME_BYTES + 16 * 1024;
    private static final int MAGIC = 0x5257534f; // RWSO
    private static final int VERSION = 1;

    private ProviderSinkStageEnvelopeCodec() { }

    static boolean hasMagic(byte[] bytes, int offset, int length) {
        if (bytes == null || offset < 0 || length < 4
                || offset > bytes.length - length) return false;
        return java.nio.ByteBuffer.wrap(bytes, offset, 4).getInt() == MAGIC;
    }

    static byte[] encode(
            ProviderBatchSink output, LongConsumer reservation) {
        if (output == null || reservation == null) {
            throw new IllegalArgumentException(
                    "provider output and reservation are required");
        }
        byte[] descriptor = BatchSinkDescriptorCodec.encode(
                output.batchSinkDescriptor());
        byte[] configuration = DurableSinkConfigurationEnvelopeCodec.encode(
                output.configurationCodec(), output.configurationBytes());
        int length = Math.addExact(16,
                Math.addExact(descriptor.length, configuration.length));
        if (length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "provider Stage configuration exceeds bound");
        }
        reservation.accept(Math.multiplyExact(2L, length));
        byte[] frame = new byte[length];
        java.nio.ByteBuffer out = java.nio.ByteBuffer.wrap(frame);
        out.putInt(MAGIC).putInt(VERSION);
        out.putInt(descriptor.length).put(descriptor);
        out.putInt(configuration.length).put(configuration);
        return frame;
    }

    static View decode(
            byte[] bytes, int offset, int length, LongConsumer reservation) {
        if (bytes == null || reservation == null || offset < 0 || length < 16
                || length > MAX_FRAME_BYTES || offset > bytes.length - length) {
            throw new IllegalArgumentException(
                    "provider Stage configuration length is invalid");
        }
        try {
            DataInputStream in = new DataInputStream(
                    new ByteArrayInputStream(bytes, offset, length));
            if (in.readInt() != MAGIC || in.readInt() != VERSION) {
                throw new IllegalArgumentException(
                        "provider Stage configuration header is invalid");
            }
            int descriptorLength = boundedLength(
                    in.readInt(), length - 16, "descriptor");
            reservation.accept(Math.multiplyExact(3L, descriptorLength));
            byte[] descriptorBytes = new byte[descriptorLength];
            in.readFully(descriptorBytes);
            BatchSinkDescriptor descriptor =
                    BatchSinkDescriptorCodec.decode(descriptorBytes);
            if (!Arrays.equals(descriptorBytes,
                    BatchSinkDescriptorCodec.encode(descriptor))) {
                throw new IllegalArgumentException(
                        "provider Stage descriptor is noncanonical");
            }
            int configurationLength = boundedLength(
                    in.readInt(), in.available(), "configuration");
            if (configurationLength != in.available()) {
                throw new IllegalArgumentException(
                        "provider Stage configuration has trailing bytes");
            }
            reservation.accept(Math.multiplyExact(3L, configurationLength));
            byte[] configurationBytes = new byte[configurationLength];
            in.readFully(configurationBytes);
            DurableSinkConfigurationEnvelope configuration =
                    DurableSinkConfigurationEnvelopeCodec.decode(
                            configurationBytes);
            return new View(descriptor, configuration);
        } catch (IOException truncated) {
            throw new IllegalArgumentException(
                    "provider Stage configuration is truncated", truncated);
        }
    }

    private static int boundedLength(int value, int remaining, String field) {
        if (value < 1 || value > remaining || value > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "provider Stage " + field + " length is invalid");
        }
        return value;
    }

    static final class View {
        private final BatchSinkDescriptor descriptor;
        private final DurableSinkConfigurationEnvelope configuration;

        private View(BatchSinkDescriptor descriptor,
                     DurableSinkConfigurationEnvelope configuration) {
            this.descriptor = descriptor;
            this.configuration = configuration;
        }

        BatchSinkDescriptor descriptor() { return descriptor; }
        io.rowweave.DurableSinkCodecDescriptor configurationCodec() {
            return configuration.configurationCodec();
        }
        byte[] configurationBytes() {
            return configuration.configurationBytes();
        }
        ProviderBatchSink materialize(DataSchema dataSchema) {
            return new ProviderBatchSink(dataSchema, descriptor,
                    configuration.configurationCodec(),
                    configuration.configurationBytes());
        }
    }
}
