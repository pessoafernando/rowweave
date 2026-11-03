package io.rowweave.connector.postgres;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Canonical checksummed external PostgreSQL CDC boundary frame. */
public final class PostgresCdcPositionCodec {
    public static final String CODEC_ID = "rowweave.postgres.cdc-position.v1";
    public static final int MAX_FRAME_BYTES = 128;
    private static final String CODE = "POSTGRES_CDC_POSITION_INVALID";
    private static final byte[] MAGIC =
            "RWXPGPO1".getBytes(StandardCharsets.US_ASCII);

    private PostgresCdcPositionCodec() { }

    public static byte[] encode(PostgresCdcPosition position) {
        if (position == null) throw invalid();
        return PostgresCodecSupport.encode(MAGIC, MAX_FRAME_BYTES,
                output -> {
                    output.writeByte(position.boundary().ordinal());
                    output.writeLong(
                            position.databaseSystemIdentifierBits());
                    output.writeInt((int) position.databaseOid());
                    output.writeLong(position.endLsnBits());
                    output.writeInt((int) position.encodedTransactionId());
                }, CODE);
    }

    public static PostgresCdcPosition decode(byte[] frame) {
        PostgresCdcPosition result = PostgresCodecSupport.decode(
                frame, MAGIC, MAX_FRAME_BYTES, input -> {
                    int boundary = input.readUnsignedByte();
                    if (boundary >= PostgresCdcPosition.Boundary
                            .values().length) throw invalid();
                    long systemIdentifier = input.readLong();
                    long databaseOid = Integer.toUnsignedLong(
                            input.readInt());
                    long endLsn = input.readLong();
                    long transactionId = Integer.toUnsignedLong(
                            input.readInt());
                    return PostgresCdcPosition.fromBits(
                            PostgresCdcPosition.Boundary.values()[boundary],
                            systemIdentifier, databaseOid, endLsn,
                            transactionId);
                }, CODE);
        if (!MessageDigest.isEqual(frame, encode(result))) throw invalid();
        return result;
    }

    private static IllegalArgumentException invalid() {
        return PostgresCodecSupport.invalid(CODE);
    }
}
