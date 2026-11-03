package io.rowweave.connector.postgres;

import java.util.Locale;

/** Exact external recovery/ack boundary for one PostgreSQL database. */
public final class PostgresCdcPosition {
    public enum Boundary { SNAPSHOT_HANDOFF, TRANSACTION_COMMIT }

    private static final String CODE = "POSTGRES_CDC_POSITION_INVALID";
    private final Boundary boundary;
    private final String databaseSystemIdentifier;
    private final long databaseOid;
    private final long endLsnBits;
    private final long transactionId;

    private PostgresCdcPosition(Boundary boundary,
            String databaseSystemIdentifier, long databaseOid,
            String endLsn, long transactionId) {
        if (boundary == null) throw invalid();
        this.boundary = boundary;
        this.databaseSystemIdentifier =
                PostgresCodecSupport.requireSystemIdentifier(
                        databaseSystemIdentifier, CODE);
        this.databaseOid = PostgresCodecSupport.requireUint32(
                databaseOid, false, CODE);
        this.endLsnBits = parseLsn(endLsn);
        if ((boundary == Boundary.TRANSACTION_COMMIT)
                != (transactionId != 0L)) {
            throw invalid();
        }
        this.transactionId = PostgresCodecSupport.requireUint32(
                transactionId, true, CODE);
    }

    public static PostgresCdcPosition snapshotHandoff(
            String databaseSystemIdentifier, long databaseOid,
            String consistentEndLsn) {
        return new PostgresCdcPosition(Boundary.SNAPSHOT_HANDOFF,
                databaseSystemIdentifier, databaseOid,
                consistentEndLsn, 0L);
    }

    public static PostgresCdcPosition transactionCommit(
            String databaseSystemIdentifier, long databaseOid,
            String endLsn, long transactionId) {
        return new PostgresCdcPosition(Boundary.TRANSACTION_COMMIT,
                databaseSystemIdentifier, databaseOid,
                endLsn, transactionId);
    }

    static PostgresCdcPosition fromBits(Boundary boundary,
            long systemIdentifierBits, long databaseOid, long lsnBits,
            long transactionId) {
        return new PostgresCdcPosition(boundary,
                Long.toUnsignedString(systemIdentifierBits), databaseOid,
                formatLsn(lsnBits), transactionId);
    }

    public Boundary boundary() { return boundary; }
    public String databaseSystemIdentifier() {
        return databaseSystemIdentifier;
    }
    public long databaseOid() { return databaseOid; }
    public String endLsn() { return formatLsn(endLsnBits); }
    long endLsnBits() { return endLsnBits; }
    long databaseSystemIdentifierBits() {
        return PostgresCodecSupport.systemIdentifierBits(
                databaseSystemIdentifier, CODE);
    }

    public long transactionId() {
        if (boundary != Boundary.TRANSACTION_COMMIT) {
            throw new IllegalStateException(
                    "snapshot handoff has no transaction id");
        }
        return transactionId;
    }

    long encodedTransactionId() { return transactionId; }

    private static long parseLsn(String value) {
        if (value == null) throw invalid();
        int slash = value.indexOf('/');
        if (slash < 1 || slash != value.lastIndexOf('/')
                || slash > 8 || value.length() - slash - 1 < 1
                || value.length() - slash - 1 > 8) {
            throw invalid();
        }
        String high = value.substring(0, slash);
        String low = value.substring(slash + 1);
        if (!canonicalHex(high) || !canonicalHex(low)) throw invalid();
        try {
            long highBits = Long.parseLong(high, 16);
            long lowBits = Long.parseLong(low, 16);
            return (highBits << 32) | lowBits;
        } catch (NumberFormatException malformed) {
            throw invalid();
        }
    }

    private static boolean canonicalHex(String value) {
        if (value.length() > 1 && value.charAt(0) == '0') return false;
        for (int index = 0; index < value.length(); index++) {
            char item = value.charAt(index);
            if (!((item >= '0' && item <= '9')
                    || (item >= 'A' && item <= 'F'))) return false;
        }
        return true;
    }

    private static String formatLsn(long bits) {
        long high = (bits >>> 32) & PostgresCodecSupport.MAX_UINT32;
        long low = bits & PostgresCodecSupport.MAX_UINT32;
        return Long.toHexString(high).toUpperCase(Locale.ROOT) + '/'
                + Long.toHexString(low).toUpperCase(Locale.ROOT);
    }

    private static IllegalArgumentException invalid() {
        return PostgresCodecSupport.invalid(CODE);
    }
}
