package io.rowweave.cluster.rpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamClass;

/**
 * A deserialization whitelist ObjectInputStream, which is a hard security baseline: control-plane RPC reads
 * bytes off a ServerSocket, and a bare {@code ObjectInputStream.readObject()} there is a gadget-chain RCE
 * surface. This class overrides {@link #resolveClass} to admit only {@code io.rowweave.*} plus the JDK
 * classes genuinely needed (collections, numbers, arrays, lambda metadata) and throws
 * {@link InvalidClassException} for everything else.
 *
 * <p>Targeting JDK 1.8, which has no {@code setObjectInputFilter} (a JDK 9+ API), so the whitelist is a
 * subclass overriding resolveClass.
 *
 * <p>Used only to decode the runtime object blob embedded in a StageCommand; the RPC message itself goes
 * through an explicit length prefix and per-field codec and never reaches this class. No deployment or
 * classloader protocol is applied today, so custom Source/Sink/Function classes from an application package
 * fail closed at this boundary.
 */
public final class WhitelistObjectInputStream extends io.rowweave.memory.ResidentObjectInputStream {

    private static final String[] ALLOWED_PREFIXES = {
            "io.rowweave.",
            "java.lang.",
            "java.math.",
            "java.util.",
            "java.time.",
            "[",  // array type descriptor, for example [B or [Lio.rowweave.DataRow;
    };
    private static final String[] ALLOWED_EXACT_CLASSES = {
            "java.sql.Timestamp",  // public scalar value of DataType.TimestampType; java.sql.* stays out
    };
    public WhitelistObjectInputStream(InputStream in) throws IOException {
        super(in);
    }

    public WhitelistObjectInputStream(InputStream in, ControlResidentLease lease,
                                      ControlResidentLease.OwnerToken owner) throws IOException {
        super(in, bytes -> lease.reserve(owner, bytes));
        if (lease == null || owner == null) {
            throw new IllegalArgumentException("resident lease and owner are required");
        }
        // The superclass has installed the filter once the ObjectInputStream header was read.
    }

    /** ObjectInputStream wraps a rejection, so the caller recovers the stable resource rejection code from it. */
    public ControlResidentBudgetExceededException residentFailure() {
        RuntimeException failure = residentReservationFailure();
        return failure instanceof ControlResidentBudgetExceededException
                ? (ControlResidentBudgetExceededException) failure : null;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        String name = desc.getName();
        for (String prefix : ALLOWED_PREFIXES) {
            if (name.startsWith(prefix)) {
                return super.resolveClass(desc);
            }
        }
        for (String allowed : ALLOWED_EXACT_CLASSES) {
            if (name.equals(allowed)) {
                return super.resolveClass(desc);
            }
        }
        throw new java.io.InvalidClassException(name,
                "blocked by RPC deserialization whitelist (decimal-safety: only io.rowweave.* + JDK)");
    }

}
