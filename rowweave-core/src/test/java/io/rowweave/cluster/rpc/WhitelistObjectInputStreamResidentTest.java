package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WhitelistObjectInputStreamResidentTest {

    @Test
    void filterAddsReferenceAndConservativeArrayChargeToRequestLease() throws Exception {
        byte[] encoded = serialize(new byte[32]);
        ControlResidentBudget budget = ControlResidentBudget.forTesting(16 * 1024L);
        ControlResidentLease lease = budget.openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        long baseline = 3L * encoded.length;
        lease.reserve(owner, baseline);

        try (WhitelistObjectInputStream in = new WhitelistObjectInputStream(
                new ByteArrayInputStream(encoded), lease, owner)) {
            assertTrue(in.readObject() instanceof byte[]);
        }
        assertTrue(lease.reservedBytes() >= baseline + 128L + align8(16L + 32L * 8L));
        assertTrue(lease.release(owner));
        assertEquals(0L, budget.snapshot().usedBytes());
    }

    @Test
    void filterBudgetFailureIsObservableWithStableCodeAndReleasesAsOneLease() throws Exception {
        byte[] encoded = serialize(new byte[1024]);
        long baseline = 3L * encoded.length;
        ControlResidentBudget budget = ControlResidentBudget.forTesting(baseline + 256L);
        ControlResidentLease lease = budget.openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        lease.reserve(owner, baseline);

        try (WhitelistObjectInputStream in = new WhitelistObjectInputStream(
                new ByteArrayInputStream(encoded), lease, owner)) {
            assertThrows(Exception.class, in::readObject);
            assertNotNull(in.residentFailure());
            assertEquals(ControlResidentBudgetExceededException.CODE,
                    in.residentFailure().code());
        }
        assertTrue(lease.release(owner));
        assertEquals(0L, budget.snapshot().usedBytes());
    }

    private static byte[] serialize(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        return bytes.toByteArray();
    }

    private static long align8(long value) {
        return (value + 7L) & ~7L;
    }
}

