package io.rowweave.pipeline;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Public bounded conformance for durable authority CAS-store adapters. */
public final class VersionedDatasetAuthorityRecordStoreConformance {
    private VersionedDatasetAuthorityRecordStoreConformance() { }

    public static VersionedDatasetAuthorityRecordStoreConformanceReport verify(
            VersionedDatasetAuthorityRecordStoreConformanceFixture fixture) {
        if (fixture == null) {
            throw new IllegalArgumentException("conformance fixture is required");
        }
        VersionedDatasetAuthorityRecordStore first = null;
        VersionedDatasetAuthorityRecordStore second = null;
        try {
            first = requireStore(fixture.openStore());
            second = requireStore(fixture.openStore());
            Set<String> passed = new LinkedHashSet<>();
            String namespace = UUID.randomUUID().toString();

            String lifecycleId = recordId(namespace, "lifecycle");
            check(!first.read(handoff(), lifecycleId).isPresent(),
                    "empty read returned a record");
            passed.add("empty-read");

            byte[] firstValue = bytes(1, 2, 3);
            check(first.compareAndSet(handoff(), lifecycleId, null, firstValue),
                    "initial CAS was rejected");
            VersionedDatasetAuthorityRecord reopened = requireRecord(
                    second.read(handoff(), lifecycleId),
                    "second handle did not observe the durable create");
            check(Arrays.equals(firstValue, reopened.bytes()),
                    "reopened value differs from durable create");
            passed.add("create-and-reopen");

            check(!second.compareAndSet(handoff(), lifecycleId, null, bytes(9)),
                    "duplicate create unexpectedly succeeded");
            check(!second.compareAndSet(handoff(), lifecycleId,
                            recordId(namespace, "wrong-token"), bytes(8)),
                    "CAS accepted a wrong version token");
            byte[] replacement = bytes(4, 5, 6);
            check(second.compareAndSet(handoff(), lifecycleId,
                            reopened.versionToken(), replacement),
                    "CAS rejected the exact durable version token");
            check(Arrays.equals(replacement,
                            requireRecord(first.read(handoff(), lifecycleId),
                                    "replacement disappeared").bytes()),
                    "successful CAS did not replace the record");
            passed.add("cas-conflict");

            check(first.compareAndSet(binding(), lifecycleId, null, bytes(7)),
                    "record kinds were not isolated on create");
            check(Arrays.equals(bytes(7), requireRecord(
                            second.read(binding(), lifecycleId),
                            "binding-kind value disappeared").bytes()),
                    "record-kind isolation returned the wrong value");
            passed.add("record-kind-isolation");

            String ownershipId = recordId(namespace, "ownership");
            byte[] owned = bytes(10, 11);
            check(first.compareAndSet(handoff(), ownershipId, null, owned),
                    "ownership setup failed");
            owned[0] = 99;
            byte[] exposed = requireRecord(first.read(handoff(), ownershipId),
                    "owned record disappeared").bytes();
            check(exposed[0] == 10, "store retained caller-owned input bytes");
            exposed[0] = 88;
            check(requireRecord(first.read(handoff(), ownershipId),
                            "owned record disappeared after read").bytes()[0] == 10,
                    "store exposed mutable durable bytes");
            passed.add("defensive-ownership");

            verifyConcurrentSingleWinner(first, second,
                    recordId(namespace, "concurrent"));
            passed.add("concurrent-single-winner");

            boolean limitRejected = false;
            try {
                first.compareAndSet(handoff(), recordId(namespace, "limit"), null,
                        new byte[VersionedDatasetHandoffLimits.MAX_RECORD_BYTES + 1]);
            } catch (IllegalArgumentException | VersionedDatasetHandoffException expected) {
                limitRejected = true;
            }
            check(limitRejected, "record limit+1 was accepted");
            passed.add("record-limit");

            return new VersionedDatasetAuthorityRecordStoreConformanceReport(passed);
        } catch (VersionedDatasetHandoffException failure) {
            if ("CONFORMANCE_FAILED".equals(failure.code())) throw failure;
            throw failure("adapter raised " + failure.code());
        } catch (Exception failure) {
            throw failure("adapter operation failed");
        } finally {
            close(fixture, second);
            if (first != second) close(fixture, first);
        }
    }

    private static void verifyConcurrentSingleWinner(
            VersionedDatasetAuthorityRecordStore first,
            VersionedDatasetAuthorityRecordStore second, String recordId)
            throws Exception {
        check(first.compareAndSet(handoff(), recordId, null, bytes(20)),
                "concurrent setup failed");
        String token = requireRecord(first.read(handoff(), recordId),
                "concurrent setup disappeared").versionToken();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> left = executor.submit(() -> {
                start.await();
                return first.compareAndSet(handoff(), recordId, token, bytes(21));
            });
            Future<Boolean> right = executor.submit(() -> {
                start.await();
                return second.compareAndSet(handoff(), recordId, token, bytes(22));
            });
            start.countDown();
            boolean leftWon = left.get();
            boolean rightWon = right.get();
            check(leftWon ^ rightWon, "concurrent CAS did not choose one winner");
            byte[] actual = requireRecord(first.read(handoff(), recordId),
                    "concurrent winner disappeared").bytes();
            check(Arrays.equals(actual, leftWon ? bytes(21) : bytes(22)),
                    "durable value does not match the acknowledged CAS winner");
        } finally {
            executor.shutdownNow();
        }
    }

    private static VersionedDatasetAuthorityRecordStore requireStore(
            VersionedDatasetAuthorityRecordStore store) {
        if (store == null) throw failure("fixture returned a null store");
        return store;
    }

    private static VersionedDatasetAuthorityRecord requireRecord(
            Optional<VersionedDatasetAuthorityRecord> record, String detail) {
        if (!record.isPresent()) throw failure(detail);
        return record.get();
    }

    private static String recordId(String namespace, String scenario) {
        return VersionedDatasetHandoffCodec.sha256Hex(
                (namespace + ':' + scenario).getBytes(StandardCharsets.UTF_8));
    }

    private static VersionedDatasetAuthorityRecordStore.RecordKind handoff() {
        return VersionedDatasetAuthorityRecordStore.RecordKind.HANDOFF;
    }

    private static VersionedDatasetAuthorityRecordStore.RecordKind binding() {
        return VersionedDatasetAuthorityRecordStore.RecordKind.CONSUMER_BINDING;
    }

    private static byte[] bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int index = 0; index < values.length; index++) {
            result[index] = (byte) values[index];
        }
        return result;
    }

    private static void check(boolean condition, String detail) {
        if (!condition) throw failure(detail);
    }

    private static VersionedDatasetHandoffException failure(String detail) {
        return new VersionedDatasetHandoffException("CONFORMANCE_FAILED", detail);
    }

    private static void close(
            VersionedDatasetAuthorityRecordStoreConformanceFixture fixture,
            VersionedDatasetAuthorityRecordStore store) {
        if (store == null) return;
        try {
            fixture.closeStore(store);
        } catch (Exception ignored) {
            // The operation result remains authoritative; close is best effort.
        }
    }
}
