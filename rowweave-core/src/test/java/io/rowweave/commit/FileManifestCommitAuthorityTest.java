package io.rowweave.commit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** The same-host cluster authority combines a durable generation with a domain lease. */
class FileManifestCommitAuthorityTest {

    @Test
    void leaseFencesConcurrentWriterAndReservationIsAttemptIdempotent(@TempDir Path domain) {
        ManifestCommitAuthority authority = new FileManifestCommitAuthority();
        ManifestCommitAuthority.Lease first = authority.tryAcquire(domain, "attempt-a");
        assertNotNull(first);
        assertNull(authority.tryAcquire(domain, "attempt-b"));

        long generation = authority.reserve(domain, "attempt-a");
        assertEquals(generation, authority.reserve(domain, "attempt-a"));
        assertEquals(generation, authority.find(domain, "attempt-a"));
        first.close();

        ManifestCommitAuthority.Lease second = authority.tryAcquire(domain, "attempt-b");
        assertNotNull(second);
        second.close();
    }
}
