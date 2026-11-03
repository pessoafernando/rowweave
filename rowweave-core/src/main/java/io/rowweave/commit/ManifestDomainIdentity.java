package io.rowweave.commit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

/** Canonical domain identity shared inside the manifest file provider. */
final class ManifestDomainIdentity {

    private ManifestDomainIdentity() {
    }

    static Path canonicalPath(Path path) {
        Path normalized = path.toAbsolutePath().normalize();
        try {
            Path existing = normalized;
            while (existing != null && !Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
                existing = existing.getParent();
            }
            if (existing == null) {
                return normalized;
            }
            return existing.toRealPath().resolve(existing.relativize(normalized)).normalize();
        } catch (IOException | SecurityException ignored) {
            return normalized;
        }
    }
}
