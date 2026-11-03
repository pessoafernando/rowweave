package io.rowweave.deployment;

import java.nio.file.attribute.BasicFileAttributes;

/** File identity captured on both sides of the one-time verified image read. */
final class DeploymentSourceIdentity {
    final Object fileKey;
    final long size;
    final long modifiedMillis;

    private DeploymentSourceIdentity(Object fileKey, long size, long modifiedMillis) {
        this.fileKey = fileKey;
        this.size = size;
        this.modifiedMillis = modifiedMillis;
    }

    static DeploymentSourceIdentity of(BasicFileAttributes attributes) {
        return new DeploymentSourceIdentity(attributes.fileKey(), attributes.size(),
                attributes.lastModifiedTime().toMillis());
    }

    boolean matches(BasicFileAttributes attributes) {
        Object candidateKey = attributes.fileKey();
        return attributes.isRegularFile()
                && size == attributes.size()
                && modifiedMillis == attributes.lastModifiedTime().toMillis()
                && (fileKey == null ? candidateKey == null : fileKey.equals(candidateKey));
    }
}
