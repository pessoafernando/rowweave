package io.rowweave.mem.spill;

import java.io.IOException;
import java.nio.file.Path;

/** Injectable filesystem-space probe; production reads the FileStore for the owned base. */
@FunctionalInterface
interface SpillUsableSpaceProbe {
    long usableBytes(Path ownedBase) throws IOException;
}
