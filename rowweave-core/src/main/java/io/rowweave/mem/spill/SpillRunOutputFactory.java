package io.rowweave.mem.spill;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/** Test seam for deterministic write/flush failures without filling the host filesystem. */
@FunctionalInterface
interface SpillRunOutputFactory {
    OutputStream open(File file) throws IOException;
}
