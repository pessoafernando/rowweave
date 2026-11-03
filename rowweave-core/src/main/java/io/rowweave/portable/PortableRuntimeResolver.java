package io.rowweave.portable;

import io.rowweave.Source;
import io.rowweave.Sink;
import io.rowweave.DataSchema;

import java.io.Serializable;
import java.util.List;

/** Worker-local runtime object resolver called only after whole-plan metadata preflight. */
public interface PortableRuntimeResolver {
    List<Source> resolveSource(PortableSourceBinding binding);
    Serializable resolveFunction(FunctionDeploymentReference reference);
    Sink resolveSink(PortableSinkBinding binding, DataSchema inputSchema);
}
