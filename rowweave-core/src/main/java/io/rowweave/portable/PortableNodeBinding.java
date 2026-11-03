package io.rowweave.portable;

/** Marker for the closed set of portable source, function and sink bindings. */
public interface PortableNodeBinding {
    PortableOperatorCode.BindingKind kind();
}
