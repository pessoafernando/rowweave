package io.rowweave.stream.contract;

/** Closed Stream provider binding marker. */
public interface StreamNodeBinding {
    StreamOperatorCode.BindingKind kind();
}
