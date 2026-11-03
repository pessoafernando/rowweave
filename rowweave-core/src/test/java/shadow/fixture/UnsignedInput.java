package shadow.fixture;

public final class UnsignedInput implements io.rowweave.Source {
    private static final long serialVersionUID = 1L;
    @Override public io.rowweave.DataSchema getSchema() { return new io.rowweave.DataSchema(); }
    @Override public java.util.Iterator<io.rowweave.DataRow> iterator() {
        return java.util.Collections.<io.rowweave.DataRow>emptyList().iterator();
    }
    public String marker() { return "classpath"; }
}
