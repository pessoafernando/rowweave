package io.rowweave;

/** The execution-side reader for ManifestFileSource; a mid-read cancel still closes the file handles of the generation already selected. */
public final class ManifestFileInputExecutor extends SourceExecutor<ManifestFileSource> {

    private FileSource.Reader reader;
    private boolean closed;

    public ManifestFileInputExecutor(ManifestFileSource input) {
        super(input, input.getSchema());
    }

    @Override
    public void open() {
        if (closed) {
            throw new IllegalStateException("ManifestFileInputExecutor is closed");
        }
        if (reader == null) {
            reader = input.openReader();
        }
    }

    @Override
    public boolean hasNext() {
        return reader != null && reader.hasNext();
    }

    @Override
    public DataRow next() {
        if (reader == null) {
            throw new IllegalStateException("ManifestFileInputExecutor is not open");
        }
        return reader.next();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        FileSource.Reader current = reader;
        reader = null;
        if (current != null) {
            current.close();
        }
    }
}
