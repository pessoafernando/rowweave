package io.rowweave;

/** The execution-side reader for FileSource; open/read/close are held by the existing RowStream lifecycle. */
public final class FileInputExecutor extends SourceExecutor<FileSource> {

    private FileSource.Reader reader;
    private boolean closed;

    public FileInputExecutor(FileSource input) {
        super(input, input.getSchema());
    }

    @Override
    public void open() {
        if (closed) {
            throw new IllegalStateException("FileInputExecutor is closed");
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
            throw new IllegalStateException("FileInputExecutor is not open");
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
