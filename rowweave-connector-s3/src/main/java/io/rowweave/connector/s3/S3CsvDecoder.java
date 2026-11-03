package io.rowweave.connector.s3;

import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;

import java.io.InputStream;
import java.util.List;

final class S3CsvDecoder implements S3RecordDecoder {
    private final S3CsvRecordReader records;
    private final StreamSchema schema;
    private final String nullLiteral;

    S3CsvDecoder(InputStream input, StreamSchema schema,
            int maximumRowBytes, String nullLiteral) throws Exception {
        this.records = new S3CsvRecordReader(input, maximumRowBytes);
        this.schema = schema;
        this.nullLiteral = nullLiteral;
        List<String> header = records.next();
        if (header == null || header.size() != schema.fieldCount()) {
            throw schemaDrift();
        }
        for (int index = 0; index < header.size(); index++) {
            if (!schema.field(index).fieldId().equals(header.get(index))) {
                throw schemaDrift();
            }
        }
    }

    @Override public DataRow next() throws Exception {
        List<String> values = records.next();
        if (values == null) return null;
        if (values.size() != schema.fieldCount()) throw schemaDrift();
        DataRow row = new DataRow(schema.fieldCount());
        for (int index = 0; index < values.size(); index++) {
            StreamField field = schema.field(index);
            row.set(index, S3ValueParser.text(values.get(index),
                    field.dataType(), field.nullable(), nullLiteral));
        }
        return row;
    }

    @Override public void close() throws Exception { records.close(); }

    private static IllegalArgumentException schemaDrift() {
        return new IllegalArgumentException("S3_ROW_SCHEMA_DRIFT");
    }
}
