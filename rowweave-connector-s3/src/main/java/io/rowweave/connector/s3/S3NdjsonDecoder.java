package io.rowweave.connector.s3;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

final class S3NdjsonDecoder implements S3RecordDecoder {
    private final S3BoundedLineReader lines;
    private final StreamSchema schema;
    private final JsonFactory json;
    private final Map<String, Integer> fields;

    S3NdjsonDecoder(InputStream input, StreamSchema schema,
            int maximumRowBytes) {
        this.lines = new S3BoundedLineReader(input, maximumRowBytes);
        this.schema = schema;
        this.fields = new HashMap<>();
        for (int index = 0; index < schema.fieldCount(); index++) {
            fields.put(schema.field(index).fieldId(), index);
        }
        this.json = new JsonFactory();
        this.json.setStreamReadConstraints(StreamReadConstraints.builder()
                .maxDocumentLength(maximumRowBytes)
                .maxNestingDepth(4)
                .maxTokenCount(schema.fieldCount() * 4L + 8L)
                .maxStringLength(maximumRowBytes)
                .maxNameLength(maximumRowBytes)
                .maxNumberLength(maximumRowBytes).build());
    }

    @Override public DataRow next() throws Exception {
        byte[] line = lines.next();
        if (line == null) return null;
        try (JsonParser parser = json.createParser(line)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw rowInvalid();
            }
            DataRow row = new DataRow(schema.fieldCount());
            boolean[] seen = new boolean[schema.fieldCount()];
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.currentToken() != JsonToken.FIELD_NAME) {
                    throw rowInvalid();
                }
                Integer index = fields.get(parser.currentName());
                if (index == null || seen[index]) throw schemaDrift();
                seen[index] = true;
                if (parser.nextToken() == null) throw rowInvalid();
                StreamField field = schema.field(index);
                row.set(index, S3ValueParser.json(parser,
                        field.dataType(), field.nullable()));
            }
            if (parser.nextToken() != null) throw rowInvalid();
            for (boolean present : seen) {
                if (!present) throw schemaDrift();
            }
            return row;
        } catch (IllegalArgumentException invalid) {
            throw invalid;
        } catch (Exception invalid) {
            throw rowInvalid();
        }
    }

    @Override public void close() throws Exception { lines.close(); }

    private static IllegalArgumentException schemaDrift() {
        return new IllegalArgumentException("S3_ROW_SCHEMA_DRIFT");
    }

    private static IllegalArgumentException rowInvalid() {
        return new IllegalArgumentException("S3_ROW_INVALID");
    }
}
