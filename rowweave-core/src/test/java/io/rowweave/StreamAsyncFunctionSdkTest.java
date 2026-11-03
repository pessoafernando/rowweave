package io.rowweave;

import io.rowweave.stream.api.StreamOutputTag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** public async callbacks exchange immutable requests. */
class StreamAsyncFunctionSdkTest {

    @Test
    void requestIdentityAttemptAndFrameAreImmutable() {
        byte[] frame = bytes("customer-7");
        StreamAsyncRequest request = StreamAsyncRequest.of(
                "lookup-0000000000000007", 2, frame);
        frame[0] = 0;

        assertEquals("lookup-0000000000000007",
                request.operationIdentity());
        assertEquals(2, request.attempt());
        assertArrayEquals(bytes("customer-7"), request.requestFrame());
        byte[] exposed = request.requestFrame();
        exposed[0] = 0;
        assertArrayEquals(bytes("customer-7"), request.requestFrame());
    }

    @Test
    void functionSeparatesRequestLookupAndMailboxCompletion() throws Exception {
        TestFunction function = new TestFunction();
        DataRow input = new DataRow(1);
        input.set(0, 7L);
        byte[] requestFrame = function.createRequest(input);
        StreamAsyncRequest request = StreamAsyncRequest.of(
                "lookup-0000000000000007", 1, requestFrame);
        byte[] response = function.invoke(request);
        Rows output = new Rows();

        function.complete(input, response, output);

        assertEquals(1, output.rows.size());
        assertEquals(7L, output.rows.get(0).getLong(0));
        assertEquals("customer-7", output.rows.get(0).getString(1));
    }

    @Test
    void malformedRequestsFailClosed() {
        assertThrows(IllegalArgumentException.class,
                () -> StreamAsyncRequest.of(null, 1, bytes("request")));
        assertThrows(IllegalArgumentException.class,
                () -> StreamAsyncRequest.of("operation", 0, bytes("request")));
        assertThrows(IllegalArgumentException.class,
                () -> StreamAsyncRequest.of("operation", 1, new byte[0]));
    }

    private static final class TestFunction implements StreamAsyncFunction {
        @Override public byte[] createRequest(DataRow input) {
            return bytes(Long.toString(input.getLong(0)));
        }

        @Override public byte[] invoke(StreamAsyncRequest request) {
            return bytes("customer-" + new String(request.requestFrame(),
                    StandardCharsets.US_ASCII));
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow row = new DataRow(2);
            row.set(0, input.getLong(0));
            row.set(1, new String(responseFrame,
                    StandardCharsets.US_ASCII));
            output.collect(row);
        }
    }

    private static final class Rows implements StreamCollector {
        private final List<DataRow> rows = new ArrayList<>();
        @Override public void collect(DataRow row) { rows.add(row); }
        @Override public void collect(StreamOutputTag tag, DataRow row) {
            throw new AssertionError("async wait has no tagged output");
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
