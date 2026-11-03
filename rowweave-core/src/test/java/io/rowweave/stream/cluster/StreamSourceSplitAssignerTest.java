package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** GDF3: every fixed source split has one deterministic subtask owner. */
class StreamSourceSplitAssignerTest {
    @Test
    void assignsAllSplitsExactlyOnceAcrossSourceSubtasks() {
        StreamSourceBinding binding = new StreamSourceBinding(
                "provider", repeat('a'), digest(new byte[0]), new byte[0],
                Arrays.asList(split("1"), split("2"), split("3"), split("4"),
                        split("5")), StreamSourceBinding.ReplayGuarantee
                        .EXACT_ORDERED);
        List<List<StreamSourceSplit>> assigned =
                StreamSourceSplitAssigner.assign(binding, 3);

        assertEquals(3, assigned.size());
        assertEquals(Arrays.asList("1", "4"), ids(assigned.get(0)));
        assertEquals(Arrays.asList("2", "5"), ids(assigned.get(1)));
        assertEquals(Arrays.asList("3"), ids(assigned.get(2)));
        Set<StreamSourceSplit> flattened = new HashSet<>();
        for (List<StreamSourceSplit> subtask : assigned) {
            flattened.addAll(subtask);
        }
        assertEquals(binding.fixedSplits().size(), flattened.size());
    }

    private static StreamSourceSplit split(String id) {
        return new StreamSourceSplit(repeat(id.charAt(0)), "position-v1",
                id.getBytes());
    }

    private static List<String> ids(List<StreamSourceSplit> splits) {
        java.util.ArrayList<String> ids = new java.util.ArrayList<>();
        for (StreamSourceSplit split : splits) {
            ids.add(new String(split.descriptorFrame()));
        }
        return ids;
    }

    private static String digest(byte[] value) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(value);
            StringBuilder text = new StringBuilder(64);
            for (byte item : digest) {
                text.append(String.format("%02x", item & 0xff));
            }
            return text.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String repeat(char value) {
        char[] text = new char[64];
        java.util.Arrays.fill(text, value);
        return new String(text);
    }
}
