package io.rowweave.cluster;

import io.rowweave.sched.net.ShuffleEndpoint;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BroadcastGroupBlockingTest {

    private static ShuffleTarget socket(int port) {
        return ShuffleTarget.socket(new ShuffleEndpoint("127.0.0.1", port));
    }

    @Test
    void threeArgConstructorDefaultsPipelined() {
        StageCommand.BroadcastGroup g = new StageCommand.BroadcastGroup(
                Arrays.asList(socket(9001)), new String[]{"k"}, false);
        assertFalse(g.blocking, "the 3-argument constructor defaults to PIPELINED (blocking=false)");
    }

    @Test
    void fourArgConstructorSetsBlocking() {
        StageCommand.BroadcastGroup g = new StageCommand.BroadcastGroup(
                Arrays.asList(socket(9002)), new String[]{"k"}, true, true);
        assertTrue(g.blocking, "the 4-argument constructor sets blocking=true");
        assertTrue(g.normalized);
    }

    @Test
    void blockingFlagSurvivesSerialization() throws Exception {
        List<StageCommand.BroadcastGroup> groups = Arrays.asList(
                new StageCommand.BroadcastGroup(Arrays.asList(socket(9003)), new String[]{"lk"}, true, true),
                new StageCommand.BroadcastGroup(Arrays.asList(socket(9004)), new String[]{"rk"}, true, true));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(groups);
        }
        @SuppressWarnings("unchecked")
        List<StageCommand.BroadcastGroup> back;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            back = (List<StageCommand.BroadcastGroup>) ois.readObject();
        }
        assertEquals(2, back.size());
        assertTrue(back.get(0).blocking, "group 0 keeps blocking through serialization");
        assertTrue(back.get(1).blocking, "group 1 keeps blocking through serialization");
        assertArrayEquals(new String[]{"lk"}, back.get(0).downKeys, "downKeys survives serialization");
        assertArrayEquals(new String[]{"rk"}, back.get(1).downKeys);
    }
}

