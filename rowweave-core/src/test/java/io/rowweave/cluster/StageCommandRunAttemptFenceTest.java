package io.rowweave.cluster;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;

import java.io.ObjectStreamClass;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StageCommandRunAttemptFenceTest {

    @Test
    void bindsExactlyOneAttemptFenceAndUsesFailClosedWireUid() {
        DataSchema meta = new DataSchema(new Field("id", DataType.LongType));
        StageCommand command = StageCommand.consumer(meta, 64, new CollectingSink(meta));
        RunAttemptFence fence = new RunAttemptFence("run", "attempt-1", 3L);
        assertSame(command, command.withRunAttemptFence(fence));
        assertEquals(fence, command.runAttemptFence());
        assertSame(command, command.withRunAttemptFence(
                new RunAttemptFence("run", "attempt-1", 3L)));
        assertThrows(IllegalStateException.class, () -> command.withRunAttemptFence(
                new RunAttemptFence("run", "attempt-2", 4L)));
        assertEquals(10L, ObjectStreamClass.lookup(StageCommand.class).getSerialVersionUID());
    }
}
