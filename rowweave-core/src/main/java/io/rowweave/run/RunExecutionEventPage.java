package io.rowweave.run;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** One public page of Run events, at most 256 entries. */
public final class RunExecutionEventPage {
    public static final int MAX_EVENTS = 256;

    public enum State {
        FOUND,
        NOT_FOUND,
        RETIRING
    }

    private final State state;
    private final long afterSequence;
    private final long nextAfterSequence;
    private final boolean hasMore;
    private final List<RunExecutionEvent> events;

    private RunExecutionEventPage(State state, long afterSequence,
                                  List<RunExecutionEvent> events, boolean hasMore) {
        if (state == null) throw new IllegalArgumentException("page state is required");
        if (afterSequence < 0L) throw new IllegalArgumentException("afterSequence must be non-negative");
        if (events == null) throw new IllegalArgumentException("events are required");
        if (events.size() > MAX_EVENTS) throw new IllegalArgumentException("event page exceeds limit");
        if (state != State.FOUND && (!events.isEmpty() || hasMore)) {
            throw new IllegalArgumentException("non-found event page must be empty");
        }
        if (events.isEmpty() && hasMore) {
            throw new IllegalArgumentException("empty event page cannot have more entries");
        }
        ArrayList<RunExecutionEvent> copy = new ArrayList<>(events.size());
        long previous = afterSequence;
        for (RunExecutionEvent event : events) {
            if (event == null || event.sequence() <= previous) {
                throw new IllegalArgumentException("event sequences must be strictly increasing after cursor");
            }
            copy.add(event);
            previous = event.sequence();
        }
        this.state = state;
        this.afterSequence = afterSequence;
        this.nextAfterSequence = previous;
        this.hasMore = hasMore;
        this.events = Collections.unmodifiableList(copy);
    }

    public static RunExecutionEventPage found(long afterSequence,
                                              List<RunExecutionEvent> events,
                                              boolean hasMore) {
        return new RunExecutionEventPage(State.FOUND, afterSequence, events, hasMore);
    }

    public static RunExecutionEventPage notFound(long afterSequence) {
        return new RunExecutionEventPage(
                State.NOT_FOUND, afterSequence, Collections.emptyList(), false);
    }

    public static RunExecutionEventPage retiring(long afterSequence) {
        return new RunExecutionEventPage(
                State.RETIRING, afterSequence, Collections.emptyList(), false);
    }

    public State state() { return state; }
    public long afterSequence() { return afterSequence; }
    public long nextAfterSequence() { return nextAfterSequence; }
    public boolean hasMore() { return hasMore; }
    public List<RunExecutionEvent> events() { return events; }

    @Override
    public boolean equals(Object value) {
        if (this == value) return true;
        if (!(value instanceof RunExecutionEventPage)) return false;
        RunExecutionEventPage other = (RunExecutionEventPage) value;
        return afterSequence == other.afterSequence
                && nextAfterSequence == other.nextAfterSequence
                && hasMore == other.hasMore
                && state == other.state
                && events.equals(other.events);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, afterSequence, nextAfterSequence, hasMore, events);
    }

    @Override
    public String toString() {
        return "RunExecutionEventPage{" +
                "state=" + state +
                ", afterSequence=" + afterSequence +
                ", nextAfterSequence=" + nextAfterSequence +
                ", hasMore=" + hasMore +
                ", eventCount=" + events.size() +
                '}';
    }
}
