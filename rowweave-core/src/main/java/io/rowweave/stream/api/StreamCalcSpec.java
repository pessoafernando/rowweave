package io.rowweave.stream.api;

import io.rowweave.portable.PortableDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;

/** Immutable public declaration for one typed filter/project CALC operation. */
public final class StreamCalcSpec {
    public static final class Projection {
        private final String outputFieldId;
        private final StreamExpression expression;
        private final boolean decimalOutputBoundary;

        private Projection(String outputFieldId, StreamExpression expression,
                boolean decimalOutputBoundary) {
            if (outputFieldId == null || outputFieldId.isEmpty()) {
                throw new IllegalArgumentException(
                        "CALC output field ID is required");
            }
            if (expression == null) {
                throw new IllegalArgumentException(
                        "CALC projection expression is required");
            }
            if (decimalOutputBoundary && expression.dataType()
                    != PortableDataType.BIG_DECIMAL) {
                throw new IllegalArgumentException(
                        "decimal output boundary requires BIG_DECIMAL");
            }
            this.outputFieldId = outputFieldId;
            this.expression = expression;
            this.decimalOutputBoundary = decimalOutputBoundary;
        }

        public String outputFieldId() { return outputFieldId; }
        public StreamExpression expression() { return expression; }
        public boolean decimalOutputBoundary() {
            return decimalOutputBoundary;
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof Projection)) return false;
            Projection that = (Projection) other;
            return decimalOutputBoundary == that.decimalOutputBoundary
                    && outputFieldId.equals(that.outputFieldId)
                    && expression.equals(that.expression);
        }

        @Override public int hashCode() {
            return Objects.hash(outputFieldId, expression,
                    decimalOutputBoundary);
        }
    }

    private final StreamExpression predicate;
    private final List<Projection> projections;

    private StreamCalcSpec(StreamExpression predicate,
            List<Projection> projections) {
        if (predicate != null
                && predicate.dataType() != PortableDataType.BOOLEAN) {
            throw new IllegalArgumentException(
                    "CALC predicate must be BOOLEAN");
        }
        if (projections == null || projections.isEmpty()
                || projections.size() > 1024) {
            throw new IllegalArgumentException(
                    "CALC requires 1..1024 projections");
        }
        Set<String> identities = new HashSet<>();
        for (Projection projection : projections) {
            if (projection == null
                    || !identities.add(projection.outputFieldId())) {
                throw new IllegalArgumentException(
                        "CALC output field IDs must be unique");
            }
        }
        this.predicate = predicate;
        this.projections = Collections.unmodifiableList(
                new ArrayList<>(projections));
    }

    public static StreamCalcSpec of(StreamExpression predicate,
            List<Projection> projections) {
        return new StreamCalcSpec(predicate, projections);
    }

    public static Projection projection(
            String outputFieldId, StreamExpression expression) {
        return new Projection(outputFieldId, expression, false);
    }

    public static Projection decimalProjection(
            String outputFieldId, StreamExpression expression) {
        return new Projection(outputFieldId, expression, true);
    }

    public StreamExpression predicate() { return predicate; }
    public List<Projection> projections() {
        return projections;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamCalcSpec)) return false;
        StreamCalcSpec that = (StreamCalcSpec) other;
        return Objects.equals(predicate, that.predicate)
                && projections.equals(that.projections);
    }

    @Override public int hashCode() {
        return Objects.hash(predicate, projections);
    }
}
