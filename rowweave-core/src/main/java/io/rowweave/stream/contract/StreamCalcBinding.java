package io.rowweave.stream.contract;

import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Typed predicate and ordered projection binding for the portable CALC node. */
public final class StreamCalcBinding implements StreamNodeBinding {
    public static final int MAX_PROJECTIONS = PortableRowSchema.MAX_FIELDS;

    public static final class Projection {
        private final String outputFieldId;
        private final StreamExpression expression;
        private final boolean decimalOutputBoundary;

        private Projection(String outputFieldId, StreamExpression expression,
                boolean decimalOutputBoundary) {
            this.outputFieldId = StreamContractSupport.text(outputFieldId,
                    "nodes.calc.projections.outputFieldId", 256);
            if (expression == null) {
                throw failure("INVALID_CALC_PROJECTION",
                        "projection expression is required");
            }
            if (decimalOutputBoundary
                    && expression.dataType() != PortableDataType.BIG_DECIMAL) {
                throw failure("DECIMAL_BOUNDARY_TYPE_MISMATCH",
                        "decimal output boundary requires BIG_DECIMAL");
            }
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

    public StreamCalcBinding(StreamExpression predicate,
            List<Projection> projections) {
        if (predicate != null
                && predicate.dataType() != PortableDataType.BOOLEAN) {
            throw failure("EXPRESSION_TYPE_MISMATCH",
                    "CALC predicate must be BOOLEAN");
        }
        if (projections == null || projections.isEmpty()
                || projections.size() > MAX_PROJECTIONS) {
            throw failure("INVALID_CALC_PROJECTION",
                    "CALC requires a bounded non-empty projection list");
        }
        Set<String> identities = new HashSet<>();
        List<Projection> copy = new ArrayList<>(projections.size());
        for (Projection projection : projections) {
            if (projection == null
                    || !identities.add(projection.outputFieldId())) {
                throw failure("DUPLICATE_PROJECTION",
                        "CALC output field IDs must be unique");
            }
            copy.add(projection);
        }
        this.predicate = predicate;
        this.projections = Collections.unmodifiableList(copy);
    }

    public static Projection projection(String outputFieldId,
            StreamExpression expression) {
        return new Projection(outputFieldId, expression, false);
    }

    public static Projection decimalProjection(String outputFieldId,
            StreamExpression expression) {
        return new Projection(outputFieldId, expression, true);
    }

    @Override public StreamOperatorCode.BindingKind kind() {
        return StreamOperatorCode.BindingKind.CALC;
    }

    public StreamExpression predicate() { return predicate; }
    public List<Projection> projections() { return projections; }

    public void validateOutputSchema(PortableRowSchema schema) {
        if (schema == null || schema.fieldCount() != projections.size()) {
            schemaFailure();
        }
        for (int index = 0; index < projections.size(); index++) {
            Projection projection = projections.get(index);
            PortableField field = schema.field(index);
            if (!projection.outputFieldId().equals(field.fieldId())
                    || projection.expression().dataType() != field.dataType()
                    || projection.expression().nullable() != field.nullable()) {
                schemaFailure();
            }
        }
    }

    public void validateInputSchema(PortableRowSchema schema) {
        if (schema == null) inputFailure();
        Map<String, PortableField> fields = new HashMap<>();
        for (PortableField field : schema.fields()) fields.put(field.fieldId(), field);
        if (predicate != null) validateInputs(predicate, fields);
        for (Projection projection : projections) {
            validateInputs(projection.expression(), fields);
        }
    }

    public static void validateExpressionInput(StreamExpression expression,
            PortableRowSchema schema) {
        if (expression == null || schema == null) inputFailure();
        Map<String, PortableField> fields = new HashMap<>();
        for (PortableField field : schema.fields()) fields.put(field.fieldId(), field);
        validateInputs(expression, fields);
    }

    private static void validateInputs(StreamExpression expression,
            Map<String, PortableField> fields) {
        if (expression.kind() == StreamExpression.Kind.INPUT) {
            PortableField field = fields.get(expression.inputFieldId());
            if (field == null || field.dataType() != expression.dataType()
                    || field.nullable() != expression.nullable()) inputFailure();
            return;
        }
        for (StreamExpression argument : expression.arguments()) {
            validateInputs(argument, fields);
        }
    }

    private static void schemaFailure() {
        throw failure("CALC_SCHEMA_MISMATCH",
                "CALC projection contract differs from its output schema");
    }

    private static void inputFailure() {
        throw failure("INVALID_EXPRESSION",
                "CALC expression input differs from its input schema");
    }

    private static StreamContractException failure(String code,
            String message) {
        return StreamContractSupport.failure(code, "nodes.calc", message);
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamCalcBinding)) return false;
        StreamCalcBinding that = (StreamCalcBinding) other;
        return Objects.equals(predicate, that.predicate)
                && projections.equals(that.projections);
    }

    @Override public int hashCode() {
        return Objects.hash(predicate, projections);
    }
}
