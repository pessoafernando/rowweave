---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Schema conversion and row validation

Two operators change or test a row's typed shape: `SchemaConversion` maps an
input header onto a different output header, and `RowValidation` asserts bounds
per row without changing anything. Both are describable, so the same object can
be rebuilt from a plan on a Worker, and both process exactly one row at a time.
For the value rules a conversion follows, see
[decimal and null semantics](decimal-null-semantics.md).

## How they enter the API

| Entry point | Declared |
|---|---|
| `DataSet.convertSchema(SchemaConversion)` | `io/rowweave/DataSet.java:335` |
| `DataSet.validateRows(RowValidation)` | `io/rowweave/DataSet.java:357` |
| `DataSet.validateRowsWithSourcePosition(...)` | `io/rowweave/DataSet.java:379` |

Both operators are entered with a constructed object rather than a schema plus a
flag set, which is what keeps the whole contract in one describable value.

## Schema conversion

[`SchemaConversion`](../../../rowweave-core/src/main/java/io/rowweave/ops/SchemaConversion.java)
is declared at `io/rowweave/ops/SchemaConversion.java:25` and is bounded by two
constants: `MAX_FIELDS` at `:27` and `MAX_UTF8_BYTES` at `:28`.

`Cast` is a closed four-value enum at `io/rowweave/ops/SchemaConversion.java:30`:

| Value | Converts |
|---|---|
| `IDENTITY` | no change, same physical type |
| `INTEGER_TO_LONG` | widening integer |
| `INTEGER_TO_DECIMAL` | integer to exact decimal |
| `LONG_TO_DECIMAL` | long to exact decimal |

Only these widen. Nothing narrows, and no cast reinterprets bytes, which is why
a conversion never needs a rounding rule. The dispatch is in the private
`convert` method that begins at `io/rowweave/ops/SchemaConversion.java:271`.

Build one through `builder(DataSchema)` at
`io/rowweave/ops/SchemaConversion.java:83` and the three field-declaration
shapes on the builder: `required` at `:360` and `:364`, `nullable` at `:368` and
`:372`, and `defaulted` at `:376`. A defaulted target carries no source column,
which the description encodes as the sentinel source index visible at `:382`,
and its value is encoded as bounded text at `:386`. `build()` rejects an empty
target list at `io/rowweave/ops/SchemaConversion.java:391` and an over-wide
conversion at `:394`.

Conversion runs one row at a time through `apply(RowStream, RowSink)` at
`io/rowweave/ops/SchemaConversion.java:224`, and the single-row form
`project(DataRow)` at `:240` builds a fresh output row rather than mutating the
input, so a failing field never exposes a half-built row.

## Row validation

[`RowValidation`](../../../rowweave-core/src/main/java/io/rowweave/ops/RowValidation.java)
is declared at `io/rowweave/ops/RowValidation.java:27`. Its ceilings are
`MAX_RULES` at `:29`, `MAX_FIELDS` at `:30`, `MAX_RULE_ID_BYTES` at `:31`,
`MAX_BOUND_BYTES` at `:32`, `MAX_SCHEMA_TEXT_BYTES` at `:33` and
`MAX_STRING_UTF8_BYTES` at `:34`.

`Kind` is a closed five-value enum at `io/rowweave/ops/RowValidation.java:36`:

| Value | Asserts |
|---|---|
| `NOT_NULL` | the field holds a value |
| `INTEGER_RANGE_CLOSED` | an integer within inclusive bounds |
| `LONG_RANGE_CLOSED` | a long within inclusive bounds |
| `DECIMAL_RANGE_CLOSED` | a decimal within inclusive bounds |
| `STRING_UTF8_BYTES_RANGE_CLOSED` | a string whose UTF-8 byte length is within inclusive bounds |

The last kind measures bytes rather than characters, so a multibyte value is
judged by the width it actually costs.

Build rules through `builder(DataSchema)` at
`io/rowweave/ops/RowValidation.java:174`: `notNull` at `:669` and `:673`,
`integerRange` at `:677` and `:681`, `longRange` at `:688` and `:692`,
`decimalRange` at `:699` and `:704`, and `stringUtf8BytesRange` at `:713` and
`:719`. Field access is by unique name or by index, so the same rule can be
written either way.

`validate(DataRow)` at `io/rowweave/ops/RowValidation.java:348` is the fail-fast
form and never calls `DataRow.toString`, so a rejected row cannot leak its
content into a log. `apply(RowStream, RowSink)` at
`io/rowweave/ops/RowValidation.java:302` is the streaming form and emits the
event through unchanged on success, including end-of-stream, error and cancel
events.

## Structured evaluation and classification

`validate` throws, but the shared engine behind it returns a structured result:
the package-private `Evaluation` at `io/rowweave/ops/RowValidation.java:45`
carries a status from the closed set at `:46`, an error code, the offending rule
id, the field index and a bounded detail. The three failure constructors are
`width` at `:76`, `type` at `:81` and `rule` at `:87`. This is what lets a
classifier route bad rows instead of aborting the job.

[`TypedRowClassification`](../../../rowweave-core/src/main/java/io/rowweave/ops/TypedRowClassification.java)
at `io/rowweave/ops/TypedRowClassification.java:28` turns that result into two
row streams. `of` is declared at `:71`, the carrier header that tags each row
with its route at `carrierSchema()` `:86`, the header a rejected row is annotated
with at `rejectedSchema()` `:90`, and the three pipeline stages at `:94`, `:98`
and `:102`. The route contract is the constant at `:31` with the two tags at `:32`
and `:33`.

`DataSet` exposes the pair through
[`DataRowClassification`](../../../rowweave-core/src/main/java/io/rowweave/DataRowClassification.java):
`validRows()` at `io/rowweave/DataRowClassification.java:21`, `rejectedRows()` at
`:25`, and `writeToManifest(TypedQualityManifest)` at `:34`, which publishes both
halves plus a summary as one atomic group. The object-storage variant of the same
carrier shape is
[`ObjectStorageQualityRoute`](../../../rowweave-core/src/main/java/io/rowweave/ops/ObjectStorageQualityRoute.java)
at `io/rowweave/ops/ObjectStorageQualityRoute.java:23`, with `routeStage` at `:36`
and `rejectedMeta` at `:42`.

Reasons are aggregated by
[`QualityReasonKey`](../../../rowweave-core/src/main/java/io/rowweave/QualityReasonKey.java)
(`io/rowweave/QualityReasonKey.java:7`, factory `of` at `:61`), whose error-code
and rule-id shapes are validated by the patterns at `:8` and `:9`. Up to a
bounded number of individual bad rows are kept as evidence through
[`QualityExemplar`](../../../rowweave-core/src/main/java/io/rowweave/QualityExemplar.java)
(`io/rowweave/QualityExemplar.java:15`, factory `of` at `:51`), which identifies
a row by split digest and ordinal rather than by content. The publication target
is
[`TypedQualityManifest`](../../../rowweave-core/src/main/java/io/rowweave/TypedQualityManifest.java)
(`io/rowweave/TypedQualityManifest.java:13`, `of` at `:44`, summary header at
`:82`), and `withQualityGate(QualityGatePolicy)` at `:69` is what makes the
publication conditional on the measured rejection rate.

## Source-position validation

`validateRowsWithSourcePosition` binds validation to the exact splits a source
declared, so a row is judged together with where it came from. The runtime form
is `applyWithSourcePosition` at `io/rowweave/ops/RowValidation.java:322`, which
requires a position on every record, appends the bounded diagnostic to a quality
failure, and strips the position before a successful emit. Positions are checked
as a sequence by the state declared at
`io/rowweave/ops/RowValidation.java:507`, so a missing, repeated or out-of-order
position is a refusal rather than a silent pass.

## Failure codes

Both operators name failures with stable codes inside an `IllegalStateException`
at execution time or an `IllegalArgumentException` at build and admission time.

| Code | Condition | Raised |
|---|---|---|
| `SCHEMA_ROW_WIDTH_MISMATCH` | input row width differs from the bound input header | `io/rowweave/ops/SchemaConversion.java:243` |
| `SCHEMA_NULL_VIOLATION` | a `required` mapping received null | `io/rowweave/ops/SchemaConversion.java:256` |
| `SCHEMA_VALUE_TYPE_MISMATCH` | a value's physical type differs from the declared source type | `io/rowweave/ops/SchemaConversion.java:299` |
| `SCHEMA_MAPPING_INVALID` | the mapping itself is not constructible, including an unsupported cast | `io/rowweave/ops/SchemaConversion.java:281`, builder errors via `:322` |
| `QUALITY_ROW_WIDTH_MISMATCH` | row width differs from the validated header | `io/rowweave/ops/RowValidation.java:353`, structured form `:78` |
| `QUALITY_VALUE_TYPE_MISMATCH` | field physical type differs from the rule's expectation | `io/rowweave/ops/RowValidation.java:356`, structured form `:83` |
| `QUALITY_RULE_VIOLATION` | the value is well-typed and out of bounds | `io/rowweave/ops/RowValidation.java:360`, structured form `:89` |
| `QUALITY_VALIDATION_RUNTIME_INVALID` | the operator reached a state it should not be in | `io/rowweave/ops/RowValidation.java:364`, `:439` |
| `QUALITY_SOURCE_POSITION_INVALID` | a bound source position is missing or wrong | `io/rowweave/ops/RowValidation.java:503` |
| `QUALITY_VALIDATION_INVALID` | the rule set is not constructible | `io/rowweave/ops/RowValidation.java:629` |

Message text beyond the leading code is diagnostic and is not a contract; the
code and the exception type are. See
[errors and refusals](errors-and-refusals.md) for the catalog.

## Describable rebuild

Both types expose a pure canonical rebuild from primitive and string lists:
`fromDescribable` at `io/rowweave/ops/SchemaConversion.java:91` and
`io/rowweave/ops/RowValidation.java:179`. Admission and Worker execution call the
same method, so a plan that builds on the client rebuilds identically on the
Worker or is refused there with the same code. Each deliberately re-throws an
already-coded `SCHEMA_MAPPING_INVALID` or `QUALITY_VALIDATION_INVALID` failure
rather than masking it, at `io/rowweave/ops/SchemaConversion.java:101` and
`io/rowweave/ops/RowValidation.java:189`.

## Tested by

- [`SchemaConversionModelTest`](../../../rowweave-core/src/test/java/io/rowweave/SchemaConversionModelTest.java)
  and [`SchemaConversionStreamingTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/SchemaConversionStreamingTest.java):
  builder rules, casts, defaults, and one-row-at-a-time streaming.
- [`SchemaConversionPlanAdmissionTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/SchemaConversionPlanAdmissionTest.java):
  describable rebuild and refusals during admission.
- [`RowValidationModelTest`](../../../rowweave-core/src/test/java/io/rowweave/RowValidationModelTest.java),
  [`RowValidationEvaluationTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/RowValidationEvaluationTest.java)
  and [`RowValidationPlanAdmissionTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/RowValidationPlanAdmissionTest.java):
  each rule kind, the structured result, and admission parity.
- [`PositionedRowValidationStreamingTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/PositionedRowValidationStreamingTest.java)
  and [`PositionedRowValidationPlanAdmissionTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/PositionedRowValidationPlanAdmissionTest.java):
  source-position binding, ordering and refusal.
- [`TypedRowClassificationRuntimeTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/TypedRowClassificationRuntimeTest.java),
  [`TypedRowClassificationExecutionTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/TypedRowClassificationExecutionTest.java)
  and [`TypedRowClassificationPlanAdmissionTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/TypedRowClassificationPlanAdmissionTest.java):
  carrier schema, routing, and cluster execution.
- [`DataRowClassificationModelTest`](../../../rowweave-core/src/test/java/io/rowweave/DataRowClassificationModelTest.java)
  and [`ObjectStorageRowClassificationModelTest`](../../../rowweave-core/src/test/java/io/rowweave/ObjectStorageRowClassificationModelTest.java):
  the valid and rejected halves.
- [`TypedQualityManifestModelTest`](../../../rowweave-core/src/test/java/io/rowweave/TypedQualityManifestModelTest.java)
  and [`TypedQualityPublicationLocalExecutionTest`](../../../rowweave-core/src/test/java/io/rowweave/TypedQualityPublicationLocalExecutionTest.java):
  summary header and atomic publication of both routes.
- [`QualityGatePolicyTest`](../../../rowweave-core/src/test/java/io/rowweave/QualityGatePolicyTest.java),
  [`QualityGateDecisionCodecTest`](../../../rowweave-core/src/test/java/io/rowweave/QualityGateDecisionCodecTest.java),
  [`QualityGatePlanBindingTest`](../../../rowweave-core/src/test/java/io/rowweave/QualityGatePlanBindingTest.java)
  and [`QualityGateInMemoryAuthorityTest`](../../../rowweave-core/src/test/java/io/rowweave/QualityGateInMemoryAuthorityTest.java):
  the threshold decision, its wire form and its authority.

Related: [built-in Batch sources and source replay](batch-sources.md),
[Batch sinks and output boundaries](batch-sinks.md),
[custom function interfaces](custom-functions.md),
[decimal and null semantics](decimal-null-semantics.md),
[errors and refusals](errors-and-refusals.md),
[data-plane limits](data-plane-limits.md).
