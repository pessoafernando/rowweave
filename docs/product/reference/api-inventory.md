---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Public API inventory

This page separates RowWeave's frozen public vocabulary from other declarations
that happen to be public in production source. Use the
[API overview](api-overview.md) for task-oriented entry points and this
inventory for exhaustive traceability.

The inventory covers every production Java module. It does not promote every
source-visible class to a supported extension point. Compatibility promises
apply only to the frozen vocabulary and to behavior explicitly documented at
the supported boundary. Internal implementation classes may still be
source-visible because Java packages do not express every product boundary.

<!-- GENERATED:PUBLIC-API-INVENTORY:START -->
## Functional inventory

### Batch DataSet

[`io.rowweave.CoGroupDataSet`](../../../rowweave-core/src/main/java/io/rowweave/CoGroupDataSet.java)
[`io.rowweave.DataSet`](../../../rowweave-core/src/main/java/io/rowweave/DataSet.java)
[`io.rowweave.ExecutionEnvironment`](../../../rowweave-core/src/main/java/io/rowweave/ExecutionEnvironment.java)
[`io.rowweave.Grouper`](../../../rowweave-core/src/main/java/io/rowweave/Grouper.java)
[`io.rowweave.JoinDataSet`](../../../rowweave-core/src/main/java/io/rowweave/JoinDataSet.java)
[`io.rowweave.RowWeave`](../../../rowweave-core/src/main/java/io/rowweave/RowWeave.java)

Boundary: Create, compose, validate, and execute a whole Batch graph.

### StreamSession / DataStream

[`io.rowweave.DataStream`](../../../rowweave-core/src/main/java/io/rowweave/DataStream.java)
[`io.rowweave.RowWeave`](../../../rowweave-core/src/main/java/io/rowweave/RowWeave.java)
[`io.rowweave.StreamCheckpointHandle`](../../../rowweave-core/src/main/java/io/rowweave/StreamCheckpointHandle.java)
[`io.rowweave.StreamCommandHandle`](../../../rowweave-core/src/main/java/io/rowweave/StreamCommandHandle.java)
[`io.rowweave.StreamJobClient`](../../../rowweave-core/src/main/java/io/rowweave/StreamJobClient.java)
[`io.rowweave.StreamJobHandle`](../../../rowweave-core/src/main/java/io/rowweave/StreamJobHandle.java)
[`io.rowweave.StreamOfflineRescaleRequest`](../../../rowweave-core/src/main/java/io/rowweave/StreamOfflineRescaleRequest.java)
[`io.rowweave.StreamRestoreRequest`](../../../rowweave-core/src/main/java/io/rowweave/StreamRestoreRequest.java)
[`io.rowweave.StreamSavepointHandle`](../../../rowweave-core/src/main/java/io/rowweave/StreamSavepointHandle.java)
[`io.rowweave.StreamSession`](../../../rowweave-core/src/main/java/io/rowweave/StreamSession.java)

Boundary: Configure a Stream session, compose its closed operator set, and control job lifecycle.

### Functions

[`io.rowweave.CoGroupFunction`](../../../rowweave-core/src/main/java/io/rowweave/CoGroupFunction.java)
[`io.rowweave.Collector`](../../../rowweave-core/src/main/java/io/rowweave/Collector.java)
[`io.rowweave.FilterFunction`](../../../rowweave-core/src/main/java/io/rowweave/FilterFunction.java)
[`io.rowweave.FlatMapFunction`](../../../rowweave-core/src/main/java/io/rowweave/FlatMapFunction.java)
[`io.rowweave.GroupCombineFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineFunction.java)
[`io.rowweave.GroupCombineReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineReduceFunction.java)
[`io.rowweave.GroupReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupReduceFunction.java)
[`io.rowweave.IncrementalMapFunction`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalMapFunction.java)
[`io.rowweave.JoinFunction`](../../../rowweave-core/src/main/java/io/rowweave/JoinFunction.java)
[`io.rowweave.MapFunction`](../../../rowweave-core/src/main/java/io/rowweave/MapFunction.java)
[`io.rowweave.MapPartitionFunction`](../../../rowweave-core/src/main/java/io/rowweave/MapPartitionFunction.java)
[`io.rowweave.ResultSchemaAware`](../../../rowweave-core/src/main/java/io/rowweave/ResultSchemaAware.java)
[`io.rowweave.RetractableStreamAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/RetractableStreamAggregateFunction.java)
[`io.rowweave.RetractableStreamWindowAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/RetractableStreamWindowAggregateFunction.java)
[`io.rowweave.StreamAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamAggregateFunction.java)
[`io.rowweave.StreamAsyncFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamAsyncFunction.java)
[`io.rowweave.StreamKeyedBroadcastProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedBroadcastProcessFunction.java)
[`io.rowweave.StreamKeyedCoProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedCoProcessFunction.java)
[`io.rowweave.StreamKeyedProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedProcessFunction.java)
[`io.rowweave.StreamWindowAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowAggregateFunction.java)
[`io.rowweave.StreamWindowProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowProcessFunction.java)

Boundary: Typed callbacks supplied by applications and connector authors.

### Sources and sinks

[`io.rowweave.AssignedSourceBatch`](../../../rowweave-core/src/main/java/io/rowweave/AssignedSourceBatch.java)
[`io.rowweave.BatchExecutableSource`](../../../rowweave-core/src/main/java/io/rowweave/BatchExecutableSource.java)
[`io.rowweave.BatchSink`](../../../rowweave-core/src/main/java/io/rowweave/BatchSink.java)
[`io.rowweave.BatchSource`](../../../rowweave-core/src/main/java/io/rowweave/BatchSource.java)
[`io.rowweave.CollectingSink`](../../../rowweave-core/src/main/java/io/rowweave/CollectingSink.java)
[`io.rowweave.CollectionSource`](../../../rowweave-core/src/main/java/io/rowweave/CollectionSource.java)
[`io.rowweave.ConcatSource`](../../../rowweave-core/src/main/java/io/rowweave/ConcatSource.java)
[`io.rowweave.DiscardSink`](../../../rowweave-core/src/main/java/io/rowweave/DiscardSink.java)
[`io.rowweave.FileSink`](../../../rowweave-core/src/main/java/io/rowweave/FileSink.java)
[`io.rowweave.FileSource`](../../../rowweave-core/src/main/java/io/rowweave/FileSource.java)
[`io.rowweave.IncrementalSourceSplit`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalSourceSplit.java)
[`io.rowweave.IterableSource`](../../../rowweave-core/src/main/java/io/rowweave/IterableSource.java)
[`io.rowweave.JdbcSnapshotSource`](../../../rowweave-core/src/main/java/io/rowweave/JdbcSnapshotSource.java)
[`io.rowweave.ManifestFileSink`](../../../rowweave-core/src/main/java/io/rowweave/ManifestFileSink.java)
[`io.rowweave.ManifestFileSource`](../../../rowweave-core/src/main/java/io/rowweave/ManifestFileSource.java)
[`io.rowweave.ObjectStorageQualitySource`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageQualitySource.java)
[`io.rowweave.ObjectStorageSnapshotSource`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageSnapshotSource.java)
[`io.rowweave.PrintSink`](../../../rowweave-core/src/main/java/io/rowweave/PrintSink.java)
[`io.rowweave.ProviderBatchSink`](../../../rowweave-core/src/main/java/io/rowweave/ProviderBatchSink.java)
[`io.rowweave.Sink`](../../../rowweave-core/src/main/java/io/rowweave/Sink.java)
[`io.rowweave.Source`](../../../rowweave-core/src/main/java/io/rowweave/Source.java)
[`io.rowweave.SourceExecutor`](../../../rowweave-core/src/main/java/io/rowweave/SourceExecutor.java)
[`io.rowweave.SourceExecutorFactory`](../../../rowweave-core/src/main/java/io/rowweave/SourceExecutorFactory.java)

Boundary: Built-in boundaries and provider-facing Batch source/sink contracts.

### Schema and data types

[`io.rowweave.DataRow`](../../../rowweave-core/src/main/java/io/rowweave/DataRow.java)
[`io.rowweave.DataRowClassification`](../../../rowweave-core/src/main/java/io/rowweave/DataRowClassification.java)
[`io.rowweave.DataSchema`](../../../rowweave-core/src/main/java/io/rowweave/DataSchema.java)
[`io.rowweave.DataType`](../../../rowweave-core/src/main/java/io/rowweave/DataType.java)
[`io.rowweave.Field`](../../../rowweave-core/src/main/java/io/rowweave/Field.java)
[`io.rowweave.PartitionDataRowCursor`](../../../rowweave-core/src/main/java/io/rowweave/PartitionDataRowCursor.java)
[`io.rowweave.StreamWindowDataRowCursor`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowDataRowCursor.java)
[`io.rowweave.stream.api.StreamDataRowCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamDataRowCodec.java)
[`io.rowweave.stream.api.StreamOutputTag`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamOutputTag.java)

Boundary: Positional cells, explicit schemas, cursors, codecs, and tagged outputs.

### Execution clients and process entry points

[`io.rowweave.StreamClusterMain`](../../../rowweave-core/src/main/java/io/rowweave/StreamClusterMain.java)
[`io.rowweave.StreamMasterRuntime`](../../../rowweave-core/src/main/java/io/rowweave/StreamMasterRuntime.java)
[`io.rowweave.StreamWorkerRuntime`](../../../rowweave-core/src/main/java/io/rowweave/StreamWorkerRuntime.java)
[`io.rowweave.cluster.ClusterMain`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ClusterMain.java)
[`io.rowweave.cluster.InProcessCluster`](../../../rowweave-core/src/main/java/io/rowweave/cluster/InProcessCluster.java)
[`io.rowweave.cluster.RealClusterJobClient`](../../../rowweave-core/src/main/java/io/rowweave/cluster/RealClusterJobClient.java)

Boundary: Single-JVM and separate-process execution entry points.

### Exceptions

[`io.rowweave.CompatibilityDataLimitException`](../../../rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimitException.java)
[`io.rowweave.StreamProcessException`](../../../rowweave-core/src/main/java/io/rowweave/StreamProcessException.java)
[`io.rowweave.stream.api.StreamApiException`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamApiException.java)
[`io.rowweave.stream.api.StreamControlException`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamControlException.java)
[`io.rowweave.stream.api.StreamExpressionException`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamExpressionException.java)
[`io.rowweave.stream.spi.conformance.StreamConnectorConformanceException`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamConnectorConformanceException.java)

Boundary: Typed failure surfaces; detailed codes belong to the dedicated error reference.

### Connector SPI

[`io.rowweave.connector.postgres.PostgresConnectorConfiguration`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorConfiguration.java)
[`io.rowweave.connector.postgres.PostgresConnectorContract`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorContract.java)
[`io.rowweave.connector.postgres.PostgresConnectorProvider`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorProvider.java)
[`io.rowweave.connector.postgres.PostgresDeploymentPackager`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresDeploymentPackager.java)
[`io.rowweave.connector.s3.S3ConnectorConfiguration`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorConfiguration.java)
[`io.rowweave.connector.s3.S3ConnectorContract`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorContract.java)
[`io.rowweave.connector.s3.S3ConnectorProvider`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorProvider.java)
[`io.rowweave.connector.s3.S3DeploymentPackager`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3DeploymentPackager.java)

Boundary: Deployable connector boundaries; codecs and clients are source-visible implementation details unless separately documented.

### Pipeline boundary

[`io.rowweave.pipeline.PipelineCompiler`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineCompiler.java)
[`io.rowweave.pipeline.PipelineDefinition`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineDefinition.java)
[`io.rowweave.pipeline.PipelineExecutionPlan`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineExecutionPlan.java)
[`io.rowweave.pipeline.PipelineValidator`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineValidator.java)
[`io.rowweave.pipeline.VersionedDatasetHandoffAuthority`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffAuthority.java)
[`io.rowweave.pipeline.VersionedDatasetHandoffSdk`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffSdk.java)

Boundary: Compose portable Batch/Stream artifacts without executing them inside Pipeline.

### Monitor public contract

[`io.rowweave.monitor.http.MonitorHttpServer`](../../../rowweave-monitor/src/main/java/io/rowweave/monitor/http/MonitorHttpServer.java)
[`io.rowweave.monitor.http.NettyMonitorLauncher`](../../../rowweave-monitor/src/main/java/io/rowweave/monitor/http/NettyMonitorLauncher.java)

Boundary: The optional read-only HTTP monitor; monitoring never gates execution.

## Frozen public API vocabulary

Version `0.2.0` freezes **60** unique public classes
and **52** method signatures. Required symbols are the primary application and provider vocabulary; retained symbols preserve the public composition and function surface.

### Classes

#### Required

- [`io.rowweave.AssignedSourceBatch`](../../../rowweave-core/src/main/java/io/rowweave/AssignedSourceBatch.java) — required in `0.2.0`.
- [`io.rowweave.BatchExecutableSource`](../../../rowweave-core/src/main/java/io/rowweave/BatchExecutableSource.java) — required in `0.2.0`.
- [`io.rowweave.BatchSink`](../../../rowweave-core/src/main/java/io/rowweave/BatchSink.java) — required in `0.2.0`.
- [`io.rowweave.BatchSource`](../../../rowweave-core/src/main/java/io/rowweave/BatchSource.java) — required in `0.2.0`.
- [`io.rowweave.CollectingSink`](../../../rowweave-core/src/main/java/io/rowweave/CollectingSink.java) — required in `0.2.0`.
- [`io.rowweave.CollectionSource`](../../../rowweave-core/src/main/java/io/rowweave/CollectionSource.java) — required in `0.2.0`.
- [`io.rowweave.Collector`](../../../rowweave-core/src/main/java/io/rowweave/Collector.java) — required in `0.2.0`.
- [`io.rowweave.ConcatSource`](../../../rowweave-core/src/main/java/io/rowweave/ConcatSource.java) — required in `0.2.0`.
- [`io.rowweave.DataRow`](../../../rowweave-core/src/main/java/io/rowweave/DataRow.java) — required in `0.2.0`.
- [`io.rowweave.DataRowClassification`](../../../rowweave-core/src/main/java/io/rowweave/DataRowClassification.java) — required in `0.2.0`.
- [`io.rowweave.DataSchema`](../../../rowweave-core/src/main/java/io/rowweave/DataSchema.java) — required in `0.2.0`.
- [`io.rowweave.DiscardSink`](../../../rowweave-core/src/main/java/io/rowweave/DiscardSink.java) — required in `0.2.0`.
- [`io.rowweave.ExecutionEnvironment`](../../../rowweave-core/src/main/java/io/rowweave/ExecutionEnvironment.java) — required in `0.2.0`.
- [`io.rowweave.FileSink`](../../../rowweave-core/src/main/java/io/rowweave/FileSink.java) — required in `0.2.0`.
- [`io.rowweave.FileSource`](../../../rowweave-core/src/main/java/io/rowweave/FileSource.java) — required in `0.2.0`.
- [`io.rowweave.IncrementalSourceSplit`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalSourceSplit.java) — required in `0.2.0`.
- [`io.rowweave.IterableSource`](../../../rowweave-core/src/main/java/io/rowweave/IterableSource.java) — required in `0.2.0`.
- [`io.rowweave.JdbcSnapshotSource`](../../../rowweave-core/src/main/java/io/rowweave/JdbcSnapshotSource.java) — required in `0.2.0`.
- [`io.rowweave.ManifestFileSink`](../../../rowweave-core/src/main/java/io/rowweave/ManifestFileSink.java) — required in `0.2.0`.
- [`io.rowweave.ManifestFileSource`](../../../rowweave-core/src/main/java/io/rowweave/ManifestFileSource.java) — required in `0.2.0`.
- [`io.rowweave.ObjectStorageQualitySource`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageQualitySource.java) — required in `0.2.0`.
- [`io.rowweave.ObjectStorageSnapshotSource`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageSnapshotSource.java) — required in `0.2.0`.
- [`io.rowweave.PartitionDataRowCursor`](../../../rowweave-core/src/main/java/io/rowweave/PartitionDataRowCursor.java) — required in `0.2.0`.
- [`io.rowweave.PrintSink`](../../../rowweave-core/src/main/java/io/rowweave/PrintSink.java) — required in `0.2.0`.
- [`io.rowweave.ProviderBatchSink`](../../../rowweave-core/src/main/java/io/rowweave/ProviderBatchSink.java) — required in `0.2.0`.
- [`io.rowweave.ResultSchemaAware`](../../../rowweave-core/src/main/java/io/rowweave/ResultSchemaAware.java) — required in `0.2.0`.
- [`io.rowweave.Sink`](../../../rowweave-core/src/main/java/io/rowweave/Sink.java) — required in `0.2.0`.
- [`io.rowweave.Source`](../../../rowweave-core/src/main/java/io/rowweave/Source.java) — required in `0.2.0`.
- [`io.rowweave.SourceExecutor`](../../../rowweave-core/src/main/java/io/rowweave/SourceExecutor.java) — required in `0.2.0`.
- [`io.rowweave.SourceExecutorFactory`](../../../rowweave-core/src/main/java/io/rowweave/SourceExecutorFactory.java) — required in `0.2.0`.
- [`io.rowweave.StreamWindowDataRowCursor`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowDataRowCursor.java) — required in `0.2.0`.
- [`io.rowweave.stream.api.StreamDataRowCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamDataRowCodec.java) — required in `0.2.0`.

#### Retained

- [`io.rowweave.CoGroupDataSet`](../../../rowweave-core/src/main/java/io/rowweave/CoGroupDataSet.java) — retained in `0.2.0`.
- [`io.rowweave.CoGroupFunction`](../../../rowweave-core/src/main/java/io/rowweave/CoGroupFunction.java) — retained in `0.2.0`.
- [`io.rowweave.DataSet`](../../../rowweave-core/src/main/java/io/rowweave/DataSet.java) — retained in `0.2.0`.
- [`io.rowweave.DataStream`](../../../rowweave-core/src/main/java/io/rowweave/DataStream.java) — retained in `0.2.0`.
- [`io.rowweave.DataType`](../../../rowweave-core/src/main/java/io/rowweave/DataType.java) — retained in `0.2.0`.
- [`io.rowweave.Field`](../../../rowweave-core/src/main/java/io/rowweave/Field.java) — retained in `0.2.0`.
- [`io.rowweave.FilterFunction`](../../../rowweave-core/src/main/java/io/rowweave/FilterFunction.java) — retained in `0.2.0`.
- [`io.rowweave.FlatMapFunction`](../../../rowweave-core/src/main/java/io/rowweave/FlatMapFunction.java) — retained in `0.2.0`.
- [`io.rowweave.GroupCombineFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineFunction.java) — retained in `0.2.0`.
- [`io.rowweave.GroupCombineReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineReduceFunction.java) — retained in `0.2.0`.
- [`io.rowweave.GroupReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupReduceFunction.java) — retained in `0.2.0`.
- [`io.rowweave.Grouper`](../../../rowweave-core/src/main/java/io/rowweave/Grouper.java) — retained in `0.2.0`.
- [`io.rowweave.IncrementalMapFunction`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalMapFunction.java) — retained in `0.2.0`.
- [`io.rowweave.JoinDataSet`](../../../rowweave-core/src/main/java/io/rowweave/JoinDataSet.java) — retained in `0.2.0`.
- [`io.rowweave.JoinFunction`](../../../rowweave-core/src/main/java/io/rowweave/JoinFunction.java) — retained in `0.2.0`.
- [`io.rowweave.MapFunction`](../../../rowweave-core/src/main/java/io/rowweave/MapFunction.java) — retained in `0.2.0`.
- [`io.rowweave.MapPartitionFunction`](../../../rowweave-core/src/main/java/io/rowweave/MapPartitionFunction.java) — retained in `0.2.0`.
- [`io.rowweave.RetractableStreamAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/RetractableStreamAggregateFunction.java) — retained in `0.2.0`.
- [`io.rowweave.RetractableStreamWindowAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/RetractableStreamWindowAggregateFunction.java) — retained in `0.2.0`.
- [`io.rowweave.RowWeave`](../../../rowweave-core/src/main/java/io/rowweave/RowWeave.java) — retained in `0.2.0`.
- [`io.rowweave.StreamAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamAggregateFunction.java) — retained in `0.2.0`.
- [`io.rowweave.StreamAsyncFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamAsyncFunction.java) — retained in `0.2.0`.
- [`io.rowweave.StreamKeyedBroadcastProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedBroadcastProcessFunction.java) — retained in `0.2.0`.
- [`io.rowweave.StreamKeyedCoProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedCoProcessFunction.java) — retained in `0.2.0`.
- [`io.rowweave.StreamKeyedProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedProcessFunction.java) — retained in `0.2.0`.
- [`io.rowweave.StreamWindowAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowAggregateFunction.java) — retained in `0.2.0`.
- [`io.rowweave.StreamWindowProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowProcessFunction.java) — retained in `0.2.0`.
- [`io.rowweave.stream.api.StreamOutputTag`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamOutputTag.java) — retained in `0.2.0`.

### Method signatures

- [`io.rowweave.DataRow`](../../../rowweave-core/src/main/java/io/rowweave/DataRow.java) — `size(): int`.
- [`io.rowweave.DataRow`](../../../rowweave-core/src/main/java/io/rowweave/DataRow.java) — `set(int, java.lang.Object): void`.
- [`io.rowweave.DataRow`](../../../rowweave-core/src/main/java/io/rowweave/DataRow.java) — `get(int): java.lang.Object`.
- [`io.rowweave.DataRow`](../../../rowweave-core/src/main/java/io/rowweave/DataRow.java) — `copy(): io.rowweave.DataRow`.
- [`io.rowweave.DataSchema`](../../../rowweave-core/src/main/java/io/rowweave/DataSchema.java) — `getFieldCount(): int`.
- [`io.rowweave.DataSchema`](../../../rowweave-core/src/main/java/io/rowweave/DataSchema.java) — `getField(int): io.rowweave.Field`.
- [`io.rowweave.DataSchema`](../../../rowweave-core/src/main/java/io/rowweave/DataSchema.java) — `getFields(): io.rowweave.Field[]`.
- [`io.rowweave.DataSchema`](../../../rowweave-core/src/main/java/io/rowweave/DataSchema.java) — `getFieldIndex(java.lang.String): int`.
- [`io.rowweave.ExecutionEnvironment`](../../../rowweave-core/src/main/java/io/rowweave/ExecutionEnvironment.java) — `fromSource(io.rowweave.Source): io.rowweave.DataSet`.
- [`io.rowweave.ExecutionEnvironment`](../../../rowweave-core/src/main/java/io/rowweave/ExecutionEnvironment.java) — `fromSource(io.rowweave.Source[]): io.rowweave.DataSet`.
- [`io.rowweave.ExecutionEnvironment`](../../../rowweave-core/src/main/java/io/rowweave/ExecutionEnvironment.java) — `setParallelism(int): void`.
- [`io.rowweave.ExecutionEnvironment`](../../../rowweave-core/src/main/java/io/rowweave/ExecutionEnvironment.java) — `getParallelism(): int`.
- [`io.rowweave.ExecutionEnvironment`](../../../rowweave-core/src/main/java/io/rowweave/ExecutionEnvironment.java) — `execute(): void`.
- [`io.rowweave.RowWeave`](../../../rowweave-core/src/main/java/io/rowweave/RowWeave.java) — `createEnvironment(java.lang.String): io.rowweave.ExecutionEnvironment`.
- [`io.rowweave.RowWeave`](../../../rowweave-core/src/main/java/io/rowweave/RowWeave.java) — `createEnvironment(java.lang.String, java.lang.String): io.rowweave.ExecutionEnvironment`.
- [`io.rowweave.DataSet`](../../../rowweave-core/src/main/java/io/rowweave/DataSet.java) — `getSchema(): io.rowweave.DataSchema`.
- [`io.rowweave.DataSet`](../../../rowweave-core/src/main/java/io/rowweave/DataSet.java) — `sinkTo(io.rowweave.Sink): void`.
- [`io.rowweave.DataSet`](../../../rowweave-core/src/main/java/io/rowweave/DataSet.java) — `sinkTo(java.lang.String, io.rowweave.Sink): void`.
- [`io.rowweave.DataStream`](../../../rowweave-core/src/main/java/io/rowweave/DataStream.java) — `getSchema(): io.rowweave.DataSchema`.
- [`io.rowweave.DataStream`](../../../rowweave-core/src/main/java/io/rowweave/DataStream.java) — `sinkTo(java.lang.String, io.rowweave.stream.api.StreamSinkSpec): void`.
- [`io.rowweave.Source`](../../../rowweave-core/src/main/java/io/rowweave/Source.java) — `getSchema(): io.rowweave.DataSchema`.
- [`io.rowweave.Sink`](../../../rowweave-core/src/main/java/io/rowweave/Sink.java) — `getSchema(): io.rowweave.DataSchema`.
- [`io.rowweave.Sink`](../../../rowweave-core/src/main/java/io/rowweave/Sink.java) — `collect(io.rowweave.DataRow): void`.
- [`io.rowweave.Collector`](../../../rowweave-core/src/main/java/io/rowweave/Collector.java) — `collect(io.rowweave.DataRow): void`.
- [`io.rowweave.ResultSchemaAware`](../../../rowweave-core/src/main/java/io/rowweave/ResultSchemaAware.java) — `getResultSchema(): io.rowweave.DataSchema`.
- [`io.rowweave.MapFunction`](../../../rowweave-core/src/main/java/io/rowweave/MapFunction.java) — `map(io.rowweave.DataRow): io.rowweave.DataRow`.
- [`io.rowweave.FilterFunction`](../../../rowweave-core/src/main/java/io/rowweave/FilterFunction.java) — `filter(io.rowweave.DataRow): boolean`.
- [`io.rowweave.FlatMapFunction`](../../../rowweave-core/src/main/java/io/rowweave/FlatMapFunction.java) — `flatMap(io.rowweave.DataRow, io.rowweave.Collector): void`.
- [`io.rowweave.MapPartitionFunction`](../../../rowweave-core/src/main/java/io/rowweave/MapPartitionFunction.java) — `mapPartition(io.rowweave.PartitionDataRowCursor, io.rowweave.Collector): void`.
- [`io.rowweave.GroupReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupReduceFunction.java) — `reduce(java.lang.Iterable, io.rowweave.Collector): void`.
- [`io.rowweave.GroupCombineFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineFunction.java) — `combine(java.lang.Iterable, io.rowweave.Collector): void`.
- [`io.rowweave.GroupCombineFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineFunction.java) — `getResultSchema(): io.rowweave.DataSchema`.
- [`io.rowweave.GroupCombineReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineReduceFunction.java) — `combine(java.lang.Iterable, io.rowweave.Collector): void`.
- [`io.rowweave.GroupCombineReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineReduceFunction.java) — `reduce(java.lang.Iterable, io.rowweave.Collector): void`.
- [`io.rowweave.JoinFunction`](../../../rowweave-core/src/main/java/io/rowweave/JoinFunction.java) — `join(io.rowweave.DataRow, io.rowweave.DataRow): io.rowweave.DataRow`.
- [`io.rowweave.CoGroupFunction`](../../../rowweave-core/src/main/java/io/rowweave/CoGroupFunction.java) — `coGroup(java.lang.Iterable, java.lang.Iterable, io.rowweave.Collector): void`.
- [`io.rowweave.StreamAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamAggregateFunction.java) — `add(io.rowweave.DataRow, byte[]): byte[]`.
- [`io.rowweave.StreamAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamAggregateFunction.java) — `getResult(byte[]): io.rowweave.DataRow`.
- [`io.rowweave.RetractableStreamAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/RetractableStreamAggregateFunction.java) — `retract(io.rowweave.DataRow, byte[]): byte[]`.
- [`io.rowweave.StreamWindowAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowAggregateFunction.java) — `add(io.rowweave.DataRow, byte[]): byte[]`.
- [`io.rowweave.StreamWindowAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowAggregateFunction.java) — `getResult(byte[], io.rowweave.StreamWindowContext): io.rowweave.DataRow`.
- [`io.rowweave.StreamAsyncFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamAsyncFunction.java) — `createRequest(io.rowweave.DataRow): byte[]`.
- [`io.rowweave.StreamAsyncFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamAsyncFunction.java) — `complete(io.rowweave.DataRow, byte[], io.rowweave.StreamCollector): void`.
- [`io.rowweave.StreamKeyedProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedProcessFunction.java) — `processElement(io.rowweave.DataRow, io.rowweave.StreamProcessContext, io.rowweave.StreamCollector): void`.
- [`io.rowweave.StreamKeyedCoProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedCoProcessFunction.java) — `processLeft(io.rowweave.DataRow, io.rowweave.StreamProcessContext, io.rowweave.StreamCollector): void`.
- [`io.rowweave.StreamKeyedCoProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedCoProcessFunction.java) — `processRight(io.rowweave.DataRow, io.rowweave.StreamProcessContext, io.rowweave.StreamCollector): void`.
- [`io.rowweave.StreamKeyedBroadcastProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedBroadcastProcessFunction.java) — `processKeyed(io.rowweave.DataRow, io.rowweave.StreamKeyedBroadcastProcessContext, io.rowweave.StreamCollector): void`.
- [`io.rowweave.StreamKeyedBroadcastProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedBroadcastProcessFunction.java) — `processBroadcast(io.rowweave.DataRow, io.rowweave.StreamBroadcastProcessContext, io.rowweave.StreamCollector): void`.
- [`io.rowweave.StreamWindowProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowProcessFunction.java) — `process(io.rowweave.StreamWindowContext, io.rowweave.StreamWindowDataRowCursor, io.rowweave.StreamWindowCollector): void`.
- [`io.rowweave.stream.api.StreamDataRowCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamDataRowCodec.java) — `encode(io.rowweave.DataRow): byte[]`.
- [`io.rowweave.stream.api.StreamDataRowCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamDataRowCodec.java) — `decode(byte[]): io.rowweave.DataRow`.
- [`io.rowweave.StreamWindowDataRowCursor`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowDataRowCursor.java) — `row(): io.rowweave.DataRow`.

## Source-visible public types

This inventory is exhaustive for top-level Java declarations with the `public` modifier in production source. Nested public types are visible through their owner and are not separate inventory rows. A declaration below is not automatically a supported extension point.

### Module and package counts

| Module | Public packages | Public top-level types | Application dependency |
|---|---:|---:|---|
| `rowweave-core` | 33 | 1031 | Required |
| `rowweave-pipeline` | 1 | 34 | Optional |
| `rowweave-connector-s3` | 1 | 13 | Optional |
| `rowweave-connector-postgres-cdc` | 1 | 12 | Optional |
| `rowweave-registry-zk` | 4 | 11 | Optional registry |
| `rowweave-monitor` | 1 | 2 | Optional monitor |
| `rowweave-examples` | 1 | 13 | Not an application dependency |
| `rowweave-soak` | 1 | 5 | Not an application dependency |
| **Total** | **43** | **1121** | — |

### Package map

| Package | Public top-level types |
|---|---:|
| `rowweave-soak` · `io.rowweave.soak` | 5 |

### Type inventory

Each name links to its production source declaration.

#### `rowweave-core`


##### `io.rowweave`

- [`io.rowweave.AssignedSourceBatch`](../../../rowweave-core/src/main/java/io/rowweave/AssignedSourceBatch.java) — public class.
- [`io.rowweave.AssignedSourceInputBatchExecutor`](../../../rowweave-core/src/main/java/io/rowweave/AssignedSourceInputBatchExecutor.java) — public class.
- [`io.rowweave.BatchExecutableSource`](../../../rowweave-core/src/main/java/io/rowweave/BatchExecutableSource.java) — public interface.
- [`io.rowweave.BatchRuntimes`](../../../rowweave-core/src/main/java/io/rowweave/BatchRuntimes.java) — public class.
- [`io.rowweave.BatchSink`](../../../rowweave-core/src/main/java/io/rowweave/BatchSink.java) — public interface.
- [`io.rowweave.BatchSinkBinding`](../../../rowweave-core/src/main/java/io/rowweave/BatchSinkBinding.java) — public class.
- [`io.rowweave.BatchSinkDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/BatchSinkDescriptor.java) — public class.
- [`io.rowweave.BatchSinkDescriptorCodec`](../../../rowweave-core/src/main/java/io/rowweave/BatchSinkDescriptorCodec.java) — public class.
- [`io.rowweave.BatchSource`](../../../rowweave-core/src/main/java/io/rowweave/BatchSource.java) — public interface.
- [`io.rowweave.BatchSourceSplitBinding`](../../../rowweave-core/src/main/java/io/rowweave/BatchSourceSplitBinding.java) — public class.
- [`io.rowweave.BatchSourceSplitDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/BatchSourceSplitDescriptor.java) — public class.
- [`io.rowweave.BatchSourceSplitDescriptorCodec`](../../../rowweave-core/src/main/java/io/rowweave/BatchSourceSplitDescriptorCodec.java) — public class.
- [`io.rowweave.CoGroupDataSet`](../../../rowweave-core/src/main/java/io/rowweave/CoGroupDataSet.java) — public class.
- [`io.rowweave.CoGroupFunction`](../../../rowweave-core/src/main/java/io/rowweave/CoGroupFunction.java) — public interface.
- [`io.rowweave.CollectingSink`](../../../rowweave-core/src/main/java/io/rowweave/CollectingSink.java) — public class.
- [`io.rowweave.CollectionSource`](../../../rowweave-core/src/main/java/io/rowweave/CollectionSource.java) — public class.
- [`io.rowweave.Collector`](../../../rowweave-core/src/main/java/io/rowweave/Collector.java) — public interface.
- [`io.rowweave.CompatibilityDataLimitException`](../../../rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimitException.java) — public class.
- [`io.rowweave.CompatibilityDataLimits`](../../../rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimits.java) — public class.
- [`io.rowweave.ConcatInputExecutor`](../../../rowweave-core/src/main/java/io/rowweave/ConcatInputExecutor.java) — public class.
- [`io.rowweave.ConcatSource`](../../../rowweave-core/src/main/java/io/rowweave/ConcatSource.java) — public class.
- [`io.rowweave.DataRow`](../../../rowweave-core/src/main/java/io/rowweave/DataRow.java) — public class.
- [`io.rowweave.DataRowClassification`](../../../rowweave-core/src/main/java/io/rowweave/DataRowClassification.java) — public class.
- [`io.rowweave.DataSchema`](../../../rowweave-core/src/main/java/io/rowweave/DataSchema.java) — public class.
- [`io.rowweave.DataSet`](../../../rowweave-core/src/main/java/io/rowweave/DataSet.java) — public class.
- [`io.rowweave.DataStream`](../../../rowweave-core/src/main/java/io/rowweave/DataStream.java) — public class.
- [`io.rowweave.DataType`](../../../rowweave-core/src/main/java/io/rowweave/DataType.java) — public enum.
- [`io.rowweave.DiscardSink`](../../../rowweave-core/src/main/java/io/rowweave/DiscardSink.java) — public class.
- [`io.rowweave.DurableBatchSinkPreparedGroup`](../../../rowweave-core/src/main/java/io/rowweave/DurableBatchSinkPreparedGroup.java) — public interface.
- [`io.rowweave.DurableBatchSinkProvider`](../../../rowweave-core/src/main/java/io/rowweave/DurableBatchSinkProvider.java) — public interface.
- [`io.rowweave.DurableBatchSinkProviderContract`](../../../rowweave-core/src/main/java/io/rowweave/DurableBatchSinkProviderContract.java) — public class.
- [`io.rowweave.DurableBatchSinkProviderContractCodec`](../../../rowweave-core/src/main/java/io/rowweave/DurableBatchSinkProviderContractCodec.java) — public class.
- [`io.rowweave.DurableBatchSinkWriteAttempt`](../../../rowweave-core/src/main/java/io/rowweave/DurableBatchSinkWriteAttempt.java) — public interface.
- [`io.rowweave.DurableSinkActionContext`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkActionContext.java) — public interface.
- [`io.rowweave.DurableSinkAttemptBudget`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkAttemptBudget.java) — public class.
- [`io.rowweave.DurableSinkCleanupContract`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkCleanupContract.java) — public class.
- [`io.rowweave.DurableSinkCodecDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkCodecDescriptor.java) — public class.
- [`io.rowweave.DurableSinkConfigurationEnvelope`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkConfigurationEnvelope.java) — public class.
- [`io.rowweave.DurableSinkConfigurationEnvelopeCodec`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkConfigurationEnvelopeCodec.java) — public class.
- [`io.rowweave.DurableSinkProviderKey`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkProviderKey.java) — public class.
- [`io.rowweave.DurableSinkResourceLease`](../../../rowweave-core/src/main/java/io/rowweave/DurableSinkResourceLease.java) — public interface.
- [`io.rowweave.ExecutionEnvironment`](../../../rowweave-core/src/main/java/io/rowweave/ExecutionEnvironment.java) — public class.
- [`io.rowweave.Field`](../../../rowweave-core/src/main/java/io/rowweave/Field.java) — public class.
- [`io.rowweave.FileInputExecutor`](../../../rowweave-core/src/main/java/io/rowweave/FileInputExecutor.java) — public class.
- [`io.rowweave.FileSink`](../../../rowweave-core/src/main/java/io/rowweave/FileSink.java) — public class.
- [`io.rowweave.FileSource`](../../../rowweave-core/src/main/java/io/rowweave/FileSource.java) — public class.
- [`io.rowweave.FilterFunction`](../../../rowweave-core/src/main/java/io/rowweave/FilterFunction.java) — public interface.
- [`io.rowweave.FlatMapFunction`](../../../rowweave-core/src/main/java/io/rowweave/FlatMapFunction.java) — public interface.
- [`io.rowweave.GroupCombineFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineFunction.java) — public interface.
- [`io.rowweave.GroupCombineReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineReduceFunction.java) — public interface.
- [`io.rowweave.GroupReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupReduceFunction.java) — public interface.
- [`io.rowweave.Grouper`](../../../rowweave-core/src/main/java/io/rowweave/Grouper.java) — public class.
- [`io.rowweave.IncrementalMapFunction`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalMapFunction.java) — public interface.
- [`io.rowweave.IncrementalSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalSourceProvider.java) — public interface.
- [`io.rowweave.IncrementalSourceRuntime`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalSourceRuntime.java) — public class.
- [`io.rowweave.IncrementalSourceSplit`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalSourceSplit.java) — public class.
- [`io.rowweave.IterableInputExecutor`](../../../rowweave-core/src/main/java/io/rowweave/IterableInputExecutor.java) — public class.
- [`io.rowweave.IterableSource`](../../../rowweave-core/src/main/java/io/rowweave/IterableSource.java) — public class.
- [`io.rowweave.JdbcSnapshotInputExecutor`](../../../rowweave-core/src/main/java/io/rowweave/JdbcSnapshotInputExecutor.java) — public class.
- [`io.rowweave.JdbcSnapshotReadHandle`](../../../rowweave-core/src/main/java/io/rowweave/JdbcSnapshotReadHandle.java) — public interface.
- [`io.rowweave.JdbcSnapshotReadRequest`](../../../rowweave-core/src/main/java/io/rowweave/JdbcSnapshotReadRequest.java) — public class.
- [`io.rowweave.JdbcSnapshotSource`](../../../rowweave-core/src/main/java/io/rowweave/JdbcSnapshotSource.java) — public class.
- [`io.rowweave.JdbcSnapshotSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/JdbcSnapshotSourceProvider.java) — public interface.
- [`io.rowweave.JoinDataSet`](../../../rowweave-core/src/main/java/io/rowweave/JoinDataSet.java) — public class.
- [`io.rowweave.JoinFunction`](../../../rowweave-core/src/main/java/io/rowweave/JoinFunction.java) — public interface.
- [`io.rowweave.ManifestFileInputExecutor`](../../../rowweave-core/src/main/java/io/rowweave/ManifestFileInputExecutor.java) — public class.
- [`io.rowweave.ManifestFileSink`](../../../rowweave-core/src/main/java/io/rowweave/ManifestFileSink.java) — public class.
- [`io.rowweave.ManifestFileSource`](../../../rowweave-core/src/main/java/io/rowweave/ManifestFileSource.java) — public class.
- [`io.rowweave.MapFunction`](../../../rowweave-core/src/main/java/io/rowweave/MapFunction.java) — public interface.
- [`io.rowweave.MapPartitionEmissionLimit`](../../../rowweave-core/src/main/java/io/rowweave/MapPartitionEmissionLimit.java) — public class.
- [`io.rowweave.MapPartitionFunction`](../../../rowweave-core/src/main/java/io/rowweave/MapPartitionFunction.java) — public interface.
- [`io.rowweave.ObjectStorageQualitySource`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageQualitySource.java) — public class.
- [`io.rowweave.ObjectStorageReadHandle`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageReadHandle.java) — public interface.
- [`io.rowweave.ObjectStorageReadRequest`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageReadRequest.java) — public class.
- [`io.rowweave.ObjectStorageRowClassification`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageRowClassification.java) — public class.
- [`io.rowweave.ObjectStorageSnapshotInputExecutor`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageSnapshotInputExecutor.java) — public class.
- [`io.rowweave.ObjectStorageSnapshotSource`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageSnapshotSource.java) — public class.
- [`io.rowweave.ObjectStorageSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageSourceProvider.java) — public interface.
- [`io.rowweave.PartitionDataRowCursor`](../../../rowweave-core/src/main/java/io/rowweave/PartitionDataRowCursor.java) — public interface.
- [`io.rowweave.PrintSink`](../../../rowweave-core/src/main/java/io/rowweave/PrintSink.java) — public class.
- [`io.rowweave.ProviderBatchSink`](../../../rowweave-core/src/main/java/io/rowweave/ProviderBatchSink.java) — public class.
- [`io.rowweave.QualityEvidenceReadGuard`](../../../rowweave-core/src/main/java/io/rowweave/QualityEvidenceReadGuard.java) — public class.
- [`io.rowweave.QualityExemplar`](../../../rowweave-core/src/main/java/io/rowweave/QualityExemplar.java) — public class.
- [`io.rowweave.QualityGateDecision`](../../../rowweave-core/src/main/java/io/rowweave/QualityGateDecision.java) — public class.
- [`io.rowweave.QualityGateDecisionCodec`](../../../rowweave-core/src/main/java/io/rowweave/QualityGateDecisionCodec.java) — public class.
- [`io.rowweave.QualityGateDecisionResult`](../../../rowweave-core/src/main/java/io/rowweave/QualityGateDecisionResult.java) — public class.
- [`io.rowweave.QualityGatePolicy`](../../../rowweave-core/src/main/java/io/rowweave/QualityGatePolicy.java) — public class.
- [`io.rowweave.QualityGatePolicyCodec`](../../../rowweave-core/src/main/java/io/rowweave/QualityGatePolicyCodec.java) — public class.
- [`io.rowweave.QualityPreparedEvidence`](../../../rowweave-core/src/main/java/io/rowweave/QualityPreparedEvidence.java) — public class.
- [`io.rowweave.QualityPreparedEvidenceReader`](../../../rowweave-core/src/main/java/io/rowweave/QualityPreparedEvidenceReader.java) — public class.
- [`io.rowweave.QualityPreparedMemberEvidence`](../../../rowweave-core/src/main/java/io/rowweave/QualityPreparedMemberEvidence.java) — public class.
- [`io.rowweave.QualityReasonKey`](../../../rowweave-core/src/main/java/io/rowweave/QualityReasonKey.java) — public class.
- [`io.rowweave.ResultSchemaAware`](../../../rowweave-core/src/main/java/io/rowweave/ResultSchemaAware.java) — public interface.
- [`io.rowweave.RetractableStreamAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/RetractableStreamAggregateFunction.java) — public interface.
- [`io.rowweave.RetractableStreamWindowAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/RetractableStreamWindowAggregateFunction.java) — public interface.
- [`io.rowweave.RowWeave`](../../../rowweave-core/src/main/java/io/rowweave/RowWeave.java) — public class.
- [`io.rowweave.Sink`](../../../rowweave-core/src/main/java/io/rowweave/Sink.java) — public interface.
- [`io.rowweave.Source`](../../../rowweave-core/src/main/java/io/rowweave/Source.java) — public interface.
- [`io.rowweave.SourceExecutor`](../../../rowweave-core/src/main/java/io/rowweave/SourceExecutor.java) — public class.
- [`io.rowweave.SourceExecutorFactory`](../../../rowweave-core/src/main/java/io/rowweave/SourceExecutorFactory.java) — public class.
- [`io.rowweave.StreamAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamAggregateFunction.java) — public interface.
- [`io.rowweave.StreamAggregatingState`](../../../rowweave-core/src/main/java/io/rowweave/StreamAggregatingState.java) — public interface.
- [`io.rowweave.StreamAsyncFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamAsyncFunction.java) — public interface.
- [`io.rowweave.StreamAsyncRequest`](../../../rowweave-core/src/main/java/io/rowweave/StreamAsyncRequest.java) — public class.
- [`io.rowweave.StreamBroadcastProcessContext`](../../../rowweave-core/src/main/java/io/rowweave/StreamBroadcastProcessContext.java) — public interface.
- [`io.rowweave.StreamBroadcastState`](../../../rowweave-core/src/main/java/io/rowweave/StreamBroadcastState.java) — public interface.
- [`io.rowweave.StreamBroadcastTimerContext`](../../../rowweave-core/src/main/java/io/rowweave/StreamBroadcastTimerContext.java) — public interface.
- [`io.rowweave.StreamCheckpointHandle`](../../../rowweave-core/src/main/java/io/rowweave/StreamCheckpointHandle.java) — public interface.
- [`io.rowweave.StreamClusterMain`](../../../rowweave-core/src/main/java/io/rowweave/StreamClusterMain.java) — public class.
- [`io.rowweave.StreamClusterRuntime`](../../../rowweave-core/src/main/java/io/rowweave/StreamClusterRuntime.java) — public class.
- [`io.rowweave.StreamCollector`](../../../rowweave-core/src/main/java/io/rowweave/StreamCollector.java) — public interface.
- [`io.rowweave.StreamCommandHandle`](../../../rowweave-core/src/main/java/io/rowweave/StreamCommandHandle.java) — public interface.
- [`io.rowweave.StreamJobClient`](../../../rowweave-core/src/main/java/io/rowweave/StreamJobClient.java) — public interface.
- [`io.rowweave.StreamJobHandle`](../../../rowweave-core/src/main/java/io/rowweave/StreamJobHandle.java) — public interface.
- [`io.rowweave.StreamKeyedBroadcastProcessContext`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedBroadcastProcessContext.java) — public interface.
- [`io.rowweave.StreamKeyedBroadcastProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedBroadcastProcessFunction.java) — public interface.
- [`io.rowweave.StreamKeyedCoProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedCoProcessFunction.java) — public interface.
- [`io.rowweave.StreamKeyedProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamKeyedProcessFunction.java) — public interface.
- [`io.rowweave.StreamListState`](../../../rowweave-core/src/main/java/io/rowweave/StreamListState.java) — public interface.
- [`io.rowweave.StreamMapState`](../../../rowweave-core/src/main/java/io/rowweave/StreamMapState.java) — public interface.
- [`io.rowweave.StreamMasterRuntime`](../../../rowweave-core/src/main/java/io/rowweave/StreamMasterRuntime.java) — public class.
- [`io.rowweave.StreamOfflineRescaleRequest`](../../../rowweave-core/src/main/java/io/rowweave/StreamOfflineRescaleRequest.java) — public class.
- [`io.rowweave.StreamOperationHandle`](../../../rowweave-core/src/main/java/io/rowweave/StreamOperationHandle.java) — public interface.
- [`io.rowweave.StreamProcessContext`](../../../rowweave-core/src/main/java/io/rowweave/StreamProcessContext.java) — public interface.
- [`io.rowweave.StreamProcessException`](../../../rowweave-core/src/main/java/io/rowweave/StreamProcessException.java) — public class.
- [`io.rowweave.StreamProcessResult`](../../../rowweave-core/src/main/java/io/rowweave/StreamProcessResult.java) — public class.
- [`io.rowweave.StreamReadOnlyBroadcastState`](../../../rowweave-core/src/main/java/io/rowweave/StreamReadOnlyBroadcastState.java) — public interface.
- [`io.rowweave.StreamRescaleHandle`](../../../rowweave-core/src/main/java/io/rowweave/StreamRescaleHandle.java) — public interface.
- [`io.rowweave.StreamRestoreRequest`](../../../rowweave-core/src/main/java/io/rowweave/StreamRestoreRequest.java) — public class.
- [`io.rowweave.StreamSavepointHandle`](../../../rowweave-core/src/main/java/io/rowweave/StreamSavepointHandle.java) — public interface.
- [`io.rowweave.StreamSavepointRequest`](../../../rowweave-core/src/main/java/io/rowweave/StreamSavepointRequest.java) — public class.
- [`io.rowweave.StreamSession`](../../../rowweave-core/src/main/java/io/rowweave/StreamSession.java) — public class.
- [`io.rowweave.StreamStateCursor`](../../../rowweave-core/src/main/java/io/rowweave/StreamStateCursor.java) — public interface.
- [`io.rowweave.StreamTimerContext`](../../../rowweave-core/src/main/java/io/rowweave/StreamTimerContext.java) — public interface.
- [`io.rowweave.StreamTimerService`](../../../rowweave-core/src/main/java/io/rowweave/StreamTimerService.java) — public interface.
- [`io.rowweave.StreamValueState`](../../../rowweave-core/src/main/java/io/rowweave/StreamValueState.java) — public interface.
- [`io.rowweave.StreamWindowAggregateFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowAggregateFunction.java) — public interface.
- [`io.rowweave.StreamWindowCollector`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowCollector.java) — public interface.
- [`io.rowweave.StreamWindowContext`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowContext.java) — public interface.
- [`io.rowweave.StreamWindowDataRowCursor`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowDataRowCursor.java) — public interface.
- [`io.rowweave.StreamWindowProcessFunction`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowProcessFunction.java) — public interface.
- [`io.rowweave.StreamWindowResult`](../../../rowweave-core/src/main/java/io/rowweave/StreamWindowResult.java) — public class.
- [`io.rowweave.StreamWorkerRegistryLease`](../../../rowweave-core/src/main/java/io/rowweave/StreamWorkerRegistryLease.java) — public class.
- [`io.rowweave.StreamWorkerRuntime`](../../../rowweave-core/src/main/java/io/rowweave/StreamWorkerRuntime.java) — public class.
- [`io.rowweave.TypedQualityManifest`](../../../rowweave-core/src/main/java/io/rowweave/TypedQualityManifest.java) — public class.

##### `io.rowweave.artifact`

- [`io.rowweave.artifact.ArtifactBackfillStatus`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactBackfillStatus.java) — public class.
- [`io.rowweave.artifact.ArtifactCandidate`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactCandidate.java) — public class.
- [`io.rowweave.artifact.ArtifactCandidatePage`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactCandidatePage.java) — public class.
- [`io.rowweave.artifact.ArtifactCollectResult`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactCollectResult.java) — public class.
- [`io.rowweave.artifact.ArtifactGcOwner`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactGcOwner.java) — public class.
- [`io.rowweave.artifact.ArtifactGcToken`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactGcToken.java) — public class.
- [`io.rowweave.artifact.ArtifactInventoryEntry`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactInventoryEntry.java) — public class.
- [`io.rowweave.artifact.ArtifactInventorySnapshot`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactInventorySnapshot.java) — public class.
- [`io.rowweave.artifact.ArtifactInventoryState`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactInventoryState.java) — public enum.
- [`io.rowweave.artifact.ArtifactInventoryUsage`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactInventoryUsage.java) — public class.
- [`io.rowweave.artifact.ArtifactJanitor`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactJanitor.java) — public class.
- [`io.rowweave.artifact.ArtifactLifecycleCodec`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactLifecycleCodec.java) — public class.
- [`io.rowweave.artifact.ArtifactLifecycleCursor`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactLifecycleCursor.java) — public class.
- [`io.rowweave.artifact.ArtifactLifecycleRepository`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactLifecycleRepository.java) — public interface.
- [`io.rowweave.artifact.ArtifactLifecycleUnavailableException`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactLifecycleUnavailableException.java) — public class.
- [`io.rowweave.artifact.ArtifactRecoveryCandidate`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactRecoveryCandidate.java) — public class.
- [`io.rowweave.artifact.ArtifactRecoveryPage`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactRecoveryPage.java) — public class.
- [`io.rowweave.artifact.ArtifactReferenceAuthority`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactReferenceAuthority.java) — public interface.
- [`io.rowweave.artifact.ArtifactReferenceAuthorityProvider`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactReferenceAuthorityProvider.java) — public interface.
- [`io.rowweave.artifact.ArtifactReferencePublisher`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactReferencePublisher.java) — public interface.
- [`io.rowweave.artifact.ArtifactReferenceRef`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactReferenceRef.java) — public class.
- [`io.rowweave.artifact.ArtifactReferenceView`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactReferenceView.java) — public class.
- [`io.rowweave.artifact.ArtifactRepository`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactRepository.java) — public interface.
- [`io.rowweave.artifact.ArtifactRepositoryProvider`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactRepositoryProvider.java) — public interface.
- [`io.rowweave.artifact.ArtifactRetentionPolicy`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactRetentionPolicy.java) — public class.
- [`io.rowweave.artifact.FileArtifactRepository`](../../../rowweave-core/src/main/java/io/rowweave/artifact/FileArtifactRepository.java) — public class.
- [`io.rowweave.artifact.InMemoryArtifactRepository`](../../../rowweave-core/src/main/java/io/rowweave/artifact/InMemoryArtifactRepository.java) — public class.
- [`io.rowweave.artifact.IncrementalPendingRunPublisher`](../../../rowweave-core/src/main/java/io/rowweave/artifact/IncrementalPendingRunPublisher.java) — public interface.
- [`io.rowweave.artifact.IncrementalRuntimeGate`](../../../rowweave-core/src/main/java/io/rowweave/artifact/IncrementalRuntimeGate.java) — public class.
- [`io.rowweave.artifact.LegacyPlanArtifactAdapter`](../../../rowweave-core/src/main/java/io/rowweave/artifact/LegacyPlanArtifactAdapter.java) — public class.
- [`io.rowweave.artifact.OperatorDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/artifact/OperatorDescriptor.java) — public class.
- [`io.rowweave.artifact.PlanArtifact`](../../../rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifact.java) — public class.
- [`io.rowweave.artifact.PlanArtifactCodec`](../../../rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifactCodec.java) — public class.
- [`io.rowweave.artifact.PlanArtifactDeploymentBinding`](../../../rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifactDeploymentBinding.java) — public class.
- [`io.rowweave.artifact.PlanArtifactLimits`](../../../rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifactLimits.java) — public class.
- [`io.rowweave.artifact.PlanArtifactValidationException`](../../../rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifactValidationException.java) — public class.
- [`io.rowweave.artifact.PlanArtifactValidator`](../../../rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifactValidator.java) — public class.
- [`io.rowweave.artifact.PortablePlanArtifact`](../../../rowweave-core/src/main/java/io/rowweave/artifact/PortablePlanArtifact.java) — public class.

##### `io.rowweave.cluster`

- [`io.rowweave.cluster.BeatHeartbeat`](../../../rowweave-core/src/main/java/io/rowweave/cluster/BeatHeartbeat.java) — public class.
- [`io.rowweave.cluster.ClusterConfig`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ClusterConfig.java) — public class.
- [`io.rowweave.cluster.ClusterMain`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ClusterMain.java) — public class.
- [`io.rowweave.cluster.ClusterSubmissionPreflight`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ClusterSubmissionPreflight.java) — public class.
- [`io.rowweave.cluster.DataPlaneConfig`](../../../rowweave-core/src/main/java/io/rowweave/cluster/DataPlaneConfig.java) — public class.
- [`io.rowweave.cluster.DurableRunSubmitException`](../../../rowweave-core/src/main/java/io/rowweave/cluster/DurableRunSubmitException.java) — public class.
- [`io.rowweave.cluster.Endpoint`](../../../rowweave-core/src/main/java/io/rowweave/cluster/Endpoint.java) — public class.
- [`io.rowweave.cluster.ExecutionGraph`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ExecutionGraph.java) — public class.
- [`io.rowweave.cluster.ExecutionGraphBuilder`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ExecutionGraphBuilder.java) — public class.
- [`io.rowweave.cluster.FixedMasterLocator`](../../../rowweave-core/src/main/java/io/rowweave/cluster/FixedMasterLocator.java) — public class.
- [`io.rowweave.cluster.Heartbeat`](../../../rowweave-core/src/main/java/io/rowweave/cluster/Heartbeat.java) — public interface.
- [`io.rowweave.cluster.HeartbeatMonitor`](../../../rowweave-core/src/main/java/io/rowweave/cluster/HeartbeatMonitor.java) — public class.
- [`io.rowweave.cluster.InProcessCluster`](../../../rowweave-core/src/main/java/io/rowweave/cluster/InProcessCluster.java) — public class.
- [`io.rowweave.cluster.IncrementalControlMemberInstallCodec`](../../../rowweave-core/src/main/java/io/rowweave/cluster/IncrementalControlMemberInstallCodec.java) — public class.
- [`io.rowweave.cluster.IncrementalSourceActionCommandCodec`](../../../rowweave-core/src/main/java/io/rowweave/cluster/IncrementalSourceActionCommandCodec.java) — public class.
- [`io.rowweave.cluster.IncrementalSourceActionResultCodec`](../../../rowweave-core/src/main/java/io/rowweave/cluster/IncrementalSourceActionResultCodec.java) — public class.
- [`io.rowweave.cluster.JobClient`](../../../rowweave-core/src/main/java/io/rowweave/cluster/JobClient.java) — public interface.
- [`io.rowweave.cluster.JvmCapacityEnvelope`](../../../rowweave-core/src/main/java/io/rowweave/cluster/JvmCapacityEnvelope.java) — public class.
- [`io.rowweave.cluster.LocalCluster`](../../../rowweave-core/src/main/java/io/rowweave/cluster/LocalCluster.java) — public class.
- [`io.rowweave.cluster.LocalWorker`](../../../rowweave-core/src/main/java/io/rowweave/cluster/LocalWorker.java) — public class.
- [`io.rowweave.cluster.Master`](../../../rowweave-core/src/main/java/io/rowweave/cluster/Master.java) — public class.
- [`io.rowweave.cluster.MasterElection`](../../../rowweave-core/src/main/java/io/rowweave/cluster/MasterElection.java) — public class.
- [`io.rowweave.cluster.MasterLocator`](../../../rowweave-core/src/main/java/io/rowweave/cluster/MasterLocator.java) — public interface.
- [`io.rowweave.cluster.MasterServer`](../../../rowweave-core/src/main/java/io/rowweave/cluster/MasterServer.java) — public class.
- [`io.rowweave.cluster.NetUtil`](../../../rowweave-core/src/main/java/io/rowweave/cluster/NetUtil.java) — public class.
- [`io.rowweave.cluster.ParallelismDowngrade`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ParallelismDowngrade.java) — public class.
- [`io.rowweave.cluster.PhysicalPlan`](../../../rowweave-core/src/main/java/io/rowweave/cluster/PhysicalPlan.java) — public class.
- [`io.rowweave.cluster.PreparedStageCommand`](../../../rowweave-core/src/main/java/io/rowweave/cluster/PreparedStageCommand.java) — public class.
- [`io.rowweave.cluster.ProcessBootstrapEnvelope`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ProcessBootstrapEnvelope.java) — public class.
- [`io.rowweave.cluster.ProcessCapacityException`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ProcessCapacityException.java) — public class.
- [`io.rowweave.cluster.ProcessTlsEnvelope`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ProcessTlsEnvelope.java) — public class.
- [`io.rowweave.cluster.RealClusterJobClient`](../../../rowweave-core/src/main/java/io/rowweave/cluster/RealClusterJobClient.java) — public class.
- [`io.rowweave.cluster.RegistryMasterLocator`](../../../rowweave-core/src/main/java/io/rowweave/cluster/RegistryMasterLocator.java) — public class.
- [`io.rowweave.cluster.RegistryRoleElection`](../../../rowweave-core/src/main/java/io/rowweave/cluster/RegistryRoleElection.java) — public class.
- [`io.rowweave.cluster.RegistryRoleLocator`](../../../rowweave-core/src/main/java/io/rowweave/cluster/RegistryRoleLocator.java) — public class.
- [`io.rowweave.cluster.ShuffleGrantPlan`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ShuffleGrantPlan.java) — public class.
- [`io.rowweave.cluster.ShuffleTarget`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ShuffleTarget.java) — public class.
- [`io.rowweave.cluster.ShuffleTransportPlan`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ShuffleTransportPlan.java) — public class.
- [`io.rowweave.cluster.SlotLedger`](../../../rowweave-core/src/main/java/io/rowweave/cluster/SlotLedger.java) — public class.
- [`io.rowweave.cluster.Stage`](../../../rowweave-core/src/main/java/io/rowweave/cluster/Stage.java) — public class.
- [`io.rowweave.cluster.StageAssignment`](../../../rowweave-core/src/main/java/io/rowweave/cluster/StageAssignment.java) — public class.
- [`io.rowweave.cluster.StageBoundary`](../../../rowweave-core/src/main/java/io/rowweave/cluster/StageBoundary.java) — public class.
- [`io.rowweave.cluster.StageBoundaryRebuilder`](../../../rowweave-core/src/main/java/io/rowweave/cluster/StageBoundaryRebuilder.java) — public class.
- [`io.rowweave.cluster.StageCommand`](../../../rowweave-core/src/main/java/io/rowweave/cluster/StageCommand.java) — public class.
- [`io.rowweave.cluster.StageCommandCodecV2`](../../../rowweave-core/src/main/java/io/rowweave/cluster/StageCommandCodecV2.java) — public class.
- [`io.rowweave.cluster.StagePlan`](../../../rowweave-core/src/main/java/io/rowweave/cluster/StagePlan.java) — public class.
- [`io.rowweave.cluster.StageTask`](../../../rowweave-core/src/main/java/io/rowweave/cluster/StageTask.java) — public class.
- [`io.rowweave.cluster.StreamClusterProcessBootstrap`](../../../rowweave-core/src/main/java/io/rowweave/cluster/StreamClusterProcessBootstrap.java) — public class.
- [`io.rowweave.cluster.TransportSecurityBootstrap`](../../../rowweave-core/src/main/java/io/rowweave/cluster/TransportSecurityBootstrap.java) — public class.
- [`io.rowweave.cluster.WorkerServer`](../../../rowweave-core/src/main/java/io/rowweave/cluster/WorkerServer.java) — public class.

##### `io.rowweave.cluster.registry`

- [`io.rowweave.cluster.registry.InProcessRegistry`](../../../rowweave-core/src/main/java/io/rowweave/cluster/registry/InProcessRegistry.java) — public class.
- [`io.rowweave.cluster.registry.RegistryWorker`](../../../rowweave-core/src/main/java/io/rowweave/cluster/registry/RegistryWorker.java) — public class.
- [`io.rowweave.cluster.registry.RegistryWorkerCodec`](../../../rowweave-core/src/main/java/io/rowweave/cluster/registry/RegistryWorkerCodec.java) — public class.
- [`io.rowweave.cluster.registry.ServiceRegistry`](../../../rowweave-core/src/main/java/io/rowweave/cluster/registry/ServiceRegistry.java) — public interface.
- [`io.rowweave.cluster.registry.ServiceRegistryBootstrap`](../../../rowweave-core/src/main/java/io/rowweave/cluster/registry/ServiceRegistryBootstrap.java) — public class.
- [`io.rowweave.cluster.registry.ServiceRegistryProvider`](../../../rowweave-core/src/main/java/io/rowweave/cluster/registry/ServiceRegistryProvider.java) — public interface.

##### `io.rowweave.cluster.rpc`

- [`io.rowweave.cluster.rpc.AuthenticatedRpcPeer`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/AuthenticatedRpcPeer.java) — public class.
- [`io.rowweave.cluster.rpc.ControlAuthzPolicy`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlAuthzPolicy.java) — public class.
- [`io.rowweave.cluster.rpc.ControlEndpointConfig`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlEndpointConfig.java) — public class.
- [`io.rowweave.cluster.rpc.ControlEndpointPolicy`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlEndpointPolicy.java) — public class.
- [`io.rowweave.cluster.rpc.ControlMessageId`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlMessageId.java) — public class.
- [`io.rowweave.cluster.rpc.ControlRequestAuthorization`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlRequestAuthorization.java) — public class.
- [`io.rowweave.cluster.rpc.ControlResidentBudget`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlResidentBudget.java) — public class.
- [`io.rowweave.cluster.rpc.ControlResidentBudgetExceededException`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlResidentBudgetExceededException.java) — public class.
- [`io.rowweave.cluster.rpc.ControlResidentConfig`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlResidentConfig.java) — public class.
- [`io.rowweave.cluster.rpc.ControlResidentLease`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlResidentLease.java) — public class.
- [`io.rowweave.cluster.rpc.ControlResidentOwner`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlResidentOwner.java) — public enum.
- [`io.rowweave.cluster.rpc.ControlRole`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlRole.java) — public enum.
- [`io.rowweave.cluster.rpc.ControlSecurityConfig`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlSecurityConfig.java) — public class.
- [`io.rowweave.cluster.rpc.ControlSecurityProfile`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlSecurityProfile.java) — public enum.
- [`io.rowweave.cluster.rpc.DataPlaneRegistration`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/DataPlaneRegistration.java) — public class.
- [`io.rowweave.cluster.rpc.DataPlaneRegistrationNegotiator`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/DataPlaneRegistrationNegotiator.java) — public class.
- [`io.rowweave.cluster.rpc.DataPlaneSecurityContext`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/DataPlaneSecurityContext.java) — public class.
- [`io.rowweave.cluster.rpc.EphemeralSubmitTimeoutPolicy`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/EphemeralSubmitTimeoutPolicy.java) — public class.
- [`io.rowweave.cluster.rpc.InProcessRpcRegistry`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/InProcessRpcRegistry.java) — public class.
- [`io.rowweave.cluster.rpc.InProcessRpcTransport`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/InProcessRpcTransport.java) — public class.
- [`io.rowweave.cluster.rpc.NegotiatedProtocol`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/NegotiatedProtocol.java) — public class.
- [`io.rowweave.cluster.rpc.ProtocolHelloConfig`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ProtocolHelloConfig.java) — public class.
- [`io.rowweave.cluster.rpc.ProtocolHelloV1`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ProtocolHelloV1.java) — public class.
- [`io.rowweave.cluster.rpc.ProtocolHelloV1Codec`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ProtocolHelloV1Codec.java) — public class.
- [`io.rowweave.cluster.rpc.ProtocolNegotiationException`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ProtocolNegotiationException.java) — public class.
- [`io.rowweave.cluster.rpc.ProtocolNegotiator`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ProtocolNegotiator.java) — public class.
- [`io.rowweave.cluster.rpc.ProtocolRevisionRange`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ProtocolRevisionRange.java) — public class.
- [`io.rowweave.cluster.rpc.RoleAuthenticatedSocketTransport`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RoleAuthenticatedSocketTransport.java) — public class.
- [`io.rowweave.cluster.rpc.RpcAdmissionConfig`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcAdmissionConfig.java) — public class.
- [`io.rowweave.cluster.rpc.RpcClient`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcClient.java) — public class.
- [`io.rowweave.cluster.rpc.RpcCodec`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcCodec.java) — public class.
- [`io.rowweave.cluster.rpc.RpcMessage`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcMessage.java) — public class.
- [`io.rowweave.cluster.rpc.RpcRequestContext`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcRequestContext.java) — public class.
- [`io.rowweave.cluster.rpc.RpcRequestPolicy`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcRequestPolicy.java) — public enum.
- [`io.rowweave.cluster.rpc.RpcRoleEndpoint`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcRoleEndpoint.java) — public interface.
- [`io.rowweave.cluster.rpc.RpcRoleEndpoints`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcRoleEndpoints.java) — public class.
- [`io.rowweave.cluster.rpc.RpcSecurityFence`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcSecurityFence.java) — public class.
- [`io.rowweave.cluster.rpc.RpcSecurityMetadata`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcSecurityMetadata.java) — public class.
- [`io.rowweave.cluster.rpc.RpcServer`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcServer.java) — public class.
- [`io.rowweave.cluster.rpc.RpcTransport`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcTransport.java) — public interface.
- [`io.rowweave.cluster.rpc.RpcTransportCache`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcTransportCache.java) — public class.
- [`io.rowweave.cluster.rpc.RpcTransportLanePool`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcTransportLanePool.java) — public class.
- [`io.rowweave.cluster.rpc.RpcTransports`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcTransports.java) — public class.
- [`io.rowweave.cluster.rpc.WhitelistObjectInputStream`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/WhitelistObjectInputStream.java) — public class.

##### `io.rowweave.commit`

- [`io.rowweave.commit.BatchSinkOutcome`](../../../rowweave-core/src/main/java/io/rowweave/commit/BatchSinkOutcome.java) — public class.
- [`io.rowweave.commit.CommitGenerationAuthority`](../../../rowweave-core/src/main/java/io/rowweave/commit/CommitGenerationAuthority.java) — public interface.
- [`io.rowweave.commit.CommitManifest`](../../../rowweave-core/src/main/java/io/rowweave/commit/CommitManifest.java) — public class.
- [`io.rowweave.commit.FileCommitDomainLease`](../../../rowweave-core/src/main/java/io/rowweave/commit/FileCommitDomainLease.java) — public class.
- [`io.rowweave.commit.FileCommitGenerationAuthority`](../../../rowweave-core/src/main/java/io/rowweave/commit/FileCommitGenerationAuthority.java) — public class.
- [`io.rowweave.commit.FileManifestCommitAuthority`](../../../rowweave-core/src/main/java/io/rowweave/commit/FileManifestCommitAuthority.java) — public class.
- [`io.rowweave.commit.ManifestCleanupAuthority`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCleanupAuthority.java) — public interface.
- [`io.rowweave.commit.ManifestCommitAuthority`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitAuthority.java) — public interface.
- [`io.rowweave.commit.ManifestCommitAuthorityProvider`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitAuthorityProvider.java) — public interface.
- [`io.rowweave.commit.ManifestCommitOutcome`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitOutcome.java) — public class.
- [`io.rowweave.commit.ManifestCommitUncertainException`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitUncertainException.java) — public class.
- [`io.rowweave.commit.ManifestCommitWire`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitWire.java) — public class.
- [`io.rowweave.commit.ManifestFileJanitor`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestFileJanitor.java) — public class.
- [`io.rowweave.commit.ManifestFileRecovery`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestFileRecovery.java) — public class.
- [`io.rowweave.commit.ManifestFileStore`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestFileStore.java) — public class.
- [`io.rowweave.commit.SchemaFingerprint`](../../../rowweave-core/src/main/java/io/rowweave/commit/SchemaFingerprint.java) — public class.

##### `io.rowweave.deployment`

- [`io.rowweave.deployment.DeploymentClassLoader`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentClassLoader.java) — public class.
- [`io.rowweave.deployment.DeploymentManifest`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentManifest.java) — public class.
- [`io.rowweave.deployment.DeploymentManifestCodec`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentManifestCodec.java) — public class.
- [`io.rowweave.deployment.DeploymentMemoryBudget`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentMemoryBudget.java) — public class.
- [`io.rowweave.deployment.DeploymentRuntime`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentRuntime.java) — public class.
- [`io.rowweave.deployment.DeploymentRuntimeConfig`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentRuntimeConfig.java) — public class.
- [`io.rowweave.deployment.DeploymentValidationException`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentValidationException.java) — public class.
- [`io.rowweave.deployment.DurableBatchSinkProviderRegistry`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DurableBatchSinkProviderRegistry.java) — public class.
- [`io.rowweave.deployment.IncrementalSourceProviderRegistry`](../../../rowweave-core/src/main/java/io/rowweave/deployment/IncrementalSourceProviderRegistry.java) — public class.
- [`io.rowweave.deployment.ManifestDurableBatchSinkProvider`](../../../rowweave-core/src/main/java/io/rowweave/deployment/ManifestDurableBatchSinkProvider.java) — public class.
- [`io.rowweave.deployment.PortableFunctionProviderRegistry`](../../../rowweave-core/src/main/java/io/rowweave/deployment/PortableFunctionProviderRegistry.java) — public class.
- [`io.rowweave.deployment.PortableRuntimeBindingRegistry`](../../../rowweave-core/src/main/java/io/rowweave/deployment/PortableRuntimeBindingRegistry.java) — public class.
- [`io.rowweave.deployment.SignedStreamStateMigrationResolver`](../../../rowweave-core/src/main/java/io/rowweave/deployment/SignedStreamStateMigrationResolver.java) — public class.
- [`io.rowweave.deployment.StreamConnectorProviderRegistry`](../../../rowweave-core/src/main/java/io/rowweave/deployment/StreamConnectorProviderRegistry.java) — public class.
- [`io.rowweave.deployment.TrustedSignerRegistry`](../../../rowweave-core/src/main/java/io/rowweave/deployment/TrustedSignerRegistry.java) — public class.
- [`io.rowweave.deployment.VerifiedDeploymentSet`](../../../rowweave-core/src/main/java/io/rowweave/deployment/VerifiedDeploymentSet.java) — public class.

##### `io.rowweave.expr`

- [`io.rowweave.expr.CompiledExpr`](../../../rowweave-core/src/main/java/io/rowweave/expr/CompiledExpr.java) — public interface.
- [`io.rowweave.expr.CompiledExprEngine`](../../../rowweave-core/src/main/java/io/rowweave/expr/CompiledExprEngine.java) — public class.
- [`io.rowweave.expr.Expr`](../../../rowweave-core/src/main/java/io/rowweave/expr/Expr.java) — public class.
- [`io.rowweave.expr.ExprEngine`](../../../rowweave-core/src/main/java/io/rowweave/expr/ExprEngine.java) — public interface.
- [`io.rowweave.expr.ExprEngineFactory`](../../../rowweave-core/src/main/java/io/rowweave/expr/ExprEngineFactory.java) — public class.
- [`io.rowweave.expr.ExprEvalException`](../../../rowweave-core/src/main/java/io/rowweave/expr/ExprEvalException.java) — public class.
- [`io.rowweave.expr.ExprInterpreter`](../../../rowweave-core/src/main/java/io/rowweave/expr/ExprInterpreter.java) — public class.
- [`io.rowweave.expr.ExprParseException`](../../../rowweave-core/src/main/java/io/rowweave/expr/ExprParseException.java) — public class.
- [`io.rowweave.expr.ExprParser`](../../../rowweave-core/src/main/java/io/rowweave/expr/ExprParser.java) — public class.
- [`io.rowweave.expr.ExprRuntime`](../../../rowweave-core/src/main/java/io/rowweave/expr/ExprRuntime.java) — public class.
- [`io.rowweave.expr.ExprSchema`](../../../rowweave-core/src/main/java/io/rowweave/expr/ExprSchema.java) — public class.
- [`io.rowweave.expr.InterpretedExprEngine`](../../../rowweave-core/src/main/java/io/rowweave/expr/InterpretedExprEngine.java) — public class.

##### `io.rowweave.expr.codegen`

- [`io.rowweave.expr.codegen.AstBytecodeGenerator`](../../../rowweave-core/src/main/java/io/rowweave/expr/codegen/AstBytecodeGenerator.java) — public class.
- [`io.rowweave.expr.codegen.UnsupportedExprException`](../../../rowweave-core/src/main/java/io/rowweave/expr/codegen/UnsupportedExprException.java) — public class.

##### `io.rowweave.incremental`

- [`io.rowweave.incremental.CapturedTargetReceipt`](../../../rowweave-core/src/main/java/io/rowweave/incremental/CapturedTargetReceipt.java) — public class.
- [`io.rowweave.incremental.CapturedTargetReceiptCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/CapturedTargetReceiptCodec.java) — public class.
- [`io.rowweave.incremental.FeedGuardShardObservation`](../../../rowweave-core/src/main/java/io/rowweave/incremental/FeedGuardShardObservation.java) — public class.
- [`io.rowweave.incremental.FileIncrementalAuthorityStore`](../../../rowweave-core/src/main/java/io/rowweave/incremental/FileIncrementalAuthorityStore.java) — public class.
- [`io.rowweave.incremental.FrontierConflictException`](../../../rowweave-core/src/main/java/io/rowweave/incremental/FrontierConflictException.java) — public class.
- [`io.rowweave.incremental.FrontierOwnershipClaim`](../../../rowweave-core/src/main/java/io/rowweave/incremental/FrontierOwnershipClaim.java) — public class.
- [`io.rowweave.incremental.FrontierRecoveryToken`](../../../rowweave-core/src/main/java/io/rowweave/incremental/FrontierRecoveryToken.java) — public class.
- [`io.rowweave.incremental.FrontierRecoveryTokenCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/FrontierRecoveryTokenCodec.java) — public class.
- [`io.rowweave.incremental.InMemoryIncrementalAuthorityStore`](../../../rowweave-core/src/main/java/io/rowweave/incremental/InMemoryIncrementalAuthorityStore.java) — public class.
- [`io.rowweave.incremental.IncrementalActiveInterval`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalActiveInterval.java) — public class.
- [`io.rowweave.incremental.IncrementalAttemptCommitPhase`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAttemptCommitPhase.java) — public enum.
- [`io.rowweave.incremental.IncrementalAttemptProgressCommand`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAttemptProgressCommand.java) — public class.
- [`io.rowweave.incremental.IncrementalAuthorityCommand`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAuthorityCommand.java) — public class.
- [`io.rowweave.incremental.IncrementalAuthorityCommandCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAuthorityCommandCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalAuthorityCommandKind`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAuthorityCommandKind.java) — public enum.
- [`io.rowweave.incremental.IncrementalAuthorityDecision`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAuthorityDecision.java) — public class.
- [`io.rowweave.incremental.IncrementalAuthorityGateway`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAuthorityGateway.java) — public interface.
- [`io.rowweave.incremental.IncrementalAuthorityGatewayProvider`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAuthorityGatewayProvider.java) — public interface.
- [`io.rowweave.incremental.IncrementalAuthorityRecord`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAuthorityRecord.java) — public class.
- [`io.rowweave.incremental.IncrementalAuthorityRecordCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAuthorityRecordCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalCaptureRequest`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalCaptureRequest.java) — public class.
- [`io.rowweave.incremental.IncrementalCaptureRequestCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalCaptureRequestCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalCaptureResponse`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalCaptureResponse.java) — public class.
- [`io.rowweave.incremental.IncrementalCaptureResponseCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalCaptureResponseCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalChangelogRowValidator`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalChangelogRowValidator.java) — public class.
- [`io.rowweave.incremental.IncrementalClaimRequest`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalClaimRequest.java) — public class.
- [`io.rowweave.incremental.IncrementalClaimRequestCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalClaimRequestCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalCommitReceipt`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalCommitReceipt.java) — public class.
- [`io.rowweave.incremental.IncrementalControlMember`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalControlMember.java) — public class.
- [`io.rowweave.incremental.IncrementalControlMemberCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalControlMemberCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalDefinition`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalDefinition.java) — public class.
- [`io.rowweave.incremental.IncrementalDefinitionCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalDefinitionCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalFrontier`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontier.java) — public class.
- [`io.rowweave.incremental.IncrementalFrontierAuthorityState`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierAuthorityState.java) — public enum.
- [`io.rowweave.incremental.IncrementalFrontierCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalFrontierOrder`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierOrder.java) — public class.
- [`io.rowweave.incremental.IncrementalFrontierStore`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierStore.java) — public interface.
- [`io.rowweave.incremental.IncrementalFrontierStoreProvider`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierStoreProvider.java) — public interface.
- [`io.rowweave.incremental.IncrementalFrontierView`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierView.java) — public class.
- [`io.rowweave.incremental.IncrementalFrontierViewCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierViewCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalInitializeRequest`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalInitializeRequest.java) — public class.
- [`io.rowweave.incremental.IncrementalInitializeRequestCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalInitializeRequestCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalIntervalIdentity`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalIntervalIdentity.java) — public class.
- [`io.rowweave.incremental.IncrementalIntervalPreparationDecision`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalIntervalPreparationDecision.java) — public class.
- [`io.rowweave.incremental.IncrementalIntervalPreparer`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalIntervalPreparer.java) — public class.
- [`io.rowweave.incremental.IncrementalJobClient`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalJobClient.java) — public class.
- [`io.rowweave.incremental.IncrementalJobResult`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalJobResult.java) — public class.
- [`io.rowweave.incremental.IncrementalJointCommitProofs`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalJointCommitProofs.java) — public class.
- [`io.rowweave.incremental.IncrementalMaterializationResponse`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalMaterializationResponse.java) — public class.
- [`io.rowweave.incremental.IncrementalMaterializationResponseCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalMaterializationResponseCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalMaterializedSplit`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalMaterializedSplit.java) — public class.
- [`io.rowweave.incremental.IncrementalPendingRunDecision`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalPendingRunDecision.java) — public class.
- [`io.rowweave.incremental.IncrementalPlanArtifactMaterializer`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalPlanArtifactMaterializer.java) — public class.
- [`io.rowweave.incremental.IncrementalPlanDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalPlanDescriptor.java) — public class.
- [`io.rowweave.incremental.IncrementalPlanDescriptorCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalPlanDescriptorCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalPlanTemplateFingerprint`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalPlanTemplateFingerprint.java) — public class.
- [`io.rowweave.incremental.IncrementalRecordPosition`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalRecordPosition.java) — public class.
- [`io.rowweave.incremental.IncrementalRecordPositionCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalRecordPositionCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalRecordPositionOrder`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalRecordPositionOrder.java) — public class.
- [`io.rowweave.incremental.IncrementalRunMode`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalRunMode.java) — public enum.
- [`io.rowweave.incremental.IncrementalRunSubmission`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalRunSubmission.java) — public class.
- [`io.rowweave.incremental.IncrementalRunSubmissionCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalRunSubmissionCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalSourceActionBudget`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceActionBudget.java) — public class.
- [`io.rowweave.incremental.IncrementalSourceActionExecutor`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceActionExecutor.java) — public class.
- [`io.rowweave.incremental.IncrementalSourceActions`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceActions.java) — public interface.
- [`io.rowweave.incremental.IncrementalSourceBindingMaterializer`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceBindingMaterializer.java) — public class.
- [`io.rowweave.incremental.IncrementalSourceCodecDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceCodecDescriptor.java) — public class.
- [`io.rowweave.incremental.IncrementalSourceConfig`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceConfig.java) — public class.
- [`io.rowweave.incremental.IncrementalSourceConfigCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceConfigCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalSourceProviderContract`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceProviderContract.java) — public class.
- [`io.rowweave.incremental.IncrementalSourceProviderContractCodec`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceProviderContractCodec.java) — public class.
- [`io.rowweave.incremental.IncrementalSourceProviderKey`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceProviderKey.java) — public class.
- [`io.rowweave.incremental.JdbcIncrementalSourceAccess`](../../../rowweave-core/src/main/java/io/rowweave/incremental/JdbcIncrementalSourceAccess.java) — public interface.
- [`io.rowweave.incremental.JdbcIncrementalSourceProfile`](../../../rowweave-core/src/main/java/io/rowweave/incremental/JdbcIncrementalSourceProfile.java) — public class.
- [`io.rowweave.incremental.JdbcSequenceIncrementalSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/incremental/JdbcSequenceIncrementalSourceProvider.java) — public class.
- [`io.rowweave.incremental.JdbcTimestampKeyIncrementalSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/incremental/JdbcTimestampKeyIncrementalSourceProvider.java) — public class.
- [`io.rowweave.incremental.ObjectManifestIncrementalSourceAccess`](../../../rowweave-core/src/main/java/io/rowweave/incremental/ObjectManifestIncrementalSourceAccess.java) — public interface.
- [`io.rowweave.incremental.ObjectManifestIncrementalSourceProfile`](../../../rowweave-core/src/main/java/io/rowweave/incremental/ObjectManifestIncrementalSourceProfile.java) — public class.
- [`io.rowweave.incremental.ObjectManifestIncrementalSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/incremental/ObjectManifestIncrementalSourceProvider.java) — public class.
- [`io.rowweave.incremental.OrderedFrontierKey`](../../../rowweave-core/src/main/java/io/rowweave/incremental/OrderedFrontierKey.java) — public class.
- [`io.rowweave.incremental.VerifiedIncrementalAbortClosureProof`](../../../rowweave-core/src/main/java/io/rowweave/incremental/VerifiedIncrementalAbortClosureProof.java) — public class.
- [`io.rowweave.incremental.VerifiedIncrementalConflictProof`](../../../rowweave-core/src/main/java/io/rowweave/incremental/VerifiedIncrementalConflictProof.java) — public class.
- [`io.rowweave.incremental.VerifiedIncrementalJointCommitProof`](../../../rowweave-core/src/main/java/io/rowweave/incremental/VerifiedIncrementalJointCommitProof.java) — public class.
- [`io.rowweave.incremental.VerifiedIncrementalPublicationProof`](../../../rowweave-core/src/main/java/io/rowweave/incremental/VerifiedIncrementalPublicationProof.java) — public class.
- [`io.rowweave.incremental.VerifiedIncrementalSinkCommitDecision`](../../../rowweave-core/src/main/java/io/rowweave/incremental/VerifiedIncrementalSinkCommitDecision.java) — public class.

##### `io.rowweave.internal`

- [`io.rowweave.internal.BatchSinkBindings`](../../../rowweave-core/src/main/java/io/rowweave/internal/BatchSinkBindings.java) — public class.
- [`io.rowweave.internal.BatchSourceSplitBindings`](../../../rowweave-core/src/main/java/io/rowweave/internal/BatchSourceSplitBindings.java) — public class.
- [`io.rowweave.internal.IncrementalSourceControlLedger`](../../../rowweave-core/src/main/java/io/rowweave/internal/IncrementalSourceControlLedger.java) — public class.
- [`io.rowweave.internal.PositionedSourceRowStream`](../../../rowweave-core/src/main/java/io/rowweave/internal/PositionedSourceRowStream.java) — public class.
- [`io.rowweave.internal.ProviderActionSupervisor`](../../../rowweave-core/src/main/java/io/rowweave/internal/ProviderActionSupervisor.java) — public class.
- [`io.rowweave.internal.RowSnapshot`](../../../rowweave-core/src/main/java/io/rowweave/internal/RowSnapshot.java) — public class.
- [`io.rowweave.internal.SequentialInputExecutorCursor`](../../../rowweave-core/src/main/java/io/rowweave/internal/SequentialInputExecutorCursor.java) — public class.
- [`io.rowweave.internal.SourceInputSchemas`](../../../rowweave-core/src/main/java/io/rowweave/internal/SourceInputSchemas.java) — public class.

##### `io.rowweave.local`

- [`io.rowweave.local.LocalJobClient`](../../../rowweave-core/src/main/java/io/rowweave/local/LocalJobClient.java) — public class.

##### `io.rowweave.mem`

- [`io.rowweave.mem.Accumulator`](../../../rowweave-core/src/main/java/io/rowweave/mem/Accumulator.java) — public interface.
- [`io.rowweave.mem.AggBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/AggBuffer.java) — public interface.
- [`io.rowweave.mem.CoordinatedExternalSortBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/CoordinatedExternalSortBuffer.java) — public interface.
- [`io.rowweave.mem.EscrowedExternalSortBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/EscrowedExternalSortBuffer.java) — public interface.
- [`io.rowweave.mem.ExecutionMemory`](../../../rowweave-core/src/main/java/io/rowweave/mem/ExecutionMemory.java) — public class.
- [`io.rowweave.mem.ExecutionMemorySnapshot`](../../../rowweave-core/src/main/java/io/rowweave/mem/ExecutionMemorySnapshot.java) — public class.
- [`io.rowweave.mem.ExternalSortBuffers`](../../../rowweave-core/src/main/java/io/rowweave/mem/ExternalSortBuffers.java) — public class.
- [`io.rowweave.mem.ExternalSortEscrow`](../../../rowweave-core/src/main/java/io/rowweave/mem/ExternalSortEscrow.java) — public class.
- [`io.rowweave.mem.GlobalMemoryPool`](../../../rowweave-core/src/main/java/io/rowweave/mem/GlobalMemoryPool.java) — public class.
- [`io.rowweave.mem.GroupAggBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/GroupAggBuffer.java) — public interface.
- [`io.rowweave.mem.HeapGroupAggBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/HeapGroupAggBuffer.java) — public class.
- [`io.rowweave.mem.HeapSortBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/HeapSortBuffer.java) — public class.
- [`io.rowweave.mem.ManagedMemory`](../../../rowweave-core/src/main/java/io/rowweave/mem/ManagedMemory.java) — public class.
- [`io.rowweave.mem.ManagedMemorySnapshot`](../../../rowweave-core/src/main/java/io/rowweave/mem/ManagedMemorySnapshot.java) — public class.
- [`io.rowweave.mem.MemoryBudgetExceededException`](../../../rowweave-core/src/main/java/io/rowweave/mem/MemoryBudgetExceededException.java) — public class.
- [`io.rowweave.mem.MemoryBudgetPolicy`](../../../rowweave-core/src/main/java/io/rowweave/mem/MemoryBudgetPolicy.java) — public class.
- [`io.rowweave.mem.MemoryManager`](../../../rowweave-core/src/main/java/io/rowweave/mem/MemoryManager.java) — public class.
- [`io.rowweave.mem.MemoryOwnerRequirement`](../../../rowweave-core/src/main/java/io/rowweave/mem/MemoryOwnerRequirement.java) — public class.
- [`io.rowweave.mem.MemorySpillable`](../../../rowweave-core/src/main/java/io/rowweave/mem/MemorySpillable.java) — public interface.
- [`io.rowweave.mem.NormalizedKeyComputer`](../../../rowweave-core/src/main/java/io/rowweave/mem/NormalizedKeyComputer.java) — public class.
- [`io.rowweave.mem.OpenAddressingGroupAggBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/OpenAddressingGroupAggBuffer.java) — public class.
- [`io.rowweave.mem.PipelinedExternalSortBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/PipelinedExternalSortBuffer.java) — public class.
- [`io.rowweave.mem.PointerSortBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/PointerSortBuffer.java) — public class.
- [`io.rowweave.mem.ResourceCloser`](../../../rowweave-core/src/main/java/io/rowweave/mem/ResourceCloser.java) — public class.
- [`io.rowweave.mem.RowSerializer`](../../../rowweave-core/src/main/java/io/rowweave/mem/RowSerializer.java) — public class.
- [`io.rowweave.mem.RowSizeEstimator`](../../../rowweave-core/src/main/java/io/rowweave/mem/RowSizeEstimator.java) — public class.
- [`io.rowweave.mem.SlotGuard`](../../../rowweave-core/src/main/java/io/rowweave/mem/SlotGuard.java) — public class.
- [`io.rowweave.mem.SlotSemaphore`](../../../rowweave-core/src/main/java/io/rowweave/mem/SlotSemaphore.java) — public class.
- [`io.rowweave.mem.SortBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/SortBuffer.java) — public interface.
- [`io.rowweave.mem.SortedRunStore`](../../../rowweave-core/src/main/java/io/rowweave/mem/SortedRunStore.java) — public class.
- [`io.rowweave.mem.SpillingSegmentGroupAggBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/SpillingSegmentGroupAggBuffer.java) — public class.
- [`io.rowweave.mem.SpillingSegmentSortBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/SpillingSegmentSortBuffer.java) — public class.
- [`io.rowweave.mem.SpillingSortBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/SpillingSortBuffer.java) — public class.
- [`io.rowweave.mem.TwoTierSortBuffer`](../../../rowweave-core/src/main/java/io/rowweave/mem/TwoTierSortBuffer.java) — public class.
- [`io.rowweave.mem.WorkerConfig`](../../../rowweave-core/src/main/java/io/rowweave/mem/WorkerConfig.java) — public class.

##### `io.rowweave.mem.seg`

- [`io.rowweave.mem.seg.HeapPool`](../../../rowweave-core/src/main/java/io/rowweave/mem/seg/HeapPool.java) — public class.
- [`io.rowweave.mem.seg.MemorySegment`](../../../rowweave-core/src/main/java/io/rowweave/mem/seg/MemorySegment.java) — public interface.
- [`io.rowweave.mem.seg.MemorySegmentPool`](../../../rowweave-core/src/main/java/io/rowweave/mem/seg/MemorySegmentPool.java) — public interface.

##### `io.rowweave.mem.spill`

- [`io.rowweave.mem.spill.BoundedReplayRun`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/BoundedReplayRun.java) — public interface.
- [`io.rowweave.mem.spill.BoundedReplayStore`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/BoundedReplayStore.java) — public interface.
- [`io.rowweave.mem.spill.BudgetedFrameReader`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/BudgetedFrameReader.java) — public interface.
- [`io.rowweave.mem.spill.DiskSpillStore`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/DiskSpillStore.java) — public class.
- [`io.rowweave.mem.spill.EqualKeyBlock`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/EqualKeyBlock.java) — public class.
- [`io.rowweave.mem.spill.MemorySpillStore`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/MemorySpillStore.java) — public class.
- [`io.rowweave.mem.spill.SpillDiskException`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillDiskException.java) — public class.
- [`io.rowweave.mem.spill.SpillFailureCode`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillFailureCode.java) — public enum.
- [`io.rowweave.mem.spill.SpillJobTelemetrySnapshot`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillJobTelemetrySnapshot.java) — public class.
- [`io.rowweave.mem.spill.SpillJobWorkspace`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillJobWorkspace.java) — public class.
- [`io.rowweave.mem.spill.SpillRun`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillRun.java) — public interface.
- [`io.rowweave.mem.spill.SpillRunWriter`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillRunWriter.java) — public class.
- [`io.rowweave.mem.spill.SpillStore`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillStore.java) — public interface.
- [`io.rowweave.mem.spill.SpillWorkspaceConfig`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillWorkspaceConfig.java) — public class.
- [`io.rowweave.mem.spill.SpillWorkspaceManager`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillWorkspaceManager.java) — public class.
- [`io.rowweave.mem.spill.SpillWorkspaces`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillWorkspaces.java) — public class.

##### `io.rowweave.memory`

- [`io.rowweave.memory.ResidentByteArrayOutputStream`](../../../rowweave-core/src/main/java/io/rowweave/memory/ResidentByteArrayOutputStream.java) — public class.
- [`io.rowweave.memory.ResidentObjectInputStream`](../../../rowweave-core/src/main/java/io/rowweave/memory/ResidentObjectInputStream.java) — public class.
- [`io.rowweave.memory.ResidentObjectOutputStreams`](../../../rowweave-core/src/main/java/io/rowweave/memory/ResidentObjectOutputStreams.java) — public class.

##### `io.rowweave.monitor`

- [`io.rowweave.monitor.ArtifactRetentionView`](../../../rowweave-core/src/main/java/io/rowweave/monitor/ArtifactRetentionView.java) — public class.
- [`io.rowweave.monitor.BatchSinkView`](../../../rowweave-core/src/main/java/io/rowweave/monitor/BatchSinkView.java) — public class.
- [`io.rowweave.monitor.InMemoryJobMonitor`](../../../rowweave-core/src/main/java/io/rowweave/monitor/InMemoryJobMonitor.java) — public class.
- [`io.rowweave.monitor.JobMonitorRegistry`](../../../rowweave-core/src/main/java/io/rowweave/monitor/JobMonitorRegistry.java) — public interface.
- [`io.rowweave.monitor.JobView`](../../../rowweave-core/src/main/java/io/rowweave/monitor/JobView.java) — public class.
- [`io.rowweave.monitor.MetricKind`](../../../rowweave-core/src/main/java/io/rowweave/monitor/MetricKind.java) — public enum.
- [`io.rowweave.monitor.MetricSet`](../../../rowweave-core/src/main/java/io/rowweave/monitor/MetricSet.java) — public class.
- [`io.rowweave.monitor.MetricValue`](../../../rowweave-core/src/main/java/io/rowweave/monitor/MetricValue.java) — public class.
- [`io.rowweave.monitor.MonitorBootstrap`](../../../rowweave-core/src/main/java/io/rowweave/monitor/MonitorBootstrap.java) — public class.
- [`io.rowweave.monitor.MonitorLauncher`](../../../rowweave-core/src/main/java/io/rowweave/monitor/MonitorLauncher.java) — public interface.
- [`io.rowweave.monitor.MonitorRuntime`](../../../rowweave-core/src/main/java/io/rowweave/monitor/MonitorRuntime.java) — public class.
- [`io.rowweave.monitor.MonitoredStreamJobClient`](../../../rowweave-core/src/main/java/io/rowweave/monitor/MonitoredStreamJobClient.java) — public class.
- [`io.rowweave.monitor.RetryRecoveryView`](../../../rowweave-core/src/main/java/io/rowweave/monitor/RetryRecoveryView.java) — public class.
- [`io.rowweave.monitor.RunRetentionView`](../../../rowweave-core/src/main/java/io/rowweave/monitor/RunRetentionView.java) — public class.
- [`io.rowweave.monitor.StageView`](../../../rowweave-core/src/main/java/io/rowweave/monitor/StageView.java) — public class.
- [`io.rowweave.monitor.StreamMonitorBridge`](../../../rowweave-core/src/main/java/io/rowweave/monitor/StreamMonitorBridge.java) — public class.
- [`io.rowweave.monitor.StreamMonitorView`](../../../rowweave-core/src/main/java/io/rowweave/monitor/StreamMonitorView.java) — public class.
- [`io.rowweave.monitor.WorkerView`](../../../rowweave-core/src/main/java/io/rowweave/monitor/WorkerView.java) — public class.

##### `io.rowweave.ops`

- [`io.rowweave.ops.AddFields`](../../../rowweave-core/src/main/java/io/rowweave/ops/AddFields.java) — public class.
- [`io.rowweave.ops.CombineGroup`](../../../rowweave-core/src/main/java/io/rowweave/ops/CombineGroup.java) — public class.
- [`io.rowweave.ops.CountAggregator`](../../../rowweave-core/src/main/java/io/rowweave/ops/CountAggregator.java) — public class.
- [`io.rowweave.ops.Distinct`](../../../rowweave-core/src/main/java/io/rowweave/ops/Distinct.java) — public class.
- [`io.rowweave.ops.Filter`](../../../rowweave-core/src/main/java/io/rowweave/ops/Filter.java) — public class.
- [`io.rowweave.ops.FlatMap`](../../../rowweave-core/src/main/java/io/rowweave/ops/FlatMap.java) — public class.
- [`io.rowweave.ops.GroupKeyComparator`](../../../rowweave-core/src/main/java/io/rowweave/ops/GroupKeyComparator.java) — public class.
- [`io.rowweave.ops.Map`](../../../rowweave-core/src/main/java/io/rowweave/ops/Map.java) — public class.
- [`io.rowweave.ops.MapPartition`](../../../rowweave-core/src/main/java/io/rowweave/ops/MapPartition.java) — public class.
- [`io.rowweave.ops.MaxAggregator`](../../../rowweave-core/src/main/java/io/rowweave/ops/MaxAggregator.java) — public class.
- [`io.rowweave.ops.MinAggregator`](../../../rowweave-core/src/main/java/io/rowweave/ops/MinAggregator.java) — public class.
- [`io.rowweave.ops.ObjectStorageQualityRoute`](../../../rowweave-core/src/main/java/io/rowweave/ops/ObjectStorageQualityRoute.java) — public class.
- [`io.rowweave.ops.OrderBy`](../../../rowweave-core/src/main/java/io/rowweave/ops/OrderBy.java) — public class.
- [`io.rowweave.ops.ParallelGroupByAgg`](../../../rowweave-core/src/main/java/io/rowweave/ops/ParallelGroupByAgg.java) — public class.
- [`io.rowweave.ops.Partitioner`](../../../rowweave-core/src/main/java/io/rowweave/ops/Partitioner.java) — public class.
- [`io.rowweave.ops.ReduceGroup`](../../../rowweave-core/src/main/java/io/rowweave/ops/ReduceGroup.java) — public class.
- [`io.rowweave.ops.RowComparator`](../../../rowweave-core/src/main/java/io/rowweave/ops/RowComparator.java) — public class.
- [`io.rowweave.ops.RowHolderAccumulator`](../../../rowweave-core/src/main/java/io/rowweave/ops/RowHolderAccumulator.java) — public class.
- [`io.rowweave.ops.RowValidation`](../../../rowweave-core/src/main/java/io/rowweave/ops/RowValidation.java) — public class.
- [`io.rowweave.ops.SchemaConversion`](../../../rowweave-core/src/main/java/io/rowweave/ops/SchemaConversion.java) — public class.
- [`io.rowweave.ops.Select`](../../../rowweave-core/src/main/java/io/rowweave/ops/Select.java) — public class.
- [`io.rowweave.ops.SortField`](../../../rowweave-core/src/main/java/io/rowweave/ops/SortField.java) — public class.
- [`io.rowweave.ops.SumAggregator`](../../../rowweave-core/src/main/java/io/rowweave/ops/SumAggregator.java) — public class.
- [`io.rowweave.ops.Top`](../../../rowweave-core/src/main/java/io/rowweave/ops/Top.java) — public class.
- [`io.rowweave.ops.TopN`](../../../rowweave-core/src/main/java/io/rowweave/ops/TopN.java) — public class.
- [`io.rowweave.ops.TypedQualitySummary`](../../../rowweave-core/src/main/java/io/rowweave/ops/TypedQualitySummary.java) — public class.
- [`io.rowweave.ops.TypedRowClassification`](../../../rowweave-core/src/main/java/io/rowweave/ops/TypedRowClassification.java) — public class.
- [`io.rowweave.ops.Union`](../../../rowweave-core/src/main/java/io/rowweave/ops/Union.java) — public class.
- [`io.rowweave.ops.WholeTableAgg`](../../../rowweave-core/src/main/java/io/rowweave/ops/WholeTableAgg.java) — public class.

##### `io.rowweave.ops.join`

- [`io.rowweave.ops.join.AutoJoinSelector`](../../../rowweave-core/src/main/java/io/rowweave/ops/join/AutoJoinSelector.java) — public class.
- [`io.rowweave.ops.join.CoGroup`](../../../rowweave-core/src/main/java/io/rowweave/ops/join/CoGroup.java) — public class.
- [`io.rowweave.ops.join.HashJoin`](../../../rowweave-core/src/main/java/io/rowweave/ops/join/HashJoin.java) — public class.
- [`io.rowweave.ops.join.JoinHint`](../../../rowweave-core/src/main/java/io/rowweave/ops/join/JoinHint.java) — public enum.
- [`io.rowweave.ops.join.JoinPhysicalStrategy`](../../../rowweave-core/src/main/java/io/rowweave/ops/join/JoinPhysicalStrategy.java) — public enum.
- [`io.rowweave.ops.join.JoinRuntimeFactory`](../../../rowweave-core/src/main/java/io/rowweave/ops/join/JoinRuntimeFactory.java) — public class.
- [`io.rowweave.ops.join.JoinType`](../../../rowweave-core/src/main/java/io/rowweave/ops/join/JoinType.java) — public enum.
- [`io.rowweave.ops.join.SortMergeJoin`](../../../rowweave-core/src/main/java/io/rowweave/ops/join/SortMergeJoin.java) — public class.

##### `io.rowweave.plan`

- [`io.rowweave.plan.BatchSourcePlanValidator`](../../../rowweave-core/src/main/java/io/rowweave/plan/BatchSourcePlanValidator.java) — public class.
- [`io.rowweave.plan.BudgetStatus`](../../../rowweave-core/src/main/java/io/rowweave/plan/BudgetStatus.java) — public enum.
- [`io.rowweave.plan.DurableSinkProviderPlanPreflight`](../../../rowweave-core/src/main/java/io/rowweave/plan/DurableSinkProviderPlanPreflight.java) — public class.
- [`io.rowweave.plan.ExecutionReport`](../../../rowweave-core/src/main/java/io/rowweave/plan/ExecutionReport.java) — public class.
- [`io.rowweave.plan.ExecutionTelemetrySummary`](../../../rowweave-core/src/main/java/io/rowweave/plan/ExecutionTelemetrySummary.java) — public class.
- [`io.rowweave.plan.ExecutionTelemetrySummaryCodec`](../../../rowweave-core/src/main/java/io/rowweave/plan/ExecutionTelemetrySummaryCodec.java) — public class.
- [`io.rowweave.plan.InputSharding`](../../../rowweave-core/src/main/java/io/rowweave/plan/InputSharding.java) — public class.
- [`io.rowweave.plan.LocalQualityGate`](../../../rowweave-core/src/main/java/io/rowweave/plan/LocalQualityGate.java) — public class.
- [`io.rowweave.plan.MaterializationPlan`](../../../rowweave-core/src/main/java/io/rowweave/plan/MaterializationPlan.java) — public class.
- [`io.rowweave.plan.ObjectStorageQualityPlanValidator`](../../../rowweave-core/src/main/java/io/rowweave/plan/ObjectStorageQualityPlanValidator.java) — public class.
- [`io.rowweave.plan.ObjectTrustContext`](../../../rowweave-core/src/main/java/io/rowweave/plan/ObjectTrustContext.java) — public class.
- [`io.rowweave.plan.OutputCommitValidator`](../../../rowweave-core/src/main/java/io/rowweave/plan/OutputCommitValidator.java) — public class.
- [`io.rowweave.plan.OutputDestinationValidator`](../../../rowweave-core/src/main/java/io/rowweave/plan/OutputDestinationValidator.java) — public class.
- [`io.rowweave.plan.PartitionDistribution`](../../../rowweave-core/src/main/java/io/rowweave/plan/PartitionDistribution.java) — public class.
- [`io.rowweave.plan.PartitionRole`](../../../rowweave-core/src/main/java/io/rowweave/plan/PartitionRole.java) — public enum.
- [`io.rowweave.plan.PlanCompiler`](../../../rowweave-core/src/main/java/io/rowweave/plan/PlanCompiler.java) — public class.
- [`io.rowweave.plan.PlanGraph`](../../../rowweave-core/src/main/java/io/rowweave/plan/PlanGraph.java) — public class.
- [`io.rowweave.plan.PlanGraphEnvelopeScanner`](../../../rowweave-core/src/main/java/io/rowweave/plan/PlanGraphEnvelopeScanner.java) — public class.
- [`io.rowweave.plan.PlanGraphResidentEstimator`](../../../rowweave-core/src/main/java/io/rowweave/plan/PlanGraphResidentEstimator.java) — public class.
- [`io.rowweave.plan.PlanNode`](../../../rowweave-core/src/main/java/io/rowweave/plan/PlanNode.java) — public class.
- [`io.rowweave.plan.PlanOps`](../../../rowweave-core/src/main/java/io/rowweave/plan/PlanOps.java) — public class.
- [`io.rowweave.plan.PushDagExecutor`](../../../rowweave-core/src/main/java/io/rowweave/plan/PushDagExecutor.java) — public class.
- [`io.rowweave.plan.SourcePositionPlanValidator`](../../../rowweave-core/src/main/java/io/rowweave/plan/SourcePositionPlanValidator.java) — public class.
- [`io.rowweave.plan.StageExecutionTelemetry`](../../../rowweave-core/src/main/java/io/rowweave/plan/StageExecutionTelemetry.java) — public class.
- [`io.rowweave.plan.StageExecutionTelemetryAccumulator`](../../../rowweave-core/src/main/java/io/rowweave/plan/StageExecutionTelemetryAccumulator.java) — public class.
- [`io.rowweave.plan.StageExecutionTelemetryCodec`](../../../rowweave-core/src/main/java/io/rowweave/plan/StageExecutionTelemetryCodec.java) — public class.
- [`io.rowweave.plan.TelemetryCoverage`](../../../rowweave-core/src/main/java/io/rowweave/plan/TelemetryCoverage.java) — public enum.
- [`io.rowweave.plan.TrustedObjectCodec`](../../../rowweave-core/src/main/java/io/rowweave/plan/TrustedObjectCodec.java) — public class.
- [`io.rowweave.plan.TrustedObjectInputStream`](../../../rowweave-core/src/main/java/io/rowweave/plan/TrustedObjectInputStream.java) — public class.
- [`io.rowweave.plan.TypedQualityPublicationPlanValidator`](../../../rowweave-core/src/main/java/io/rowweave/plan/TypedQualityPublicationPlanValidator.java) — public class.
- [`io.rowweave.plan.TypedRowClassificationPlanValidator`](../../../rowweave-core/src/main/java/io/rowweave/plan/TypedRowClassificationPlanValidator.java) — public class.

##### `io.rowweave.portable`

- [`io.rowweave.portable.FunctionAbi`](../../../rowweave-core/src/main/java/io/rowweave/portable/FunctionAbi.java) — public enum.
- [`io.rowweave.portable.FunctionDeploymentReference`](../../../rowweave-core/src/main/java/io/rowweave/portable/FunctionDeploymentReference.java) — public class.
- [`io.rowweave.portable.FunctionResourceProfile`](../../../rowweave-core/src/main/java/io/rowweave/portable/FunctionResourceProfile.java) — public class.
- [`io.rowweave.portable.PortableBatchPlan`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableBatchPlan.java) — public class.
- [`io.rowweave.portable.PortableBatchPlanCodec`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableBatchPlanCodec.java) — public class.
- [`io.rowweave.portable.PortableBatchSinkProvider`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableBatchSinkProvider.java) — public interface.
- [`io.rowweave.portable.PortableBatchSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableBatchSourceProvider.java) — public interface.
- [`io.rowweave.portable.PortableBindingLimits`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableBindingLimits.java) — public class.
- [`io.rowweave.portable.PortableBindingVerifier`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableBindingVerifier.java) — public interface.
- [`io.rowweave.portable.PortableConnectorProviderContract`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableConnectorProviderContract.java) — public class.
- [`io.rowweave.portable.PortableConnectorProviderContractCodec`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableConnectorProviderContractCodec.java) — public class.
- [`io.rowweave.portable.PortableDataType`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableDataType.java) — public enum.
- [`io.rowweave.portable.PortableField`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableField.java) — public class.
- [`io.rowweave.portable.PortableFunctionProvider`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableFunctionProvider.java) — public interface.
- [`io.rowweave.portable.PortableFunctionProviderContract`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableFunctionProviderContract.java) — public class.
- [`io.rowweave.portable.PortableFunctionProviderContractCodec`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableFunctionProviderContractCodec.java) — public class.
- [`io.rowweave.portable.PortableNodeBinding`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableNodeBinding.java) — public interface.
- [`io.rowweave.portable.PortableOperatorCode`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableOperatorCode.java) — public enum.
- [`io.rowweave.portable.PortablePlanCompiler`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortablePlanCompiler.java) — public class.
- [`io.rowweave.portable.PortablePlanException`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortablePlanException.java) — public class.
- [`io.rowweave.portable.PortablePlanNode`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortablePlanNode.java) — public class.
- [`io.rowweave.portable.PortableRowSchema`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableRowSchema.java) — public class.
- [`io.rowweave.portable.PortableRuntimeResolver`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableRuntimeResolver.java) — public interface.
- [`io.rowweave.portable.PortableSinkBinding`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableSinkBinding.java) — public class.
- [`io.rowweave.portable.PortableSourceBinding`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortableSourceBinding.java) — public class.

##### `io.rowweave.release`

- [`io.rowweave.release.ReleaseComponentContract`](../../../rowweave-core/src/main/java/io/rowweave/release/ReleaseComponentContract.java) — public class.
- [`io.rowweave.release.ReleaseContractCatalog`](../../../rowweave-core/src/main/java/io/rowweave/release/ReleaseContractCatalog.java) — public class.
- [`io.rowweave.release.ReleaseContractException`](../../../rowweave-core/src/main/java/io/rowweave/release/ReleaseContractException.java) — public class.
- [`io.rowweave.release.ReleaseContractManifest`](../../../rowweave-core/src/main/java/io/rowweave/release/ReleaseContractManifest.java) — public class.
- [`io.rowweave.release.ReleaseContractManifestCodec`](../../../rowweave-core/src/main/java/io/rowweave/release/ReleaseContractManifestCodec.java) — public class.
- [`io.rowweave.release.ReleaseContractSignature`](../../../rowweave-core/src/main/java/io/rowweave/release/ReleaseContractSignature.java) — public class.

##### `io.rowweave.run`

- [`io.rowweave.run.AttemptOutputProfile`](../../../rowweave-core/src/main/java/io/rowweave/run/AttemptOutputProfile.java) — public enum.
- [`io.rowweave.run.AttemptSnapshot`](../../../rowweave-core/src/main/java/io/rowweave/run/AttemptSnapshot.java) — public class.
- [`io.rowweave.run.AttemptStatus`](../../../rowweave-core/src/main/java/io/rowweave/run/AttemptStatus.java) — public enum.
- [`io.rowweave.run.BatchSinkAbortDecision`](../../../rowweave-core/src/main/java/io/rowweave/run/BatchSinkAbortDecision.java) — public class.
- [`io.rowweave.run.BatchSinkAbortRecord`](../../../rowweave-core/src/main/java/io/rowweave/run/BatchSinkAbortRecord.java) — public class.
- [`io.rowweave.run.BatchSinkAbortRecordCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/BatchSinkAbortRecordCodec.java) — public class.
- [`io.rowweave.run.CancellationCause`](../../../rowweave-core/src/main/java/io/rowweave/run/CancellationCause.java) — public enum.
- [`io.rowweave.run.DurableExecutionEvidenceAuthority`](../../../rowweave-core/src/main/java/io/rowweave/run/DurableExecutionEvidenceAuthority.java) — public interface.
- [`io.rowweave.run.DurableExecutionEvidenceAuthorityProvider`](../../../rowweave-core/src/main/java/io/rowweave/run/DurableExecutionEvidenceAuthorityProvider.java) — public interface.
- [`io.rowweave.run.DurableExecutionEvidenceView`](../../../rowweave-core/src/main/java/io/rowweave/run/DurableExecutionEvidenceView.java) — public class.
- [`io.rowweave.run.DurableManifestCommitIntentObservation`](../../../rowweave-core/src/main/java/io/rowweave/run/DurableManifestCommitIntentObservation.java) — public class.
- [`io.rowweave.run.DurableRunDisposition`](../../../rowweave-core/src/main/java/io/rowweave/run/DurableRunDisposition.java) — public enum.
- [`io.rowweave.run.DurableStageAssignmentAuthority`](../../../rowweave-core/src/main/java/io/rowweave/run/DurableStageAssignmentAuthority.java) — public interface.
- [`io.rowweave.run.DurableStageAssignmentAuthorityProvider`](../../../rowweave-core/src/main/java/io/rowweave/run/DurableStageAssignmentAuthorityProvider.java) — public interface.
- [`io.rowweave.run.ExecutionEvent`](../../../rowweave-core/src/main/java/io/rowweave/run/ExecutionEvent.java) — public class.
- [`io.rowweave.run.FileRunStateStore`](../../../rowweave-core/src/main/java/io/rowweave/run/FileRunStateStore.java) — public class.
- [`io.rowweave.run.InMemoryRunStateStore`](../../../rowweave-core/src/main/java/io/rowweave/run/InMemoryRunStateStore.java) — public class.
- [`io.rowweave.run.ManifestCommitIntent`](../../../rowweave-core/src/main/java/io/rowweave/run/ManifestCommitIntent.java) — public class.
- [`io.rowweave.run.ManifestCommitJournalCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/ManifestCommitJournalCodec.java) — public class.
- [`io.rowweave.run.ManifestCommitJournalSnapshot`](../../../rowweave-core/src/main/java/io/rowweave/run/ManifestCommitJournalSnapshot.java) — public class.
- [`io.rowweave.run.ManifestIntentDecision`](../../../rowweave-core/src/main/java/io/rowweave/run/ManifestIntentDecision.java) — public class.
- [`io.rowweave.run.ManifestMemberDecision`](../../../rowweave-core/src/main/java/io/rowweave/run/ManifestMemberDecision.java) — public class.
- [`io.rowweave.run.ManifestMemberReceipt`](../../../rowweave-core/src/main/java/io/rowweave/run/ManifestMemberReceipt.java) — public class.
- [`io.rowweave.run.PreparedSinkDecision`](../../../rowweave-core/src/main/java/io/rowweave/run/PreparedSinkDecision.java) — public class.
- [`io.rowweave.run.PreparedSinkLocator`](../../../rowweave-core/src/main/java/io/rowweave/run/PreparedSinkLocator.java) — public class.
- [`io.rowweave.run.PreparedSinkLocatorCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/PreparedSinkLocatorCodec.java) — public class.
- [`io.rowweave.run.PreparedSinkReceipt`](../../../rowweave-core/src/main/java/io/rowweave/run/PreparedSinkReceipt.java) — public class.
- [`io.rowweave.run.PreparedSinkReceiptCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/PreparedSinkReceiptCodec.java) — public class.
- [`io.rowweave.run.QualityGateAssignmentBinding`](../../../rowweave-core/src/main/java/io/rowweave/run/QualityGateAssignmentBinding.java) — public class.
- [`io.rowweave.run.QualityGateCommitJournalSnapshot`](../../../rowweave-core/src/main/java/io/rowweave/run/QualityGateCommitJournalSnapshot.java) — public class.
- [`io.rowweave.run.RecoverableRunCatalog`](../../../rowweave-core/src/main/java/io/rowweave/run/RecoverableRunCatalog.java) — public interface.
- [`io.rowweave.run.RecoverableRunCatalogProvider`](../../../rowweave-core/src/main/java/io/rowweave/run/RecoverableRunCatalogProvider.java) — public interface.
- [`io.rowweave.run.RecoverableRunPage`](../../../rowweave-core/src/main/java/io/rowweave/run/RecoverableRunPage.java) — public class.
- [`io.rowweave.run.RecoverableRunRef`](../../../rowweave-core/src/main/java/io/rowweave/run/RecoverableRunRef.java) — public class.
- [`io.rowweave.run.RecoveryClaim`](../../../rowweave-core/src/main/java/io/rowweave/run/RecoveryClaim.java) — public class.
- [`io.rowweave.run.RecoveryFencedRunStateStore`](../../../rowweave-core/src/main/java/io/rowweave/run/RecoveryFencedRunStateStore.java) — public interface.
- [`io.rowweave.run.RecoveryFencedStageAssignmentAuthority`](../../../rowweave-core/src/main/java/io/rowweave/run/RecoveryFencedStageAssignmentAuthority.java) — public interface.
- [`io.rowweave.run.RecoveryOwner`](../../../rowweave-core/src/main/java/io/rowweave/run/RecoveryOwner.java) — public class.
- [`io.rowweave.run.RecoveryOwnershipCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/RecoveryOwnershipCodec.java) — public class.
- [`io.rowweave.run.RecoveryToken`](../../../rowweave-core/src/main/java/io/rowweave/run/RecoveryToken.java) — public class.
- [`io.rowweave.run.RetentionOwner`](../../../rowweave-core/src/main/java/io/rowweave/run/RetentionOwner.java) — public class.
- [`io.rowweave.run.RetentionToken`](../../../rowweave-core/src/main/java/io/rowweave/run/RetentionToken.java) — public class.
- [`io.rowweave.run.RetryAttemptClaim`](../../../rowweave-core/src/main/java/io/rowweave/run/RetryAttemptClaim.java) — public class.
- [`io.rowweave.run.RetryAttemptIdentity`](../../../rowweave-core/src/main/java/io/rowweave/run/RetryAttemptIdentity.java) — public class.
- [`io.rowweave.run.RetryReadyEvaluation`](../../../rowweave-core/src/main/java/io/rowweave/run/RetryReadyEvaluation.java) — public class.
- [`io.rowweave.run.RunAttemptFence`](../../../rowweave-core/src/main/java/io/rowweave/run/RunAttemptFence.java) — public class.
- [`io.rowweave.run.RunCancellationAuthority`](../../../rowweave-core/src/main/java/io/rowweave/run/RunCancellationAuthority.java) — public interface.
- [`io.rowweave.run.RunCancellationAuthorityKernel`](../../../rowweave-core/src/main/java/io/rowweave/run/RunCancellationAuthorityKernel.java) — public class.
- [`io.rowweave.run.RunCancellationResult`](../../../rowweave-core/src/main/java/io/rowweave/run/RunCancellationResult.java) — public class.
- [`io.rowweave.run.RunCommand`](../../../rowweave-core/src/main/java/io/rowweave/run/RunCommand.java) — public class.
- [`io.rowweave.run.RunDeadlineAuthority`](../../../rowweave-core/src/main/java/io/rowweave/run/RunDeadlineAuthority.java) — public interface.
- [`io.rowweave.run.RunDeadlineAuthorityKernel`](../../../rowweave-core/src/main/java/io/rowweave/run/RunDeadlineAuthorityKernel.java) — public class.
- [`io.rowweave.run.RunDeadlineResult`](../../../rowweave-core/src/main/java/io/rowweave/run/RunDeadlineResult.java) — public class.
- [`io.rowweave.run.RunDecision`](../../../rowweave-core/src/main/java/io/rowweave/run/RunDecision.java) — public class.
- [`io.rowweave.run.RunExecutionEvent`](../../../rowweave-core/src/main/java/io/rowweave/run/RunExecutionEvent.java) — public class.
- [`io.rowweave.run.RunExecutionEventPage`](../../../rowweave-core/src/main/java/io/rowweave/run/RunExecutionEventPage.java) — public class.
- [`io.rowweave.run.RunExecutionEventPageCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/RunExecutionEventPageCodec.java) — public class.
- [`io.rowweave.run.RunHistoryRetiringException`](../../../rowweave-core/src/main/java/io/rowweave/run/RunHistoryRetiringException.java) — public class.
- [`io.rowweave.run.RunJournalRecord`](../../../rowweave-core/src/main/java/io/rowweave/run/RunJournalRecord.java) — public class.
- [`io.rowweave.run.RunLifecycleCatalog`](../../../rowweave-core/src/main/java/io/rowweave/run/RunLifecycleCatalog.java) — public interface.
- [`io.rowweave.run.RunLifecycleCatalogKernel`](../../../rowweave-core/src/main/java/io/rowweave/run/RunLifecycleCatalogKernel.java) — public class.
- [`io.rowweave.run.RunLifecycleCatalogProvider`](../../../rowweave-core/src/main/java/io/rowweave/run/RunLifecycleCatalogProvider.java) — public interface.
- [`io.rowweave.run.RunLifecyclePage`](../../../rowweave-core/src/main/java/io/rowweave/run/RunLifecyclePage.java) — public class.
- [`io.rowweave.run.RunLifecycleRef`](../../../rowweave-core/src/main/java/io/rowweave/run/RunLifecycleRef.java) — public class.
- [`io.rowweave.run.RunLifecycleState`](../../../rowweave-core/src/main/java/io/rowweave/run/RunLifecycleState.java) — public enum.
- [`io.rowweave.run.RunPurgeProgress`](../../../rowweave-core/src/main/java/io/rowweave/run/RunPurgeProgress.java) — public class.
- [`io.rowweave.run.RunRecoveryCatalogKernel`](../../../rowweave-core/src/main/java/io/rowweave/run/RunRecoveryCatalogKernel.java) — public class.
- [`io.rowweave.run.RunRecoveryOwnership`](../../../rowweave-core/src/main/java/io/rowweave/run/RunRecoveryOwnership.java) — public interface.
- [`io.rowweave.run.RunRecoveryOwnershipProvider`](../../../rowweave-core/src/main/java/io/rowweave/run/RunRecoveryOwnershipProvider.java) — public interface.
- [`io.rowweave.run.RunResultSummary`](../../../rowweave-core/src/main/java/io/rowweave/run/RunResultSummary.java) — public class.
- [`io.rowweave.run.RunRetentionAuthority`](../../../rowweave-core/src/main/java/io/rowweave/run/RunRetentionAuthority.java) — public interface.
- [`io.rowweave.run.RunRetentionAuthorityKernel`](../../../rowweave-core/src/main/java/io/rowweave/run/RunRetentionAuthorityKernel.java) — public class.
- [`io.rowweave.run.RunRetentionAuthorityProvider`](../../../rowweave-core/src/main/java/io/rowweave/run/RunRetentionAuthorityProvider.java) — public interface.
- [`io.rowweave.run.RunRetentionClaim`](../../../rowweave-core/src/main/java/io/rowweave/run/RunRetentionClaim.java) — public class.
- [`io.rowweave.run.RunRetentionCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/RunRetentionCodec.java) — public class.
- [`io.rowweave.run.RunRetentionPolicy`](../../../rowweave-core/src/main/java/io/rowweave/run/RunRetentionPolicy.java) — public class.
- [`io.rowweave.run.RunSnapshot`](../../../rowweave-core/src/main/java/io/rowweave/run/RunSnapshot.java) — public class.
- [`io.rowweave.run.RunStateCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/RunStateCodec.java) — public class.
- [`io.rowweave.run.RunStateStore`](../../../rowweave-core/src/main/java/io/rowweave/run/RunStateStore.java) — public interface.
- [`io.rowweave.run.RunStateStoreKernel`](../../../rowweave-core/src/main/java/io/rowweave/run/RunStateStoreKernel.java) — public class.
- [`io.rowweave.run.RunStateStoreProvider`](../../../rowweave-core/src/main/java/io/rowweave/run/RunStateStoreProvider.java) — public interface.
- [`io.rowweave.run.RunStateStoreUnavailableException`](../../../rowweave-core/src/main/java/io/rowweave/run/RunStateStoreUnavailableException.java) — public class.
- [`io.rowweave.run.RunStatus`](../../../rowweave-core/src/main/java/io/rowweave/run/RunStatus.java) — public enum.
- [`io.rowweave.run.RunSubmission`](../../../rowweave-core/src/main/java/io/rowweave/run/RunSubmission.java) — public class.
- [`io.rowweave.run.RunSubmissionCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/RunSubmissionCodec.java) — public class.
- [`io.rowweave.run.RunSubmissionPolicyValidator`](../../../rowweave-core/src/main/java/io/rowweave/run/RunSubmissionPolicyValidator.java) — public class.
- [`io.rowweave.run.SinkAttemptIdentity`](../../../rowweave-core/src/main/java/io/rowweave/run/SinkAttemptIdentity.java) — public class.
- [`io.rowweave.run.SinkAttemptIdentityCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/SinkAttemptIdentityCodec.java) — public class.
- [`io.rowweave.run.SourceSplitAssignment`](../../../rowweave-core/src/main/java/io/rowweave/run/SourceSplitAssignment.java) — public class.
- [`io.rowweave.run.StageAcceptanceDecision`](../../../rowweave-core/src/main/java/io/rowweave/run/StageAcceptanceDecision.java) — public class.
- [`io.rowweave.run.StageAssignmentAcceptance`](../../../rowweave-core/src/main/java/io/rowweave/run/StageAssignmentAcceptance.java) — public class.
- [`io.rowweave.run.StageAssignmentManifest`](../../../rowweave-core/src/main/java/io/rowweave/run/StageAssignmentManifest.java) — public class.
- [`io.rowweave.run.StageAssignmentManifestCodec`](../../../rowweave-core/src/main/java/io/rowweave/run/StageAssignmentManifestCodec.java) — public class.
- [`io.rowweave.run.StageAssignmentRecord`](../../../rowweave-core/src/main/java/io/rowweave/run/StageAssignmentRecord.java) — public class.
- [`io.rowweave.run.StageAssignmentRole`](../../../rowweave-core/src/main/java/io/rowweave/run/StageAssignmentRole.java) — public enum.
- [`io.rowweave.run.StageAssignmentSnapshot`](../../../rowweave-core/src/main/java/io/rowweave/run/StageAssignmentSnapshot.java) — public class.
- [`io.rowweave.run.StageAssignmentToken`](../../../rowweave-core/src/main/java/io/rowweave/run/StageAssignmentToken.java) — public class.
- [`io.rowweave.run.StageManifestPublication`](../../../rowweave-core/src/main/java/io/rowweave/run/StageManifestPublication.java) — public class.
- [`io.rowweave.run.StageTerminalDecision`](../../../rowweave-core/src/main/java/io/rowweave/run/StageTerminalDecision.java) — public class.
- [`io.rowweave.run.StageTerminalOutcome`](../../../rowweave-core/src/main/java/io/rowweave/run/StageTerminalOutcome.java) — public enum.
- [`io.rowweave.run.StageTerminalReceipt`](../../../rowweave-core/src/main/java/io/rowweave/run/StageTerminalReceipt.java) — public class.
- [`io.rowweave.run.TerminalRunJanitor`](../../../rowweave-core/src/main/java/io/rowweave/run/TerminalRunJanitor.java) — public class.
- [`io.rowweave.run.WholeAttemptRetryAuthority`](../../../rowweave-core/src/main/java/io/rowweave/run/WholeAttemptRetryAuthority.java) — public interface.
- [`io.rowweave.run.WholeAttemptRetryAuthorityKernel`](../../../rowweave-core/src/main/java/io/rowweave/run/WholeAttemptRetryAuthorityKernel.java) — public class.
- [`io.rowweave.run.WholeAttemptRetryClass`](../../../rowweave-core/src/main/java/io/rowweave/run/WholeAttemptRetryClass.java) — public enum.
- [`io.rowweave.run.WholeAttemptRetryDecision`](../../../rowweave-core/src/main/java/io/rowweave/run/WholeAttemptRetryDecision.java) — public class.
- [`io.rowweave.run.WholeAttemptRetryPolicy`](../../../rowweave-core/src/main/java/io/rowweave/run/WholeAttemptRetryPolicy.java) — public class.

##### `io.rowweave.sched`

- [`io.rowweave.sched.BlockingFanout`](../../../rowweave-core/src/main/java/io/rowweave/sched/BlockingFanout.java) — public class.
- [`io.rowweave.sched.Channel`](../../../rowweave-core/src/main/java/io/rowweave/sched/Channel.java) — public interface.
- [`io.rowweave.sched.ChannelPoisonedException`](../../../rowweave-core/src/main/java/io/rowweave/sched/ChannelPoisonedException.java) — public class.
- [`io.rowweave.sched.ConcurrentStages`](../../../rowweave-core/src/main/java/io/rowweave/sched/ConcurrentStages.java) — public class.
- [`io.rowweave.sched.DeadlockException`](../../../rowweave-core/src/main/java/io/rowweave/sched/DeadlockException.java) — public class.
- [`io.rowweave.sched.LocalChannel`](../../../rowweave-core/src/main/java/io/rowweave/sched/LocalChannel.java) — public class.
- [`io.rowweave.sched.LocalChannelSink`](../../../rowweave-core/src/main/java/io/rowweave/sched/LocalChannelSink.java) — public class.
- [`io.rowweave.sched.LocalShuffleRegistry`](../../../rowweave-core/src/main/java/io/rowweave/sched/LocalShuffleRegistry.java) — public class.
- [`io.rowweave.sched.MaterializedRowBuffer`](../../../rowweave-core/src/main/java/io/rowweave/sched/MaterializedRowBuffer.java) — public class.
- [`io.rowweave.sched.MultiProducerChannel`](../../../rowweave-core/src/main/java/io/rowweave/sched/MultiProducerChannel.java) — public class.
- [`io.rowweave.sched.PipelineExecutor`](../../../rowweave-core/src/main/java/io/rowweave/sched/PipelineExecutor.java) — public class.
- [`io.rowweave.sched.Task`](../../../rowweave-core/src/main/java/io/rowweave/sched/Task.java) — public class.

##### `io.rowweave.sched.net`

- [`io.rowweave.sched.net.DataDemuxHandler`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/DataDemuxHandler.java) — public class.
- [`io.rowweave.sched.net.DataInboundAuthorizationRegistry`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/DataInboundAuthorizationRegistry.java) — public class.
- [`io.rowweave.sched.net.DataInboundRegistry`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/DataInboundRegistry.java) — public class.
- [`io.rowweave.sched.net.DataPlaneHandshakeAdmission`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/DataPlaneHandshakeAdmission.java) — public class.
- [`io.rowweave.sched.net.Frame`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/Frame.java) — public class.
- [`io.rowweave.sched.net.FrameCodec`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/FrameCodec.java) — public class.
- [`io.rowweave.sched.net.FrameDecoder`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/FrameDecoder.java) — public class.
- [`io.rowweave.sched.net.FrameEncoder`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/FrameEncoder.java) — public class.
- [`io.rowweave.sched.net.MultiSocketChannelSource`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/MultiSocketChannelSource.java) — public class.
- [`io.rowweave.sched.net.MuxConnectionPool`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/MuxConnectionPool.java) — public class.
- [`io.rowweave.sched.net.OutboundShuffleCohortRegistry`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/OutboundShuffleCohortRegistry.java) — public class.
- [`io.rowweave.sched.net.ShuffleAuthorizationRecord`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/ShuffleAuthorizationRecord.java) — public class.
- [`io.rowweave.sched.net.ShuffleEndpoint`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/ShuffleEndpoint.java) — public class.
- [`io.rowweave.sched.net.ShuffleInboundAuthorization`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/ShuffleInboundAuthorization.java) — public class.
- [`io.rowweave.sched.net.ShuffleIngressAdmissionConfig`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/ShuffleIngressAdmissionConfig.java) — public class.
- [`io.rowweave.sched.net.ShuffleProducerGrant`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/ShuffleProducerGrant.java) — public class.
- [`io.rowweave.sched.net.ShuffleRegistry`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/ShuffleRegistry.java) — public class.
- [`io.rowweave.sched.net.SocketChannelSink`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/SocketChannelSink.java) — public class.
- [`io.rowweave.sched.net.SocketChannelSource`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/SocketChannelSource.java) — public class.
- [`io.rowweave.sched.net.WorkerDataHello`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/WorkerDataHello.java) — public class.
- [`io.rowweave.sched.net.WorkerDataIdentity`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/WorkerDataIdentity.java) — public class.
- [`io.rowweave.sched.net.WorkerDataServer`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/WorkerDataServer.java) — public class.

##### `io.rowweave.sched.stream`

- [`io.rowweave.sched.stream.RowSink`](../../../rowweave-core/src/main/java/io/rowweave/sched/stream/RowSink.java) — public interface.
- [`io.rowweave.sched.stream.RowStream`](../../../rowweave-core/src/main/java/io/rowweave/sched/stream/RowStream.java) — public interface.
- [`io.rowweave.sched.stream.SourceRecordPosition`](../../../rowweave-core/src/main/java/io/rowweave/sched/stream/SourceRecordPosition.java) — public class.
- [`io.rowweave.sched.stream.StreamEvent`](../../../rowweave-core/src/main/java/io/rowweave/sched/stream/StreamEvent.java) — public class.

##### `io.rowweave.stream.api`

- [`io.rowweave.stream.api.StreamAccumulationMode`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamAccumulationMode.java) — public enum.
- [`io.rowweave.stream.api.StreamApiException`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamApiException.java) — public class.
- [`io.rowweave.stream.api.StreamAsyncOrdering`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamAsyncOrdering.java) — public enum.
- [`io.rowweave.stream.api.StreamAsyncReplaySpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamAsyncReplaySpec.java) — public class.
- [`io.rowweave.stream.api.StreamAsyncResourceSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamAsyncResourceSpec.java) — public class.
- [`io.rowweave.stream.api.StreamAsyncWaitSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamAsyncWaitSpec.java) — public class.
- [`io.rowweave.stream.api.StreamBroadcastStateSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamBroadcastStateSpec.java) — public class.
- [`io.rowweave.stream.api.StreamCalcSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamCalcSpec.java) — public class.
- [`io.rowweave.stream.api.StreamCdcPhase`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamCdcPhase.java) — public enum.
- [`io.rowweave.stream.api.StreamChangelogMode`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamChangelogMode.java) — public enum.
- [`io.rowweave.stream.api.StreamCheckpointConfig`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamCheckpointConfig.java) — public class.
- [`io.rowweave.stream.api.StreamCheckpointMode`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamCheckpointMode.java) — public enum.
- [`io.rowweave.stream.api.StreamClusterConfig`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamClusterConfig.java) — public class.
- [`io.rowweave.stream.api.StreamCoProcessSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamCoProcessSpec.java) — public class.
- [`io.rowweave.stream.api.StreamCommandId`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamCommandId.java) — public class.
- [`io.rowweave.stream.api.StreamConnectorConfiguration`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamConnectorConfiguration.java) — public class.
- [`io.rowweave.stream.api.StreamControlException`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamControlException.java) — public class.
- [`io.rowweave.stream.api.StreamDataRowCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamDataRowCodec.java) — public class.
- [`io.rowweave.stream.api.StreamDeliveryGuarantee`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamDeliveryGuarantee.java) — public enum.
- [`io.rowweave.stream.api.StreamEmissionLimit`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamEmissionLimit.java) — public class.
- [`io.rowweave.stream.api.StreamEventTimeSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamEventTimeSpec.java) — public class.
- [`io.rowweave.stream.api.StreamExpression`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamExpression.java) — public class.
- [`io.rowweave.stream.api.StreamExpressionException`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamExpressionException.java) — public class.
- [`io.rowweave.stream.api.StreamField`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamField.java) — public class.
- [`io.rowweave.stream.api.StreamGenericWindowSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamGenericWindowSpec.java) — public class.
- [`io.rowweave.stream.api.StreamJobEvidence`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamJobEvidence.java) — public class.
- [`io.rowweave.stream.api.StreamJobId`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamJobId.java) — public class.
- [`io.rowweave.stream.api.StreamJobProgress`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamJobProgress.java) — public class.
- [`io.rowweave.stream.api.StreamJobStatus`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamJobStatus.java) — public enum.
- [`io.rowweave.stream.api.StreamManagedStateSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamManagedStateSpec.java) — public class.
- [`io.rowweave.stream.api.StreamOperationStatus`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamOperationStatus.java) — public enum.
- [`io.rowweave.stream.api.StreamOutputTag`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamOutputTag.java) — public class.
- [`io.rowweave.stream.api.StreamProcessSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamProcessSpec.java) — public class.
- [`io.rowweave.stream.api.StreamRecoveryPointId`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamRecoveryPointId.java) — public class.
- [`io.rowweave.stream.api.StreamResourceProfile`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamResourceProfile.java) — public class.
- [`io.rowweave.stream.api.StreamRestartPolicy`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamRestartPolicy.java) — public class.
- [`io.rowweave.stream.api.StreamSchema`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamSchema.java) — public class.
- [`io.rowweave.stream.api.StreamSinkSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamSinkSpec.java) — public class.
- [`io.rowweave.stream.api.StreamSourceDiscoverySpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamSourceDiscoverySpec.java) — public class.
- [`io.rowweave.stream.api.StreamSourceSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamSourceSpec.java) — public class.
- [`io.rowweave.stream.api.StreamSplitSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamSplitSpec.java) — public class.
- [`io.rowweave.stream.api.StreamStateSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamStateSpec.java) — public class.
- [`io.rowweave.stream.api.StreamSubmissionId`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamSubmissionId.java) — public class.
- [`io.rowweave.stream.api.StreamTemporalJoinSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamTemporalJoinSpec.java) — public class.
- [`io.rowweave.stream.api.StreamTimeMode`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamTimeMode.java) — public enum.
- [`io.rowweave.stream.api.StreamTimerSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamTimerSpec.java) — public class.
- [`io.rowweave.stream.api.StreamUpsertCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamUpsertCodec.java) — public class.
- [`io.rowweave.stream.api.StreamUpsertValue`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamUpsertValue.java) — public class.
- [`io.rowweave.stream.api.StreamWatermarkPolicy`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamWatermarkPolicy.java) — public class.
- [`io.rowweave.stream.api.StreamWindowAssignerSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamWindowAssignerSpec.java) — public class.
- [`io.rowweave.stream.api.StreamWindowResourceSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamWindowResourceSpec.java) — public class.
- [`io.rowweave.stream.api.StreamWindowSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamWindowSpec.java) — public class.
- [`io.rowweave.stream.api.StreamWindowTriggerSpec`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamWindowTriggerSpec.java) — public class.
- [`io.rowweave.stream.api.StreamWorkerConfig`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamWorkerConfig.java) — public class.
- [`io.rowweave.stream.api.StreamWorkerEndpoint`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamWorkerEndpoint.java) — public class.

##### `io.rowweave.stream.cluster`

- [`io.rowweave.stream.cluster.FileStreamClusterRunAuthority`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/FileStreamClusterRunAuthority.java) — public class.
- [`io.rowweave.stream.cluster.FileStreamProductJobAuthority`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/FileStreamProductJobAuthority.java) — public class.
- [`io.rowweave.stream.cluster.InMemoryStreamClusterRunStateStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/InMemoryStreamClusterRunStateStore.java) — public class.
- [`io.rowweave.stream.cluster.InMemoryStreamProductJobStateStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/InMemoryStreamProductJobStateStore.java) — public class.
- [`io.rowweave.stream.cluster.LeadershipBoundStreamProductRunFactory`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/LeadershipBoundStreamProductRunFactory.java) — public class.
- [`io.rowweave.stream.cluster.LocalStreamRuntimeFactory`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/LocalStreamRuntimeFactory.java) — public class.
- [`io.rowweave.stream.cluster.RealClusterStreamJobClient`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/RealClusterStreamJobClient.java) — public class.
- [`io.rowweave.stream.cluster.RegistryStreamWorkerDirectory`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/RegistryStreamWorkerDirectory.java) — public class.
- [`io.rowweave.stream.cluster.RemoteStreamSourceSinkRunFactory`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/RemoteStreamSourceSinkRunFactory.java) — public class.
- [`io.rowweave.stream.cluster.RemoteStreamWorkerEndpoint`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/RemoteStreamWorkerEndpoint.java) — public class.
- [`io.rowweave.stream.cluster.StreamClusterProtocolException`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterProtocolException.java) — public class.
- [`io.rowweave.stream.cluster.StreamClusterRoles`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterRoles.java) — public class.
- [`io.rowweave.stream.cluster.StreamClusterRunState`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterRunState.java) — public class.
- [`io.rowweave.stream.cluster.StreamClusterRunStateCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterRunStateCodec.java) — public class.
- [`io.rowweave.stream.cluster.StreamClusterRunStateStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterRunStateStore.java) — public interface.
- [`io.rowweave.stream.cluster.StreamClusterRunStateStoreProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterRunStateStoreProvider.java) — public interface.
- [`io.rowweave.stream.cluster.StreamClusterWorker`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterWorker.java) — public class.
- [`io.rowweave.stream.cluster.StreamControlMessage`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamControlMessage.java) — public class.
- [`io.rowweave.stream.cluster.StreamControlMessageCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamControlMessageCodec.java) — public class.
- [`io.rowweave.stream.cluster.StreamFunctionResolver`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamFunctionResolver.java) — public interface.
- [`io.rowweave.stream.cluster.StreamProductControlException`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductControlException.java) — public class.
- [`io.rowweave.stream.cluster.StreamProductJobMasterServer`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductJobMasterServer.java) — public class.
- [`io.rowweave.stream.cluster.StreamProductJobSnapshot`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductJobSnapshot.java) — public class.
- [`io.rowweave.stream.cluster.StreamProductJobSnapshotCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductJobSnapshotCodec.java) — public class.
- [`io.rowweave.stream.cluster.StreamProductJobStateStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductJobStateStore.java) — public interface.
- [`io.rowweave.stream.cluster.StreamProductJobStateStoreProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductJobStateStoreProvider.java) — public interface.
- [`io.rowweave.stream.cluster.StreamProductRun`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductRun.java) — public interface.
- [`io.rowweave.stream.cluster.StreamProductRunFactory`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductRunFactory.java) — public interface.
- [`io.rowweave.stream.cluster.StreamRemoteWorkerServer`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamRemoteWorkerServer.java) — public class.
- [`io.rowweave.stream.cluster.StreamSocketChannel`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamSocketChannel.java) — public class.
- [`io.rowweave.stream.cluster.StreamTaskIdentity`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamTaskIdentity.java) — public class.
- [`io.rowweave.stream.cluster.StreamTaskPlacementRequest`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamTaskPlacementRequest.java) — public class.
- [`io.rowweave.stream.cluster.StreamTaskResourceRequest`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamTaskResourceRequest.java) — public class.
- [`io.rowweave.stream.cluster.StreamWorkerCapabilityPool`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamWorkerCapabilityPool.java) — public class.
- [`io.rowweave.stream.cluster.StreamWorkerCapabilityProfile`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamWorkerCapabilityProfile.java) — public class.
- [`io.rowweave.stream.cluster.StreamWorkerDirectory`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamWorkerDirectory.java) — public interface.
- [`io.rowweave.stream.cluster.StreamWorkerPlacement`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamWorkerPlacement.java) — public class.

##### `io.rowweave.stream.contract`

- [`io.rowweave.stream.contract.CheckpointAuthorityReference`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointAuthorityReference.java) — public class.
- [`io.rowweave.stream.contract.CheckpointDecision`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointDecision.java) — public class.
- [`io.rowweave.stream.contract.CheckpointDecisionCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointDecisionCodec.java) — public class.
- [`io.rowweave.stream.contract.CheckpointEvidence`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointEvidence.java) — public class.
- [`io.rowweave.stream.contract.CheckpointEvidenceKey`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointEvidenceKey.java) — public class.
- [`io.rowweave.stream.contract.CheckpointExpectation`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointExpectation.java) — public class.
- [`io.rowweave.stream.contract.CheckpointExpectedTask`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointExpectedTask.java) — public class.
- [`io.rowweave.stream.contract.CheckpointIdentity`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointIdentity.java) — public class.
- [`io.rowweave.stream.contract.CheckpointManifest`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointManifest.java) — public class.
- [`io.rowweave.stream.contract.CheckpointManifestCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointManifestCodec.java) — public class.
- [`io.rowweave.stream.contract.CheckpointTaskReceipt`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointTaskReceipt.java) — public class.
- [`io.rowweave.stream.contract.PortableStreamJob`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/PortableStreamJob.java) — public class.
- [`io.rowweave.stream.contract.PortableStreamJobArtifact`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/PortableStreamJobArtifact.java) — public class.
- [`io.rowweave.stream.contract.PortableStreamJobCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/PortableStreamJobCodec.java) — public class.
- [`io.rowweave.stream.contract.PortableStreamNode`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/PortableStreamNode.java) — public class.
- [`io.rowweave.stream.contract.PortableStreamPlan`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/PortableStreamPlan.java) — public class.
- [`io.rowweave.stream.contract.PortableStreamPlanArtifact`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/PortableStreamPlanArtifact.java) — public class.
- [`io.rowweave.stream.contract.PortableStreamPlanCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/PortableStreamPlanCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamAsyncBinding`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamAsyncBinding.java) — public class.
- [`io.rowweave.stream.contract.StreamAsyncResourceDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamAsyncResourceDescriptor.java) — public class.
- [`io.rowweave.stream.contract.StreamBroadcastStateDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamBroadcastStateDescriptor.java) — public class.
- [`io.rowweave.stream.contract.StreamCalcBinding`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamCalcBinding.java) — public class.
- [`io.rowweave.stream.contract.StreamCdcEnumeratorState`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamCdcEnumeratorState.java) — public class.
- [`io.rowweave.stream.contract.StreamCdcEnumeratorStateCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamCdcEnumeratorStateCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamChangeException`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamChangeException.java) — public class.
- [`io.rowweave.stream.contract.StreamChangeRecord`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamChangeRecord.java) — public class.
- [`io.rowweave.stream.contract.StreamChangeRecordCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamChangeRecordCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamChannelIdentity`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamChannelIdentity.java) — public class.
- [`io.rowweave.stream.contract.StreamChannelState`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamChannelState.java) — public class.
- [`io.rowweave.stream.contract.StreamChannelStateCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamChannelStateCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamCheckpointMember`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamCheckpointMember.java) — public class.
- [`io.rowweave.stream.contract.StreamCheckpointMemberCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamCheckpointMemberCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamCheckpointRunReference`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamCheckpointRunReference.java) — public class.
- [`io.rowweave.stream.contract.StreamConnectorProviderContract`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamConnectorProviderContract.java) — public class.
- [`io.rowweave.stream.contract.StreamConnectorProviderContractCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamConnectorProviderContractCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamContractException`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamContractException.java) — public class.
- [`io.rowweave.stream.contract.StreamEdge`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamEdge.java) — public class.
- [`io.rowweave.stream.contract.StreamEnvelope`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamEnvelope.java) — public class.
- [`io.rowweave.stream.contract.StreamEnvelopeCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamEnvelopeCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamEnvelopeSequenceGuard`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamEnvelopeSequenceGuard.java) — public class.
- [`io.rowweave.stream.contract.StreamExpressionCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamExpressionCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamFunctionBinding`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamFunctionBinding.java) — public class.
- [`io.rowweave.stream.contract.StreamManagedStateDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamManagedStateDescriptor.java) — public class.
- [`io.rowweave.stream.contract.StreamNodeBinding`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamNodeBinding.java) — public interface.
- [`io.rowweave.stream.contract.StreamNodeExecutionDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamNodeExecutionDescriptor.java) — public class.
- [`io.rowweave.stream.contract.StreamOperatorCode`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamOperatorCode.java) — public enum.
- [`io.rowweave.stream.contract.StreamOutputTagDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamOutputTagDescriptor.java) — public class.
- [`io.rowweave.stream.contract.StreamProcessBinding`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamProcessBinding.java) — public class.
- [`io.rowweave.stream.contract.StreamSavepoint`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSavepoint.java) — public class.
- [`io.rowweave.stream.contract.StreamSavepointCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSavepointCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamSinkBinding`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSinkBinding.java) — public class.
- [`io.rowweave.stream.contract.StreamSourceAssignment`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSourceAssignment.java) — public class.
- [`io.rowweave.stream.contract.StreamSourceBinding`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSourceBinding.java) — public class.
- [`io.rowweave.stream.contract.StreamSourceCoordinatorSplit`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSourceCoordinatorSplit.java) — public class.
- [`io.rowweave.stream.contract.StreamSourceCoordinatorState`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSourceCoordinatorState.java) — public class.
- [`io.rowweave.stream.contract.StreamSourceCoordinatorStateCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSourceCoordinatorStateCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamSourceDiscoveryDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSourceDiscoveryDescriptor.java) — public class.
- [`io.rowweave.stream.contract.StreamSourceSplit`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSourceSplit.java) — public class.
- [`io.rowweave.stream.contract.StreamSplitDiscoveryCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSplitDiscoveryCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamStateDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamStateDescriptor.java) — public class.
- [`io.rowweave.stream.contract.StreamStateMigrationBinding`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamStateMigrationBinding.java) — public class.
- [`io.rowweave.stream.contract.StreamStateMigrationBindingCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamStateMigrationBindingCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamTimeBinding`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamTimeBinding.java) — public class.
- [`io.rowweave.stream.contract.StreamTimerDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamTimerDescriptor.java) — public class.
- [`io.rowweave.stream.contract.StreamUpsertException`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamUpsertException.java) — public class.
- [`io.rowweave.stream.contract.StreamUpsertRecord`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamUpsertRecord.java) — public class.
- [`io.rowweave.stream.contract.StreamUpsertRecordCodec`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamUpsertRecordCodec.java) — public class.
- [`io.rowweave.stream.contract.StreamWindowBinding`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamWindowBinding.java) — public class.
- [`io.rowweave.stream.contract.StreamWindowResourceDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamWindowResourceDescriptor.java) — public class.

##### `io.rowweave.stream.runtime`

- [`io.rowweave.stream.runtime.AlignedCheckpointGate`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/AlignedCheckpointGate.java) — public class.
- [`io.rowweave.stream.runtime.BoundedEventTimeTemporalJoin`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/BoundedEventTimeTemporalJoin.java) — public class.
- [`io.rowweave.stream.runtime.CheckpointTransactionalSink`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/CheckpointTransactionalSink.java) — public class.
- [`io.rowweave.stream.runtime.DurableStreamSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/DurableStreamSourceProvider.java) — public interface.
- [`io.rowweave.stream.runtime.DynamicDurableStreamSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/DynamicDurableStreamSourceProvider.java) — public interface.
- [`io.rowweave.stream.runtime.EagerStreamSinkProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/EagerStreamSinkProvider.java) — public interface.
- [`io.rowweave.stream.runtime.EagerStreamSinkWriter`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/EagerStreamSinkWriter.java) — public interface.
- [`io.rowweave.stream.runtime.FileCheckpointAuthority`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileCheckpointAuthority.java) — public class.
- [`io.rowweave.stream.runtime.FileCheckpointChunkStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileCheckpointChunkStore.java) — public class.
- [`io.rowweave.stream.runtime.FileCheckpointMemberStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileCheckpointMemberStore.java) — public class.
- [`io.rowweave.stream.runtime.FileCheckpointPayloadStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileCheckpointPayloadStore.java) — public class.
- [`io.rowweave.stream.runtime.FileCheckpointRetention`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileCheckpointRetention.java) — public class.
- [`io.rowweave.stream.runtime.FileOfflineRescaleAuthority`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileOfflineRescaleAuthority.java) — public class.
- [`io.rowweave.stream.runtime.FileStreamCheckpointCatalog`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileStreamCheckpointCatalog.java) — public class.
- [`io.rowweave.stream.runtime.FileStreamSavepointStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileStreamSavepointStore.java) — public class.
- [`io.rowweave.stream.runtime.FileStreamStateMigrationAuthority`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileStreamStateMigrationAuthority.java) — public class.
- [`io.rowweave.stream.runtime.FinalEventTimeDecimalWindow`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FinalEventTimeDecimalWindow.java) — public class.
- [`io.rowweave.stream.runtime.InMemoryStreamCheckpointAuthority`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/InMemoryStreamCheckpointAuthority.java) — public class.
- [`io.rowweave.stream.runtime.InMemoryStreamCheckpointCatalog`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/InMemoryStreamCheckpointCatalog.java) — public class.
- [`io.rowweave.stream.runtime.LocalResidentCdcSourceTask`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/LocalResidentCdcSourceTask.java) — public class.
- [`io.rowweave.stream.runtime.LocalStreamChannel`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/LocalStreamChannel.java) — public class.
- [`io.rowweave.stream.runtime.LocalStreamSinkTask`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/LocalStreamSinkTask.java) — public class.
- [`io.rowweave.stream.runtime.LocalStreamSourceTask`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/LocalStreamSourceTask.java) — public class.
- [`io.rowweave.stream.runtime.LocalTransactionalStreamSinkTask`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/LocalTransactionalStreamSinkTask.java) — public class.
- [`io.rowweave.stream.runtime.ManagedKeyedValueState`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/ManagedKeyedValueState.java) — public class.
- [`io.rowweave.stream.runtime.OfflineKeyedStateRescaler`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/OfflineKeyedStateRescaler.java) — public class.
- [`io.rowweave.stream.runtime.PublicStreamSinkAdapter`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/PublicStreamSinkAdapter.java) — public class.
- [`io.rowweave.stream.runtime.PublicStreamSourceAdapter`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/PublicStreamSourceAdapter.java) — public class.
- [`io.rowweave.stream.runtime.PublicTransactionalStreamSinkAdapter`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/PublicTransactionalStreamSinkAdapter.java) — public class.
- [`io.rowweave.stream.runtime.ResidentCdcStreamSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/ResidentCdcStreamSourceProvider.java) — public interface.
- [`io.rowweave.stream.runtime.ResidentStreamCdcEnumerator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/ResidentStreamCdcEnumerator.java) — public class.
- [`io.rowweave.stream.runtime.SignedStreamConnectorAdapters`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/SignedStreamConnectorAdapters.java) — public class.
- [`io.rowweave.stream.runtime.StaticStreamSourceEnumerator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StaticStreamSourceEnumerator.java) — public class.
- [`io.rowweave.stream.runtime.StreamCdcException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCdcException.java) — public class.
- [`io.rowweave.stream.runtime.StreamCdcStatusView`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCdcStatusView.java) — public class.
- [`io.rowweave.stream.runtime.StreamChangeKeyValidator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamChangeKeyValidator.java) — public class.
- [`io.rowweave.stream.runtime.StreamChannelCancelledException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamChannelCancelledException.java) — public class.
- [`io.rowweave.stream.runtime.StreamChannelFailedException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamChannelFailedException.java) — public class.
- [`io.rowweave.stream.runtime.StreamChannelInput`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamChannelInput.java) — public interface.
- [`io.rowweave.stream.runtime.StreamChannelMetrics`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamChannelMetrics.java) — public class.
- [`io.rowweave.stream.runtime.StreamChannelOutput`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamChannelOutput.java) — public interface.
- [`io.rowweave.stream.runtime.StreamCheckpointAuthority`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointAuthority.java) — public interface.
- [`io.rowweave.stream.runtime.StreamCheckpointAuthorityProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointAuthorityProvider.java) — public interface.
- [`io.rowweave.stream.runtime.StreamCheckpointCatalog`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointCatalog.java) — public interface.
- [`io.rowweave.stream.runtime.StreamCheckpointCatalogProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointCatalogProvider.java) — public interface.
- [`io.rowweave.stream.runtime.StreamCheckpointCoordinator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointCoordinator.java) — public class.
- [`io.rowweave.stream.runtime.StreamCheckpointException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointException.java) — public class.
- [`io.rowweave.stream.runtime.StreamSavepointRestoreValidator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSavepointRestoreValidator.java) — public class.
- [`io.rowweave.stream.runtime.StreamSinkOpenRequest`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSinkOpenRequest.java) — public class.
- [`io.rowweave.stream.runtime.StreamSinkTransactionOpenRequest`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSinkTransactionOpenRequest.java) — public class.
- [`io.rowweave.stream.runtime.StreamSourceCheckpointRuntime`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourceCheckpointRuntime.java) — public class.
- [`io.rowweave.stream.runtime.StreamSourceCoordinator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourceCoordinator.java) — public class.
- [`io.rowweave.stream.runtime.StreamSourceEnumeratorOpenRequest`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourceEnumeratorOpenRequest.java) — public class.
- [`io.rowweave.stream.runtime.StreamSourceEvent`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourceEvent.java) — public class.
- [`io.rowweave.stream.runtime.StreamSourceOpenRequest`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourceOpenRequest.java) — public class.
- [`io.rowweave.stream.runtime.StreamSourcePositionCheckpoint`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourcePositionCheckpoint.java) — public class.
- [`io.rowweave.stream.runtime.StreamSourcePositionMigrator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourcePositionMigrator.java) — public class.
- [`io.rowweave.stream.runtime.StreamSourceReader`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourceReader.java) — public interface.
- [`io.rowweave.stream.runtime.StreamSourceRecord`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourceRecord.java) — public class.
- [`io.rowweave.stream.runtime.StreamStateException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamStateException.java) — public class.
- [`io.rowweave.stream.runtime.StreamStateMigrationAdapter`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamStateMigrationAdapter.java) — public interface.
- [`io.rowweave.stream.runtime.StreamStateShadowMigrator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamStateShadowMigrator.java) — public class.
- [`io.rowweave.stream.runtime.StreamTask`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamTask.java) — public class.
- [`io.rowweave.stream.runtime.StreamTemporalJoinException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamTemporalJoinException.java) — public class.
- [`io.rowweave.stream.runtime.StreamTimeException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamTimeException.java) — public class.
- [`io.rowweave.stream.runtime.StreamTimeService`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamTimeService.java) — public class.
- [`io.rowweave.stream.runtime.StreamUpsertKeyValidator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamUpsertKeyValidator.java) — public class.
- [`io.rowweave.stream.runtime.StreamWindowException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamWindowException.java) — public class.
- [`io.rowweave.stream.runtime.TransactionStatus`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/TransactionStatus.java) — public enum.
- [`io.rowweave.stream.runtime.TransactionalStreamSinkProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/TransactionalStreamSinkProvider.java) — public interface.
- [`io.rowweave.stream.runtime.TransactionalStreamSinkWriter`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/TransactionalStreamSinkWriter.java) — public interface.
- [`io.rowweave.stream.runtime.UnalignedCheckpointGate`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/UnalignedCheckpointGate.java) — public class.

##### `io.rowweave.stream.spi`

- [`io.rowweave.stream.spi.StreamCdcCheckpointCompletion`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamCdcCheckpointCompletion.java) — public class.
- [`io.rowweave.stream.spi.StreamCdcSourceContext`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamCdcSourceContext.java) — public class.
- [`io.rowweave.stream.spi.StreamCdcSourceEvent`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamCdcSourceEvent.java) — public class.
- [`io.rowweave.stream.spi.StreamCdcSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamCdcSourceProvider.java) — public interface.
- [`io.rowweave.stream.spi.StreamCdcSourceReader`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamCdcSourceReader.java) — public interface.
- [`io.rowweave.stream.spi.StreamDiscoveryCursor`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamDiscoveryCursor.java) — public class.
- [`io.rowweave.stream.spi.StreamDynamicSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamDynamicSourceProvider.java) — public interface.
- [`io.rowweave.stream.spi.StreamFunctionProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamFunctionProvider.java) — public interface.
- [`io.rowweave.stream.spi.StreamSecretAccess`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSecretAccess.java) — public interface.
- [`io.rowweave.stream.spi.StreamSecretResolver`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSecretResolver.java) — public interface.
- [`io.rowweave.stream.spi.StreamSinkContext`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSinkContext.java) — public class.
- [`io.rowweave.stream.spi.StreamSinkProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSinkProvider.java) — public interface.
- [`io.rowweave.stream.spi.StreamSinkWriter`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSinkWriter.java) — public interface.
- [`io.rowweave.stream.spi.StreamSourceContext`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSourceContext.java) — public class.
- [`io.rowweave.stream.spi.StreamSourceEnumerator`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSourceEnumerator.java) — public interface.
- [`io.rowweave.stream.spi.StreamSourceEnumeratorContext`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSourceEnumeratorContext.java) — public class.
- [`io.rowweave.stream.spi.StreamSourceEvent`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSourceEvent.java) — public class.
- [`io.rowweave.stream.spi.StreamSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSourceProvider.java) — public interface.
- [`io.rowweave.stream.spi.StreamSourceReader`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSourceReader.java) — public interface.
- [`io.rowweave.stream.spi.StreamSourceRecord`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSourceRecord.java) — public class.
- [`io.rowweave.stream.spi.StreamSplitChange`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSplitChange.java) — public class.
- [`io.rowweave.stream.spi.StreamSplitDiscovery`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamSplitDiscovery.java) — public class.
- [`io.rowweave.stream.spi.StreamTransactionStatus`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamTransactionStatus.java) — public enum.
- [`io.rowweave.stream.spi.StreamTransactionalSinkContext`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamTransactionalSinkContext.java) — public class.
- [`io.rowweave.stream.spi.StreamTransactionalSinkProvider`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamTransactionalSinkProvider.java) — public interface.
- [`io.rowweave.stream.spi.StreamTransactionalSinkWriter`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/StreamTransactionalSinkWriter.java) — public interface.

##### `io.rowweave.stream.spi.conformance`

- [`io.rowweave.stream.spi.conformance.StreamConformanceReport`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamConformanceReport.java) — public class.
- [`io.rowweave.stream.spi.conformance.StreamConnectorConformance`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamConnectorConformance.java) — public class.
- [`io.rowweave.stream.spi.conformance.StreamConnectorConformanceException`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamConnectorConformanceException.java) — public class.
- [`io.rowweave.stream.spi.conformance.StreamEagerSinkConformanceFixture`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamEagerSinkConformanceFixture.java) — public interface.
- [`io.rowweave.stream.spi.conformance.StreamSourceConformanceFixture`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamSourceConformanceFixture.java) — public interface.
- [`io.rowweave.stream.spi.conformance.StreamTransactionalSinkConformanceFixture`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamTransactionalSinkConformanceFixture.java) — public interface.

##### `io.rowweave.upgrade`

- [`io.rowweave.upgrade.FileUpgradeAuthority`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/FileUpgradeAuthority.java) — public class.
- [`io.rowweave.upgrade.NamespaceVerification`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/NamespaceVerification.java) — public class.
- [`io.rowweave.upgrade.RecoverableRunUpgradeCensus`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/RecoverableRunUpgradeCensus.java) — public class.
- [`io.rowweave.upgrade.StoreFormatHeader`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/StoreFormatHeader.java) — public class.
- [`io.rowweave.upgrade.StoreFormatHeaderCodec`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/StoreFormatHeaderCodec.java) — public class.
- [`io.rowweave.upgrade.UpgradeActiveWorkCensus`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeActiveWorkCensus.java) — public interface.
- [`io.rowweave.upgrade.UpgradeActiveWorkSnapshot`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeActiveWorkSnapshot.java) — public class.
- [`io.rowweave.upgrade.UpgradeAuthorityKernel`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeAuthorityKernel.java) — public class.
- [`io.rowweave.upgrade.UpgradeAuthorityRecord`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeAuthorityRecord.java) — public class.
- [`io.rowweave.upgrade.UpgradeAuthorityRecordCodec`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeAuthorityRecordCodec.java) — public class.
- [`io.rowweave.upgrade.UpgradeAuthorityStore`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeAuthorityStore.java) — public interface.
- [`io.rowweave.upgrade.UpgradeAuthorityStoreProvider`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeAuthorityStoreProvider.java) — public interface.
- [`io.rowweave.upgrade.UpgradeCommand`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeCommand.java) — public class.
- [`io.rowweave.upgrade.UpgradeCommandEvidence`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeCommandEvidence.java) — public class.
- [`io.rowweave.upgrade.UpgradeObjectEvidence`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeObjectEvidence.java) — public class.
- [`io.rowweave.upgrade.UpgradeObjectEvidenceCodec`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeObjectEvidenceCodec.java) — public class.
- [`io.rowweave.upgrade.UpgradeOwner`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeOwner.java) — public class.
- [`io.rowweave.upgrade.UpgradePhase`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradePhase.java) — public enum.
- [`io.rowweave.upgrade.UpgradeRuntimeCoordinator`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeRuntimeCoordinator.java) — public class.
- [`io.rowweave.upgrade.UpgradeTransition`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeTransition.java) — public class.
- [`io.rowweave.upgrade.UpgradeWriteAdmission`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeWriteAdmission.java) — public class.
- [`io.rowweave.upgrade.WriterRevisionDecision`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/WriterRevisionDecision.java) — public class.
- [`io.rowweave.upgrade.WriterRevisionGate`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/WriterRevisionGate.java) — public class.
- [`io.rowweave.upgrade.WriterRevisionRejectedException`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/WriterRevisionRejectedException.java) — public class.

#### `rowweave-pipeline`


##### `io.rowweave.pipeline`

- [`io.rowweave.pipeline.FileVersionedDatasetAuthorityRecordStore`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/FileVersionedDatasetAuthorityRecordStore.java) — public class.
- [`io.rowweave.pipeline.FileVersionedDatasetHandoffAuthority`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/FileVersionedDatasetHandoffAuthority.java) — public class.
- [`io.rowweave.pipeline.InMemoryVersionedDatasetHandoffAuthority`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/InMemoryVersionedDatasetHandoffAuthority.java) — public class.
- [`io.rowweave.pipeline.PipelineArtifactAdapter`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineArtifactAdapter.java) — public class.
- [`io.rowweave.pipeline.PipelineCompiler`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineCompiler.java) — public class.
- [`io.rowweave.pipeline.PipelineDefinition`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineDefinition.java) — public class.
- [`io.rowweave.pipeline.PipelineDefinitionBuilder`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineDefinitionBuilder.java) — public class.
- [`io.rowweave.pipeline.PipelineDefinitionCodec`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineDefinitionCodec.java) — public class.
- [`io.rowweave.pipeline.PipelineEdge`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineEdge.java) — public class.
- [`io.rowweave.pipeline.PipelineExecutionPlan`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineExecutionPlan.java) — public class.
- [`io.rowweave.pipeline.PipelineExecutionUnit`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineExecutionUnit.java) — public class.
- [`io.rowweave.pipeline.PipelineExplainer`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineExplainer.java) — public class.
- [`io.rowweave.pipeline.PipelineLimits`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineLimits.java) — public class.
- [`io.rowweave.pipeline.PipelineOutput`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineOutput.java) — public class.
- [`io.rowweave.pipeline.PipelineStep`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineStep.java) — public class.
- [`io.rowweave.pipeline.PipelineValidationException`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineValidationException.java) — public class.
- [`io.rowweave.pipeline.PipelineValidator`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineValidator.java) — public class.
- [`io.rowweave.pipeline.StoreBackedVersionedDatasetHandoffAuthority`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/StoreBackedVersionedDatasetHandoffAuthority.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetAuthorityRecord`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetAuthorityRecord.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetAuthorityRecordStore`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetAuthorityRecordStore.java) — public interface.
- [`io.rowweave.pipeline.VersionedDatasetAuthorityRecordStoreConformance`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetAuthorityRecordStoreConformance.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetAuthorityRecordStoreConformanceFixture`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetAuthorityRecordStoreConformanceFixture.java) — public interface.
- [`io.rowweave.pipeline.VersionedDatasetAuthorityRecordStoreConformanceReport`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetAuthorityRecordStoreConformanceReport.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetCommitEvidence`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetCommitEvidence.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetConsumerBinding`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetConsumerBinding.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetConsumerRequest`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetConsumerRequest.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetHandoffAuthority`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffAuthority.java) — public interface.
- [`io.rowweave.pipeline.VersionedDatasetHandoffCodec`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffCodec.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetHandoffException`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffException.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetHandoffKey`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffKey.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetHandoffLimits`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffLimits.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetHandoffSdk`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffSdk.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetProducerAttempt`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetProducerAttempt.java) — public class.
- [`io.rowweave.pipeline.VersionedDatasetVisibleWinner`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetVisibleWinner.java) — public class.

#### `rowweave-connector-s3`


##### `io.rowweave.connector.s3`

- [`io.rowweave.connector.s3.S3ConnectorConfiguration`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorConfiguration.java) — public class.
- [`io.rowweave.connector.s3.S3ConnectorContract`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorContract.java) — public class.
- [`io.rowweave.connector.s3.S3ConnectorProvider`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorProvider.java) — public class.
- [`io.rowweave.connector.s3.S3CredentialCodec`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3CredentialCodec.java) — public class.
- [`io.rowweave.connector.s3.S3Credentials`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3Credentials.java) — public class.
- [`io.rowweave.connector.s3.S3DeploymentPackager`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3DeploymentPackager.java) — public class.
- [`io.rowweave.connector.s3.S3Manifest`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3Manifest.java) — public class.
- [`io.rowweave.connector.s3.S3ManifestPlanner`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ManifestPlanner.java) — public class.
- [`io.rowweave.connector.s3.S3ObjectDescriptor`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ObjectDescriptor.java) — public class.
- [`io.rowweave.connector.s3.S3ObjectDescriptorCodec`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ObjectDescriptorCodec.java) — public class.
- [`io.rowweave.connector.s3.S3ObjectFormat`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ObjectFormat.java) — public enum.
- [`io.rowweave.connector.s3.S3SourcePosition`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3SourcePosition.java) — public class.
- [`io.rowweave.connector.s3.S3SourcePositionCodec`](../../../rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3SourcePositionCodec.java) — public class.

#### `rowweave-connector-postgres-cdc`


##### `io.rowweave.connector.postgres`

- [`io.rowweave.connector.postgres.PostgresCdcPosition`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCdcPosition.java) — public class.
- [`io.rowweave.connector.postgres.PostgresCdcPositionCodec`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCdcPositionCodec.java) — public class.
- [`io.rowweave.connector.postgres.PostgresColumnBinding`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresColumnBinding.java) — public class.
- [`io.rowweave.connector.postgres.PostgresConnectorConfiguration`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorConfiguration.java) — public class.
- [`io.rowweave.connector.postgres.PostgresConnectorContract`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorContract.java) — public class.
- [`io.rowweave.connector.postgres.PostgresConnectorProvider`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorProvider.java) — public class.
- [`io.rowweave.connector.postgres.PostgresCredentialCodec`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCredentialCodec.java) — public class.
- [`io.rowweave.connector.postgres.PostgresCredentials`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCredentials.java) — public class.
- [`io.rowweave.connector.postgres.PostgresDeploymentPackager`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresDeploymentPackager.java) — public class.
- [`io.rowweave.connector.postgres.PostgresReplicaIdentity`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresReplicaIdentity.java) — public enum.
- [`io.rowweave.connector.postgres.PostgresTableBinding`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresTableBinding.java) — public class.
- [`io.rowweave.connector.postgres.PostgresTlsMode`](../../../rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresTlsMode.java) — public enum.

#### `rowweave-registry-zk`


##### `io.rowweave.cluster.registry.zk`

- [`io.rowweave.cluster.registry.zk.ZkIncrementalFrontierStore`](../../../rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkIncrementalFrontierStore.java) — public class.
- [`io.rowweave.cluster.registry.zk.ZkRemoteJobClient`](../../../rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkRemoteJobClient.java) — public class.
- [`io.rowweave.cluster.registry.zk.ZkServiceRegistry`](../../../rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkServiceRegistry.java) — public class.
- [`io.rowweave.cluster.registry.zk.ZkServiceRegistryProvider`](../../../rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkServiceRegistryProvider.java) — public class.
- [`io.rowweave.cluster.registry.zk.ZkStreamJobClient`](../../../rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkStreamJobClient.java) — public class.
- [`io.rowweave.cluster.registry.zk.ZkUpgradeAuthority`](../../../rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkUpgradeAuthority.java) — public class.

##### `io.rowweave.incremental`

- [`io.rowweave.incremental.ZkIncrementalAbortProofAuthority`](../../../rowweave-registry-zk/src/main/java/io/rowweave/incremental/ZkIncrementalAbortProofAuthority.java) — public class.

##### `io.rowweave.stream.cluster`

- [`io.rowweave.stream.cluster.ZkStreamClusterRunStateStore`](../../../rowweave-registry-zk/src/main/java/io/rowweave/stream/cluster/ZkStreamClusterRunStateStore.java) — public class.
- [`io.rowweave.stream.cluster.ZkStreamProductJobStateStore`](../../../rowweave-registry-zk/src/main/java/io/rowweave/stream/cluster/ZkStreamProductJobStateStore.java) — public class.

##### `io.rowweave.stream.runtime`

- [`io.rowweave.stream.runtime.ZkStreamCheckpointAuthority`](../../../rowweave-registry-zk/src/main/java/io/rowweave/stream/runtime/ZkStreamCheckpointAuthority.java) — public class.
- [`io.rowweave.stream.runtime.ZkStreamCheckpointCatalog`](../../../rowweave-registry-zk/src/main/java/io/rowweave/stream/runtime/ZkStreamCheckpointCatalog.java) — public class.

#### `rowweave-monitor`


##### `io.rowweave.monitor.http`

- [`io.rowweave.monitor.http.MonitorHttpServer`](../../../rowweave-monitor/src/main/java/io/rowweave/monitor/http/MonitorHttpServer.java) — public class.
- [`io.rowweave.monitor.http.NettyMonitorLauncher`](../../../rowweave-monitor/src/main/java/io/rowweave/monitor/http/NettyMonitorLauncher.java) — public class.

#### `rowweave-examples`


##### `io.rowweave.examples`

- [`io.rowweave.examples.AmountRollupExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/AmountRollupExample.java) — public class.
- [`io.rowweave.examples.BatchFirstJobExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/BatchFirstJobExample.java) — public class.
- [`io.rowweave.examples.GroupStatExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/GroupStatExample.java) — public class.
- [`io.rowweave.examples.JoinDimEnrichExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/JoinDimEnrichExample.java) — public class.
- [`io.rowweave.examples.PipelineCompositionExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/PipelineCompositionExample.java) — public class.
- [`io.rowweave.examples.RunModesExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/RunModesExample.java) — public class.
- [`io.rowweave.examples.StreamAtomicCompositionExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamAtomicCompositionExample.java) — public class.
- [`io.rowweave.examples.StreamClusterCdcExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamClusterCdcExample.java) — public class.
- [`io.rowweave.examples.StreamConnectorConformanceExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamConnectorConformanceExample.java) — public class.
- [`io.rowweave.examples.StreamElasticExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamElasticExample.java) — public class.
- [`io.rowweave.examples.StreamLocalAppendExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamLocalAppendExample.java) — public class.
- [`io.rowweave.examples.StreamLocalEventTimeRecoveryExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamLocalEventTimeRecoveryExample.java) — public class.
- [`io.rowweave.examples.WordCountExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/WordCountExample.java) — public class.

#### `rowweave-soak`


##### `io.rowweave.soak`

- [`io.rowweave.soak.ManagedMemorySoakMain`](../../../rowweave-soak/src/main/java/io/rowweave/soak/ManagedMemorySoakMain.java) — public class.
- [`io.rowweave.soak.StreamSoakEvidenceLog`](../../../rowweave-soak/src/main/java/io/rowweave/soak/StreamSoakEvidenceLog.java) — public class.
- [`io.rowweave.soak.StreamSoakMain`](../../../rowweave-soak/src/main/java/io/rowweave/soak/StreamSoakMain.java) — public class.
- [`io.rowweave.soak.StreamSoakManifest`](../../../rowweave-soak/src/main/java/io/rowweave/soak/StreamSoakManifest.java) — public class.
- [`io.rowweave.soak.StreamSoakSecretResolver`](../../../rowweave-soak/src/main/java/io/rowweave/soak/StreamSoakSecretResolver.java) — public class.

## Traceability and qualification

The inventory generator checks source declarations, functional rows, the frozen class fixture, and method owners on every documentation run. The Java contract test additionally verifies that every approved class exists and every approved signature remains binary-present.

Run both gates from the repository root:

```bash
python3 tools/oss_api_inventory_gate.py
mvn -q -pl rowweave-core test -Dtest=PublicApiVocabularyContractTest
```
<!-- GENERATED:PUBLIC-API-INVENTORY:END -->
