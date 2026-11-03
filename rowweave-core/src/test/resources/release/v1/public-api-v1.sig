# RowWeave public v1 binary floor. Additions are compatible; changing/removing an entry is not.
TYPE io.rowweave.release.ReleaseContractManifest
TYPE io.rowweave.release.ReleaseComponentContract
TYPE io.rowweave.release.ReleaseContractSignature
METHOD io.rowweave.release.ReleaseContractCatalog#publicV1Baseline (Ljava/lang/String;)Lio/rowweave/release/ReleaseContractManifest;
METHOD io.rowweave.release.ReleaseContractSignature#verify ([B[BLjava/security/PublicKey;)Lio/rowweave/release/ReleaseContractManifest;
TYPE io.rowweave.artifact.PlanArtifact
TYPE io.rowweave.artifact.PortablePlanArtifact
TYPE io.rowweave.portable.PortableBatchPlan
FIELD io.rowweave.artifact.PlanArtifact#FEATURE_PORTABLE_BATCH_PLAN_V1 Ljava/lang/String;
FIELD io.rowweave.artifact.PlanArtifact#FEATURE_BOUNDED_INCREMENTAL_FRONTIER Ljava/lang/String;
FIELD io.rowweave.portable.PortableBatchPlan#BATCH_CAPABILITY Ljava/lang/String;
METHOD io.rowweave.artifact.PortablePlanArtifact#wrap (Ljava/lang/String;Lio/rowweave/portable/PortableBatchPlan;)Lio/rowweave/artifact/PlanArtifact;
METHOD io.rowweave.artifact.PortablePlanArtifact#decodeValidated (Lio/rowweave/artifact/PlanArtifact;)Lio/rowweave/portable/PortableBatchPlan;
METHOD io.rowweave.artifact.PlanArtifact#definitionId ()Ljava/lang/String;
METHOD io.rowweave.artifact.PlanArtifact#artifactId ()Ljava/lang/String;
METHOD io.rowweave.artifact.PlanArtifact#payloadFormat ()Lio/rowweave/artifact/PlanArtifact$PayloadFormat;
METHOD io.rowweave.artifact.PlanArtifact#payload ()[B
METHOD io.rowweave.portable.PortableBatchPlan#requiredCapabilities ()Ljava/util/List;
METHOD io.rowweave.portable.PortableBatchPlan#nodes ()Ljava/util/List;
METHOD io.rowweave.portable.PortableBatchPlan#rootSinkOperatorIds ()Ljava/util/List;
TYPE io.rowweave.cluster.RealClusterJobClient
CTOR io.rowweave.cluster.RealClusterJobClient#<init> (Ljava/lang/String;I)V
METHOD io.rowweave.cluster.RealClusterJobClient#submitRun (Lio/rowweave/artifact/PlanArtifact;Lio/rowweave/run/RunSubmission;)Lio/rowweave/plan/ExecutionReport;
METHOD io.rowweave.cluster.RealClusterJobClient#submitIncrementalRun (Lio/rowweave/incremental/IncrementalRunSubmission;)Lio/rowweave/plan/ExecutionReport;
METHOD io.rowweave.cluster.RealClusterJobClient#queryRun (Ljava/lang/String;)Lio/rowweave/run/RunSnapshot;
METHOD io.rowweave.cluster.RealClusterJobClient#queryRunEvents (Ljava/lang/String;JI)Lio/rowweave/run/RunExecutionEventPage;
METHOD io.rowweave.cluster.RealClusterJobClient#cancelRun (Ljava/lang/String;Ljava/lang/String;)Lio/rowweave/run/RunCancellationResult;
METHOD io.rowweave.cluster.RealClusterJobClient#queryBatchSinkOutcome (Ljava/lang/String;)Lio/rowweave/commit/BatchSinkOutcome;
METHOD io.rowweave.cluster.RealClusterJobClient#queryQualityGateDecision (Ljava/lang/String;)Lio/rowweave/QualityGateDecision;
METHOD io.rowweave.cluster.RealClusterJobClient#queryIncrementalFrontier (Ljava/lang/String;)Lio/rowweave/incremental/IncrementalFrontierView;
METHOD io.rowweave.cluster.RealClusterJobClient#close ()V
TYPE io.rowweave.run.RunSubmission
CTOR io.rowweave.run.RunSubmission#<init> (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;IJJ)V
METHOD io.rowweave.run.RunSubmission#runId ()Ljava/lang/String;
METHOD io.rowweave.run.RunSubmission#attemptId ()Ljava/lang/String;
METHOD io.rowweave.run.RunSubmission#publicationId ()Ljava/lang/String;
TYPE io.rowweave.run.RunSnapshot
METHOD io.rowweave.run.RunSnapshot#status ()Lio/rowweave/run/RunStatus;
METHOD io.rowweave.run.RunSnapshot#revision ()J
METHOD io.rowweave.run.RunSnapshot#currentAttempt ()Lio/rowweave/run/AttemptSnapshot;
METHOD io.rowweave.run.RunSnapshot#resultSummary ()Lio/rowweave/run/RunResultSummary;
TYPE io.rowweave.run.RunExecutionEventPage
METHOD io.rowweave.run.RunExecutionEventPage#events ()Ljava/util/List;
METHOD io.rowweave.run.RunExecutionEventPage#nextAfterSequence ()J
METHOD io.rowweave.run.RunExecutionEventPage#hasMore ()Z
TYPE io.rowweave.commit.BatchSinkOutcome
METHOD io.rowweave.commit.BatchSinkOutcome#state ()Lio/rowweave/commit/BatchSinkOutcome$State;
METHOD io.rowweave.commit.BatchSinkOutcome#generation ()Ljava/lang/Long;
TYPE io.rowweave.QualityGateDecision
METHOD io.rowweave.QualityGateDecision#outcome ()Lio/rowweave/QualityGateDecision$Outcome;
METHOD io.rowweave.QualityGateDecision#failureCodes ()Ljava/util/List;
TYPE io.rowweave.incremental.IncrementalRunSubmission
METHOD io.rowweave.incremental.IncrementalRunSubmission#normal (Ljava/lang/String;Ljava/lang/String;Lio/rowweave/incremental/IncrementalRunMode;Ljava/lang/String;Ljava/lang/String;IJJLio/rowweave/incremental/IncrementalDefinition;Lio/rowweave/artifact/PlanArtifact;Lio/rowweave/incremental/IncrementalSourceProviderContract;Lio/rowweave/incremental/IncrementalSourceConfig;)Lio/rowweave/incremental/IncrementalRunSubmission;
TYPE io.rowweave.incremental.IncrementalFrontierView
METHOD io.rowweave.incremental.IncrementalFrontierView#feedId ()Ljava/lang/String;
METHOD io.rowweave.incremental.IncrementalFrontierView#revision ()J
METHOD io.rowweave.incremental.IncrementalFrontierView#state ()Ljava/lang/String;
TYPE io.rowweave.DurableBatchSinkProvider
METHOD io.rowweave.DurableBatchSinkProvider#contract ()Lio/rowweave/DurableBatchSinkProviderContract;
METHOD io.rowweave.DurableBatchSinkProvider#openWriteAttempt (Lio/rowweave/ProviderBatchSink;Lio/rowweave/run/SinkAttemptIdentity;Lio/rowweave/DurableSinkActionContext;)Lio/rowweave/DurableBatchSinkWriteAttempt;
TYPE io.rowweave.DurableBatchSinkProviderContract
METHOD io.rowweave.DurableBatchSinkProviderContract#providerKey ()Lio/rowweave/DurableSinkProviderKey;
METHOD io.rowweave.DurableBatchSinkProviderContract#attemptBudget ()Lio/rowweave/DurableSinkAttemptBudget;
TYPE io.rowweave.incremental.IncrementalSourceProviderContract
METHOD io.rowweave.incremental.IncrementalSourceProviderContract#providerKey ()Lio/rowweave/incremental/IncrementalSourceProviderKey;
METHOD io.rowweave.incremental.IncrementalSourceProviderContract#frontierKind ()Lio/rowweave/incremental/IncrementalFrontier$Kind;
METHOD io.rowweave.incremental.IncrementalSourceProviderContract#actionBudget ()Lio/rowweave/incremental/IncrementalSourceActionBudget;
TYPE io.rowweave.portable.PortableFunctionProviderContract
METHOD io.rowweave.portable.PortableFunctionProviderContract#validateReference (Lio/rowweave/portable/FunctionDeploymentReference;)V
TYPE io.rowweave.portable.PortableConnectorProviderContract
METHOD io.rowweave.portable.PortableConnectorProviderContract#providerKey ()Ljava/lang/String;
TYPE io.rowweave.portable.PortableBindingVerifier
METHOD io.rowweave.portable.PortableBindingVerifier#verifySource (Lio/rowweave/portable/PortableSourceBinding;)V
METHOD io.rowweave.portable.PortableBindingVerifier#verifyFunction (Lio/rowweave/portable/FunctionDeploymentReference;)V
METHOD io.rowweave.portable.PortableBindingVerifier#verifySink (Lio/rowweave/portable/PortableSinkBinding;)V
