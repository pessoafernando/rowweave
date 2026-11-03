#!/usr/bin/env python3
"""Formats, limits, and SPI reference parity gate.

Checks the eight externally visible categories required by the reference
stage, their sole authority routes, source anchors, constant presence, and
test-file existence. Opaque fixed-width magics remain owned by the format
constants page and are forbidden in this page.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/product/reference/formats-limits-spi.md"

REQUIRED_SECTIONS = (
    "Portable artifact and Pipeline limits",
    "Application data limits",
    "Stream checkpoint state formats",
    "Rejected-row record",
    "Object-storage manifest contract",
    "PostgreSQL CDC frontier and connector limits",
    "Provider and conformance SPI",
)

AUTHORITIES = {
    "Opaque format tags and magic families": "format-constants.md",
    "Shuffle socket behavior and certified transport limits": "data-plane-limits.md",
    "Portable artifact and Pipeline numeric limits": "this page",
    "Stream checkpoint state shapes and bounds": "this page",
    "Rejected-row record shape": "this page",
    "Object-storage manifest contract": "this page",
    "PostgreSQL CDC frontier and connector limits": "this page",
    "Provider and conformance SPI": "this page",
    "Pipeline versioned composition and handoff refusals": "pipeline-boundary.md",
    "Release compatibility and frozen protocol policy": "versioned-release-v1.md",
}

SOURCE_CONSTANTS = {
    "PlanArtifactLimits.java": [
        "MAX_TEXT_BYTES", "MAX_SCHEMA_FINGERPRINT_BYTES", "MAX_FEATURES",
        "MAX_OPERATORS", "MAX_OPERATOR_INPUTS", "MAX_PAYLOAD_BYTES",
        "MAX_BODY_BYTES",
    ],
    "PipelineLimits.java": [
        "MAX_ID_BYTES", "MAX_FEATURE_BYTES", "MAX_FEATURES", "MAX_STEPS",
        "MAX_EDGES", "MAX_OUTPUTS", "MAX_BODY_BYTES", "SHA256_BYTES",
    ],
    "VersionedDatasetHandoffLimits.java": [
        "MAX_ID_BYTES", "MAX_READER_BINDING_BYTES", "MAX_RECORD_BYTES",
    ],
    "CompatibilityDataLimits.java": [
        "DEFAULT_MAX_ROWS", "DEFAULT_MAX_BYTES", "HARD_MAX_ROWS",
        "HARD_MAX_BYTES",
    ],
    "MapPartitionEmissionLimit.java": [
        "MAX_OUTPUT_ROWS_PER_PARTITION", "MAX_OUTPUT_BYTES_PER_PARTITION",
    ],
    "StreamSourcePositionCheckpoint.java": [
        "VERSION", "MAX_TEXT_BYTES", "MAX_POSITION_FRAME_BYTES",
    ],
    "CheckpointManifestCodec.java": [
        "HEADER_BYTES", "DIGEST_BYTES", "MAX_BODY_BYTES", "MAX_TASK_RECEIPTS",
        "MAX_FRAME_BYTES",
    ],
    "CheckpointDecisionCodec.java": ["MAX_BODY_BYTES"],
    "FileCheckpointChunkStore.java": [
        "MAX_CHUNK_BYTES", "MAX_CHUNKS", "MAX_MANIFEST_BODY_BYTES",
    ],
    "FileCheckpointMemberStore.java": ["MAX_MEMBER_FRAME_BYTES"],
    "ObjectStorageQualityRecordDecoder.java": [
        "VALID_TAG", "REJECTED_TAG", "RULE_ID",
    ],
    "QualityGateDecisionCodec.java": [
        "MAX_FRAME_BYTES", "MAX_EXEMPLAR_SECTION_BYTES",
    ],
    "QualityGatePolicy.java": [
        "CURRENT_VERSION", "MAX_REASON_THRESHOLDS", "MAX_EXEMPLARS",
    ],
    "S3Manifest.java": ["MAX_OBJECTS"],
    "S3ManifestPlanner.java": ["PAGE_KEYS", "BUFFER_BYTES"],
    "S3ConnectorConfiguration.java": [
        "MAX_OBJECT_BYTES", "MIN_ROW_BYTES", "MAX_ROW_BYTES",
        "MAX_PAYLOAD_BYTES",
    ],
    "S3CredentialCodec.java": [
        "MAX_ACCESS_KEY_BYTES", "MAX_SECRET_KEY_BYTES",
        "MAX_SESSION_TOKEN_BYTES", "MAX_FRAME_BYTES",
    ],
    "PostgresCdcPositionCodec.java": ["CODEC_ID", "MAX_FRAME_BYTES"],
    "PostgresConnectorConfiguration.java": [
        "MAX_TABLES", "MAX_TOTAL_COLUMNS", "MAX_CONFIGURATION_BYTES",
        "MAX_TRANSACTION_CHANGES", "MAX_TRANSACTION_BYTES",
        "MAX_RETAINED_WAL_BYTES",
    ],
    "PostgresTableBinding.java": ["MAX_COLUMNS"],
    "PostgresCredentialCodec.java": [
        "MAX_USERNAME_BYTES", "MAX_PASSWORD_BYTES", "MAX_FRAME_BYTES",
    ],
    "StreamConnectorConformance.java": ["MAX_FRAME_BYTES"],
}

SOURCE_LINKS = {
    "PlanArtifactLimits.java": "rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifactLimits.java",
    "PipelineLimits.java": "rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineLimits.java",
    "VersionedDatasetHandoffLimits.java": "rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffLimits.java",
    "CompatibilityDataLimits.java": "rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimits.java",
    "MapPartitionEmissionLimit.java": "rowweave-core/src/main/java/io/rowweave/MapPartitionEmissionLimit.java",
    "StreamSourcePositionCheckpoint.java": "rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSourcePositionCheckpoint.java",
    "CheckpointManifestCodec.java": "rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointManifestCodec.java",
    "CheckpointDecisionCodec.java": "rowweave-core/src/main/java/io/rowweave/stream/contract/CheckpointDecisionCodec.java",
    "FileCheckpointChunkStore.java": "rowweave-core/src/main/java/io/rowweave/stream/runtime/FileCheckpointChunkStore.java",
    "FileCheckpointMemberStore.java": "rowweave-core/src/main/java/io/rowweave/stream/runtime/FileCheckpointMemberStore.java",
    "ObjectStorageQualityRecordDecoder.java": "rowweave-core/src/main/java/io/rowweave/ObjectStorageQualityRecordDecoder.java",
    "QualityReasonKey.java": "rowweave-core/src/main/java/io/rowweave/QualityReasonKey.java",
    "TypedQualityManifest.java": "rowweave-core/src/main/java/io/rowweave/TypedQualityManifest.java",
    "QualityGateDecisionCodec.java": "rowweave-core/src/main/java/io/rowweave/QualityGateDecisionCodec.java",
    "QualityGatePolicy.java": "rowweave-core/src/main/java/io/rowweave/QualityGatePolicy.java",
    "S3Manifest.java": "rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3Manifest.java",
    "S3ManifestPlanner.java": "rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ManifestPlanner.java",
    "S3ConnectorConfiguration.java": "rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3ConnectorConfiguration.java",
    "S3CredentialCodec.java": "rowweave-connector-s3/src/main/java/io/rowweave/connector/s3/S3CredentialCodec.java",
    "PostgresCdcPosition.java": "rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCdcPosition.java",
    "PostgresCdcPositionCodec.java": "rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCdcPositionCodec.java",
    "PostgresConnectorConfiguration.java": "rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresConnectorConfiguration.java",
    "PostgresTableBinding.java": "rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresTableBinding.java",
    "PostgresCredentialCodec.java": "rowweave-connector-postgres-cdc/src/main/java/io/rowweave/connector/postgres/PostgresCredentialCodec.java",
    "StreamConnectorConformance.java": "rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamConnectorConformance.java",
    "DurableBatchSinkProviderConformance.java": "rowweave-core/src/test/java/io/rowweave/sink/DurableBatchSinkProviderConformance.java",
    "VersionedDatasetAuthorityRecordStoreConformance.java": "rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetAuthorityRecordStoreConformance.java",
}

REQUIRED_TESTS = (
    "PlanArtifactModelTest", "PipelineResourceBoundaryTest",
    "PipelineDefinitionCodecTest", "VersionedDatasetHandoffCodecTest",
    "CompatibilityDataLimitsTest", "DataSetOutputLimitsTest",
    "MapPartitionEmissionLimitTest", "StreamSourcePositionCheckpointTest",
    "CheckpointMemberContractTest", "StreamCheckpointCoordinatorTest",
    "CheckpointDecisionContractTest", "FileCheckpointChunkStoreTest",
    "FileCheckpointMemberStoreTest", "ObjectStorageQualityBoundsLifecycleTest",
    "TypedQualityManifestModelTest", "QualityEvidenceReadGuardTest",
    "QualityGateDecisionCodecTest", "QualityGatePolicyTest",
    "S3ManifestPlannerTest", "S3ConnectorProviderConformanceTest",
    "S3CredentialCodecTest", "PostgresCdcPositionCodecTest",
    "StreamConnectorConfigurationTest", "PostgresCredentialCodecTest",
    "StreamBoundedSourceConformanceKitTest",
    "StreamConnectorConformanceKitTest",
    "DurableBatchSinkProviderConformanceRedTest",
    "ProviderClusterConformanceHarness", "SignedProviderClusterTest",
    "VersionedDatasetAuthorityRecordStoreConformanceTest",
    "MuxConnectionPoolFanoutTest",
)


def sections(body: str) -> dict[str, str]:
    result: dict[str, str] = {}
    current: str | None = None
    buffer: list[str] = []
    for line in body.splitlines():
        heading = re.match(r"^##\s+(.+?)\s*$", line)
        if heading:
            if current:
                result[current] = "\n".join(buffer)
            current = heading.group(1)
            buffer = []
        elif current:
            buffer.append(line)
    if current:
        result[current] = "\n".join(buffer)
    return result


def test_exists(name: str) -> bool:
    roots = (
        "rowweave-core/src/test/java", "rowweave-pipeline/src/test/java",
        "rowweave-connector-s3/src/test/java",
        "rowweave-connector-postgres-cdc/src/test/java",
        "rowweave-registry-zk/src/test/java",
    )
    return any((ROOT / root).rglob(f"{name}.java") for root in roots)


def main() -> int:
    try:
        body = DOC.read_text(encoding="utf-8")
        failures = []
        if not re.search(r"^#\s+Formats, limits, and SPI reference\s*$", body, re.M):
            failures.append("missing page title")
        parts = sections(body)
        for heading in REQUIRED_SECTIONS:
            if heading not in parts:
                failures.append(f"missing section: {heading}")
        for label, route in AUTHORITIES.items():
            row = next((line for line in body.splitlines()
                        if f"| {label} |" in line), None)
            if row is None or f"]({route})" not in row and f"| {route} |" not in row:
                failures.append(f"authority route missing: {label} -> {route}")
        if re.search(r"\bRWX{1,2}[A-Z0-9]{4,}\b|\b0x5257", body, re.I):
            failures.append("opaque fixed-width magic explained outside format-constants")
        for filename, constants in SOURCE_CONSTANTS.items():
            relative = SOURCE_LINKS[filename]
            source_path = ROOT / relative
            if not source_path.is_file():
                failures.append(f"source missing: {relative}")
                continue
            text = source_path.read_text(encoding="utf-8", errors="replace")
            for constant in constants:
                if not re.search(rf"\b{re.escape(constant)}\b", text):
                    failures.append(f"{filename}: source constant absent: {constant}")
            if f"](../../../{relative})" not in body:
                failures.append(f"source link absent: {relative}")
        for test in REQUIRED_TESTS:
            if f"`{test}`" not in body:
                failures.append(f"test evidence absent: {test}")
            if not test_exists(test):
                failures.append(f"test source missing: {test}")
        if failures:
            print(f"FORMATS LIMITS SPI PARITY: FAIL ({len(failures)} problem(s))")
            for failure in failures:
                print(f"- {failure}")
            return 1
        constants = sum(len(values) for values in SOURCE_CONSTANTS.values())
        print(
            f"FORMATS LIMITS SPI PARITY: PASS ({len(REQUIRED_SECTIONS)} sections, "
            f"{len(AUTHORITIES)} authority routes, {len(SOURCE_LINKS)} source files, "
            f"{constants} source constants, {len(REQUIRED_TESTS)} test classes)"
        )
        return 0
    except (OSError, ValueError) as failure:
        print(f"FORMATS LIMITS SPI PARITY: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
