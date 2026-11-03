#!/usr/bin/env bash
# Qualify every indexed example and the documented connector consumer journeys.
#
# The closing line of this gate is an evidence claim, so it only states what was
# actually proven: each number comes from a failsafe or surefire summary written
# by this run. A report that predates the run, a journey that was skipped, and a
# Java 8 label without a resolved JDK 8 are all failures rather than silence.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() {
    printf 'EXAMPLE QUALIFICATION: FAIL %s\n' "$1" >&2
    exit 1
}

for command_name in mvn java diff grep find; do
    command -v "$command_name" >/dev/null 2>&1 || fail "missing $command_name"
done

work_root="$(mktemp -d "${TMPDIR:-/tmp}/rowweave-example-qualification.XXXXXXXX")"
qual_sources="$(mktemp "${TMPDIR:-/tmp}/rowweave-example-sources.XXXXXX")"
qual_documented="$(mktemp "${TMPDIR:-/tmp}/rowweave-example-documented.XXXXXX")"
# Every public example source must remain discoverable from documentation.
for source in rowweave-examples/src/main/java/io/rowweave/examples/*Example.java; do
    [[ -f "$source" ]] || fail "no example sources found"
    basename "$source" .java
done | sort -u > "$qual_sources"
[[ -s "$qual_sources" ]] || fail "no example sources found"
grep -RhoE '\b[A-Z][A-Za-z0-9]*Example\b' docs README.md | sort -u > "$qual_documented"
diff -u "$qual_sources" "$qual_documented" >/dev/null \
    || fail "example source and documentation inventories differ"

example_count="$(wc -l < "$qual_sources" | tr -d ' ')"
printf 'EXAMPLE QUALIFICATION: inventory PASS (%s public examples)\n' "$example_count"

# Install every module once so the example and connector journeys run against the
# published reactor artifacts, including the signed connector deployment images.
mvn -q -DskipTests -Djacoco.skip=true install

cleanup() {
    rm -f "$qual_sources" "$qual_documented"
    find "$work_root" -depth -mindepth 1 -delete 2>/dev/null || true
    rmdir "$work_root" 2>/dev/null || true
}
trap cleanup EXIT

# One Java 8 contract for every claim made below. A consumer home is only
# accepted when it can actually compile, so a Java 8 runtime cannot be traded
# for the host JDK while still reporting Java 8 evidence.
# shellcheck source=tools/java8-home.sh
source tools/java8-home.sh
java8_evidence=""
if rowweave_resolve_java8_home; then
    java8_evidence="$ROWWEAVE_RESOLVED_JAVA8_HOME"
    export ROWWEAVE_JAVA8_HOME="$java8_evidence"
    export JAVA8_HOME="$java8_evidence"
    printf 'EXAMPLE QUALIFICATION: Java 8 JDK resolved (%s)\n' "$java8_evidence"
else
    printf 'EXAMPLE QUALIFICATION: no Java 8 JDK resolved; reasons:\n'
    rowweave_java8_home_reasons | sed 's/^/  /'
    printf 'EXAMPLE QUALIFICATION: nothing below is claimed as Java 8 evidence\n'
fi

# rowweave.java8.required makes the consumer ITs fail instead of skip, so a
# journey reported as Java 8 evidence cannot quietly have been a skip.
if [[ -n "$java8_evidence" ]]; then
    java8_properties=(-Drowweave.java8.home="$java8_evidence" -Drowweave.java8.required=true)
else
    java8_properties=()
fi

# Reports are read from target/, which survives an install without clean. This
# marker pins every accepted report to this run.
qualification_marker="$work_root/qualification-start"
touch "$qualification_marker"

# Summarize one failsafe or surefire report written by this run: echoes the
# number of tests actually executed, failing on anything else.
report_executed() {
    local report="$1" line run skipped
    [[ -f "$report" ]] || fail "missing test report: $report"
    [[ "$report" -nt "$qualification_marker" ]] \
        || fail "test report predates this run: $report"
    line="$(grep -m1 'Tests run:' "$report")" \
        || fail "test report has no summary: $report"
    run="${line#*Tests run: }"; run="${run%%,*}"
    skipped="${line##*Skipped: }"; skipped="${skipped%%,*}"
    [[ "$run" =~ ^[0-9]+$ && "$skipped" =~ ^[0-9]+$ ]] \
        || fail "unreadable summary in $report: $line"
    [[ "$skipped" -eq 0 ]] \
        || fail "$report skipped $skipped of $run test(s)"
    printf '%s' "$run"
}

# Every indexed example test plus the published-jar consumer ITs, which are the
# black-box proof that an outside project can build against the packaged JARs.
mvn -q -pl rowweave-examples "${java8_properties[@]}" verify
for report in \
    rowweave-examples/target/failsafe-reports/*PublishedPipelineJarConsumerIT.txt \
    rowweave-examples/target/failsafe-reports/*PublishedStreamJarConsumerIT.txt; do
    report_executed "$report" >/dev/null
done
printf 'EXAMPLE QUALIFICATION: published-jar consumer ITs PASS (2 executed)\n'

# Build the exact classpath shown in the example index.
mvn -q -pl rowweave-examples dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
example_classpath="rowweave-examples/target/classes:$(cat rowweave-examples/target/classpath.txt)"

mode_root="$work_root/run-modes"
mkdir -p "$mode_root"
java -Djava.io.tmpdir="$mode_root" -cp "$example_classpath" \
    io.rowweave.examples.RunModesExample > "$work_root/run-modes.log"
for expected in \
    'Local result:       [C1=300.0000000000, C2=100.0000000000, C3=300.0000000000]' \
    'In-process result:  [C1=300.0000000000, C2=100.0000000000, C3=300.0000000000]' \
    'Equivalent result sets: true'; do
    grep -Fxq "$expected" "$work_root/run-modes.log" || fail "RunModes output mismatch"
done
printf 'EXAMPLE QUALIFICATION: RunModes command PASS (Local + InProcess)\n'

bash tools/verify-map-partition-jar-consumer.sh
bash tools/verify-stream-connector-jar-consumer.sh

mvn -q -pl rowweave-connector-s3 \
    -Dtest='S3ConnectorProviderConformanceTest' \
    -Dit.test='S3DeploymentArtifactIT,S3PublishedJava8ConsumerIT' \
    -DfailIfNoTests=false "${java8_properties[@]}" verify
mvn -q -pl rowweave-connector-postgres-cdc \
    -Dtest='PostgresConnectorModelTest,PostgresCredentialCodecTest' \
    -Dit.test='PostgresDeploymentArtifactIT,PostgresPublishedJava8ConsumerIT' \
    -DfailIfNoTests=false "${java8_properties[@]}" verify

java8_journeys=0
for report in \
    rowweave-connector-s3/target/failsafe-reports/*PublishedJava8ConsumerIT.txt \
    rowweave-connector-postgres-cdc/target/failsafe-reports/*PublishedJava8ConsumerIT.txt; do
    report_executed "$report" >/dev/null
    java8_journeys=$((java8_journeys + 1))
done
for report in \
    rowweave-connector-s3/target/failsafe-reports/*DeploymentArtifactIT.txt \
    rowweave-connector-postgres-cdc/target/failsafe-reports/*DeploymentArtifactIT.txt; do
    report_executed "$report" >/dev/null
done
printf 'EXAMPLE QUALIFICATION: connector deployment images PASS (2 executed)\n'

if [[ -n "$java8_evidence" ]]; then
    [[ "$java8_journeys" -eq 2 ]] \
        || fail "Java 8 evidence claimed but $java8_journeys of 2 journeys ran"
    printf 'EXAMPLE QUALIFICATION: PASS (%s examples, RunModes CLI, 2 connector deployment images, 2 Java 8 consumer journeys verified on %s)\n' \
        "$example_count" "$java8_evidence"
else
    printf 'EXAMPLE QUALIFICATION: PASS (%s examples, RunModes CLI, 2 connector deployment images, 0 Java 8 consumer journeys)\n' \
        "$example_count"
fi
