#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
core_jar="${1:-$repository_root/rowweave-core/target/rowweave-core-0.2.0.jar}"
consumer_source="$repository_root/tools/map-partition-jar-consumer/ExternalMapPartitionConsumer.java"

if [[ ! -f "$core_jar" ]]; then
    printf 'Missing published core JAR: %s\n' "$core_jar" >&2
    exit 1
fi

# shellcheck source=tools/java8-home.sh
source "$repository_root/tools/java8-home.sh"
gate_directory="$(mktemp -d "${TMPDIR:-/tmp}/rowweave-map-partition-consumer.XXXXXXXX")"
trap 'rm -r -- "$gate_directory"' EXIT
mkdir "$gate_directory/classes"

# A real JDK 8 is the only way to prove a Java 8 toolchain. Without one the
# host JDK compiles with --release 8 and the consumer itself still verifies the
# class-file version, so the claim below is scoped to what was actually shown.
if rowweave_resolve_java8_home; then
    javac_command="$ROWWEAVE_RESOLVED_JAVA8_HOME/bin/javac"
    java_command="$ROWWEAVE_RESOLVED_JAVA8_HOME/bin/java"
    toolchain="JDK 8 $ROWWEAVE_RESOLVED_JAVA8_HOME"
    "$javac_command" -source 8 -target 8 -cp "$core_jar" \
        -d "$gate_directory/classes" "$consumer_source"
else
    javac_command="${JAVAC:-javac}"
    java_command="${JAVA:-java}"
    toolchain="host JDK fallback ($(command java -version 2>&1 | head -1))"
    "$javac_command" --release 8 -cp "$core_jar" \
        -d "$gate_directory/classes" "$consumer_source"
fi

"$java_command" -cp "$core_jar:$gate_directory/classes" \
    external.ExternalMapPartitionConsumer "$core_jar" \
    "$gate_directory/classes/external/ExternalMapPartitionConsumer.class"
printf 'Public map-partition JAR consumer: PASS on %s\n' "$toolchain"
