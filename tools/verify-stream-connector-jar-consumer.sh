#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
core_jar="${1:-$repository_root/rowweave-core/target/rowweave-core-0.2.0.jar}"
consumer_source="$repository_root/tools/stream-connector-jar-consumer/ExternalConnectorConsumer.java"

if [[ ! -f "$core_jar" ]]; then
    printf 'Missing published core JAR: %s\n' "$core_jar" >&2
    exit 1
fi

# shellcheck source=tools/java8-home.sh
source "$repository_root/tools/java8-home.sh"
gate_directory="$(mktemp -d "${TMPDIR:-/tmp}/rowweave-stream-jar-consumer.XXXXXXXX")"
trap 'rm -r -- "$gate_directory"' EXIT
mkdir "$gate_directory/classes"

# Deliberately exclude target/classes, test classes and every other reactor module.
if rowweave_resolve_java8_home; then
    javac_command="$ROWWEAVE_RESOLVED_JAVA8_HOME/bin/javac"
    java_command="$ROWWEAVE_RESOLVED_JAVA8_HOME/bin/java"
    jar_command="$ROWWEAVE_RESOLVED_JAVA8_HOME/bin/jar"
    toolchain="JDK 8 $ROWWEAVE_RESOLVED_JAVA8_HOME"
    "$javac_command" -source 8 -target 8 -cp "$core_jar" \
        -d "$gate_directory/classes" "$consumer_source"
else
    java_command="${JAVA:-java}"
    jar_command="${JAR:-jar}"
    toolchain="host JDK fallback ($(command java -version 2>&1 | head -1))"
    "${JAVAC:-javac}" --release 8 -cp "$core_jar" \
        -d "$gate_directory/classes" "$consumer_source"
fi
"$jar_command" cf "$gate_directory/consumer.jar" -C "$gate_directory/classes" .

# The consumer inspects the class-file major version of both JARs, so the
# Java 8 bytecode claim is proven by the artifact rather than by the toolchain.
"$java_command" -cp "$core_jar:$gate_directory/consumer.jar" \
    external.ExternalConnectorConsumer "$core_jar" "$gate_directory/consumer.jar"
printf 'Public Stream connector JAR consumer: PASS on %s\n' "$toolchain"
