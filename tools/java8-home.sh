#!/usr/bin/env bash
# One Java 8 consumer-home contract for every gate that claims Java 8 evidence.
#
# Resolution order, first accepted candidate wins:
#   1. ROWWEAVE_JAVA8_HOME  (canonical name used by the release gate and CI)
#   2. JAVA8_HOME           (documented alias, kept so existing invocations work)
#   3. /usr/libexec/java_home -v 1.8  (macOS probe)
#
# A candidate is accepted only when it is a JDK 8, not a Java 8 runtime:
# bin/java must be executable, bin/javac must be executable, and the launcher
# must report version 1.8. Without that last check the macOS probe happily
# returns the browser-plugin JRE, which has java but no javac, and a gate then
# silently exercises the host JDK while reporting Java 8 evidence.
#
# Usage:
#   source tools/java8-home.sh
#   if rowweave_resolve_java8_home; then
#       javac="$ROWWEAVE_RESOLVED_JAVA8_HOME/bin/javac"
#   else
#       rowweave_java8_home_reasons      # why nothing was accepted
#   fi
#   rowweave_require_java8_home "GATE NAME"   # explicit failure, never a skip

ROWWEAVE_RESOLVED_JAVA8_HOME=""
ROWWEAVE_JAVA8_HOME_REASONS=""

rowweave_java8_home_reasons() {
    if [[ -z "$ROWWEAVE_JAVA8_HOME_REASONS" ]]; then
        printf 'no candidate offered: unset ROWWEAVE_JAVA8_HOME and JAVA8_HOME\n'
    else
        printf '%s\n' "$ROWWEAVE_JAVA8_HOME_REASONS"
    fi
}

_rowweave_reject() {
    ROWWEAVE_JAVA8_HOME_REASONS+="$1: $2"$'\n'
    return 1
}

# Validate one candidate home. Echoes nothing; sets acceptance through status.
_rowweave_validate_java8_home() {
    local home="$1" label="$2" version
    if [[ -z "$home" ]]; then
        _rowweave_reject "$label" "empty"
    elif [[ ! -x "$home/bin/java" ]]; then
        _rowweave_reject "$label" "$home has no executable bin/java"
    elif [[ ! -x "$home/bin/javac" ]]; then
        _rowweave_reject "$label" \
            "$home has no executable bin/javac, so it is a runtime, not a JDK"
    else
        version="$("$home/bin/java" -version 2>&1 | head -1)"
        case "$version" in
            *1.8*|*'"8"'*) return 0 ;;
            *) _rowweave_reject "$label" "not a Java 8 JDK: $version" ;;
        esac
    fi
}

rowweave_resolve_java8_home() {
    ROWWEAVE_RESOLVED_JAVA8_HOME=""
    ROWWEAVE_JAVA8_HOME_REASONS=""

    if [[ -n "${ROWWEAVE_JAVA8_HOME:-}" ]] \
            && _rowweave_validate_java8_home "$ROWWEAVE_JAVA8_HOME" "ROWWEAVE_JAVA8_HOME"; then
        ROWWEAVE_RESOLVED_JAVA8_HOME="$ROWWEAVE_JAVA8_HOME"
        return 0
    fi
    if [[ -n "${JAVA8_HOME:-}" ]] \
            && _rowweave_validate_java8_home "$JAVA8_HOME" "JAVA8_HOME"; then
        ROWWEAVE_RESOLVED_JAVA8_HOME="$JAVA8_HOME"
        return 0
    fi
    if [[ -x /usr/libexec/java_home ]]; then
        local probe
        probe="$(/usr/libexec/java_home -v 1.8 2>/dev/null || true)"
        if [[ -n "$probe" ]] \
                && _rowweave_validate_java8_home "$probe" "java_home -v 1.8"; then
            ROWWEAVE_RESOLVED_JAVA8_HOME="$probe"
            return 0
        fi
    fi
    return 1
}

rowweave_require_java8_home() {
    local gate="$1"
    if ! rowweave_resolve_java8_home; then
        printf '%s: no Java 8 JDK resolved — refusing to claim Java 8 evidence\n' \
            "$gate" >&2
        while IFS= read -r line; do
            [[ -n "$line" ]] && printf '  %s\n' "$line" >&2
        done < <(rowweave_java8_home_reasons)
        printf '  set ROWWEAVE_JAVA8_HOME to a JDK 8 that has bin/javac\n' >&2
        exit 1
    fi
    export ROWWEAVE_JAVA8_HOME="$ROWWEAVE_RESOLVED_JAVA8_HOME"
    export JAVA8_HOME="$ROWWEAVE_RESOLVED_JAVA8_HOME"
    printf '%s: Java 8 consumer home %s (%s)\n' "$gate" \
        "$ROWWEAVE_RESOLVED_JAVA8_HOME" \
        "$("$ROWWEAVE_RESOLVED_JAVA8_HOME/bin/java" -version 2>&1 | head -1)"
}
