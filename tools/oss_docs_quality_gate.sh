#!/usr/bin/env bash
# One entry point for canonical documentation qualification.
set -uo pipefail
cd "$(dirname "$0")/.."

status=0

python3 tools/oss_docs_quality_gate.py || status=1

pages=$(find . -name '*.md' -not -path './target/*' -not -path './.git/*' | sort)
if [ -z "$pages" ]; then
  echo "DOCS QUALITY GATE: FAIL (no Markdown pages found)"
  status=1
else
  # shellcheck disable=SC2086
  python3 tools/oss_doc_anchor_check.py $pages || status=1
fi

bash tools/oss_docs_language_gate.sh || status=1
python3 tools/oss_docs_translation_gate.py || status=1
python3 tools/oss_docs_maintenance_gate.py || status=1
python3 tools/oss_java_identifier_gate.py || status=1
bash tools/oss_identity_residue_gate.sh || status=1
python3 tools/oss_java_identifier_gate.py || status=1
bash tools/oss_content_boundary_gate.sh || status=1
python3 tools/oss_source_language_gate.py || status=1
python3 tools/oss_batch_docs_parity_gate.py || status=1
python3 tools/oss_stream_docs_parity_gate.py || status=1
python3 tools/oss_configuration_parity_gate.py || status=1
python3 tools/oss_error_docs_parity_gate.py || status=1
python3 tools/oss_formats_limits_spi_gate.py || status=1
python3 tools/oss_execution_architecture_gate.py || status=1
python3 tools/oss_cluster_architecture_gate.py || status=1
python3 tools/oss_recovery_authority_gate.py || status=1
python3 tools/oss_resource_model_gate.py || status=1
python3 tools/oss_architecture_reading_map_gate.py || status=1
bash tools/oss_repo_url_check.sh || status=1

if [ "$status" -eq 0 ]; then
  echo "DOCS QUALITY SUITE: PASS"
else
  echo "DOCS QUALITY SUITE: FAIL"
fi
exit "$status"
