#!/usr/bin/env bash
# Shows subjects, versions and schema ids stored in Schema Registry.
set -uo pipefail
SR=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
SUBJECT=${1:-ping-messages-value}

echo "== Global compatibility level";  curl -s "$SR/config"; echo
echo "== Subjects";                     curl -s "$SR/subjects"; echo
VERSIONS=$(curl -s "$SR/subjects/$SUBJECT/versions")
echo "== Versions of $SUBJECT";         echo "$VERSIONS"

if [[ "$VERSIONS" == \[* ]]; then
  for v in $(echo "$VERSIONS" | tr -d '[]' | tr ',' ' '); do
    echo "== $SUBJECT version $v  (look at the \"id\" field = bytes 1-4 of the message)"
    curl -s "$SR/subjects/$SUBJECT/versions/$v"; echo
  done
fi
