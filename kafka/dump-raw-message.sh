#!/usr/bin/env bash
# Dumps the raw bytes of the first N messages (independently of the Java consumer).
# Look at the first 5 bytes: 00 | 00 00 00 0X  -> magic byte | schema id
set -euo pipefail
TOPIC=${1:-ping-messages}
COUNT=${2:-1}

docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic "$TOPIC" --from-beginning --max-messages "$COUNT" 2>/dev/null | od -A d -t x1 | head -20
