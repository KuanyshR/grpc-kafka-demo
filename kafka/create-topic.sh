#!/usr/bin/env bash
# Creates the demo topic inside the running 'kafka' container.
set -euo pipefail
TOPIC=${1:-ping-messages}

docker exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1

docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic "$TOPIC"
