#!/bin/bash
# shellcheck source=../.env
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"

source "$ENV_FILE"

topics=("$RAW_EVENTS_TOPIC" "$DLQ_TOPIC")

echo "Topics to delete and recreate:"
printf '  - %s\n' "${topics[@]}"

echo ""
echo -n "Are you sure? [yes/no]: "
read -r confirm

[[ "$confirm" == "yes" ]] || exit 0

for topic in "${topics[@]}"; do
    docker run --rm --network tanilytics_tanilytics_network \
        --entrypoint /bin/bash "${REDPANDA_IMAGE}" \
        -c "rpk topic delete $topic \
        -X user=${REDPANDA_SUPERUSER} \
        -X pass=${REDPANDA_PASSWORD} \
        -X brokers=${REDPANDA_BROKERS}" 2>&1 || true
    echo "Deleted: $topic"
done

echo ""
echo "Recreating topics..."

docker run --rm --network tanilytics_tanilytics_network \
    --entrypoint /bin/bash "${REDPANDA_IMAGE}" \
    -c "rpk topic create $RAW_EVENTS_TOPIC \
    -p ${RAW_EVENTS_PARTITIONS} \
    -r ${RAW_EVENTS_REPLICAS} \
    -c cleanup.policy=${RAW_EVENTS_CLEANUP_POLICY} \
    -c retention.ms=${RAW_EVENTS_RETENTION_MS} \
    -c retention.bytes=${RAW_EVENTS_RETENTION_BYTES} \
    -c compression.type=${RAW_EVENTS_COMPRESSION} \
    -X user=${REDPANDA_SUPERUSER} \
    -X pass=${REDPANDA_PASSWORD} \
    -X brokers=${REDPANDA_BROKERS}" 2>&1
echo "Created: $RAW_EVENTS_TOPIC"

docker run --rm --network tanilytics_tanilytics_network \
    --entrypoint /bin/bash "${REDPANDA_IMAGE}" \
    -c "rpk topic create $DLQ_TOPIC \
    -p ${DLQ_PARTITIONS} \
    -r ${DLQ_REPLICAS} \
    -c cleanup.policy=${DLQ_CLEANUP_POLICY} \
    -c retention.ms=${DLQ_RETENTION_MS} \
    -c compression.type=${DLQ_COMPRESSION} \
    -X user=${REDPANDA_SUPERUSER} \
    -X pass=${REDPANDA_PASSWORD} \
    -X brokers=${REDPANDA_BROKERS}" 2>&1
echo "Created: $DLQ_TOPIC"

echo "Done."
