#!/bin/bash
KAFKA_CONTAINER="kafka"

topics=("latest_prices" "spot_prices" "currency_rates" "authority_prices")

for topic in "${topics[@]}"; do
    docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
        --create \
        --topic $topic \
        --bootstrap-server localhost:9092 \
        --partitions 1 \
        --replication-factor 1 \
        --if-not-exists
    echo "Created topic: $topic"
done
