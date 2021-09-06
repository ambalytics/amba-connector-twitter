#!/usr/bin/env bash

/scripts/wait-for-it.sh "$KAFKA_ZOOKEEPER_CONNECT" -t 5 -- echo "Zookeeper started"
/scripts/wait-for-it.sh "$KAFKA_BOOTRSTRAP_SERVER" -t 5 -- echo "Kafka started"

python ./src/twitter_connector.py