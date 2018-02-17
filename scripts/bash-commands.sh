#!/usr/bin/env bash

export KAFKA="kafka:9092"

kafka-console-consumer --property print.key=true --from-beginning --bootstrap-server $KAFKA --topic lab_kafka_streams_in