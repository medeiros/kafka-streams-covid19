#!/bin/bash

# create topics from scratch
kafka-topics.sh --zookeeper localhost:2181 --delete --topic covid-input
kafka-topics.sh --zookeeper localhost:2181 --delete --topic covid-output
kafka-topics.sh --zookeeper localhost:2181 --create --topic covid-input --partitions 1 \
--replication-factor 1
kafka-topics.sh --zookeeper localhost:2181 --create --topic covid-output --partitions 1 \
--replication-factor 1 --config cleanup.policy=compact --config segment.ms=5000 \
--config min.cleanable.dirty.ratio=0.001

# consumers
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic covid-input

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic covid-output \
--from-beginning