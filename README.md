# Kafka Streams for Covid19API

The purpose of this application is to summarize a daily list of information regarding Covid-19
numbers across countries, ranking Brazil in that context. The metrics are: new cases, total cases, 
new deaths, total deaths, new recovered and total recovered.

This is a Kafka Streams application. It gets its input JSON data from a topic that was 
previously loaded by "[kafka-connect-covid19api](https://github.com/medeiros/kafka-connect-covid19api)" Kafka connector, 
and delivers output JSON data to a topic that will be later sink to Twitter.

## Topology

```
1. STREAM -> <null, countriesJSONArray>
2. MAPVALUES -> <null, List<Country>>
3. FILTER Today -> <null, List<Country>>
4. SELECTKEY date -> <date, List<Country>>
5. GROUPBYKEY date -> <date, List<Country>>
6. WINDOW Session -> <date, SessionWindowedKStream<String, List<Country>>>
7. AGGREGATE -> <date, KTable<Windowed<String>, CountryRanking>>
8. BRANCH -> <date, KStream<Windowed<String>, String>>
9. TO destination -> <date, JSONBrazilRankingString>
```

## Useful Commands

```bash
# create topics from scratch
kafka-topics.sh --zookeeper localhost:2181 --delete --topic covid-input
kafka-topics.sh --zookeeper localhost:2181 --delete --topic covid-output
kafka-topics.sh --zookeeper localhost:2181 --create --topic covid-input --partitions 3 \
--replication-factor 1 --config cleanup.policy=compact --config segment.ms=5000 \
--config min.cleanable.dirty.ratio=0.001
kafka-topics.sh --zookeeper localhost:2181 --create --topic covid-output --partitions 3 \
--replication-factor 1

# consumers
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic covid-input \
--from-beginning

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic covid-output \
--from-beginning
```

## References

- https://www.confluent.io/blog/kafka-streams-take-on-watermarks-and-triggers/
- https://stackoverflow.com/questions/54890239/kafka-streams-suppress-closing-a-timewindow-by-timeout
- https://stackoverflow.com/questions/61066969/unable-to-force-window-suppression-when-using-topologytestdriver
- punctuator: https://docs.confluent.io/current/streams/developer-guide/processor-api.html