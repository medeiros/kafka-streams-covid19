package com.arneam.kafka.streams.covid19api;

import com.arneam.kafka.streams.covid19api.model.Country;
import com.arneam.kafka.streams.covid19api.model.CountryRanking;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.JSONObject;

/**
 * Covid19App
 *
$ kafka-topics.sh --zookeeper localhost:2181 --delete --topic covid-input
$ kafka-topics.sh --zookeeper localhost:2181 --delete --topic covid-output

$ kafka-topics.sh --zookeeper localhost:2181 --create --topic covid-input --partitions 1 \
--replication-factor 1
$ kafka-topics.sh --zookeeper localhost:2181 --create --topic covid-output --partitions 1 \
--replication-factor 1 --config cleanup.policy=compact --config segment.ms=100 \
--config min.cleanable.dirty.ratio=0.001

$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic covid-output \
--from-beginning
*/
public class Covid19App {

  public static final String INPUT_TOPIC = "covid-input";
  public static final String OUTPUT_TOPIC = "covid-output";
  public static final String APPLICATION_ID = "covid19-application-019";
  public static final String BOOTSTRAP_SERVERS = "localhost:9092";

  public static void main(String[] args) {
    final Topology topology = new Covid19App().topology(Instant.now(), INPUT_TOPIC, OUTPUT_TOPIC);
    final Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

    KafkaStreams streams = new KafkaStreams(topology, config);
    streams.cleanUp();
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  public Topology topology(Instant instant, String inputTopic, String outputTopic) {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> stream = builder.stream(inputTopic,
        Consumed.with(new Serdes.StringSerde(), new Serdes.StringSerde()));
    stream.print(Printed.toSysOut());

    KStream<String, Country> mapJSONToCountry = stream
        .mapValues(s -> Country.fromJSON(new JSONObject(s)));
    mapJSONToCountry.print(Printed.toSysOut());

    KStream<String, Country> filterTodayData = mapJSONToCountry
        .filter((s, country) -> country.date().truncatedTo(ChronoUnit.DAYS)
            .equals(instant.truncatedTo(ChronoUnit.DAYS)));
    filterTodayData.print(Printed.toSysOut());

    final CountryRanking ranking = new CountryRanking();
    KTable<String, CountryRanking> aggregate = filterTodayData
        .selectKey((s, country) -> country.date().toString())
        .groupByKey(Grouped.with(Serdes.String(), new CountrySerde()))
        .aggregate(() -> ranking, (s, country, countryRanking) -> {
          countryRanking.addCountry(country);
          return countryRanking;
        }, Materialized.<String, CountryRanking, KeyValueStore<Bytes, byte[]>>as(
            "aggregated-table-store")
            .withKeySerde(Serdes.String()).withValueSerde(new CountryRankingSerde()));
    aggregate.toStream().print(Printed.toSysOut());

    KStream<String, String> finalStream = aggregate
        .mapValues((s, countryRanking) -> countryRanking.createSummary().toString()).toStream();
    finalStream.print(Printed.toSysOut());
    finalStream.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }


}
