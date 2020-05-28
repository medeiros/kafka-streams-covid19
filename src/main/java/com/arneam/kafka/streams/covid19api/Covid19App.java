package com.arneam.kafka.streams.covid19api;

import com.arneam.kafka.streams.covid19api.model.Country;
import com.arneam.kafka.streams.covid19api.model.CountryRanking;
import java.time.Duration;
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
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
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
  public static final String APPLICATION_ID = "covid19-application-33";
  public static final String BOOTSTRAP_SERVERS = "localhost:9092";

  public Logger log = LoggerFactory.getLogger(Covid19App.class);

  public static void main(String[] args) {
    final Topology topology = new Covid19App().topology(Instant.now(), INPUT_TOPIC, OUTPUT_TOPIC);
    final Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

    // to make sure that only one agregate message will come out
    // https://docs.confluent.io/current/streams/developer-guide/memory-mgmt.html
    //config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 5 * 1024 * 1024);
    //config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000);

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
    TimeWindowedKStream<String, Country> window = filterTodayData
        .selectKey((s, country) -> country.date().toString())
        .groupByKey(Grouped.with(Serdes.String(), new CountrySerde()))
        .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ofSeconds(5)));

    // entender porque registro de execucao anterior entra no window, mesmo tendo outra key (data)
    // usar through, peek, foreach ou algo para limpar o objeto ranking (mudar estado)?
    // tentar limpar o ranking para proximas execucoes! -- chamar clearCountries
    KTable<Windowed<String>, CountryRanking> aggregate = window
        .aggregate(() -> ranking, (s, country, countryRanking) -> {
          countryRanking.addCountry(country);
          log.info("----> country added: {}", country.toString());
          return countryRanking;
        }, Materialized.<String, CountryRanking, WindowStore<Bytes, byte[]>>as(
            "aggregated-table-store").withCachingDisabled()
            .withKeySerde(Serdes.String()).withValueSerde(new CountryRankingSerde()))
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()));
    aggregate.toStream()
        .print(Printed.<Windowed<String>, CountryRanking>toSysOut().withLabel("TimeWindow"));

    KStream<Windowed<String>, String> finalStream = aggregate
        .mapValues((s, countryRanking) -> countryRanking.createSummary().toString()).toStream()
        .peek((stringWindowed, s) -> ranking.clearCountries());
    finalStream.print(Printed.toSysOut());
    finalStream.to(outputTopic,
        Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));

    return builder.build();
  }

}
