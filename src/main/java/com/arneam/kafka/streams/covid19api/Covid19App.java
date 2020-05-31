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
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.SessionStore;
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
--replication-factor 1 --config cleanup.policy=compact --config segment.ms=5000 \
--config min.cleanable.dirty.ratio=0.001

$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic covid-output \
--from-beginning
*/
public class Covid19App {

  public static final String INPUT_TOPIC = "covid-input";
  public static final String OUTPUT_TOPIC = "covid-output";
  public static final String APPLICATION_ID = "covid19-application-411";
  public static final String BOOTSTRAP_SERVERS = "localhost:9092";

  public Logger log = LoggerFactory.getLogger(Covid19App.class);

  public static void main(String[] args) {
    final Topology topology = new Covid19App().topology(Instant.now(), INPUT_TOPIC, OUTPUT_TOPIC);
    final KafkaStreams streams = new KafkaStreams(topology, config());
    streams.cleanUp();
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static Properties config() {
    final Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    // to make sure that only one agregate message will come out - using Window instead
    // https://docs.confluent.io/current/streams/developer-guide/memory-mgmt.html
    //config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 5 * 1024 * 1024);
    //config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000);
    return config;
  }

  public Topology topology(Instant instant, String inputTopic, String outputTopic) {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> stream = baseStream(inputTopic, builder);
    KStream<String, Country> mapJSONToCountry = mapJSONToCountry(stream);
    KStream<String, Country> filterTodayData = filterSameDay(instant, mapJSONToCountry);
    SessionWindowedKStream<String, Country> window = createWindow(filterTodayData);
    KTable<Windowed<String>, CountryRanking> aggregate = aggregateWindow(window);
    KStream<Windowed<String>, String> finalStream = summarizeData(aggregate);
    toDestination(outputTopic, finalStream);
    return builder.build();
  }

  private KStream<String, String> baseStream(String inputTopic, StreamsBuilder builder) {
    KStream<String, String> stream = builder.stream(inputTopic,
        Consumed.with(new Serdes.StringSerde(), new Serdes.StringSerde()));
    stream.print(Printed.toSysOut());
    return stream;
  }

  private KStream<String, Country> mapJSONToCountry(KStream<String, String> stream) {
    KStream<String, Country> mapJSONToCountry = stream
        .mapValues(s -> Country.fromJSON(new JSONObject(s)));
    mapJSONToCountry.print(Printed.toSysOut());
    return mapJSONToCountry;
  }

  private KStream<String, Country> filterSameDay(Instant instant, KStream<String, Country> stream) {
    KStream<String, Country> filterTodayData = stream
        .filter((s, country) -> country.date().truncatedTo(ChronoUnit.DAYS)
            .equals(instant.truncatedTo(ChronoUnit.DAYS)));
    filterTodayData.print(Printed.toSysOut());
    return filterTodayData;
  }

  private SessionWindowedKStream<String, Country> createWindow(KStream<String, Country> stream) {
    return stream
        .selectKey((s, country) -> country.date().toString())
        .groupByKey(Grouped.with(Serdes.String(), new CountrySerde()))
        .windowedBy(SessionWindows.with(Duration.ofSeconds(5)).grace(Duration.ofSeconds(5)));
  }

  private KTable<Windowed<String>, CountryRanking> aggregateWindow(
      SessionWindowedKStream<String, Country> windowStream) {
    KTable<Windowed<String>, CountryRanking> aggregate = windowStream
        .aggregate(
            CountryRanking::new,
            (key, value, countryRanking) -> {
              CountryRanking result = countryRanking.addCountry(value);
              log.info("----> country added: {}", value.toString());
              return result;
            },
            (key, cr1, cr2) -> new CountryRanking(cr1, cr2),
            Materialized.<String, CountryRanking, SessionStore<Bytes, byte[]>>as(
                "aggregated-table-store")
                .withCachingDisabled()
                .withKeySerde(Serdes.String())
                .withValueSerde(new CountryRankingSerde())
        )
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()));
    aggregate.toStream()
        .print(Printed.<Windowed<String>, CountryRanking>toSysOut().withLabel("Window"));
    return aggregate;
  }

  private KStream<Windowed<String>, String> summarizeData(
      KTable<Windowed<String>, CountryRanking> aggregate) {

    // the dummy record (used to flush window data from previous execution) still exist and
    // will be counted in the next window. In order to ignore this dummy record, I'm using
    // branch (filter could do the job, but it will generate a null record in the output, and
    // this is not a good behavior - with branch, nothing is generated - not even null)
    KStream<Windowed<String>, CountryRanking>[] branches = aggregate.toStream()
        .branch((stringWindowed, countryRanking) -> ignoreDummy(countryRanking));

    KStream<Windowed<String>, String> branchStream = branches[0]
        .mapValues((s, countryRanking) -> countryRanking.createSummary().toString());
    branchStream.print(Printed.toSysOut());
    return branchStream;
  }

  private boolean ignoreDummy(CountryRanking countryRanking) {
    return countryRanking.getCountries().size() > 0
        && !countryRanking.getCountries().get(0).country().isEmpty();
  }

  private void toDestination(String outputTopic, KStream<Windowed<String>, String> stream) {
    stream.to(outputTopic,
        Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));
  }

}
