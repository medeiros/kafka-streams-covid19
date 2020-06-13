package com.arneam.kafka.streams.covid19api;

import com.arneam.kafka.streams.covid19api.model.Country;
import com.arneam.kafka.streams.covid19api.model.CountryRanking;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
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

public class Covid19App {

  public Logger log = LoggerFactory.getLogger(Covid19App.class);

  public static void main(String[] args) {
    Properties config = Covid19Config.config();

    final Topology topology = new Covid19App().topology(Instant.now(),
        config.getProperty("input.topic"), config.getProperty("output.topic"));

    final KafkaStreams streams = new KafkaStreams(topology, config);
    streams.cleanUp();
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
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
        .selectKey((s, country) -> {
          log.info("-----> Select Key: {}", country.date().toString());
          return country.date().toString();
        })
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
    @SuppressWarnings("unchecked") KStream<Windowed<String>, CountryRanking>[] branches = aggregate
        .toStream()
        .branch((stringWindowed, countryRanking) -> ignoreDummy(countryRanking));

    KStream<Windowed<String>, String> branchStream = branches[0]
        .mapValues((s, countryRanking) -> countryRanking.createSummary().toString());
    branchStream.print(Printed.toSysOut());
    return branchStream;
  }

  private boolean ignoreDummy(CountryRanking countryRanking) {
    log.info("-----> checking for dummy record: {}" + countryRanking.toString());
    return countryRanking.getCountries().size() > 0
        && !countryRanking.getCountries().get(0).countryCode().equals("XX");
  }

  private void toDestination(String outputTopic, KStream<Windowed<String>, String> stream) {
    stream.to(outputTopic,
        Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));
  }

}
