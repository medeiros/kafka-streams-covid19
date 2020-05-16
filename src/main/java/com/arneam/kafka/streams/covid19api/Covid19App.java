package com.arneam.kafka.streams.covid19api;

import com.arneam.kafka.streams.covid19api.model.BrazilRankingSummary;
import com.arneam.kafka.streams.covid19api.model.Country;
import com.arneam.kafka.streams.covid19api.model.CountryRanking;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
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

public class Covid19App {

  public Topology topology(Instant instant) {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> stream = builder
        .stream("covid-input", Consumed.with(new Serdes.StringSerde(), new Serdes.StringSerde()));
    stream.print(Printed.toSysOut());

    KStream<String, Country> mapJSONToCountry = stream
        .mapValues(s -> Country.fromJSON(new JSONObject(s)));
    mapJSONToCountry.print(Printed.toSysOut());

    KStream<String, Country> filterTodayData = mapJSONToCountry
        .filter((s, country) -> country.date().truncatedTo(ChronoUnit.DAYS)
            .equals(instant.truncatedTo(ChronoUnit.DAYS)));
    filterTodayData.print(Printed.toSysOut());

    KTable<String, CountryRanking> aggregate = filterTodayData
        .selectKey((s, country) -> country.countryCode())
        .groupByKey(Grouped.with(Serdes.String(), new CountrySerde()))
        .aggregate(CountryRanking::new, (s, country, countryRanking) -> {
          countryRanking.countries.add(country);
          return countryRanking;
        }, Materialized.<String, CountryRanking, KeyValueStore<Bytes, byte[]>>as(
            "aggregated-table-store").withKeySerde(Serdes.String()).withValueSerde(new CountryRankingSerde()));

    KStream<String, BrazilRankingSummary> finalStream = aggregate
        .mapValues((s, countryRanking) -> countryRanking.createSummary()).toStream();
    finalStream.print(Printed.toSysOut());

    finalStream.to("covid-output",
        Produced.with(new Serdes.StringSerde(), new BrazilRankingSummarySerde()));

//    filterTodayData.to("covid-output", Produced.with(new Serdes.StringSerde(), new CountrySerde()));

    return builder.build();
  }


  public static void main(String[] args) {

//
//    KTable<String, BrazilRankingSummary> summary;
//    stream.mapValues(s -> Country.fromJSON(new JSONObject(s)))
//        .filter((s, country) -> country.date().truncatedTo(ChronoUnit.DAYS)
//            .equals(Instant.now().truncatedTo(ChronoUnit.DAYS)))
//        .selectKey((s, country) -> country.date())
//        .groupByKey()
////        .groupBy((s, country) -> country.date());
//        .aggregate(new ArrayList<BrazilRanking>(), (instant, country, vr) -> {
//
//
//        });
//
//
//
//    Collections.sort(countries,
//        Collections.reverseOrder(Comparator.comparingInt(Country::getNewConfirmed)));
//    Optional<Country> brazil = countries.stream()
//        .filter(country -> country.getCountry().equals("Brazil")).findFirst();
//    return countries.indexOf(brazil.get()) + 1;
  }

}
