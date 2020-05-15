package com.arneam.kafka.streams.covid19api;

import com.arneam.kafka.streams.covid19api.model.BrazilRanking;
import com.arneam.kafka.streams.covid19api.model.BrazilRankingSummary;
import com.arneam.kafka.streams.covid19api.model.Country;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.json.JSONObject;

public class Covid19App {

  public Topology topology(Instant instant) {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> stream = builder.stream("covid-input", Consumed.with(new Serdes.StringSerde(), new Serdes.StringSerde()));
    stream.print(Printed.toSysOut());

    KStream<String, Country> mapJSONToCountry = stream
        .mapValues(s -> Country.fromJSON(new JSONObject(s)));
    mapJSONToCountry.print(Printed.toSysOut());

//    KStream<String, Country> filterTodayData = mapJSONToCountry
//        .filter((s, country) -> country.date().truncatedTo(ChronoUnit.DAYS)
//            .equals(instant.truncatedTo(ChronoUnit.DAYS)));
//    filterTodayData.print(Printed.toSysOut());

    mapJSONToCountry.to("covid-output", Produced.with(new Serdes.StringSerde(), new CountrySerde()));

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
