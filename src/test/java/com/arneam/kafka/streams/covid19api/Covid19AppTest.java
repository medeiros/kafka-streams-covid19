package com.arneam.kafka.streams.covid19api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;

import com.arneam.kafka.streams.covid19api.model.Country;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Covid19AppTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Country> outputTopic;
  private StringSerde stringSerde = new Serdes.StringSerde();
  private CountrySerde countrySerde = new CountrySerde();

  @BeforeEach
  void init() {
    final Topology topology = new Covid19App().topology(Instant.now());

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "covid19-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    this.testDriver = new TopologyTestDriver(topology, config);
    this.inputTopic = this.testDriver
        .createInputTopic("covid-input", stringSerde.serializer(), stringSerde.serializer());
    this.outputTopic = this.testDriver
        .createOutputTopic("covid-output", stringSerde.deserializer(), countrySerde.deserializer());
  }

  @AfterEach
  void finish() {
    this.testDriver.close();
  }

  @Test
  void shouldGenerateDataTable() {
    String bosniaJson = "{\n"
        + "      \"Country\": \"Bosnia and Herzegovina\",\n"
        + "      \"CountryCode\": \"BA\",\n"
        + "      \"Slug\": \"bosnia-and-herzegovina\",\n"
        + "      \"NewConfirmed\": 23,\n"
        + "      \"TotalConfirmed\": 2181,\n"
        + "      \"NewDeaths\": 3,\n"
        + "      \"TotalDeaths\": 120,\n"
        + "      \"NewRecovered\": 60,\n"
        + "      \"TotalRecovered\": 1228,\n"
        + "      \"Date\": \"" + Instant.now() + "\"\n"
        + "    }";
    String brazilJson = "{\n"
        + "      \"Country\": \"Brazil\",\n"
        + "      \"CountryCode\": \"BR\",\n"
        + "      \"Slug\": \"brazil\",\n"
        + "      \"NewConfirmed\": 12,\n"
        + "      \"TotalConfirmed\": 13,\n"
        + "      \"NewDeaths\": 14,\n"
        + "      \"TotalDeaths\": 15,\n"
        + "      \"NewRecovered\": 16,\n"
        + "      \"TotalRecovered\": 17,\n"
        + "      \"Date\": \"2020-05-14T08:13:33Z\"\n"
        + "    }";
    String argentinaJson = "{\n"
        + "      \"Country\": \"Argentina\",\n"
        + "      \"CountryCode\": \"AR\",\n"
        + "      \"Slug\": \"argentina\",\n"
        + "      \"NewConfirmed\": 1,\n"
        + "      \"TotalConfirmed\": 2,\n"
        + "      \"NewDeaths\": 3,\n"
        + "      \"TotalDeaths\": 4,\n"
        + "      \"NewRecovered\": 5,\n"
        + "      \"TotalRecovered\": 6,\n"
        + "      \"Date\": \"2020-05-14T08:13:33Z\"\n"
        + "    }";

    List<String> countriesJson = new ArrayList<>();
    countriesJson.add(bosniaJson);
    countriesJson.add(brazilJson);
    countriesJson.add(argentinaJson);
    this.inputTopic.pipeValueList(countriesJson);

    List<KeyValue<String, Country>> countries = new ArrayList<>();
    countries.add(new KeyValue<>(null, Country.fromJSON(new JSONObject(bosniaJson))));
    countries.add(new KeyValue<>(null, Country.fromJSON(new JSONObject(argentinaJson))));
    countries.add(new KeyValue<>(null, Country.fromJSON(new JSONObject(brazilJson))));

    assertThat(outputTopic.isEmpty(), is(false));
    assertThat(outputTopic.readKeyValuesToList(),
        hasItems(countries.get(0), countries.get(1), countries.get(2)));
    assertThat(outputTopic.isEmpty(), is(true));
  }

}