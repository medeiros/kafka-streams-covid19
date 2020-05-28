package com.arneam.kafka.streams.covid19api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.arneam.kafka.streams.covid19api.model.BrazilRankingSummary;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Covid19AppTest {

  public static final String INPUT_TOPIC = "covid-input";
  public static final String OUTPUT_TOPIC = "covid-output";
  public static final String APPLICATION_ID = "covid19-application";
  public static final String BOOTSTRAP_SERVERS = "dummy:1234";

  private static final Instant today = Instant.now();

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<Windowed<String>, String> outputTopic;
  private StringSerde stringSerde = new Serdes.StringSerde();

  @BeforeEach
  void init() {
    final Topology topology = new Covid19App().topology(today, INPUT_TOPIC, OUTPUT_TOPIC);
    this.testDriver = new TopologyTestDriver(topology, config());
    this.inputTopic = this.testDriver
        .createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
    this.outputTopic = this.testDriver.createOutputTopic(OUTPUT_TOPIC,
        WindowedSerdes.timeWindowedSerdeFrom(String.class).deserializer(),
        stringSerde.deserializer());
  }

  private Properties config() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.RETRIES_CONFIG, 3);
    config.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    return config;
  }

  @AfterEach
  void finish() {
    this.testDriver.close();
  }

  @Test
  /*  https://www.confluent.io/blog/kafka-streams-take-on-watermarks-and-triggers/
      https://stackoverflow.com/questions/54890239/kafka-streams-suppress-closing-a-timewindow-by-timeout
      https://stackoverflow.com/questions/61066969/unable-to-force-window-suppression-when-using-topologytestdriver
      punctuator: https://docs.confluent.io/current/streams/developer-guide/processor-api.html
  */
  void shouldGenerateDataTable() {
    List<String> data = dataFromToday();
    data.addAll(dataFromYesterday());

    for (String datum : data) {
      this.inputTopic.pipeInput(datum, Instant.now());
    }
    this.inputTopic.pipeInput(dummyRecord(), Instant.now().plusSeconds(30));

    BrazilRankingSummary expectedRanking = BrazilRankingSummary.builder().newConfirmed(1)
        .totalConfirmed(2).newDeaths(1).totalDeaths(3).newRecovered(2).totalRecovered(3).build();

    assertThat(outputTopic.isEmpty(), is(false));
    assertThat(outputTopic.readValue(), is(equalTo(expectedRanking.toString())));
    assertThat(outputTopic.isEmpty(), is(true));
  }

  private List<String> dataFromToday() {
    return dataFrom(today, 17);
  }

  private List<String> dataFromYesterday() {
    return dataFrom(today.minus(1, ChronoUnit.DAYS), 1000000);
  }

  private List<String> dataFrom(Instant date, int totalRecoveredValueToChangeOrder) {
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
        + "      \"Date\": \"" + date + "\"\n"
        + "    }";
    String brazilJson = "{\n"
        + "      \"Country\": \"Brazil\",\n"
        + "      \"CountryCode\": \"BR\",\n"
        + "      \"Slug\": \"brazil\",\n"
        + "      \"NewConfirmed\": 12000,\n"
        + "      \"TotalConfirmed\": 13,\n"
        + "      \"NewDeaths\": 14,\n"
        + "      \"TotalDeaths\": 15,\n"
        + "      \"NewRecovered\": 16,\n"
        + "      \"TotalRecovered\": " + totalRecoveredValueToChangeOrder + ",\n"
        + "      \"Date\": \"" + date + "\"\n"
        + "    }";
    String argentinaJson = "{\n"
        + "      \"Country\": \"Argentina\",\n"
        + "      \"CountryCode\": \"AR\",\n"
        + "      \"Slug\": \"argentina\",\n"
        + "      \"NewConfirmed\": 1,\n"
        + "      \"TotalConfirmed\": 2,\n"
        + "      \"NewDeaths\": 4,\n"
        + "      \"TotalDeaths\": 20,\n"
        + "      \"NewRecovered\": 5,\n"
        + "      \"TotalRecovered\": 27,\n"
        + "      \"Date\": \"" + date + "\"\n"
        + "    }";
    List<String> countriesJson = new ArrayList<>();
    countriesJson.add(brazilJson);
    countriesJson.add(bosniaJson);
    countriesJson.add(argentinaJson);
    return countriesJson;
  }

  private String dummyRecord() {
    return "{\n"
        + "      \"Country\": \"\",\n"
        + "      \"CountryCode\": \"\",\n"
        + "      \"Slug\": \"\",\n"
        + "      \"NewConfirmed\": 0,\n"
        + "      \"TotalConfirmed\": 0,\n"
        + "      \"NewDeaths\": 0,\n"
        + "      \"TotalDeaths\": 0,\n"
        + "      \"NewRecovered\": 0,\n"
        + "      \"TotalRecovered\": 0,\n"
        + "      \"Date\": \"" + Covid19AppTest.today + "\"\n"
        + "    }";
  }

}
