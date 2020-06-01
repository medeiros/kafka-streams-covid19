package com.arneam.kafka.streams.covid19api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class Covid19ConfigIT {

  static Properties config;
  static Properties configTest;

  @BeforeAll
  static void init() {
    config = Covid19Config.config();
    configTest = Covid19Config.configTest();
  }

  @ParameterizedTest
  @CsvSource(delimiter = '=', value = {"application.id=covid19api-application",
    "bootstrap.servers=localhost:9092", "auto.offset.reset=earliest",
    "processing.guarantee=exactly_once", "input.topic=covid-input", "output.topic=covid-output"})
  void shouldLoadPropertiesFile(String key, String value) {
    assertThat(config.getProperty(key), is(equalTo(value)));
  }

  @ParameterizedTest
  @CsvSource(delimiter = '=', value = {"bootstrap.servers=localhost:9092",
      "key.serializer=org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer=org.apache.kafka.common.serialization.StringSerializer",
      "acks=all", "retries=3", "linger.ms=1", "enable.idempotence=true", "input.topic=covid-input"})
  void shouldLoadTestPropertiesFile(String key, String value) {
    assertThat(configTest.getProperty(key), is(equalTo(value)));
  }

}