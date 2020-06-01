package com.arneam.kafka.streams.covid19api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

class Covid19ConfigIT {

  @Test
  void shouldLoadPropertiesFile() {
    Properties config = Covid19Config.config();

    assertThat(config, is(notNullValue()));
    assertThat(config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG),
        is(equalTo("covid19api-application")));
    assertThat(config.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG),
        is(equalTo("localhost:9092")));
    assertThat(config.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
        is(equalTo("earliest")));
    assertThat(config.getProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
        is(equalTo("exactly_once")));
    assertThat(config.getProperty("input.topic"), is(equalTo("covid-input")));
    assertThat(config.getProperty("output.topic"), is(equalTo("covid-output")));
  }

}