package com.arneam.kafka.streams.covid19api;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Covid19Producer {

  private static Logger log = LoggerFactory.getLogger(Covid19Producer.class);
  private static Instant today = Instant.now();

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    String bootStrapServer = "localhost:9092";
    new Covid19Producer().produce("covid-input", bootStrapServer);
  }

  public void produce(String topic, String bootstrapServer)
      throws InterruptedException, ExecutionException {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.RETRIES_CONFIG, 3);
    config.put(ProducerConfig.LINGER_MS_CONFIG, 1);

    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    KafkaProducer<String, String> producer = new KafkaProducer<>(config);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      producer.flush();
      producer.close();
    }));

    List<String> data = dataFromToday();
    data.addAll(dataFromYesterday());
    for (String countryAsJson : data) {
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, countryAsJson);
      producer.send(record, (recordMetadata, e) -> {
        if (e != null) {
          log.error("Error: ", e);
        }
      }).get();
    }

    Thread.sleep(20000);
    producer.send(new ProducerRecord<>(topic, dummyRecord(today)));
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

  private String dummyRecord(Instant date) {
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
        + "      \"Date\": \"" + date + "\"\n"
        + "    }";
  }
}
