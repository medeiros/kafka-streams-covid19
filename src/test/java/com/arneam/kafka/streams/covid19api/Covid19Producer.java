package com.arneam.kafka.streams.covid19api;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Covid19Producer {

  private static Logger log = LoggerFactory.getLogger(Covid19Producer.class);
  private static String today = Instant.now().toString();
  private static Properties config = Covid19Config.configTest();

  public static void main(String[] args) throws InterruptedException {
    new Covid19Producer().produce(config.getProperty("input.topic"), config);
  }

  private void produce(String topic, Properties config) throws InterruptedException {
    KafkaProducer<String, String> producer = new KafkaProducer<>(config);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      producer.flush();
      producer.close();
    }));

    for (String countryAsJson : data()) {
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, countryAsJson);
      producer.send(record, (recordMetadata, e) -> {
        log.info("Sending record {}", record.toString().replace("\n", ""));
        if (e != null) {
          log.error("Error: ", e);
        }
      });
    }

    Thread.sleep(15000);
    ProducerRecord<String, String> dummy = new ProducerRecord<>(topic, dummyRecord());
    producer.send(dummy, (r, e) -> log.info("Sending dummy record: {}", dummy.toString()));
  }

  private List<String> data() {
    List<String> data = new ArrayList<>();
    data.addAll(dataFromToday());
    data.addAll(dataFromYesterday());
    return data;
  }

  private List<String> dataFromToday() {
    return dataFrom(Instant.parse(today), 17);
  }

  private List<String> dataFromYesterday() {
    return dataFrom(Instant.parse(today).minus(1, ChronoUnit.DAYS), 1000000);
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
        + "      \"Date\": \"" + Covid19Producer.today + "\"\n"
        + "    }";
  }
}
