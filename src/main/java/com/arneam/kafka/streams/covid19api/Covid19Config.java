package com.arneam.kafka.streams.covid19api;

import java.io.IOException;
import java.util.Properties;

public class Covid19Config {

  private static final String PROPERTIES_FILE = "/config.properties";
  private static final String TEST_PROPERTIES_FILE = "/config-test.properties";

  public static Properties config() {
    return configuration(PROPERTIES_FILE);
  }

  public static Properties configTest() {
    return configuration(TEST_PROPERTIES_FILE);
  }

  private static Properties configuration(String type) {
    Properties properties = new Properties();
    try {
      properties.load(Covid19Config.class.getResourceAsStream(defineResource(type)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return properties;
  }

  private static String defineResource(String type) {
    String resource = "";
    switch (type) {
      case PROPERTIES_FILE:
        resource = PROPERTIES_FILE;
        break;
      case TEST_PROPERTIES_FILE:
        resource = TEST_PROPERTIES_FILE;
        break;
      default:
        throw new IllegalArgumentException("Invalid property file type");
    }
    return resource;
  }

}
