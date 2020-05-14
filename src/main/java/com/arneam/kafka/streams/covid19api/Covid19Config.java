package com.arneam.kafka.streams.covid19api;

import java.io.IOException;
import java.util.Properties;

public class Covid19Config {

  private static final String PROPERTIES_FILE = "/config.properties";

  public static Properties config() {
    Properties properties = new Properties();
    try {
      properties.load(Covid19Config.class.getResourceAsStream(PROPERTIES_FILE));
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
    return properties;
  }

}
