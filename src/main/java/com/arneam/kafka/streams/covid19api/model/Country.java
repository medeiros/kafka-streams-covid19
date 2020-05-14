package com.arneam.kafka.streams.covid19api.model;

import java.time.Instant;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Builder
@Accessors(fluent = true)
@Value
public class Country {

  String country;
  String countryCode;
  String slug;
  int newConfirmed;
  int totalConfirmed;
  int newDeaths;
  int totalDeaths;
  int newRecovered;
  int totalRecovered;
  Instant date;

}
