package com.arneam.kafka.streams.covid19api.model;

import java.time.Instant;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.json.JSONObject;

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

  public static Country fromJSON(JSONObject jsonObject) {
    return Country.builder()
        .country(jsonObject.getString(CountrySchema.COUNTRY_FIELD))
        .countryCode(jsonObject.getString(CountrySchema.COUNTRY_CODE_FIELD))
        .slug(jsonObject.getString(CountrySchema.SLUG_FIELD))
        .newConfirmed(jsonObject.getInt(CountrySchema.NEW_CONFIRMED_FIELD))
        .totalConfirmed(jsonObject.getInt(CountrySchema.TOTAL_CONFIRMED_FIELD))
        .newDeaths(jsonObject.getInt(CountrySchema.NEW_DEATHS_FIELD))
        .totalDeaths(jsonObject.getInt(CountrySchema.TOTAL_DEATHS_FIELD))
        .newRecovered(jsonObject.getInt(CountrySchema.NEW_RECOVERED_FIELD))
        .totalRecovered(jsonObject.getInt(CountrySchema.TOTAL_RECOVERED_FIELD))
        .date(Instant.parse(jsonObject.getString(CountrySchema.DATE_FIELD))).build();
  }
}
