package com.arneam.kafka.streams.covid19api.model;

import java.io.Serializable;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import org.json.JSONObject;

@Builder
@Accessors(fluent = true)
@Value
@ToString
public class Country implements Serializable {

  String country;
  String countryCode;
  String slug;
  int newConfirmed;
  int totalConfirmed;
  int newDeaths;
  int totalDeaths;
  int newRecovered;
  int totalRecovered;
  String date;

  public static Country fromJSON(JSONObject jsonObject) {
    return Country.builder()
        .country(payloadOf(jsonObject).getString(CountrySchema.COUNTRY_FIELD))
        .countryCode(payloadOf(jsonObject).getString(CountrySchema.COUNTRY_CODE_FIELD))
        .slug(payloadOf(jsonObject).getString(CountrySchema.SLUG_FIELD))
        .newConfirmed(payloadOf(jsonObject).getInt(CountrySchema.NEW_CONFIRMED_FIELD))
        .totalConfirmed(payloadOf(jsonObject).getInt(CountrySchema.TOTAL_CONFIRMED_FIELD))
        .newDeaths(payloadOf(jsonObject).getInt(CountrySchema.NEW_DEATHS_FIELD))
        .totalDeaths(payloadOf(jsonObject).getInt(CountrySchema.TOTAL_DEATHS_FIELD))
        .newRecovered(payloadOf(jsonObject).getInt(CountrySchema.NEW_RECOVERED_FIELD))
        .totalRecovered(payloadOf(jsonObject).getInt(CountrySchema.TOTAL_RECOVERED_FIELD))
        .date(payloadOf(jsonObject).getString(CountrySchema.DATE_FIELD)).build();
  }

  private static JSONObject payloadOf(JSONObject jsonObject) {
    return jsonObject.getJSONObject("payload");
  }

}
