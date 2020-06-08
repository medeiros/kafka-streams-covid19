package com.arneam.kafka.streams.covid19api.model;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.time.Instant;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

class CountryTest {

  @Test
  void shouldCreateCountryObject() {
    Country country = Country.builder().country("Brazil")
        .countryCode("BR").slug("brazil")
        .newConfirmed(11923).totalConfirmed(190137)
        .newDeaths(779).totalDeaths(13240)
        .newRecovered(5827).totalRecovered(78424)
        .date(Instant.parse("2020-05-14T08:13:33Z"))
        .build();

    assertThat(country, is(notNullValue()));
    assertThat(country.country(), is(equalTo("Brazil")));
    assertThat(country.countryCode(), is(equalTo("BR")));
    assertThat(country.slug(), is(equalTo("brazil")));
    assertThat(country.newConfirmed(), is(equalTo(11923)));
    assertThat(country.totalConfirmed(), is(equalTo(190137)));
    assertThat(country.newDeaths(), is(equalTo(779)));
    assertThat(country.totalDeaths(), is(equalTo(13240)));
    assertThat(country.newRecovered(), is(equalTo(5827)));
    assertThat(country.totalRecovered(), is(equalTo(78424)));
    assertThat(country.date(), is(equalTo(Instant.parse("2020-05-14T08:13:33Z"))));
  }

  @Test
  void shouldCreateCountryFromJSON() {
    JSONObject jsonObject = new JSONObject(
        "{\"schema\":{\"name\":\"com.arneam.covid19.CountryValue\",\"optional\":false,\"type\":\"struct\",\"fields\":[{\"field\":\"Country\",\"optional\":false,\"type\":\"string\"},{\"field\":\"Slug\",\"optional\":false,\"type\":\"string\"},{\"field\":\"NewConfirmed\",\"optional\":false,\"type\":\"int32\"},{\"field\":\"TotalConfirmed\",\"optional\":false,\"type\":\"int32\"},{\"field\":\"NewDeaths\",\"optional\":false,\"type\":\"int32\"},{\"field\":\"TotalDeaths\",\"optional\":false,\"type\":\"int32\"},{\"field\":\"NewRecovered\",\"optional\":false,\"type\":\"int32\"},{\"field\":\"TotalRecovered\",\"optional\":false,\"type\":\"int32\"},{\"field\":\"Date\",\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"optional\":false,\"type\":\"int64\",\"version\":1}],\"version\":2}"
            + ",\"payload\": {"
            + "      \"Country\": \"Bosnia and Herzegovina\",\n"
            + "      \"CountryCode\": \"BA\",\n"
            + "      \"Slug\": \"bosnia-and-herzegovina\",\n"
            + "      \"NewConfirmed\": 23,\n"
            + "      \"TotalConfirmed\": 2181,\n"
            + "      \"NewDeaths\": 3,\n"
            + "      \"TotalDeaths\": 120,\n"
            + "      \"NewRecovered\": 60,\n"
            + "      \"TotalRecovered\": 1228,\n"
            + "      \"Date\": \"2020-05-14T08:13:33Z\"\n"
            + "    }}");
    Country country = Country.fromJSON(jsonObject);
    assertThat(country, is(notNullValue()));
    assertThat(country.country(), is(equalTo("Bosnia and Herzegovina")));
    assertThat(country.countryCode(), is(equalTo("BA")));
    assertThat(country.slug(), is(equalTo("bosnia-and-herzegovina")));
    assertThat(country.newConfirmed(), is(equalTo(23)));
    assertThat(country.totalConfirmed(), is(equalTo(2181)));
    assertThat(country.newDeaths(), is(equalTo(3)));
    assertThat(country.totalDeaths(), is(equalTo(120)));
    assertThat(country.newRecovered(), is(equalTo(60)));
    assertThat(country.totalRecovered(), is(equalTo(1228)));
    assertThat(country.date(), is(equalTo(Instant.parse("2020-05-14T08:13:33Z"))));
  }

}