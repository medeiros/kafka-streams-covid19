package com.arneam.kafka.streams.covid19api.model;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.time.Instant;
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
  }

}