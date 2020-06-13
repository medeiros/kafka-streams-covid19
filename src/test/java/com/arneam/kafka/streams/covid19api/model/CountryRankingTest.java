package com.arneam.kafka.streams.covid19api.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CountryRankingTest {

  private static List<Country> countries;

  @BeforeAll
  static void init() {
    countries = new ArrayList<>();
    countries.add(Country.builder().country("Brazil")
        .countryCode("BR").slug("brazil")
        .newConfirmed(11923).totalConfirmed(190137)
        .newDeaths(779).totalDeaths(13240)
        .newRecovered(5827).totalRecovered(78424)
        .date("2020-05-14T08:13:33Z")
        .build());
    countries.add(Country.builder().country("Argentina")
        .countryCode("AR").slug("argentina")
        .newConfirmed(10).totalConfirmed(11)
        .newDeaths(10000).totalDeaths(100000)
        .newRecovered(12).totalRecovered(13)
        .date("2020-05-14T08:13:33Z")
        .build());
    countries.add(Country.builder().country("Colombia")
        .countryCode("CO").slug("colombia")
        .newConfirmed(13).totalConfirmed(13)
        .newDeaths(12).totalDeaths(12)
        .newRecovered(6000).totalRecovered(13)
        .date("2020-05-14T08:13:33Z")
        .build());
  }

  @Test
  void shouldCalculateBrazilRanking() {
    CountryRanking countryRanking = new CountryRanking();
    countryRanking.addCountry(countries.get(0));
    countryRanking.addCountry(countries.get(1));
    countryRanking.addCountry(countries.get(2));
    BrazilRankingSummary brazilRanking = countryRanking.createSummary();
    assertThat(brazilRanking.newConfirmed(), is(equalTo(1)));
    assertThat(brazilRanking.totalConfirmed(), is(equalTo(1)));
    assertThat(brazilRanking.newDeaths(), is(equalTo(2)));
    assertThat(brazilRanking.totalDeaths(), is(equalTo(2)));
    assertThat(brazilRanking.newRecovered(), is(equalTo(2)));
    assertThat(brazilRanking.totalRecovered(), is(equalTo(1)));
  }

}