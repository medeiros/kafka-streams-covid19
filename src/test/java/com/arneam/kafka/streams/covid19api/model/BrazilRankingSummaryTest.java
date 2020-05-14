package com.arneam.kafka.streams.covid19api.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

class BrazilRankingSummaryTest {

  @Test
  void shouldCreateMessage() {
    String expectedMessage =
        "Ranking Covid19 do Brasil no mundo: 1o. em novos casos confirmados; 2o. em "
            + "total de casos confirmados; 3o. em novas mortes; 4o. em total de mortes; "
            + "5o. em novos casos recuperados; 6o. em total de casos recuperados.";
    BrazilRankingSummary summary = BrazilRankingSummary.builder().newConfirmed(1).totalConfirmed(2)
        .newDeaths(3).totalDeaths(4).newRecovered(5).totalRecovered(6).build();
    assertThat(summary.toString(), is(equalTo(expectedMessage)));
  }

}