package com.arneam.kafka.streams.covid19api.model;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;

@Builder
@Accessors(fluent = true)
@Getter
public class BrazilRankingSummary {

  private static final String MESSAGE_FORMAT =
      "Ranking Covid19 do Brasil no mundo: %so. em novos casos confirmados; %so. em "
          + "total de casos confirmados; %so. em novas mortes; %so. em total de mortes; "
          + "%so. em novos casos recuperados; %so. em total de casos recuperados.";

  int newConfirmed;
  int totalConfirmed;
  int newDeaths;
  int totalDeaths;
  int newRecovered;
  int totalRecovered;

  @Override
  public String toString() {
    return String.format(MESSAGE_FORMAT, newConfirmed, totalConfirmed, newDeaths, totalDeaths,
        newRecovered, totalRecovered);
  }
}
