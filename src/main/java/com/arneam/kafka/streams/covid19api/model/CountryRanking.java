package com.arneam.kafka.streams.covid19api.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

public class CountryRanking implements Serializable {

  public static final String BRAZIL_COUNTRY_NAME = "Brazil";

  private List<Country> countries = new ArrayList<>();

  public void addCountry(Country country) {
    this.countries.add(country);
  }

  public BrazilRankingSummary createSummary() {
    return BrazilRankingSummary.builder()
        .newConfirmed(brazilRanking(Country::newConfirmed))
        .totalConfirmed(brazilRanking(Country::totalConfirmed))
        .newDeaths(brazilRanking(Country::newDeaths))
        .totalDeaths(brazilRanking(Country::totalDeaths))
        .newRecovered(brazilRanking(Country::newRecovered))
        .totalRecovered(brazilRanking(Country::totalRecovered))
        .build();
  }

  int brazilRanking(ToIntFunction<Country> function) {
    countries.sort(Collections.reverseOrder(Comparator.comparingInt(function)));
    Optional<Country> brazil = countries.stream()
        .filter(country -> country.country().equals(BRAZIL_COUNTRY_NAME)).findFirst();
    return countries.indexOf(brazil.get()) + 1;
  }

  @Override
  public String toString() {
    StringBuilder allValues = new StringBuilder("CountryRanking{countries=");
    for (Country country : countries) {
      allValues.append(country.toString()).append("; ");
    }
    allValues.append("}");
    return allValues.toString();
  }
}
