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

  public CountryRanking() {
  }

  public CountryRanking(CountryRanking cr1, CountryRanking cr2) {
    countries.addAll(cr1.countries);
    countries.addAll(cr2.countries);
  }

  public CountryRanking addCountry(Country country) {
    this.countries.add(country);
    return this;
  }

  public List<Country> getCountries() {
    return Collections.unmodifiableList(countries);
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

  private int brazilRanking(ToIntFunction<Country> function) {
    countries.sort(Collections.reverseOrder(Comparator.comparingInt(function)));
    Optional<Country> brazil = countries.stream()
        .filter(country -> country.country().equals(BRAZIL_COUNTRY_NAME)).findFirst();
    return brazil.map(country -> countries.indexOf(country) + 1).orElse(0);
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
