package com.arneam.kafka.streams.covid19api.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class CountryRanking implements Serializable {

  public List<Country> countries = new ArrayList<>();

  public BrazilRankingSummary createSummary() {
    BrazilRankingSummary brazilRankingSummary = BrazilRankingSummary.builder()
        .newConfirmed(brazilRankindNewConfirmed((countries)))
        .totalConfirmed(brazilRankindTotalConfirmed(countries))
        .newDeaths(brazilRankindNewDeaths(countries))
        .totalDeaths(brazilRankindTotalDeaths(countries))
        .newRecovered(brazilRankindNewRecovered(countries))
        .totalRecovered(brazilRankindTotalRecovered(countries))
        .build();
    return brazilRankingSummary;
  }

  int brazilRankindNewConfirmed(List<Country> countries) {
    Collections.sort(countries,
        Collections.reverseOrder(Comparator.comparingInt(Country::newConfirmed)));
    Optional<Country> brazil = countries.stream()
        .filter(country -> country.country().equals("Brazil")).findFirst();
    return countries.indexOf(brazil.get()) + 1;
  }

  int brazilRankindTotalConfirmed(List<Country> countries) {
    Collections.sort(countries,
        Collections.reverseOrder(Comparator.comparingInt(Country::totalConfirmed)));
    Optional<Country> brazil = countries.stream()
        .filter(country -> country.country().equals("Brazil")).findFirst();
    return countries.indexOf(brazil.get()) + 1;
  }

  int brazilRankindNewDeaths(List<Country> countries) {
    Collections
        .sort(countries, Collections.reverseOrder(Comparator.comparingInt(Country::newDeaths)));
    Optional<Country> brazil = countries.stream()
        .filter(country -> country.country().equals("Brazil")).findFirst();
    return countries.indexOf(brazil.get()) + 1;
  }

  int brazilRankindTotalDeaths(List<Country> countries) {
    Collections.sort(countries,
        Collections.reverseOrder(Comparator.comparingInt(Country::totalDeaths)));
    Optional<Country> brazil = countries.stream()
        .filter(country -> country.country().equals("Brazil")).findFirst();
    return countries.indexOf(brazil.get()) + 1;
  }

  int brazilRankindNewRecovered(List<Country> countries) {
    Collections.sort(countries,
        Collections.reverseOrder(Comparator.comparingInt(Country::newRecovered)));
    Optional<Country> brazil = countries.stream()
        .filter(country -> country.country().equals("Brazil")).findFirst();
    return countries.indexOf(brazil.get()) + 1;
  }

  int brazilRankindTotalRecovered(List<Country> countries) {
    Collections.sort(countries,
        Collections.reverseOrder(Comparator.comparingInt(Country::totalRecovered)));
    Optional<Country> brazil = countries.stream()
        .filter(country -> country.country().equals("Brazil")).findFirst();
    return countries.indexOf(brazil.get()) + 1;
  }


}
