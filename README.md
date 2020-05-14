# Kafka Streams for Covid19API

This application get raw data from Covid19API (https://covid19api.com/summary) and 
summarize daily data in terms of Brazil ranking in the world (number of: new cases, total cases,
new deaths, total death, new recovered and total recovered).

It is intended to work with kafka-connect-covid19api Connector Source (https://github.com/medeiros/kafka-connect-covid19api) 
and Twitter Connector Sink (https://github.com/Eneco/kafka-connect-twitter).  

# Topology

```
1. STREAM -> <null, countriesJSONArray>
2. MAPVALUES -> <null, List<Country>>
3. MAPVALUES NewConfirmed -> <null, BrazilRankingSummary>
4. MAPVALUES TotalConfirmed -> <null, BrazilRankingSummary>
5. MAPVALUES NewDeaths -> <null, BrazilRankingSummary>
6. MAPVALUES TotalDeaths -> <null, BrazilRankingSummary>
7. MAPVALUES NewRecovered -> <null, BrazilRankingSummary>
8. MAPVALUES TotalRecovered -> <null, BrazilRankingSummary>
9. TO Kafka -> <null, BrazilRankingSummary>
```