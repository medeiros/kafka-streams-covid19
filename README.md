# Kafka Streams for Covid19API

This application get raw data from Covid19API (https://covid19api.com/summary) and 
summarize daily data in terms of Brazil ranking in the world (number of: new cases, total cases,
new deaths, total death, new recovered and total recovered).

It is intended to work with kafka-connect-covid19api Connector Source (https://github.com/medeiros/kafka-connect-covid19api) 
and Twitter Connector Sink (https://github.com/Eneco/kafka-connect-twitter).  

# Topology

1. STREAM -> <null, country>
2. 