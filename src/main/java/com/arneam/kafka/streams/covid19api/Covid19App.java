package com.arneam.kafka.streams.covid19api;

import com.arneam.kafka.streams.covid19api.model.BrazilRankingSummary;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public class Covid19App {

  public static void main(String[] args) {

    KStream<String, String> stream = new StreamsBuilder().stream("covid19.rawdata");

    //KTable<String, BrazilRankingSummary>  summary = stream.mapValues()

  }

}
