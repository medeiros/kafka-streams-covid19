package com.arneam.kafka.streams.covid19api;

import com.arneam.kafka.streams.covid19api.model.Country;
import com.arneam.kafka.streams.covid19api.model.CountryRanking;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CountryRankingSerde implements Serializer<CountryRanking>, Deserializer<CountryRanking>, Serde<CountryRanking> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public Serializer<CountryRanking> serializer() {
    return this;
  }

  @Override
  public Deserializer<CountryRanking> deserializer() {
    return this;
  }

  @Override
  public CountryRanking deserialize(String s, byte[] bytes) {
    ObjectInputStream stream;
    CountryRanking countryRanking;
    try {
      stream = new ObjectInputStream(new ByteArrayInputStream(bytes));
      countryRanking = (CountryRanking) stream.readObject();
      stream.close();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return countryRanking;
  }

  @Override
  public byte[] serialize(String s, CountryRanking countryRanking) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      ObjectOutputStream objectOutputStream;
      objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(countryRanking);
      objectOutputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return byteArrayOutputStream.toByteArray();
  }

  @Override
  public void close() {
  }

}
