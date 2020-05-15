package com.arneam.kafka.streams.covid19api;

import com.arneam.kafka.streams.covid19api.model.Country;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CountrySerde implements Serializer<Country>, Deserializer<Country>, Serde<Country> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public Serializer<Country> serializer() {
    return this;
  }

  @Override
  public Deserializer<Country> deserializer() {
    return this;
  }

  @Override
  public Country deserialize(String s, byte[] bytes) {
    ObjectInputStream stream;
    Country country;
    try {
      stream = new ObjectInputStream(new ByteArrayInputStream(bytes));
      country = (Country) stream.readObject();
      stream.close();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return country;
  }

  @Override
  public byte[] serialize(String s, Country country) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      ObjectOutputStream objectOutputStream;
      objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(country);
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
