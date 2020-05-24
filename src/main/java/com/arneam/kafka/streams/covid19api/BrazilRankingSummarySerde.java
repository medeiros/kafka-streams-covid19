package com.arneam.kafka.streams.covid19api;

import com.arneam.kafka.streams.covid19api.model.BrazilRankingSummary;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class BrazilRankingSummarySerde implements Serializer<BrazilRankingSummary>,
    Deserializer<BrazilRankingSummary>, Serde<BrazilRankingSummary> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public Serializer<BrazilRankingSummary> serializer() {
    return this;
  }

  @Override
  public Deserializer<BrazilRankingSummary> deserializer() {
    return this;
  }

  @Override
  public BrazilRankingSummary deserialize(String s, byte[] bytes) {
    ObjectInputStream stream;
    BrazilRankingSummary summary;
    try {
      stream = new ObjectInputStream(new ByteArrayInputStream(bytes));
      summary = (BrazilRankingSummary) stream.readObject();
      stream.close();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return summary;
  }

  @Override
  public byte[] serialize(String s, BrazilRankingSummary summary) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      ObjectOutputStream objectOutputStream;
      objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(summary);
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
