package me.jjbuchan.tryflink.tryflink.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class Producers {

  public static FlinkKafkaProducer011<String> createStringProducer(String topic, String kafkaAddress) {
    return new FlinkKafkaProducer011<>(kafkaAddress, topic, new SimpleStringSchema());
  }

}
