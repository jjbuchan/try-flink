package me.jjbuchan.tryflink.tryflink.connector;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

public class Consumers {

  public static FlinkKafkaConsumer011<String> createStringConsumerForTopic(
      String topic, String kafkaAddress, String kafkaGroup) {

    Properties props = new Properties();
    props.setProperty("bootstrap.servers", kafkaAddress);
    props.setProperty("group.id", kafkaGroup);

    return new FlinkKafkaConsumer011<>(
        topic, new SimpleStringSchema(), props);
  }
}
