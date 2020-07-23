package me.jjbuchan.tryflink.tryflink.service;

import static me.jjbuchan.tryflink.tryflink.connector.Consumers.createStringConsumerForTopic;
import static me.jjbuchan.tryflink.tryflink.connector.Producers.createStringProducer;

import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import me.jjbuchan.tryflink.tryflink.operator.WordCapitalizer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Configuration
@Slf4j
public class DataProcessor {

  @EventListener(ApplicationReadyEvent.class)
  public static void capitalize() throws Exception {
    String inputTopic = "inputTopic";
    String outputTopic = "outputTopic";
    String consumerGroup = "testGroup";
    String address = "localhost:9092";

    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(
        inputTopic, address, consumerGroup);

    flinkKafkaConsumer.setStartFromEarliest();
    log.info("Created consumer and starting from earliest offset");

    DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);

    FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(outputTopic, address);
    log.info("Created producer");

    log.info("Processing kafka messages");
    stringInputStream
        .map(new WordCapitalizer())
        .addSink(flinkKafkaProducer);

    environment.execute("Flink Kafka Example");
  }
}
