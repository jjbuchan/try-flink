package me.jjbuchan.tryflink.tryflink.service;

import static me.jjbuchan.tryflink.tryflink.connector.Consumers.createStringConsumerForTopic;
import static me.jjbuchan.tryflink.tryflink.connector.Producers.createStringProducer;

import lombok.extern.slf4j.Slf4j;
import me.jjbuchan.tryflink.tryflink.operator.JoinString;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

@Configuration
@Slf4j
public class DataProcessor {

  /**
   * Simple method that consumes string messages from kafka, converts them to uppercase,
   * then produces them back to kafka.
   *
   * @throws Exception
   */
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

    DataStream<String> stringInputStream2 = environment.fromElements("asas", "sdsdsda", "TET", "34");




    DataStream<String> alertData = environment.fromElements("OK", "CRITICAL", "WARN");

    FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(outputTopic, address);
    log.info("Created producer");

    log.info("Processing kafka messages");
//    stringInputStream.union(stringInputStream2)
//        .map(new WordCapitalizer())
//        .addSink(flinkKafkaProducer);

    stringInputStream.union(alertData)
        .keyBy(s -> s.charAt(0))
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
        .aggregate(new JoinString())
        .addSink(flinkKafkaProducer);

    environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
    environment.execute("Flink Kafka Example");
  }
}
