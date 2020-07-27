package me.jjbuchan.tryflink.tryflink.service;

import static me.jjbuchan.tryflink.tryflink.connector.Consumers.createStringConsumerForTopic;
import static me.jjbuchan.tryflink.tryflink.connector.Producers.createStringProducer;

import lombok.extern.slf4j.Slf4j;
import me.jjbuchan.tryflink.tryflink.operator.JoinString;
import me.jjbuchan.tryflink.tryflink.operator.WordCapitalizer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DataProcessor {

  /**
   * Simple method that consumes string messages from kafka, converts them to uppercase,
   * then produces them back to kafka.
   *
   * @throws Exception
   */
  public static void capitalize() throws Exception {
    log.info("Initializing capitalize processor");

    String inputTopic = "inputTopic";
    String outputTopic = "outputTopic";
    String consumerGroup = "testGroup";
    String address = "localhost:9092";

    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    log.info("Creating consumer and starting from earliest offset");
    FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(
        inputTopic, address, consumerGroup);
    flinkKafkaConsumer.setStartFromEarliest();

    log.info("Creating producer");
    FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(outputTopic, address);

    log.info("Configuring sources");
    DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
    DataStream<String> stringInputStream2 = environment.fromElements("asas", "sdsdsda", "TET", "34");

    log.info("Processing kafka messages");
    stringInputStream.union(stringInputStream2)
        .map(new WordCapitalizer())
        .addSink(flinkKafkaProducer);

    environment.execute("Flink capitalize Example");
  }

  public static void join() throws Exception {
    log.info("Initializing join processor");

    String inputTopic = "inputTopic";
    String outputTopic = "outputTopic";
    String consumerGroup = "testGroup";
    String address = "localhost:9092";

    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    log.info("Creating consumer and starting from earliest offset");
    FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(
        inputTopic, address, consumerGroup);
    flinkKafkaConsumer.setStartFromEarliest();

    log.info("Creating producer");
    FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(outputTopic, address);

    log.info("Configuring sources");
    DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
    DataStream<String> alertData = environment.fromElements("OK", "CRITICAL", "WARN");

    log.info("Processing kafka messages");
    stringInputStream.union(alertData)
        .keyBy(s -> s.charAt(0))
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
        .aggregate(new JoinString())
        .addSink(flinkKafkaProducer);

    environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
    environment.execute("Flink join Example");
  }


}
