package me.jjbuchan.tryflink.tryflink.service;

import static me.jjbuchan.tryflink.tryflink.connector.Consumers.createStringConsumerForTopic;
import static me.jjbuchan.tryflink.tryflink.connector.Producers.createStringProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import me.jjbuchan.tryflink.tryflink.operator.JoinString;
import me.jjbuchan.tryflink.tryflink.operator.WordCapitalizer;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
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

  /**
   * Attempt to group events up until a duplicate item is seen, then output that group
   * and continue processing after cutting out the first event that was duplicated (but while
   * retaining the newest dupe.
   *
   * Experimental method that doesn't work as I'd hoped.
   *
   * @throws Exception
   */
  public static void cep() throws Exception {
    // flatSelect fails when using lambdas; must use an anonymous class instead.
    class MySelect implements PatternFlatSelectFunction<String, String> {
      @Override
      public void flatSelect(Map<String, List<String>> map, Collector<String> collector)
          throws Exception {
        String output = String.format("Evaluated these states %s", String.join(":", map.get("start")));
        collector.collect(output);
      }
    }

    class customEventSorter implements EventComparator<String> {

      @Override
      public int compare(String o1, String o2) {
        return o2.length() - o1.length();
      }
    }

    class NoDupes extends IterativeCondition<String> {
      @Override
      public boolean filter(String s, Context<String> context) throws Exception {
        List<String> start = Lists.newArrayList(context.getEventsForPattern("start").iterator());
        List<String> middle = Lists.newArrayList(context.getEventsForPattern("middle").iterator());
        log.info("NoDupes filter - start = {}", start);
        log.info("NoDupes filter - middle = {}", middle);
        return Stream.concat(start.stream(),middle.stream())
            .peek(state -> {
              log.info("Evaluating seen state {} with new state {}", state, s);
            })
            .anyMatch(state -> state.equals(s));
      }
    }


    log.info("Initializing cep processor");

    String inputTopic = "inputTopic";
    String outputTopic = "outputTopic";
    String consumerGroup = "testGroup";
    String address = "localhost:9092";

    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    log.info("Creating consumer and starting from earliest offset");


    FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(
        inputTopic, address, consumerGroup);
    flinkKafkaConsumer.setStartFromLatest();

    log.info("Creating producer");
    FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(outputTopic, address);

    log.info("Configuring sources");
    DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
    DataStream<String> alertData = environment.fromElements(
        "OK", "CRITICAL", "WARN",
        "WARN",
        "WARN", "OK", "CRITIAL",
        "WARN", "OK");
//        "WARN", "OK", "CRITICAL",
//        "OK", "WARN", "CRITICAL",
//        "WARN", "OK");

    log.info("Processing kafka messages");

    AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToFirst("middle");
    Pattern<String, ?> pattern = Pattern.<String>begin("start")
        .next("middle").oneOrMore().until(new NoDupes())
        .followedBy("end");

    PatternStream<String> patternStream = CEP.pattern(stringInputStream, pattern/*, new customEventSorter()*/);
//    DataStream<String> result = patternStream.flatSelect(new MySelect());
    DataStream<String> result = patternStream.select(
        (PatternSelectFunction<String, String>) map -> {
          // return the first state we saw
          List<String> states = new ArrayList<>();
          if (map.get("start") != null) {
            log.info("States in start={}", map.get("start"));
            states.addAll(map.get("start"));
          }
          if (map.get("middle") != null) {
            log.info("States in middle={}", map.get("middle"));
            states.addAll(map.get("middle"));
          }
          if (map.get("end") != null) {
            log.info("States in end={}", map.get("end"));
//            states.addAll(map.get("end"));
          }

          return String.format("Evaluated these states %s", String.join(":", states));
        }
    );
    result.addSink(flinkKafkaProducer);

    environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
    environment.execute("Flink cep Example");

    // TODO: need to get only the longest pattern and not all patterns
  }

  /**
   * Attempt to group events up until a duplicate item is seen, then output that group
   * and continue processing after cutting out the first event that was duplicated (but while
   * retaining the newest dupe.
   *
   * This fails to act as expected when duplicate events are seen.
   *
   * @throws Exception
   */
  public static void cep2() throws Exception {
    log.info("Initializing cep processor");

    String inputTopic = "inputTopic";
    String outputTopic = "outputTopic";
    String consumerGroup = "testGroup";
    String address = "localhost:9092";

    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    log.info("Creating consumer");
    FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(
        inputTopic, address, consumerGroup);
    flinkKafkaConsumer.setStartFromLatest();

    log.info("Creating producer");
    FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(outputTopic, address);

    log.info("Configuring sources");
    DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);

    log.info("Processing kafka messages");
    AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToFirst("start");
    Pattern<String, ?> pattern = Pattern.<String>begin("start", skipStrategy)
        .oneOrMore()
        .until(new IterativeCondition<>() {
          @Override
          public boolean filter(String s, Context<String> context) throws Exception {
            return StreamSupport.stream(context.getEventsForPattern("start").spliterator(), false)
                .anyMatch(state -> state.equals(s));
          }
        });

    PatternStream<String> patternStream = CEP.pattern(stringInputStream, pattern);
    DataStream<String> result = patternStream.select(
        (PatternSelectFunction<String, String>) map ->
            String.format("Evaluated these states %s", String.join(":", map.get("start")))
    );
    result.addSink(flinkKafkaProducer);

    environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
    environment.execute("Flink cep Example");
  }
}
