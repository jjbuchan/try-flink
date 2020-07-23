Experimenting with Flink by following the article at https://www.baeldung.com/kafka-flink-data-pipeline.

## Running

```
# Start Kafka
docker-compose up -d

# Run App
mvn spring-boot:run

# Consume output topic (in new window)
docker exec -it try-flink_kafka_1 \
       kafka-console-consumer --bootstrap-server localhost:9092 --topic outputTopic

# Produce to input topic (in new window)

docker exec -it try-flink_kafka_1 \
       kafka-console-producer --topic inputTopic --broker-list localhost:9092

>this is a test
>This Is A Test
>blah,blah..blah-

# View messages in consumer window
THIS IS A TEST
THIS IS A TEST
BLAH,BLAH..BLAH-
```