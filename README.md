# Kafka Spring Boot JSON Serialization Demo Project

Spring Boot application demonstrating serializing and deserializing messages in that are in the JSON format.

This repo accompanies the following article: 

- [Kafka JSON Serialization](coming soon)

## Build

With Java version 17:

```
mvn clean install
```

## Run Spring Boot Application

### Run docker containers

From root dir run the following to start dockerised Kafka, Zookeeper:
```
docker-compose up -d
```

### Start demo spring boot application
```
java -jar target/kafka-json-serialization-1.0.0.jar
```

### Produce an inbound event:

Jump onto Kafka docker container:
```
docker exec -ti kafka bash
```

Produce a demo-inbound message:
```
kafka-console-producer \
--topic demo-inbound-topic \
--broker-list kafka:29092 \
--property "key.separator=;" \
--property parse.key=true
```
Now enter the message (with key prefix):
```
{"primaryId":"f6914736-bbd1-4a4b-b1a7-58fac2d0e4d2", "secondaryId": "c081f385-38e4-4a1d-a93e-5412eeb0121d"};{"id": "168f141e-93ae-427f-9e88-2eda6ac5823d", "inboundData": "my-data"}
```
The demo-inbound message is consumed by the application, which emits a resulting demo-outbound message.

Check for the emitted demo-outbound message:
```
kafka-console-consumer \
--topic demo-outbound-topic \
--bootstrap-server kafka:29092 \
--from-beginning \
--property print.key=true \
--property key.separator=";"
```
Output:
```
{"id":"a210c3f0-a2e9-4d0d-8928-9c20549bbbd8","outboundData":"my-data"}
```

### Command Line Tools

#### View topics

Jump on to Kafka docker container:
```
docker exec -ti kafka bash
```

List topics:
```
kafka-topics --list --bootstrap-server localhost:9092
```

## Integration Tests

Run integration tests with `mvn clean test`

The tests demonstrate sending JSON events to an embedded in-memory Kafka that are consumed by the application, resulting in outbound JSON events being published.

## Component Tests

The tests demonstrate sending JSON events to a dockerised Kafka that are consumed by the dockerised application, resulting in outbound JSON events being published.

For more on the component tests see: https://github.com/lydtechconsulting/component-test-framework

Build Spring Boot application jar:
```
mvn clean install
```

Build Docker container:
```
docker build -t ct/kafka-json-serialization:latest .
```

Run tests:
```
mvn test -Pcomponent
```

Run tests leaving containers up:
```
mvn test -Pcomponent -Dcontainers.stayup
```

Manual clean up (if left containers up):
```
docker rm -f $(docker ps -aq)
```

Further docker clean up if network/other issues:
```
docker system prune
docker volume prune
```
