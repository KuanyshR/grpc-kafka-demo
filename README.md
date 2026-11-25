# gRPC + Kafka/Avro demo (Java 21, Spring Boot 4, Gradle)

## Part 1 - gRPC Ping/Pong
* `proto/ping_pong.proto` - shared contract
* `grpc-server` (port 8080), `grpc-client` (Java), `python-client` (Python)

    ./gradlew :grpc-server:bootRun
    ./gradlew :grpc-client:bootRun
    cd python-client && pip install -r requirements.txt && python client.py

## Part 2 - Kafka + Avro + Schema Registry
* `avro/` - schemas v1, v2 (compatible), v3 (incompatible)
* `kafka-producer`, `kafka-consumer`, `kafka-common`
* `docker-compose.yml` - Kafka (KRaft, 9092) + Schema Registry (8081) [+ UI 8085 with --profile ui]

    docker compose up -d
    ./kafka/create-topic.sh
    ./gradlew :kafka-consumer:bootRun                                            # terminal 1
    ./gradlew :kafka-producer:bootRun --args='--app.producer.schema-version=v1'  # terminal 2
    ./gradlew :kafka-producer:bootRun --args='--app.producer.schema-version=v2'
    ./gradlew :kafka-producer:bootRun --args='--app.producer.schema-version=v3'  # rejected (409)
    ./kafka/show-schema-registry.sh
    ./kafka/dump-raw-message.sh ping-messages 2

Consumer with another reader schema / from the beginning:

    ./gradlew :kafka-consumer:bootRun --args='--app.consumer.reader-schema-version=v1 --app.consumer.group-id=reader-v1'

Wire format: byte 0 = magic byte (0), bytes 1-4 = schema id, bytes 5.. = Avro data.

Stop infrastructure: `docker compose down` (add `-v` to wipe data).
