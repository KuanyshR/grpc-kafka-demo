package com.example.kafka.producer;

import com.example.kafka.common.AvroPayloads;
import com.example.kafka.common.AvroSchemas;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Serializes messages with KafkaAvroSerializer (registers the schema in Schema Registry),
 * prints the Confluent wire-format header and sends the bytes to Kafka.
 */
@Component
public class AvroProducerRunner implements CommandLineRunner {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String topic;
    private final String schemaVersion;
    private final int count;
    private final String message;

    public AvroProducerRunner(@Value("${app.kafka.bootstrap-servers}") String bootstrapServers,
                              @Value("${app.kafka.schema-registry-url}") String schemaRegistryUrl,
                              @Value("${app.kafka.topic}") String topic,
                              @Value("${app.producer.schema-version}") String schemaVersion,
                              @Value("${app.producer.count}") int count,
                              @Value("${app.producer.message}") String message) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
        this.schemaVersion = schemaVersion;
        this.count = count;
        this.message = message;
    }

    @Override
    public void run(String... args) throws Exception {
        Schema schema = AvroSchemas.load(schemaVersion);
        String subject = topic + "-value"; // TopicNameStrategy

        SchemaRegistryClient registry = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        Map<String, Object> serdeConfig = Map.of(
                "schema.registry.url", schemaRegistryUrl,
                "auto.register.schemas", true);

        System.out.printf(">>> Producing %d message(s) to topic '%s' with WRITER schema %s%n",
                count, topic, schemaVersion);
        System.out.println("    schema fields: " + AvroSchemas.fieldNames(schema));

        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer(registry, serdeConfig);
             KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps())) {

            for (int i = 1; i <= count; i++) {
                GenericRecord record = buildRecord(schema, i);

                byte[] payload;
                try {
                    payload = serializer.serialize(topic, record);
                } catch (SerializationException e) {
                    System.err.println("!!! Schema Registry rejected schema " + schemaVersion
                            + ": " + rootCauseMessage(e));
                    throw e;
                }

                int schemaId = AvroPayloads.schemaId(payload);
                int subjectVersion = registry.getVersion(subject, new AvroSchema(schema));

                RecordMetadata md = producer
                        .send(new ProducerRecord<>(topic, record.get("id").toString(), payload))
                        .get();

                System.out.println("------------------------------------------------------------------");
                System.out.printf("[%d] record        : %s%n", i, record);
                System.out.printf("    registry      : subject=%s, version=%d, schema id=%d%n",
                        subject, subjectVersion, schemaId);
                System.out.printf("    payload bytes : %s%n", AvroPayloads.hex(payload, 24));
                System.out.printf("    byte 0        : 0x%02X (magic byte)%n", payload[0]);
                System.out.printf("    bytes 1-4     : %s -> schema id %d%n",
                        AvroPayloads.hex(java.util.Arrays.copyOfRange(payload, 1, 5), 4), schemaId);
                System.out.printf("    sent to       : partition=%d, offset=%d (%d bytes)%n",
                        md.partition(), md.offset(), payload.length);
            }
            producer.flush();
        }
        System.out.println(">>> Done.");
    }

    private GenericRecord buildRecord(Schema schema, int index) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema)
                .set("id", UUID.randomUUID().toString())
                .set("message", message)
                .set("timestamp", System.currentTimeMillis());
        // Optional fields - only set if the chosen schema version has them
        setIfPresent(builder, schema, "source", "java-producer");
        setIfPresent(builder, schema, "priority", index % 3);
        setIfPresent(builder, schema, "region", "eu-central-1");
        return builder.build();
    }

    private static void setIfPresent(GenericRecordBuilder builder, Schema schema, String field, Object value) {
        if (schema.getField(field) != null) {
            builder.set(field, value);
        }
    }

    private Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-avro-producer");
        return props;
    }

    private static String rootCauseMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage();
    }
}
