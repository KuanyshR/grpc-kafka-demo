package com.example.kafka.consumer;

import com.example.kafka.common.AvroPayloads;
import com.example.kafka.common.AvroSchemas;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads raw bytes from Kafka, shows the Confluent wire format (magic byte + schema id in bytes 1-4),
 * and deserializes the message both with the WRITER schema and with the consumer's own READER schema.
 */
@Component
public class AvroConsumerRunner implements CommandLineRunner, DisposableBean {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String topic;
    private final String groupId;
    private final String readerVersion;
    private final Schema readerSchema;
    private final int maxMessages;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile KafkaConsumer<String, byte[]> consumer;

    public AvroConsumerRunner(@Value("${app.kafka.bootstrap-servers}") String bootstrapServers,
                              @Value("${app.kafka.schema-registry-url}") String schemaRegistryUrl,
                              @Value("${app.kafka.topic}") String topic,
                              @Value("${app.consumer.group-id}") String groupId,
                              @Value("${app.consumer.reader-schema-version}") String readerVersion,
                              @Value("${app.consumer.max-messages}") int maxMessages) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
        this.groupId = groupId;
        this.readerVersion = readerVersion;
        this.readerSchema = AvroSchemas.load(readerVersion);
        this.maxMessages = maxMessages;
    }

    @Override
    public void run(String... args) {
        SchemaRegistryClient registry = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        Map<String, Object> serdeConfig = Map.of("schema.registry.url", schemaRegistryUrl);

        System.out.printf("<<< Listening on topic '%s' (group '%s'), READER schema %s %s%n",
                topic, groupId, readerVersion, AvroSchemas.fieldNames(readerSchema));
        System.out.println("    Press Ctrl+C to stop.");

        int received = 0;
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(consumerProps());
             KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(registry, serdeConfig)) {
            consumer = c;
            c.subscribe(List.of(topic));

            outer:
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    handle(record, registry, deserializer);
                    received++;
                    if (maxMessages > 0 && received >= maxMessages) {
                        break outer;
                    }
                }
            }
        } catch (WakeupException e) {
            // expected on shutdown
        }
        System.out.printf("<<< Consumer stopped after %d message(s).%n", received);
    }

    private void handle(ConsumerRecord<String, byte[]> record,
                        SchemaRegistryClient registry,
                        KafkaAvroDeserializer deserializer) {
        byte[] payload = record.value();
        String subject = record.topic() + "-value";

        System.out.println("==================================================================");
        System.out.printf("<<< partition=%d offset=%d key=%s%n", record.partition(), record.offset(), record.key());

        if (payload == null) {
            System.out.println("    (null value)");
            return;
        }
        System.out.println("    raw bytes      : " + AvroPayloads.hex(payload, 24));
        if (!AvroPayloads.isConfluentFormat(payload)) {
            System.out.println("    not in Confluent Avro wire format - skipping");
            return;
        }

        int schemaId = AvroPayloads.schemaId(payload);
        System.out.printf("    byte 0         : 0x%02X (magic byte)%n", payload[0]);
        System.out.printf("    bytes 1-4      : %s -> schema id = %d%n",
                AvroPayloads.hex(Arrays.copyOfRange(payload, 1, 5), 4), schemaId);
        System.out.printf("    bytes 5-%-6d : Avro binary data (%d bytes)%n",
                payload.length - 1, payload.length - AvroPayloads.HEADER_SIZE);

        try {
            AvroSchema writer = (AvroSchema) registry.getSchemaById(schemaId);
            int version = registry.getVersion(subject, writer);
            System.out.printf("    writer schema  : id=%d, subject=%s, version=%d, fields=%s%n",
                    schemaId, subject, version, AvroSchemas.fieldNames(writer.rawSchema()));

            // 1) Standard Confluent deserialization -> record in the WRITER schema
            Object writerView = deserializer.deserialize(record.topic(), payload);
            System.out.println("    WRITER view    : " + writerView);

            // 2) Schema resolution: WRITER schema -> consumer's READER schema
            GenericRecord readerView = AvroPayloads.decode(payload, writer.rawSchema(), readerSchema);
            System.out.println("    READER view " + readerVersion + " : " + readerView);
        } catch (AvroRuntimeException e) {
            System.out.println("    !!! cannot resolve writer schema -> reader schema " + readerVersion
                    + ": " + e.getMessage());
        } catch (Exception e) {
            System.out.println("    !!! failed to process message: " + e);
        }
    }

    private Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-avro-consumer");
        return props;
    }

    @Override
    public void destroy() {
        running.set(false);
        KafkaConsumer<String, byte[]> c = consumer;
        if (c != null) {
            c.wakeup();
        }
    }
}
