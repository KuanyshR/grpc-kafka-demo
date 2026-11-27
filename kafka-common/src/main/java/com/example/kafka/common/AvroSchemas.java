package com.example.kafka.common;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Loads the shared Avro schemas (avro/ping-message-v1.avsc, -v2, -v3) from the classpath.
 */
public final class AvroSchemas {

    private AvroSchemas() {
    }

    public static Schema load(String version) {
        String resource = "/avro/ping-message-" + version.trim().toLowerCase() + ".avsc";
        try (InputStream in = AvroSchemas.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException(
                        "Unknown schema version '" + version + "'. Available: v1, v2, v3");
            }
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + resource, e);
        }
    }

    public static List<String> fieldNames(Schema schema) {
        return schema.getFields().stream().map(Schema.Field::name).toList();
    }
}
