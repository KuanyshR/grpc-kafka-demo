package com.example.kafka.common;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroSchemasTest {

    private static SchemaCompatibilityType compat(String reader, String writer) {
        return SchemaCompatibility.checkReaderWriterCompatibility(
                AvroSchemas.load(reader), AvroSchemas.load(writer)).getType();
    }

    @Test
    void v2ReaderCanReadV1Data() {
        assertEquals(SchemaCompatibilityType.COMPATIBLE, compat("v2", "v1"));
    }

    @Test
    void v1ReaderCanReadV2Data() {
        assertEquals(SchemaCompatibilityType.COMPATIBLE, compat("v1", "v2"));
    }

    @Test
    void v3ReaderCannotReadV2Data() {
        assertEquals(SchemaCompatibilityType.INCOMPATIBLE, compat("v3", "v2"));
    }

    @Test
    void extractsSchemaIdFromBytes1To4() {
        byte[] payload = {0x00, 0x00, 0x00, 0x01, 0x02, 0x09};
        assertEquals(258, AvroPayloads.schemaId(payload));
    }

    @Test
    void decodesV1PayloadWithV2ReaderUsingDefaults() throws IOException {
        Schema v1 = AvroSchemas.load("v1");
        Schema v2 = AvroSchemas.load("v2");
        GenericRecord record = new GenericRecordBuilder(v1)
                .set("id", "1").set("message", "Ping").set("timestamp", 1L).build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(new byte[]{0, 0, 0, 0, 7});            // magic byte + schema id 7
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(v1).write(record, encoder);
        encoder.flush();

        GenericRecord decoded = AvroPayloads.decode(out.toByteArray(), v1, v2);

        assertEquals("Ping", decoded.get("message").toString());
        assertEquals("unknown", decoded.get("source").toString());
        assertEquals(0, decoded.get("priority"));
    }
}
