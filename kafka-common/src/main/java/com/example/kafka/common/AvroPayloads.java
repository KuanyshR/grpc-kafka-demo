package com.example.kafka.common;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Helpers for the Confluent wire format:
 * <pre>
 *  byte 0      : magic byte (always 0)
 *  bytes 1-4   : schema id (big-endian int) from Schema Registry
 *  bytes 5..n  : Avro binary encoded data
 * </pre>
 */
public final class AvroPayloads {

    public static final byte MAGIC_BYTE = 0x0;
    public static final int HEADER_SIZE = 5;

    private AvroPayloads() {
    }

    public static boolean isConfluentFormat(byte[] payload) {
        return payload != null && payload.length >= HEADER_SIZE && payload[0] == MAGIC_BYTE;
    }

    /** Reads the schema id from bytes 1-4. */
    public static int schemaId(byte[] payload) {
        if (!isConfluentFormat(payload)) {
            throw new IllegalArgumentException("Payload is not in Confluent wire format");
        }
        return ByteBuffer.wrap(payload, 1, 4).getInt();
    }

    public static String hex(byte[] data, int maxBytes) {
        StringBuilder sb = new StringBuilder();
        int limit = Math.min(maxBytes, data.length);
        for (int i = 0; i < limit; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(String.format("%02X", data[i]));
        }
        if (data.length > maxBytes) {
            sb.append(" ...");
        }
        return sb.toString();
    }

    /** Decodes the Avro part of the payload using writer -> reader schema resolution. */
    public static GenericRecord decode(byte[] payload, Schema writerSchema, Schema readerSchema) throws IOException {
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder decoder = DecoderFactory.get()
                .binaryDecoder(payload, HEADER_SIZE, payload.length - HEADER_SIZE, null);
        return datumReader.read(null, decoder);
    }
}
