package io.github.rkolesnev.lucenekstreams;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.locationtech.spatial4j.io.jackson.ShapesAsGeoJSONModule;

public class BufferedPersonJsonSerde implements Serializer<BufferedPerson>, Deserializer<BufferedPerson>,
        Serde<BufferedPerson> {

    private static final ObjectMapper OBJECT_MAPPER = objectMapper();

    private static ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        ShapesAsGeoJSONModule shapesAsGeoJSONModule = new ShapesAsGeoJSONModule();
        objectMapper.registerModule(shapesAsGeoJSONModule);
        return objectMapper;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public BufferedPerson deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(data, BufferedPerson.class);
        } catch (final IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(final String topic, final BufferedPerson data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (final Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<BufferedPerson> serializer() {
        return this;
    }

    @Override
    public Deserializer<BufferedPerson> deserializer() {
        return this;
    }
}

