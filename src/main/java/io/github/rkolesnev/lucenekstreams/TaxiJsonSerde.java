package io.github.rkolesnev.lucenekstreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.locationtech.spatial4j.io.jackson.ShapesAsGeoJSONModule;

import java.io.IOException;
import java.util.Map;

public class TaxiJsonSerde implements Serializer<Taxi>, Deserializer<Taxi>,
        Serde<Taxi> {

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
    public Taxi deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(data, Taxi.class);
        } catch (final IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(final String topic, final Taxi data) {
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
    public Serializer<Taxi> serializer() {
        return this;
    }

    @Override
    public Deserializer<Taxi> deserializer() {
        return this;
    }
}
