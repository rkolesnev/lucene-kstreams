package io.github.rkolesnev.lucenekstreams;

import ch.hsr.geohash.GeoHash;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.locationtech.spatial4j.io.jackson.ShapeDeserializer;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class Taxi implements Geohashable{
    TaxiId id;
    @JsonDeserialize(using = ShapeDeserializer.class)
    Point position;

    @Override
    public GeoHash getHash(Shape point) {
        return null;
    }
}
