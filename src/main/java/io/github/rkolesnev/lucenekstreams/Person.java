package io.github.rkolesnev.lucenekstreams;

import ch.hsr.geohash.GeoHash;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.locationtech.spatial4j.io.jackson.ShapeDeserializer;
import org.locationtech.spatial4j.shape.Point;

import org.locationtech.spatial4j.shape.Shape;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Person implements Geohashable {

    PersonId personId;
    @JsonDeserialize(using = ShapeDeserializer.class)
    Point position;

    @Override
    public GeoHash getHash(Shape point) {
        return null;
    }


}
