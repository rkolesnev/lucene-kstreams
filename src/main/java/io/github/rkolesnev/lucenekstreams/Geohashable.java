package io.github.rkolesnev.lucenekstreams;

import ch.hsr.geohash.GeoHash;
import org.locationtech.spatial4j.shape.Shape;

public interface Geohashable {

    GeoHash getHash(Shape point);
}
