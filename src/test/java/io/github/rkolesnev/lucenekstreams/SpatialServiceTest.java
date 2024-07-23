package io.github.rkolesnev.lucenekstreams;

import static com.google.common.truth.Truth.assertThat;

import ch.hsr.geohash.GeoHash;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;

@Slf4j
class SpatialServiceTest {

  @Test
  void testBufferPoint() {
    // arrange
    double radius = 5 * (double) 1 / 60;

    Point pt = new PointImpl(-122.121017456055, 37.7742691040039,
        SpatialService.getInstance().getSpatialContext());

    // act
    Circle buffer = SpatialService.getInstance().bufferPoint(pt, radius);
    // assert

    assertThat(buffer.getRadius()).isEqualTo(radius);

  }

  @Test
  void testGeohash() {
    Point pt = new PointImpl(-122.121017456055, 37.7742691040039,
        SpatialService.getInstance().getSpatialContext());
    GeoHash geohash = SpatialService.getInstance().geohash(pt);
    String hash = geohash.toBase32();
    assertThat(hash).isEqualTo("9q9n");

  }

  @Test
  void testIntersectTrue() {
    double radius = 5 * (double) 1 / 60;
    Point pt = new PointImpl(-122.121017456055, 37.7742691040039,
        SpatialService.getInstance().getSpatialContext());
    Circle buffer = SpatialService.getInstance().bufferPoint(pt, radius);

    // act
    boolean isWithin = SpatialService.getInstance().contained(buffer, pt);

    // assert
    assertThat(isWithin).isTrue();
  }

  @Test
  void testIntersectFalse() {
    double radius = 5 * (double) 1 / 60;
    Point pt1 = new PointImpl(-122.121017456055, 37.7742691040039,
        SpatialService.getInstance().getSpatialContext());
    Circle buffer = SpatialService.getInstance().bufferPoint(pt1, radius);
    Point pt2 = new PointImpl(-122.0, 37.0, SpatialService.getInstance().getSpatialContext());

    // act
    boolean isWithin = SpatialService.getInstance().contained(buffer, pt2);

    // assert
    assertThat(isWithin).isFalse();
  }

  @Test
  void testGeoHashOfACircle() {
    // arrange
    double radius = 5 * (double) 1 / 60;

    Point pt = new PointImpl(-122.121017456055, 37.7742691040039,
        SpatialService.getInstance().getSpatialContext());

    // act
    Circle buffer = SpatialService.getInstance().bufferPoint(pt, radius);
    List<GeoHash> geoHashes = SpatialService.getInstance().geohash(buffer);
    log.info("Geohashes : {}", geoHashes);
    // assert
    assertThat(geoHashes).hasSize(2);
  }

}
