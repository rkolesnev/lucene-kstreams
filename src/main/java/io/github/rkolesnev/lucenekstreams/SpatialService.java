package io.github.rkolesnev.lucenekstreams;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.SpatialRelation;

@Slf4j
public class SpatialService {

  int LEVEL = 4;

  private static SpatialService instance = new SpatialService();

  public static SpatialService getInstance() {
    return instance;
  }

  private final SpatialContext spatialContext;

  public SpatialService() {
    SpatialContextFactory scf = new JtsSpatialContextFactory();
    this.spatialContext = scf.newSpatialContext();
  }

  public SpatialContext getSpatialContext() {
    return this.spatialContext;
  }

  public Circle bufferPoint(Point pt, double distance) {
    return (Circle) pt.getBuffered(distance, this.spatialContext);
  }

  public GeoHash geohash(Point pt) {
    GeoHash geohash = GeoHash.withCharacterPrecision(pt.getY(), pt.getX(), LEVEL);
    return geohash;
  }

  public List<GeoHash> geohash(Circle circle) {
    return coveringGeohashes(circle);
  }

  private List<GeoHash> coveringGeohashes(Circle c) {
    // get bounding box
    Rectangle bb = c.getBoundingBox();
    GeometryFactory gf = new GeometryFactory();
    Coordinate[] bbCords = new Coordinate[5];

    bbCords[0] = new Coordinate(bb.getMinX(), bb.getMinY());
    bbCords[1] = new Coordinate(bb.getMinX(), bb.getMaxY());
    bbCords[2] = new Coordinate(bb.getMaxX(), bb.getMaxY());
    bbCords[3] = new Coordinate(bb.getMaxX(), bb.getMinY());
    bbCords[4] = new Coordinate(bb.getMinX(), bb.getMinY());

    Polygon p = gf.createPolygon(bbCords);

    return polygonToGeohashes(p, LEVEL);
  }

  private List<Coordinate> envelopeBox(Polygon polygon) {
    /**
     * SouthEast -> NorthEast -> NorthWest -> SouthWest
     */
    List<Coordinate> envelopeCoordinates = new ArrayList<>(Arrays.asList(polygon.getCoordinates()));
    envelopeCoordinates.remove(0);
    return envelopeCoordinates;
  }

  private org.locationtech.jts.geom.Point toJTSPoint(WGS84Point wgs84Point) {
    return new GeometryFactory().createPoint(
        new Coordinate(wgs84Point.getLongitude(), wgs84Point.getLatitude()));
  }

  Polygon toJTSPolygon(BoundingBox boundingBox) {
    /**
     * ch.hsr.GeoHash to JTS Polygon
     */
    var points = new Coordinate[]{
        new Coordinate(boundingBox.getWestLongitude(), boundingBox.getNorthLatitude()),
        new Coordinate(boundingBox.getWestLongitude(), boundingBox.getSouthLatitude()),
        new Coordinate(boundingBox.getEastLongitude(), boundingBox.getSouthLatitude()),
        new Coordinate(boundingBox.getEastLongitude(), boundingBox.getNorthLatitude()),
        new Coordinate(boundingBox.getWestLongitude(), boundingBox.getNorthLatitude())
    };

    GeometryFactory geometryFactory = new GeometryFactory();
    return geometryFactory.createPolygon(
        new LinearRing(new CoordinateArraySequence(points), geometryFactory), null);
  }

  private List<GeoHash> polygonToGeohashes(Polygon polygon, int precision) {

    var b = envelopeBox(polygon);

    GeoHash hashNorthEast = GeoHash.withCharacterPrecision(b.get(1).y, b.get(1).x, precision);
    GeoHash hashSouthWest = GeoHash.withCharacterPrecision(b.get(3).y, b.get(3).x, precision);

    double perLat = hashNorthEast.getBoundingBox().getLatitudeSize();
    double perLng = hashNorthEast.getBoundingBox().getLongitudeSize();

    int latStep = (int) Math.floor(
        (hashNorthEast.getBoundingBoxCenter().getLatitude() - hashSouthWest.getBoundingBoxCenter()
            .getLatitude()) / perLat) + 1;
    int lngStep = (int) Math.floor(
        (hashNorthEast.getBoundingBoxCenter().getLongitude() - hashSouthWest.getBoundingBoxCenter()
            .getLongitude()) / perLng) + 1;

    List<GeoHash> hashList = new ArrayList<>();
    var p = new ArrayList<ArrayList<Double>>();

    GeoHash baseHash = hashSouthWest.getSouthernNeighbour().getSouthernNeighbour()
        .getWesternNeighbour().getWesternNeighbour();

    for (int lat = 0; lat <= latStep + 1; lat++) {

      baseHash = baseHash.getNorthernNeighbour();

      GeoHash tmp = baseHash;

      for (int lng = 0; lng <= lngStep + 1; lng++) {

        var next = tmp.getEasternNeighbour();

        var points = new ArrayList<Double>();
        points.add(next.getBoundingBoxCenter().getLatitude());
        points.add(next.getBoundingBoxCenter().getLongitude());
        p.add(points);

        var bbox = next.getBoundingBox();

        if (polygon.intersects(toJTSPolygon(bbox))) {
          hashList.add(next);
        }

        tmp = next;
      }
    }
    return hashList;

  }

  public boolean contained(Circle circle, Point point) {
    SpatialRelation relation = circle.relate(point);
    log.info("Relation = {}", relation);
    return relation == SpatialRelation.CONTAINS;
  }
}
