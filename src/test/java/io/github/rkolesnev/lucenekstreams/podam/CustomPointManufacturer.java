package io.github.rkolesnev.lucenekstreams.podam;

import java.util.Map;
import org.apache.commons.lang3.RandomUtils;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;
import uk.co.jemos.podam.api.AttributeMetadata;
import uk.co.jemos.podam.api.DataProviderStrategy;
import uk.co.jemos.podam.typeManufacturers.AbstractTypeManufacturer;

public class CustomPointManufacturer extends AbstractTypeManufacturer<Point> {

  @Override
  public Point getType(DataProviderStrategy dataProviderStrategy,
      AttributeMetadata attributeMetadata, Map map) {
    double longitude = RandomUtils.nextDouble(0, 360) - 180;
    double latitude = RandomUtils.nextDouble(0, 180) - 90;

    return new PointImpl(longitude, latitude, SpatialContext.GEO);
  }
}

