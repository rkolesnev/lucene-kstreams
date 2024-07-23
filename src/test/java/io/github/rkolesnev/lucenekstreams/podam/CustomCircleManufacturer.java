package io.github.rkolesnev.lucenekstreams.podam;

import java.util.Map;

import io.github.rkolesnev.lucenekstreams.BufferedPerson;
import org.apache.commons.lang3.RandomUtils;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.CircleImpl;
import org.locationtech.spatial4j.shape.impl.PointImpl;
import uk.co.jemos.podam.api.AttributeMetadata;
import uk.co.jemos.podam.api.DataProviderStrategy;
import uk.co.jemos.podam.typeManufacturers.AbstractTypeManufacturer;

public class CustomCircleManufacturer extends AbstractTypeManufacturer<Circle> {

  @Override
  public Circle getType(DataProviderStrategy dataProviderStrategy,
      AttributeMetadata attributeMetadata, Map map) {
    double longitude = RandomUtils.nextDouble(0, 360) - 180;
    double latitude = RandomUtils.nextDouble(0, 180) - 90;

    SpatialContext spatialContext = SpatialContext.GEO;
    Point center = new PointImpl(longitude, latitude, spatialContext);
    return new CircleImpl(center, BufferedPerson.BUFFER_DISTANCE, spatialContext);
  }
}

