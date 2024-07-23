package io.github.rkolesnev.lucenekstreams.statestore;

import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.locationtech.spatial4j.shape.Point;

public interface SpatialWindowStore<K, V> extends WindowStore<K, V> {

  WindowStoreIterator<V> fetchByPointRadius(K key, long timeFrom, long timeTo, Point point,
      double radiusMeters);

  void putSpatial(K key, V value, long windowStartTimestamp, Point point);
}
