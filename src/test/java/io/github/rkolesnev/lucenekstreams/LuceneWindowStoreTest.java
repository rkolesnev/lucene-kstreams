package io.github.rkolesnev.lucenekstreams;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.rkolesnev.lucenekstreams.statestore.LuceneSpatialWindowStore;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import java.io.File;

class LuceneWindowStoreTest {

  @Test
  void fetchTest(@TempDir File temp) {
    // setup
    LuceneSpatialWindowStore store = new LuceneSpatialWindowStore("MyLuceneWindowStore");
    StateStoreContext ctx = mock(StateStoreContext.class);
    when(ctx.stateDir()).thenReturn(temp);
    store.init(ctx, null);
    SpatialService spatialService = new SpatialService();
    Point pt = new PointImpl(-122.121017456055, 37.7742691040039,
        SpatialService.getInstance().getSpatialContext());

    // arrange
    long timeFrom = 12345678000L;
    long timeTo = 12345679000L;
    spatialService.geohash(pt);
    byte[] data = serialize(spatialService.geohash(pt));
    Bytes key = new Bytes(data);
    byte[] value = "SomeValue".getBytes();
    store.put(key, value, timeFrom + 500);

    // act
    WindowStoreIterator<byte[]> result = store.fetch(key, timeFrom, timeTo);
    KeyValue<Long, byte[]> record = result.next();

    // assert
    assertThat(record.key).isEqualTo(timeFrom + 500);
    assertThat(record.value).isEqualTo(value);

  }

  @Test
  void fetchSpatialTest(@TempDir File temp) {
    // setup
    LuceneSpatialWindowStore store = new LuceneSpatialWindowStore("MyLuceneWindowStore");
    StateStoreContext ctx = mock(StateStoreContext.class);
    when(ctx.stateDir()).thenReturn(temp);
    store.init(ctx, null);
    SpatialService spatialService = new SpatialService();

    Point pt = new PointImpl(0.04312, 0.06121, SpatialService.getInstance().getSpatialContext());
    Point pt2 = new PointImpl(0.05, 0.06121, SpatialService.getInstance().getSpatialContext());

    // arrange
    long timeFrom = 12345678000L;
    long timeTo = 12345679000L;
    spatialService.geohash(pt);
    byte[] data = serialize(spatialService.geohash(pt));
    Bytes key = new Bytes(data);
    byte[] value = "SomeValue".getBytes();
    store.putSpatial(key, value, timeFrom + 500, pt);

    // act
    WindowStoreIterator<byte[]> result = store.fetchByPointRadius(key, timeFrom, timeTo, pt2, 20000);
    KeyValue<Long, byte[]> record = result.next();

    // assert
    assertThat(record.key).isEqualTo(timeFrom + 500);
    assertThat(record.value).isEqualTo(value);
  }

}
