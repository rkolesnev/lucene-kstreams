package io.github.rkolesnev.lucenekstreams.transformer;

import static org.apache.kafka.streams.processor.internals.StateStoreUnwrapper.unwrapWindowStoreReadWriteDecorator;

import io.github.rkolesnev.lucenekstreams.statestore.SpatialWindowStore;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.locationtech.spatial4j.shape.Point;

public class SpatialJoinByPointRadiusTransformer<K, V1, V2, VR> implements
    ValueTransformerWithKey<K, V1, Iterable<VR>> {

  ProcessorContext context;
  SpatialWindowStore<K, V1> thisStateStore;
  SpatialWindowStore<K, V2> otherStateStore;
  private final String thisStateStoreName;

  private final String otherStateStoreName;
  private final long joinBeforeMs;
  private final long joinAfterMs;
  private final long joinGraceMs;
  private final boolean isLeftSide;
  private final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VR> joiner;
  private final Function<? super V1, Point> pointExtractor;
  private final double radiusMeters;

  public SpatialJoinByPointRadiusTransformer(final boolean isLeftSide,
      final String thisStateStoreName,
      final String otherStateStoreName,
      final JoinWindows windows,
      final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VR> joiner,
      final Function<? super V1, Point> pointExtractor,
      final double radiusMeters
  ) {
    this.joiner = joiner;
    this.thisStateStoreName = thisStateStoreName;
    this.otherStateStoreName = otherStateStoreName;
    this.isLeftSide = isLeftSide;
    if (isLeftSide) {
      this.joinBeforeMs = windows.beforeMs;
      this.joinAfterMs = windows.afterMs;
    } else {
      this.joinBeforeMs = windows.afterMs;
      this.joinAfterMs = windows.beforeMs;
    }
    this.joinGraceMs = windows.gracePeriodMs();
    this.pointExtractor = pointExtractor;
    this.radiusMeters = radiusMeters;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.thisStateStore = unwrapWindowStoreReadWriteDecorator(context.getStateStore(thisStateStoreName));
    this.otherStateStore = unwrapWindowStoreReadWriteDecorator(context.getStateStore(otherStateStoreName));
  }

  @Override
  public Iterable<VR> transform(K readOnlyKey, V1 value) {
    final long inputRecordTimestamp = context.timestamp();
    final long timeFrom = Math.max(0L, inputRecordTimestamp - joinBeforeMs);
    final long timeTo = Math.max(0L, inputRecordTimestamp + joinAfterMs);

    storeRecordInStateStore(thisStateStore, readOnlyKey, value, inputRecordTimestamp);

    List<KeyValue<Long, V2>> recordsToJoin = fetchRecordsToJoin(readOnlyKey, otherStateStore,
        timeFrom, timeTo, pointExtractor.apply(value), radiusMeters);

    List<VR> result = joinRecords(joiner, recordsToJoin, readOnlyKey, value);

    return (result.isEmpty() ? null : result);
  }

  private List<VR> joinRecords(
      ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VR> joiner,
      List<KeyValue<Long, V2>> recordsToJoin, K readOnlyKey, V1 value) {
    return recordsToJoin.stream()
        .map(otherRecord -> joiner.apply(readOnlyKey, value, otherRecord.value)).collect(
            Collectors.toList());
  }

  private List<KeyValue<Long, V2>> fetchRecordsToJoin(K key,
      SpatialWindowStore<K, V2> otherStateStore, long timeFrom, long timeTo,
      Point point, double radiusMeters) {
    List<KeyValue<Long, V2>> result = new ArrayList<>();
    try (final WindowStoreIterator<V2> iter = otherStateStore.fetchByPointRadius(key, timeFrom,
        timeTo, point, radiusMeters)) {
      while (iter.hasNext()) {
        final KeyValue<Long, V2> otherRecord = iter.next();
        result.add(otherRecord);
      }
      return result;
    }
  }

  private void storeRecordInStateStore(SpatialWindowStore<K, V1> thisStateStore, K readOnlyKey,
      V1 value,
      long inputRecordTimestamp) {
    thisStateStore.putSpatial(readOnlyKey, value, inputRecordTimestamp,
        pointExtractor.apply(value));
  }

  @Override
  public void close() {

  }
}
