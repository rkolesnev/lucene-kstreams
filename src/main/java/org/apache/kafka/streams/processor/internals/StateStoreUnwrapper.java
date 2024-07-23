package org.apache.kafka.streams.processor.internals;

import io.github.rkolesnev.lucenekstreams.statestore.SpatialWindowStore;
import org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.WindowStoreReadWriteDecorator;
import org.apache.kafka.streams.state.WindowStore;

public class StateStoreUnwrapper {

  public static <K, V> SpatialWindowStore<K, V> unwrapWindowStoreReadWriteDecorator(
      WindowStore<K, V> windowStore) {
    if (windowStore instanceof WindowStoreReadWriteDecorator<K, V>) {
      return (SpatialWindowStore<K, V>) ((WindowStoreReadWriteDecorator<K, V>) windowStore).wrapped();
    } else if (windowStore instanceof SpatialWindowStore<K, V>) {
      return (SpatialWindowStore<K, V>) windowStore;
    } else {
      throw new IllegalArgumentException(
          "Unexpected state store - cannot cast / unwrap to SpatialStateStore, stateStore=" +
              windowStore.toString());
    }
  }

}
