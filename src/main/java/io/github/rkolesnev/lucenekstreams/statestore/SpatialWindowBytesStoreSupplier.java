package io.github.rkolesnev.lucenekstreams.statestore;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

public interface SpatialWindowBytesStoreSupplier extends
    StoreSupplier<SpatialWindowStore<Bytes, byte[]>> {

  /**
   * The size of the segments (in milliseconds) the store has. If your store is segmented then this
   * should be the size of segments in the underlying store. It is also used to reduce the amount of
   * data that is scanned when caching is enabled.
   *
   * @return size of the segments (in milliseconds)
   */
  long segmentIntervalMs();

  /**
   * The size of the windows (in milliseconds) any store created from this supplier is creating.
   *
   * @return window size
   */
  long windowSize();

  /**
   * Whether or not this store is retaining duplicate keys. Usually only true if the store is being
   * used for joins. Note this should return false if caching is enabled.
   *
   * @return true if duplicates should be retained
   */
  boolean retainDuplicates();

  /**
   * The time period for which the {@link SpatialWindowStore} will retain historic data.
   *
   * @return retentionPeriod
   */
  long retentionPeriod();

  WindowBytesStoreSupplier asWindowBytesStoreSupplier();
}
