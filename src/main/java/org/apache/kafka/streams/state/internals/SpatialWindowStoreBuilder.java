package org.apache.kafka.streams.state.internals;

import io.github.rkolesnev.lucenekstreams.statestore.SpatialWindowBytesStoreSupplier;
import io.github.rkolesnev.lucenekstreams.statestore.SpatialWindowStore;
import java.util.Objects;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpatialWindowStoreBuilder <K, V> extends AbstractStoreBuilder<K, V, SpatialWindowStore<K, V>> {
    private final Logger log = LoggerFactory.getLogger(
        org.apache.kafka.streams.state.internals.SpatialWindowStoreBuilder.class);

    private final SpatialWindowBytesStoreSupplier storeSupplier;

    public SpatialWindowStoreBuilder(final SpatialWindowBytesStoreSupplier storeSupplier,
        final Serde<K> keySerde,
        final Serde<V> valueSerde,
        final Time time) {
      super(storeSupplier.name(), keySerde, valueSerde, time);
      Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
      Objects.requireNonNull(storeSupplier.metricsScope(), "storeSupplier's metricsScope can't be null");
      this.storeSupplier = storeSupplier;

      if (storeSupplier.retainDuplicates()) {
        this.logConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
      }
    }

    @Override
    public SpatialWindowStore<K, V> build() {

      return new SpatialMeteredWindowStore<>(
          maybeWrapLogging(storeSupplier.get()),
          storeSupplier.windowSize(),
          storeSupplier.metricsScope(),
          time,
          keySerde,
          valueSerde);
    }

    private SpatialWindowStore<Bytes, byte[]> maybeWrapLogging(final SpatialWindowStore<Bytes, byte[]> inner) {
      if (!enableLogging) {
        return inner;
      }
      return new ChangeLoggingSpatialWindowBytesStore(
          inner,
          storeSupplier.retainDuplicates(),
          WindowKeySchema::toStoreKeyBinary
      );
    }

    public long retentionPeriod() {
      return storeSupplier.retentionPeriod();
    }

}
