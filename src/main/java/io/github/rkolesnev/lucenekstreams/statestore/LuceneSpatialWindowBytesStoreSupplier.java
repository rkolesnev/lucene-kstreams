package io.github.rkolesnev.lucenekstreams.statestore;

import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

@RequiredArgsConstructor
public class LuceneSpatialWindowBytesStoreSupplier implements SpatialWindowBytesStoreSupplier {

  private final String name;
  private final long windowSize;
  private final long retentionPeriod;

  @Override
  public long segmentIntervalMs() {
    return 0;
  }

  @Override
  public long windowSize() {
    return windowSize;
  }

  @Override
  public boolean retainDuplicates() {
    return true;
  }

  @Override
  public long retentionPeriod() {
    return retentionPeriod;
  }

  @Override
  public WindowBytesStoreSupplier asWindowBytesStoreSupplier() {
    return new SpatialToWindowBytesStoreSupplierAdaptor(name, windowSize, retentionPeriod,
        this::get);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public SpatialWindowStore<Bytes, byte[]> get() {
    return new LuceneSpatialWindowStore(name);
  }

  @Override
  public String metricsScope() {
    return "lucene-window";
  }

  @RequiredArgsConstructor
  class SpatialToWindowBytesStoreSupplierAdaptor implements WindowBytesStoreSupplier {

    private final String name;
    private final long windowSize;
    private final long retentionPeriod;

    private final Supplier<WindowStore<Bytes, byte[]>> storeSupplier;

    @Override
    public long segmentIntervalMs() {
      return 0;
    }

    @Override
    public long windowSize() {
      return windowSize;
    }

    @Override
    public boolean retainDuplicates() {
      return true;
    }

    @Override
    public long retentionPeriod() {
      return retentionPeriod;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public WindowStore<Bytes, byte[]> get() {
      return storeSupplier.get();
    }

    @Override
    public String metricsScope() {
      return "lucene-window";
    }
  }
}
