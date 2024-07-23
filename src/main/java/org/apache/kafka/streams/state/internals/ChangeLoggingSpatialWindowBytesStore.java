package org.apache.kafka.streams.state.internals;

import io.github.rkolesnev.lucenekstreams.statestore.SpatialWindowStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextAccessor;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.locationtech.spatial4j.shape.Point;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

@Slf4j
public class ChangeLoggingSpatialWindowBytesStore extends
        WrappedStateStore<SpatialWindowStore<Bytes, byte[]>, byte[], byte[]>
        implements SpatialWindowStore<Bytes, byte[]> {

    private final boolean retainDuplicates;
    InternalProcessorContext context;
    private int seqnum = 0;
    private final ChangeLoggingWindowBytesStore.ChangeLoggingKeySerializer keySerializer;

    Serdes.DoubleSerde doubleSerde = new Serdes.DoubleSerde();

    ChangeLoggingSpatialWindowBytesStore(final SpatialWindowStore<Bytes, byte[]> bytesStore,
                                         final boolean retainDuplicates,
                                         final ChangeLoggingWindowBytesStore.ChangeLoggingKeySerializer keySerializer) {
        super(bytesStore);
        this.retainDuplicates = retainDuplicates;
        this.keySerializer = requireNonNull(keySerializer, "keySerializer");
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = asInternalProcessorContext(context);
        super.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        this.context = asInternalProcessorContext(context);
        super.init(context, root);
    }

    @Override
    public byte[] fetch(final Bytes key,
                        final long timestamp) {
        return wrapped().fetch(key, timestamp);
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key,
                                             final long from,
                                             final long to) {
        return wrapped().fetch(key, from, to);
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key,
                                                     final long timeFrom,
                                                     final long timeTo) {
        return wrapped().backwardFetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final long timeFrom,
                                                           final long to) {
        return wrapped().fetch(keyFrom, keyTo, timeFrom, to);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final long timeFrom,
                                                                   final long timeTo) {
        return wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return wrapped().all();
    }


    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        return wrapped().backwardAll();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom,
                                                              final long timeTo) {
        return wrapped().fetchAll(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom,
                                                                      final long timeTo) {
        return wrapped().backwardFetchAll(timeFrom, timeTo);
    }

    @Override
    public void put(final Bytes key,
                    final byte[] value,
                    final long windowStartTimestamp) {
        wrapped().put(key, value, windowStartTimestamp);
        log(keySerializer.serialize(key, windowStartTimestamp, maybeUpdateSeqnumForDups()), value, null); //no additional info to record in headers
    }

    @Override
    public WindowStoreIterator<byte[]> fetchByPointRadius(Bytes key, long timeFrom, long timeTo, Point point,
                                                          double radiusMeters) {
        return wrapped().fetchByPointRadius(key, timeFrom, timeTo, point, radiusMeters);
    }

    @Override
    public void putSpatial(Bytes key, byte[] value, long windowStartTimestamp, Point point) {
        wrapped().putSpatial(key, value, windowStartTimestamp, point);
        Headers headers = headersForSpatialPut(point); // prepare headers with additional info required for spatial put restore
        log(keySerializer.serialize(key, windowStartTimestamp, maybeUpdateSeqnumForDups()), value, headers);
    }

    private Headers headersForSpatialPut(Point point) {
        Headers headers = new RecordHeaders();
        headers.add("op", "SP".getBytes());
        headers.add("p1", doubleSerde.serializer().serialize("t", point.getX()));
        headers.add("p2", doubleSerde.serializer().serialize("t", point.getY()));
        return headers;
    }

    void log(final Bytes key, final byte[] value, Headers headers) {
        log.info("Writing to changelog for name {}, key {}, value {}, timestamp {}, headers {}",
                name(), key, value, context.timestamp(), headers);
        ProcessorContextAccessor.logChange(name(), key, value, context.timestamp(), headers, (ProcessorContextImpl) context);
    }

    private int maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
        return seqnum;
    }
}
