package io.github.rkolesnev.lucenekstreams.statestore;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.StoreToProcessorContextAdapter;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.apache.kafka.streams.state.internals.WindowKeySchemaAccessor;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static org.apache.lucene.search.BooleanClause.Occur.MUST;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class LuceneSpatialWindowStore implements SpatialWindowStore<Bytes, byte[]> {

    static final String KEY_FIELD = "Key";
    static final String TIMESTAMP_INDEX_FIELD = "Time";
    static final String TIMESTAMP_STORED_FIELD = "TimeValue";
    static final String VALUE_FIELD = "Value";
    static final String POINT_FIELD = "Point";

    final String name;

    File parentDir;

    Directory index;
    IndexWriter writer;
    IndexSearcher indexSearcher;

    final Position position = Position.emptyPosition();

    StateStoreContext context;

    final JtsSpatialContext spatialContext = new JtsSpatialContextFactory().newSpatialContext();

    Serdes.DoubleSerde doubleSerde = new Serdes.DoubleSerde();

    @Override
    public void init(ProcessorContext context, StateStore root) {
        this.parentDir = context.stateDir();

        try {
            Files.createDirectories(parentDir.toPath());
            Files.createDirectories(parentDir.getAbsoluteFile().toPath());
        } catch (final IOException fatal) {
            throw new ProcessorStateException(fatal);
        }
        try {
            index = FSDirectory.open(new File(parentDir, name).toPath());
        } catch (IOException fatal) {
            throw new ProcessorStateException(fatal);
        }
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        try {
            writer = new IndexWriter(index, indexWriterConfig);
        } catch (IOException e) {
            throw new RuntimeException("Error initializing lucene state store writer", e);
        }
        if (root != null) {
            context.register(
                    root,
                    (RecordBatchingStateRestoreCallback) this::restoreBatch);
        }
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        init(StoreToProcessorContextAdapter.adapt(context), root);
        this.context = context;
    }

    @Override
    public void put(Bytes key, byte[] value, long windowStartTimestamp) {

        Document doc = new Document();
        doc.add(new StringField(KEY_FIELD, new BytesRef(key.get()), Field.Store.NO));
        doc.add(new LongPoint(TIMESTAMP_INDEX_FIELD, windowStartTimestamp));
        doc.add(new StoredField(TIMESTAMP_STORED_FIELD, windowStartTimestamp));
        doc.add(new StoredField(VALUE_FIELD, value));
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        StoreQueryUtils.updatePosition(position, context);
    }

    @Override
    public void putSpatial(Bytes key, byte[] value, long windowStartTimestamp, Point point) {

        Document doc = new Document();
        doc.add(new StringField(KEY_FIELD, new BytesRef(key.get()), Field.Store.NO));
        doc.add(new LongPoint(TIMESTAMP_INDEX_FIELD, windowStartTimestamp));
        doc.add(new StoredField(TIMESTAMP_STORED_FIELD, windowStartTimestamp));
        doc.add(new StoredField(VALUE_FIELD, value));
        doc.add(new LatLonPoint(POINT_FIELD, point.getLat(), point.getLon()));
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        StoreQueryUtils.updatePosition(position, context);
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo) {
        try {
            indexSearcher = new IndexSearcher(DirectoryReader.open(writer));
            var timeQuery = LongPoint.newRangeQuery(TIMESTAMP_INDEX_FIELD, new long[]{timeFrom},
                    new long[]{timeTo});

            var fieldQuery = new TermQuery(new Term(KEY_FIELD, new BytesRef(key.get())));
            BooleanQuery.Builder query = new BooleanQuery.Builder();
            query.add(timeQuery, MUST);
            query.add(fieldQuery, MUST);

            var topDocs = indexSearcher.search(query.build(), Integer.MAX_VALUE);
            //TODO - lazy load docs from iterator potentially.

            return new LuceneWindowStoreIterator(topDocs.scoreDocs.length - 1, indexSearcher,
                    topDocs.scoreDocs);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public WindowStoreIterator<byte[]> fetchByPointRadius(Bytes key, long timeFrom, long timeTo, Point point,
                                                          double radiusMeters) {
        try {
            indexSearcher = new IndexSearcher(DirectoryReader.open(writer));
            var timeQuery = LongPoint.newRangeQuery(TIMESTAMP_INDEX_FIELD, new long[]{timeFrom},
                    new long[]{timeTo});

            var radiusQuery = LatLonPoint.newDistanceQuery(POINT_FIELD, point.getLat(), point.getLon(),
                    radiusMeters);

            var fieldQuery = new TermQuery(new Term(KEY_FIELD, new BytesRef(key.get())));

            BooleanQuery.Builder query = new BooleanQuery.Builder();
            query.add(timeQuery, MUST);
            query.add(radiusQuery, MUST);
            query.add(fieldQuery, MUST);

            var topDocs = indexSearcher.search(query.build(), Integer.MAX_VALUE);
            //TODO - lazy load docs from iterator potentially.

            return new LuceneWindowStoreIterator(topDocs.scoreDocs.length - 1, indexSearcher,
                    topDocs.scoreDocs);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes keyFrom, Bytes keyTo, long timeFrom,
                                                           long timeTo) {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo) {
        return null;
    }

    @Override
    public String name() {
        return name;
    }


    @Override
    public void flush() {
        try {
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Error flushing lucene writer", e);
        }
    }

    @Override
    public void close() {
        if (writer.isOpen()) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException("Error closing writer", e);
            }
        }
        try {
            index.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing lucene index", e);
        }
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return writer.isOpen();
    }

    @Override
    public byte[] fetch(Bytes key, long time) {
        return new byte[0];
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return null;
    }

    @Override
    public Position getPosition() {
        return this.position;
    }

    void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        final List<KeyValue<byte[], byte[]>> keyValues = new ArrayList<>();
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            byte[] key = WindowKeySchemaAccessor.getKeyBytes(record.key());
            long timestamp = WindowKeySchemaAccessor.getTimestamp(record.key());
            Header opHeader = record.headers().lastHeader("op");
            if (opHeader != null && opHeader.value() != null && Arrays.equals(opHeader.value(), ("SP".getBytes()))) {
                //Spatial put - extract location from location (param1) header
                Header locationHeader1 = record.headers().lastHeader("p1");
                Header locationHeader2 = record.headers().lastHeader("p2");

                if (locationHeader1 == null || locationHeader1.value() == null) {
                    throw new RuntimeException("Spatial put operation requires location headers (p1 and p2)");
                }

                if (locationHeader2 == null || locationHeader2.value() == null) {
                    throw new RuntimeException("Spatial put operation requires location headers (p1 and p2)");
                }

                Double p1 = doubleSerde.deserializer().deserialize("t", locationHeader1.value());
                Double p2 = doubleSerde.deserializer().deserialize("t", locationHeader2.value());
                putSpatial(new Bytes(key), record.value(), timestamp, new PointImpl(p1, p2, spatialContext));
                log.info("Restored record with spatial put for name {}, key {}, value {}, timestamp {}", name, key,
                        record.value(), timestamp);
            } else {
                put(new Bytes(key), record.value(), timestamp);
                log.info("Restored record with put for name {}, key {}, value {}, timestamp {}", name, key,
                        record.value(), timestamp);
            }
        }
    }

    @RequiredArgsConstructor
    class LuceneWindowStoreIterator implements WindowStoreIterator<byte[]> {

        private final int lastPos;
        private int currentPos = -1;
        private final IndexSearcher searcher;
        private final ScoreDoc[] scoreDocs;

        @Override
        public void close() {

        }

        @Override
        public Long peekNextKey() {
            if (hasNext()) {
                try {
                    return searcher.doc(scoreDocs[currentPos + 1].doc).getField(TIMESTAMP_STORED_FIELD)
                            .numericValue().longValue();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public boolean hasNext() {
            return (currentPos < lastPos) && lastPos > -1;
        }

        @Override
        public KeyValue<Long, byte[]> next() {
            if (hasNext()) {
                currentPos++;
                try {
                    Document doc = searcher.doc(scoreDocs[currentPos].doc);
                    return new KeyValue<>(doc.getField(TIMESTAMP_STORED_FIELD).numericValue().longValue(),
                            doc.getField(VALUE_FIELD).binaryValue().bytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new NoSuchElementException();
            }
        }


    }
}
