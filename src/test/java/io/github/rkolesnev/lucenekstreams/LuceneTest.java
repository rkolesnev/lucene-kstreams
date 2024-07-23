package io.github.rkolesnev.lucenekstreams;

import lombok.SneakyThrows;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;
import static com.google.common.truth.Truth.assertThat;

public class LuceneTest {

    @Test
    void addSomethingToLucene()
            throws IOException {

        Directory index = new ByteBuffersDirectory();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        var writer = new IndexWriter(index, indexWriterConfig);
        Document doc = new Document();
        doc.add(new StringField("Test", "Test", NO));
        writer.addDocument(doc);

        var searcher = new IndexSearcher(DirectoryReader.open(writer));
        assertThat(searcher.search(new TermQuery(new Term("Test", "Test")), 5).totalHits.value).isEqualTo(1);
    }

    @SneakyThrows
    @Test
    void testSpatial() {
        /*
        Key (Bytes) - searchable/stored | Timestamp (?) - searchable/stored(?) | Location ? - searchable/non-stored | Value (Bytes) - stored/non-searchable
        KeyBytes + TimeBytes
         */
        Directory index = new ByteBuffersDirectory();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(index, indexWriterConfig);
        Document taxi = new Document();
        taxi.add(new StoredField("TaxiId", "123"));
        taxi.add(new StringField("Title","Taxi", YES));

        LatLonPoint taxiLoc = new LatLonPoint("Location", 0.0612, 0.0432);
        taxi.add(new BinaryDocValuesField("Payload", new BytesRef("ABC".getBytes(UTF_8))));
        taxi.add(taxiLoc);
        writer.addDocument(taxi);

        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer));
        TopDocs res = searcher.search(LatLonPoint.newDistanceQuery("Location", 0.0611, 0.0432, 9000), 5);
        Document result = searcher.doc(res.scoreDocs[0].doc);
        assertThat(result.get("TaxiId")).isEqualTo("123");
        //LatLonPoint isn't stored
//        assertThat((LatLonPoint) result.getField("Location")).isEqualTo(taxiLoc);
    }


}
