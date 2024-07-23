package io.github.rkolesnev.lucenekstreams;

import static com.google.common.truth.Truth.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.impl.PointImpl;

@Slf4j
public class TaxiPersonJsonSerdeTest {

  private TaxiPersonJsonSerde classUnderTest = new TaxiPersonJsonSerde();

  @Test
  void testPersonSerialization() {
    PersonId personId = new PersonId(UUID.randomUUID());
    TaxiId taxiId = new TaxiId(UUID.randomUUID());
    TaxiPerson taxiPerson = new TaxiPerson(
        new BufferedPerson(new Person(personId, new PointImpl(4.22, 6.33, SpatialContext.GEO))),
        new Taxi(taxiId, new PointImpl(4.22, 6.3301,
            SpatialContext.GEO)));
    log.info("{}", taxiPerson);
    assertThat(new String(classUnderTest.serialize("Test", taxiPerson), StandardCharsets.UTF_8))
        .isEqualTo("{\"bufferedPerson\":{\"personId\":{\"id\":\"" + personId.id.toString()
            + "\"},\"position\":{\"type\":\"Point\",\"coordinates\":[4.22,6.33]},\"buffer\":{\"type\":\"Circle\",\"coordinates\":[4.22,6.33],\"radius\":9.266257,\"properties\":{\"radius_units\":\"km\"}}},\"taxi\":{\"id\":{\"id\":\""
            + taxiId.id.toString()
            + "\"},\"position\":{\"type\":\"Point\",\"coordinates\":[4.22,6.3301]}}}");
  }
}
