package io.github.rkolesnev.lucenekstreams;

import static com.google.common.truth.Truth.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.impl.PointImpl;

@Slf4j
public class PersonJsonSerdeTest {

    private PersonJsonSerde classUnderTest = new PersonJsonSerde();

    @Test
    void testPersonSerialization() {
        PersonId personId = new PersonId(UUID.fromString("f613c6d2-25ba-4670-9ed2-86356118959d"));
        Person person = new Person(personId, new PointImpl(4.22, 6.33, SpatialService.getInstance().getSpatialContext()));
        assertThat(new String(classUnderTest.serialize("Test", person), StandardCharsets.UTF_8))
                .isEqualTo("{\"personId\":{\"id\":\"" + personId.id.toString() + "\"},\"position\":{\"type\":\"Point\",\"coordinates\":[4.22,6.33]}}");
    }
}
