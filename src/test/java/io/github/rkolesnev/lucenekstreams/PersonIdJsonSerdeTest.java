package io.github.rkolesnev.lucenekstreams;

import static com.google.common.truth.Truth.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class PersonIdJsonSerdeTest {

    private PersonIdJsonSerde classUnderTest = new PersonIdJsonSerde();

    @Test
    void testPersonSerialization() {
        PersonId personId = new PersonId(UUID.fromString("f613c6d2-25ba-4670-9ed2-86356118959d"));

        assertThat(new String(classUnderTest.serialize("Test", personId), StandardCharsets.UTF_8))
                .isEqualTo("{\"id\":\"" + personId.id.toString() + "\"}");
    }
}
