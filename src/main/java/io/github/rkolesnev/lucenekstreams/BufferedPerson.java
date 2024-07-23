package io.github.rkolesnev.lucenekstreams;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.locationtech.spatial4j.io.jackson.ShapeDeserializer;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;


@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class BufferedPerson extends Person {
    public static final double BUFFER_DISTANCE = 5 * ((double) 1 / 60); // 5 miles in degrees
    @JsonDeserialize(using = ShapeDeserializer.class)
    Circle buffer;

    public BufferedPerson(PersonId personId, Point position, Circle buffer) {
        super(personId, position);
        this.buffer = buffer;
    }

    public BufferedPerson(Person person) {
        super(person.getPersonId(), person.getPosition());
        this.buffer = SpatialService.getInstance().bufferPoint(person.getPosition(), BUFFER_DISTANCE);
    }


}
