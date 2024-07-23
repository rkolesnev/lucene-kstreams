package io.github.rkolesnev.lucenekstreams;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class TaxiPerson {
    BufferedPerson bufferedPerson;
    Taxi taxi;
}
