package io.github.rkolesnev.lucenekstreams;

import static com.google.common.truth.Truth.assertThat;

import io.github.rkolesnev.lucenekstreams.podam.CustomCircleManufacturer;
import io.github.rkolesnev.lucenekstreams.podam.CustomPointManufacturer;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;
import uk.co.jemos.podam.api.PodamFactoryImpl;

@Slf4j
class TaxiTopologyTest {

  PodamFactoryImpl factory = new PodamFactoryImpl();
  TopologyTestDriver testDriver;

  TestInputTopic<PersonId, Person> personInputTopic;
  TestInputTopic<TaxiId, Taxi> taxiInputTopic;
  TestOutputTopic<GeoHashId, TaxiPerson> outputTopic;

  Serializer<PersonId> personIdSerializer = new PersonIdJsonSerde().serializer();
  Serializer<Person> personSerializer = new PersonJsonSerde().serializer();
  Serializer<TaxiId> taxiIdSerializer = new TaxiIdJsonSerde().serializer();
  Serializer<Taxi> taxiSerializer = new TaxiJsonSerde().serializer();

  SpatialContext spatialContext;

  @BeforeEach
  public void setup() {

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "taxiTest");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "taxiTest:1234");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

    Topology taxiTopology = new TaxiTopology(SpatialService.getInstance()).build(
        new StreamsBuilder());

    testDriver = new TopologyTestDriver(taxiTopology, props);

    // setup test topics
    personInputTopic = testDriver.createInputTopic(TaxiTopology.PEOPLE_TOPIC, personIdSerializer,
        personSerializer);
    taxiInputTopic = testDriver.createInputTopic(TaxiTopology.TAXI_TOPIC, taxiIdSerializer, taxiSerializer);

    outputTopic = testDriver.createOutputTopic(TaxiTopology.TAXI_PEOPLE_TOPIC,
        new GeoHashIdJsonSerde().deserializer(), new TaxiPersonJsonSerde().deserializer());

    factory.getStrategy().addOrReplaceTypeManufacturer(Point.class, new CustomPointManufacturer());
    factory.getStrategy()
        .addOrReplaceTypeManufacturer(Circle.class, new CustomCircleManufacturer());

  }

  @AfterEach
  void cleanup() {
    testDriver.close();
  }

  @Test
  void noIntersectingPeopleAndTaxis() {
    //Before
    pipePerson();
    pipeTaxi();

    //During
    var list = outputTopic.readKeyValuesToList();

    //After
    assertThat(list).isEmpty();
  }

  @Test
  void intersecting() {
    //Before
    var fakePoint = factory.manufacturePojo(Point.class);

    var person = pipePerson(getFakePerson().toBuilder().position(fakePoint).build());
    var taxi = pipeTaxi(getFakeTaxi().toBuilder().position(fakePoint).build());

    //During
    var taxiPerson = outputTopic.readValue();

    //After
    assertThat(taxiPerson.getTaxi()).isEqualTo(taxi);
    assertThat(taxiPerson.getBufferedPerson().getPersonId()).isEqualTo(person.getPersonId());
    assertThat(taxiPerson.getBufferedPerson().getPosition()).isEqualTo(person.getPosition());

  }

  @Test
  void intersectingMultipleTaxis() {
    //Before
    var fakePersonPoint1 = getPoint(0.04312, 0.06121);
    var fakePersonPoint2 = getPoint(20, 2.00201);

    var fakeTaxiPoint1 = getPoint(0.04312, 0.06121);
    var fakeTaxiPoint2 = getPoint(20, 2);
    var fakeTaxiPoint3 = getPoint(3, 33);

    var person1 = pipePerson(getFakePerson().toBuilder().position(fakePersonPoint1).build());

    var taxi1 = pipeTaxi(getFakeTaxi().toBuilder().position(fakeTaxiPoint1).build());
    var taxi2 = pipeTaxi(getFakeTaxi().toBuilder().position(fakeTaxiPoint2).build());
    var taxi3 = pipeTaxi(getFakeTaxi().toBuilder().position(fakeTaxiPoint3).build());

    var person2 = pipePerson(getFakePerson().toBuilder().position(fakePersonPoint2).build());

    //During
    List<TaxiPerson> taxiPerson = outputTopic.readValuesToList();

    //After
    assertThat(taxiPerson).hasSize(2);
    TaxiPerson actual1 = taxiPerson.get(0);

    assertThat(actual1.getTaxi()).isEqualTo(taxi1);
    assertThat(actual1.getBufferedPerson().getPersonId()).isEqualTo(person1.getPersonId());

    assertThat(actual1.getBufferedPerson().getPersonId()).isNotEqualTo(person2.getPersonId());

    TaxiPerson actual2 = taxiPerson.get(1);

    assertThat(actual2.getTaxi()).isEqualTo(taxi2);
    assertThat(actual2.getBufferedPerson().getPersonId()).isEqualTo(person2.getPersonId());

    assertThat(actual2.getBufferedPerson().getPersonId()).isNotEqualTo(person1.getPersonId());
  }

  private Person pipePerson(Person person) {
    personInputTopic.pipeInput(new TestRecord<>(person.getPersonId(), person));
    return person;
  }

  private Person pipePerson() {
    var fakePerson = getFakePerson();
    personInputTopic.pipeInput(new TestRecord<>(fakePerson.getPersonId(), fakePerson));
    return fakePerson;
  }

  private Person getFakePerson() {
    return factory.manufacturePojo(Person.class);
  }


  private Taxi pipeTaxi(Taxi taxi) {
    taxiInputTopic.pipeInput(new TestRecord<>(taxi.getId(), taxi));
    return taxi;
  }

  private Taxi pipeTaxi() {
    var fakeTaxi = getFakeTaxi();
    return pipeTaxi(fakeTaxi);
  }

  private Taxi getFakeTaxi() {
    return factory.manufacturePojo(Taxi.class);
  }


  private Point getPoint(double x, double y) {
    return new PointImpl(x, y, spatialContext);
  }

//    @After
//    public void tearDown() {
//        testDriver.close();
//    }
//
//    @Test
//    public void shouldFlushStoreForFirstInput() {
//        inputTopic.pipeInput("a", 1L);
//        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
//        assertThat(outputTopic.isEmpty(), is(true));
//    }
//
//    @Test
//    public void shouldNotUpdateStoreForSmallerValue() {
//        inputTopic.pipeInput("a", 1L);
//        assertThat(store.get("a"), equalTo(21L));
//        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
//        assertThat(outputTopic.isEmpty(), is(true));
//    }
//
//    @Test
//    public void shouldNotUpdateStoreForLargerValue() {
//        inputTopic.pipeInput("a", 42L);
//        assertThat(store.get("a"), equalTo(42L));
//        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 42L)));
//        assertThat(outputTopic.isEmpty(), is(true));
//    }
//
//    @Test
//    public void shouldUpdateStoreForNewKey() {
//        inputTopic.pipeInput("b", 21L);
//        assertThat(store.get("b"), equalTo(21L));
//        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
//        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("b", 21L)));
//        assertThat(outputTopic.isEmpty(), is(true));
//    }
//
//    @Test
//    public void shouldPunctuateIfEvenTimeAdvances() {
//        final Instant recordTime = Instant.now();
//        inputTopic.pipeInput("a", 1L, recordTime);
//        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
//
//        inputTopic.pipeInput("a", 1L, recordTime);
//        assertThat(outputTopic.isEmpty(), is(true));
//
//        inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(10L));
//        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
//        assertThat(outputTopic.isEmpty(), is(true));
//    }
//
//    @Test
//    public void shouldPunctuateIfWallClockTimeAdvances() {
//        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
//        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
//        assertThat(outputTopic.isEmpty(), is(true));
//    }
//
//    public class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long> {
//        @Override
//        public Processor<String, Long> get() {
//            return new CustomMaxAggregator();
//        }
//    }
//
//    public class CustomMaxAggregator implements Processor<String, Long> {
//        ProcessorContext context;
//        private KeyValueStore<String, Long> store;
//
//        @SuppressWarnings("unchecked")
//        @Override
//        public void init(ProcessorContext context) {
//            this.context = context;
//            context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, time -> flushStore());
//            context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, time -> flushStore());
//            store = (KeyValueStore<String, Long>) context.getStateStore("aggStore");
//        }
//
//        @Override
//        public void process(String key, Long value) {
//            Long oldValue = store.get(key);
//            if (oldValue == null || value > oldValue) {
//                store.put(key, value);
//            }
//        }
//
//        private void flushStore() {
//            KeyValueIterator<String, Long> it = store.all();
//            while (it.hasNext()) {
//                KeyValue<String, Long> next = it.next();
//                context.forward(next.key, next.value);
//            }
//        }
//
//        @Override
//        public void close() {
//        }
//    }
}
