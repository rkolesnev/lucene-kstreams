package io.github.rkolesnev.lucenekstreams;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import static org.awaitility.Awaitility.await;

public class EndToEndTestIT {

  private String taxiInputTopic = TaxiTopology.TAXI_TOPIC;
  private String personInputTopic = TaxiTopology.PEOPLE_TOPIC;

  private String taxiPersonOutputTopic = TaxiTopology.TAXI_PEOPLE_TOPIC;
  private CommonTestUtils commonTestUtils;
  CountDownLatch streamsLatch = new CountDownLatch(1);

  @BeforeEach
  void setup() {
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
  }

  @AfterEach
  void teardown() {
    streamsLatch.countDown();
    commonTestUtils.stopKafkaContainer();
  }

  @Test
  void testEndToEndFlow() {
    TaxiIdJsonSerde taxiIdJsonSerde = new TaxiIdJsonSerde();
    TaxiJsonSerde taxiJsonSerde = new TaxiJsonSerde();
    PersonIdJsonSerde personIdJsonSerde = new PersonIdJsonSerde();
    PersonJsonSerde personJsonSerde = new PersonJsonSerde();
    TaxiPersonJsonSerde taxiPersonJsonSerde = new TaxiPersonJsonSerde();
    GeoHashIdJsonSerde geoHashIdJsonSerde = new GeoHashIdJsonSerde();

    SpatialService spatialService = new SpatialService();
    TaxiTopology taxiTopology = new TaxiTopology(spatialService);
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    Properties streamsConfig = commonTestUtils.getPropertiesForStreams();
    String kstreamsAppID =streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
    KafkaStreams kafkaStreams = new KafkaStreams(taxiTopology.build(streamsBuilder),
        streamsConfig);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, taxiInputTopic,
        personInputTopic, taxiPersonOutputTopic);
    await().atMost(Duration.ofSeconds(120)).until(() -> kafkaStreams.state().equals(KafkaStreams.State.RUNNING));

    Taxi taxi = new Taxi(new TaxiId(UUID.randomUUID()),
        new PointImpl(0.4, 0.6, spatialService.getSpatialContext()));

    Taxi taxi2 = new Taxi(new TaxiId(UUID.randomUUID()),
        new PointImpl(0.4001, 0.6001, spatialService.getSpatialContext()));

    Person person = new Person(new PersonId(UUID.randomUUID()),
        new PointImpl(0.4, 0.6001, spatialService.getSpatialContext()));

    //Produce 1 person
    commonTestUtils.produceSingleEvent(personInputTopic,person.getPersonId(), person, new Header[0], personIdJsonSerde.serializer().getClass(),personJsonSerde.serializer().getClass());

    //Produce 2 taxis
    commonTestUtils.produceSingleEvent(taxiInputTopic, taxi.id, taxi, new Header[0], taxiIdJsonSerde.serializer().getClass(),taxiJsonSerde.serializer().getClass());
    commonTestUtils.produceSingleEvent(taxiInputTopic, taxi2.id, taxi2, new Header[0], taxiIdJsonSerde.serializer().getClass(),taxiJsonSerde.serializer().getClass());


    //Expecting to consume 2 events on the output topic
    commonTestUtils.consumeAtLeastXEvents( geoHashIdJsonSerde.deserializer().getClass(),
        taxiPersonJsonSerde.deserializer().getClass(), taxiPersonOutputTopic, 2);

    streamsLatch.countDown();
    await().atMost(Duration.ofSeconds(5)).until(() -> kafkaStreams.state().hasCompletedShutdown());

    streamsLatch = new CountDownLatch(1);
    streamsBuilder = new StreamsBuilder();
    streamsConfig = commonTestUtils.getPropertiesForStreams();
    streamsConfig.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, kstreamsAppID);
    KafkaStreams kafkaStreams2 = new KafkaStreams(taxiTopology.build(streamsBuilder),
            streamsConfig);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams2, streamsLatch);

    await().atMost(Duration.ofSeconds(120)).until(() -> kafkaStreams2.state().equals(KafkaStreams.State.RUNNING));

    Taxi taxi3 = new Taxi(new TaxiId(UUID.randomUUID()),
            new PointImpl(0.4001, 0.6, spatialService.getSpatialContext()));
    commonTestUtils.produceSingleEvent(taxiInputTopic, taxi3.id, taxi3, new Header[0], taxiIdJsonSerde.serializer().getClass(),taxiJsonSerde.serializer().getClass());

    commonTestUtils.consumeAtLeastXEvents( geoHashIdJsonSerde.deserializer().getClass(),
            taxiPersonJsonSerde.deserializer().getClass(), taxiPersonOutputTopic, 3);
  }
}
