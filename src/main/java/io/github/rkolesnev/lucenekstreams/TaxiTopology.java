package io.github.rkolesnev.lucenekstreams;

import static org.apache.kafka.streams.KeyValue.pair;

import io.github.rkolesnev.lucenekstreams.statestore.LuceneSpatialWindowBytesStoreSupplier;
import io.github.rkolesnev.lucenekstreams.transformer.SpatialJoinByPointRadiusTransformer;
import java.time.Duration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.internals.SpatialWindowStoreBuilder;

@Slf4j
@RequiredArgsConstructor
public class TaxiTopology {

  public static final String PEOPLE_TOPIC = "people";
  public static final Duration JOIN_WINDOW = Duration.ofMinutes(2);
  public static final String TAXI_PEOPLE_TOPIC = "taxiPeople";
  public static final String TAXI_TOPIC = "taxiTopic";
  private final SpatialService spatialService;

  private static final double radiusMeters = 2.5 * 1609.34; // 2.5 miles in meters

  public Topology buildOld(StreamsBuilder streamsBuilder) {
    PersonIdJsonSerde personIdJsonSerde = new PersonIdJsonSerde();
    PersonJsonSerde personJsonSerde = new PersonJsonSerde();
    BufferedPersonJsonSerde bufferedPersonJsonSerde = new BufferedPersonJsonSerde();
    GeoHashIdJsonSerde geoHashIdJsonSerde = new GeoHashIdJsonSerde();
    TaxiIdJsonSerde taxiIdJsonSerde = new TaxiIdJsonSerde();
    TaxiJsonSerde taxiJsonSerde = new TaxiJsonSerde();
    TaxiPersonJsonSerde taxiPersonJsonSerde = new TaxiPersonJsonSerde();

    var peopleTopic = streamsBuilder.stream(PEOPLE_TOPIC,
        Consumed.with(personIdJsonSerde, personJsonSerde));
    var peopleHash = peopleTopic.mapValues((readOnlyKey, value) -> new BufferedPerson(value))
        .flatMap((key, bufferedPerson) -> processGeoHash(bufferedPerson));
    JoinWindows joinWindows = JoinWindows.of(JOIN_WINDOW).grace(Duration.ZERO);
    LuceneSpatialWindowBytesStoreSupplier thisLuceneSpatialWindowBytesStoreSupplier = new LuceneSpatialWindowBytesStoreSupplier(
        "this-join-state-store", joinWindows.size() + joinWindows.gracePeriodMs(),
        joinWindows.size());
    LuceneSpatialWindowBytesStoreSupplier otherLuceneSpatialWindowBytesStoreSupplier = new LuceneSpatialWindowBytesStoreSupplier(
        "other-join-state-store", joinWindows.size() + joinWindows.gracePeriodMs(),
        joinWindows.size());

    streamsBuilder.<TaxiId, Taxi>stream(TAXI_TOPIC, Consumed.with(taxiIdJsonSerde, taxiJsonSerde))
        .map(
            (key, value) -> pair(new GeoHashId(spatialService.geohash(value.getPosition())), value))
        .join(peopleHash, this::joinFunction, joinWindows,
            StreamJoined.with(geoHashIdJsonSerde, taxiJsonSerde, bufferedPersonJsonSerde)
                .withThisStoreSupplier(
                    thisLuceneSpatialWindowBytesStoreSupplier.asWindowBytesStoreSupplier())
                .withOtherStoreSupplier(
                    otherLuceneSpatialWindowBytesStoreSupplier.asWindowBytesStoreSupplier()))
        .filter((key, value) ->
            spatialService.contained(value.getBufferedPerson().getBuffer(),
                value.getTaxi().getPosition())
        ).to(TAXI_PEOPLE_TOPIC, Produced.with(geoHashIdJsonSerde, taxiPersonJsonSerde));
    ;

    return streamsBuilder.build();
  }


  public Topology build(StreamsBuilder streamsBuilder) {
    PersonIdJsonSerde personIdJsonSerde = new PersonIdJsonSerde();
    PersonJsonSerde personJsonSerde = new PersonJsonSerde();
    BufferedPersonJsonSerde bufferedPersonJsonSerde = new BufferedPersonJsonSerde();
    GeoHashIdJsonSerde geoHashIdJsonSerde = new GeoHashIdJsonSerde();
    TaxiIdJsonSerde taxiIdJsonSerde = new TaxiIdJsonSerde();
    TaxiJsonSerde taxiJsonSerde = new TaxiJsonSerde();
    TaxiPersonJsonSerde taxiPersonJsonSerde = new TaxiPersonJsonSerde();
    String thisStateStoreName = "this-join-state-store";
    String otherStateStoreName = "other-join-state-store";

    JoinWindows joinWindows = JoinWindows.of(JOIN_WINDOW).grace(Duration.ZERO).before(Duration.ofMillis(5));
    LuceneSpatialWindowBytesStoreSupplier thisLuceneSpatialWindowBytesStoreSupplier = new LuceneSpatialWindowBytesStoreSupplier(
        thisStateStoreName, joinWindows.size() + joinWindows.gracePeriodMs(),
        joinWindows.size());
    LuceneSpatialWindowBytesStoreSupplier otherLuceneSpatialWindowBytesStoreSupplier = new LuceneSpatialWindowBytesStoreSupplier(
        otherStateStoreName, joinWindows.size() + joinWindows.gracePeriodMs(),
        joinWindows.size());

    SpatialWindowStoreBuilder<GeoHashId, BufferedPerson> thisStoreBuilder = new SpatialWindowStoreBuilder<>(
        thisLuceneSpatialWindowBytesStoreSupplier, geoHashIdJsonSerde, bufferedPersonJsonSerde,
        Time.SYSTEM);
    SpatialWindowStoreBuilder<GeoHashId, Taxi> otherStoreBuilder = new SpatialWindowStoreBuilder<>(
        otherLuceneSpatialWindowBytesStoreSupplier, geoHashIdJsonSerde, taxiJsonSerde,
        Time.SYSTEM);

    streamsBuilder.addStateStore(thisStoreBuilder);
    streamsBuilder.addStateStore(otherStoreBuilder);

    var peopleTopic = streamsBuilder.<PersonId, Person>stream(PEOPLE_TOPIC,
        Consumed.with(personIdJsonSerde, personJsonSerde));


    var peopleHashStream = peopleTopic.mapValues((readOnlyKey, value) -> new BufferedPerson(value))
        .flatMap((key, bufferedPerson) -> {
          Iterable<KeyValue<GeoHashId, BufferedPerson>> processed = processGeoHash(bufferedPerson);
          log.info("BufferedPerson -> {}", processed);
          return processed;
        }).flatTransformValues(
            () -> new SpatialJoinByPointRadiusTransformer<GeoHashId, BufferedPerson, Taxi, TaxiPerson>(
                true, thisStateStoreName, otherStateStoreName, joinWindows,
                (readOnlyKey, bufferedPerson, taxi) -> joinFunction(taxi, bufferedPerson),
                BufferedPerson::getPosition, radiusMeters),
            thisStateStoreName, otherStateStoreName);


    var taxiStream = streamsBuilder.<TaxiId, Taxi>stream(TAXI_TOPIC,
            Consumed.with(taxiIdJsonSerde, taxiJsonSerde))
        .map(
            (key, value) -> {
              KeyValue<GeoHashId, Taxi> pair = pair(
                  new GeoHashId(spatialService.geohash(value.getPosition())), value);
              log.info("Taxi -> {}", pair);
              return pair;
            }).flatTransformValues(
            () -> new SpatialJoinByPointRadiusTransformer<GeoHashId, Taxi, BufferedPerson, TaxiPerson>(
                false, otherStateStoreName, thisStateStoreName, joinWindows,
                (readOnlyKey, taxi, bufferedPerson) -> joinFunction(taxi, bufferedPerson),
                Taxi::getPosition, radiusMeters),
            otherStateStoreName, thisStateStoreName);

    var mergedStream = taxiStream.merge(peopleHashStream);

    mergedStream.to(TAXI_PEOPLE_TOPIC, Produced.with(geoHashIdJsonSerde, taxiPersonJsonSerde));
    // Transformer doing the lookup against opposite stream's state store
    // Transformer Saving to Windowed State Store - based on KStreamJoinWindow
    // Merge node (common for both streams).

    return streamsBuilder.build();

  }

  private TaxiPerson joinFunction(Taxi taxi, BufferedPerson person) {
    return new TaxiPerson(person, taxi);
  }

  private Iterable<KeyValue<GeoHashId, BufferedPerson>> processGeoHash(
      BufferedPerson bufferedPerson) {
    return spatialService.geohash(bufferedPerson.getBuffer())
        .stream()
        .map(geohash -> pair(new GeoHashId(geohash.toBase32()), bufferedPerson))
        .toList();
  }
}
