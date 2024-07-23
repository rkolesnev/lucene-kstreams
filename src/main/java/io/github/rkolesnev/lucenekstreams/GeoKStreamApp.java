package io.github.rkolesnev.lucenekstreams;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public class GeoKStreamApp {

  public static void main(final String args[]) {

    final CountDownLatch latch = new CountDownLatch(1);
    SpatialService spatialService = new SpatialService();
    TaxiTopology taxiTopology = new TaxiTopology(spatialService);
    Topology topology = taxiTopology.build(new StreamsBuilder());

    Properties properties = getPropertiesForStreams();
    overrideKafkaBootstrapServerFromArgs(properties, args);
    createTopicsIfMissing(properties, TaxiTopology.PEOPLE_TOPIC, TaxiTopology.TAXI_TOPIC,
        TaxiTopology.TAXI_PEOPLE_TOPIC);
    try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties)) {
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().

          addShutdownHook(new Thread("GeoKStreamApp-shutdown-hook") {
            @Override
            public void run() {
              kafkaStreams.close();
              latch.countDown();
            }
          });

      kafkaStreams.start();
      latch.await();
    } catch (
        final Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }
    System.exit(0);
  }

  private static void createTopicsIfMissing(Properties properties, String... topics) {
    try {
      AdminClient adminClient = KafkaAdminClient.create(
          properties);
      adminClient.createTopics(Arrays.stream(topics).map(topic ->
          new NewTopic(topic, 1, (short) 1)).collect(Collectors.toList()));
    } catch (Exception e) { //suppressed
    }
  }

  private static void overrideKafkaBootstrapServerFromArgs(Properties properties,
      String[] args) {
    if (args.length == 1) {
      properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
    }
  }

  public static Properties getPropertiesForStreams() {
    Properties props = new Properties();
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "GeoKStream-app" + UUID.randomUUID());
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    return props;
  }
}
