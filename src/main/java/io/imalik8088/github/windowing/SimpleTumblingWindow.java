package io.imalik8088.github.windowing;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import schema.OneMessageValue;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SimpleTumblingWindow {


    public static void main(String[] args) {
        KafkaStreams streams = buildTumblingWindowStream(KafkaProperties.KAFKA_SERVER_URL, KafkaProperties.SCHEMA_REGISTRY_URL, "/tmp/kafka-streams");
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    public static KafkaStreams buildTumblingWindowStream(final String bootstrapServer, final String schemaRegistryUrl, final String stateDir) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab_simple_tumling_window");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "avro-tumbling-window-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // define
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, OneMessageValue> clickStream = builder.stream(KafkaProperties.TOPIC_SOURCE);

        long windowSizeMs = TimeUnit.SECONDS.toMillis(5);

        clickStream
                .mapValues(click -> "https://" + click.getUrl())
                .groupByKey()
                .windowedBy(TimeWindows.of(windowSizeMs))
                .count()
                .toStream().to(KafkaProperties.TOPIC_SINK);


        // build & start
        return new KafkaStreams(builder.build(), props);
    }

}
