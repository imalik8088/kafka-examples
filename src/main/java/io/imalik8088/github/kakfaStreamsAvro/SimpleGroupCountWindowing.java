package io.imalik8088.github.kakfaStreamsAvro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import schema.OneMessageValue;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.kstream.Printed.toSysOut;

public class SimpleGroupCountWindowing {

    public static void main(String[] args) {
        SimpleGroupCountWindowing simpleGroupCount = new SimpleGroupCountWindowing();
        simpleGroupCount.startStream();
    }

    private void startStream() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "APP" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaProperties.SCHEMA_REGISTRY_URL);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/test");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaStreams streams = new KafkaStreams(this.createTopology(), props);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, OneMessageValue> clickStream = builder.stream(KafkaProperties.TOPIC_SOURCE_WINDOWING);

        long windowSizeMs = TimeUnit.SECONDS.toMillis(10);
        long advancedWindowSizeMs = TimeUnit.SECONDS.toMillis(2);

        KStream<String, Long> ttttttt = clickStream
//        clickStream
                .mapValues(click -> "https://" + click.getUrl())
                .groupByKey()
                .windowedBy(TimeWindows.of(windowSizeMs).advanceBy(advancedWindowSizeMs))
                .count()
                .toStream((windowHop, count) -> windowHop.key());

        ttttttt.to(KafkaProperties.TOPIC_SINK_WINDOWING, Produced.with(Serdes.String(), Serdes.Long()));
        ttttttt.print(Printed.<String, Long>toSysOut().withLabel("SimpleGroupCountWindowing"));

        return builder.build();
    }
}
