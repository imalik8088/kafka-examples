package io.imalik8088.github.KafkaStreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleKStream {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab_simple_kstreams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Integer> product = builder.stream(KafkaProperties.TOPIC_IN);


        final KStream<String, Integer> stringIntegerKStream = product.mapValues(i -> i + 1);


        stringIntegerKStream.print();
        stringIntegerKStream.to(KafkaProperties.TOPIC_SIMPLE_KSTREAM);


        KafkaStreams streams = new KafkaStreams(builder.build(),props);
        streams.start();
    }
}
