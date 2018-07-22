package io.imalik8088.github.kafkaStreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class SimpleKTable {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab_simple_ktable");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());


        StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Integer> kTableProducts = builder.table(KafkaProperties.TOPIC_IN);


        final KTable<String, Integer> product = kTableProducts.mapValues(i -> i + 1);

        product.print();

        System.out.println("queryableStoreName => " + product.queryableStoreName());


        product.toStream().to(KafkaProperties.TOPIC_SIMPLE_KTABLE);

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
