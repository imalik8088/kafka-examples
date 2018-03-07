package io.imalik8088.github.wordcup2018;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import schema.Retailer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class StreamToKTable {

    //    https://docs.confluent.io/current/streams/developer-guide/datatypes.html
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaProperties.APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaProperties.SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);


//        Schema-Registry
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaProperties.SCHEMA_REGISTRY_URL);

        final Serde<Integer> integerSerde = Serdes.Integer();
        final Serde<Retailer> valueSpecificAvroSerde = new SpecificAvroSerde<>();
        valueSpecificAvroSerde.configure(serdeConfig, false);


        StreamsBuilder builder = new StreamsBuilder();
        final KTable<Integer, Retailer> retailerKTable = builder.table(
                KafkaProperties.TOPIC_RETAILERS,
                Consumed.with(integerSerde, valueSpecificAvroSerde)
        );




        retailerKTable.toStream().to(KafkaProperties.TOPIC_RETAILERS_KTABLE, Produced.with(integerSerde, valueSpecificAvroSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
