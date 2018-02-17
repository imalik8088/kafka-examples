package io.imalik8088.github.KafkaStreams;

public class KafkaProperties {
    public static final String KAFKA_SERVER_URL = "192.168.99.100:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://192.168.99.100:8081";

    public static final String TOPIC_IN = "lab_kafka_streams_in";
    public static final String TOPIC_SIMPLE_KSTREAM = "lab_simple_kstream_out";
    public static final String TOPIC_KTABLE = "lab_ktable_out";

    public KafkaProperties() {
    }
}
