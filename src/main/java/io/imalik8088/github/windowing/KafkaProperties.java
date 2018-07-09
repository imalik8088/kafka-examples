package io.imalik8088.github.windowing;

public class KafkaProperties {

    public static final String KAFKA_SERVER_URL = "192.168.99.100:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://192.168.99.100:8081";
    public static final String TOPIC_SOURCE = "avro_streams_topic_source";
    public static final String TOPIC_SINK = "avro_streams_topic_sink";

    private KafkaProperties() {}
}
