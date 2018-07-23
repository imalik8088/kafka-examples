package io.imalik8088.github.kakfaStreamsAvro;

public class KafkaProperties {

    public static final String KAFKA_SERVER_URL = "127.0.0.1:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081";
    public static final String TOPIC_SOURCE = "avro_streams_topic_source";
    public static final String TOPIC_SINK = "avro_streams_topic_sink";
    public static final String TOPIC_SOURCE_WINDOWING = "avro_windowing_streams_topic_source";
    public static final String TOPIC_SINK_WINDOWING = "avro_windowing_streams_topic_sink";

    private KafkaProperties() {}
}
