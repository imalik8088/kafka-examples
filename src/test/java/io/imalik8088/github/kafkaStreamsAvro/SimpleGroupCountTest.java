package io.imalik8088.github.kafkaStreamsAvro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.imalik8088.github.kakfaStreamsAvro.KafkaProperties;
import io.imalik8088.github.kakfaStreamsAvro.SimpleGroupCount;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import schema.OneMessageValue;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertNull;

public class SimpleGroupCountTest {

    private TopologyTestDriver testDriver;
    private final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", KafkaProperties.SCHEMA_REGISTRY_URL);
    private StringSerializer stringSerializer = new StringSerializer();
    private final Serde<OneMessageValue> valueAvroSerde = new SpecificAvroSerde<>();

    private ConsumerRecordFactory<String, OneMessageValue> recordFactory;

    @Before
    public void setUpTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaProperties.SCHEMA_REGISTRY_URL);

        valueAvroSerde.configure(serdeConfig, false);
        recordFactory = new ConsumerRecordFactory<>(stringSerializer, valueAvroSerde.serializer());

        SimpleGroupCount simpleGroupCount = new SimpleGroupCount();
        Topology topology = simpleGroupCount.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void simpleCountTest() {
        OneMessageValue msg = new OneMessageValue("192.1.1.1", System.currentTimeMillis(), "www.example.com", "Firefox",
                new Random().nextInt(10000));
        pushNewInputRecord("Firefox", msg);
        pushNewInputRecord("Firefox", msg);
        pushNewInputRecord("Safari", msg);
        OutputVerifier.compareKeyValue(readOutput(), "Firefox", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "Firefox", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "Safari", 1L);
        assertNull(readOutput());

    }


    private void pushNewInputRecord(String key, OneMessageValue value) {
        testDriver.pipeInput(recordFactory.create(KafkaProperties.TOPIC_SOURCE, key, value));
    }

    private ProducerRecord<String, Long> readOutput() {
        return testDriver.readOutput(KafkaProperties.TOPIC_SINK, new StringDeserializer(), new LongDeserializer());
    }
}
