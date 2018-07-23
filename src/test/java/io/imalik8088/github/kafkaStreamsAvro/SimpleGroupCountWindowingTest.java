package io.imalik8088.github.kafkaStreamsAvro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.imalik8088.github.kakfaStreamsAvro.KafkaProperties;
import io.imalik8088.github.kakfaStreamsAvro.SimpleGroupCount;
import io.imalik8088.github.kakfaStreamsAvro.SimpleGroupCountWindowing;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import schema.OneMessageValue;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNull;

public class SimpleGroupCountWindowingTest {

    private TopologyTestDriver testDriver;
    private final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", KafkaProperties.SCHEMA_REGISTRY_URL);
    private StringSerializer stringSerializer = new StringSerializer();
    private final Serde<OneMessageValue> valueAvroSerde = new SpecificAvroSerde<>();
    private final long currentTime = System.currentTimeMillis();

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

        SimpleGroupCountWindowing simpleGroupCountWindowing = new SimpleGroupCountWindowing();
        Topology topology = simpleGroupCountWindowing.createTopology();
        testDriver = new TopologyTestDriver(topology, config, currentTime);
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void simpleCountTest() throws InterruptedException {
        OneMessageValue msg = new OneMessageValue("192.1.1.1", System.currentTimeMillis(), "www.example.com", "Firefox",
                new Random().nextInt(10000));

        // INPUT
        pushNewInputRecord("Firefox", msg, currentTime);
        pushNewInputRecord("Firefox", msg, currentTime);
        pushNewInputRecord("Safari", msg, currentTime);
        pushNewInputRecord("Firefox", msg, moveInFutureInSec(10));

        // VERIFICATION
        readWindowOutputAndVerify(5, "Firefox", 1L);
        readWindowOutputAndVerify(5, "Firefox", 2L);
        readWindowOutputAndVerify(5, "Safari", 1L);

        readWindowOutputAndVerify(5, "Firefox", 1L);
        assertNull(readOutput());

    }

    private long moveInFutureInSec(long seconds) {
        return currentTime + TimeUnit.SECONDS.toMillis(seconds);
    }


    private void pushNewInputRecord(String key, OneMessageValue value, long time) {
        testDriver.pipeInput(recordFactory.create(KafkaProperties.TOPIC_SOURCE_WINDOWING, key, value, time));
    }

    private ProducerRecord<String, Long> readOutput() {
        return testDriver.readOutput(KafkaProperties.TOPIC_SINK_WINDOWING, new StringDeserializer(), new LongDeserializer());
    }

    private void readWindowOutputAndVerify(int WindowBatches, String key, long expectedValue) {
        for (int i = 0; i < WindowBatches; i++) {
            OutputVerifier.compareKeyValue(readOutput(), key, expectedValue);
        }
    }

}
