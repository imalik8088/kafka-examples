package io.imalik8088.github.window;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.imalik8088.github.embeddedKafka.EmbeddedSingleNodeKafkaCluster;
import io.imalik8088.github.windowing.KafkaProperties;
import io.imalik8088.github.windowing.SimpleTumblingWindow;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.test.TestUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schema.OneMessageValue;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class SimpleTumblingWindowTest {
    private final Logger LOGGER = LoggerFactory.getLogger(SimpleTumblingWindowTest.class);


    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private KafkaStreams streams;

    @BeforeClass
    public static void createTopics() {
        CLUSTER.createTopic(KafkaProperties.TOPIC_SOURCE);
    }


    @Before
    public void createStreams() {
        streams =
                SimpleTumblingWindow.buildTumblingWindowStream(CLUSTER.bootstrapServers(),
                        CLUSTER.schemaRegistryUrl(),
                        TestUtils.tempDirectory().getPath());
    }

    @After
    public void stopStreams() {
        streams.close();
    }

    @Test
    public void shouldRunTheWikipediaFeedExample() throws Exception {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
        final KafkaProducer<String, OneMessageValue> producer = new KafkaProducer<>(props);

        producer.send(new ProducerRecord<>(KafkaProperties.TOPIC_SOURCE, new OneMessageValue("191.22.34.1", System.currentTimeMillis(), "www.confluent.io", "Firefox", 123)));
        producer.send(new ProducerRecord<>(KafkaProperties.TOPIC_SOURCE, new OneMessageValue("191.22.34.2", System.currentTimeMillis(), "www.google.de", "Safari", 124)));
        producer.send(new ProducerRecord<>(KafkaProperties.TOPIC_SOURCE, new OneMessageValue("191.22.34.3", System.currentTimeMillis(), "www.google.de", "Chrome", 125)));
        producer.send(new ProducerRecord<>(KafkaProperties.TOPIC_SOURCE, new OneMessageValue("191.22.34.4", System.currentTimeMillis(), "www.apache.com", "Safari", 126)));
        producer.send(new ProducerRecord<>(KafkaProperties.TOPIC_SOURCE, new OneMessageValue("191.22.34.5", System.currentTimeMillis(), "www.apache.com", "Safari", 127)));
        producer.send(new ProducerRecord<>(KafkaProperties.TOPIC_SOURCE, new OneMessageValue("191.22.34.6", System.currentTimeMillis(), "www.google.de", "Chrome", 128)));

        producer.flush();
        streams.start();

        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-avro-tumbling-window-client");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, Long>
                consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new LongDeserializer());

        Map<String, Long> expected = new HashMap<>();
        expected.put("www.apache.com", 2L);
        expected.put("www.google.de", 3L);
        expected.put("www.confluent.io", 1L);

        final Map<String, Long> actual = new HashMap<>();

        consumer.subscribe(Collections.singleton(KafkaProperties.TOPIC_SINK));

        LOGGER.info("aaaaaaaaaaaaaaaaaaaaaaaa {}", KafkaProperties.TOPIC_SINK);

        final long timeout = System.currentTimeMillis() + 3000L;
        while (!actual.equals(expected) && System.currentTimeMillis() < timeout) {
            final ConsumerRecords<String, Long> records = consumer.poll(1000);
            records.forEach(record -> actual.put(record.key(), record.value()));
        assertThat(expected, equalTo(actual));
        }


    }


}
