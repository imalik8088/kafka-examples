package io.imalik8088.github.kakfaStreamsAvro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import schema.OneMessageValue;

import java.util.*;

public class AvroStreamsProducer {

    private static ArrayList<String> useragents = new ArrayList<String>(Arrays.asList("Firefox", "Chrome", "Safari", "IE", "Opera", "Tor"));
    private static ArrayList<String> urls = new ArrayList<String>(Arrays.asList("www.example.com", "www.confluent.io", "www.google.com", "www.apache.com"));

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put("schema.registry.url", KafkaProperties.SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        String topic = KafkaProperties.TOPIC_SOURCE;

        int numberOfRecords = 100; // number of records to send

        try (KafkaProducer<String, Object> producer = new KafkaProducer<String, Object>(props)) {
            for (int i = 0; i < numberOfRecords; i++) {
                OneMessageValue msg = new OneMessageValue();
                msg.setIp(String.format("191.22.34.%d", i % 256));
                msg.setTimestamp(System.currentTimeMillis());
                msg.setUrl(urls.get(i % urls.size()));
                String randomUserAgent = useragents.get(i % useragents.size());
                msg.setUseragent(randomUserAgent);
                msg.setSessionid((i * i) % 100000);

                long startTime = System.currentTimeMillis();
                ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(topic, randomUserAgent, msg);
                producer.send(record, new DemoCallBack(startTime, randomUserAgent, msg));
                int rand = new Random().nextInt(750);
                Thread.sleep(rand * 2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final String key;
    private final Object message;

    public DemoCallBack(long startTime, String key, Object message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(String.format("Key=%s : Value=%s \t Partition=%d \t Offset=%d \t Elapsed time=%dms",
                    key, message, metadata.partition(), metadata.offset(), elapsedTime));
        } else {
            exception.printStackTrace();
        }
    }
}
