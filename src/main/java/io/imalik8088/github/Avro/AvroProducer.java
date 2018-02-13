package io.imalik8088.github.Avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import schema.OneMessageValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class AvroProducer {

    private static ArrayList<String> useragents = new ArrayList<String>(Arrays.asList("Firefox", "Chrome", "Safari", "IE"));
    private static ArrayList<String> urls = new ArrayList<String>(Arrays.asList("www.example.com", "www.confluent.io", "www.google.com", "www.apache.com"));

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put("schema.registry.url", KafkaProperties.SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        KafkaProducer<Integer, Object> producer = new KafkaProducer<Integer, Object>(props);
        String topic = KafkaProperties.TOPIC1;

        int numberOfRecords = 10; // number of records to send

        try {
            for (int i = 0; i < numberOfRecords; i++) {
                OneMessageValue msg = new OneMessageValue();
                msg.setIp(String.format("91.22.34.%d", i % 256));
                msg.setTimestamp(System.currentTimeMillis());
                msg.setUrl(urls.get(i % urls.size()));
                msg.setUseragent(useragents.get(i % useragents.size()));
                msg.setSessionid((i * i) % 100000);

                long startTime = System.currentTimeMillis();
                ProducerRecord<Integer, Object> record = new ProducerRecord<Integer, Object>(topic, i, msg);
                producer.send(record, new DemoCallBack(startTime, i, msg));
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final Object message;

    public DemoCallBack(long startTime, int key, Object message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(String.format("Key=%d : Value=%s \t Partition=%d \t Offset=%d \t Elapsed time=%dms",
                    key, message, metadata.partition(), metadata.offset(), elapsedTime));
        } else {
            exception.printStackTrace();
        }
    }
}
