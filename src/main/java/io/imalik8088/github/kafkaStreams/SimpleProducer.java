package io.imalik8088.github.kafkaStreams;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class SimpleProducer {

    private static ArrayList<String> products = new ArrayList<String>(Arrays.asList("jeans", "shirt", "pants", "sneaker", "running shoes", "watch", "suit"));

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        KafkaProducer<String, Integer> producer = new KafkaProducer<String, Integer>(props);
        String topic = KafkaProperties.TOPIC_IN;

        int numberOfRecords = 10; // number of records to send
        Random rand = new Random();

        try {
            for (int i = 0; i < numberOfRecords; i++) {
                long startTime = System.currentTimeMillis();
                Integer amount = rand.nextInt(11);
                final int randomProduct = amount % products.size();
                final String product = products.get(randomProduct);

                ProducerRecord<String, Integer> record = new ProducerRecord<String, Integer>(topic, product, amount);
                producer.send(record, new DemoCallBack(startTime, product, amount));
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
    private final String key;
    private final int message;

    public DemoCallBack(long startTime, String product, Integer amount) {
        this.startTime = startTime;
        this.key = product;
        this.message = amount;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {

            System.out.println(String.format("Key=%s : Value=%d \t Partition=%d \t Offset=%d \t Elapsed time=%dms",
                    key, message, metadata.partition(), metadata.offset(), elapsedTime));
        } else {
            exception.printStackTrace();
        }
    }
}
