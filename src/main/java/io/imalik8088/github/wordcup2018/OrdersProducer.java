package io.imalik8088.github.wordcup2018;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import schema.Order;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class OrdersProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put("schema.registry.url", io.imalik8088.github.Avro.KafkaProperties.SCHEMA_REGISTRY_URL);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        String topic = KafkaProperties.TOPIC_ORDERS;
        Random rand = new Random();


        int numberOfRecords = 20; // number of records to send

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < numberOfRecords; i++) {

                // RANDOM VALUES
                final String orderId = UUID.randomUUID().toString();
                final int quantity = rand.nextInt(4) + 1;
                final String product = KafkaProperties.products.get(i * quantity % KafkaProperties.products.size());
                final int retailerId = KafkaProperties.retailerId.get(i * quantity % KafkaProperties.retailerId.size());

                final Order order = new Order(orderId, retailerId, quantity, product);

                ProducerRecord<String, Order> record = new ProducerRecord<>(topic, orderId, order);

                producer.send(record, new ProducerCallback(orderId, order));
                Thread.sleep(700);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class ProducerCallback implements Callback {

    private final String orderId;
    private final Order oder;

    public ProducerCallback(String orderId, Order oder) {
        this.orderId = orderId;
        this.oder = oder;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {

            System.out.println(String.format("Key=%s : Value=%s \t Partition=%d \t Offset=%d",
                    orderId, oder, metadata.partition(), metadata.offset()));
        } else exception.printStackTrace();
    }
}
