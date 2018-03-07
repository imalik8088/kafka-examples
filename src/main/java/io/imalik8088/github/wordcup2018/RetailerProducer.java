package io.imalik8088.github.wordcup2018;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import schema.Order;
import schema.Retailer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class RetailerProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put("schema.registry.url", io.imalik8088.github.Avro.KafkaProperties.SCHEMA_REGISTRY_URL);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        String topic = KafkaProperties.TOPIC_RETAILERS;
        Random rand = new Random();

        final int numberOfRecords = KafkaProperties.retailerId.size(); // number of records to send

        try (KafkaProducer<Integer, Retailer> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < numberOfRecords; i++) {

                // RANDOM VALUES
                final int retailerId = KafkaProperties.retailerId.get(i);
                final String retailerName = KafkaProperties.retailerName.get(i % KafkaProperties.retailerName.size());
                final double ratailPrice = rand.nextDouble() * (rand.nextInt(3) + 3 );
                final String location = KafkaProperties.retailerLocation.get(rand.nextInt(11) % KafkaProperties.retailerLocation.size());

                final Retailer retailer = new Retailer(retailerId, retailerName, ratailPrice, location);


                ProducerRecord<Integer, Retailer> record = new ProducerRecord<>(topic, retailerId, retailer);

                producer.send(record, new RetailerCallback(retailerId, retailer));
                Thread.sleep(400);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class RetailerCallback implements Callback {

    private final int orderId;
    private final Retailer retailer;

    public RetailerCallback(Integer orderId, Retailer retailer) {
        this.orderId = orderId;
        this.retailer = retailer;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {

            System.out.println(String.format("Key=%d : Value=%s \t Partition=%d \t Offset=%d",
                    orderId, retailer, metadata.partition(), metadata.offset()));
        } else exception.printStackTrace();
    }
}
