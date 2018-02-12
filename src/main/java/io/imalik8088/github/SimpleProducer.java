package io.imalik8088.github;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class SimpleProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.CLIENT_ID_CONFIG , UUID.randomUUID().toString());

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        String topic = KafkaProperties.TOPIC1;

        int numberOfRecords = 10; // number of records to send

        try {
            for (int i = 0; i < numberOfRecords; i++ ) {
                String messageStr = "Message_" + i;
                long startTime = System.currentTimeMillis();
                ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, i, messageStr);
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
