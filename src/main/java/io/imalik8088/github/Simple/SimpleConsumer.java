package io.imalik8088.github.Simple;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

public class SimpleConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(props);
        ArrayList<String> topics = new ArrayList<String>();
        topics.add(KafkaProperties.TOPIC1);
        consumer.subscribe(topics);

        System.out.println("****** Subscribed topics ******");
        int index = 0;
        for (String topic : consumer.subscription()) {
            System.out.println(++index + ": " + topic);
        }

        try {
            while (true) {
                ConsumerRecords records = consumer.poll(1000);
                printRecords(records);
            }
        } finally {
            consumer.close();
        }

    }

    private static void printRecords(ConsumerRecords<Integer, String> records) {
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}
