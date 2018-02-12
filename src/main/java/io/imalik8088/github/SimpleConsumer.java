package io.imalik8088.github;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

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
            while (true){
                ConsumerRecords records = consumer.poll(1000);
                printRecords(records);
            }
        } finally {
            consumer.close();
        }



//        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
//        props.put(ProducerConfig.CLIENT_ID_CONFIG , UUID.randomUUID().toString());
//
//        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
//        String topic = KafkaProperties.TOPIC1;
//
//        int numberOfRecords = 1000; // number of records to send
//
//        try {
//            for (int i = 0; i < numberOfRecords; i++ ) {
//                String messageStr = "Message_" + i;
//                long startTime = System.currentTimeMillis();
//                ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, i, messageStr);
//                producer.send(record);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            producer.close();
//        }
    }

    private static void printRecords(ConsumerRecords<Integer, String> records)
    {
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}
