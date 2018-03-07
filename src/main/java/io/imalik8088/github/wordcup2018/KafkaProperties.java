package io.imalik8088.github.wordcup2018;

import java.util.ArrayList;
import java.util.Arrays;

public class KafkaProperties {

    public static final String KAFKA_SERVER_URL = "192.168.99.100:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://192.168.99.100:8081";

    public static final String TOPIC_ORDERS = "lab_wc18_orders";
    public static final String TOPIC_RETAILERS = "lab_wc18_retailers";

//    Kafka Strams
    public static final String TOPIC_RETAILERS_KTABLE = "lab_wc18_retailers_ktable";
    public static final String APP_ID = "wc18_retailer_ktable";









//    RANDOM COLLECTIONS FOR DATA GENERATOR

    public static ArrayList<String> products = new ArrayList<String>(
            Arrays.asList("vuvuzela", "vuvuzela", "vuvuzela", "vuvuzela", "vuvuzela", "wodka", "vuvuzela"));


    public static ArrayList<Integer> retailerId = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

    public static ArrayList<String> retailerName = new ArrayList<String>(
            Arrays.asList("Shanghai", "The Vuvuzelers", "Masterretailer", "Hongkong", "Noise Maker", "Foxconn", "Samsung"));

    public static ArrayList<String> retailerLocation = new ArrayList<String>(Arrays.asList("China", "Japan", "South Korea"));

    public KafkaProperties() {
    }
}
