package com.example.kafkaproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaApplication {
    private final static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

    private final static String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TOPIC_NAME = "test";

    public static void main(String[] args) {
        System.out.println("test start");
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        String messageValue = "testMessage";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        logger.info("before | {}", record);
        producer.send(record);
        logger.info("after | {}", record);

        ProducerRecord<String, String> pangyoRecord = new ProducerRecord<>(TOPIC_NAME, "Pangyo",
            "Pangyo");
        producer.send(pangyoRecord);
        ProducerRecord<String, String> busanRecord = new ProducerRecord<>(TOPIC_NAME, "Busan",
            "Busan");
        producer.send(busanRecord);

        producer.flush();
        producer.close();
    }

}
