package com.example.kafkaproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafkaproducer.consumer.SimpleConsumer;
import com.example.kafkaproducer.producer.SimpleProducer;

public class KafkaApplication {
    private final static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

    private final static String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TOPIC_NAME = "test";

    public static void main(String[] args) {
        logger.info("test start");
        // SimpleProducer.produce();
        SimpleConsumer.consume();
        logger.info("test end");
    }

}
