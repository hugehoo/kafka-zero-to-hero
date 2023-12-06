package com.example.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.consumer.SimpleConsumer;

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
