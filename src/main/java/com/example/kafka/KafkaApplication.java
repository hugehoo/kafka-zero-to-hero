package com.example.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.consumer.SimpleConsumer;
import com.example.kafka.producer.SimpleProducer;

public class KafkaApplication {
    private final static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

    public static void main(String[] args) {
        logger.info("test start");
        SimpleProducer.produceIterableMessages(1_000_000);
        SimpleConsumer.consume();
        logger.info("test end");
    }

}
