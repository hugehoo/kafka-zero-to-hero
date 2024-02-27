package com.example.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.consumer.SimpleKotlinConsumer;
import com.example.kafka.producer.SimpleKotlinProducer;
import com.example.kafka.producer.SimpleProducer;

public class KafkaApplication {
    private final static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

    public static void main(String[] args) {
        logger.info("test start");
        SimpleKotlinProducer producer = new SimpleKotlinProducer();
        producer.iterableProduce(1000);

        SimpleKotlinConsumer simpleKotlinConsumer = new SimpleKotlinConsumer();
        simpleKotlinConsumer.consume();
        logger.info("test end");
    }

}
