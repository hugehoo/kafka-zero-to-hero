package com.example.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.consumer.AvroConsumer;
import com.example.kafka.producer.AvroProducer;

public class KafkaApplication {
    private final static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

    public static void main(String[] args) {
        logger.info("test start");
        AvroProducer avroProducer = new AvroProducer();
        avroProducer.producerAvro();
        AvroConsumer avroConsumer = new AvroConsumer();
        avroConsumer.consumerAvro();
        logger.info("test end");
    }

}
