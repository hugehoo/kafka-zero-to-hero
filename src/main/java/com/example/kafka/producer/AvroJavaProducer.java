package com.example.kafka.producer;

import static com.example.kafka.commons.Constants.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.Customer;
import com.example.kafka.KafkaApplication;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroJavaProducer {

    private final static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        return properties;
    }

    public void producerAvro() {
        Properties configs = getProperties();

        KafkaProducer<String, Customer> producer = new KafkaProducer<>(configs);

        List<String> name = List.of("성후", "은찬", "나영", "현구", "동인", "재휘", "성윤", "정현", "정수", "주형");
        List<String> color = List.of("RED", "BLUE", "BLACK", "PURPLE", "ORANGE", "NAVY", "SKYBLUE", "MINT", "YELLOW",
            "WHITE");

        long start = System.currentTimeMillis();
        List<Integer> iterate = new ArrayList<>();
        for (int iter = 0; iter < 20; iter++) {
            int i = (int)(Math.random() * 10);
            logger.info("index : {}", i);
            Customer customer = Customer.newBuilder()
                .setName(name.get(i))
                .setFavoriteColor(color.get(i))
                // .setFavoriteNumber(21)
                .build();
            ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(TOPIC_AVRO, customer);
            producer.send(producerRecord);
            iterate.add(i);
        }
        long end = System.currentTimeMillis();
        logger.info("Duration : {}, size : {}", end - start, iterate.size());
        producer.flush();
        producer.close();
    }
}
