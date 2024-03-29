package com.example.kafka.consumer;

import static com.example.kafka.commons.Constants.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.Customer;
import com.example.kafka.KafkaApplication;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class AvroConsumer {
    private final static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);


    public void consumerAvro() {
        Properties props = getProperties();

        final Consumer<String, Customer> consumer = new KafkaConsumer<>(props); // Referencing Customer
        // final Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        try (consumer) {
            consumer.subscribe(Collections.singletonList(TOPIC_AVRO));
            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Customer> record : records) {
                    System.out.printf("Received message: value = %s%n", record.value());
                }
            }
        }
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        return properties;
    }
}
