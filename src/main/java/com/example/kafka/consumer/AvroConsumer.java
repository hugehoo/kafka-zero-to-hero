package com.example.kafka.consumer;

import static com.example.kafka.commons.Constants.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
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

        final Consumer<String, Customer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_AVRO));

        try {
            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Customer> record : records) {
                    System.out.printf("Received message: key = %s, value = %s%n", record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://127.0.0.1:8081");
        return props;
    }
}
