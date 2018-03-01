package com.example.kafka.producer;

import static com.example.kafka.commons.Constants.*;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.Customer;
import com.example.kafka.KafkaApplication;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroProducer {

    private final static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

    private static Properties getProperties() {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        configs.put("schema.registry.url", "http://127.0.0.1:8081");
        // configs.put(ProducerConfig., KafkaAvroSerializer.class.getName());
        return configs;
    }

    public void produceAvroScheme() {
        Properties configs = getProperties();

        KafkaProducer<String, Customer> producer = new KafkaProducer<>(configs);
        Customer customer = Customer.newBuilder()
            .setName("Hoo")
            .setFavoriteColor("Blue")
            .setFavoriteNumber(21)
            .build();


        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(TOPIC_AVRO, customer);
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println("Success!");
                    System.out.println(metadata.toString());
                } else {
                    exception.printStackTrace();
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
