package com.example.kafkaproducer.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

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

import com.example.kafkaproducer.KafkaApplication;

public class SimpleConsumer {
    private final static String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TOPIC_NAME = "test";
    private static final String GROUP_ID = "test-group";
    private final static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

    public static void consume() {

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // 명시적으로 commitSync() 를 할 필요 없다.
        // configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 60000); // 특정 단위시간당 커밋하게 됨
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 명시적으로 commitSync() 를 할 필요 없다.

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record | {}", record.value());
            }

            // background 에서 OffsetCommitCallback() 진행
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                    if (e != null) {
                        System.err.println("Commit Failed");
                    } else {
                        System.out.println("Commit Succeeded" + offsets);
                    }
                    if (e != null) {
                        logger.error("Commit failed for offsets {}", offsets, e);
                    }
                }
            });
        }
    }
}
