package com.example.kafka.producer;

import static com.example.kafka.commons.Constants.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.KafkaApplication;

public class SimpleProducer {

    private final static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

    private static Properties getProperties() {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return configs;
    }

    public static void produce() {
        Properties configs = getProperties();

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        ProducerRecord<String, String> pangyoRecord = new ProducerRecord<>(TOPIC_TEST, "Pangyo", "Pangyo");
        logger.info("before | {}", pangyoRecord);
        producer.send(pangyoRecord);
        logger.info("after | {}", pangyoRecord);

        ProducerRecord<String, String> busanRecord = new ProducerRecord<>(TOPIC_TEST, "Busan", "Busan");
        logger.info("before | {}", busanRecord);
        producer.send(busanRecord);
        logger.info("after | {}", busanRecord);

        producer.flush();
        producer.close();
    }

    public static void produceIterableMessages(int iter) {
        Properties configs = getProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        Random rand = new Random();
        List<String> domains = getDomainCollection();
        int i = 0;
        while (i < iter) {
            String domain = getRandomDomain(domains, rand);
            String key = "TEST_ONE_KEY";
            LocalDateTime now = LocalDateTime.now();
            String value = String.format("[%s] | %s", i, domain);


            // basic (async)
            producer.send(new ProducerRecord<String, String>(TOPIC_TEST, value));
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_TEST, value);

            // async
            producer.send(record, ((metadata, exception) -> {
                if (exception == null) {
                    logger.info("\n ###### record metadata received ##### \n" +
                        "partition:" + metadata.partition() +
                        "offset:" + metadata.offset() +
                        "timestamp:" + metadata.timestamp());
                } else {
                    logger.error("exception error from broker " + exception.getMessage());
                }
            }));
            i += 1;
        }
    }

    private static List<String> getDomainCollection() {
        List<String> domains = new ArrayList<>();
        domains.add("user");
        domains.add("product");
        domains.add("hotel");
        domains.add("admin");
        domains.add("travel");
        return domains;
    }

    private static String getRandomDomain(List<String> domains, Random rand) {
        return domains.get(rand.nextInt(5));
    }

}
