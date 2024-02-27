package com.example.kafka.consumer

import com.example.kafka.commons.Constants.BOOTSTRAP_SERVERS
import com.example.kafka.commons.Constants.GROUP_ID
import com.example.kafka.commons.Constants.TOPIC_TEST
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import kotlin.collections.HashMap

class SimpleKotlinConsumer {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SimpleKotlinConsumer::class.java)
    }

    fun consume() {
        val configs = Properties()
        configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        configs[ConsumerConfig.GROUP_ID_CONFIG] = GROUP_ID
        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        configs[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = true

        val consumer = KafkaConsumer<String, String>(configs)
        consumer.subscribe(Collections.singletonList(TOPIC_TEST))

        while (true) {
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(1))
            for (record: ConsumerRecord<String, String> in records) {
                logger.info("{} | {} | record | {} | ", record.partition(), record.timestamp(), record.value())
            }

            // background 에서 OffsetCommitCallback() 진행
            consumer.commitAsync(OffsetCommitCallback { offsets: Map<TopicPartition, OffsetAndMetadata>, e: Exception? ->
                if (e != null) {
                    System.err.println("Commit Failed")
                } else {
                    System.out.printf("Commit Succeeded $offsets")
                }
                if (e != null) {
                    logger.error("Commit failed for offsets {}", offsets, e)
                }
            })
        }
    }
}