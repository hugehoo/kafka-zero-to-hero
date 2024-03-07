package com.example.kafka.producer

import com.example.Customer
import com.example.kafka.KafkaApplication
import com.example.kafka.commons.Constants
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*


class AvroProducer {
    private val name = listOf("성후", "은찬", "나영", "현구", "동인", "재휘", "성윤", "정현", "정수", "주형")
    private val color = listOf(
        "RED", "BLUE", "BLACK", "PURPLE", "ORANGE", "NAVY", "SKYBLUE", "MINT", "YELLOW",
        "WHITE"
    )

    fun producerAvro() {
        val start = System.currentTimeMillis()
        val iterate: MutableList<Int> = ArrayList()
        val configs = properties
        val producer = KafkaProducer<String, Customer>(configs)

        for (iter in 0..19) {
            val i = (Math.random() * 10).toInt()
            val customer = Customer.newBuilder()
                .setName(name[i])
                .setFavoriteColor(color[i])
//                .setFavoriteNumber(21)
                .build()
            val producerRecord = ProducerRecord<String, Customer>(Constants.TOPIC_AVRO, customer)
            producer.send(producerRecord)
            iterate.add(i)
        }
        val end = System.currentTimeMillis()
        logger.info("Duration : {}, size : {}", end - start, iterate.size)
        producer.flush()
        producer.close()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaApplication::class.java)
        private val properties: Properties
            get() {
                val properties = Properties()
                properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = Constants.BOOTSTRAP_SERVERS
                properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
                properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java.name
                properties["schema.registry.url"] = Constants.SCHEMA_REGISTRY_URL
                return properties
            }
    }
}
