package com.example.kafka.producer


import com.example.kafka.commons.Constants.BOOTSTRAP_SERVERS
import com.example.kafka.commons.Constants.TOPIC_TEST
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*


class SimpleKotlinProducer {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SimpleKotlinProducer::class.java)
    }

    private fun getProperties(): Properties {
        val configs = Properties()
        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        return configs
    }

    fun simpleProduce() {
        val configs = getProperties()
        val producer = KafkaProducer<String, String>(configs)

        val busanRecord = ProducerRecord<String, String>(TOPIC_TEST, "city", "busan")
        producer.send(busanRecord)

        val seoulRecord = ProducerRecord<String, String>(TOPIC_TEST, "city", "seoul")
        producer.send(seoulRecord)

        producer.flush()
        producer.close()
    }

    fun iterableProduce(iter: Int) {
        val configs = getProperties()
        val producer = KafkaProducer<String, String>(configs)
        val random = Random()
        val domains: List<String> = getDomainCollection()
        var i:Int = 0
        while (i <= iter) {
            val domain = getRandomDomain(domains, random)
            val key = "TEST_ONE_KEY"
            val now = LocalDateTime.now()
            val value = String.format("[%s] | %s", i, domain)

            // async
            producer.send(ProducerRecord(TOPIC_TEST, value)) { metadata: RecordMetadata, exception: Exception? ->
                if (exception == null) {
                    logger.info(
                        "\n ###### record metadata received ##### \n" +
                                "partition:" + metadata.partition() +
                                "offset:" + metadata.offset() +
                                "timestamp:" + metadata.timestamp()
                    )
                } else {
                    logger.error("exception error from broker " + exception.message)
                }
            }
            i += 1;
        }
    }

    private fun getDomainCollection(): List<String> {
        val domains: MutableList<String> = ArrayList()
        domains.add("user")
        domains.add("product")
        domains.add("hotel")
        domains.add("admin")
        domains.add("travel")
        return domains
    }

    private fun getRandomDomain(domains: List<String>, rand: java.util.Random): String {
        return domains[rand.nextInt(5)]
    }
}