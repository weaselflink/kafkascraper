package de.bissell.kafkascraper

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.time.Instant
import java.util.*

class Scraper(private val scraperOptions: ScraperOptions) {

    private val consumer: KafkaConsumer<String, String>
    private val offsets: Offsets
    private val currentOffsets: MutableMap<TopicPartition, Long> = mutableMapOf()
    private var receivedCount: Long = 0

    init {
        consumer = createConsumer()
        offsets = OffsetCalculator(consumer, scraperOptions).offsets()
        println(offsets.toString())
        offsets.topicPartitions
                .forEach { currentOffsets[it] = 0L }
    }

    fun scrape() {
        seekToStart()

        var recordsWithoutMatch = 0
        var lastSymbolWasDot = false
        while (!Thread.currentThread().isInterrupted && !finalOffsetsReached()) {
            val records = consumer.poll(Duration.ofSeconds(1))
            recordsWithoutMatch += records.count()
            updateCurrentOffsets(records)
            records.forEach {
                val time = Instant.ofEpochMilli(it.timestamp())
                val key = it.key()
                val body = it.value()

                if (recordInTimeSpan(it) && recordMatchesFilter(it)) {
                    if (lastSymbolWasDot) {
                        println()
                        lastSymbolWasDot = false
                    }
                    println("$time $key $body")
                    receivedCount++
                    recordsWithoutMatch = 0
                }
            }
            while (scraperOptions.progress > 0 && recordsWithoutMatch >= scraperOptions.progress) {
                print(".")
                lastSymbolWasDot = true
                recordsWithoutMatch -= scraperOptions.progress
            }
        }
        if (lastSymbolWasDot) {
            println()
        }
        println("Received $receivedCount records")

        consumer.close()
    }

    private fun recordMatchesFilter(record: ConsumerRecord<String, String>): Boolean {
        if (scraperOptions.filter == null) {
            return true
        }

        return (record.key() != null && scraperOptions.filter.matches(record.key())) ||
                (record.value() != null && scraperOptions.filter.matches(record.value()))
    }

    private fun recordInTimeSpan(record: ConsumerRecord<String, String>): Boolean {
        val time = Instant.ofEpochMilli(record.timestamp())

        return time.isAfter(scraperOptions.start) && time.isBefore(scraperOptions.end)
    }

    private fun updateCurrentOffsets(records: ConsumerRecords<String, String>) {
        records
                .filter { it.topic() == scraperOptions.topic }
                .forEach {
                    val offsetKey = TopicPartition(scraperOptions.topic, it.partition())
                    val currentOffset = currentOffsets[offsetKey]!!
                    if (it.offset() > currentOffset) {
                        currentOffsets[offsetKey] = it.offset()
                    }
                }
    }

    private fun finalOffsetsReached() =
            currentOffsets
                    .all { it.value >= offsets.endOffsets[it.key]!! }

    private fun seekToStart() {
        consumer.assign(offsets.topicPartitions)
        offsets.startOffsets
                .forEach { consumer.seek(it.key, it.value) }
    }

    private fun createConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().apply {
            this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = scraperOptions.bootstrap
            this[ConsumerConfig.GROUP_ID_CONFIG] = "kafkascraper"
            this[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
            this[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
            this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        }
        return KafkaConsumer(properties)
    }
}