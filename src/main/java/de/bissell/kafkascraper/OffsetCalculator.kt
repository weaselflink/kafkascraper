package de.bissell.kafkascraper

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import java.time.Duration
import java.time.Instant

class OffsetCalculator(
        private val consumer: KafkaConsumer<*, *>,
        private val scraperOptions: ScraperOptions
) {

    fun offsets(): Offsets {
        checkForTopic()
        return Offsets(
                createTopicPartitions(),
                startOffsets(scraperOptions.start),
                endOffsets(scraperOptions.end)
        )
    }

    private fun checkForTopic() {
        try {
            val topics = consumer.listTopics(Duration.ofSeconds(10))
            val topicNames = topics.orEmpty().keys
            if (!topicNames.contains(scraperOptions.topic)) {
                throw TopicException("Could not find topic ${scraperOptions.topic} in available topics: $topicNames")
            }
        } catch (e: TimeoutException) {
            throw TopicException("Error while fetching topic list from ${scraperOptions.bootstrap}")
        }
    }

    private fun startOffsets(start: Instant): Map<TopicPartition, Long> {
        val lastOffsets = lastOffsets()
        val startOffsets = createOffsetsForTime(start)

        return lastOffsets
                .mapValues {
                    startOffsets[it.key] ?: 0L
                }

    }

    private fun endOffsets(end: Instant): Map<TopicPartition, Long> {
        val lastOffsets = lastOffsets()
        val endOffsets = createOffsetsForTime(end)

        return lastOffsets
                .mapValues {
                    endOffsets[it.key] ?: it.value - 1
                }
    }

    private fun lastOffsets(): Map<TopicPartition, Long> =
            consumer.endOffsets(createTopicPartitions())

    private fun createOffsetsForTime(time: Instant): Map<TopicPartition, Long> =
            consumer.offsetsForTimes(createPartitionTimeMap(time))
                    .filter { it.value != null }
                    .mapValues { it.value.offset() }

    private fun createPartitionTimeMap(time: Instant): Map<TopicPartition, Long> =
            createTopicPartitions()
                    .map { it to time.toEpochMilli() }
                    .toMap()

    private fun createTopicPartitions(): List<TopicPartition> =
            consumer.partitionsFor(scraperOptions.topic)
                    .map { TopicPartition(it.topic(), it.partition()) }
}

data class Offsets(
        val topicPartitions: List<TopicPartition>,
        val startOffsets: Map<TopicPartition, Long>,
        val endOffsets: Map<TopicPartition, Long>
)

class TopicException(message: String?) : Exception(message)

