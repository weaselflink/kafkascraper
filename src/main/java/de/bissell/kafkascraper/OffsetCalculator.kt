package de.bissell.kafkascraper

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Instant

class OffsetCalculator(
        private val consumer: KafkaConsumer<*, *>,
        private val scraperOptions: ScraperOptions
) {

    fun offsets() =
            Offsets(
                    createTopicPartitions(),
                    startOffsets(scraperOptions.start),
                    endOffsets(scraperOptions.end)
            )

    private fun startOffsets(start: Instant): Map<TopicPartition, Long> =
            createOffsetsForTime(start)

    private fun endOffsets(end: Instant): Map<TopicPartition, Long>  {
        val lastOffsets = lastOffsets()
        val endOffsets = createOffsetsForTime(end)

        return lastOffsets
                .mapValues {
                    val endOffset = endOffsets[it.key]
                    if (endOffset != null) {
                        endOffset
                    } else {
                        it.value - 1
                    }
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

