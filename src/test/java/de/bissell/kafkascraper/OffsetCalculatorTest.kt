package de.bissell.kafkascraper

import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.KotlinAssertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class OffsetCalculatorTest {

    private val dummyTopic = "topic"
    private val dummyPartition = 42

    private val consumer = mockk<KafkaConsumer<String, String>>(relaxed = true)
    private val scraperOptions = ScraperOptions(
            bootstrap = "localhost",
            topic = dummyTopic,
            start = Instant.ofEpochMilli(100),
            end = Instant.ofEpochMilli(200))

    @BeforeEach
    internal fun setUp() {
        clearMocks(consumer)
    }

    @Test
    internal fun createsTopicPartitions() {
        every { consumer.partitionsFor(any()) } returns listOf(aPartitionInfo())

        val offsetCalculator = OffsetCalculator(consumer, scraperOptions)

        assertThat(offsetCalculator.offsets().topicPartitions)
                .containsExactly(TopicPartition(dummyTopic, dummyPartition))
    }

    private fun aPartitionInfo() =
        PartitionInfo(dummyTopic, dummyPartition, null, null, null)
}