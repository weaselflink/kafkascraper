package de.bissell.kafkascraper

import org.assertj.core.api.KotlinAssertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

internal class TimeParsingTest {

    val relativeTo = Instant.parse("2019-01-02T12:00:00Z")

    @Test
    internal fun parsesNumberAndUnit() {
        assertThat(parseTime("15s", relativeTo)).isEqualTo(relativeTo.minusSeconds(15))
    }

    @Test
    internal fun parsesMinusNumberAndUnit() {
        assertThat(parseTime("-60m", relativeTo)).isEqualTo(relativeTo.minusSeconds(3600))
    }

    @Test
    internal fun parsesMinusNumberSpaceAndUnit() {
        assertThat(parseTime("-2 h", relativeTo)).isEqualTo(relativeTo.minusSeconds(7200))
    }

    @Test
    internal fun parsesNumberSpaceUnitAndAgo() {
        assertThat(parseTime("4 m ago", relativeTo)).isEqualTo(relativeTo.minusSeconds(240))
    }
}