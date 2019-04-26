package de.bissell.kafkascraper

import java.lang.Long
import java.time.Instant
import kotlin.system.exitProcess

val relativeRegex = Regex("""-?([0-9]+) ?([smh])(?: ago)?""")

fun parseTime(input: String, relativeTo: Instant) : Instant {
    if (relativeRegex.matches(input)) {
        return parseRelativeTime(input, relativeTo)
    }
    return Instant.parse(input)
}

fun parseRelativeTime(input: String, relativeTo: Instant): Instant {
    val matchResult = relativeRegex.matchEntire(input)
    if (matchResult == null || matchResult.groupValues.size != 3) {
        println("Could not match time: $input")
        exitProcess(1)
    }
    val number = Long.parseUnsignedLong(matchResult.groupValues[1])
    val unit = matchResult.groupValues[2]

    return when (unit) {
        "s" -> relativeTo.minusSeconds(number)
        "m" -> relativeTo.minusSeconds(number * 60)
        else -> relativeTo.minusSeconds(number * 3600)
    }
}
