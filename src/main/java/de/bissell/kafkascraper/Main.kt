package de.bissell.kafkascraper

import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import java.time.Instant
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    parseCommandLine(args).also {
        println(it)
        scrape(it)
    }
}

private fun scrape(scraperOptions: ScraperOptions) {
    Scraper(scraperOptions).scrape()
}

private fun parseCommandLine(args: Array<String>): ScraperOptions {
    val options = getAvailableOptions()

    val commandLine = DefaultParser().parse(options, args)

    if (commandLine.options.isEmpty() || commandLine.hasOption("h")) {
        printHelp()
    }
    if (!commandLine.hasOption("b") || !commandLine.hasOption("t")) {
        printHelp()
    }

    val now = Instant.now()
    val start = if (commandLine.getOptionValue("s") != null) {
        parseTime(commandLine.getOptionValue("s"), now)
    } else {
        now.minusSeconds(60L)
    }
    val end = if (commandLine.getOptionValue("e") != null) {
        parseTime(commandLine.getOptionValue("e"), now)
    } else {
        now.plusSeconds(60L)
    }
    val filter = if (commandLine.getOptionValue("f") != null) {
        Regex(commandLine.getOptionValue("f"))
    } else {
        null
    }

    val progress = if (commandLine.getOptionValue("p") != null) {
        Integer.parseInt(commandLine.getOptionValue("p"))
    } else {
        0
    }
    val count = if (commandLine.getOptionValue("c") != null) {
        Integer.parseInt(commandLine.getOptionValue("c"))
    } else {
        null
    }

    return ScraperOptions(
            bootstrap = commandLine.getOptionValue("b"),
            topic = commandLine.getOptionValue("t"),
            start = start,
            end = end,
            filter = filter,
            progress = progress,
            count = count
    )
}

private fun printHelp() {
    HelpFormatter().printHelp("kafkascraper", getAvailableOptions())
    exitProcess(0)
}

private fun getAvailableOptions(): Options {
    val options = Options().apply {
        addOption("b", "bootstrap", true, "List of bootstrap server URLs (required, comma separated)")
        addOption("t", "topic", true, "Name of topic (required)")
        addOption("s", "start", true, """Start time in ISO-8601 or as a relative string like "-10s", "-1 m" or "5 h ago" (optional, defaults to one minute ago)""")
        addOption("e", "end", true, """End time in ISO-8601 or as a relative string like "-10s", "-1 m" or "5 h ago" (optional, defaults to one minute in the future)""")
        addOption("f", "filter", true, "Regular expression for filtering (optional)")
        addOption("h", "help", false, "Print usage")
        addOption("p", "progress", true, "Print a dot every n messages without match (optional)")
        addOption("c", "count", true, "Only output given count of matching records (optional)")
    }
    return options
}

data class ScraperOptions(
        val bootstrap: String,
        val topic: String,
        val start: Instant = Instant.now().minusSeconds(1),
        val end: Instant = Instant.now(),
        val filter: Regex? = null,
        val progress: Int = 0,
        val count: Int? = null
)
