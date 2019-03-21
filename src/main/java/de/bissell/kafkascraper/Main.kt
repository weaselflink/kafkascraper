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

    val start = if (commandLine.getOptionValue("s") != null) {
        Instant.parse(commandLine.getOptionValue("s"))
    } else {
        Instant.now().minusSeconds(60L)
    }
    val end = if (commandLine.getOptionValue("e") != null) {
        Instant.parse(commandLine.getOptionValue("e"))
    } else {
        Instant.now().plusSeconds(60L)
    }
    val filter = if (commandLine.getOptionValue("f") != null) {
        Regex(commandLine.getOptionValue("f"))
    } else {
        null
    }

    val progress = if (commandLine.hasOption("p")) {
        if (commandLine.getOptionValue("p") != null) {
            Integer.parseInt(commandLine.getOptionValue("p"))
        } else {
            10_000
        }
    } else {
        0
    }

    return ScraperOptions(
            bootstrap = commandLine.getOptionValue("b"),
            topic = commandLine.getOptionValue("t"),
            start = start,
            end = end,
            filter = filter,
            progress = progress
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
        addOption("s", "start", true, "Start time in ISO-8601 (optional, defaults to one minute ago)")
        addOption("e", "end", true, "End time in ISO-8601 (optional, defaults to one minute in the future)")
        addOption("f", "filter", true, "Regular expression for filtering (optional)")
        addOption("h", "help", false, "Print usage")
        addOption("p", "progress", false, "Print a dot every n messages without match (default n is 10,000)")
    }
    return options
}

data class ScraperOptions(
        val bootstrap: String,
        val topic: String,
        val start: Instant = Instant.now().minusSeconds(1),
        val end: Instant = Instant.now(),
        val filter: Regex? = null,
        val progress: Int = 0
)
