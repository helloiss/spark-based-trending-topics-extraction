package nl.svanwouw.trending.components

/**
 * Helper for extracting tweets per hour.
 */
object TweetExtractorHours extends TweetExtractor(periodSize = 3600) {
}
