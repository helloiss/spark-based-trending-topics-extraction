package nl.svanwouw.trending.components

import nl.svanwouw.trending.types.{Topic, Tweet, Period}
import org.apache.spark.rdd.RDD

/**
 * Tokenizes tweets sorted by period into individual containing topics per period.
 * filtering the non-alphabetic characters, URLs, and short words.
 */
object Tokenizer extends PipelineComponent[(Period, Tweet), (Period, Topic)] {

  val MatchURL = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]"
  val MatchShortWords = "\\b\\w{1,2}\\b"
  val MatchNonAlphanumericSpace = "[^a-zA-Z0-9\\s]"

  /**
   * Processes tweets into individual topics.
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[(Period, Tweet)]): RDD[(Period, Topic)] = {
    input.flatMap {
      case (period, tweet) =>
        for (token <- tokenize(tweet)) yield {
        (period, token)
      }
    }
  }

  /**
   * Clean the string and split into tokens.
   * @param tweet The raw input of the tweet.
   * @return A list of tokens.
   */
  def tokenize(tweet: Tweet): Array[Topic]  = {
    tweet.v
      .toLowerCase // Convert all topics to lower case.
      .replaceAll(MatchURL, "") // Strip all URLs.
      .replaceAll(MatchNonAlphanumericSpace, " ") // Replace all remaining non-alphanumeric characters with a space.
      .replaceAll(MatchShortWords, "") // Remove all words shorter than 3 characters.
      .trim // Trim the whitespace (possible created) at the start and end.
      .split("\\s+").map(new Topic(_)) // Tokenize the remaining words into topics.
  }
}
