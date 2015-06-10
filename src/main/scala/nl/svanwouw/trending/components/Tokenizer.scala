package nl.svanwouw.trending.components

import org.apache.spark.rdd.RDD

/**
 * Tokenizes tweets sorted by period into individual containing topics per period.
 * filtering the non-alphabetic characters, URLs, and short words.
 */
object Tokenizer extends PipelineComponent[(Long, String), (Long,String)] {

  val MatchURL = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]"
  val MatchShortWords = "\\b\\w{1,2}\\b"
  val MatchNonAlphanumericSpace = "[^a-zA-Z0-9\\s]"

  /**
   * Processes the following input to the following output.
   * (period : Long, tweet: String) => (period: Long, topic: String)
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[(Long, String)]): RDD[(Long,String)] = {
    input.flatMap {
      case (period, tweet) =>
        for (token <- tokenize(tweet)) yield {
        (period, token)
      }
    }
  }

  /**
   * Clean the string and split into tokens.
   * @param rawString The raw input.
   * @return A list of tokens.
   */
  private def tokenize(rawString: String): Array[String]  = {
    rawString
      .toLowerCase // Convert all topics to lower case.
      .replaceAll(MatchURL, "") // Strip all URLs.
      .replaceAll(MatchNonAlphanumericSpace, " ") // Replace all remaining non-alphanumeric characters with a space.
      .replaceAll(MatchShortWords, "") // Remove all words shorter than 3 characters.
      .trim // Trim the whitespace (possible created) at the start and end.
      .split("\\s+") // Tokenize the remaining words into topics.
  }
}
