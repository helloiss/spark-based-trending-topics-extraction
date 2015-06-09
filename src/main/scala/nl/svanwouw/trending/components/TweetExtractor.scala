package nl.svanwouw.trending.components

import org.apache.spark.rdd.RDD
import twitter4j.TwitterException
import twitter4j.json.DataObjectFactory

/**
 * Extracts tweets text (topics) from Twitter Status objects that are provided in JSON.
 */
object TweetExtractor extends PipelineComponent[String, String] {

  /**
   * (rawJson: String) => (period : Int, topics: String)
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[String]): RDD[String] = {
    input.flatMap(line => extractTweet(line))
  }

  /**
   * Extract the 140 char long tweet contents from the status.
   * @param rawJson The raw json to parse.
   * @return The tweet contents.
   */
  def extractTweet(rawJson: String) : Option[String] = {
    try {
      val txt = DataObjectFactory.createStatus(rawJson).getText
      Some(txt)
    } catch {
      case e : TwitterException =>
        None
    }
  }
}
