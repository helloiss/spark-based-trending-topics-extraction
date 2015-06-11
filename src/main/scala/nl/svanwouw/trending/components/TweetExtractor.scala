package nl.svanwouw.trending.components

import nl.svanwouw.trending.types.{Tweet, Period}
import org.apache.spark.rdd.RDD
import twitter4j.TwitterException
import twitter4j.json.DataObjectFactory

/**
 * Extracts tweets text (topics) from Twitter Status objects that are provided in JSON.
 */
class TweetExtractor(periodSize: Int) extends PipelineComponent[String, (Period,Tweet)] {

  var _periodSize: Int = periodSize



  /**
   * Processes the input RDD of raw json / faulty strings.
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[String]): RDD[(Period,Tweet)] = {
    input.flatMap(line => extractTweet(line, _periodSize))
  }

  /**
   * Extract the 140 char long tweet contents from the status.
   * @param rawJson The raw json to parse.
   * @return The tweet contents.
   */
  private def extractTweet(rawJson: String, periodSize: Int) : Option[(Period, Tweet)] = {
    try {
      val status = DataObjectFactory.createStatus(rawJson)
      Some((new Period(Math.ceil(status.getCreatedAt.getTime/periodSize).toLong), new Tweet(status.getText)))
    } catch {
      case e : TwitterException =>
        // TODO: Add support for other input file format.
        None
    }

  }
}

/**
 * Helper for extracting per day.
 */
object TweetExtractor extends TweetExtractor(periodSize = 86400) {
}
