package nl.svanwouw.trending.components

import java.util.Date

import org.apache.spark.rdd.RDD
import twitter4j.TwitterException
import twitter4j.json.DataObjectFactory

/**
 * Extracts tweets text (topics) from Twitter Status objects that are provided in JSON.
 */
class TweetExtractor(periodSize: Int) extends PipelineComponent[String, (Long,String)] {

  var _periodSize: Int = periodSize



  /**
   * (rawJson: String) => (period : Long, tweet: String)
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[String]): RDD[(Long,String)] = {
    input.flatMap(line => extractTweet(line, _periodSize))
  }

  /**
   * Extract the 140 char long tweet contents from the status.
   * @param rawJson The raw json to parse.
   * @return The tweet contents.
   */
  def extractTweet(rawJson: String, periodSize: Int) : Option[(Long, String)] = {
    try {
      val status = DataObjectFactory.createStatus(rawJson)
      Some((Math.ceil(status.getCreatedAt.getTime/periodSize).toLong, status.getText))
    } catch {
      case e : TwitterException =>
        None
    }
  }
}
