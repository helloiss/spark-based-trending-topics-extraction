package nl.svanwouw.trending.components

import java.util.Date

import nl.svanwouw.trending.types.{Period, Tweet}
import nl.svanwouw.trending.util.SerializableDateFormat
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import twitter4j.{TwitterException, TwitterObjectFactory}

/**
 * Extracts tweets text (topics) from Twitter Status objects that are provided in JSON.
 */
class TweetExtractor(val periodSize: Int) extends PipelineComponent[String, (Period,Tweet)] {





  /**
   * Processes the input RDD of raw json / faulty strings.
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[String]): RDD[(Period,Tweet)] = {
    input.flatMap(line => extractTweet(line, periodSize))
  }

  /**
   * Extract the 140 char long tweet contents from the status.
   * @param rawJson The raw json to parse.
   * @return The tweet contents.
   */
  def extractTweet(rawJson: String, periodSize: Int) : Option[(Period, Tweet)] = {
    implicit val formats = new Formats {
      override def dateFormat: DateFormat = new SerializableDateFormat("E, dd MMM yyyy HH:mm:ss")
    }
    // Try matching using the Twitter4j library, otherwise match on hard json content.
    try {
      val status = TwitterObjectFactory.createStatus(rawJson)
      Some((new Period(Math.ceil(status.getCreatedAt.getTime/periodSize).toLong), new Tweet(status.getText)))
    } catch {
      case e : TwitterException => // TODO Clean this part.
        try {
          val tweet = parse(rawJson) findField {
            case JField("twitter", _) => true
            case _ => false
          }
          if (tweet.isDefined) {
            val c = tweet.get._2 findField {
              case JField("created_at", JString(_)) => true
              case _ => false
            }
            val t = tweet.get._2 findField {
              case JField("text", _) => true
              case _ => false
            }
            if (c.isDefined && t.isDefined) Some((new Period(Math.ceil(c.get._2.extract[Date].getTime/periodSize).toLong), new Tweet(t.get._2.extract[String]))) else None
          } else {
            None
          }
        } catch {
          case e: Exception =>
            None
        }
    }

  }
}



/**
 * Helper for extracting per day.
 */
object TweetExtractor extends TweetExtractor(periodSize = 86400) {
}
