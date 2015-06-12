package nl.svanwouw.trending.util

import java.text.SimpleDateFormat
import java.util.Date

import org.json4s.DateFormat
import org.json4s.ParserUtil.ParseException

/**
 * Make simple date format serializable for Spark.
 */
class SerializableDateFormat(pattern: String) extends DateFormat {


  private[this] def formatter = new SimpleDateFormat(pattern)

  override def parse(s: String) = try {
    Some(formatter.parse(s))
  } catch {
    case e: ParseException => None
  }

  override def format(d: Date) = formatter.format(d)

}
