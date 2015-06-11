package nl.svanwouw.trending.types

/**
 * Syntactic sugar for a tweet string.
 * Represents a 140 character raw Twitter message.
 * @param v The value with run time value type.
 */
class Tweet(val v: String) extends AnyVal with Serializable {
  override def toString = v.toString
}

