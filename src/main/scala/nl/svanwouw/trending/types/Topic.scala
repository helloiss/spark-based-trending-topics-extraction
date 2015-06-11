package nl.svanwouw.trending.types

/**
 * Syntactic sugar for a topic string.
 * Represents a possible trending topic on Twitter.
 * @param v The value with run time value type.
 */
class Topic(val v: String) extends AnyVal with Serializable {
  override def toString = v.toString
}

