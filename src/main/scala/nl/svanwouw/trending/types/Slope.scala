package nl.svanwouw.trending.types

/**
 * Syntactic sugar for a slope integer.
 * Represents the slope of the frequency of occurence of a certain topic in a certain period.
 * @param v The value with run time value type.
 */
class Slope(val v: Double) extends AnyVal with Serializable {
  override def toString = v.toString
}

