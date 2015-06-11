package nl.svanwouw.trending.types

/**
 * Syntactic sugar for a period integer.
 * Represents period in form of a unix timestamp divided by the period size (e.g. 3600 for hourly).
 * @param v The value with run time value type.
 */
class Period(val v: Long) extends AnyVal with Serializable {
  override def toString = v.toString
}

