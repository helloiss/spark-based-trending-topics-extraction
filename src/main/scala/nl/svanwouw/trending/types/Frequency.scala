package nl.svanwouw.trending.types

/**
 * Syntactic sugar for a frequency integer.
 * Represents topic frequency.
 * @param v The value with run time value type.
 */
class Frequency(val v: Int) extends AnyVal with Serializable {
  def +(o: Frequency): Frequency = new Frequency(v + o.v)
  override def toString = v.toString

}

