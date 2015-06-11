package nl.svanwouw.trending.components

import nl.svanwouw.trending.SparkTestContext
import nl.svanwouw.trending.types.{Frequency, Topic, Period}
import org.specs2.mutable.SpecificationWithJUnit

/**
 * Test the frequency counter component.
 */
class FrequencyCounterTest extends SpecificationWithJUnit {

  "A FrequencyCounter" should {
    "count frequencies correctly" in {
      val p1 = new Period(1L)
      val p2 = new Period(2L)
      val mad = new Topic("mad")
      val happy = new Topic("happy")
      val bored = new Topic("bored")
      val input = SparkTestContext.sc.parallelize(List((p1, mad), (p1, mad), (p1, happy), (p2, bored)))
      val expected = Set(
        ((p1, mad), new Frequency(2)),
        ((p1, happy), new Frequency(1)),
        ((p2, bored), new Frequency(1)))
      lazy val output = {
        FrequencyCounter.process(input).collect()
      }
      output must not(throwA[Exception])
      output.toSet mustEqual expected
    }

  }

}
