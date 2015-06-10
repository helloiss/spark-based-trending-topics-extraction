package nl.svanwouw.trending.components

import nl.svanwouw.trending.SparkTestContext
import org.specs2.mutable.SpecificationWithJUnit

/**
 * Test the tokenizer component.
 */
class FrequencyCounterTest extends SpecificationWithJUnit {

  "A FrequencyCounter" should {
    "count frequencies correctly" in {
      val input = SparkTestContext.sc.parallelize(List((1L,"mad"), (1L, "mad"), (1L, "happy"), (2L, "bored")))
      val expected = Set(((1L,"mad"),2), ((1L, "happy"),1), ((2L, "bored"),1))
      lazy val output = {
        FrequencyCounter.process(input).collect()
      }
      output must not(throwA[Exception])
      output.toSet mustEqual expected
    }

  }

}
