package nl.svanwouw.trending.components

import nl.svanwouw.trending.SparkTestContext
import org.specs2.mutable.SpecificationWithJUnit

/**
 * Test the tokenizer component.
 */
class TokenizerTest extends SpecificationWithJUnit {

  "A Tokenizer" should {
    "tokenize correctly" in {
      val input = SparkTestContext.sc.parallelize(List((1L,"Person X, 21, jumped from her moving car and hid in a tree. Meet her here: http://bit.ly/eeQ0gK")))
      val expected = List("person", "jumped", "from", "her", "moving", "car", "and", "hid", "tree", "meet", "her", "here")
      lazy val output = {
        Tokenizer.process(input).collect()
      }
      output must not(throwA[Exception])
      output.length mustEqual expected.size

      // Check that the tokens returned equal the expected.
      val topics = output.map {
        case(period, topic) =>
          period mustEqual 1
          topic
      }.toList
      topics mustEqual expected

    }

  }

}
