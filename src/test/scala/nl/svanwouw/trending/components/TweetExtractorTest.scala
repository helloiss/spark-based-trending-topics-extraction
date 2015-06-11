package nl.svanwouw.trending.components

import nl.svanwouw.trending.SparkTestContext
import org.specs2.mutable.SpecificationWithJUnit

class TweetExtractorTest extends SpecificationWithJUnit {


  "A TweetExtractor" should {
    "extract tweets correctly" in {

      val classLoader = getClass.getClassLoader
      val file = classLoader.getResource("test-tweets.json")
      val input = SparkTestContext.sc.textFile(file.getPath)
      lazy val output = {
        TweetExtractor.process(input).collect()
      }
      output must not(throwA[Exception])
      output.length mustEqual 10
    }

    "handle lines not containing a tweet gracefully" in {
      val input = SparkTestContext.sc.parallelize(List("this is not a valid status", "this neither"))
      lazy val output = {
        TweetExtractor.process(input).collect()
      }
      output must not(throwA[Exception])
      output.length mustEqual 0

    }

  }

}



