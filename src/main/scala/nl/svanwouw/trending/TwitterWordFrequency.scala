package nl.svanwouw.trending

import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.{TwitterException, Status}
import twitter4j.json.DataObjectFactory

object TwitterWordFrequency {

  private val AppName = "TwitterWordFrequencyJob"

  def execute(master: String, inputLocation: String, outputLocation: String) {

    val sc = {
      val conf = new SparkConf()
        .setAppName(AppName)
        .setMaster(master)
        .set("spark.hadoop.validateOutputSpecs", "false")
      new SparkContext(conf)
    }

    // Adapted from Word Count example on http://spark-project.org/examples/
    val file = sc.textFile(inputLocation)
    val statusTexts = file.flatMap(line => extractTwitterText(line))
    val words = statusTexts.flatMap(text => tokenize(text))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    words.m()
    wordCounts.saveAsTextFile(outputLocation)
  }


  // Split a piece of text into individual words.
  private def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s\\#]", "").split("\\s+")
  }
}

