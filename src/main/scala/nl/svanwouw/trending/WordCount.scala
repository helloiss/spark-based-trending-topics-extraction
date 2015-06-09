package nl.svanwouw.trending

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  private val AppName = "WordCountJob"

  /**
   * Run Word count.
   * @param master The spark master string.
   * @param args The other arguments.
   */
  def execute(master: String, args: List[String]) {

    val sc = {
      val conf = new SparkConf().setAppName(AppName).setMaster(master)
      new SparkContext(conf)
    }

    // Adapted from Word Count example on http://spark-project.org/examples/
    val file = sc.textFile(args(0))
    val words = file.flatMap(line => tokenize(line))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.saveAsTextFile(args(1))
  }

  // Split a piece of text into individual words.
  private def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}

