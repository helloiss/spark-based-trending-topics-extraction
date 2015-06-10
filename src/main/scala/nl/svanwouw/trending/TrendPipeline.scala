package nl.svanwouw.trending

import nl.svanwouw.trending.components.{FrequencyCounter, Tokenizer, TweetExtractor}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * A pipeline that can be executed through the commandline or re-used in another module.
 */
object TrendPipeline {

  val AppName = "SparkBasedTrending"

  implicit class PipelineOperation[A](val value: A) {
    def |>[B] (f: A => B) = f(value)
  }

  /**
   * Execute the pipeline
   * @param master Master config to use for Spark.
   * @param inputFile The input file location.
   * @param outputDir The output dir location.
   * @param periodSize The number of seconds per period (3600 for an hour, 86400 for a day).
   */
  def execute(master: String, inputFile: String, outputDir: String, periodSize: Int) {
    val sc = {
      val conf = new SparkConf().setAppName(AppName).setMaster(master)
      new SparkContext(conf)
    }

    val input = sc.textFile(inputFile)

    // Chain the pipeline components using our own defined implicit class "pipeline operator".
    val output = input |>
      new TweetExtractor(periodSize).process |>
      Tokenizer.process |>
      FrequencyCounter.process
    output.saveAsTextFile(outputDir)
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: Main <host> <input_file> <output_dir> <period in secs>")
      System.exit(1)
    }
    execute(args(0), args(1), args(2), args(3).toInt)
  }

}
