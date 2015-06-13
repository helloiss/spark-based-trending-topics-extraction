package nl.svanwouw.trending

import nl.svanwouw.trending.components._
import nl.svanwouw.trending.types.{Slope, Frequency, Period, Topic}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * A pipeline that can be executed through the commandline or re-used in another module.
 */
object TrendingTopicPipeline {

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
  def execute(master: String, inputFile: String, outputDir: String, periodSize: Int, topN: Int) {
    val sc = {
      val conf = new SparkConf().setAppName(AppName).setMaster(master)
      new SparkContext(conf)
    }

    val input = sc.textFile(inputFile)

    // Chain the pipeline components using our own defined implicit class "pipeline operator".
 /**   val output = input |>
      new TweetExtractor(periodSize).process |>
      Tokenizer.process |>
      FrequencyCounter.process |>
      SlopeCalculator.process |>
      new TopFilter(topN).process
   **/

    val output = input |> extractTopics |> sumFrequencies |> calcSlopes |> calcTopN
    output.saveAsTextFile(outputDir)
  }


  def extractTopics(input: RDD[String]) : RDD[((Topic, Period) ,Frequency)] = {
    input.flatMap(line => TweetExtractor.extractTweet(line, 86400)).flatMap {
      case (period, tweet) =>
        for (token <- Tokenizer.tokenize(tweet)) yield {
          ((token, period), new Frequency(1))
        }
    }
  }

  def sumFrequencies(input: RDD[((Topic, Period), Frequency)]) : RDD[(Topic, List[(Period, Frequency)])]  = {
      input.reduceByKey((x, y) => new Frequency(x.v +y.v)).flatMap {
      case ((topic, period), frequency) => Some((topic, List((period, frequency))))
    }.reduceByKey(_ ++ _)
  }

  def calcSlopes(input: RDD[(Topic, List[(Period, Frequency)])]) : RDD[(Period, (Topic, Slope))] = {
    input.flatMap {
      case (topic, pfList) if pfList.size == 1 => Some((pfList.head._1, (topic, new Slope(0))))
      case (topic, pfList) =>

        // Sort the list based on period.
        val sortList = pfList.sortBy(_._1.v)


        // This is intended because the slope is undefined for such a list.
        val fPairs = sortList zip sortList.tail


        // Calculate slopes for each period.
        val sPairs = fPairs.map {
          case ((x1, y1), (x2, y2)) =>
            (x1, new Slope((y2.v - y1.v).toDouble / (x2.v - x1.v).toDouble))
        }

        // Convert back to the original key/val grouping => ((period,topic), slope)
        for ((period,slope) <- sPairs) yield {
          (period,(topic, slope))
        }

    }
  }

  def calcTopN(input: RDD[(Period, (Topic, Slope))]) : RDD[(Period, List[Topic])] = {
    input.flatMap {
      case (period, (topic, slope)) => Some((period, List((topic, slope))))
    }.reduceByKey(mergeTop).flatMap {
      case ((period, list)) => Some((period, list.map(_._1)))
    }

  }

  private def mergeTop(l: List[(Topic, Slope)], r: List[(Topic, Slope)]): List[(Topic, Slope)] = {

    (l ++ r).groupBy(_._1).map {
      case (topic,tuples) => (topic, new Slope(tuples.map(_._2.v).fold(Double.MinValue) { (a, b) => if (a > b) a else b}))
    }.toList.sortBy(-_._2.v).take(5)

  }






  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: Main <host> <input_file> <output_dir> <period in secs> <top n>")
      System.exit(1)
    }
    execute(args(0), args(1), args(2), args(3).toInt, args(4).toInt)
  }

}
