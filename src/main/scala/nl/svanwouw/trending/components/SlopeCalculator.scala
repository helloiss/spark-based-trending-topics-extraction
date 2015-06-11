package nl.svanwouw.trending.components

import nl.svanwouw.trending.types.{Slope, Period, Frequency, Topic}
import org.apache.spark.rdd.RDD

/**
 * Calculates the slopes of the topics per period.
 */
object SlopeCalculator extends PipelineComponent[((Period,Topic), Frequency), ((Period,Topic), Slope)] {


  /**
   * Custom pipeline operator.
   */
  implicit class PipelineOperation[A](val value: A) {
    def |>[B] (f: A => B) = f(value)
  }

  /**
   * Transforms the frequency into derived slope per period.
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[((Period,Topic), Frequency)]): RDD[((Period,Topic), Slope)] = {
    input |> groupByTopic |> calculateSlopes
  }

  /**
   * Map to a list of ordered (period,frequency) tuples per topic.
   * Note that run time might increase significantly with adding more periods this way,
   * but premature optimization is not applied.
   * @param input RDD of ((period,topic),frequency).
   * @return RDD in form of (topic, List[(period,frequency)).
   */
  def groupByTopic(input: RDD[((Period, Topic), Frequency)]): RDD[(Topic, List[(Period,Frequency)])] = {
    input.map {
      case ((period, topic), frequency) => (topic, List((period, frequency)))
    }.reduceByKey(_ ++ _)
  }

  /**
   * Calculate the slopes per topic.
   * @param input RDD containing (period,frequency) grouped by topic.
   * @return RDD with the frequencies replaced by calculated slopes.
   */
  def calculateSlopes(input: RDD[(Topic, List[(Period,Frequency)])]): RDD[((Period, Topic), Slope)] = {
    input.flatMap {
    case (topic, pfList) =>

      // Sort the list based on period.
      val sortList = pfList.sortBy(_._1.v)

      // Create list of adjacent (period, frequency) pairs.
      val fPairs = sortList zip sortList.tail

      // Calculate slopes for each period.
      val sPairs = fPairs.map {
        case ((x1, y1), (x2, y2)) =>
          (x1, new Slope(((y2.v - y1.v).toDouble / (x2.v - x1.v).toDouble).toInt))
      }

      // Convert back to the original key/val grouping => ((period,topic), slope)
      for ((period,slope) <- sPairs) yield {
        ((period,topic), slope)
      }
    }
  }

}
