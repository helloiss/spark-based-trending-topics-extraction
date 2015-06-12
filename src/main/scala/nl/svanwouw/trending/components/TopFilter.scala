package nl.svanwouw.trending.components

import nl.svanwouw.trending.types.{Period, Slope, Topic}
import org.apache.spark.rdd.RDD

/**
 * Calculates the top N topics.
 */
class TopFilter(val topN: Int) extends PipelineComponent[((Period,Topic), Slope), (Period,List[Topic])] {




  /**
   * Calculate the top N elements.
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[((Period,Topic), Slope)]): RDD[(Period, List[Topic])] = {
    input.map {
      case ((period, topic), slope) => (period, List((slope, topic)))
    }.reduceByKey(_ ++ _).flatMap {
      case ((period, slopeList)) =>
        // Take n sorted by slope, descending.
        Some((period, slopeList.sortBy(-_._1.v).take(topN).map(_._2)))
    }
  }


}
