package nl.svanwouw.trending.components

import nl.svanwouw.trending.types.{Frequency, Topic, Period}
import org.apache.spark.rdd.RDD

/**
 * Counts the frequencies of the topics per period.
 */
object FrequencyCounter extends PipelineComponent[(Period, Topic), ((Period,Topic), Frequency)] {


  /**
   * Processes the frequency of each topic in a specified period.
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[(Period,Topic)]): RDD[((Period,Topic), Frequency)] = {
    input.map(x => (x, new Frequency(1))).reduceByKey(_+_)
  }
}
