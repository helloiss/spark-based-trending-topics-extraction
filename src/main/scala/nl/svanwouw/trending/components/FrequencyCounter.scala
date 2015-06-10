package nl.svanwouw.trending.components

import org.apache.spark.rdd.RDD

/**
 * Counts the frequencies of the topics.
 */
object FrequencyCounter extends PipelineComponent[(Long, String), ((Long,String), Int)] {


  /**
   * Processes the following input to the following output.
   * (period : Long, topic: String) => ((period: Long, topic: String), frequency: Int)
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[(Long,String)]): RDD[((Long,String), Int)] = {
    input.map(x => (x,1)).reduceByKey(_+_)
  }
}
