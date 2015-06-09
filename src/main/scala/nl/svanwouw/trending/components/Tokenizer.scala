package nl.svanwouw.trending.components

import org.apache.spark.rdd.RDD

/**
 * Tokenizes tweets sorted by period into individual containing topics per period.
 * filtering the non
 */
object Tokenizer extends PipelineComponent[String, String] {
  /**
   * Processes the following input to the following output.
   * (period : Long, tweet: String) => ((period: Long, topic: String), 1)
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  override def process(input: RDD[String]): RDD[String] = {
    input
  }
}
