package nl.svanwouw.trending.components

import org.apache.spark.rdd.RDD

/**
 * Describes the method signature of a pipeline component.
 */
trait PipelineComponent[T, U] extends Serializable {

  /**
   * Execute the processing component in the pipeline on an input RDD.
   * @param input The RDD to process.
   * @return A transformation of the input.
   */
  def process(input: RDD[T]): RDD[U]
}
