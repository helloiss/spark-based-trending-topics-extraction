package nl.svanwouw.trending

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Test context which sets the spark context to be available for every test.
 */
object SparkTestContext {
  val AppName = "SparkTestJob"
  val Master = "local"
  val sc = new SparkContext(new SparkConf().setAppName(AppName).setMaster(Master))
}

