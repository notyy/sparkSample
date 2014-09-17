package sample

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}

trait SparkContextHelper extends LazyLogging {

  def withSparkContext[T](sparkConf: SparkConf)(f: SparkContext => T): T = {
    logger.info("starting spark context")
    val sc = new SparkContext(sparkConf)
    logger.info("spark context started")
    try {
      logger.info("calling function on spark context")
      val rs = f(sc)
      logger.info("function completed")
      rs
    }finally {
      logger.info("stopping spark context")
      sc.stop()
      logger.info("spark context stopped")
    }
  }

  def withLocalSparkContext[T](appName: String) = withSparkContext[T](
    new SparkConf().setAppName(appName).setMaster("local").set("spark.ui.port","4041")) _
}
