package sample

import org.apache.spark.rdd.RDD

trait OlympicCalc {
  def findMostMedalsYear4USA: RDD[String] => String = rdd => {
    rdd.map(_.split(",")).filter(_(1) == "United States").
      map(a => (a(2),a(7).toInt)).reduceByKey(_ + _).
      sortBy({case (c,m) => m}, ascending = false).map(_._1).first()
  }
}
