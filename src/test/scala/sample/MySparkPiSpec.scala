package sample

import org.apache.spark._
import org.scalatest.FunSpec

import scala.math.random

class MySparkPiSpec extends FunSpec {
  describe("MySparkPi"){
    it("can be run locally"){
      val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[4]")
      val spark = new SparkContext(conf)
      val slices = 100
      val n = 100000 * slices
      val count = spark.parallelize(1 to n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
      println("Pi is roughly " + 4.0 * count / n)
      spark.stop()
    }
  }

}
