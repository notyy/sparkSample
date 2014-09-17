package sample

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait OrderAnalyzer extends Serializable{

  def analyzeOrderCount(sc: SparkContext, inputPath: String, outputPath: String): Unit = {
    doAnalyzeOrderCount(sc.textFile(inputPath)).saveAsTextFile(outputPath)
  }

  def echo(src: RDD[String]): RDD[String] = src

  private def nameIs(name: String)(line: String): Boolean = line.split(",")(1) == name

  def doAnalyzeOrderCount(src: RDD[String]): RDD[String] = {
    src.groupBy(_.split(",")(0)).map {
      case (k, vs) => s"$k,${vs.size},${vs.count(nameIs("xx"))},${vs.count(nameIs("yy"))}"
    }
  }
}

object OrderAnalyzerApp extends SparkAppBootstrap with SparkAppConfProvider with OrderAnalyzer {
  override def appName: String = "OrderAnalyzer"

  override def doMain(sc: SparkContext, args: Array[String]): Unit = {
    if (args.size < 2) {
      println("not enough parameter")
      println("inputPath and outputPath required")
    } else {
      analyzeOrderCount(sc, args(0), args(1))
    }
  }
}