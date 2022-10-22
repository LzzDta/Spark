package test

import org.apache.spark.{SparkConf, SparkContext}

class Git_Test {
  def main(args: Array[String]): Unit = {
    val conf3: SparkConf = new SparkConf().setMaster("local").setAppName("dataWordCount")
    val context = new SparkContext(conf3)

    context.textFile("dataWordCount").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println(_))
    println("TestCode===============================")
  }
}
