package rdd.spark.core.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorldCountPractice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WorldCountP")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile("dataWordCount")

    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))

    val rdd3: RDD[(String, Int)] = rdd2.map((_, 1))

    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_ + _)

    rdd4.collect().foreach(println)

  }
}
