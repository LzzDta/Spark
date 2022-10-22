package rdd.spark.core.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorldCount2 {
  def main(args: Array[String]): Unit = {
    //进行基本的设置
    val conf = new SparkConf().setMaster("local").setAppName("WordCount2")
    //进行上下连接
    val sparkContext = new SparkContext(conf)

    val lines: RDD[String] = sparkContext.textFile("dataWordCount")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = words.map((_, 1))

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    resultRDD.collect().foreach(println)
  }
}
