package rdd.spark.core.framework.controller

import org.apache.spark.rdd.RDD
import rdd.spark.core.framework.service.WordCountService

/**
 * 持久层
 */
class WordCountController {

  private val wordCountService = new WordCountService()

  def execute() : Unit ={
//    val lines: RDD[String] = sparkContext.textFile("dataWordCount")
//    val words: RDD[String] = lines.flatMap(_.split(" "))
//
//
//    //将相同key的 tuple进行分组
//    val mapRDD: RDD[(String, Int)] = words.map((_, 1))
//    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
//
//    resultRDD.collect().foreach(println)
  }
}
