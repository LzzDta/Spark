package rdd.spark.core.framework.application

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import rdd.spark.core.framework.controller.WordCountController

class WordCountApplication {

  val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount2")

  val sparkContext = new SparkContext(conf)


  val controller = new WordCountController()

  sparkContext.stop()
}
