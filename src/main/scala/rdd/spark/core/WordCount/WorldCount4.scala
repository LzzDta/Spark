package rdd.spark.core.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorldCount4 {
  def main(args: Array[String]): Unit = {
    var conf3: SparkConf = new SparkConf().setMaster("local").setAppName("WorldCount3")
    val sc = new SparkContext(conf3)
    wordcount3(sc)

  }

  def wordcount1(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))

    val groupWord: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    val wordCount: RDD[(String, Int)] = groupWord.mapValues(iter => iter.size)

    wordCount.collect().foreach(println)
  }
  def wordcount2(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Flink Scala", "Kafka Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))

    val wordOne: RDD[(String, Int)] = words.map((_, 1))

    val groupRDD: RDD[(String, Iterable[Int])] = wordOne.groupByKey()

    val wordCount: RDD[(String, Int)] = groupRDD.mapValues(iter => iter.size)

    wordCount.collect().foreach(println)
  }
  def wordcount3(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Flink Scala", "Kafka Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))

    val wordOne: RDD[(String, Int)] = words.map((_, 1))

    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)


    wordCount.collect().foreach(println)
  }
}
