package rdd.spark.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount2")
    val sc = new SparkContext(conf)

    val list: List[String] = List("HBase Flink", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("*******************************************")

    val rdd1: RDD[String] = sc.makeRDD(list)

    val flatRDD1: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD1: RDD[(String, Int)] = flatRDD.map((_, 1))

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD1.groupByKey()
    groupRDD.collect().foreach(println)
  }
}
