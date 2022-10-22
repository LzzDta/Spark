package rdd.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_countByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

//    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 1, 4))
    val rdd = sc.makeRDD(List(
            ("a",1),("a",1),("a",3),("a",999)
        ))

    //TODO - 行动算子
    val intToLong: collection.Map[(String, Int), Long] = rdd.countByValue()
    val intToLong1: collection.Map[String, Long] = rdd.countByKey()


    println(intToLong)
    println("==============")
    println(intToLong1)
    sc.stop()
  }
}
