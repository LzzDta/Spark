package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark16_RDD_Operator_groupByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -Key-Value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))
    //groupByKey将数据源中的数据相同的Key分在一个组中，形成一个对偶元组
    //          元组中的第一个元素就是Key
    //          元组中的第二个元素就是相同key的value集合
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupRDD.collect().foreach(println)
    println("============================")
//    val groupRDD: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    val tes: RDD[(String, Int)] = groupRDD.map {
      case (world, list) =>
        val i: Int = list.sum
        (world, i)
    }
    tes.collect().foreach(println)
    sc.stop()
  }
}
