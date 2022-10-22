package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark03_RDD_Operator_Transform2_flatMap {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -map
//    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(5, 6), List(3, 4)))
val rdd: RDD[Any] = sc.makeRDD(List(List(3, 4), 4,List(1, 2, 3), List(9, 0)))

    val rdd1: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )
    rdd1.collect().foreach(println)
//    val rdd2: RDD[Int] = rdd.flatMap(
//      list => {
//        list
//      }
//    )
//    rdd2.collect().foreach(println)
    sc.stop()
  }
}
