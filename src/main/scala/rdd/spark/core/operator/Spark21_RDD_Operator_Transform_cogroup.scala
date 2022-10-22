package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark21_RDD_Operator_Transform_cogroup {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  cogroup
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3)
    ))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("b", 4), ("b", 5), ("a", 6)))
    val cpRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(rdd1)
    cpRDD.collect().foreach(println)
    sc.stop()
  }
}
