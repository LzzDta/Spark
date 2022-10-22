package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object Spark15_RDD_Operator_reduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -Key-Value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))
    //相同的key的数据进行value数据的聚合操作
    //scala语言中一般的聚合操作都是两两聚合，spark基于scala开发，所以也是两两聚合

    //reduceByKey中如果key的数据只有一个，是不会参与运算的
    val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    rdd1.collect().foreach(println)
    sc.stop()
  }
}
