package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date


object Spark08_RDD_Operator_Transform_filter {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -filter
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)
    filterRDD.collect().foreach(println)

    sc.stop()
  }
}
