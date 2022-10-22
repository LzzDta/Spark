package rdd.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_reduce {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO - 行动算子
//    val i: Int = rdd.reduce(_ + _)
    // collect : 方法会将不同分区的数据按照分区的顺序采集到Driver端内存中，形成数组
//    val ints: Array[Int] = rdd.collect()
//    println(ints.mkString(","))

    // count : 数据源中数据的个数
    val cnt = rdd.count()
    println(cnt)

    //first : 获取数据源中数据的第一个
    val first: Int = rdd.first()
    println(first)

    //take : 获取前N个数据
    val ints: Array[Int] = rdd.take(3)
    println(ints)
    sc.stop()
  }
}
