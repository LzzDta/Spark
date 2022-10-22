package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark20_RDD_Operator_join {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -Key-Value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3)
    ))

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("b", 4), ("b", 5), ("a", 6)))
    //join: 两个不同数据源的数据，相同的key的value会连接在一起，形成元组
    //      如果两个数据源中的Key没有匹配上，那么数据不会出现在结果中
    //      如果两个数据源中的key有多个相同，那就会导致数据呈几何增长，加大内存负担
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd)
    joinRDD.collect().foreach(println)
    sc.stop()
  }
}
