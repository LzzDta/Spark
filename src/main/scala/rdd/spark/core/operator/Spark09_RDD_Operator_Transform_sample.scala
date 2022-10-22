package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark09_RDD_Operator_Transform_sample {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //1. 第一个参数表示，抽取数据后是否将数据返回  true(放回)  false(丢弃)
    //2. 第二个参数表示，数据源中每条数据被抽取
    //                基准值的概念
    //3. 第三个参数表示，抽取数据时随机算法的种子
                  //  如果不传递第三个参数，那么使用的是当前的系统时间
    println(rdd.sample(
      true,
      0.4,
      1
    ).collect().mkString(","))
    sc.stop()
  }
}
