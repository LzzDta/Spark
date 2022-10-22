package rdd.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_aggregate {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO - 行动算子
    //aggregateByKey : 初始值只会参与分区内的计算
    //aggregate: 初始值不仅参与分区内计算，也会参与分区间的计算
//    val result: Int = rdd.aggregate(10)(_ + _, _ + _) //结果是40

    //当分区内的计算和分区间的计算一样的时候  使用  fold进行简化
    val i: Int = rdd.fold(0)(_ + _)
    sc.stop()
  }
}
