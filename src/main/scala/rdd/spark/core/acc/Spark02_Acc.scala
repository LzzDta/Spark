package rdd.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorCount")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //获取系统累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    val mapRDD: RDD[Unit] = rdd.map(
      num => {
        sumAcc.add(num)
      }
    )
    //少加: 转换算子中，调用累加器，如果没有行动算子的话不会执行
    //多加: 转换算子中调用累加器,如果有多个行动算子，那么累加器会重复执行累加
    //一般情况下，累加器会放置在行动算子中进行操作
    println(sumAcc.value)
  }
}
