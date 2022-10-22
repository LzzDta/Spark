package rdd.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorCount")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //获取系统累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    rdd.foreach(
      num => {
        sumAcc.add(num)
      }
    )
    //获取累加器的值
    println(sumAcc.value)
  }
}
