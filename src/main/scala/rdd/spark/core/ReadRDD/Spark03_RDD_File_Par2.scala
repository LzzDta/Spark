package rdd.spark.core.ReadRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 创建RDD
    //TODO 数据分区的分配

    val rdd: RDD[String] = sc.textFile("datas/word.txt", 2)

    rdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }
}
