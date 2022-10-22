package rdd.spark.core.ReadRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //    sparkConf.set("spark.default.parallelism","5")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 创建RDD
    //RDD的并行度 & 分区
    //makeRDD方法可以传递第二个参数，这个参数表示分区的数量
    //第二个参数如果不传递，那么makeRDD方法会使用默认值：defaultParallelism
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 5)
    //默认情况下会从配置对象中获取配置参数，如果获取不到，那么使用totalCores属性，这个属性为当前环境运行可用的最大核数 setMaster("local[*]")
    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2))

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }
}
