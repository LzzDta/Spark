package rdd.spark.core.ReadRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从文件中创建RDD  将文件中的数据作为处理的数据源
    val seq = Seq[Int](1, 2, 3, 4)
    //path路径默认以当前环境的根路径为基准，可以写绝对路径
    //sc.textFile("F:\\Spark\\datas\\1.txt")
    //val rdd: RDD[String] = sc.textFile("datas/1.txt")

    //path路径可以是文件的具体路径，也可以是目录名称
    val rdd: RDD[String] = sc.textFile("datas")
    rdd.collect().foreach(println)


    //TODO 关闭环境
    sc.stop()
  }
}
