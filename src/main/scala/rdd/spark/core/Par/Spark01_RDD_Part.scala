package rdd.spark.core.Par

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "xxxxxxxxxx"),
      ("cba", "xxxxxxxxxx"),
      ("wnba", "xxxxxxxxxx"),
      ("nba", "xxxxxxxxxx")
    ), 3)

    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    sc.stop()
  }

  /**
   * 自定义分区器
   * 1.继承Partitioner
   * 2.重写方法
   */
  class MyPartitioner extends Partitioner{
    //分区数量
    override def numPartitions: Int = 3

    //根据数据的Key值返回数据所在的分区的索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }
}
