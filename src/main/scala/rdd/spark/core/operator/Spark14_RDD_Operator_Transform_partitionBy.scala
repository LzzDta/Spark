package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object Spark14_RDD_Operator_Transform_partitionBy {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -Key-Value类型
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    //RDD => PairRDDFunctions
    // 隐式转换(二次编译 )

    //partitionBy根据指定的分区规则对数据进行重分区
    //spark的默认分区器是HashPartitioner
    mapRDD.partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output")
    sc.stop()
  }
}
