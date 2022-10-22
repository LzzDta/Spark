package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark12_RDD_Operator_Transform_repartition {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -repartition
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),2)

    //coalesce算子可以扩大分区，但是如果不进行shuffle操作是没有意义的，不起作用，
    // 所以如果想要实现扩大分区的效果，必须开启shuffle
    //spark提供了一个简化的操作
    //缩减分区:coalesce,如果想要数据均衡，使用shuffle
    //扩大分区: repartition
    //        repartition 底层就调用了 coalesce(numPartitions, shuffle = true),肯定采用shuffle
     val newRDD:RDD[Int] = rdd.repartition(3)
//    val newRDD: RDD[Int] = rdd.coalesce(3,true)

    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}
