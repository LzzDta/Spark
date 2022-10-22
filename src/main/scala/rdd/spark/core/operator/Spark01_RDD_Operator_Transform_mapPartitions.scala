package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark01_RDD_Operator_Transform_mapPartitions {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -map
    //mapPartitions : 可以以分区为单位进行数据转换操作
    //                但是会将整个分区的数据加载到内存中进行引用
    //                处理完的数据是不会被释放掉，存在对象的引用
    //                在内存较小，数据量较大的场合下，容易出现内存溢出
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2: RDD[Int] = rdd.mapPartitions({
      iter => {
        println(">>>>>>>")
        iter.map(_ * 2)
      }
    }
    )
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
