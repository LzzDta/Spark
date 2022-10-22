package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark03_RDD_Operator_Transform_mapPartitionsWithIndex1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //[1,2],[3,4]
    val mpiRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )
    mpiRDD.collect().foreach(println)
    sc.stop()
  }
}
