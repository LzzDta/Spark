package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark06_RDD_Operator_Transform_groupBy {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -groupBy
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
val rdd: RDD[String] = sc.makeRDD(List("Hello", "Spark", "Hadoop", "Scala"),2)

    //groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
//    def groupFunction(num:Int):Int = {
//      num%2
//    }
    /**
     * 分区和分组没有必然的关系
     */
    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))
//    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
