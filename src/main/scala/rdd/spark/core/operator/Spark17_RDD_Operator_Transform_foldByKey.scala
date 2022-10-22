package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark17_RDD_Operator_Transform_foldByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -foldByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

    //如果聚合计算时，分区内和分区间计算规则相同,spark提供了简化的方法  foldByKey
    rdd.foldByKey(0)(_+_).collect().foreach(println)

//    val resultRDDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)
//    resultRDDD.collect().foreach(println)
    sc.stop()
  }
}
