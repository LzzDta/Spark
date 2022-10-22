package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark13_RDD_Operator_Transform_sortBy {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -sortBy
    val rdd: RDD[Int] = sc.makeRDD(List(3, 2, 4, 6, 5, 1),2)

    //sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式..false就是倒叙
    //sortBy默认情况下，不会改变分区，但是中间存在shuffle操作    先排序，再分区
    val sortRDD: RDD[Int] = rdd.sortBy(num => num,false)

    sortRDD.saveAsTextFile("output")
    sc.stop()
  }
}
