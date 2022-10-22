package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark18_RDD_Operator_Transform_aggregateByKey2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO  算子  -Key-Value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4),("b",5),("a",6)
    ), 2)

    //TODO 测试代码
    if (1==1){
      rdd.groupByKey().collect().foreach(println(_))
      println("****************************")
    }
    /**
     * 求平均值
     */
    val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    newRDD.collect().foreach(println(_))
    println("****************************")
    val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cnt) => num / cnt
    }
    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
