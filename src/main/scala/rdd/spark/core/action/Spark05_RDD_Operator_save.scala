package rdd.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_save {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
            ("a",1),("a",1),("a",3)
        ))

    //TODO - 行动算子
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    //saveAsSequenceFile 方法要求数据的格式必须为K-V类型
    rdd.saveAsSequenceFile("output2")
    sc.stop()
  }
}
