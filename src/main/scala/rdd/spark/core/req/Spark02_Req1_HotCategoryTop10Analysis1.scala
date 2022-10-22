package rdd.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io


object Spark02_Req1_HotCategoryTop10Analysis1 {
  def main(args: Array[String]): Unit = {
    //TODO : Top10热门品类
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WorCount")
    val sc = new SparkContext(sparkConf)
     //1.读取原始日志数据
     val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
     // Q:actionRDD重复使用
     actionRDD.cache()

    //Q : 存在大量的shuffle操作（reduceByKey）
    //reduceByKey 聚合算子，Spark会提供优化，缓存

    //2.将数据转换结构
    //  点击的场合：（品类ID，（1，0，0））
    //  下单的场合：（品类ID，（0，1，0））
    //  支付的场合：（品类ID，（0，0，1））
    actionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {

          List(datas(6), (1, 0, 0))
        } else if (datas(8) != "null") {

          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {

          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    //3.将相同的品类ID的数据进行分组聚合
    //  （品类ID，（点击数量，下单数量，支付数量））

    //4.将统计结果根据数量进行降序处理，取前十名
//    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)


    //5.将结果采集到控制台打印出来
//    resultRDD.foreach(println)
  }
}
