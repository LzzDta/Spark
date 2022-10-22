package rdd.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DaftPaper {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO 案例实操
    //1.获取原始数据： 时间戳、省份、城市、用户、广告
    val rdd: RDD[String] = sc.textFile("datas/agent.log")

    //2.将原始数据进行结构的转换，方便统计
    //  时间戳，省份，城市，用户，广告
    //  =>( ( 省份，广告 ）,1)
    val rdd1: RDD[((String, String), Int)] = rdd.map(
      lines => {
        val datas: Array[String] = lines.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )
    //3.将转换结构后的数据，进行分组聚合
    //  ( (省份，广告 ），1 ） => (( 省份，广告 ），sum )
    val rdd2: RDD[((String, String), Int)] = rdd1.reduceByKey(_ + _)

    //4.将聚合后的结果进行结构转换
    // (( 省份，广告 ），sum ) => (省份，(广告，sum) )
    val rdd3: RDD[(String, (String, Int))] = rdd2.map {
      case ((w1, w2), cnt) => (w1, (w2, cnt))
    }

    //5.将转换结构后的数据根据省份进行分组
    //  ( 省份，(广告A，sum) ),(广告B，sum) )
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()

    //6.将分组后的数据组内排序，根据sum排序（降序），取前3名
    val rdd5: RDD[(String, List[(String, Int)])] = rdd4.mapValues(
      iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )

    //7.采集数据打印在控制台
    rdd5.collect().foreach(println)
  }
}
