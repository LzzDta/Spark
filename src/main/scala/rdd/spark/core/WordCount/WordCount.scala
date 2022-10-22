package rdd.spark.core.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("rdd/spark/core/WordCount")
    val sc = new SparkContext(sparConf)

    //1.读取文件，获取一行一行的数据
    val lines: RDD[String] = sc.textFile("datas")
    //2.将一行数据进行拆分，形成一个一个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //”hello world“ => hello,world
    //3.将数据根据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    //4.对分组后的数据进行转换
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    //（hello,hello,hello）,(world,world)
    // (hello,3),(world,2)
    //5.将转换结果采集到控制台打印出来
    var array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println(_))
    sc.stop()
  }
}
