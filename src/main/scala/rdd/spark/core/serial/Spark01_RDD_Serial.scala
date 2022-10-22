package rdd.spark.core.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val ser = new Search("h")
    val rdd1: RDD[String] = ser.getMatch1(rdd)

    rdd1.collect().foreach(println)
    sc.stop()
  }
}
// 查询对象
// 类的构造参数其实是类的属性，构造参数需要进行闭包检测，其实就等于类进行闭包检测
class Search(query:String) extends Serializable {
  def isMatch(s: String): Boolean = {
    s.contains(this.query)     //前面相当于加了this
  }
  // 函数序列化案例
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    //rdd.filter(this.isMatch)
    rdd.filter(isMatch)
  }
  // 属性序列化案例
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    //rdd.filter(x => x.contains(this.query))
    rdd.filter(x => x.contains(query))
    //val q = query
    //rdd.filter(x => x.contains(q))
  }
}

