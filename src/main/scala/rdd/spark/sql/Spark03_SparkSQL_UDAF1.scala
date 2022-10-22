package rdd.spark.sql

import org.apache.spark.{Aggregator, SparkConf}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Spark03_SparkSQL_UDAF1 {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    //spark.udf.register("ageAvg",new MyAvgUDAF())

    //spark.sql("select ageAvg(age) from user").show()

    //TODO 执行逻辑操作

    //TODO 关闭环境

    spark.close()
  }

  /**
   * 自定义聚合函数类： 计算年龄的平均值
   * 1.继承Aggregator
   *    IN : 输入的数据类型 Long
   *    BUF : 缓冲区的数据类型
   *    OUT : 输出的数据类型 Long
   * 2.重写方法
   */
  case class Buff(var total : Long,var count : Long)
//  class MyAvgUDAF extends Aggregator[Long,Buff,Long]{   //版本过低？？？？
//            尚硅谷  P 171
//  }
}
