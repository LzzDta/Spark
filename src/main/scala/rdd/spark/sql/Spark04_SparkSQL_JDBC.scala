package rdd.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //读取MySQL的数据
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://slave02:3306/hue")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "12345678")
      .option("dbtable", "oozie_fs")
      .load().show

    //TODO 关闭环境

    spark.close()
  }
}
