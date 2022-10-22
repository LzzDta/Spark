package rdd.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark05_SparkSQL_HIVE {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    //使用SparkSQL连接外置的Hive
    // 1. 拷贝Hive-site.xml文件到classpath下
    // 2. 启动HIve的支持
    // 3.增加对应的依赖关系（包含Mysql驱动）
    spark.sql("show databases").show()
    spark.sql("select * from ods.ods_trade_product_info limit 10").show()
    
    spark.table("ods.ods_trade_product_info").show()
    //TODO 关闭环境

    spark.close()
  }
}
