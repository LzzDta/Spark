package rdd.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark06_SparkSQL_Practice {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use aitguigu")

    //TODO 准备数据

    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        | `date` string,
        | `user_id` bigint,
        | `session_id` string,
        | `page_id` bigint,
        | `action_time` string,
        | `search_keyword` string,
        | `click_category_id` bigint,
        | `click_product_id` bigint,
        | `order_category_ids` string,
        | `order_product_ids` string,
        | `pay_category_ids` string,
        | `pay_product_ids` string,
        | `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql("load data local inpath 'datas1/user_visit_action.txt' into table user_visit_action")

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        | `product_id` bigint,
        | `product_name` string,
        | `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql("load data local inpath 'datas1/product_info.txt' into table product_info")

    spark.sql(
      """
        |CREATE TABLE `city_info`(
        | `city_id` bigint,
        | `city_name` string,
        | `area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)


    spark.sql("load data local inpath 'datas1/city_info.txt' into table city_info")

    spark.sql("select * from city_info").show()
    //TODO 关闭环境

    spark.close()
  }
}
