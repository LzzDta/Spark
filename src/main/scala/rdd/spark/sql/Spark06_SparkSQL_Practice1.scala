package rdd.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark06_SparkSQL_Practice1 {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use aitguigu")

    spark.sql(
      """
        |select
        |	*
        |from (
        |	select
        |	*,
        |	rank(cnt)over(partition by area order by cnt desc) as Rank
        |	from (
        |		select area,
        |		product_name,
        |		count(*) as cnt
        |		from (
        |			select
        |			a.*,
        |			p.product_name,
        |			c.city_name,
        |			c.area
        |			from user_visit_action as a
        |			join product_info as p on a.click_product_id = p.product_id
        |			join city_info as c on a.city_id = c.city_id
        |     where a.click_product_id > -1
        |		) as t1
        |		group by area,product_name
        |	) as t2
        |) where Rank <= 3
        |""".stripMargin).show()


    //TODO 关闭环境

    spark.close()
  }
}
