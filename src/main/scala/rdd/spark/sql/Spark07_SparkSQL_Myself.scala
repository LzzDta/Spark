package rdd.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
/**
 * 写SQL（需要SQL和DSL）将以上数据变化为：

2019-03-04  2019-10-09

2019-10-09  2020-02-03

2020-02-03  2020-04-05

2020-04-05  2020-06-11

2020-06-11  2020-08-04

2020-08-04  2020-08-04
 */
object Spark07_SparkSQL_Myself {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("datas/Date.json")
    df.createOrReplaceTempView("date")
    val df1: DataFrame = df.sqlContext.sql("select startdate from date")

    val df2: DataFrame = df.sqlContext.sql("select enddate from date")

    val ds: Dataset[Row] = df1.union(df2).orderBy("startdate")

    ds.createOrReplaceTempView("date1")

    ds.sqlContext.sql("select startdate as d1,nvl(lead(startdate)over(order by startdate),startdate) as d2 from date1").show()
  }
}