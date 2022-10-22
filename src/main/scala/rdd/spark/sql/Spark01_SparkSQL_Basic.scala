package rdd.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //TODO 执行逻辑操作

//    DataFrame
    val df: DataFrame = spark.read.json("datas/user.json")
//    df.show()

    //DataFrame => SQL
//    df.createOrReplaceTempView("user")
//
//    spark.sql("select age from user").show()
//    println("==================================")
//    spark.sql("select avg(age) from user").show()

    //DataFrame => DSL
    //在使用DataFrame时，涉及到转换操作，需要引入转换规则,这里的spark是对象名，不是包名

    //df.select("age","username").show()
    //df.select($"age"+1).show()

    // TODO DataSet
    // DataFrame其实是特定泛型的DataSet
    //val seq = Seq(1,2,3,4)
    //val ds: Dataset[Int] = seq.toDS()
    //ds.select("value").show()

    //RDD <=> DataFrame
//    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
//    val df: DataFrame = rdd.toDF("id", "name", "age")
//    val rowRDD: RDD[Row] = df.rdd
//    //DataFrame <=> DataSet
//
//    val ds: Dataset[student] = df.as[student]
//    val df1: DataFrame = ds.toDF()
//
//    //RDD <=> DataSet
//
//    val ds1: Dataset[student] = rdd.map {
//      case (id, name, age) =>
//        student(id, name, age)
//    }.toDS()
//
//    val studentRDD: RDD[student] = ds1.rdd
//    //TODO 关闭环境

    spark.close()
  }
  case class student(id:Int,name:String,age:Int)
}
