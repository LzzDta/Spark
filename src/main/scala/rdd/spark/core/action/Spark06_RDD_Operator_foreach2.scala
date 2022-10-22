package rdd.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_foreach2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
            1,2,3,4
        ))
    val user = new User
    //TODO - 行动算子
    //RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    //闭包检测
    rdd.foreach(
      num => {
        println("age = " +user.age + num)
      }
    )

    sc.stop()
  }


//  class User() extends Serializable
  //样例类在编译时，会自动混入序列化特质（实现可序列化接口）
    case class User() {
    var age : Int = 32
  }
}
