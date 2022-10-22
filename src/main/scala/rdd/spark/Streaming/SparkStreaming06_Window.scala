package rdd.spark.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_Window {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext 创建时需要传递两个参数
    //第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    val linens: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        //窗口的范围应该是采集周期的整数倍
    //但是这样的话可能会出现重复数据的计算，为了避免这种情况，可以改变滑动幅度(第二个参数)
//    val wordToOne: DStream[(String, Int)] = linens.map((_, 1))
//    val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6),Seconds(6))
    val windowDS: DStream[String] = linens.window(Seconds(6), Seconds(6))
    val wordToOne: DStream[(String, Int)] = windowDS.map((_, 1))


    val resultRDD: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    resultRDD.print()

    ssc.start()

    //等待采集器得关闭
    ssc.awaitTermination()
  }
}
