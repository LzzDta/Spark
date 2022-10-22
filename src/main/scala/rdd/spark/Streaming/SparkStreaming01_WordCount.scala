package rdd.spark.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext 创建时需要传递两个参数
    //第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //TODO 逻辑处理
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordToOne: DStream[(String, Int)] = words.map((_, 1))

    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    //犹豫SparkStreaming采集器是长期执行得任务，所以不能直接关闭
    //如果main方法执行完毕，应用程序也会自动结束。所以不能让main方法执行完
//    ssc.stop()
    wordToCount.print()
    //TODO 启动采集器
    ssc.start()
    //等待采集器得关闭
    ssc.awaitTermination()
  }
}
