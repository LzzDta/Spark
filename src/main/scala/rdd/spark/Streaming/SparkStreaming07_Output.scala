package rdd.spark.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Output {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    /**
     * SparkStreaming如果没有输出操作，会报错
     */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))


    ssc.start()

    //等待采集器得关闭
    ssc.awaitTermination()
  }
}
