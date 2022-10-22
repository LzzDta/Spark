package rdd.spark.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_Close {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    /**
     * 线程的关闭 :
     * val thread = new Thread()
     * thread.start()
     *
     * thread.stop() : 强制关闭
     */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))
    wordToOne.print()

    ssc.start()
    //如果想要关闭采集器，那么需要创建新的线程
    new Thread(
      new Runnable {
        //计算节点不再接收新的数据，而是将现有的数据处理完毕然后关闭
        //Mysql : Table =>
        override def run(): Unit =ssc.stop(true,true)
      }
    ).start()

    //block 阻塞main线程
    ssc.awaitTermination()

    //优雅的关闭
  }
}
