package rdd.spark.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object SparkStreaming04_stage {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext 创建时需要传递两个参数
    //第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    ssc.checkpoint("cp")
    val datas: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    //无状态数据操作，只对当前的采集周期内的数据进行处理
    //在某些场合下，需要保留数据统计的结果，实现数据汇总
    val wordToOne: DStream[(String, Int)] = datas.map((_, 1))

    //val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    // updateStateByKey : 根据key对数据的状态进行更新
    // 传递的参数中含有两个值
    // 第一个值表示相同key的value数据
    // 第二个值表示缓存区相同key的value数据
    val stage: DStream[(String, Int)] = wordToOne.updateStateByKey(
      (seq: Seq[Int], opt: Option[Int]) => {
        val newCount = opt.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )
    stage.print()

    ssc.start()

    //等待采集器得关闭
    ssc.awaitTermination()
  }
}
