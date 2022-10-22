package rdd.spark.Streaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SparkStreaming09_MockData {
  def main(args: Array[String]): Unit = {
    //TODO 生成模拟数据
    // 时间戳 区域 城市 用户 广告
    //Application => Kafka => SparkStreaming
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"slave02:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prop)
    while (true) {
      mockdata().foreach(   //遍历每一条数据
        data => {
          //向Kafka中生成数据
          val record = new ProducerRecord[String,String]("atguiguNew",data)
          producer.send(record)
        }
      )
      Thread.sleep(2000)
    }
  }
  def mockdata()  = {
    val list = ListBuffer[String]()
    val areaList = ListBuffer[String]("华北","华东","华南")
    val cityList = ListBuffer[String]("北京","上海","深圳")
    for ( i <- 1 to 30){
        val area = areaList(new Random().nextInt(3))
        val city: String = cityList(new Random().nextInt(3))
        val userid: Int = new Random().nextInt(6)+1
        val adid: Int = new Random().nextInt(6)+1
        list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }
    list
  }
}
