package rdd.spark.Streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import rdd.spark.util.JDBCUtil

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑
 */
object SparkStreaming12_Kafka_Myself {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "master:9092,slave01:9092,slave02:9092", //
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val kafkaDataDs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("atguiguNew"), kafkaPara)
    )
    val adClickData: DStream[AdClickData] = kafkaDataDs.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

        //TODO 通过JDBC周期性获取黑名单数据
        val ds: DStream[((String, String, String), Int)] = adClickData.transform( //TODO 周期性采集数据，在driver端执行
          rdd => {
//            val blacklist:ListBuffer[String] = null
            val blacklist = ListBuffer[String]()
            val con: Connection = JDBCUtil.getConnection
            val pre1: PreparedStatement = con.prepareStatement("select userid from black_list")
            val set1: ResultSet = pre1.executeQuery()
            while (set1.next()){
              blacklist.append(set1.getString(1))
            }
            set1.close()
            pre1.close()
            con.close()
            //TODO 判断点击用户是否在黑名单
            val filterRDD: RDD[AdClickData] = rdd.filter(
              data => {!blacklist.contains(data.user)}
            )
            //TODO 如果用户不在黑名单中，那么进行统计数量
            val mapRDD: RDD[((String, String, String), Int)] = filterRDD.map(
              data => {
                val sdf = new SimpleDateFormat("yyyy-MM-dd")
                val day: String = sdf.format(new Date(data.ts.toLong))
                val user = data.user
                val ad = data.ad
                ((day, user, ad), 1)
              }
            )
            val reduceRDD: RDD[((String, String, String), Int)] = mapRDD.reduceByKey(_ + _)
            reduceRDD
          }
        )




        //TODO 如果统计数量超过点击阈值(30)，那么将用户拉入到黑名单中
      ds.foreachRDD(
        rdd => {
          rdd.foreach {
            case ((day, user, ad), cnt) => {
              println(s"${day} ${user} ${ad} ${cnt}")
              if (cnt > 30){
                //TODO 如果统计数量超过点击阈值(30)，那么将用户拉入到黑名单中
                val con1: Connection = JDBCUtil.getConnection
                val pre2: PreparedStatement = con1.prepareStatement(
                  """
                    |insert into black_list value(?)
                    |on DUPLICATE KEY
                    |UPDATE userid = ?
                    |""".stripMargin)
                pre2.setString(1,user)
                pre2.setString(2,user)
                pre2.close()
                con1.close()
              }else {
                //TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新
                val con1: Connection = JDBCUtil.getConnection
               //查询统计表的数据
               val pre2: PreparedStatement = con1.prepareStatement(
                 """
                   |select * from user_ad_count
                   |where dt = ?
                   |and userid = ?
                   |and adid = ?
                   |""".stripMargin)
                pre2.setString(1,day)
                pre2.setString(2,user)
                pre2.setString(3,ad)
                val rs: ResultSet = pre2.executeQuery()
               //如果存在，就进行更新
                if (rs.next()){
                  //那么接下来就需要进行update更新，但是是继续用con1这个连接吗还是？  不需要，直接用上面的
                  val pre3: PreparedStatement = con1.prepareStatement(
                    """
                      |update user_ad_count
                      |set count = count + ?
                      |where dt = ? and userid = ? and adid = ?
                      |""".stripMargin)
                  pre3.setInt(1,cnt)
                  pre3.setString(2,day)
                  pre3.setString(3,user)
                  pre3.setString(4,ad)
                  pre3.executeUpdate()
                  pre3.close()
                  // TODO 判断更新后的点击数据是否超过阈值，如果超过，那么将用户拉入到黑名单
                  val pre4: PreparedStatement = con1.prepareStatement(
                    """
                      |select userid
                      |from user_ad_count
                      |where count > 30
                      |""".stripMargin)
//                  pre4.setString(1,day)
//                  pre4.setString(2,user)
//                  pre4.setString(3,ad)
                  val rs1: ResultSet = pre4.executeQuery()
                  if (rs1.next()){
                    val pre5: PreparedStatement = con1.prepareStatement(
                      """
                        |insert into black_list (userid) value(?)
                        |ON DUPLICATE KEY
                        |UPDATE userid = ?
                        |""".stripMargin)
                    pre5.setString(1,user)
                    pre5.setString(2,user)
                    pre5.executeUpdate()
                    pre5.close()
                  }
                  rs1.close()
                  pre4.close()
                }else {
                    //如果不存在，那么需要新增数据
                    val pre6: PreparedStatement = con1.prepareStatement(
                      """
                        |insert into user_ad_count (dt,userid,adid,count) values (?,?,?,?)
                        |""".stripMargin)
                  pre6.setString(1,day)
                  pre6.setString(2,user)
                  pre6.setString(3,ad)
                  pre6.setInt(4,cnt)
                  pre6.executeUpdate()
                  pre6.close()
                }
                rs.close()
                pre2.close()
                con1.close()
              }
            }
          }
        }
      )
    ssc.start()
    ssc.awaitTermination()
  }
case class AdClickData(ts:String,area:String,city:String,user:String,ad:String)
}
