package rdd.spark.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD对象的持久化操作不一定是为了重用，在数据执行较长，或数据比较重要的场合也可以采用持久化操作
 */
object Spark02_RDD_Persis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("datas")

    val list: List[String] = List("HBase Flink", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))


    //cache默认持久化的操作，只能将数据只能将数据保存到内存中，如果想要保存到磁盘文件，需要更改存储级别
    //   apRDD.cache() 持久化操作，后续对mapRDD进行重用的时候不需要从头读数据

    //持久化操作必须在行动算子执行时完成
//    mapRDD.persist(StorageLevel.DISK_ONLY)



    //checkpoint 需要落盘，需要指定检查点保存路径
    //检查点路径保存的文件，当作业执行完毕后，不会被删除
    //一般保存路径都是在分布式存储系统：HDFS
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("*******************************************")
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
  }
}
