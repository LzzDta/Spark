package rdd.spark.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD对象的持久化操作不一定是为了重用，在数据执行较长，或数据比较重要的场合也可以采用持久化操作
 */
object Spark03_RDD_Persis {
  def main(args: Array[String]): Unit = {
    /**
     * cache : 将数据临时存储在内存中进行数据重用，数据不安全
     *         会在血缘关系添加新的依赖，一旦出现问题可以重头读取数据
     * persist : 将数据临时存储在磁盘文件中进行数据重用，数据安全，但是性能较差，涉及到磁盘I/O
     *           如果作业执行完毕，临时保存的数据文件就会丢失
     * checkpoint : 将数据长久地保存在磁盘文件中进行数据重用，涉及到磁盘I/O，性能较低，但是数据安全
     *              为了保证数据安全，所以一般情况下，会独立执行作业，为了提高效率，一般情况下和cache
     *              联合使用。执行过程中会切断血缘关系，重新建立新的血缘关系，等同于改变数据源
     */
    val conf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("cp")

    val list: List[String] = List("HBase Flink", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))


    mapRDD.cache()
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("*******************************************")
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
  }
}
