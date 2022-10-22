package rdd.spark. DataSkew

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

/**
 * key.hashCode % reduce个数 = 分区号
 */
object Consider {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.WARN)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 30000000)

    val rdd2: RDD[(Int, Int)] = rdd1.map(
      x => (if (x > 3000000) (x % 3000000) * 6 else x, 1)
    )

    rdd2.groupByKey().mapPartitionsWithIndex{
      (index,iter) => val elementCount = iter.toArray.length
        Iterator(index + ":" + elementCount)
    }.collect()

    println("ok!!!!")

    Thread.sleep(1000000000)

    sc.stop()
  }
}
