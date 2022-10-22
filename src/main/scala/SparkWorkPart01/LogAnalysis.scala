package SparkWorkPart01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LogAnalysis {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("hi")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //TODO 数据准备
    val click : Array[String] = Array(
      "INFO 2019-09-01 00:29:53 requestURI:/click?app=1&p=1&adid=18005472&industry=469&adid=31",
      "INFO 2019-09-01 00:30:31 requestURI:/click?app=2&p=1&adid=18005472&industry=469&adid=31",
      "INFO 2019-09-01 00:31:03 requestURI:/click?app=1&p=1&adid=18005472&industry=469&adid=32",
      "INFO 2019-09-01 00:31:51 requestURI:/click?app=1&p=1&adid=18005472&industry=469&adid=33"
    )

    val imp : Array[String] = Array(
      "INFO 2019-09-01 00:29:53 requestURI:/imp?app=1&p=1&adid=18005472&industry=469&adid=31",
      "INFO 2019-09-01 00:29:53 requestURI:/imp?app=1&p=1&adid=18005472&industry=469&adid=31",
      "INFO 2019-09-01 00:29:53 requestURI:/imp?app=1&p=1&adid=18005472&industry=469&adid=34"
    )

    val rdd1: RDD[(String, (Int, Int))] = sc.makeRDD(click).map(
      lines => {
        val index = lines.lastIndexOf("=")
        /**
         * 第二个参数是截止的索引位置，对应String中的结束位置
         * 从beginIndex开始取，到endIndex结束，从0开始数，其中不包括endIndex位置的字符
         */
        val cli = lines.substring(index + 1, lines.length) //TODO 出现在click.log中的adid
        (cli, (1, 0)) //TODO 此时数据的形式是  (31,"Click")  (31,"Click")  (32,"Click")  (33,"Click")
      }
    )
    val rdd2: RDD[(String, (Int, Int))] = sc.makeRDD(imp).map(
      lines => {
        val index = lines.lastIndexOf("=")
        val imp = lines.substring(index + 1, lines.length)
        (imp, (0, 1))
      }
    )
    /*
    此时的思考，这个时候rdd1和rdd2有几个分区？  根据配置Local[*]，本机是一共有4个cpu（核数） 所以默认情况下有4个分区
     */
    /**
     * 问题: spark中, union方法是否重新分区, 是否会触发shuffle
      结论: 不会shuffle, 不会划分stage, 但是可能重新分区(窄依赖)
      解释:
      (1) 宽窄依赖对应的原称为ShuffleDependency和NarrowDependency, 字面上可以看出来, 只有宽依赖才会发生shuffle. 但是两种依赖都会重新分区,
          因此重分区和是否shuffle没有关系
      (2) 如果被union的多个rdd, 分区规则相同, 那么index相同的分区, 会被整合到多数分区所在的节点. 比如a节点有2个index为0的分区, b节点有1个, union之后的0分区会全部转移到a节点
      (3) 如果分区规则不同, union后生成的UnionRDD, 不会进行重新分区, 而是把每个分区合并记录到分区数组中
     */

    val rdd3: RDD[(String,(Int, Int))] = rdd1.union(rdd2)




    /**
     * pairs.reduceByKey((a: Int, b: Int) => a + b)
     * 源码 : pairs.reduceByKey((accumulatedValue: Int, currentValue: Int) => accumulatedValue + currentValue)
     * 可以看出a代表的是已经叠加后的值，b代表的是当前的值，要进行的操作是对同一个key值的相加
     */
    rdd3.reduceByKey(
      (x,y) => (x._1+y._1,x._2+y._2)       //这里的(x,y)是 ((当前累加值:(x1,x2)),(当前值:(y1,y2))
    ).collect().foreach(println(_))

    /*
    ================打印结果===============
    (33,1,0)
    (34,0,1)
    (31,2,2)
    (32,1,0)
     */
  }
}
