package rdd.spark.core.framework.service

import org.apache.spark.rdd.RDD
import rdd.spark.core.framework.dao.WordCountDao


class WordCountService {
  private val wordCountDao = new WordCountDao()

  def dataAnalysis(): Unit ={

  }
}
