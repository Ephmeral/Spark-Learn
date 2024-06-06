package com.ephmeral.bigdata.sparkcore.framework.service

import com.ephmeral.bigdata.sparkcore.framework.common.TService
import com.ephmeral.bigdata.sparkcore.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/*
* 服务层
 */
class WordCountService extends TService {

  private val wordCountDao = new WordCountDao;

  // 数据分析
  def dataAnalysis(): Array[(String, List[(String, Int)])] = {

    val dataRDD: RDD[String] = wordCountDao.readFile("data/agent.log")

    val mapRDD = dataRDD.map(
      line => {
        val datas = line.split(" ");
        ((datas(1), datas(4)), 1)
      }
    )
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    val newMapRDD = reduceRDD.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey();
    val result: Array[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3);
      }
    ).collect()
    result
  }
}
