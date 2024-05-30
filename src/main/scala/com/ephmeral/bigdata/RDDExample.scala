package com.ephmeral.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");
    val sc = new SparkContext(sparkConf);

    // 时间戳，省份，城市，用户，广告
    // => ((省份，广告), 1)
    // => ((省份，广告), sum) => (省份，(广告，sum))
    // => 转换后的数据根据省份进行分组，然后降序取top3
    val dataRDD = sc.textFile("data/agent.log");
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
    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3);
      }
    )
    resultRDD.collect().foreach(println)
  }
}
