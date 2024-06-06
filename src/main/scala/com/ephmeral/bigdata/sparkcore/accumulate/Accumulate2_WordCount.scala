package com.ephmeral.bigdata.sparkcore.accumulate

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Accumulate2_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");
    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List("hello", "word", "hello", "silas", "wxl"));
    val wcAcc = new WordCountAccumulator;
    sc.register(wcAcc, "wordCountAcc");

    rdd.foreach(
      word => {
        wcAcc.add(word);
      }
    )
    println("sum = " + wcAcc.value);
    sc.stop();
  }

  /*
  1. 继承AccumulatorV2，自定义泛型
    IN：累加器输入的数据类型
    OUT：累加器输出的数据类型
  2. 重写方法

   */
  class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    var map: mutable.Map[String, Long] = mutable.Map();

    // 判断是否为初始状态
    override def isZero: Boolean = {
      map.isEmpty;
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new WordCountAccumulator();
    }

    override def reset(): Unit = {
      map.clear();
    }

    override def add(word: String): Unit = {
      map(word) = map.getOrElse(word, 0L) + 1L;
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = map;
      val map2 = other.value;

      map = map1.foldLeft(map2)(
        (innerMap, kv) => {
          innerMap(kv._1) = innerMap.getOrElse(kv._1, 0L) + kv._2;
          innerMap;
        }
      )
    }

    override def value: mutable.Map[String, Long] = map;
  }
}
