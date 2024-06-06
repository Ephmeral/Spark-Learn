package com.ephmeral.bigdata.sparkcore.accumulate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastValue {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");
    val sc = new SparkContext(sparkConf);
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3),
    ));
    val rdd2 = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("c", 6),
    ));
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2);
    joinRDD.collect().foreach(println);
    sc.stop();
  }

}
