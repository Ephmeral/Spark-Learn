package com.ephmeral.bigdata.accumulate

import org.apache.spark.{SparkConf, SparkContext}

object Accumulate1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");
    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(1, 2, 3, 4));
    val sumAcc = sc.longAccumulator("sum");
    rdd.foreach(
      num => {
        sumAcc.add(num);
      }
    )
    println("sum = " + sumAcc.value);
    sc.stop();
  }
}
