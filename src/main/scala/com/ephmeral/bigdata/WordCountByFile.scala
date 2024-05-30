package com.ephmeral.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountByFile {
  val file: String = "data/wikiOfSpark.txt";
  val sparkConf = new SparkConf().setMaster("local").setAppName("WordCountByFile");
  val sc = new SparkContext(sparkConf);

  def wordCount1(): Unit = {
    val lineRDD: RDD[String] = sc.textFile(file);
    val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" ")).filter(word => !word.equals(""))
    val kvRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1));
    val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)

    // 打印词频最高的5个词汇
    val tuples: Array[(Int, String)] = wordCounts.map {
      case (k, v) => (v, k)
    }.sortByKey(false).take(5);
    tuples.foreach(println);
  }

  def main(args: Array[String]): Unit = {
    wordCount1();
  }
}
