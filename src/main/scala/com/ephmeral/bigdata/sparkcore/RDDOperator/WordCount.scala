package com.ephmeral.bigdata.sparkcore.RDDOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount {
  val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
  val sc = new SparkContext(sparkConf);

  def main(args: Array[String]): Unit = {
    wordCount9();
  }

  def wordCount1(): Unit = {
    val rdd = sc.makeRDD(List("hello world", "hello silas", "hello spark", "spark silas is a good"));

    val words = rdd.flatMap(_.split(" "));
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount = group.mapValues(iter => iter.size);
    wordCount.collect().foreach(println);
  }

  def wordCount2(): Unit = {
    val rdd = sc.makeRDD(List("hello world", "hello silas", "hello spark", "spark silas is a good"));

    val words = rdd.flatMap(_.split(" "));
    val wordOne = words.map((_, 1));
    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    val wordCount = group.mapValues(iter => iter.size);
    wordCount.collect().foreach(println);
  }

  def wordCount3(): Unit = {
    val rdd = sc.makeRDD(List("hello world", "hello silas", "hello spark", "spark silas is a good"));

    val words = rdd.flatMap(_.split(" "));
    val wordOne = words.map((_, 1));
    val wordCount = wordOne.reduceByKey(_ + _);
    wordCount.collect().foreach(println);
  }

  def wordCount4(): Unit = {
    val rdd = sc.makeRDD(List("hello world", "hello silas", "hello spark", "spark silas is a good"));

    val words = rdd.flatMap(_.split(" "));
    val wordOne = words.map((_, 1));
    val wordCount = wordOne.aggregateByKey(0)(_ + _, _ + _);
    wordCount.collect().foreach(println);
  }

  def wordCount5(): Unit = {
    val rdd = sc.makeRDD(List("hello world", "hello silas", "hello spark", "spark silas is a good"));

    val words = rdd.flatMap(_.split(" "));
    val wordOne = words.map((_, 1));
    val wordCount = wordOne.foldByKey(0)(_ + _);
    wordCount.collect().foreach(println);
  }

  def wordCount6(): Unit = {
    val rdd = sc.makeRDD(List("hello world", "hello silas", "hello spark", "spark silas is a good"));

    val words = rdd.flatMap(_.split(" "));
    val wordOne = words.map((_, 1));
    val wordCount = wordOne.combineByKey(
      v => v,
      (x: Int, y) => x + y,
      (x: Int, y: Int) => x + y,
    );
    wordCount.collect().foreach(println);
  }

  def wordCount7(): Unit = {
    val rdd = sc.makeRDD(List("hello world", "hello silas", "hello spark", "spark silas is a good"));

    val words = rdd.flatMap(_.split(" "));
    val wordOne = words.map((_, 1));
    val wordCount: collection.Map[String, Long] = wordOne.countByKey()
    println(wordCount);
  }

  def wordCount8(): Unit = {
    val rdd = sc.makeRDD(List("hello world", "hello silas", "hello spark", "spark silas is a good"));

    val words = rdd.flatMap(_.split(" "));
    val wordCount = words.countByValue()
    println(wordCount);
  }


  def wordCount9(): Unit = {
    val rdd = sc.makeRDD(List("hello world", "hello silas", "hello spark", "spark silas is a good"));

    val words = rdd.flatMap(_.split(" "));
    val mapWord = words.map(
      word => {
        mutable.Map[String, Long]((word, 1));
      }
    )

    val wordCount = mapWord.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, cnt) => {
            val newCount = map1.getOrElse(word, 0L) + cnt;
            map1.update(word, newCount);
          }
        }
        map1
      }
    )

    println(wordCount);
  }
}
