package com.ephmeral.bigdata.sparkcore.RDDOperator

import org.apache.spark.{SparkConf, SparkContext}

object RDDActionOp {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");
  val sc = new SparkContext(sparkConf);

  def main(args: Array[String]): Unit = {
    RDDReduce();
    RDDAggregate();
    RDDCountByKey;
    RDDForeach()
  }

  def RDDReduce(): Unit = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6));
    //    val i = rdd.reduce(_+_);
    //    println(i); // 21

    //    val nums = rdd.collect();
    //    println(nums.mkString(","));
    val cnt = rdd.count();
    println("cnt = " + cnt);

    // 返回 RDD 中元素的个数
    val firstResult: Int = rdd.first()
    println(firstResult)

    // 返回 RDD 中元素的个数
    val takeResult: Array[Int] = rdd.take(2)
    println(takeResult.mkString(","))
  }

  def RDDAggregate(): Unit = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    val result = rdd.aggregate(0)(_ + _, _ + _);
    println("result = " + result);

    val rdd2 = sc.makeRDD(List(1, 2, 3, 4), 2);
    val res = rdd2.aggregate(10)(_ + _, _ + _);
    println("res = " + res);

    {
      val res = rdd2.fold(10)(_ + _);
      println("res = " + res);
    }
  }

  def RDDCountByKey(): Unit = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2), 2);

    val intToLong: collection.Map[Int, Long] = rdd.countByValue();
    println(intToLong)

    {
      val rdd = sc.makeRDD(List(
        ('a', 1), ('a', 2), ('a', 3), ('b', 1), ('b', 2), ('c', 3)
      ));

      val charToLong = rdd.countByKey();
      println(charToLong);
    }
  }

  def RDDSave(): Unit = {
    val rdd = sc.makeRDD(List(('a', 1), ('a', 2), ('a', 3), ('b', 1), ('b', 2), ('c', 3)));

    rdd.saveAsTextFile("output1");
    rdd.saveAsObjectFile("output2");
  }

  def RDDForeach(): Unit = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4));
    rdd.collect().foreach(println);

    println("*******************************");
    rdd.foreach(println);
  }
}
