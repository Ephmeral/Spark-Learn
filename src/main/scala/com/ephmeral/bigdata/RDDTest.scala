package com.ephmeral.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");
  val sparkContext = new SparkContext(sparkConf);

  def main(args: Array[String]): Unit = {
    //    rddValueMap();
    //    rddValueFlatMap();
    //    rddValueGlom();
    //    rddValueFilter();
    //    rddValueDistinct();
    //    rddValueSortBy();
    //    rddTwoValueZip();
    //    rddKeyValuePartitionBy();
//    rddKeyValueReduceByKey();
    rddKeyValueAggregateByKey();
  }


  def rddCreateFromMemory(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    sparkConf.set("spark.port.maxRetries", "1280")

    val rdd1 = sparkContext.parallelize(
      List(1, 2, 3, 4)
    )
    val rdd2 = sparkContext.makeRDD(
      List(1, 2, 3, 4)
    )
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)
    sparkContext.stop()
  }

  def rddCreateFromFile(): Unit = {
    val fileRDD: RDD[String] = sparkContext.textFile("data/word.txt")
    fileRDD.collect().foreach(println)
    sparkContext.stop()
  }

  def rddParallelCount(): Unit = {
    val fileRDD: RDD[String] = sparkContext.textFile("data/word.txt", 2)
    fileRDD.collect().foreach(println)
    sparkContext.stop()
  }

  def rddValueMap(): Unit = {
    val dataRDD: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6));
    val dataRDD2: RDD[Int] = dataRDD.map(num => num * 2);
    val dataRDD3: RDD[String] = dataRDD2.map(num => "#" + num);
    dataRDD3.collect().foreach(println);
    sparkContext.stop();
  }

  def rddValueMapPartitions(): Unit = {
    val dataRDD: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6));
    val dataRDD2: RDD[Int] = dataRDD.mapPartitions(num => {
      num.filter(_ % 2 == 0);
    });
    val dataRDD3: RDD[String] = dataRDD2.map(num => "#" + num);
    dataRDD3.collect().foreach(println);
    sparkContext.stop();
  }

  def rddValueMapPartitionsWithIndex(): Unit = {
    val dataRDD: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6));
    val dataRDD2: RDD[String] = dataRDD.mapPartitionsWithIndex(
      (index, data) => {
        data.map(elem => s"Index: $index, Value: $elem")
      }
    );
    val dataRDD3: RDD[String] = dataRDD2.map(num => "#" + num);
    dataRDD3.collect().foreach(println);
    sparkContext.stop();
  }

  def rddValueFlatMap(): Unit = {
    val dataRDD = sparkContext.makeRDD(List(
      List(1, 3, 5), List(2, 4, 6)
    ), 1);
    val dataRDD1 = dataRDD.flatMap(list => list);
    dataRDD1.collect().foreach(println);
    sparkContext.stop();
  }

  def rddValueGlom(): Unit = {
    val dataRDD = sparkContext.makeRDD(List(
      1, 2, 3, 4
    ), 1);
    val dataRDD1: RDD[Array[Int]] = dataRDD.glom();
    dataRDD1.collect().foreach(arr => {
      for (x <- arr) println("x = " + x);
    });
    sparkContext.stop();
  }

  def rddValueGroupBy(): Unit = {
    val dataRDD = sparkContext.makeRDD(List(
      "Hello", "hive", "hbase", "Hadoop"
    ), 1);
    val dataRDD1 = dataRDD.groupBy(
      s => s(0)
    );
    dataRDD1.collect().foreach(println);
    sparkContext.stop();
  }

  def rddValueFilter(): Unit = {
    val dataRDD = sparkContext.makeRDD(List(
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ), 1);
    val dataRDD1 = dataRDD.filter(_ % 2 == 0);
    dataRDD1.collect().foreach(println);
    sparkContext.stop();
  }

  def rddValueDistinct(): Unit = {
    val dataRDD = sparkContext.makeRDD(List(
      1, 2, 3, 4, 1, 2
    ), 1)
    val dataRDD1 = dataRDD.distinct()
    val dataRDD2 = dataRDD.distinct(2)
    dataRDD2.collect().foreach(println);
    sparkContext.stop();
  }

  def rddValueSortBy(): Unit = {
    val dataRDD = sparkContext.makeRDD(List(
      9, 8, 10, 20, 3, 4, 5, 6, 1, 80
    ), 2)
    val dataRDD1 = dataRDD.sortBy(x => x, true, 4)
    dataRDD1.collect().foreach(println);
    sparkContext.stop();
  }

  def rddTwoValueIntersection(): Unit = {
    val dataRDD1 = sparkContext.makeRDD(List(1, 2, 3, 4))
    val dataRDD2 = sparkContext.makeRDD(List(3, 4, 5, 6))
    val dataRDD = dataRDD1.intersection(dataRDD2)
    dataRDD.collect().foreach(println);
    sparkContext.stop();
  }

  def rddTwoValueZip(): Unit = {
    val dataRDD1 = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    val dataRDD2 = sparkContext.makeRDD(List("a", "b", "c", "d", "e"), 2)
    val dataRDD = dataRDD1.zip(dataRDD2)
    dataRDD.collect().foreach(println);
    sparkContext.stop();
  }


  def rddKeyValuePartitionBy(): Unit = {
    val rdd: RDD[(Int, String)] =
      sparkContext.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

    import org.apache.spark.HashPartitioner

    val rdd2: RDD[(Int, String)] =
      rdd.partitionBy(new HashPartitioner(2))
    rdd2.collect().foreach(println);
    sparkContext.stop();
  }

  def rddKeyValueReduceByKey(): Unit = {
    val rdd1 = sparkContext.makeRDD(List(("a", 1), ("b", 2), ("c", 3)));
    val rdd2 = rdd1.reduceByKey(_ + _);
    val rdd3 = rdd1.reduceByKey(_ + _, 2);
    rdd2.collect().foreach(println);
    sparkContext.stop();
  }

  def rddKeyValueAggregateByKey(): Unit = {
    val rdd = sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("c", 3),
      ("b", 4), ("c", 5), ("c", 6)
    ), 2)
    // 0:("a",1),("a",2),("c",3) => (a,10)(c,10)
    // => (a,10)(b,10)(c,20)
    // 1:("b",4),("c",5),("c",6) => (b,10)(c,10)
    val resultRDD = rdd.aggregateByKey(5)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )
    resultRDD.collect().foreach(println)
  }
}
