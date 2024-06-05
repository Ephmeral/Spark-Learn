package com.ephmeral.bigdata.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotCategoryTop10Session {
  def main(args: Array[String]): Unit = {
    // TOP10 热门品类

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");
    val sc = new SparkContext(sparkConf);
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt");
    actionRDD.cache();


    val top10Ids: Array[String] = top10Category(actionRDD)

    // 1. 过滤原始数据，保留点击和前10品类ID
    val filterActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas = action.split("_");
        if (datas(6) != "-1") {
          top10Ids.contains(datas(6));
        } else {
          false;
        }
      }
    )
    // 2. 根据品类ID和session进行点击量统计
    val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
      action => {
        val datas = action.split("_");
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 将统计结果进行结构转换
    // ((ID, SessionID), sum) => (ID, (sessionID, sum))

    val mapRDD = reduceRDD.map{
      case ((cid, sid), sum) =>
        (cid, (sid, sum))
    }

    // 4. 相同品类进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    // 5. 将分组后的数据进行点击量的排序，取前10
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10);
      }
    )

    resultRDD.collect().foreach(println);

    sc.stop();
  }

  def top10Category(actionRDD: RDD[String]): Array[String] = {
    // 2. 统计品类的点击数量：（ID，点击数）
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1";
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas = action.split("_");
        (datas(6), 1);
      }
    ).reduceByKey(_ + _);

    // 3. 统计品类的下单数量：（ID，下单数）
    val orderActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas = action.split("_");
        datas(8) != "null";
      }
    )
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_");
        val cids = datas(8).split(",");
        cids.map(id => (id, 1));
      }
    ).reduceByKey(_ + _);

    // 4. 统计品类的支付数量：（ID，支付数）
    val payActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas = action.split("_");
        datas(10) != "null";
      }
    )
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val datas = action.split("_");
        val cids = datas(10).split(",");
        cids.map(id => (id, 1));
      }
    ).reduceByKey(_ + _);

    val rdd1: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0));
      }
    }
    val rdd2: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0));
      }
    }
    val rdd3: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt));
      }
    }

    // 将三个数据源合并在一起，统一进行聚合计算
    val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

    val analysisRDD = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD: Array[String] = analysisRDD.sortBy(_._2, false).take(10).map(_._1)
    resultRDD
  }

}
