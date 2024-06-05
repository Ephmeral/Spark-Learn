package com.ephmeral.bigdata.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotCategoryTop10_2 {
  def main(args: Array[String]): Unit = {
    // TOP10 热门品类

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");
    val sc = new SparkContext(sparkConf);

    // 方案一的缺点：
    // actionRDD重复使用 => 加缓存
    // cogroup存在性能低的情况

    // （ID，点击数） => (ID, (点击数, 0, 0))
    // （ID，下单数） => (ID, (0, 下单数, 0))
    // （ID，支付数） => (ID, (0, 0, 支付数))

    // 1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt");
    actionRDD.cache();

    // 2. 统计品类的点击数量：（ID，点击数）
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_");
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


    // 5. 将品类进行排序，并且取前10名
    // 点击数，下单数，支付数
    // 元祖排序，先比较第一个，接着是第二个，以此类推
    // (ID, (点击数，下单数，支付数））
    // cogroup = connect + group
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

    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)

    // 6. 将结果采集并输出
    resultRDD.foreach(println);

    sc.stop();
  }

}
