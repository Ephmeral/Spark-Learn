package com.ephmeral.bigdata.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotCategoryTop10_3 {
  def main(args: Array[String]): Unit = {
    // TOP10 热门品类

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");
    val sc = new SparkContext(sparkConf);

    // 方案二的缺点：
    // 存在大量的shuffle操作（reduceByKey）

    // （ID，点击数） => (ID, (点击数, 0, 0))
    // （ID，下单数） => (ID, (0, 下单数, 0))
    // （ID，支付数） => (ID, (0, 0, 支付数))

    // 1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt");

    // 2. 将数据结构转换
    // 点击： (ID, (1, 0, 0))
    // 下单： (ID, (0, 1, 0))
    // 支付： (ID, (0, 0, 1))

    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas = action.split("_");
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)));
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",");
          ids.map(id => (id, (0, 1, 0)));
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",");
          ids.map(id => (id, (0, 0, 1)));
        } else {
          Nil
        }
      }
    )

    // 3. 将相同ID的数据进行聚合
    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
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
