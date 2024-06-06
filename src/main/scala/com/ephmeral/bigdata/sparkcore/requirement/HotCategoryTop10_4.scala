package com.ephmeral.bigdata.sparkcore.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object HotCategoryTop10_4 {
  def main(args: Array[String]): Unit = {
    // TOP10 热门品类

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");
    val sc = new SparkContext(sparkConf);

    // 方案三的缺点：
    // 依旧有shuffle操作 ===> 累加器

    val acc = new HotCategoryAccumulator;
    sc.register(acc, "hotCategory");

    // 1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt");

    // 2. 将数据结构转换
    // 点击： (ID, (1, 0, 0))
    // 下单： (ID, (0, 1, 0))
    // 支付： (ID, (0, 0, 1))

    actionRDD.foreach(
      action => {
        val datas = action.split("_");
        if (datas(6) != "-1") {
          acc.add((datas(6), "click"))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",");
          ids.foreach(
            id => {
              acc.add((id, "order"));
            }
          )
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",");
          ids.foreach(
            id => {
              acc.add((id, "pay"));
            }
          )
        } else {
          Nil
        }
      }
    )

    val accValue: mutable.Map[String, HotCategory] = acc.value

    val categories: mutable.Iterable[HotCategory] = accValue.map(_._2)

    val resultRDD: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true;
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true;
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt;
          } else {
            false;
          }
        } else {
          false;
        }
      }
    ).take(10)

    // 6. 将结果采集并输出
    resultRDD.foreach(println);

    sc.stop();
  }

  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int) {

  }

  /* 自定义累加器
   * IN: (ID, 行为类型)
   * OUT: mutable.Map[String, HotCategory]
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
    private val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actiontype = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actiontype == "click") {
        category.clickCnt += 1;
      } else if (actiontype == "order") {
        category.orderCnt += 1;
      } else if (actiontype == "pay") {
        category.payCnt += 1;
      }
      hcMap.update(cid, category);
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap;
      val map2 = other.value;
      map2.foreach {
        case (cid, hc) => {
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt;
          category.orderCnt += hc.orderCnt;
          category.payCnt += hc.payCnt;
          map1.update(cid, category);
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }

}
