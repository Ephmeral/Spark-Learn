package com.ephmeral.bigdata.framework.common

import com.ephmeral.bigdata.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
  def start(master: String = "local[*]", appName: String = "Application")(op: => Unit): Unit = {
    val sparkConf = new SparkConf().setMaster(master).setAppName(appName);
    val sc = new SparkContext(sparkConf);
    EnvUtil.put(sc);

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }
    sc.stop()
    EnvUtil.clear()
  }
}
