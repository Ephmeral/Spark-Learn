package com.ephmeral.bigdata.framework.util

import org.apache.spark.SparkContext

object EnvUtil {

  private val scLocl = new ThreadLocal[SparkContext]()

  def put(sc: SparkContext): Unit = {
    scLocl.set(sc);
  }

  def take(): SparkContext = {
    scLocl.get()
  }

  def clear(): Unit = {
    scLocl.remove();
  }
}
