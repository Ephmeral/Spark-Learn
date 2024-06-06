package com.ephmeral.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlUDF {
  def main(args: Array[String]): Unit = {
    // 创建sparkSQL运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark SQL basic")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefixName", (name: String) => {
      "Name:" + name
    })
    spark.sql("select prefixName(username), age from user").show

    // 关闭环境
    spark.close();
  }
}
